// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    mem,
    sync::{Arc, Mutex},
};

use collections::HashMap;
use file_system::{set_io_type, IoType};
use kvproto::{kvrpcpb::CommandPri, pdpb::QueryKind};
use pd_client::{Feature, FeatureGate};
use prometheus::local::*;
use raftstore::store::WriteStats;
use resource_control::{ControlledFuture, ResourceController};
use tikv_util::{
    sys::SysQuota,
    yatp_pool::{Full, FuturePool, PoolTicker, YatpPoolBuilder},
};
use yatp::queue::Extras;

use crate::storage::{
    kv::{destroy_tls_engine, set_tls_engine, Engine, FlowStatsReporter, Statistics},
    metrics::*,
    test_util::latest_feature_gate,
};

pub struct SchedLocalMetrics {
    local_scan_details: HashMap<&'static str, Statistics>,
    command_keyread_histogram_vec: LocalHistogramVec,
    local_write_stats: WriteStats,
}

thread_local! {
    static TLS_SCHED_METRICS: RefCell<SchedLocalMetrics> = RefCell::new(
        SchedLocalMetrics {
            local_scan_details: HashMap::default(),
            command_keyread_histogram_vec: KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local(),
            local_write_stats:WriteStats::default(),
        }
    );

    static TLS_FEATURE_GATE: RefCell<FeatureGate> = RefCell::new(latest_feature_gate());
}

#[derive(Clone)]
pub struct SchedTicker<R: FlowStatsReporter> {
    reporter: R,
}

impl<R: FlowStatsReporter> PoolTicker for SchedTicker<R> {
    fn on_tick(&mut self) {
        tls_flush(&self.reporter);
    }
}

#[derive(Clone)]
pub enum SchedPool {
    // separated thread pools for different priority commands
<<<<<<< HEAD
    Vanilla {
        high_worker_pool: FuturePool,
        worker_pool: FuturePool,
    },
    // one priority based thread pool to handle all commands
    Priority {
        worker_pool: FuturePool,
        resource_ctl: Arc<ResourceController>,
    },
=======
    Vanilla,
    // automatically switch between the `single-queue pool` and `priority-queue pool` based on the
    // resource group settings, only used when the resource control feature is enabled.
    Dynamic,
}

#[derive(Clone)]
struct VanillaQueue {
    high_worker_pool: FuturePool,
    worker_pool: FuturePool,
}

impl VanillaQueue {
    fn spawn(
        &self,
        priority_level: CommandPri,
        f: impl futures::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Full> {
        if priority_level == CommandPri::High {
            self.high_worker_pool.spawn(f)
        } else {
            self.worker_pool.spawn(f)
        }
    }

    fn scale_pool_size(&self, pool_size: usize) {
        self.high_worker_pool
            .scale_pool_size(std::cmp::max(1, pool_size / 2));
        self.worker_pool.scale_pool_size(pool_size);
    }

    fn get_pool_size(&self, priority_level: CommandPri) -> usize {
        if priority_level == CommandPri::High {
            self.high_worker_pool.get_pool_size()
        } else {
            self.worker_pool.get_pool_size()
        }
    }
}

#[derive(Clone)]
struct PriorityQueue {
    worker_pool: FuturePool,
    resource_ctl: Arc<ResourceController>,
    resource_mgr: Arc<ResourceGroupManager>,
}

impl PriorityQueue {
    fn spawn(
        &self,
        metadata: TaskMetadata<'_>,
        priority_level: CommandPri,
        f: impl futures::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Full> {
        let fixed_level = match priority_level {
            CommandPri::High => Some(0),
            CommandPri::Normal => None,
            CommandPri::Low => Some(2),
        };
        // TODO: maybe use a better way to generate task_id
        let task_id = rand::random::<u64>();
        let group_name = metadata.group_name().to_owned();
        let resource_limiter = self.resource_mgr.get_resource_limiter(
            unsafe { std::str::from_utf8_unchecked(&group_name) },
            "",
            metadata.override_priority() as u64,
        );
        let mut extras = Extras::new_multilevel(task_id, fixed_level);
        extras.set_metadata(metadata.to_vec());
        self.worker_pool.spawn_with_extras(
            with_resource_limiter(
                ControlledFuture::new(f, self.resource_ctl.clone(), group_name),
                resource_limiter,
            ),
            extras,
        )
    }

    fn scale_pool_size(&self, pool_size: usize) {
        self.worker_pool.scale_pool_size(pool_size);
    }

    fn get_pool_size(&self) -> usize {
        self.worker_pool.get_pool_size()
    }
}

#[derive(Clone)]
pub struct SchedPool {
    vanilla: VanillaQueue,
    priority: Option<PriorityQueue>,
    queue_type: QueueType,
>>>>>>> 66847e9c5a (*: remove unnecessary async blocks to save memory (#16541))
}

impl SchedPool {
    pub fn new<E: Engine, R: FlowStatsReporter>(
        engine: E,
        pool_size: usize,
        reporter: R,
        feature_gate: FeatureGate,
        resource_ctl: Option<Arc<ResourceController>>,
    ) -> Self {
        let builder = |pool_size: usize, name_prefix: &str| {
            let engine = Arc::new(Mutex::new(engine.clone()));
            let feature_gate = feature_gate.clone();
            let reporter = reporter.clone();
            // for low cpu quota env, set the max-thread-count as 4 to allow potential cases
            // that we need more thread than cpu num.
            let max_pool_size = std::cmp::max(
                pool_size,
                std::cmp::max(4, SysQuota::cpu_cores_quota() as usize),
            );
            YatpPoolBuilder::new(SchedTicker {reporter:reporter.clone()})
                .thread_count(1, pool_size, max_pool_size)
                .name_prefix(name_prefix)
                // Safety: by setting `after_start` and `before_stop`, `FuturePool` ensures
                // the tls_engine invariants.
                .after_start(move || {
                    set_tls_engine(engine.lock().unwrap().clone());
                    set_io_type(IoType::ForegroundWrite);
                    TLS_FEATURE_GATE.with(|c| *c.borrow_mut() = feature_gate.clone());
                })
                .before_stop(move || unsafe {
                    // Safety: we ensure the `set_` and `destroy_` calls use the same engine type.
                    destroy_tls_engine::<E>();
                    tls_flush(&reporter);
                })
        };
        if let Some(ref r) = resource_ctl {
            SchedPool::Priority {
                worker_pool: builder(pool_size, "sched-worker-pool")
                    .build_priority_future_pool(r.clone()),
                resource_ctl: r.clone(),
            }
        } else {
            SchedPool::Vanilla {
                worker_pool: builder(pool_size, "sched-worker-pool").build_future_pool(),
                high_worker_pool: builder(std::cmp::max(1, pool_size / 2), "sched-high-pri-pool")
                    .build_future_pool(),
            }
        }
    }

    pub fn spawn(
        &self,
        group_name: &str,
        priority: CommandPri,
        f: impl futures::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Full> {
        match self {
            SchedPool::Vanilla {
                high_worker_pool,
                worker_pool,
            } => {
                if priority == CommandPri::High {
                    high_worker_pool.spawn(f)
                } else {
                    worker_pool.spawn(f)
                }
            }
            SchedPool::Priority {
                worker_pool,
                resource_ctl,
            } => {
                let fixed_level = match priority {
                    CommandPri::High => Some(0),
                    CommandPri::Normal => None,
                    CommandPri::Low => Some(2),
                };
                // TODO: maybe use a better way to generate task_id
                let task_id = rand::random::<u64>();
                let mut extras = Extras::new_multilevel(task_id, fixed_level);
                extras.set_metadata(group_name.as_bytes().to_owned());
                worker_pool.spawn_with_extras(
                    ControlledFuture::new(
                        async move {
                            f.await;
                        },
                        resource_ctl.clone(),
                        group_name.as_bytes().to_owned(),
                    ),
                    extras,
                )
            }
        }
    }

    pub fn scale_pool_size(&self, pool_size: usize) {
        match self {
            SchedPool::Vanilla {
                high_worker_pool,
                worker_pool,
            } => {
                high_worker_pool.scale_pool_size(std::cmp::max(1, pool_size / 2));
                worker_pool.scale_pool_size(pool_size);
            }
            SchedPool::Priority { worker_pool, .. } => {
                worker_pool.scale_pool_size(pool_size);
            }
        }
    }

    pub fn get_pool_size(&self, priority: CommandPri) -> usize {
        match self {
            SchedPool::Vanilla {
                high_worker_pool,
                worker_pool,
            } => {
                if priority == CommandPri::High {
                    high_worker_pool.get_pool_size()
                } else {
                    worker_pool.get_pool_size()
                }
            }
            SchedPool::Priority { worker_pool, .. } => worker_pool.get_pool_size(),
        }
    }
}

pub fn tls_collect_scan_details(cmd: &'static str, stats: &Statistics) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .local_scan_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
    });
}

pub fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_SCHED_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        for (cmd, stat) in m.local_scan_details.drain() {
            for (cf, cf_details) in stat.details().iter() {
                for (tag, count) in cf_details.iter() {
                    KV_COMMAND_SCAN_DETAILS
                        .with_label_values(&[cmd, *cf, *tag])
                        .inc_by(*count as u64);
                }
            }
        }
        m.command_keyread_histogram_vec.flush();

        // Report PD metrics
        if !m.local_write_stats.is_empty() {
            let mut write_stats = WriteStats::default();
            mem::swap(&mut write_stats, &mut m.local_write_stats);
            reporter.report_write_stats(write_stats);
        }
    });
}

pub fn tls_collect_query(region_id: u64, kind: QueryKind) {
    TLS_SCHED_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.local_write_stats.add_query_num(region_id, kind);
    });
}

pub fn tls_collect_keyread_histogram_vec(cmd: &str, count: f64) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .command_keyread_histogram_vec
            .with_label_values(&[cmd])
            .observe(count);
    });
}

pub fn tls_can_enable(feature: Feature) -> bool {
    TLS_FEATURE_GATE.with(|feature_gate| feature_gate.borrow().can_enable(feature))
}

#[cfg(test)]
pub fn set_tls_feature_gate(feature_gate: FeatureGate) {
    TLS_FEATURE_GATE.with(|f| *f.borrow_mut() = feature_gate);
}
