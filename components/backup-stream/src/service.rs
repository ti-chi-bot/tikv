// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use grpcio::RpcContext;
use kvproto::{logbackuppb::*, metapb::Region};
use tikv_util::{warn, worker::Scheduler};

use crate::{
    checkpoint_manager::{GetCheckpointResult, RegionIdWithVersion},
    endpoint::{FlushResult as InternalFlushResult, RegionCheckpointOperation, RegionSet},
    try_send, Task,
};

#[derive(Clone)]
pub struct BackupStreamGrpcService {
    endpoint: Scheduler<Task>,
}

impl BackupStreamGrpcService {
    pub fn new(endpoint: Scheduler<Task>) -> Self {
        Self { endpoint }
    }
}

fn id_of(region: &Region) -> RegionIdentity {
    let mut id = RegionIdentity::new();
    id.set_id(region.get_id());
    id.set_epoch_version(region.get_region_epoch().get_version());
    id
}

impl From<RegionIdWithVersion> for RegionIdentity {
    fn from(val: RegionIdWithVersion) -> Self {
        let mut id = RegionIdentity::new();
        id.set_id(val.region_id);
        id.set_epoch_version(val.region_epoch_version);
        id
    }
}

impl LogBackup for BackupStreamGrpcService {
    fn get_last_flush_ts_of_region(
        &mut self,
        _ctx: RpcContext<'_>,
        mut req: GetLastFlushTsOfRegionRequest,
        sink: grpcio::UnarySink<GetLastFlushTsOfRegionResponse>,
    ) {
        let regions = req
            .take_regions()
            .into_iter()
            .map(|id| (id.id, id.epoch_version))
            .collect::<HashSet<_>>();
        let t = Task::RegionCheckpointsOp(RegionCheckpointOperation::Get(
            RegionSet::Regions(regions),
            Box::new(move |rs| {
                let mut resp = GetLastFlushTsOfRegionResponse::new();
                resp.set_checkpoints(
                    rs.into_iter()
                        .map(|r| match r {
                            GetCheckpointResult::Ok { region, checkpoint } => {
                                let mut r = RegionCheckpoint::new();
                                let id = id_of(&region);
                                r.set_region(id);
                                r.set_checkpoint(checkpoint.into_inner());
                                r
                            }
                            GetCheckpointResult::NotFound { id, err } => {
                                let mut r = RegionCheckpoint::new();
                                r.set_region(id.into());
                                r.set_err(err);
                                r
                            }
                            GetCheckpointResult::EpochNotMatch { region, err } => {
                                let mut r = RegionCheckpoint::new();
                                r.set_region(id_of(&region));
                                r.set_err(err);
                                r
                            }
                        })
                        .collect(),
                );
                tokio::spawn(async {
                    if let Err(e) = sink.success(resp).await {
                        warn!("failed to reply grpc resonse."; "err" => %e)
                    }
                });
            }),
        ));
        try_send!(self.endpoint, t);
    }

    fn subscribe_flush_event(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: SubscribeFlushEventRequest,
        #[allow(unused_variables)] sink: grpcio::ServerStreamingSink<SubscribeFlushEventResponse>,
    ) {
        #[cfg(test)]
        panic!("Service should not be used in an unit test");
        #[cfg(not(test))]
        try_send!(
            self.endpoint,
            Task::RegionCheckpointsOp(RegionCheckpointOperation::Subscribe(sink))
        );
    }

    fn flush_now(
        &mut self,
        ctx: RpcContext<'_>,
        _req: FlushNowRequest,
        sink: grpcio::UnarySink<FlushNowResponse>,
    ) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<InternalFlushResult>(16);
        let t = Task::ForceFlush(crate::router::TaskSelector::All, tx);
        try_send!(self.endpoint, t);
        ctx.spawn(async move {
            let mut results = Vec::new();
            while let Some(flush_result) = rx.recv().await {
                let mut r = FlushResult::new();
                r.set_task_name(flush_result.task);
                match flush_result.error {
                    None => r.set_success(true),
                    Some(err) => {
                        r.set_success(false);
                        r.set_error_message(err.to_string());
                    }
                }
                results.push(r);
            }
            let mut resp = FlushNowResponse::new();
            resp.set_results(results.into());
            if let Err(e) = sink.success(resp).await {
                warn!("failed to reply flush_now response"; "err" => %e);
            }
        });
    }
}
