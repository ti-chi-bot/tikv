// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! A helper class to detect flush event and trace apply index.
//!
//! The whole idea is when all CFs have flushed to disk, then the apply index
//! should be able to be advanced to the latest. The implementations depends on
//! the assumption that memtable/write buffer is frozen one by one and flushed
//! one by one.
//!
//! Because apply index can be arbitrary value after restart, so apply related
//! states like `RaftApplyState` and `RegionLocalState` are mapped to index.
//! Once apply index is confirmed, the latest states before apply index should
//! be used as the start state.

use std::{
    collections::{HashMap, LinkedList},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};

use slog_global::info;
use tikv_util::set_panic_mark;

use crate::{data_cf_offset, RaftEngine, RaftLogBatch, DATA_CFS_LEN};

#[derive(Debug)]
pub struct ApplyProgress {
    cf: String,
    apply_index: u64,
    smallest_seqno: u64,
}

impl ApplyProgress {
    fn merge(&mut self, pr: ApplyProgress) {
        debug_assert_eq!(self.cf, pr.cf);
        debug_assert!(self.apply_index <= pr.apply_index);
        self.apply_index = pr.apply_index;
    }

    pub fn applied_index(&self) -> u64 {
        self.apply_index
    }

    pub fn cf(&self) -> &str {
        &self.cf
    }
}

#[derive(Default, Debug)]
struct FlushProgress {
    prs: LinkedList<ApplyProgress>,
    last_flushed: [u64; DATA_CFS_LEN],
}

/// A share state between raftstore and underlying engine.
///
/// raftstore will update state changes and corresponding sst apply index, when
/// apply ingest sst request, it should ensure the sst can be deleted
/// if the flushed index greater than it .
#[derive(Debug, Clone)]
pub struct SstApplyState {
    sst_map: Arc<RwLock<HashMap<Vec<u8>, u64>>>,
}

impl SstApplyState {
    pub fn new() -> Self {
        Self {
            sst_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set the latest sst applied index.
    #[inline]
    pub fn registe_ssts(&self, uuids: Vec<Vec<u8>>, sst_applied_index: u64) {
        let mut map = self.sst_map.write().unwrap();
        for uuid in uuids {
            map.insert(uuid, sst_applied_index);
        }
    }

    /// Query the sst applied index.
    #[inline]
    pub fn sst_applied_index(&self, uuid: &Vec<u8>) -> Option<u64> {
        self.sst_map.read().unwrap().get(uuid).copied()
    }
}

/// A share state between raftstore and underlying engine.
///
/// raftstore will update state changes and corresponding apply index, when
/// flush, `PersistenceListener` will query states related to the memtable
/// and persist the relation to raft engine.
#[derive(Debug)]
pub struct FlushState {
    applied_index: AtomicU64,
}

impl FlushState {
    pub fn new(applied_index: u64) -> Self {
        Self {
            applied_index: AtomicU64::new(applied_index),
        }
    }

    /// Set the latest applied index.
    #[inline]
    pub fn set_applied_index(&self, index: u64) {
        self.applied_index.store(index, Ordering::Release);
    }

    /// Query the applied index.
    #[inline]
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }
}

/// A helper trait to avoid exposing `RaftEngine` to `TabletFactory`.
pub trait StateStorage: Sync + Send {
    fn persist_progress(&self, region_id: u64, tablet_index: u64, pr: ApplyProgress);
}

/// A flush listener that maps memtable to apply index and persist the relation
/// to raft engine.
pub struct PersistenceListener {
    region_id: u64,
    tablet_index: u64,
    state: Arc<FlushState>,
    progress: Mutex<FlushProgress>,
    storage: Arc<dyn StateStorage>,
}

impl PersistenceListener {
    pub fn new(
        region_id: u64,
        tablet_index: u64,
        state: Arc<FlushState>,
        storage: Arc<dyn StateStorage>,
    ) -> Self {
        Self {
            region_id,
            tablet_index,
            state,
            progress: Mutex::new(FlushProgress::default()),
            storage,
        }
    }
}

impl PersistenceListener {
    pub fn flush_state(&self) -> &Arc<FlushState> {
        &self.state
    }

    /// Called when memtable is frozen.
    ///
    /// `smallest_seqno` should be the smallest seqno of the memtable.
    pub fn on_memtable_sealed(&self, cf: String, smallest_seqno: u64) {
        // The correctness relies on the assumption that there will be only one
        // thread writting to the DB and increasing apply index.
        // Apply index will be set within DB lock, so it's correct even with manual
        // flush.
        let offset = data_cf_offset(&cf);
        let apply_index = self.state.applied_index.load(Ordering::SeqCst);
        let mut prs = self.progress.lock().unwrap();
        let flushed = prs.last_flushed[offset];
        if flushed > smallest_seqno {
            panic!(
                "sealed seqno has been flushed {} {} {} <= {}",
                cf, apply_index, smallest_seqno, flushed
            );
        }
        prs.prs.push_back(ApplyProgress {
            cf,
            apply_index,
            smallest_seqno,
        });
    }

    /// Called a memtable finished flushing.
    ///
    /// `largest_seqno` should be the largest seqno of the generated file.
    pub fn on_flush_completed(&self, cf: &str, largest_seqno: u64, file_no: u64) {
        // Maybe we should hook the compaction to avoid the file is compacted before
        // being recorded.
        let offset = data_cf_offset(cf);
        let pr = {
            let mut prs = self.progress.lock().unwrap();
            let flushed = prs.last_flushed[offset];
            if flushed >= largest_seqno {
                // According to facebook/rocksdb#11183, it's possible OnFlushCompleted can be
                // called out of order. But it's guaranteed files are installed in order.
                info!("flush complete reorder found"; "flushed" => flushed, "largest_seqno" => largest_seqno, "file_no" => file_no, "cf" => cf);
                return;
            }
            prs.last_flushed[offset] = largest_seqno;
            let mut cursor = prs.prs.cursor_front_mut();
            let mut flushed_pr = None;
            while let Some(pr) = cursor.current() {
                if pr.cf != cf {
                    cursor.move_next();
                    continue;
                }
                if pr.smallest_seqno <= largest_seqno {
                    match &mut flushed_pr {
                        None => flushed_pr = cursor.remove_current(),
                        Some(flushed_pr) => {
                            flushed_pr.merge(cursor.remove_current().unwrap());
                        }
                    }
                    continue;
                }
                break;
            }
            match flushed_pr {
                Some(pr) => pr,
                None => {
                    set_panic_mark();
                    panic!(
                        "[region_id={}] [tablet_index={}] {} {} {} not found in {:?}",
                        self.region_id, self.tablet_index, cf, largest_seqno, file_no, prs
                    )
                }
            }
        };
        self.storage
            .persist_progress(self.region_id, self.tablet_index, pr);
    }
}

impl<R: RaftEngine> StateStorage for R {
    fn persist_progress(&self, region_id: u64, tablet_index: u64, pr: ApplyProgress) {
        if pr.apply_index == 0 {
            return;
        }
        let mut batch = self.log_batch(1);
        // TODO: It's possible that flush succeeds but fails to call
        // `on_flush_completed` before exit. In this case the flushed data will
        // be replayed again after restarted. To solve the problem, we need to
        // (1) persist flushed file numbers in `on_flush_begin` and (2) check
        // the file number in `on_compaction_begin`. After restart, (3) check if the
        // file exists. If (1) && ((2) || (3)), then we don't need to replay the data.
        batch
            .put_flushed_index(region_id, &pr.cf, tablet_index, pr.apply_index)
            .unwrap();
        self.consume(&mut batch, true).unwrap();
    }
}
