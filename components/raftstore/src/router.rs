// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crossbeam::channel::TrySendError;
use engine_traits::{KvEngine, RaftEngine, Snapshot};
use kvproto::{raft_cmdpb::RaftCmdRequest, raft_serverpb::RaftMessage};
use raft::SnapshotStatus;
use tikv_util::time::ThreadReadId;

use crate::{
    store::{
        fsm::RaftRouter,
        transport::{CasualRouter, ProposalRouter, SignificantRouter},
        Callback, CasualMessage, LocalReader, PeerMsg, RaftCmdExtraOpts, RaftCommand,
        SignificantMsg, StoreMsg, StoreRouter,
    },
    DiscardReason, Error as RaftStoreError, Result as RaftStoreResult,
};
/// Routes messages to the raftstore.
pub trait RaftStoreRouter<EK>:
    StoreRouter<EK>
    + ProposalRouter<EK::Snapshot>
    + CasualRouter<EK>
    + SignificantRouter<EK>
    + Send
    + Clone
where
    EK: KvEngine,
{
    /// Sends RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()>;

    /// Broadcast a message generated by `msg_gen` to all Raft groups.
    fn broadcast_normal(&self, msg_gen: impl FnMut() -> PeerMsg<EK>);

    /// Send a casual message to the given region.
    fn send_casual_msg(&self, region_id: u64, msg: CasualMessage<EK>) -> RaftStoreResult<()> {
        <Self as CasualRouter<EK>>::send(self, region_id, msg)
    }

    /// Send a store message to the backend raft batch system.
    fn send_store_msg(&self, msg: StoreMsg<EK>) -> RaftStoreResult<()> {
        <Self as StoreRouter<EK>>::send(self, msg)
    }

    /// Sends RaftCmdRequest to local store.
    fn send_command(
        &self,
        req: RaftCmdRequest,
        cb: Callback<EK::Snapshot>,
        extra_opts: RaftCmdExtraOpts,
    ) -> RaftStoreResult<()> {
        send_command_impl::<EK, _>(self, req, cb, extra_opts)
    }

    /// Reports the peer being unreachable to the Region.
    fn report_unreachable(&self, region_id: u64, to_peer_id: u64) -> RaftStoreResult<()> {
        let msg = SignificantMsg::Unreachable {
            region_id,
            to_peer_id,
        };
        self.significant_send(region_id, msg)
    }

    /// Reports the sending snapshot status to the peer of the Region.
    fn report_snapshot_status(
        &self,
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    ) -> RaftStoreResult<()> {
        let msg = SignificantMsg::SnapshotStatus {
            region_id,
            to_peer_id,
            status,
        };
        self.significant_send(region_id, msg)
    }

    /// Broadcast an `StoreUnreachable` event to all Raft groups.
    fn broadcast_unreachable(&self, store_id: u64) {
        let _ = self.send_store_msg(StoreMsg::StoreUnreachable { store_id });
    }

    /// Report a `StoreResolved` event to all Raft groups.
    fn report_resolved(&self, store_id: u64, group_id: u64) {
        self.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::StoreResolved { store_id, group_id })
        })
    }
}

fn send_command_impl<EK, PR>(
    router: &PR,
    req: RaftCmdRequest,
    cb: Callback<EK::Snapshot>,
    extra_opts: RaftCmdExtraOpts,
) -> RaftStoreResult<()>
where
    EK: KvEngine,
    PR: ProposalRouter<EK::Snapshot>,
{
    let region_id = req.get_header().get_region_id();
    let mut cmd = RaftCommand::new(req, cb);
    cmd.extra_opts = extra_opts;
    router
        .send(cmd)
        .map_err(|e| handle_send_error(region_id, e))
}

pub trait LocalReadRouter<EK>: Send + Clone
where
    EK: KvEngine,
{
    fn read(
        &mut self,
        read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback<EK::Snapshot>,
    ) -> RaftStoreResult<()>;

    fn release_snapshot_cache(&mut self);
}

#[derive(Clone)]
pub struct RaftStoreBlackHole;

impl<EK: KvEngine> CasualRouter<EK> for RaftStoreBlackHole {
    fn send(&self, _: u64, _: CasualMessage<EK>) -> RaftStoreResult<()> {
        Ok(())
    }
}

impl<EK: KvEngine> SignificantRouter<EK> for RaftStoreBlackHole {
    fn significant_send(&self, _: u64, _: SignificantMsg<EK::Snapshot>) -> RaftStoreResult<()> {
        Ok(())
    }
}

impl<S: Snapshot> ProposalRouter<S> for RaftStoreBlackHole {
    fn send(&self, _: RaftCommand<S>) -> std::result::Result<(), TrySendError<RaftCommand<S>>> {
        Ok(())
    }
}

impl<EK> StoreRouter<EK> for RaftStoreBlackHole
where
    EK: KvEngine,
{
    fn send(&self, _: StoreMsg<EK>) -> RaftStoreResult<()> {
        Ok(())
    }
}

impl<EK> RaftStoreRouter<EK> for RaftStoreBlackHole
where
    EK: KvEngine,
{
    /// Sends RaftMessage to local store.
    fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
        Ok(())
    }

    fn broadcast_normal(&self, _: impl FnMut() -> PeerMsg<EK>) {}
}

/// A router that routes messages to the raftstore
pub struct ServerRaftStoreRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    router: RaftRouter<EK, ER>,
    local_reader: LocalReader<EK, RaftRouter<EK, ER>>,
}

impl<EK, ER> Clone for ServerRaftStoreRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn clone(&self) -> Self {
        ServerRaftStoreRouter {
            router: self.router.clone(),
            local_reader: self.local_reader.clone(),
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> ServerRaftStoreRouter<EK, ER> {
    /// Creates a new router.
    pub fn new(
        router: RaftRouter<EK, ER>,
        local_reader: LocalReader<EK, RaftRouter<EK, ER>>,
    ) -> ServerRaftStoreRouter<EK, ER> {
        ServerRaftStoreRouter {
            router,
            local_reader,
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> StoreRouter<EK> for ServerRaftStoreRouter<EK, ER> {
    fn send(&self, msg: StoreMsg<EK>) -> RaftStoreResult<()> {
        StoreRouter::send(&self.router, msg)
    }
}

impl<EK: KvEngine, ER: RaftEngine> ProposalRouter<EK::Snapshot> for ServerRaftStoreRouter<EK, ER> {
    fn send(
        &self,
        cmd: RaftCommand<EK::Snapshot>,
    ) -> std::result::Result<(), TrySendError<RaftCommand<EK::Snapshot>>> {
        ProposalRouter::send(&self.router, cmd)
    }
}

impl<EK: KvEngine, ER: RaftEngine> CasualRouter<EK> for ServerRaftStoreRouter<EK, ER> {
    fn send(&self, region_id: u64, msg: CasualMessage<EK>) -> RaftStoreResult<()> {
        CasualRouter::send(&self.router, region_id, msg)
    }
}

impl<EK: KvEngine, ER: RaftEngine> SignificantRouter<EK> for ServerRaftStoreRouter<EK, ER> {
    fn significant_send(
        &self,
        region_id: u64,
        msg: SignificantMsg<EK::Snapshot>,
    ) -> RaftStoreResult<()> {
        self.router.significant_send(region_id, msg)
    }
}

impl<EK: KvEngine, ER: RaftEngine> RaftStoreRouter<EK> for ServerRaftStoreRouter<EK, ER> {
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        RaftStoreRouter::send_raft_msg(&self.router, msg)
    }

    fn broadcast_normal(&self, msg_gen: impl FnMut() -> PeerMsg<EK>) {
        self.router.broadcast_normal(msg_gen)
    }
}

impl<EK: KvEngine, ER: RaftEngine> LocalReadRouter<EK> for ServerRaftStoreRouter<EK, ER> {
    fn read(
        &mut self,
        read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback<EK::Snapshot>,
    ) -> RaftStoreResult<()> {
        self.local_reader.read(read_id, req, cb);
        Ok(())
    }

    fn release_snapshot_cache(&mut self) {
        self.local_reader.release_snapshot_cache();
    }
}

#[inline]
pub fn handle_send_error<T>(region_id: u64, e: TrySendError<T>) -> RaftStoreError {
    match e {
        TrySendError::Full(_) => RaftStoreError::Transport(DiscardReason::Full),
        TrySendError::Disconnected(_) => RaftStoreError::RegionNotFound(region_id),
    }
}

impl<EK: KvEngine, ER: RaftEngine> RaftStoreRouter<EK> for RaftRouter<EK, ER> {
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let region_id = msg.get_region_id();
        self.send_raft_message(msg)
            .map_err(|e| handle_send_error(region_id, e))
    }

    fn broadcast_normal(&self, msg_gen: impl FnMut() -> PeerMsg<EK>) {
        batch_system::Router::broadcast_normal(self, msg_gen)
    }
}
<<<<<<< HEAD
=======

// Because `CasualRouter` needs an generic while `RaftRotuer` doesn't. We have
// to bridge two by manually implementations. Using functions to reduce
// duplicated codes.

impl<EK: KvEngine, ER: RaftEngine> crate::coprocessor::StoreHandle for RaftRouter<EK, ER> {
    fn update_approximate_size(&self, region_id: u64, size: u64) {
        if let Err(e) = CasualRouter::send(
            self,
            region_id,
            CasualMessage::RegionApproximateSize { size },
        ) {
            warn!(
                "failed to send approximate region size";
                "region_id" => region_id,
                "err" => %e,
                "error_code" => %e.error_code(),
            );
        }
    }

    fn update_approximate_keys(&self, region_id: u64, keys: u64) {
        if let Err(e) = CasualRouter::send(
            self,
            region_id,
            CasualMessage::RegionApproximateKeys { keys },
        ) {
            warn!(
                "failed to send approximate region keys";
                "region_id" => region_id,
                "err" => %e,
                "error_code" => %e.error_code(),
            );
        }
    }

    fn ask_split(
        &self,
        region_id: u64,
        region_epoch: metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        source: Cow<'static, str>,
    ) {
        if let Err(e) = CasualRouter::send(
            self,
            region_id,
            CasualMessage::SplitRegion {
                region_epoch,
                split_keys,
                callback: Callback::None,
                source,
                share_source_region_size: true,
            },
        ) {
            warn!(
                "failed to send ask split";
                "region_id" => region_id,
                "err" => %e,
            );
        }
    }

    fn update_compute_hash_result(
        &self,
        region_id: u64,
        index: u64,
        context: Vec<u8>,
        hash: Vec<u8>,
    ) {
        if let Err(e) = CasualRouter::send(
            self,
            region_id,
            CasualMessage::ComputeHashResult {
                index,
                context,
                hash,
            },
        ) {
            warn!(
                "failed to send hash compute result";
                "region_id" => region_id,
                "err" => %e,
            );
        }
    }

    fn refresh_region_buckets(
        &self,
        region_id: u64,
        region_epoch: metapb::RegionEpoch,
        buckets: Vec<crate::coprocessor::Bucket>,
        bucket_ranges: Option<Vec<crate::store::BucketRange>>,
    ) {
        let _ = CasualRouter::send(
            self,
            region_id,
            CasualMessage::RefreshRegionBuckets {
                region_epoch,
                buckets,
                bucket_ranges,
                cb: Callback::None,
            },
        );
    }
}

/// A handle for cdc and pitr to schedule some command back to raftstore.
pub trait CdcHandle<EK>: Clone + Send
where
    EK: KvEngine,
{
    fn capture_change(
        &self,
        region_id: u64,
        region_epoch: metapb::RegionEpoch,
        change_observer: ChangeObserver,
        callback: Callback<EK::Snapshot>,
    ) -> RaftStoreResult<()>;

    fn check_leadership(
        &self,
        region_id: u64,
        callback: Callback<EK::Snapshot>,
    ) -> RaftStoreResult<()>;
}

/// A wrapper of SignificantRouter that is specialized for implementing
/// CdcHandle.
#[derive(Clone)]
pub struct CdcRaftRouter<T>(pub T);

impl<T> std::ops::Deref for CdcRaftRouter<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<EK, T> CdcHandle<EK> for CdcRaftRouter<T>
where
    EK: KvEngine,
    T: SignificantRouter<EK> + Send + Clone,
{
    fn capture_change(
        &self,
        region_id: u64,
        region_epoch: metapb::RegionEpoch,
        change_observer: ChangeObserver,
        callback: Callback<EK::Snapshot>,
    ) -> RaftStoreResult<()> {
        self.0.significant_send(
            region_id,
            SignificantMsg::CaptureChange {
                cmd: change_observer,
                region_epoch,
                callback,
            },
        )
    }

    fn check_leadership(
        &self,
        region_id: u64,
        callback: Callback<EK::Snapshot>,
    ) -> RaftStoreResult<()> {
        self.0
            .significant_send(region_id, SignificantMsg::LeaderCallback(callback))
    }
}
>>>>>>> 640143a2da (raftstore: region initial size depends on the split resource . (#15456))
