// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::RaftCmdRequest;
use raft::{
    eraftpb::{self, MessageType},
    Storage,
};
use raftstore::{
    store::{
<<<<<<< HEAD
        can_amend_read, fsm::apply::notify_stale_req, metrics::RAFT_READ_INDEX_PENDING_COUNT,
        msg::ReadCallback, propose_read_index, should_renew_lease, util::LeaseState, ReadDelegate,
        ReadIndexRequest, ReadProgress, Transport,
=======
        can_amend_read, cmd_resp,
        fsm::{apply::notify_stale_req, new_read_index_request},
        metrics::RAFT_READ_INDEX_PENDING_COUNT,
        msg::{ErrorCallback, ReadCallback},
        propose_read_index, should_renew_lease,
        simple_write::SimpleWriteEncoder,
        util::{check_req_region_epoch, LeaseState},
        ReadDelegate, ReadIndexRequest, ReadProgress, Transport,
>>>>>>> 6ca4a629a1 (raftstore-v2: check region epoch before response read index (#15046))
    },
    Error, Result,
};
use slog::debug;
use tikv_util::time::monotonic_raw_now;
use time::Timespec;
use tracker::GLOBAL_TRACKERS;

use crate::{
    batch::StoreContext,
    fsm::StoreMeta,
    raft::Peer,
    router::{QueryResChannel, QueryResult, ReadResponse},
};

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_step_read_index<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        m: &mut eraftpb::Message,
    ) -> bool {
        assert_eq!(m.get_msg_type(), MessageType::MsgReadIndex);

        fail::fail_point!("on_step_read_index_msg");
        ctx.coprocessor_host
            .on_step_read_index(m, self.state_role());
        // Must use the commit index of `PeerStorage` instead of the commit index
        // in raft-rs which may be greater than the former one.
        // For more details, see the annotations above `on_leader_commit_idx_changed`.
        let index = self.storage().entry_storage().commit_index();
        // Check if the log term of this index is equal to current term, if so,
        // this index can be used to reply the read index request if the leader holds
        // the lease. Please also take a look at raft-rs.
        if self.storage().term(index).unwrap() == self.term() {
            let state = self.inspect_lease();
            if let LeaseState::Valid = state {
                // If current peer has valid lease, then we could handle the
                // request directly, rather than send a heartbeat to check quorum.
                let mut resp = eraftpb::Message::default();
                resp.set_msg_type(MessageType::MsgReadIndexResp);
                resp.term = self.term();
                resp.to = m.from;

                resp.index = index;
                resp.set_entries(m.take_entries());

                self.raft_group_mut().raft.msgs.push(resp);
                return true;
            }
        }
        false
    }

    pub fn pre_read_index(&self) -> Result<()> {
        fail::fail_point!("before_propose_readindex", |s| if s
            .map_or(true, |s| s.parse().unwrap_or(true))
        {
            Ok(())
        } else {
            Err(tikv_util::box_err!(
                "[{}] {} can not read due to injected failure",
                self.region_id(),
                self.peer_id()
            ))
        });

        // See more in ready_to_handle_read().
        if self.proposal_control().is_splitting() {
            return Err(Error::ReadIndexNotReady {
                reason: "can not read index due to split",
                region_id: self.region_id(),
            });
        }
        if self.proposal_control().is_merging() {
            return Err(Error::ReadIndexNotReady {
                reason: "can not read index due to merge",
                region_id: self.region_id(),
            });
        }
        Ok(())
    }

    pub(crate) fn read_index_leader<T: Transport>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        ch: QueryResChannel,
    ) {
        let now = monotonic_raw_now();
        let lease_state = self.inspect_lease();
        if can_amend_read::<QueryResChannel>(
            self.pending_reads().back(),
            &req,
            lease_state,
            ctx.cfg.raft_store_max_leader_lease(),
            now,
        ) {
            // Must use the commit index of `PeerStorage` instead of the commit index
            // in raft-rs which may be greater than the former one.
            // For more details, see the annotations above `on_leader_commit_idx_changed`.
            let commit_index = self.storage().entry_storage().commit_index();
            if let Some(read) = self.pending_reads_mut().back_mut() {
                // A read request proposed in the current lease is found; combine the new
                // read request to that previous one, so that no proposing needed.
                read.push_command(req, ch, commit_index);
                return;
            }
        }

        ctx.raft_metrics.propose.read_index.inc();

        let request = req
            .mut_requests()
            .get_mut(0)
            .filter(|req| req.has_read_index())
            .map(|req| req.take_read_index());
        let (id, dropped) = propose_read_index(self.raft_group_mut(), request.as_ref());
        if dropped {
            // The message gets dropped silently, can't be handled anymore.
            notify_stale_req(self.term(), ch);
            ctx.raft_metrics.propose.dropped_read_index.inc();
            return;
        }

        let mut read = ReadIndexRequest::with_command(id, req, ch, now);
        read.addition_request = request.map(Box::new);
        self.pending_reads_mut().push_back(read, true);
        debug!(
            self.logger,
            "request to get a read index";
            "request_id" => ?id,
        );

        self.set_has_ready();
        // TimeoutNow has been sent out, so we need to propose explicitly to
        // update leader lease.
        // TODO:add following when propose is done
        // if self.leader_lease.is_suspect() {
        // let req = RaftCmdRequest::default();
        // if let Ok(Either::Left(index)) = self.propose_normal(ctx, req) {
        // let (callback, _) = CmdResChannel::pair();
        // let p = Proposal {
        // is_conf_change: false,
        // index,
        // term: self.term(),
        // cb: callback,
        // propose_time: Some(now),
        // must_pass_epoch_check: false,
        // };
        //
        // self.post_propose(ctx, p);
        // }
        // }
    }

    /// response the read index request
    ///
    /// awake the read tasks waiting in frontend (such as unified thread pool)
    /// In v1, it's named as response_read.
    pub(crate) fn respond_read_index(
        &self,
        read_index_req: &mut ReadIndexRequest<QueryResChannel>,
    ) {
        debug!(
            self.logger,
            "handle reads with a read index";
            "request_id" => ?read_index_req.id,
        );
        RAFT_READ_INDEX_PENDING_COUNT.sub(read_index_req.cmds().len() as i64);
        let time = monotonic_raw_now();
        for (req, ch, mut read_index) in read_index_req.take_cmds().drain(..) {
            ch.read_tracker().map(|tracker| {
                GLOBAL_TRACKERS.with_tracker(tracker, |t| {
                    t.metrics.read_index_confirm_wait_nanos = (time - read_index_req.propose_time)
                        .to_std()
                        .unwrap()
                        .as_nanos()
                        as u64;
                })
            });

            // Check region epoch before responding read index because region
            // may be splitted or merged during read index.
            if let Err(e) = check_req_region_epoch(&req, self.region(), true) {
                debug!(self.logger,
                    "read index epoch not match";
                    "region_id" => self.region_id(),
                    "err" => ?e,
                );
                let mut response = cmd_resp::new_error(e);
                cmd_resp::bind_term(&mut response, self.term());
                ch.report_error(response);
                return;
            }

            // Key lock should not happen when read_index is running at the leader.
            // Because it only happens when concurrent read and write requests on the same
            // region on different TiKVs.
            assert!(read_index_req.locked.is_none());
            match (read_index, read_index_req.read_index) {
                (Some(local_responsed_index), Some(batch_index)) => {
                    // `read_index` could be less than `read_index_req.read_index` because the
                    // former is filled with `committed index` when
                    // proposed, and the latter is filled
                    // after a read-index procedure finished.
                    read_index = Some(std::cmp::max(local_responsed_index, batch_index));
                }
                (None, _) => {
                    // Actually, the read_index is none if and only if it's the first one in
                    // read_index_req.cmds. Starting from the second, all the following ones'
                    // read_index is not none.
                    read_index = read_index_req.read_index;
                }
                _ => {}
            }
            let read_resp = ReadResponse::new(read_index.unwrap_or(0));
            ch.set_result(QueryResult::Read(read_resp));
        }
    }

    /// Try to renew leader lease.
    pub(crate) fn maybe_renew_leader_lease(
        &mut self,
        ts: Timespec,
        store_meta: &Mutex<StoreMeta<EK>>,
        progress: Option<ReadProgress>,
    ) {
        // A nonleader peer should never has leader lease.
        let read_progress = if !should_renew_lease(
            self.is_leader(),
            self.proposal_control().is_splitting(),
            self.proposal_control().is_merging(),
            self.has_force_leader(),
        ) {
            None
        } else {
            self.leader_lease_mut().renew(ts);
            let term = self.term();
            self.leader_lease_mut()
                .maybe_new_remote_lease(term)
                .map(ReadProgress::set_leader_lease)
        };
        if let Some(progress) = progress {
            let mut meta = store_meta.lock().unwrap();
            let reader = &mut meta.readers.get_mut(&self.region_id()).unwrap().0;
            self.maybe_update_read_progress(reader, progress);
        }
        if let Some(progress) = read_progress {
            let mut meta = store_meta.lock().unwrap();
            let reader = &mut meta.readers.get_mut(&self.region_id()).unwrap().0;
            self.maybe_update_read_progress(reader, progress);
        }
    }

    // Expire lease and unset lease in read delegate on role changed to follower.
    pub(crate) fn expire_lease_on_became_follower(&mut self, store_meta: &Mutex<StoreMeta<EK>>) {
        self.leader_lease_mut().expire();
        let mut meta = store_meta.lock().unwrap();
        if let Some((reader, _)) = meta.readers.get_mut(&self.region_id()) {
            self.maybe_update_read_progress(reader, ReadProgress::unset_leader_lease());
        }
    }

    pub(crate) fn maybe_update_read_progress(
        &self,
        reader: &mut ReadDelegate,
        progress: ReadProgress,
    ) {
        debug!(
            self.logger,
            "update read progress";
            "progress" => ?progress,
        );
        reader.update(progress);
    }

    pub(crate) fn inspect_lease(&mut self) -> LeaseState {
        if !self.raft_group().raft.in_lease() {
            return LeaseState::Suspect;
        }
        // None means now.
        let state = self.leader_lease().inspect(None);
        if LeaseState::Expired == state {
            debug!(
                self.logger,
                "leader lease is expired, lease {:?}",
                self.leader_lease(),
            );
            // The lease is expired, call `expire` explicitly.
            self.leader_lease_mut().expire();
        }
        state
    }
}
