// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use api_version::{match_template_api_version, KvFormat, RawValue};
use engine_traits::{raw_ttl::ttl_to_expire_ts, CfName};
use kvproto::kvrpcpb::ApiVersion;
use raw::RawStore;
use tikv_kv::Statistics;
use txn_types::{Key, Value};

use crate::storage::{
    kv::{Modify, WriteData},
    lock_manager::LockManager,
    raw,
    txn::{
        commands::{
            Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext,
            WriteResult,
        },
        Result,
    },
    ProcessResult, Snapshot,
};

// TODO: consider add `KvFormat` generic parameter.
command! {
    /// RawCompareAndSwap checks whether the previous value of the key equals to the given value.
    /// If they are equal, write the new value. The bool indicates whether they are equal.
    /// The previous value is always returned regardless of whether the new value is set.
    RawCompareAndSwap:
        cmd_ty => (Option<Value>, bool),
        display => "kv::command::raw_compare_and_swap {:?}", (ctx),
        content => {
            cf: CfName,
            key: Key,
            previous_value: Option<Value>,
            value: Value,
            ttl: u64,
            api_version: ApiVersion,
        }
}

impl CommandExt for RawCompareAndSwap {
    ctx!();
    tag!(raw_compare_and_swap);
    gen_lock!(key);

    fn write_bytes(&self) -> usize {
        self.key.as_encoded().len() + self.value.len()
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawCompareAndSwap {
    fn process_write(self, snapshot: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let (cf, key, value, previous_value, ctx) =
            (self.cf, self.key, self.value, self.previous_value, self.ctx);
        let mut data = vec![];
        let old_value = RawStore::new(snapshot, self.api_version).raw_get_key_value(
            cf,
            &key,
            &mut Statistics::default(),
        )?;

        let pr = if old_value == previous_value {
            let raw_value = RawValue {
                user_value: value,
                expire_ts: ttl_to_expire_ts(self.ttl),
                is_delete: false,
            };
            let encoded_raw_value = match_template_api_version!(
                API,
                match self.api_version {
                    ApiVersion::API => API::encode_raw_value_owned(raw_value),
                }
            );
            let m = Modify::Put(cf, key, encoded_raw_value);
            data.push(m);
            ProcessResult::RawCompareAndSwapRes {
                previous_value: old_value,
                succeed: true,
            }
        } else {
            ProcessResult::RawCompareAndSwapRes {
                previous_value: old_value,
                succeed: false,
            }
        };
        fail_point!("txn_commands_compare_and_swap");
        let rows = data.len();
        let mut to_be_write = WriteData::from_modifies(data);
        to_be_write.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use api_version::test_kv_format_impl;
    use concurrency_manager::ConcurrencyManager;
    use engine_traits::CF_DEFAULT;
    use kvproto::kvrpcpb::Context;

    use super::*;
<<<<<<< HEAD
    use crate::storage::{lock_manager::DummyLockManager, Engine, Statistics, TestEngineBuilder};
=======
    use crate::storage::{
        lock_manager::MockLockManager,
        txn::{scheduler::get_raw_ext, txn_status_cache::TxnStatusCache},
        Engine, Statistics, TestEngineBuilder,
    };
>>>>>>> 0a34c6f479 (txn: Fix to the prewrite requests retry problem by using TxnStatusCache (#15658))

    #[test]
    fn test_cas_basic() {
        test_kv_format_impl!(test_cas_basic_impl);
    }

    /// Note: for API V2, TestEngine don't support MVCC reading, so `pre_propose` observer is ignored,
    /// and no timestamp will be append to key.
    /// The full test of `RawCompareAndSwap` is in `src/storage/mod.rs`.
    fn test_cas_basic_impl<F: KvFormat>() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let key = b"rk";

        let encoded_key = F::encode_raw_key(key, None);

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            encoded_key.clone(),
            None,
            b"v1".to_vec(),
            0,
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&engine, cm.clone(), cmd).unwrap();
        assert!(prev_val.is_none());
        assert!(succeed);

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            encoded_key.clone(),
            None,
            b"v2".to_vec(),
            1,
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&engine, cm.clone(), cmd).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(!succeed);

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            encoded_key,
            Some(b"v1".to_vec()),
            b"v3".to_vec(),
            2,
            F::TAG,
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&engine, cm, cmd).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(succeed);
    }

    pub fn sched_command<E: Engine>(
        engine: &E,
        cm: ConcurrencyManager,
        cmd: TypedCommand<(Option<Value>, bool)>,
    ) -> Result<(Option<Value>, bool)> {
        let snap = engine.snapshot(Default::default())?;
        use kvproto::kvrpcpb::ExtraOp;
        let mut statistic = Statistics::default();
        let context = WriteContext {
            lock_mgr: &DummyLockManager {},
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics: &mut statistic,
            async_apply_prewrite: false,
<<<<<<< HEAD
=======
            raw_ext,
            txn_status_cache: &TxnStatusCache::new_for_test(),
>>>>>>> 0a34c6f479 (txn: Fix to the prewrite requests retry problem by using TxnStatusCache (#15658))
        };
        let ret = cmd.cmd.process_write(snap, context)?;
        match ret.pr {
            ProcessResult::RawCompareAndSwapRes {
                previous_value,
                succeed,
            } => {
                if succeed {
                    let ctx = Context::default();
                    engine.write(&ctx, ret.to_be_write).unwrap();
                }
                Ok((previous_value, succeed))
            }
            _ => unreachable!(),
        }
    }
<<<<<<< HEAD
=======

    #[test]
    fn test_cas_process_write() {
        test_kv_format_impl!(test_cas_process_write_impl);
    }

    fn test_cas_process_write_impl<F: KvFormat>() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let ts_provider = super::super::test_util::gen_ts_provider(F::TAG);

        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let raw_key = b"rk";
        let raw_value = b"valuek";
        let ttl = 30;
        let encode_value = RawValue {
            user_value: raw_value.to_vec(),
            expire_ts: ttl_to_expire_ts(ttl),
            is_delete: false,
        };
        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            F::encode_raw_key(raw_key, None),
            None,
            raw_value.to_vec(),
            ttl,
            F::TAG,
            Context::default(),
        );
        let mut statistic = Statistics::default();
        let snap = engine.snapshot(Default::default()).unwrap();
        let raw_ext = block_on(get_raw_ext(ts_provider, cm.clone(), true, &cmd.cmd)).unwrap();
        let context = WriteContext {
            lock_mgr: &MockLockManager::new(),
            concurrency_manager: cm,
            extra_op: kvproto::kvrpcpb::ExtraOp::Noop,
            statistics: &mut statistic,
            async_apply_prewrite: false,
            raw_ext,
            txn_status_cache: &TxnStatusCache::new_for_test(),
        };
        let cmd: Command = cmd.into();
        let write_result = cmd.process_write(snap, context).unwrap();
        let modifies_with_ts = vec![Modify::Put(
            CF_DEFAULT,
            F::encode_raw_key(raw_key, Some(101.into())),
            F::encode_raw_value_owned(encode_value),
        )];
        assert_eq!(write_result.to_be_write.modifies, modifies_with_ts);
        if F::TAG == ApiVersion::V2 {
            assert_eq!(write_result.lock_guards.len(), 1);
            let raw_key = vec![api_version::api_v2::RAW_KEY_PREFIX];
            let encoded_key = ApiV2::encode_raw_key(&raw_key, Some(100.into()));
            assert_eq!(
                write_result.lock_guards.first().unwrap().key(),
                &encoded_key
            );
        }
    }
>>>>>>> 0a34c6f479 (txn: Fix to the prewrite requests retry problem by using TxnStatusCache (#15658))
}
