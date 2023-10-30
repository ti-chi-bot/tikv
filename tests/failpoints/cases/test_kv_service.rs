// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use grpcio::{ChannelBuilder, Environment};
<<<<<<< HEAD
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use test_raftstore::{must_kv_prewrite, must_new_cluster_and_kv_client, must_new_cluster_mul};
=======
use kvproto::{
    kvrpcpb::{PrewriteRequestPessimisticAction::SkipPessimisticCheck, *},
    tikvpb::TikvClient,
};
use test_raftstore::{
    configure_for_lease_read, must_kv_commit, must_kv_have_locks, must_kv_prewrite,
    must_kv_prewrite_with, must_new_cluster_and_kv_client, must_new_cluster_mul,
    new_server_cluster, try_kv_prewrite_with, try_kv_prewrite_with_impl,
};
use tikv_util::{config::ReadableDuration, HandyRwLock};
>>>>>>> 0a34c6f479 (txn: Fix to the prewrite requests retry problem by using TxnStatusCache (#15658))

#[test]
fn test_batch_get_memory_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let mut req = BatchGetRequest::default();
    req.set_context(ctx);
    req.set_keys(vec![b"a".to_vec(), b"b".to_vec()].into());
    req.version = 50;

    fail::cfg("raftkv_async_snapshot_err", "return").unwrap();
    let resp = client.kv_batch_get(&req).unwrap();
    // the injected error should be returned at both places for backward compatibility.
    assert!(!resp.pairs[0].get_error().get_abort().is_empty());
    assert!(!resp.get_error().get_abort().is_empty());
    fail::remove("raftkv_async_snapshot_err");
}

#[test]
fn test_kv_scan_memory_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let mut req = ScanRequest::default();
    req.set_context(ctx);
    req.set_start_key(b"a".to_vec());
    req.version = 50;

    fail::cfg("raftkv_async_snapshot_err", "return").unwrap();
    let resp = client.kv_scan(&req).unwrap();
    // the injected error should be returned at both places for backward compatibility.
    assert!(!resp.pairs[0].get_error().get_abort().is_empty());
    assert!(!resp.get_error().get_abort().is_empty());
    fail::remove("raftkv_async_snapshot_err");
}

#[test]
fn test_scan_lock_push_async_commit() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    for (use_green_gc, ts) in &[(false, 100), (true, 200)] {
        // We will perform a async commit transaction with start_ts == `ts`.
        // First, try pushing max_ts to `ts + 10`.
        if *use_green_gc {
            let mut req = RegisterLockObserverRequest::default();
            req.set_max_ts(ts + 10);
            let resp = client.register_lock_observer(&req).unwrap();
            assert_eq!(resp.error.len(), 0);
        } else {
            let mut req = ScanLockRequest::default();
            req.set_context(ctx.clone());
            req.set_max_version(ts + 10);
            let resp = client.kv_scan_lock(&req).unwrap();
            assert!(!resp.has_region_error());
            assert!(!resp.has_error());
        }

        let k1 = b"k1";
        let v1 = b"v1";

        // The following code simulates another case: prewrite is locking the memlock, and then
        // another scan lock operation request meets the memlock.

        fail::cfg("before-set-lock-in-memory", "pause").unwrap();
        let client1 = client.clone();
        let ctx1 = ctx.clone();
        let handle1 = std::thread::spawn(move || {
            let mut prewrite = PrewriteRequest::default();
            prewrite.set_context(ctx1);
            let mut mutation = Mutation::default();
            mutation.set_op(Op::Put);
            mutation.set_key(k1.to_vec());
            mutation.set_value(v1.to_vec());
            prewrite.set_mutations(vec![mutation].into());
            prewrite.set_primary_lock(k1.to_vec());
            prewrite.set_start_version(*ts);
            prewrite.set_lock_ttl(1000);
            prewrite.set_use_async_commit(true);

            let resp = client1.kv_prewrite(&prewrite).unwrap();
            assert!(!resp.has_region_error());
            assert_eq!(resp.get_errors(), &[]);
            // min_commit_ts should be the last scan_lock ts + 1.
            assert_eq!(resp.min_commit_ts, ts + 11);
        });

        // Wait for the prewrite acquires the memlock
        std::thread::sleep(Duration::from_millis(200));

        let client1 = client.clone();
        let ctx1 = ctx.clone();
        let handle2 = std::thread::spawn(move || {
            if *use_green_gc {
                let mut req = RegisterLockObserverRequest::default();
                req.set_max_ts(ts + 20);
                let resp = client1.register_lock_observer(&req).unwrap();
                assert!(!resp.error.is_empty());
            } else {
                let mut req = ScanLockRequest::default();
                req.set_context(ctx1);
                req.set_max_version(ts + 20);
                let resp = client1.kv_scan_lock(&req).unwrap();
                assert!(!resp.has_region_error());
                assert!(resp.has_error());
            }
        });

        fail::remove("before-set-lock-in-memory");

        handle1.join().unwrap();
        handle2.join().unwrap();

        // Commit the key so that next turn of test will work.
        let mut req = CommitRequest::default();
        req.set_context(ctx.clone());
        req.set_start_version(*ts);
        req.set_commit_version(ts + 11);
        req.set_keys(vec![k1.to_vec()].into());
        let resp = client.kv_commit(&req).unwrap();
        assert!(!resp.has_region_error());
        assert!(!resp.has_error());
        assert_eq!(resp.commit_version, ts + 11);
    }
}

#[test]
fn test_snapshot_not_block_grpc() {
    let (cluster, leader, ctx) = must_new_cluster_mul(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env)
        .keepalive_time(Duration::from_millis(500))
        .keepalive_timeout(Duration::from_millis(500))
        .connect(&cluster.sim.read().unwrap().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k".to_vec());
    mutation.set_value(b"v".to_vec());
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mutation.clone()],
        b"k".to_vec(),
        10,
    );
    // Block getting snapshot. It shouldn't trigger keepalive watchdog timeout.
    fail::cfg("after-snapshot", "sleep(2000)").unwrap();
    must_kv_prewrite(&client, ctx, vec![mutation], b"k".to_vec(), 10);
    fail::remove("after-snapshot");
}
<<<<<<< HEAD
=======

#[test]
fn test_undetermined_write_err() {
    let (cluster, leader, ctx) = must_new_cluster_mul(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env)
        .keepalive_time(Duration::from_millis(500))
        .keepalive_timeout(Duration::from_millis(500))
        .connect(&cluster.sim.read().unwrap().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k".to_vec());
    mutation.set_value(b"v".to_vec());
    fail::cfg("applied_cb_return_undetermined_err", "return()").unwrap();
    let err = try_kv_prewrite_with_impl(
        &client,
        ctx,
        vec![mutation],
        vec![],
        b"k".to_vec(),
        10,
        0,
        false,
        false,
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "RpcFailure: 1-CANCELLED CANCELLED",);
    fail::remove("applied_cb_return_undetermined_err");
    // The previous panic hasn't been captured.
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(cluster))).is_err());
}
#[test]
fn test_stale_read_on_local_leader() {
    let mut cluster = new_server_cluster(0, 1);
    // Increase the election tick to make this test case running reliably.
    configure_for_lease_read(&mut cluster.cfg, Some(50), Some(10_000));
    let max_lease = Duration::from_secs(2);
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration(max_lease);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(epoch);
    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    let v1 = b"value1".to_vec();

    // Write record.
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v.clone());
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), 10);
    must_kv_commit(&client, ctx.clone(), vec![k.clone()], 10, 30, 30);

    // Prewrite and leave a lock.
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v1);
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), 50);

    let mut req = GetRequest::default();
    req.set_context(ctx);
    req.set_key(k);
    req.version = 40;
    req.mut_context().set_stale_read(true);

    // The stale read should fallback and succeed on the leader peer.
    let resp = client.kv_get(&req).unwrap();
    assert!(resp.error.is_none());
    assert!(resp.region_error.is_none());
    assert_eq!(v, resp.get_value());
}

#[test]
fn test_storage_do_not_update_txn_status_cache_on_write_error() {
    let cache_hit_fp = "before_prewrite_txn_status_cache_hit";
    let cache_miss_fp = "before_prewrite_txn_status_cache_miss";

    let (cluster, leader, ctx) = must_new_cluster_mul(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env)
        .connect(&cluster.sim.read().unwrap().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let pk = b"pk".to_vec();

    // Case 1: Test write successfully.

    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k1".to_vec());
    mutation.set_value(b"v1".to_vec());
    must_kv_prewrite_with(
        &client,
        ctx.clone(),
        vec![mutation.clone()],
        vec![SkipPessimisticCheck],
        pk.clone(),
        10,
        10,
        true,
        false,
    );
    must_kv_commit(&client, ctx.clone(), vec![b"k1".to_vec()], 10, 15, 15);

    // Expect cache hit
    fail::cfg(cache_miss_fp, "panic").unwrap();
    must_kv_prewrite_with(
        &client,
        ctx.clone(),
        vec![mutation],
        vec![SkipPessimisticCheck],
        pk.clone(),
        10,
        10,
        true,
        false,
    );
    // Key not locked.
    must_kv_have_locks(&client, ctx.clone(), 19, b"k1", b"k2", &[]);
    fail::remove(cache_miss_fp);

    // Case 2: Write failed.

    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k2".to_vec());
    mutation.set_value(b"v2".to_vec());

    try_kv_prewrite_with(
        &client,
        ctx.clone(),
        vec![mutation.clone()],
        vec![SkipPessimisticCheck],
        pk.clone(),
        20,
        20,
        true,
        false,
    );
    fail::cfg("raftkv_early_error_report", "return").unwrap();
    let mut commit_req = CommitRequest::default();
    commit_req.set_context(ctx.clone());
    commit_req.set_start_version(20);
    commit_req.set_commit_version(25);
    commit_req.set_keys(vec![b"k2".to_vec()].into());
    let commit_resp = client.kv_commit(&commit_req).unwrap();
    assert!(commit_resp.has_region_error());
    fail::remove("raftkv_early_error_report");
    must_kv_have_locks(
        &client,
        ctx.clone(),
        29,
        b"k2",
        b"k3",
        &[(b"k2", Op::Put, 20, 20)],
    );

    // Expect cache miss
    fail::cfg(cache_hit_fp, "panic").unwrap();
    try_kv_prewrite_with(
        &client,
        ctx.clone(),
        vec![mutation],
        vec![SkipPessimisticCheck],
        pk,
        20,
        20,
        true,
        false,
    );
    must_kv_have_locks(&client, ctx, 29, b"k2", b"k3", &[(b"k2", Op::Put, 20, 20)]);
    fail::remove(cache_hit_fp);
}
>>>>>>> 0a34c6f479 (txn: Fix to the prewrite requests retry problem by using TxnStatusCache (#15658))
