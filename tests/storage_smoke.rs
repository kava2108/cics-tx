use cics_tx::storage::Environment;

#[test]
fn put_get_commit_and_reopen_persists() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");

    {
        let env = Environment::open(&path).unwrap();
        let mut wtx = env.begin_write();
        wtx.put(b"alice", b"100");
        wtx.put(b"bob", b"50");
        wtx.commit().unwrap();
    }

    // Reopen: data must have survived (durability).
    let env = Environment::open(&path).unwrap();
    let rtx = env.begin_read();
    assert_eq!(rtx.get(b"alice"), Some(b"100".to_vec()));
    assert_eq!(rtx.get(b"bob"), Some(b"50".to_vec()));
    assert_eq!(rtx.get(b"carol"), None);
}

#[test]
fn rollback_discards_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let env = Environment::open(&path).unwrap();

    {
        let mut wtx = env.begin_write();
        wtx.put(b"x", b"1");
        wtx.commit().unwrap();
    }
    {
        let mut wtx = env.begin_write();
        wtx.put(b"x", b"2");
        wtx.put(b"y", b"new");
        wtx.rollback();
    }
    let rtx = env.begin_read();
    assert_eq!(rtx.get(b"x"), Some(b"1".to_vec()));
    assert_eq!(rtx.get(b"y"), None);
}

#[test]
fn reader_snapshot_is_isolated_from_concurrent_writer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let env = Environment::open(&path).unwrap();
    {
        let mut wtx = env.begin_write();
        wtx.put(b"k", b"v1");
        wtx.commit().unwrap();
    }

    let rtx = env.begin_read(); // snapshot before the next write
    {
        let mut wtx = env.begin_write();
        wtx.put(b"k", b"v2");
        wtx.commit().unwrap();
    }
    assert_eq!(rtx.get(b"k"), Some(b"v1".to_vec())); // old snapshot unaffected
    assert_eq!(env.begin_read().get(b"k"), Some(b"v2".to_vec()));
}

#[test]
fn many_keys_trigger_splits_and_stay_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let env = Environment::open(&path).unwrap();

    {
        let mut wtx = env.begin_write();
        for i in 0..5000u32 {
            let k = format!("key-{i:06}");
            let v = format!("value-{i}");
            wtx.put(k.as_bytes(), v.as_bytes());
        }
        wtx.commit().unwrap();
    }

    let rtx = env.begin_read();
    for i in 0..5000u32 {
        let k = format!("key-{i:06}");
        let v = format!("value-{i}");
        assert_eq!(rtx.get(k.as_bytes()), Some(v.into_bytes()), "key {k}");
    }
    let all = rtx.range(None, None);
    assert_eq!(all.len(), 5000);
    for w in all.windows(2) {
        assert!(w[0].0 < w[1].0, "range() must be sorted");
    }
}

#[test]
fn delete_removes_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let env = Environment::open(&path).unwrap();
    {
        let mut wtx = env.begin_write();
        wtx.put(b"a", b"1");
        wtx.put(b"b", b"2");
        assert!(wtx.delete(b"a"));
        assert!(!wtx.delete(b"nonexistent"));
        wtx.commit().unwrap();
    }
    let rtx = env.begin_read();
    assert_eq!(rtx.get(b"a"), None);
    assert_eq!(rtx.get(b"b"), Some(b"2".to_vec()));
}

#[test]
fn deleted_pages_are_reused_so_the_file_does_not_grow_unboundedly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let env = Environment::open(&path).unwrap();

    // Several full insert/delete rounds with no readers held open, so
    // every obsoleted page becomes reclaimable immediately.
    for round in 0..5 {
        let mut wtx = env.begin_write();
        for i in 0..2000u32 {
            wtx.put(format!("k{i}").as_bytes(), format!("round{round}-value-{i}").as_bytes());
        }
        wtx.commit().unwrap();

        let mut wtx = env.begin_write();
        for i in 0..2000u32 {
            wtx.delete(format!("k{i}").as_bytes());
        }
        wtx.commit().unwrap();
    }
    let size_after_five_rounds = std::fs::metadata(&path).unwrap().len();

    // One more steady-state round shouldn't meaningfully grow the file if
    // pages are actually being recycled instead of accumulating forever.
    let mut wtx = env.begin_write();
    for i in 0..2000u32 {
        wtx.put(format!("k{i}").as_bytes(), format!("more-{i}").as_bytes());
    }
    wtx.commit().unwrap();
    let mut wtx = env.begin_write();
    for i in 0..2000u32 {
        wtx.delete(format!("k{i}").as_bytes());
    }
    wtx.commit().unwrap();
    let size_after_six_rounds = std::fs::metadata(&path).unwrap().len();

    let growth = size_after_six_rounds.saturating_sub(size_after_five_rounds);
    // Without reuse, 2000 fresh inserts alone would add several hundred
    // KB of brand-new pages every round; with reuse+merge, a steady-state
    // round should cost only a small, bounded amount (a page or two of
    // free-list bookkeeping), not anything proportional to 2000 pages.
    assert!(growth < 50_000, "expected page reuse to bound file growth, but it grew by {growth} bytes");
}

#[test]
fn active_reader_snapshot_survives_writer_deleting_and_reusing_pages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let env = Environment::open(&path).unwrap();

    {
        let mut wtx = env.begin_write();
        for i in 0..500u32 {
            wtx.put(format!("k{i}").as_bytes(), format!("orig-{i}").as_bytes());
        }
        wtx.commit().unwrap();
    }

    let old_reader = env.begin_read(); // pins the original tree's pages

    {
        let mut wtx = env.begin_write();
        for i in 0..500u32 {
            wtx.delete(format!("k{i}").as_bytes());
        }
        wtx.commit().unwrap();
    }
    {
        // These writes must not be allowed to reuse the pages just freed
        // above -- old_reader is still alive and its snapshot depends on
        // their original content.
        let mut wtx = env.begin_write();
        for i in 0..500u32 {
            wtx.put(format!("new{i}").as_bytes(), format!("fresh-{i}").as_bytes());
        }
        wtx.commit().unwrap();
    }

    for i in 0..500u32 {
        assert_eq!(
            old_reader.get(format!("k{i}").as_bytes()),
            Some(format!("orig-{i}").into_bytes()),
            "old snapshot must be untouched by page reuse while it's still alive"
        );
    }
    drop(old_reader);

    let fresh = env.begin_read();
    for i in 0..500u32 {
        assert_eq!(fresh.get(format!("k{i}").as_bytes()), None);
        assert_eq!(fresh.get(format!("new{i}").as_bytes()), Some(format!("fresh-{i}").into_bytes()));
    }
}

#[test]
fn merge_and_reclaim_do_not_corrupt_a_large_dataset_under_mixed_churn() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let env = Environment::open(&path).unwrap();

    {
        let mut wtx = env.begin_write();
        for i in 0..3000u32 {
            wtx.put(format!("key-{i:06}").as_bytes(), format!("v{i}").as_bytes());
        }
        wtx.commit().unwrap();
    }
    // Delete every third key, then reinsert half of those with new values,
    // exercising merge (deletes) and reuse (the reinserts) together.
    {
        let mut wtx = env.begin_write();
        for i in (0..3000u32).step_by(3) {
            assert!(wtx.delete(format!("key-{i:06}").as_bytes()));
        }
        wtx.commit().unwrap();
    }
    {
        let mut wtx = env.begin_write();
        for i in (0..3000u32).step_by(6) {
            wtx.put(format!("key-{i:06}").as_bytes(), format!("v2-{i}").as_bytes());
        }
        wtx.commit().unwrap();
    }

    let rtx = env.begin_read();
    for i in 0..3000u32 {
        let key = format!("key-{i:06}");
        let expected = if i % 6 == 0 {
            Some(format!("v2-{i}").into_bytes())
        } else if i % 3 == 0 {
            None
        } else {
            Some(format!("v{i}").into_bytes())
        };
        assert_eq!(rtx.get(key.as_bytes()), expected, "key {key}");
    }
    let all = rtx.range(None, None);
    for w in all.windows(2) {
        assert!(w[0].0 < w[1].0, "range() must stay sorted after merges/reuse");
    }
}
