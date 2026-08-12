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
