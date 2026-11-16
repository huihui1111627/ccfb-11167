use mqueue_storage::{Partition, RetentionPolicy};

#[test]
fn append_after_recovery_does_not_overwrite() {
    let dir = std::env::temp_dir().join(format!("mqueue-aar-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let policy = RetentionPolicy { max_segment_bytes: 4096, max_age_ms: None, ..Default::default() };
    // 第一次生命周期：写 300 条后 drop（模拟崩溃，不优雅关闭）
    {
        let p = Partition::open("t", 0, &dir, policy.clone()).unwrap();
        for i in 0..300u64 {
            p.append(1, i, 1000+i, b"k", vec![0u8;20].as_ref()).unwrap();
        }
        drop(p);
    }
    // 恢复后继续追加 300 条
    let p2 = Partition::open("t", 0, &dir, policy.clone()).unwrap();
    let end_before = p2.log_end_offset();
    assert_eq!(end_before, 300);
    for i in 300..600u64 {
        p2.append(1, i, 2000+i, b"k", vec![0u8;20].as_ref()).unwrap();
    }
    assert_eq!(p2.log_end_offset(), 600);
    // 全量读回：offset 0..600 必须连续且 sequence 与 offset 一一对应
    let mut cursor = 0u64; let mut seen = Vec::new();
    while cursor < 600 {
        let (b, next) = p2.read_from(cursor, 4096).unwrap();
        assert!(next > cursor, "no progress at {cursor}");
        cursor = next;
        seen.extend(b);
    }
    assert_eq!(seen.len(), 600, "lost records after recovery+append");
    for (i, r) in seen.iter().enumerate() {
        assert_eq!(r.offset, i as u64, "offset gap at {i}");
        assert_eq!(r.sequence, i as u64, "sequence mismatch at {i}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
