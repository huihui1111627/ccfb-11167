use mqueue_storage::{Partition, RetentionPolicy};

#[test]
fn sealed_segment_index_matches_log_without_crash() {
    let dir = std::env::temp_dir().join(format!("mqueue-roll-idx-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let p = Partition::open("t", 0, &dir, RetentionPolicy {
        max_segment_bytes: 4096, max_age_ms: None, ..Default::default()
    }).unwrap();
    for i in 0..500u64 {
        p.append(1, i, 1000+i, b"k", vec![0u8;20].as_ref()).unwrap();
    }
    // 直接检查磁盘上每个段的 idx 与 log
    for e in std::fs::read_dir(&dir).unwrap() {
        let lp = e.unwrap().path();
        if lp.extension().map(|x|x=="log").unwrap_or(false) {
            let ip = lp.with_extension("idx");
            let log = std::fs::read(&lp).unwrap();
            let idx = std::fs::read(&ip).unwrap();
            for i in (0..idx.len()).step_by(24) {
                let (off,pos,_ts) = (
                    u64::from_le_bytes(idx[i..i+8].try_into().unwrap()),
                    u64::from_le_bytes(idx[i+8..i+16].try_into().unwrap()),
                    u64::from_le_bytes(idx[i+16..i+24].try_into().unwrap()));
                let pos = pos as usize;
                let magic = u16::from_le_bytes(log[pos..pos+2].try_into().unwrap());
                let frame_off = u64::from_le_bytes(log[pos+12..pos+20].try_into().unwrap());
                assert_eq!(magic, 0x4d51, "bad magic in {} at {}", lp.display(), pos);
                assert_eq!(frame_off, off, "idx offset {} != frame offset {} in {}", off, frame_off, lp.display());
            }
            eprintln!("OK: {} ({} bytes)", lp.file_name().unwrap().to_string_lossy(), log.len());
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
