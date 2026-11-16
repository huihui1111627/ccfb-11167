use mqueue_storage::{Partition, RetentionPolicy};
use std::io::Write;

#[test]
fn index_correct_after_simulated_crash() {
    let dir = std::env::temp_dir().join(format!("mqueue-crash-idx-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    {
        let p = Partition::open("t", 0, &dir, RetentionPolicy {
            max_segment_bytes: 4096, max_age_ms: None, ..Default::default()
        }).unwrap();
        for i in 0..500u64 {
            p.append(1, i, 1000+i, b"k", vec![0u8;20].as_ref()).unwrap();
        }
        // 不调用 sync/优雅关闭，直接 drop（模拟进程被杀死；BufWriter 由 OS 回收）
        drop(p);
    }
    // 重开触发完整恢复（密封段+活动段索引重建）
    let p2 = Partition::open("t", 0, &dir, RetentionPolicy {
        max_segment_bytes: 4096, max_age_ms: None, ..Default::default()
    }).unwrap();
    eprintln!("recovered start={} end={}", p2.log_start_offset(), p2.log_end_offset());
    for e in std::fs::read_dir(&dir).unwrap() {
        let lp = e.unwrap().path();
        if lp.extension().map(|x|x=="log").unwrap_or(false) {
            let ip = lp.with_extension("idx");
            let log = std::fs::read(&lp).unwrap();
            let idx = std::fs::read(&ip).unwrap();
            for i in (0..idx.len()).step_by(24) {
                let off=u64::from_le_bytes(idx[i..i+8].try_into().unwrap());
                let pos=u64::from_le_bytes(idx[i+8..i+16].try_into().unwrap()) as usize;
                if pos+20 > log.len() { panic!("idx pos {} beyond log {} in {}", pos, log.len(), lp.display()); }
                let magic=u16::from_le_bytes(log[pos..pos+2].try_into().unwrap());
                let frame_off=u64::from_le_bytes(log[pos+12..pos+20].try_into().unwrap());
                assert_eq!(magic,0x4d51,"bad magic in {}",lp.display());
                assert_eq!(frame_off,off,"mismatch in {}: idx {} frame {}",lp.display(),off,frame_off);
            }
            eprintln!("OK {}", lp.file_name().unwrap().to_string_lossy());
        }
    }
    // 全量有序读取
    let mut cursor=p2.log_start_offset(); let mut n=0u64;
    while cursor < p2.log_end_offset() {
        let (b,next)=p2.read_from(cursor,4096).unwrap();
        assert!(next>cursor); cursor=next; n+=b.len() as u64;
    }
    eprintln!("read back {} records", n);
    let _ = std::fs::remove_dir_all(&dir);
}
