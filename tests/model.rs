// Model-based randomized check: candystore lists, queues, KV and big values vs an in-memory oracle,
// with clean reopens and simulated crashes (CandyStore::_abort_for_testing) mixed in.
use candystore::{CandyStore, Config};
use std::collections::VecDeque;
use tempfile::TempDir;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn val(rng: &mut Rng) -> Vec<u8> {
    let len = rng.below(60) as usize;
    (0..len).map(|_| b'a' + rng.below(26) as u8).collect()
}

fn run(seed: u64, ops: usize, file_size: u32) -> (usize, usize) {
    let mut restarts = 0usize;
    let dir = TempDir::new().unwrap();
    let config = Config { max_data_file_size: file_size, ..Default::default() };
    let mut db = Some(CandyStore::open(dir.path(), config).unwrap());
    let mut rng = Rng(seed);
    let lists = ["L1", "L2"];
    let queues = ["Q1", "Q2"];
    let mut lmodel: Vec<Vec<(Vec<u8>, Vec<u8>)>> = vec![vec![], vec![]];
    let mut qmodel: Vec<VecDeque<(usize, Vec<u8>)>> = vec![VecDeque::new(), VecDeque::new()];
    let mut kv: std::collections::HashMap<Vec<u8>, Vec<u8>> = Default::default();
    let mut big: std::collections::HashMap<Vec<u8>, Vec<u8>> = Default::default();
    let mut log: Vec<String> = vec![];

    for i in 0..ops {
        let d = db.as_ref().unwrap();
        let li = rng.below(2) as usize;
        let lk = lists[li];
        let m = &mut lmodel[li];
        let item = format!("k{}", rng.below(12)).into_bytes();
        let op = rng.below(100);
        let ctx = |log: &Vec<String>| format!("seed {seed} op {i}: last ops {:?}", &log[log.len().saturating_sub(12)..]);
        if op < 25 {
            let v = val(&mut rng);
            d.set_in_list(lk, &item, &v).unwrap();
            log.push(format!("set {lk} {:?}", String::from_utf8_lossy(&item)));
            if let Some(e) = m.iter_mut().find(|e| e.0 == item) { e.1 = v; } else { m.push((item, v)); }
        } else if op < 35 {
            let v = val(&mut rng);
            d.set_in_list_promoting(lk, &item, &v).unwrap();
            log.push(format!("promote {lk} {:?}", String::from_utf8_lossy(&item)));
            m.retain(|e| e.0 != item);
            m.push((item, v));
        } else if op < 45 {
            let r = d.remove_from_list(lk, &item).unwrap();
            log.push(format!("remove {lk} {:?}", String::from_utf8_lossy(&item)));
            let pos = m.iter().position(|e| e.0 == item);
            let exp = pos.map(|p| m.remove(p).1);
            assert_eq!(r, exp, "{}", ctx(&log));
        } else if op < 50 {
            let r = d.pop_list_head(lk).unwrap();
            log.push(format!("pophead {lk}"));
            let exp = if m.is_empty() { None } else { Some(m.remove(0)) };
            assert_eq!(r.map(|(k, v)| (k, v)), exp, "{}", ctx(&log));
        } else if op < 55 {
            let r = d.pop_list_tail(lk).unwrap();
            log.push(format!("poptail {lk}"));
            let exp = m.pop();
            assert_eq!(r.map(|(k, v)| (k, v)), exp, "{}", ctx(&log));
        } else if op < 60 {
            let got: Vec<_> = d.iter_list(lk).map(|r| r.unwrap()).collect();
            assert_eq!(got, *m, "iter {}", ctx(&log));
            let mut rev: Vec<_> = d.iter_list(lk).rev().map(|r| r.unwrap()).collect();
            rev.reverse();
            assert_eq!(rev, *m, "rev iter {}", ctx(&log));
            assert_eq!(d.list_len(lk).unwrap(), m.len(), "len {}", ctx(&log));
            assert_eq!(d.peek_list_head(lk).unwrap(), m.first().cloned(), "peekhead {}", ctx(&log));
            assert_eq!(d.peek_list_tail(lk).unwrap(), m.last().cloned(), "peektail {}", ctx(&log));
        } else if op < 63 {
            d.compact_list_if_needed(lk, candystore::ListCompactionParams { min_length: 1, min_holes_ratio: 0.0 }).unwrap();
            log.push(format!("compact {lk}"));
        } else if op < 64 {
            d.discard_list(lk).unwrap();
            log.push(format!("discard {lk}"));
            m.clear();
        } else if op < 66 {
            let keep = rng.below(3);
            d.retain_in_list(lk, |k, _| Ok(k.last().map(|c| (*c as u64) % 3 != keep).unwrap_or(true))).unwrap();
            log.push(format!("retain {lk} {keep}"));
            m.retain(|e| e.0.last().map(|c| (*c as u64) % 3 != keep).unwrap_or(true));
        } else {
            let qi = rng.below(2) as usize;
            let qk = queues[qi];
            let q = &mut qmodel[qi];
            let qop = rng.below(14);
            if qop < 3 {
                let v = val(&mut rng);
                let idx = d.push_to_queue_tail(qk, &v).unwrap();
                log.push(format!("qpushtail {qk}"));
                q.push_back((idx, v));
            } else if qop < 5 {
                let v = val(&mut rng);
                let idx = d.push_to_queue_head(qk, &v).unwrap();
                log.push(format!("qpushhead {qk}"));
                q.push_front((idx, v));
            } else if qop < 6 {
                assert_eq!(d.pop_queue_head_with_idx(qk).unwrap(), q.pop_front(), "qpophead {}", ctx(&log));
                log.push(format!("qpophead {qk}"));
            } else if qop < 7 {
                assert_eq!(d.pop_queue_tail_with_idx(qk).unwrap(), q.pop_back(), "qpoptail {}", ctx(&log));
                log.push(format!("qpoptail {qk}"));
            } else if qop < 9 {
                assert_eq!(d.queue_len(qk).unwrap(), q.len(), "qlen {}", ctx(&log));
                let got: Vec<(usize, Vec<u8>)> = d.iter_queue(qk).map(|r| r.unwrap()).collect();
                assert_eq!(got, q.iter().cloned().collect::<Vec<_>>(), "qiter {}", ctx(&log));
                assert_eq!(d.peek_queue_head_with_idx(qk).unwrap(), q.front().cloned(), "qpeek {}", ctx(&log));
                assert_eq!(d.peek_queue_tail_with_idx(qk).unwrap(), q.back().cloned(), "qpeekt {}", ctx(&log));
                if let (Some(f), Some(b)) = (q.front(), q.back()) {
                    let rg = d.queue_range(qk).unwrap();
                    assert!(rg.start <= f.0 && b.0 < rg.end, "qrange {:?} vs {}..{} {}", rg, f.0, b.0, ctx(&log));
                }
            } else if qop < 11 && !q.is_empty() {
                let pos = rng.below(q.len() as u64) as usize;
                let (idx, v) = q.remove(pos).unwrap();
                log.push(format!("qremove {qk} idx {idx}"));
                assert_eq!(d.remove_from_queue(qk, idx).unwrap(), Some(v), "qremove {}", ctx(&log));
                assert_eq!(d.remove_from_queue(qk, idx).unwrap(), None, "qremove twice {}", ctx(&log));
            } else if qop < 12 {
                d.discard_queue(qk).unwrap();
                log.push(format!("qdiscard {qk}"));
                q.clear();
            } else if qop < 13 {
                let k = [lists[li].as_bytes(), queues[qi].as_bytes(), b"k1", b"x"][rng.below(4) as usize].to_vec();
                if rng.below(3) == 0 { d.remove(&k).unwrap(); kv.remove(&k); log.push("kvremove".into()); }
                else { let v = val(&mut rng); d.set(&k, &v).unwrap(); kv.insert(k.clone(), v); log.push("kvset".into()); }
                for (k, v) in kv.iter() { assert_eq!(d.get(k).unwrap().as_ref(), Some(v), "kv {}", ctx(&log)); }
            } else {
                let k = format!("B{}", rng.below(3)).into_bytes();
                match rng.below(3) {
                    0 => { let n = rng.below(300_000) as usize; let v: Vec<u8> = (0..n).map(|j| (j as u64 * seed % 251) as u8).collect(); d.set_big(&k, &v).unwrap(); big.insert(k, v); log.push(format!("setbig {n}")); }
                    1 => { assert_eq!(d.remove_big(&k).unwrap(), big.remove(&k).is_some(), "rmbig {}", ctx(&log)); log.push("rmbig".into()); }
                    _ => { assert_eq!(d.get_big(&k).unwrap(), big.get(&k).cloned(), "getbig {}", ctx(&log)); }
                }
            }
        }
        let r = rng.below(200);
        if r == 0 {
            drop(db.take());
            db = Some(CandyStore::open(dir.path(), config).unwrap());
            log.push("reopen".into());
            restarts += 1;
        } else if r == 1 {
            db.take().unwrap()._abort_for_testing();
            db = Some(CandyStore::open(dir.path(), config).unwrap());
            log.push("crash".into());
            restarts += 1;
        }
    }
    let d = db.as_ref().unwrap();
    for (li, lk) in lists.iter().enumerate() {
        let got: Vec<_> = d.iter_list(lk).map(|r| r.unwrap()).collect();
        assert_eq!(got, lmodel[li], "final list seed {seed}");
    }
    for (k, v) in kv.iter() { assert_eq!(d.get(k).unwrap().as_ref(), Some(v), "final kv seed {seed}"); }
    for (k, v) in big.iter() { assert_eq!(d.get_big(k).unwrap().as_ref(), Some(v), "final big seed {seed}"); }
    (ops, restarts)
}

#[test]
fn model_fuzz() {
    let seeds: u64 = std::env::var("SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(4);
    let (mut ops, mut restarts) = (0, 0);
    for s in 1..=seeds {
        // tiny 4 KiB data files (constant rotation/compaction) and the 64 MiB default-ish size
        for (seed, file_size) in [(s * 0x9E3779B97F4A7C15 | 1, 4096), (s * 0x2545F4914F6CDD1D | 1, 64 * 1024 * 1024)] {
            let (o, r) = run(seed, 3000, file_size);
            ops += o;
            restarts += r;
        }
    }
    println!("{seeds} seeds, {ops} operations, {restarts} reopens/crashes, 0 divergences");
}
