# candystore-model-check

A model-based randomized test for [candystore](https://github.com/sweet-security/candystore), Sweet Security's persistent in-process key-value store.

candystore's own property test (`tests/proptest_state_machine.rs`) drives `set` / `get` / `remove` against a `BTreeMap` oracle with clean shutdowns and simulated crashes. I wanted the same kind of check for the parts built on top of the hash table, so this test runs random sequences of:

- lists: `set_in_list`, `set_in_list_promoting`, `remove_from_list`, `pop_list_head` / `pop_list_tail`, `retain_in_list`, `discard_list`, forced `compact_list_if_needed`, plus forward and reverse `iter_list`, `list_len` and peeks
- queues: push / pop at both ends with indexes, `remove_from_queue` from the middle (and again, which must return `None`), `discard_queue`, `iter_queue`, `queue_range`, peeks
- plain KV writes on keys that share names with the lists and queues
- `set_big` / `get_big` / `remove_big` with values up to ~300 KB

Every operation is mirrored into an in-memory model (a `Vec` per list, a `VecDeque` of `(index, value)` per queue, two `HashMap`s), and reads are compared against it. About 1 in 200 steps closes and reopens the store, and about 1 in 200 calls `_abort_for_testing()` to simulate a crash before reopening. Each seed runs once with 4 KiB data files (so files rotate and compaction runs constantly) and once with 64 MiB files.

## Result

Against candystore 1.2.2 from crates.io, on an Apple Silicon Mac:

```
$ SEEDS=10 cargo test --release -- --nocapture
10 seeds, 60000 operations, 581 reopens/crashes, 0 divergences
test result: ok. 1 passed; 0 failed
```

No divergences. I also ran it against `main` (2006ab6) with 30 seeds of the list/queue-only version before adding KV and big values, again with no failures.

## Running it

```
cargo test --release -- --nocapture          # 4 seeds, about 2 minutes
SEEDS=50 cargo test --release -- --nocapture  # longer run
```

On a failure the assertion message prints the seed, the step number and the last dozen operations, so a run can be replayed by hard-coding that seed.

## Limits

- "Crash" here is a process-level abort via candystore's test hook, not a power loss, so it says nothing about fsync ordering on real disks.
- Everything is single-threaded. candystore's concurrency tests are separate and this does not cover them.
- The model assumes `set_in_list` updates an existing item in place and `set_in_list_promoting` moves it to the tail, which is what the docs describe.
