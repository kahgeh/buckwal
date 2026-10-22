# MVCC recon probes

The coordinator's review probe for `docs/mvcc-recon.md`, copied here so the finding does not cite
a temporary path.

- `p2b_review.rs`: the probe's source. It checks that a read inside `BEGIN` or `BEGIN CONCURRENT`
  sees another connection's commit while that commit's log sync is held (a speculative read), and
  that an autocommit read waits.
- `p2b_review_output.txt`: its output from the run quoted in the finding
  (`REVIEW reader inside BEGIN: SELECT id=2 before release -> Done value=1 (169µs)`).

They ran in a scratchpad crate (`mvcc-probe`, `turso_core =0.8.3` with the features `fs`, `json`,
`uuid`, `encryption`, its own `[workspace]`) outside this repository. They are not part of the
workspace and are not built here: `p2b_review.rs` is kept for reading and for copying into a
scratch crate. The behaviour is pinned in a test that does build:
`speculative_read_inside_begin_sees_unacked_commit` in `crates/buckwal/tests/mvcc_limits.rs`.
Both files are unchanged from the originals (checked for secrets and home paths: none).
