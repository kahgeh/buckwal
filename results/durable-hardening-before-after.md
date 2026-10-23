# Durable hardening: before and after

Sections in fix order: item 9 (S3Store::new), the head fields (Task 2), item 2 (takeover quiet), item 1 (skipped generation, floor, GC floor check), the HKDF vectors (Task 4), then Task 5, 5b, 6 and 5c. Each section is copied from the implementer report in `.superpowers/sdd/durable-hardening/`; where a report has no RED or GREEN block for a fix, the section says so.

## Item 9: S3Store::new refuses a CA bundle with an http endpoint (Task 1)

RED: `cargo test --locked -p buckwal s3_new_`

```
s3_new_accepts_ca_bundle_with_https_endpoint ... ok
s3_new_refuses_ca_bundle_with_http_endpoint ... FAILED
  assertion failed: "HTTP://x"
  left: "CA bundle /ca.pem: No such file or directory (os error 2)"
  right: "BUCKWAL_CA_BUNDLE is set but the endpoint is http://"
```

GREEN: same command: 2 passed; 0 failed. config tests: 29 passed (incl. `ca_bundle_with_http_endpoint_is_refused`). `./scripts/test.sh unit`: rc=0.

The accepts test passes before the fix too (it guards against over-refusal).

## Head fields lease_ms, request_timeout_ms, next_generation_floor (Task 2)

RED: with head.rs and mod.rs at HEAD and the new tests in place, `cargo test --locked -p buckwal --lib format::` fails to compile: `cannot find value FORMAT_V3`, `Head has no field lease_ms / request_timeout_ms / next_generation_floor` (27 each), `LocalState has no field next_generation_floor` (9+1). The report notes the literal fixes were applied before this run, so the RED is the missing fields and constant; the implementation had been written first and was reverted temporarily to capture it. This is a compile failure, not a failing assertion.

GREEN: `./scripts/test.sh unit` passes (fmt, clippy -D warnings, 502 buckwal lib tests plus the other crates' unit tests).

Review fix: `previous_build_reads_heads_carrying_the_new_fields` (format/tests.rs) decodes a format 1 head and a format 2 mvcc head carrying the three fields through `decode_accepting(&[FORMAT])` and `(&[FORMAT, FORMAT_V2])`, with the other fields intact; the json Obj parser ignores unknown keys. The report shows no RED block for this test (it covers behaviour the code already had). `cargo test --locked -p buckwal --lib format::tests::previous_build`: 1 passed.

## Item 2: the takeover quiet uses both writers' timeouts (Task 3, commit a)

RED (`cargo test --locked -p buckwal --lib -- quiet_is_larger quiet_of_a_head quiet_over_max every_head_carries`):

```
db::tests::every_head_carries_the_writers_timeouts: assertion failed: create
  left: (None, None)  right: (Some(300), Some(500))
db::tests::quiet_is_larger_of_both_writers: left: (None, None) right: (Some(400), Some(600))
test result: FAILED. 2 passed; 2 failed
```

`quiet_of_a_head_without_the_fields_is_the_own_sum` and `quiet_over_max_is_corrupt` pass on the old code too: the head decoder already refused a sum over a day (Task 1), and absent fields already meant own values. They pin that behaviour at takeover level.

GREEN: the same four tests: 4 passed. `./scripts/test.sh unit`: all ok (507 lib tests plus the CLI, server and integration binaries; ignored RustFS tests only compiled).

## Item 1: a takeover skips a generation; the floor; the GC floor check (Task 3, spec revision 6)

RED (code at bc66c12, new tests only; `cargo test --locked -p buckwal --lib -- <names>`):

```
mvcc_takeover_seal_lands_on_g_plus_2: left: Ok(3) right: Ok(4)
mvcc_seal_with_lost_head_reply_adopts_g_plus_2: the head PUT stored: left (3, Some(2)) right (4, Some(3))
restart_between_takeover_and_first_seal_keeps_floor: after the takeover: left 0 right 3
skipped_generation_restore_verify_gc_maintenance_reconcile_second_takeover: the takeover's head: left None right Some(3)
two_takeovers_before_first_seal_skip_both: left None right Some(3)
previous_build_restores_from_new_head: left (2, Some(1)) right (3, Some(2))
gc_pause_between_takeover_and_skip_seal_ends: left (2, Some(1), None) right (2, Some(1), Some(4))
gc_ignores_fenced_writers_leftover_manifest: Head { generation: 3, manifest: Some(2), .. } left (3, Some(2)) right (4, Some(3))
late_put_cannot_replace_new_writers_page: the head skips 2: left Ok(2) right Ok(3)
test result: FAILED. 0 passed; 8 failed (+ the late_put test run alone: 1 failed)
```

The late_put test originally hung on old code, because a failed assertion inside the thread scope left A's PUT held; its assertions now run after A is released.

RED with the GC floor check alone removed (everything else in place), `gc_ignores_fenced_writers_leftover_manifest` and `gc_pause_between_takeover_and_skip_seal_ends`:

```
GcPass { generation: 4, trimmed_against: Some(2), age: Some(642.1s), deleted: 98, outcome: Trimmed }
  left: Trimmed right: TooYoung   (GC trimmed against A's leftover manifest/2: 98 objects, incl. pages B maps)
GcPass { generation: 2, trimmed_against: Some(1), deleted: 63, outcome: Trimmed }
  left: (Trimmed, 63) right: (Nothing, 0)   (GC trimmed between the takeover and the skip seal)
```

GREEN: the nine new tests: 9 passed, with the floor check back. `./scripts/test.sh unit`: all ok (522 lib tests, then the CLI, server and integration binaries; RustFS tests only compiled).

Tests covered: bucket/tests.rs `late_put_cannot_replace_new_writers_page`; db/tests.rs `skipped_generation_restore_verify_gc_maintenance_reconcile_second_takeover`, `two_takeovers_before_first_seal_skip_both`, `restart_between_takeover_and_first_seal_keeps_floor`, `previous_build_restores_from_new_head`; gc/tests.rs `gc_ignores_fenced_writers_leftover_manifest`, `gc_pause_between_takeover_and_skip_seal_ends`; io/log_file.rs MVCC tests `mvcc_takeover_seal_lands_on_g_plus_2` and `mvcc_seal_with_lost_head_reply_adopts_g_plus_2` (kind 3 records in `log/<G+2>`, none in `log/<G+1>`); system.rs (`#[ignore]`, compiled only) `late_put_from_fenced_writer_lands_on_unreferenced_key`. The report records no separate RED output for `maintenance_skips_during_takeover_quiet`, which was changed to assert the maintenance seal lands on G+2.

The report notes `db::tests::close_stops_prefetch_before_releasing_the_dir` failed once on the final run (timing based) and also fails alone at bc66c12, so it is unrelated.

## HKDF-SHA256 and the integrity key (Task 4)

RED: tests written first (RFC 5869 cases 1-3, key differs from page key, newline-insensitive, Debug redacted, verify), `cargo test --locked -p buckwal --lib encryption`: compile errors (`hkdf_sha256`, `integrity_key` not found). The report gives no assertion output; the RED is a compile failure.

GREEN: same command: 17 passed, 0 failed. `./scripts/test.sh unit`: fmt, clippy -D warnings, all unit tests pass.


## Task 5: integrity codes on pages, records, manifests and the head (format 3)

Each tamper test was run before the fix (the codes not yet wired in: `Keys` carried no integrity key and every object was read and written as a plaintext bucket's). Command:

    cargo test --locked -p buckwal --lib db::tests::integrity

### Before (8 of 9 fail; the untouched bucket restores)

```
running 9 tests
test db::tests::integrity::an_encrypted_bucket_has_a_format_3_head_and_a_keyless_open_is_refused ... FAILED
test db::tests::integrity::a_page_object_shorter_than_a_code_fails_without_a_panic ... FAILED
test db::tests::integrity::an_encrypted_open_refuses_a_head_written_before_integrity_codes ... FAILED
test db::tests::integrity::a_record_replayed_at_another_seq_fails_the_read ... FAILED
test db::tests::integrity::an_edited_manifest_fails_the_read ... FAILED
test db::tests::integrity::an_older_generations_page_put_back_fails_the_read ... FAILED
test db::tests::integrity::swapped_pages_fail_the_read ... FAILED
test db::tests::integrity::an_edited_head_fails_the_read ... FAILED
test db::tests::integrity::untouched_encrypted_bucket_restores_and_verifies ... ok
failures:
---- db::tests::integrity::an_encrypted_bucket_has_a_format_3_head_and_a_keyless_open_is_refused stdout ----
thread 'db::tests::integrity::an_encrypted_bucket_has_a_format_3_head_and_a_keyless_open_is_refused'  panicked at crates/buckwal/src/db/tests/integrity.rs:342:9:
assertion `left == right` failed: Wal
  left: 1
 right: 3
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
---- db::tests::integrity::a_page_object_shorter_than_a_code_fails_without_a_panic stdout ----
thread 'db::tests::integrity::a_page_object_shorter_than_a_code_fails_without_a_panic'  panicked at crates/buckwal/src/db/tests/integrity.rs:104:13:
Wal page of 0 bytes: "page 1 is shorter than its header" is not an integrity error for ["t/pages/0000000002/0000000001"]
---- db::tests::integrity::an_encrypted_open_refuses_a_head_written_before_integrity_codes stdout ----
thread 'db::tests::integrity::an_encrypted_open_refuses_a_head_written_before_integrity_codes'  panicked at crates/buckwal/src/db/tests/integrity.rs:327:22:
Wal format 1: Ok(0)
---- db::tests::integrity::a_record_replayed_at_another_seq_fails_the_read stdout ----
thread 'db::tests::integrity::a_record_replayed_at_another_seq_fails_the_read'  panicked at crates/buckwal/src/db/tests/integrity.rs:106:14:
Wal record 2 replayed at 7: expected Corrupt(integrity: ..), got Ok(44)
---- db::tests::integrity::an_edited_manifest_fails_the_read stdout ----
thread 'db::tests::integrity::an_edited_manifest_fails_the_read'  panicked at crates/buckwal/src/db/tests/integrity.rs:106:14:
Wal manifest: expected Corrupt(integrity: ..), got Ok(27)
---- db::tests::integrity::an_older_generations_page_put_back_fails_the_read stdout ----
thread 'db::tests::integrity::an_older_generations_page_put_back_fails_the_read'  panicked at crates/buckwal/src/db/tests/integrity.rs:106:14:
Wal old page put back over t/pages/0000000002/0000000001: expected Corrupt(integrity: ..), got Ok(45)
---- db::tests::integrity::swapped_pages_fail_the_read stdout ----
thread 'db::tests::integrity::swapped_pages_fail_the_read'  panicked at crates/buckwal/src/db/tests/integrity.rs:106:14:
Wal swap: expected Corrupt(integrity: ..), got Ok(40)
---- db::tests::integrity::an_edited_head_fails_the_read stdout ----
thread 'db::tests::integrity::an_edited_head_fails_the_read'  panicked at crates/buckwal/src/db/tests/integrity.rs:106:14:
Wal head: expected Corrupt(integrity: ..), got Ok(45)
failures:
    db::tests::integrity::a_page_object_shorter_than_a_code_fails_without_a_panic
    db::tests::integrity::a_record_replayed_at_another_seq_fails_the_read
    db::tests::integrity::an_edited_head_fails_the_read
    db::tests::integrity::an_edited_manifest_fails_the_read
    db::tests::integrity::an_encrypted_bucket_has_a_format_3_head_and_a_keyless_open_is_refused
    db::tests::integrity::an_encrypted_open_refuses_a_head_written_before_integrity_codes
    db::tests::integrity::an_older_generations_page_put_back_fails_the_read
    db::tests::integrity::swapped_pages_fail_the_read
test result: FAILED. 1 passed; 8 failed; 0 ignored; 0 measured; 522 filtered out; finished in 1.56s
error: test failed, to rerun pass `-p buckwal --lib`
```

The swap of two table leaves loaded silently (`Ok(40)` rows, 45 written), as did the put-back page, the replayed record (`Ok(44)`), the re-encoded manifest and the edited head.

### After (all pass)

```
test encryption::tests::integrity_key_debug_is_redacted ... ok
test format::tests::integrity_codes::code_kinds_are_domain_separated ... ok
test encryption::tests::integrity_key_differs_from_page_key_and_ignores_newline ... ok
test format::tests::integrity_codes::new_bucket_format_follows_encryption_and_mode ... ok
test encryption::tests::integrity_code_covers_parts_and_verifies ... ok
test format::tests::integrity_codes::coded_head_refused_by_older_decoders ... ok
test format::tests::integrity_codes::page_code_binds_page_number_and_generation ... ok
test format::tests::integrity_codes::encrypted_open_refuses_heads_written_before_integrity_codes ... ok
test format::tests::integrity_codes::plaintext_open_refuses_a_format_3_head_and_reads_the_others ... ok
test format::tests::integrity_codes::previous_build_follows_a_skipped_generation ... ok
test format::tests::integrity_codes::record_code_replaces_the_checksum_and_binds_generation_and_seq ... ok
test format::tests::integrity_codes::manifest_code_binds_the_generation ... ok
test format::tests::integrity_codes::plaintext_objects_are_byte_identical_to_main ... ok
test format::tests::integrity_codes::coded_head_round_trips_and_every_field_is_covered ... ok
test format::tests::integrity_codes::a_failed_check_logs_one_error_record_without_data ... ok
test db::tests::integrity::an_encrypted_bucket_has_a_format_3_head_and_a_keyless_open_is_refused ... ok
test db::tests::integrity::an_older_generations_page_put_back_fails_the_read ... ok
test db::tests::integrity::an_edited_manifest_fails_the_read ... ok
test db::tests::integrity::a_record_replayed_at_another_seq_fails_the_read ... ok
test db::tests::integrity::an_encrypted_open_refuses_a_head_written_before_integrity_codes ... ok
test db::tests::integrity::a_page_object_shorter_than_a_code_fails_without_a_panic ... ok
test db::tests::integrity::an_edited_head_fails_the_read ... ok
test db::tests::integrity::swapped_pages_fail_the_read ... ok
test db::tests::integrity::untouched_encrypted_bucket_restores_and_verifies ... ok
test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 519 filtered out; finished in 2.52s
```

`./scripts/test.sh unit`: exit 0; buckwal lib 543 passed (including `db::tests::close_stops_prefetch_before_releasing_the_dir`), the rest of the workspace passed.

## key_check (Task 5b, spec rev 7)

### Before (HEAD c60bd16, tests added first; 5 of 16 fail)

```
---- db::tests::integrity::a_key_on_a_plaintext_bucket_has_no_integrity_codes stdout ----
thread 'db::tests::integrity::a_key_on_a_plaintext_bucket_has_no_integrity_codes' (171085886) panicked at crates/buckwal/src/db/tests/integrity.rs:534:14:
takeover false: expected Encryption("the bucket has no integrity codes: it is plaintext, or was encrypted before this version"), got Err(Corrupt("integrity: bucket written before integrity codes"))
--
---- db::tests::integrity::after_a_good_open_an_edited_head_or_key_check_is_tampering stdout ----
thread 'db::tests::integrity::after_a_good_open_an_edited_head_or_key_check_is_tampering' (171085889) panicked at crates/buckwal/src/db/tests/integrity.rs:542:5:
assertion `left != right` failed: the edit changed nothing: {"format":3,"generation":3,"manifest":"manifest/0000000002","writer_id":"3e3e02acd22de3d87d4221be62ae668c","epoch":1,"journal_mode":"wal","lease_ms":30000,"request_timeout_ms":10000,"integrity":"df4aa353029a321d69b787311969b8738ae7c9c302604c37d430ba58e6635aa2"}
--
---- db::tests::integrity::an_edited_key_check_is_the_wrong_key_error_and_nothing_is_restored stdout ----
thread 'db::tests::integrity::an_edited_key_check_is_the_wrong_key_error_and_nothing_is_restored' (171085891) panicked at crates/buckwal/src/db/tests/integrity.rs:542:5:
assertion `left != right` failed: the edit changed nothing: {"format":3,"generation":3,"manifest":"manifest/0000000002","writer_id":"9acc285e7bbbf12ea28d7489fbe8c3ba","epoch":1,"journal_mode":"wal","lease_ms":30000,"request_timeout_ms":10000,"integrity":"604f887c9ac797af77bce99cd88c576da1e873b413a3e724a537e28315ec7825"}
--
---- db::tests::integrity::an_encrypted_open_refuses_a_head_written_before_integrity_codes stdout ----
thread 'db::tests::integrity::an_encrypted_open_refuses_a_head_written_before_integrity_codes' (171085894) panicked at crates/buckwal/src/db/tests/integrity.rs:534:14:
Wal format 1: expected Encryption("the bucket has no integrity codes: it is plaintext, or was encrypted before this version"), got Err(Corrupt("integrity: bucket written before integrity codes"))
---- db::tests::integrity::a_different_valid_key_is_the_wrong_key_and_not_tampering stdout ----
thread 'db::tests::integrity::a_different_valid_key_is_the_wrong_key_and_not_tampering' (171085884) panicked at crates/buckwal/src/db/tests/integrity.rs:534:14:
Wal ReadOnly: expected Encryption("the key in BUCKWAL_ENCRYPTION_KEY_FILE is not the key this bucket was written with"), got Err(Corrupt("integrity: t/head"))
--
test result: FAILED. 11 passed; 5 failed; 0 ignored; 0 measured; 534 filtered out; finished in 3.46s
error: test failed, to rerun pass `-p buckwal --lib`
```

A different valid key gave Corrupt("integrity: t/head") (and the paging ERROR record); a key on a format 1 head gave the old Corrupt text; the head had no key_check to edit.

### After (16 of 16 pass)

```
test db::tests::integrity::a_key_on_a_plaintext_bucket_has_no_integrity_codes ... ok
test db::tests::integrity::an_older_generations_page_put_back_fails_the_read ... ok
test db::tests::integrity::after_a_good_open_an_edited_head_or_key_check_is_tampering ... ok
test db::tests::integrity::an_encrypted_bucket_has_a_format_3_head_and_a_keyless_open_is_refused ... ok
test db::tests::integrity::gc_over_a_tampered_manifest_fails ... ok
test db::tests::integrity::an_edited_head_fails_the_read ... ok
test db::tests::integrity::a_format_3_head_without_its_integrity_field_fails_the_read ... ok
test db::tests::integrity::an_edited_manifest_fails_the_read ... ok
test db::tests::integrity::an_encrypted_open_refuses_a_head_written_before_integrity_codes ... ok
test db::tests::integrity::a_page_object_shorter_than_a_code_fails_without_a_panic ... ok
test db::tests::integrity::a_record_replayed_at_another_seq_fails_the_read ... ok
test db::tests::integrity::a_different_valid_key_is_the_wrong_key_and_not_tampering ... ok
test db::tests::integrity::swapped_pages_fail_the_read ... ok
test db::tests::integrity::untouched_encrypted_bucket_restores_and_verifies ... ok
test db::tests::integrity::verify_fails_over_tampered_objects ... ok
test db::tests::integrity::an_edited_key_check_is_the_wrong_key_error_and_nothing_is_restored ... ok
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 535 filtered out; finished in 4.66s
```

Also: format::tests::integrity_codes::key_check_is_compared_at_open_before_the_integrity_code (open vs later read, no record on wrong key, key_check never in an error). Reconciled: open_with_wrong_key_is_refused, takeover_with_a_mismatched_key_is_refused_before_the_claim, takeover_with_a_key_on_a_plaintext_database_is_refused, encrypted_open_refuses_heads_written_before_integrity_codes (format and db tests).

`./scripts/test.sh unit`: exit 0.

## Task 6: telemetry (before the code, then after)

Before (new tests, instrumentation not written; `cargo test --locked -p buckwal --lib -- db::tests::telemetry db::tests::integrity::telemetry_signals`): 7 failed, 1 passed (the wrong-key test: nothing to fail yet).

```
a_takeover_after_a_commit_reports_a_zero_quiet                      buckwal.quiet_ms: left None, right Some("0")
the_larger_writer_sets_the_quiet_and_a_head_without_timeouts_...    buckwal.quiet_own_ms: left None, right Some("800")
takeover_span_carries_the_quiet_and_the_next_generation_floor       buckwal.quiet_ms: left None, right Some("800")
only_the_first_seal_after_a_takeover_has_a_skipped_generation       buckwal.skipped_generation: left None, right Some("3")
the_mvcc_seal_span_names_the_skipped_generation_too                 buckwal.skipped_generation: left None, right Some("4")
telemetry_signals::a_tampered_page_yields_one_record_...            caller span buckwal.outcome: left None, right Some("integrity")
telemetry_signals::restore_..._spans_end_with_outcome_integrity     buckwal.restore buckwal.outcome: left None, right Some("integrity")
telemetry_signals::a_wrong_key_yields_no_record_...                 ok (no record, no integrity outcome, before and after)
```

SealStarted/SealFinished after a skip (reverting the fix only, `seal_events_report_the_generation_sealed_after_a_skip_too`): `SealStarted { generation: 2 }`, `SealFinished { generation: 3 }` (left [3], right [2]). After: both report 2, the generation sealed.

After: the same 8 tests, plus the seal-events test: 9 passed. `./scripts/test.sh unit`: see the Task 6 report.

## Task 5c: a downgraded head after a good open is tampering

Before (new test `db::tests::integrity::after_a_good_open_a_downgraded_or_stripped_head_is_tampering`, fix not written; `cargo test --locked -p buckwal --lib -- after_a_good_open`): 1 failed, 1 passed (the Task 5b edited key_check/epoch test).

```
Wal format 1: expected Corrupt, got Err(Encryption("the bucket has no integrity codes: it is plaintext, or was encrypted before this version"))
```

After (`Head::decode_checked`: a format 1 or 2 head is `Encryption` no-codes only at open; later it is `Corrupt("integrity: <prefix>/head format downgraded")` through `integrity_failed_as`, kind head): 2 passed. The test covers both modes: format 1 head, format 2 head, edited key_check, stripped key_check, each after a good open (GC pass): the exact text, exactly one record each, restore refuses, and the good head put back is accepted. Open-time tests (wrong key, key on plaintext, no codes: Encryption, zero records) unchanged and green.

System version (pass 2, `#[ignore]`, compiled only): `encrypted_downgraded_head_after_open_is_tampering`.
