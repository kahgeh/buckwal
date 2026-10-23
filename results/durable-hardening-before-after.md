
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
