
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
