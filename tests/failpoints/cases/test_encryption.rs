// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use encryption::FileDictionaryFile;
use kvproto::encryptionpb::{EncryptionMethod, FileInfo};

#[test]
fn test_file_dict_file_record_corrupted() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut file_dict_file = FileDictionaryFile::new(
        tempdir.path(),
        "test_file_dict_file_record_corrupted_1",
        true,
        10, // file_rewrite_threshold
    )
    .unwrap();
    let info1 = create_file_info(1, EncryptionMethod::Aes256Ctr);
    let info2 = create_file_info(2, EncryptionMethod::Unknown);
    // 9 represents that the first 9 bytes will be discarded.
    // Crc32 (4 bytes) + File name length (2 bytes) + FileInfo length (2 bytes) +
    // Log type (1 bytes)
    fail::cfg("file_dict_log_append_incomplete", "return(9)").unwrap();
    file_dict_file.insert("info1", &info1).unwrap();
    fail::remove("file_dict_log_append_incomplete");
    file_dict_file.insert("info2", &info2).unwrap();
    // Intermediate record damage is not allowed.
    file_dict_file.recovery().unwrap_err();

    let mut file_dict_file = FileDictionaryFile::new(
        tempdir.path(),
        "test_file_dict_file_record_corrupted_2",
        true,
        10, // file_rewrite_threshold
    )
    .unwrap();
    let info1 = create_file_info(1, EncryptionMethod::Aes256Ctr);
    let info2 = create_file_info(2, EncryptionMethod::Unknown);
    file_dict_file.insert("info1", &info1).unwrap();
    fail::cfg("file_dict_log_append_incomplete", "return(9)").unwrap();
    file_dict_file.insert("info2", &info2).unwrap();
    fail::remove("file_dict_log_append_incomplete");
    // The ending record can be discarded.
    let file_dict = file_dict_file.recovery().unwrap();
    assert_eq!(*file_dict.files.get("info1").unwrap(), info1);
    assert_eq!(file_dict.files.len(), 1);
}

fn create_file_info(id: u64, method: EncryptionMethod) -> FileInfo {
    FileInfo {
        key_id: id,
        method,
        ..Default::default()
    }
}

#[test]
fn test_kms_provider_temporary_unavailable() {
    #[cfg(any(test, feature = "testexport"))]
    use encryption::fake::*;

    // Simulate temporary unavailable with a timeout error during encryption.
    // Expect the backend to handle the timeout gracefully and succeed on the
    // subsequent retry.
    fail::cfg("kms_api_timeout_encrypt", "1*return(true)").unwrap();
    let (iv, pt, plainkey, ..) = prepare_data_for_encrypt();
    let mut backend = prepare_kms_backend(plainkey, false);
    let encrypted_content = backend.encrypt_content(&pt, iv).unwrap();
    // Clear the cached state to ensure that the subsequent
    // backend.decrypt_content() invocation bypasses the cache and triggers the
    // mocked FakeKMS::decrypt_data_key() function.
    backend.clear_state();

    // Same as above.
    fail::cfg("kms_api_timeout_decrypt", "1*return(true)").unwrap();
    let pt_decrypt = backend.decrypt_content(&encrypted_content).unwrap();
    assert_eq!(pt_decrypt, pt);
}
