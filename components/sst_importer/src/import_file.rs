// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use api_version::api_v2::TIDB_RANGES_COMPLEMENT;
use encryption::{DataKeyManager, EncrypterWriter};
use engine_rocks::{get_env, RocksSstReader};
use engine_traits::{
    iter_option, EncryptionKeyManager, IterOptions, Iterator, KvEngine, RefIterable, SstExt,
    SstMetaInfo, SstReader,
};
use file_system::{get_io_rate_limiter, sync_dir, File, OpenOptions};
use kvproto::{import_sstpb::*, kvrpcpb::ApiVersion};
use protobuf::Message;
use tikv_util::time::Instant;
use uuid::{Builder as UuidBuilder, Uuid};

use crate::{metrics::*, Error, Result};

// `SyncableWrite` extends io::Write with sync
trait SyncableWrite: io::Write + Send {
    // sync all metadata to storage
    fn sync(&self) -> io::Result<()>;
}

impl SyncableWrite for File {
    fn sync(&self) -> io::Result<()> {
        self.sync_all()
    }
}

impl SyncableWrite for EncrypterWriter<File> {
    fn sync(&self) -> io::Result<()> {
        self.sync_all()
    }
}

#[derive(Clone)]
pub struct ImportPath {
    // The path of the file that has been uploaded.
    pub save: PathBuf,
    // The path of the file that is being uploaded.
    pub temp: PathBuf,
    // The path of the file that is going to be ingested.
    pub clone: PathBuf,
    // The path of the origin `SstMeta` (encoded by wire) of the file.
    // We have encoded a subset of SstMeta to the file name, but that isn't enough,
    // for now, we need to get the range of the SST to check whether we are still needing it
    // (Check it solely by region ID isn't good enough -- regions may be destroyed by merging.).
    // "But why not directly read the start key from the SST file?"
    // "Because that couples the `Engine` and `ImportDir` -- In fact, we cannot access `Engine` in
    // the context of validating the SST."
    pub meta: PathBuf,
}

impl ImportPath {
    // move file from temp to save.
    pub fn save(mut self, key_manager: Option<&DataKeyManager>) -> Result<()> {
        if let Some(key_manager) = key_manager {
            let temp_str = self
                .temp
                .to_str()
                .ok_or_else(|| Error::InvalidSstPath(self.temp.clone()))?;
            let save_str = self
                .save
                .to_str()
                .ok_or_else(|| Error::InvalidSstPath(self.save.clone()))?;
            key_manager.link_file(temp_str, save_str)?;
            let r = file_system::rename(&self.temp, &self.save);
            let del_file = if r.is_ok() { temp_str } else { save_str };
            if let Err(e) = key_manager.delete_file(del_file) {
                warn!("fail to remove encryption metadata during 'save'";
                      "file" => ?self, "err" => ?e);
            }
            r?;
        } else {
            file_system::rename(&self.temp, &self.save)?;
        }
        // sync the directory after rename
        self.save.pop();
        sync_dir(&self.save)?;
        Ok(())
    }

    pub fn save_meta(&self, km: Option<&DataKeyManager>, meta: &SstMeta) -> Result<()> {
        use std::io::{Error as IoErr, ErrorKind as IoErrs};
        meta.write_to_bytes()
            .map_err(|err| Error::from(IoErr::new(IoErrs::InvalidInput, err)))
            .and_then(|v| write_bytes(&self.meta, v, km))
    }
}

impl fmt::Debug for ImportPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportPath")
            .field("save", &self.save)
            .field("temp", &self.temp)
            .field("clone", &self.clone)
            .field("meta", &self.meta)
            .finish()
    }
}

/// ImportFile is used to handle the writing and verification of SST files.
pub struct ImportFile {
    meta: SstMeta,
    path: ImportPath,
    file: Option<Box<dyn SyncableWrite>>,
    digest: crc32fast::Hasher,
    key_manager: Option<Arc<DataKeyManager>>,
}

impl ImportFile {
    pub fn create(
        meta: SstMeta,
        path: ImportPath,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<ImportFile> {
        let file: Box<dyn SyncableWrite> = if let Some(ref manager) = key_manager {
            // key manager will truncate existed file, so we should check exist manually.
            if path.temp.exists() {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("file already exists, {}", path.temp.to_str().unwrap()),
                )));
            }
            Box::new(manager.create_file_for_write(&path.temp)?)
        } else {
            Box::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path.temp)?,
            )
        };
        Ok(ImportFile {
            meta,
            path,
            file: Some(file),
            digest: crc32fast::Hasher::new(),
            key_manager,
        })
    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.as_mut().unwrap().write_all(data)?;
        self.digest.update(data);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.validate()?;
        // sync is a wrapping for File::sync_all
        self.file.take().unwrap().sync()?;
        if self.path.save.exists() {
            return Err(Error::FileExists(
                self.path.save.clone(),
                "finalize SST write cache",
            ));
        }
        if let Some(ref manager) = self.key_manager {
            let tmp_str = self.path.temp.to_str().unwrap();
            let save_str = self.path.save.to_str().unwrap();
            manager.link_file(tmp_str, save_str)?;
            let r = file_system::rename(&self.path.temp, &self.path.save);
            let del_file = if r.is_ok() { tmp_str } else { save_str };
            if let Err(e) = manager.delete_file(del_file) {
                warn!("fail to remove encryption metadata during finishing importing files.";
                      "err" => ?e);
            }
            r?;
        } else {
            file_system::rename(&self.path.temp, &self.path.save)?;
        }
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        self.file.take();
        let delete_file_if_exist = |path: &Path| {
            if path.exists() {
                if let Some(ref manager) = self.key_manager {
                    manager.delete_file(path.to_str().unwrap())?;
                }
                file_system::remove_file(path)?;
            }
            Result::Ok(())
        };
        delete_file_if_exist(&self.path.temp)?;
        delete_file_if_exist(&self.path.meta)?;
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        let crc32 = self.digest.clone().finalize();
        let expect = self.meta.get_crc32();
        if crc32 != expect {
            let reason = format!("crc32 {}, expect {}", crc32, expect);
            return Err(Error::FileCorrupted(self.path.temp.clone(), reason));
        }
        Ok(())
    }

    pub fn get_import_path(&self) -> &ImportPath {
        &self.path
    }
}

impl fmt::Debug for ImportFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportFile")
            .field("meta", &self.meta)
            .field("path", &self.path)
            .finish()
    }
}

impl Drop for ImportFile {
    fn drop(&mut self) {
        if let Err(e) = self.cleanup() {
            warn!("cleanup failed"; "file" => ?self, "err" => %e);
        }
    }
}

/// ImportDir is responsible for operating SST files and related path
/// calculations.
///
/// The file being written is stored in `$root/.temp/$file_name`. After writing
/// is completed, the file is moved to `$root/$file_name`. The file generated
/// from the ingestion process will be placed in `$root/.clone/$file_name`.
pub struct ImportDir {
    root_dir: PathBuf,
    temp_dir: PathBuf,
    clone_dir: PathBuf,
    meta_dir: PathBuf,
}

impl ImportDir {
    const TEMP_DIR: &'static str = ".temp";
    const CLONE_DIR: &'static str = ".clone";
    const META_DIR: &'static str = ".meta";

    pub fn new<P: AsRef<Path>>(root: P) -> Result<ImportDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        let clone_dir = root_dir.join(Self::CLONE_DIR);
        let meta_dir = root_dir.join(Self::META_DIR);
        if temp_dir.exists() {
            file_system::remove_dir_all(&temp_dir)?;
        }
        if clone_dir.exists() {
            file_system::remove_dir_all(&clone_dir)?;
        }
        file_system::create_dir_all(&temp_dir)?;
        file_system::create_dir_all(&clone_dir)?;
        file_system::create_dir_all(&meta_dir)?;
        Ok(ImportDir {
            root_dir,
            temp_dir,
            clone_dir,
            meta_dir,
        })
    }

    pub fn get_root_dir(&self) -> &PathBuf {
        &self.root_dir
    }

    /// Make an import path base on the basic path and the file name.
    pub fn get_import_path(&self, file_name: &str) -> Result<ImportPath> {
        let save_path = self.root_dir.join(file_name);
        let temp_path = self.temp_dir.join(file_name);
        let clone_path = self.clone_dir.join(file_name);
        let meta_path = self.meta_dir.join(file_name);
        Ok(ImportPath {
            save: save_path,
            temp: temp_path,
            clone: clone_path,
            meta: meta_path,
        })
    }

    pub fn join(&self, meta: &SstMeta) -> Result<ImportPath> {
        let file_name = sst_meta_to_path(meta)?;
        self.get_import_path(file_name.to_str().unwrap())
    }

    pub fn create(
        &self,
        meta: &SstMeta,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<ImportFile> {
        let path = self.join(meta)?;
        if path.save.exists() {
            return Err(Error::FileExists(path.save, "create SST upload cache"));
        }
        ImportFile::create(meta.clone(), path, key_manager)
    }

    pub fn delete_file(&self, path: &Path, key_manager: Option<&DataKeyManager>) -> Result<()> {
        if path.exists() {
            file_system::remove_file(path)?;
            if let Some(manager) = key_manager {
                manager.delete_file(path.to_str().unwrap())?;
            }
        }

        Ok(())
    }

    pub fn delete(&self, meta: &SstMeta, manager: Option<&DataKeyManager>) -> Result<ImportPath> {
        let path = self.join(meta)?;
        self.delete_file(&path.save, manager)?;
        self.delete_file(&path.temp, manager)?;
        self.delete_file(&path.clone, manager)?;
        self.delete_file(&path.meta, manager)?;
        Ok(path)
    }

    pub fn exist(&self, meta: &SstMeta) -> Result<bool> {
        let path = self.join(meta)?;
        Ok(path.save.exists())
    }

    pub fn validate(
        &self,
        meta: &SstMeta,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<SstMetaInfo> {
        let path = self.join(meta)?;
        let path_str = path.save.to_str().unwrap();
        let env = get_env(key_manager, get_io_rate_limiter())?;
        let sst_reader = RocksSstReader::open_with_env(path_str, Some(env))?;
        // TODO: check the length and crc32 of ingested file.
        let meta_info = sst_reader.sst_meta_info(meta.to_owned());
        Ok(meta_info)
    }

    /// check if api version of sst files are compatible
    pub fn check_api_version(
        &self,
        metas: &[SstMeta],
        key_manager: Option<Arc<DataKeyManager>>,
        api_version: ApiVersion,
    ) -> Result<bool> {
        for meta in metas {
            match (api_version, meta.api_version) {
                (cur_version, meta_version) if cur_version == meta_version => continue,
                // sometimes client do not know whether ttl is enabled, so a general V1 is accepted
                // as V1ttl
                (ApiVersion::V1ttl, ApiVersion::V1) => continue,
                // import V1ttl as V1 will immediatly be rejected because it is never correct.
                (ApiVersion::V1, ApiVersion::V1ttl) => return Ok(false),
                // otherwise we are upgrade/downgrade between V1 and V2
                // this can be done if all keys are written by TiDB
                _ => {
                    let path = self.join(meta)?;
                    let path_str = path.save.to_str().unwrap();
                    let env = get_env(key_manager.clone(), get_io_rate_limiter())?;
                    let sst_reader = RocksSstReader::open_with_env(path_str, Some(env))?;

                    for &(start, end) in TIDB_RANGES_COMPLEMENT {
                        let opt = iter_option(start, end, false);
                        let mut iter = sst_reader.iter(opt)?;
                        if iter.seek(start)? {
                            error!(
                                "unable to import: switch api version with non-tidb key";
                                "sst" => ?meta.api_version,
                                "current" => ?api_version,
                                "key" => ?log_wrappers::hex_encode_upper(iter.key())
                            );
                            return Ok(false);
                        }
                    }
                }
            }
        }
        info!("api_check success");
        Ok(true)
    }

    pub fn ingest<E: KvEngine>(
        &self,
        metas: &[SstMetaInfo],
        engine: &E,
        key_manager: Option<Arc<DataKeyManager>>,
        api_version: ApiVersion,
    ) -> Result<()> {
        let start = Instant::now();

        let meta_vec = metas
            .iter()
            .map(|info| info.meta.clone())
            .collect::<Vec<_>>();
        if !self
            .check_api_version(&meta_vec, key_manager.clone(), api_version)
            .unwrap()
        {
            panic!("cannot ingest because of incompatible api version");
        }

        let mut paths = HashMap::new();
        let mut ingest_bytes = 0;
        for info in metas {
            let path = self.join(&info.meta)?;
            let cf = info.meta.get_cf_name();
            super::prepare_sst_for_ingestion(&path.save, &path.clone, key_manager.as_deref())?;
            ingest_bytes += info.total_bytes;
            paths.entry(cf).or_insert_with(Vec::new).push(path);
        }

        for (cf, cf_paths) in paths {
            let files: Vec<&str> = cf_paths.iter().map(|p| p.clone.to_str().unwrap()).collect();
            engine.ingest_external_file_cf(cf, &files)?;
        }
        INPORTER_INGEST_COUNT.observe(metas.len() as _);
        IMPORTER_INGEST_BYTES.observe(ingest_bytes as _);
        IMPORTER_INGEST_DURATION
            .with_label_values(&["ingest"])
            .observe(start.saturating_elapsed().as_secs_f64());
        Ok(())
    }

    pub fn verify_checksum(
        &self,
        metas: &[SstMeta],
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Result<()> {
        for meta in metas {
            let path = self.join(meta)?;
            let path_str = path.save.to_str().unwrap();
            let env = get_env(key_manager.clone(), get_io_rate_limiter())?;
            let sst_reader = RocksSstReader::open_with_env(path_str, Some(env))?;
            sst_reader.verify_checksum()?;
        }
        Ok(())
    }

    fn fill_by_persisted_meta(
        &self,
        p: &Path,
        sc: Option<&DataKeyManager>,
        m0: &mut SstMeta,
    ) -> Result<()> {
        use std::io::{Error as IoErr, ErrorKind as IoErrs};
        let fname = p
            .file_name()
            .ok_or_else(|| Error::InvalidSstPath(p.to_owned()))?;
        let meta_path = self.meta_dir.join(fname);

        let meta_bytes = match sc {
            None => file_system::read(&meta_path)?,
            Some(s) => {
                let mut v = Vec::new();
                s.open_file_for_read(&meta_path)?.read_to_end(&mut v)?;
                v
            }
        };
        let old_uuid = m0.get_uuid().to_owned();
        let backup = m0.clone();
        m0.merge_from_bytes(&meta_bytes)
            .map_err(|err| IoErr::new(IoErrs::InvalidData, err))?;
        if old_uuid != m0.get_uuid() {
            // The meta might be modified by the `merge_from_bytes`, let's
            // restore it.
            *m0 = backup;
            return Err(Error::FileCorrupted(
                meta_path,
                format!(
                    "the meta file uuid doesn't match the filename: {:?} vs {:?}",
                    old_uuid,
                    m0.get_uuid()
                ),
            ));
        }
        Ok(())
    }

    pub fn clean_unused_meta(&self, km: Option<&DataKeyManager>) -> Result<()> {
        let start = Instant::now_coarse();
        let ssts = self.list_ssts()?;
        let sst_set = ssts
            .iter()
            .filter_map(|m| {
                let p: PathBuf = sst_meta_to_path(m).ok()?;
                let file_name = p.file_name()?.to_owned();
                Some(file_name)
            })
            .collect::<HashSet<_>>();
        let mut cleaned = 0;
        for e in file_system::read_dir(&self.meta_dir)? {
            let e = e?;
            if !sst_set.contains(&e.file_name()) {
                self.delete_file(&e.path(), km)?;
                cleaned += 1;
            }
        }
        info!("SST metadata dir cleaned."; "removed_stale_meta" => %cleaned, "take" => ?start.saturating_elapsed());
        Ok(())
    }

    pub fn load_start_key_by_meta<E: SstExt>(
        &self,
        meta: &SstMeta,
        km: Option<Arc<DataKeyManager>>,
    ) -> Result<Option<Vec<u8>>> {
        let path = self.join(meta)?;
        let r = match km {
            Some(km) => E::SstReader::open_encrypted(&path.save.to_string_lossy(), km)?,
            None => E::SstReader::open(&path.save.to_string_lossy())?,
        };
        let opts = IterOptions::new(None, None, false);
        let mut i = r.iter(opts)?;
        if !i.seek_to_first()? || !i.valid()? {
            return Ok(None);
        }
        Ok(Some(i.key().to_owned()))
    }

    pub fn try_fetch_full_meta(
        &self,
        meta: &SstMeta,
        km: Option<&DataKeyManager>,
    ) -> Result<SstMeta> {
        let path = sst_meta_to_path(meta)?;
        let meta_path = self.meta_dir.join(path);
        let mut m1 = meta.clone();
        self.fill_by_persisted_meta(&meta_path, km, &mut m1)?;
        Ok(m1)
    }

    pub fn list_ssts(&self) -> Result<Vec<SstMeta>> {
        let mut ssts = Vec::new();
        for e in file_system::read_dir(&self.root_dir)? {
            let e = e?;
            if !e.file_type()?.is_file() {
                continue;
            }
            let path = e.path();
            match parse_meta_from_path(&path) {
                Ok(sst) => ssts.push(sst),
                Err(e) => error!(%e; "path_to_sst_meta failed"; "path" => %path.display(),),
            }
        }
        Ok(ssts)
    }
}

const SST_SUFFIX: &str = ".sst";

fn write_bytes(p: impl AsRef<Path>, content: Vec<u8>, km: Option<&DataKeyManager>) -> Result<()> {
    match km {
        Some(sc) => sc.create_file_for_write(p)?.write_all(&content)?,
        None => file_system::write(p, content)?,
    };
    Ok(())
}

pub fn sst_meta_to_path(meta: &SstMeta) -> Result<PathBuf> {
    Ok(PathBuf::from(format!(
        "{}_{}_{}_{}_{}{}",
        UuidBuilder::from_slice(meta.get_uuid())?.build(),
        meta.get_region_id(),
        meta.get_region_epoch().get_conf_ver(),
        meta.get_region_epoch().get_version(),
        meta.get_cf_name(),
        SST_SUFFIX,
    )))
}

pub fn parse_meta_from_path<P: AsRef<Path>>(path: P) -> Result<SstMeta> {
    let path = path.as_ref();
    let file_name = match path.file_name().and_then(|n| n.to_str()) {
        Some(name) => name,
        None => return Err(Error::InvalidSstPath(path.to_owned())),
    };

    // A valid file name should be in the format:
    // "{uuid}_{region_id}_{region_epoch.conf_ver}_{region_epoch.version}_{cf}.sst"
    if !file_name.ends_with(SST_SUFFIX) {
        return Err(Error::InvalidSstPath(path.to_owned()));
    }
    let elems: Vec<_> = file_name.trim_end_matches(SST_SUFFIX).split('_').collect();
    if elems.len() < 4 {
        return Err(Error::InvalidSstPath(path.to_owned()));
    }

    let mut meta = SstMeta::default();
    let uuid = Uuid::parse_str(elems[0])?;
    meta.set_uuid(uuid.as_bytes().to_vec());
    meta.set_region_id(elems[1].parse()?);
    meta.mut_region_epoch().set_conf_ver(elems[2].parse()?);
    meta.mut_region_epoch().set_version(elems[3].parse()?);
    if elems.len() > 4 {
        // If we upgrade TiKV from 3.0.x to 4.0.x and higher version, we can not read
        // cf_name from the file path, because TiKV 3.0.x does not encode
        // cf_name to path.
        meta.set_cf_name(elems[4].to_owned());
    }
    Ok(meta)
}

#[cfg(test)]
mod test {
    use std::mem::ManuallyDrop;

    use engine_rocks::{RocksEngine, RocksSstWriter, RocksSstWriterBuilder};
    use engine_test::ctor::{CfOptions, DbOptions};
    use engine_traits::{SstWriter, SstWriterBuilder, CF_DEFAULT};
    use tempfile::TempDir;
    use test_util::new_test_key_manager;

    use super::*;

    #[test]
    fn test_sst_meta_to_path() {
        let mut meta = SstMeta::default();
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_region_id(1);
        meta.set_cf_name(CF_DEFAULT.to_owned());
        meta.mut_region_epoch().set_conf_ver(2);
        meta.mut_region_epoch().set_version(3);

        let path = sst_meta_to_path(&meta).unwrap();
        let expected_path = format!("{}_1_2_3_default.sst", uuid);
        assert_eq!(path.to_str().unwrap(), &expected_path);

        let new_meta = parse_meta_from_path(path).unwrap();
        assert_eq!(meta, new_meta);
    }

    #[test]
    fn test_path_to_sst_meta() {
        let uuid = Uuid::new_v4();
        let mut meta = SstMeta::default();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_region_id(1);
        meta.mut_region_epoch().set_conf_ver(222);
        meta.mut_region_epoch().set_version(333);
        let path = PathBuf::from(format!(
            "{}_{}_{}_{}{}",
            UuidBuilder::from_slice(meta.get_uuid()).unwrap().build(),
            meta.get_region_id(),
            meta.get_region_epoch().get_conf_ver(),
            meta.get_region_epoch().get_version(),
            SST_SUFFIX,
        ));
        let new_meta = parse_meta_from_path(path).unwrap();
        assert_eq!(meta, new_meta);
    }

    fn test_path_with_range_and_km(km: Option<DataKeyManager>) {
        let arcmgr = km.map(Arc::new);
        let tmp = TempDir::new().unwrap();
        let dir = ImportDir::new(tmp.path()).unwrap();
        let mut meta = SstMeta::default();
        let mut rng = Range::new();
        rng.set_start(b"hello".to_vec());
        let uuid = Uuid::new_v4();
        meta.set_uuid(uuid.as_bytes().to_vec());
        meta.set_region_id(1);
        meta.set_range(rng);
        meta.mut_region_epoch().set_conf_ver(222);
        meta.mut_region_epoch().set_version(333);
        let mut db_opt = DbOptions::default();
        db_opt.set_key_manager(arcmgr.clone());
        let e = engine_test::kv::new_engine_opt(
            &tmp.path().join("eng").to_string_lossy(),
            db_opt,
            vec![(CF_DEFAULT, CfOptions::new())],
        )
        .unwrap();
        let f = dir.create(&meta, arcmgr.clone()).unwrap();
        let dp = f.path.clone();
        let mut w = RocksSstWriterBuilder::new()
            .set_db(&e)
            .set_cf(CF_DEFAULT)
            .build(f.path.temp.to_str().unwrap())
            .unwrap();
        w.put(b"hello", concat!("This is the start key of the SST, ",
              "how about some of our users uploads metas with range not aligned with the content of SST?",
              "May the real region still be in the current store.").as_bytes()).unwrap();
        w.put(
            b"world",
            concat!(
                "This is the end key of the SST, ",
                "it is so we can check whether we are loding the right key from the SST."
            )
            .as_bytes(),
        )
        .unwrap();
        w.finish().unwrap();
        dp.save(arcmgr.as_deref()).unwrap();
        let mut ssts = dir.list_ssts().unwrap();
        ssts.iter_mut().for_each(|meta| {
            let start = dir
                .load_start_key_by_meta::<RocksEngine>(meta, arcmgr.clone())
                .unwrap()
                .unwrap();
            meta.mut_range().set_start(start)
        });
        assert_eq!(ssts, vec![meta]);
    }

    #[test]
    fn test_path_with_range() {
        test_path_with_range_and_km(None)
    }

    #[test]
    fn test_path_with_range_encrypted() {
        let dir = TempDir::new().unwrap();
        let enc = new_test_key_manager(&dir, None, None, None)
            .unwrap()
            .unwrap();
        test_path_with_range_and_km(Some(enc));
    }
}
