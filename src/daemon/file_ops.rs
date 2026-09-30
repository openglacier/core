//! Files and data worker requests: metadata, raw byte streams and local sync.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;
use super::serve::{diverged, failed, reply, request_error, store_failed, Connection, Handled, Stop};

/// Checks that the caller may read, or with `write` change, the Files of an AppInstance.
#[cfg(feature = "files")]
fn ensure_file_access(engine: &Engine, delegation: Option<&GatewayDelegation>, principal: &Principal, id: RequestId, place_id: &str, instance_id: &str, write: bool) -> Handled {
    let delegated = delegation.filter(|value| {
        value.capability == "files"
            && value.place_id.as_deref() == Some(place_id)
            && value.app_instance_id.as_deref().is_none_or(|delegated_instance| delegated_instance == instance_id)
    });
    match delegated {
        // The Authority resolved the grant: a delegated write still needs a writing grant.
        Some(value) => {
            if write && value.access().is_some_and(|access| !access.can_write()) {
                reject!(id, "authorization.denied", "Place access is read-only for Files");
            }
        }
        None => { resolve_file_context(engine, id, principal, place_id, instance_id, write)?; }
    }
    Ok(())
}

/// The local Files sync state, reachable only on the node itself.
#[cfg(feature = "files")]
fn local_sync_runtime<'a>(settings: &'a ConnectionSettings, delegation: Option<&GatewayDelegation>, id: RequestId, what: &str) -> Result<std::sync::MutexGuard<'a, FileSyncRuntime>, Stop> {
    if delegation.is_some() {
        reject!(id, "file.sync.local_only", format!("Files sync {what} is local to this node"));
    }
    settings.file_sync.lock().map_err(|_| request_error(id, "file.sync.state_poisoned", "Files sync local state is unavailable"))
}

/// The Identity and Device running Files sync: the node Identity configured for this ogd.
#[cfg(feature = "files")]
fn sync_node_device(settings: &ConnectionSettings, principal: &Principal, id: RequestId, anonymous: &str, other_identity: &str) -> Result<(String, String), Stop> {
    let Principal::Identity { identity_id, device_id } = principal else {
        reject!(id, "authorization.denied", anonymous);
    };
    if settings.upstream_identity.as_ref().is_some_and(|credential| &credential.identity_id != identity_id || &credential.device_id != device_id) {
        reject!(id, "file.sync.node_identity_required", other_identity);
    }
    Ok((identity_id.clone(), device_id.clone()))
}

#[cfg(feature = "files")]
fn record_remote_id(id: RequestId, version: &JsonValue) -> Result<&str, Stop> {
    version.str_field("remoteId").ok_or_else(|| request_error(id, "file.invalid_record", "version has no remoteId"))
}

#[cfg(any(feature = "files", feature = "data-import"))]
pub(super) fn handle_file_operation(connection: &mut Connection, settings: &ConnectionSettings, engine: Option<&Engine>, delegation: Option<&GatewayDelegation>, operation: RoutedOperation) -> Handled {
    #[cfg(feature = "files")]
    let engine = engine.expect("files operation routed without db-engine");
    let principal = connection.authentication.principal();
    let writer = &mut connection.writer;
    let reader = &mut connection.reader;
    match operation {
        #[cfg(feature = "data-import")]
        RoutedOperation::DataWorkerRun(Routed { id, input: DataWorkerRunInput { place_id, file_name, size, operation, mapping } }) => {
            let delegated = delegation.is_some_and(|value| value.capability == "data.import" && value.place_id.as_deref() == Some(place_id.as_str()));
            if !delegated {
                reject!(id, "authorization.denied", "data.worker.run requires a delegated data.import channel");
            }
            reply(writer, id, serde_json::json!({"ready":true,"bytes":size,"stream":"raw"}))?;
            writer.flush().map_err(ConnectionError::Write)?;
            let extension=Path::new(&file_name).extension().and_then(|v|v.to_str()).unwrap_or("dat");
            let temp=env::temp_dir().join(format!("og-data-{}-{}.{}",std::process::id(),unix_time_millis(),extension));
            let mut out=fs::File::create(&temp).map_err(ConnectionError::Write)?;
            let mut limited=reader.by_ref().take(size);
            let copied=io::copy(&mut limited,&mut out).map_err(ConnectionError::Read)?;
            if copied!=size {
                let _=fs::remove_file(&temp);
                return Err(ConnectionError::Read(io::Error::new(io::ErrorKind::UnexpectedEof,format!("data worker upload ended after {copied} of {size} bytes"))).into());
            }
            drop(out);
            let result=run_data_worker_for_path(id,&temp,&operation,mapping.as_ref());
            let _=fs::remove_file(&temp);
            reply(writer, id, result?)?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileRead(Routed { id, input: FileReadInput { place_id, instance_id, file_id, offset, length } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, false)?;
            let file_store=scoped_native_file_store(settings,id,&place_id,&instance_id)?;
            let entry=load_file_entry(engine,id,&place_id,&instance_id,&file_id)?;
            let stat=file_store.stat(&entry.remote_id).map_err(store_failed(id))?;
            let total=stat.metadata.size.ok_or_else(|| request_error(id,"file.not_file","File entry has no byte size"))?;
            if offset>total || length.is_some_and(|requested|requested>total.saturating_sub(offset)){
                reject!(id,"file.invalid_range","requested byte range is outside the file");
            }
            let bytes=length.unwrap_or_else(||total.saturating_sub(offset));
            let range=(offset!=0 || length.is_some()).then(||FileRange::new(offset,length));
            let mut source=file_store.read(&entry.remote_id,range).map_err(store_failed(id))?;
            reply(writer, id, serde_json::json!({ "fileId":file_id, "name":entry.name, "contentType":entry.metadata.content_type, "etag":stat.metadata.etag, "size":total, "offset":offset, "bytes":bytes, "stream":"raw" }))?;
            writer.flush().map_err(ConnectionError::Write)?;
            let copied=io::copy(&mut source,writer).map_err(ConnectionError::Write)?;
            if copied!=bytes{return Err(ConnectionError::Write(io::Error::new(io::ErrorKind::UnexpectedEof,format!("file stream ended after {copied} of {bytes} bytes"))).into());}
            writer.flush().map_err(ConnectionError::Write)?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileWrite(Routed { id, input: FileWriteInput { place_id, instance_id, file_id, parent_id, name, content_type, size } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, true)?;
            let file_store=scoped_native_file_store(settings,id,&place_id,&instance_id)?;
            let existing=file_id.as_deref().map(|value|load_file_entry(engine,id,&place_id,&instance_id,value)).transpose()?;
            if existing.as_ref().is_some_and(|entry|entry.kind!=crate::files::FileKind::File){
                reject!(id,"file.not_file","cannot replace a directory with file content");
            }
            let parent=if let Some(existing)=existing.as_ref(){existing.parent_id.as_ref().map(|value|value.as_str().to_owned())}else{parent_id.clone()};
            let parent_remote=parent.as_deref().map(|value|load_file_entry(engine,id,&place_id,&instance_id,value)).transpose()?.map(|entry|entry.remote_id);
            let target_name=existing.as_ref().map(|entry|entry.name.as_str()).or(name.as_deref()).expect("router validates create name");
            let remote_id=existing.as_ref().map(|entry|entry.remote_id.as_str());
            if let Some(previous)=existing.as_ref(){
                archive_current_file(engine,settings,id,previous)?;
            }
            reply(writer, id, serde_json::json!({"ready":true,"bytes":size,"stream":"raw"}))?;
            writer.flush().map_err(ConnectionError::Write)?;
            let mut limited=reader.by_ref().take(size);
            let target=FileWrite{remote_id,parent_remote_id:parent_remote.as_deref(),name:target_name,content_type:content_type.as_deref(),size:Some(size)};
            let mut stored=file_store.write(target,&mut limited).map_err(store_failed(id))?;
            stored.metadata.content_type=content_type.clone().or_else(||existing.as_ref().and_then(|entry|entry.metadata.content_type.clone()));
            let updated=match existing{
                Some(previous)=>FileEntry{file_id:previous.file_id,store_id:previous.store_id,remote_id:stored.remote_id,parent_id:previous.parent_id,name:previous.name,kind:stored.kind,metadata:stored.metadata,place_id:place_id.clone(),app_instance_id:instance_id.clone()},
                None=>file_entry_from_store(&place_id,&instance_id,parent_id.as_deref(),stored),
            };
            let result=if file_id.is_some(){replace_file_entry(engine,id,&updated)}else{persist_file_entry(engine,id,&updated)};
            let json=result.inspect_err(|_| if file_id.is_none(){let _=file_store.delete(&updated.remote_id);})?;
            if let Ok(audience)=place_audience(engine,id,&place_id) {
                let event_type=if file_id.is_some(){"file.updated"}else{"file.created"};
                publish_durable_event(engine,audience,event_type,serde_json::json!({
                    "placeId":place_id,
                    "instanceId":instance_id,
                    "fileId":updated.file_id.as_str(),
                    "name":updated.name,
                }));
            }
            reply(writer, id, serde_json::json!({"file":json,"bytes":size,"stream":"raw"}))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileVersions(Routed { id, input: FileEntryInput { place_id, instance_id, file_id } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, false)?;
            let versions=list_file_versions(engine,id,&place_id,&instance_id,&file_id)?;
            reply(writer, id, serde_json::json!({"versions":versions}))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileVersionRead(Routed { id, input: FileVersionReadInput { place_id, instance_id, file_id, version_id, offset, length } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, false)?;
            let version=load_file_version(engine,id,&place_id,&instance_id,&file_id,&version_id)?;
            let remote_id=record_remote_id(id,&version)?;
            let store=scoped_native_version_store(settings,id,&place_id,&instance_id)?;
            let total=version.u64_field("size").unwrap_or(0);
            let range=if offset.is_some() || length.is_some(){Some(FileRange::new(offset.unwrap_or(0),length))}else{None};
            let mut source=store.read(remote_id,range).map_err(store_failed(id))?;
            let start=offset.unwrap_or(0);
            let available=total.saturating_sub(start);
            let bytes=length.unwrap_or(available).min(available);
            reply(writer, id, serde_json::json!({ "stream":"raw","bytes":bytes,"size":total,"offset":offset.unwrap_or(0), "contentType":version.get("contentType").cloned().unwrap_or(JsonValue::Null), "etag":version.get("etag").cloned().unwrap_or(JsonValue::Null), "versionId":version_id,"fileId":file_id }))?;
            writer.flush().map_err(ConnectionError::Write)?;
            io::copy(&mut source,writer).map_err(ConnectionError::Write)?;
            writer.flush().map_err(ConnectionError::Write)?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileVersionRestore(Routed { id, input: FileVersionInput { place_id, instance_id, file_id, version_id } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, true)?;
            let current=load_file_entry(engine,id,&place_id,&instance_id,&file_id)?;
            let version=load_file_version(engine,id,&place_id,&instance_id,&file_id,&version_id)?;
            archive_current_file(engine,settings,id,&current)?;
            let version_remote=record_remote_id(id,&version)?;
            let versions=scoped_native_version_store(settings,id,&place_id,&instance_id)?;
            let current_store=scoped_native_file_store(settings,id,&place_id,&instance_id)?;
            let mut source=versions.read(version_remote,None).map_err(store_failed(id))?;
            let size=version.u64_field("size");
            let content_type=version.str_field("contentType");
            let stored=current_store.write(FileWrite{remote_id:Some(&current.remote_id),parent_remote_id:None,name:&current.name,content_type,size},&mut source).map_err(store_failed(id))?;
            let updated=FileEntry{file_id:current.file_id,store_id:current.store_id,remote_id:stored.remote_id,parent_id:current.parent_id,name:current.name,kind:stored.kind,metadata:stored.metadata,place_id,app_instance_id:instance_id};
            let file=replace_file_entry(engine,id,&updated)?;
            if let Ok(audience)=place_audience(engine,id,&updated.place_id) {
                publish_durable_event(engine,audience,"file.updated",serde_json::json!({
                    "placeId":updated.place_id,"instanceId":updated.app_instance_id,"fileId":updated.file_id.as_str(),"restoredVersionId":version_id,
                }));
            }
            reply(writer, id, serde_json::json!({"file":file,"restoredVersionId":version_id}))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileVersionDelete(Routed { id, input: FileVersionInput { place_id, instance_id, file_id, version_id } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, true)?;
            let version=load_file_version(engine,id,&place_id,&instance_id,&file_id,&version_id)?;
            let remote_id=record_remote_id(id,&version)?;
            let store=scoped_native_version_store(settings,id,&place_id,&instance_id)?;
            store.delete(remote_id).map_err(store_failed(id))?;
            delete_file_version_metadata(engine,id,&place_id,&instance_id,&file_id,&version_id)?;
            reply(writer, id, serde_json::json!({"deleted":true,"versionId":version_id}))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileSyncConfigGet(Routed { id, .. }) => {
            let runtime = local_sync_runtime(settings, delegation, id, "configuration")?;
            reply(writer, id, serde_json::json!({
                "configured": runtime.config.is_some(),
                "config": runtime.config.as_ref(),
                "projection": {
                    "appsDirectory": APP_FILES_DIRECTORY,
                    "primaryAppsCollisionDirectory": PRIMARY_APPS_COLLISION_NAME,
                }
            }))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileSyncConfigSet(Routed { id, input: FileSyncConfigSetInput { root } }) => {
            if delegation.is_some() {
                reject!(id, "file.sync.local_only", "Files sync configuration is local to this node");
            }
            let root = PathBuf::from(root);
            if let Err(error) = fs::create_dir_all(&root) {
                reject!(id, "file.sync.root_unavailable", format!("cannot prepare Files sync root {}: {error}", root.display()));
            }
            let mut runtime = local_sync_runtime(settings, delegation, id, "configuration")?;
            let mut config = runtime.config.clone().unwrap_or_else(|| FileSyncConfig::new(root.clone()));
            let changed = config.root != root;
            config.root = root;
            if changed {
                config.local_baseline_required = true;
            }
            if let Err(error) = config.save(&runtime.config_path) {
                reject!(id, "file.sync.config_write_failed", format!("cannot persist Files sync configuration: {error}"));
            }
            runtime.config = Some(config.clone());
            file_sync_signal(&runtime, "config.changed");
            reply(writer, id, serde_json::json!({
                "configured": true,
                "rootChanged": changed,
                "config": config,
            }))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileSyncSelectionSet(Routed { id, input: FileSyncSelectionSetInput { place_id, instance_id, all, folder_ids } }) => {
            let mut runtime = local_sync_runtime(settings, delegation, id, "selection")?;
            let Some(mut config) = runtime.config.clone() else {
                reject!(id, "file.sync.not_configured", "configure a Files sync root before selecting folders");
            };
            let mode = if all { FileSyncSelectionMode::All } else { FileSyncSelectionMode::Selected };
            config.set_selection(place_id.clone(), instance_id.clone(), mode, folder_ids);
            if let Err(error) = config.save(&runtime.config_path) {
                reject!(id, "file.sync.config_write_failed", format!("cannot persist Files sync selection: {error}"));
            }
            let selection = config.selection(&place_id, &instance_id).cloned();
            runtime.config = Some(config);
            file_sync_signal(&runtime, "selection.changed");
            reply(writer, id, serde_json::json!({
                "placeId": place_id,
                "instanceId": instance_id,
                "selection": selection,
            }))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileSyncSelectionRemove(Routed { id, input: FileSyncSelectionRemoveInput { place_id, instance_id } }) => {
            let mut runtime = local_sync_runtime(settings, delegation, id, "selection")?;
            let Some(mut config) = runtime.config.clone() else {
                reject!(id, "file.sync.not_configured", "Files sync is not configured");
            };
            let existed = config.selection(&place_id, &instance_id).is_some();
            config.remove_selection(&place_id, &instance_id);
            if let Err(error) = config.save(&runtime.config_path) {
                reject!(id, "file.sync.config_write_failed", format!("cannot persist Files sync selection: {error}"));
            }
            runtime.config = Some(config);
            file_sync_signal(&runtime, "selection.removed");
            reply(writer, id, serde_json::json!({
                "placeId": place_id,
                "instanceId": instance_id,
                "removed": existed,
            }))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileSyncStatus(Routed { id, .. }) => {
            let runtime = local_sync_runtime(settings, delegation, id, "status")?;
            reply(writer, id, file_sync_status(settings, &runtime).map_err(|error| request_error(id, "file.sync.index_read_failed", format!("cannot read Files sync index: {error}")))?)?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileSyncRun(Routed { id, .. }) => {
            if local_sync_runtime(settings, delegation, id, "execution")?.config.is_none() {
                reject!(id, "file.sync.not_configured", "configure a Files sync root before running synchronization");
            }
            let (sync_identity_id, sync_device_id) = sync_node_device(settings, principal, id, "Files sync requires an authenticated device", "Files sync must be run by the node Identity configured for this ogd")?;
            // Keep the explicit operation useful for diagnostics/manual forcing,
            // while sharing the exact same reconciler and serialization lock as the
            // continuous worker.
            let (run_at, summary) = run_file_sync_once(settings, engine, &sync_identity_id, &sync_device_id).map_err(failed(id, "file.sync.run_failed"))?;
            reply(writer, id, serde_json::json!({"completedAt": run_at, "summary": summary}))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileSyncFolders(Routed { id, input: FileListInput { place_id, instance_id, parent_id } }) => {
            if delegation.is_some() {
                reject!(id, "file.sync.local_only", "Files sync folder discovery is local to this node");
            }
            sync_node_device(settings, principal, id, "Files sync folder discovery requires an authenticated device", "Files sync folder discovery must use the node Identity configured for this ogd")?;
            let mut source = FileSyncSource::connect(settings, engine).map_err(failed(id, "file.sync.source_unavailable"))?;
            let entries = source.file_children(&place_id, &instance_id, parent_id.as_deref()).map_err(failed(id, "file.sync.folder_list_failed"))?;
            let folders = entries.into_iter()
                .filter(|entry| entry.kind == crate::files::FileKind::Directory)
                .map(|entry| file_entry_json(&entry))
                .collect::<Vec<_>>();
            reply(writer, id, JsonValue::Array(folders))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileCapabilities(Routed { id, input: FileScopeInput { place_id, instance_id } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, false)?;
            let file_store=scoped_native_file_store(settings,id,&place_id,&instance_id)?;
            reply(writer, id, serde_json::to_value(file_store.capabilities()).expect("capabilities serialize"))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileList(Routed { id, input: FileListInput { place_id, instance_id, parent_id } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, false)?;
            reply(writer, id, JsonValue::Array(list_file_entries(engine,id,&place_id,&instance_id,parent_id.as_deref())?))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileStat(Routed { id, input: FileEntryInput { place_id, instance_id, file_id } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, false)?;
            reply(writer, id, load_file_entry_json(engine,id,&place_id,&instance_id,&file_id)?)?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileMkdir(Routed { id, input: FileMkdirInput { place_id, instance_id, parent_id, name } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, true)?;
            let file_store=scoped_native_file_store(settings,id,&place_id,&instance_id)?;
            let parent_remote=parent_id.as_deref().map(|parent| load_file_entry(engine,id,&place_id,&instance_id,parent)).transpose()?.map(|entry|entry.remote_id);
            let stored=file_store.mkdir(parent_remote.as_deref(),&name).map_err(store_failed(id))?;
            let entry=file_entry_from_store(&place_id,&instance_id,parent_id.as_deref(),stored);
            let json=persist_file_entry(engine,id,&entry).inspect_err(|_| { let _=file_store.delete(&entry.remote_id); })?;
            if let Ok(audience)=place_audience(engine,id,&place_id) {
                publish_durable_event(engine,audience,"file.created",serde_json::json!({
                    "placeId":place_id,"instanceId":instance_id,"fileId":entry.file_id.as_str(),"name":entry.name,
                }));
            }
            reply(writer, id, json)?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileMove(Routed { id, input: FileMoveInput { place_id, instance_id, file_id, parent_id, name } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, true)?;
            let file_store=scoped_native_file_store(settings,id,&place_id,&instance_id)?;
            let entry=load_file_entry(engine,id,&place_id,&instance_id,&file_id)?;
            let parent_remote=parent_id.as_deref().map(|parent| load_file_entry(engine,id,&place_id,&instance_id,parent)).transpose()?.map(|e|e.remote_id);
            let stored=file_store.move_entry(&entry.remote_id,parent_remote.as_deref(),&name).map_err(store_failed(id))?;
            let updated=FileEntry{file_id:entry.file_id.clone(),store_id:entry.store_id.clone(),remote_id:stored.remote_id,parent_id:parent_id.as_deref().map(FileId::from),name:stored.name,kind:stored.kind,metadata:stored.metadata,place_id:place_id.clone(),app_instance_id:instance_id.clone()};
            let json=replace_file_entry(engine,id,&updated)?;
            if updated.kind == crate::files::FileKind::Directory {
                sync_moved_children(engine,id,&file_store,&place_id,&instance_id,&updated)?;
            }
            if let Ok(audience)=place_audience(engine,id,&place_id) {
                publish_durable_event(engine,audience,"file.moved",serde_json::json!({
                    "placeId":place_id,"instanceId":instance_id,"fileId":updated.file_id.as_str(),"name":updated.name,
                }));
            }
            reply(writer, id, json)?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileCopy(Routed { id, input: FileMoveInput { place_id, instance_id, file_id, parent_id, name } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, true)?;
            let file_store=scoped_native_file_store(settings,id,&place_id,&instance_id)?;
            let entry=load_file_entry(engine,id,&place_id,&instance_id,&file_id)?;
            if entry.kind==crate::files::FileKind::Directory && subtree_has_trashed_entries(engine,id,&place_id,&instance_id,&file_id)? {
                reject!(id,"file.trash_conflict","restore or permanently delete trashed descendants before copying this directory");
            }
            let parent_remote=parent_id.as_deref().map(|parent| load_file_entry(engine,id,&place_id,&instance_id,parent)).transpose()?.map(|e|e.remote_id);
            let stored=file_store.copy(&entry.remote_id,parent_remote.as_deref(),&name).map_err(store_failed(id))?;
            let copied=file_entry_from_store(&place_id,&instance_id,parent_id.as_deref(),stored);
            let persisted=persist_file_entry(engine,id,&copied).and_then(|json| {
                if copied.kind == crate::files::FileKind::Directory {
                    persist_copied_children(engine,id,&file_store,&place_id,&instance_id,&copied)?;
                }
                Ok(json)
            });
            let json=persisted.inspect_err(|_| { let _=file_store.delete(&copied.remote_id); })?;
            reply(writer, id, json)?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileDelete(Routed { id, input: FileEntryInput { place_id, instance_id, file_id } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, true)?;
            let file=trash_file_entry(engine,settings,id,&place_id,&instance_id,&file_id)?;
            if let Ok(audience)=place_audience(engine,id,&place_id) {
                publish_durable_event(engine,audience,"file.deleted",serde_json::json!({
                    "placeId":place_id,"instanceId":instance_id,"fileId":file_id,
                }));
            }
            reply(writer, id, serde_json::json!({"trashed":true,"file":file}))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileTrashList(Routed { id, input: FileScopeInput { place_id, instance_id } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, false)?;
            let entries=list_trashed_file_entries(engine,id,&place_id,&instance_id)?;
            reply(writer, id, serde_json::json!({"entries":entries}))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileRestore(Routed { id, input: FileEntryInput { place_id, instance_id, file_id } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, true)?;
            let file=restore_file_entry(engine,settings,id,&place_id,&instance_id,&file_id)?;
            if let Ok(audience)=place_audience(engine,id,&place_id) {
                publish_durable_event(engine,audience,"file.restored",serde_json::json!({
                    "placeId":place_id,"instanceId":instance_id,"fileId":file_id,
                }));
            }
            reply(writer, id, serde_json::json!({"restored":true,"file":file}))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileDeletePermanent(Routed { id, input: FileEntryInput { place_id, instance_id, file_id } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, true)?;
            if !file_is_trashed(engine,id,&place_id,&instance_id,&file_id).unwrap_or(false){
                reject!(id,"file.not_trashed","Only trashed entries may be permanently deleted");
            }
            permanently_delete_file_tree(engine,settings,id,&place_id,&instance_id,&file_id)?;
            reply(writer, id, serde_json::json!({"deleted":true,"fileId":file_id}))?;
        }
        #[cfg(feature = "files")]
        RoutedOperation::FileTrashEmpty(Routed { id, input: FileScopeInput { place_id, instance_id } }) => {
            ensure_file_access(engine, delegation, principal, id, &place_id, &instance_id, true)?;
            let entries=list_trashed_file_entries(engine,id,&place_id,&instance_id)?;
            let mut deleted=0u64;
            for entry in entries {
                let Some(file_id)=entry.str_field("fileId") else {continue;};
                permanently_delete_file_tree(engine,settings,id,&place_id,&instance_id,file_id)?;
                deleted=deleted.saturating_add(1);
            }
            reply(writer, id, serde_json::json!({"emptied":true,"deleted":deleted}))?;
        }
        other => return diverged(other.id()),
    }
    Ok(())
}

/// The Files sync status: entry counts, and the evidence behind every entry not synced.
#[cfg(feature = "files")]
fn file_sync_status(settings: &ConnectionSettings, runtime: &FileSyncRuntime) -> io::Result<JsonValue> {
    let index = FileSyncIndex::load(&runtime.index_path)?.unwrap_or_default();
    let mut synced = 0u64;
    let mut pending_upload = 0u64;
    let mut pending_download = 0u64;
    let mut conflicts = 0u64;
    let mut ignored = 0u64;
    let mut errors = 0u64;
    let mut diagnostics = Vec::<JsonValue>::new();
    let mut reasons = HashMap::<String, u64>::new();
    for entry in &index.entries {
        match entry.state {
            FileSyncEntryState::Synced => synced += 1,
            FileSyncEntryState::PendingUpload => pending_upload += 1,
            FileSyncEntryState::PendingDownload => pending_download += 1,
            FileSyncEntryState::Conflict => conflicts += 1,
            FileSyncEntryState::Ignored => ignored += 1,
            FileSyncEntryState::Error => errors += 1,
        }

        // Keep normal status compact for healthy entries, but expose
        // enough evidence through ogcli to diagnose every non-synced
        // decision without enabling verbose daemon logging.
        if entry.state != FileSyncEntryState::Synced {
            let reason = entry.last_error.clone().unwrap_or_else(|| "state has no recorded reason".to_owned());
            *reasons.entry(reason.clone()).or_insert(0) += 1;
            let local_path = runtime.config.as_ref().map(|config| config.root.join(&entry.materialized_path));
            let local = local_path.as_ref().map(|path| {
                match fs::symlink_metadata(path) {
                    Ok(metadata) => {
                        let kind = if metadata.file_type().is_symlink() {
                            "symlink"
                        } else if metadata.is_dir() {
                            "directory"
                        } else if metadata.is_file() {
                            "file"
                        } else {
                            "other"
                        };
                        let observed_modified_at = metadata.modified().ok().and_then(system_time_millis);
                        let observed_size = metadata.is_file().then_some(metadata.len());
                        serde_json::json!({
                            "exists": true,
                            "kind": kind,
                            "size": observed_size,
                            "modifiedAt": observed_modified_at,
                            "matchesIndex": file_sync_local_matches(path, entry),
                        })
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => serde_json::json!({
                        "exists": false,
                        "matchesIndex": false,
                    }),
                    Err(error) => serde_json::json!({
                        "exists": null,
                        "matchesIndex": false,
                        "statError": error.to_string(),
                    }),
                }
            }).unwrap_or_else(|| serde_json::json!({"exists": null, "matchesIndex": false}));

            diagnostics.push(serde_json::json!({
                "state": entry.state,
                "reason": reason,
                "placeId": entry.place_id,
                "instanceId": entry.app_instance_id,
                "fileId": entry.file_id,
                "path": entry.materialized_path,
                "kind": entry.kind,
                "base": {
                    "remoteEtag": entry.remote_etag,
                    "remoteModifiedAt": entry.remote_modified_at,
                    "localSize": entry.local_size,
                    "localModifiedAt": entry.local_modified_at,
                    "localFingerprint": entry.local_fingerprint,
                },
                "local": local,
            }));
        }
    }
    diagnostics.sort_by(|left, right| {
        left.str_field("path").unwrap_or_default()
            .cmp(right.str_field("path").unwrap_or_default())
    });
    let mut reason_counts = reasons.into_iter()
        .map(|(reason, count)| serde_json::json!({"reason": reason, "count": count}))
        .collect::<Vec<_>>();
    reason_counts.sort_by(|left, right| {
        std::cmp::Reverse(left.u64_field("count").unwrap_or_default())
            .cmp(&std::cmp::Reverse(right.u64_field("count").unwrap_or_default()))
            .then_with(|| left.str_field("reason").unwrap_or_default()
                .cmp(right.str_field("reason").unwrap_or_default()))
    });
    Ok(serde_json::json!({
        "configured": runtime.config.is_some(),
        "root": runtime.config.as_ref().map(|config| &config.root),
        "selections": runtime.config.as_ref().map(|config| &config.selections).cloned().unwrap_or_default(),
        "worker": "continuous",
        // worker_running means the background worker thread is alive,
        // not that a reconciliation is currently executing. Expose
        // both concepts explicitly so Desktop can render Syncing only
        // while the reconciler is actually applying a run.
        "workerRunning": runtime.worker_running,
        "running": settings.file_sync_applying.load(Ordering::Acquire),
        "remoteEvents": runtime.remote_events.clone(),
        "localWatcher": runtime.local_watcher.clone(),
        "pending": runtime.worker_pending,
        "lastTrigger": runtime.last_trigger.clone(),
        "lastRunAt": runtime.last_run_at,
        "lastRunError": runtime.last_run_error.clone(),
        "entries": {
            "total": index.entries.len(),
            "synced": synced,
            "pendingUpload": pending_upload,
            "pendingDownload": pending_download,
            "conflicts": conflicts,
            "ignored": ignored,
            "errors": errors,
        },
        "diagnostics": {
            "reasonCounts": reason_counts,
            "problemEntries": diagnostics,
        }
    }))
}

pub(super) fn file_scope_component(value:&str)->String{
    crate::helpers::hex(value.as_bytes())
}


pub(super) fn resolve_app_table_collection(app:&JsonValue, table:&str)->Option<String>{
    let definition=app.get("definition")?; let model=definition.get("model")?; let table_def=model.get("tables")?.get(table)?; let collection_alias=table_def.get("collection")?.as_str()?; let declaration=model.get("collections")?.get(collection_alias)?;
    declaration.as_str().or_else(||declaration.str_field("name")).map(str::to_owned)
}

pub(super) fn run_data_worker_for_path(id:RequestId,path:&Path,operation:&str,mapping:Option<&JsonValue>)->Result<JsonValue,QueryResponse>{
    let python=env::var("OG_DATA_WORKER_PYTHON").unwrap_or_else(|_|"python3".to_owned());
    let module=env::var("OG_DATA_WORKER_MODULE").unwrap_or_else(|_|"openglacier.data_worker".to_owned());
    let mut child=Command::new(python).args(["-m",module.as_str()]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(|e|QueryResponse::request_error(id,"worker.unavailable",format!("cannot start data worker: {e}")))?;
    let request=serde_json::json!({"operation":operation,"file":path.to_string_lossy(),"mapping":mapping});
    if let Some(mut stdin)=child.stdin.take(){serde_json::to_writer(&mut stdin,&request).map_err(|e|QueryResponse::request_error(id,"worker.protocol_error",e.to_string()))?;}
    let output=child.wait_with_output().map_err(|e|QueryResponse::request_error(id,"worker.failed",e.to_string()))?;
    let value:JsonValue=serde_json::from_slice(&output.stdout).map_err(|e|QueryResponse::request_error(id,"worker.protocol_error",format!("invalid worker JSON: {e}; stderr={}",String::from_utf8_lossy(&output.stderr))))?;
    if !output.status.success() || value.str_field("status")==Some("error"){return Err(QueryResponse::request_error(id,"worker.failed",value.get("error").and_then(|v|v.get("message")).and_then(JsonValue::as_str).unwrap_or("data worker failed")));}
    Ok(value)
}

pub(super) fn run_data_worker_for_file(settings:&ConnectionSettings,engine:&Engine,id:RequestId,place_id:&str,files_instance_id:&str,file_id:&str,operation:&str,mapping:Option<&JsonValue>)->Result<JsonValue,QueryResponse>{
    let _context=resolve_file_context(engine,id,&Principal::Anonymous,place_id,files_instance_id,false).ok(); // actual caller authorization is checked before this helper
    let store=scoped_native_file_store(settings,id,place_id,files_instance_id)?; let entry=load_file_entry(engine,id,place_id,files_instance_id,file_id)?;
    let mut source=store.read(&entry.remote_id,None).map_err(|e|QueryResponse::request_error(id,"file.store_error",e.to_string()))?;
    let extension=Path::new(&entry.name).extension().and_then(|v|v.to_str()).unwrap_or("dat"); let temp=env::temp_dir().join(format!("og-data-{}-{}.{}",std::process::id(),unix_time_millis(),extension));
    {let mut out=fs::File::create(&temp).map_err(|e|QueryResponse::request_error(id,"worker.io_error",e.to_string()))?; io::copy(&mut source,&mut out).map_err(|e|QueryResponse::request_error(id,"worker.io_error",e.to_string()))?;}
    let result=run_data_worker_for_path(id,&temp,operation,mapping);
    let _=fs::remove_file(&temp);
    result
}

pub(super) fn scoped_native_file_store( settings:&ConnectionSettings, id:RequestId, place_id:&str, instance_id:&str, )->Result<NativeFileStore,QueryResponse>{
    let root=settings.files_path
        .join(file_scope_component(place_id))
        .join(file_scope_component(instance_id));
    NativeFileStore::new(StoreId::from(DEFAULT_FILES_STORE_ID),root)
        .map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))
}

pub(super) fn scoped_native_version_store( settings:&ConnectionSettings, id:RequestId, place_id:&str, instance_id:&str, )->Result<NativeFileStore,QueryResponse>{
    let root=settings.files_path
        .join(".versions")
        .join(file_scope_component(place_id))
        .join(file_scope_component(instance_id));
    NativeFileStore::new(StoreId::from(DEFAULT_FILES_STORE_ID),root)
        .map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))
}

pub(super) static NEXT_FILE_VERSION_ID: AtomicU64 = AtomicU64::new(1);

pub(super) fn file_version_json( version_id:&str,file:&FileEntry,remote_id:&str,metadata:&crate::files::FileMetadata,created_at:u64 )->JsonValue{
    serde_json::json!({
        "versionId":version_id,
        "fileId":file.file_id.as_str(),
        "storeId":file.store_id.as_str(),
        "remoteId":remote_id,
        "name":file.name,
        "size":metadata.size,
        "contentType":metadata.content_type,
        "etag":metadata.etag,
        "createdAt":created_at,
        "_place":file.place_id,
        "_app_instance":file.app_instance_id,
    })
}

#[cfg(feature = "files")]
pub(super) fn list_file_versions( engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str )->Result<Vec<JsonValue>,QueryResponse>{
    let query=format!( "on _file_versions | where _place == {} and _app_instance == {} and fileId == {} | sort createdAt desc", query_string(place_id),query_string(instance_id),query_string(file_id) );
    match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{documents,..}=>Ok(documents),error@QueryResponse::Error{..}=>Err(error)}
}

#[cfg(feature = "files")]
pub(super) fn load_file_version( engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str,version_id:&str )->Result<JsonValue,QueryResponse>{
    let query=format!( "on _file_versions | where _place == {} and _app_instance == {} and fileId == {} and versionId == {} | limit 1", query_string(place_id),query_string(instance_id),query_string(file_id),query_string(version_id) );
    match execute_request(engine,QueryRequest::new(id,query)){
        QueryResponse::Ok{documents,..}=>documents.into_iter().next().ok_or_else(||QueryResponse::request_error(id,"file.version_not_found","File version was not found")),
        error@QueryResponse::Error{..}=>Err(error),
    }
}

#[cfg(feature = "files")]
pub(super) fn delete_file_version_metadata( engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str,version_id:&str )->Result<(),QueryResponse>{
    let query=format!( "on _file_versions | where _place == {} and _app_instance == {} and fileId == {} and versionId == {} | delete", query_string(place_id),query_string(instance_id),query_string(file_id),query_string(version_id) );
    match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{..}=>Ok(()),error@QueryResponse::Error{..}=>Err(error)}
}

pub(super) fn archive_current_file( engine:&Engine,settings:&ConnectionSettings,id:RequestId,file:&FileEntry )->Result<JsonValue,QueryResponse>{
    let source=scoped_native_file_store(settings,id,&file.place_id,&file.app_instance_id)?;
    let versions=scoped_native_version_store(settings,id,&file.place_id,&file.app_instance_id)?;
    let created_at=unix_time_millis();
    let sequence=NEXT_FILE_VERSION_ID.fetch_add(1,Ordering::Relaxed);
    let version_id=format!("version-{created_at}-{sequence}");
    let version_name=version_id.clone();
    let mut reader=source.read(&file.remote_id,None).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    let mut stored=versions.write(
        FileWrite{remote_id:None,parent_remote_id:None,name:&version_name,content_type:file.metadata.content_type.as_deref(),size:file.metadata.size},
        &mut reader
    ).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    stored.metadata.content_type=file.metadata.content_type.clone();
    let json=file_version_json(&version_id,file,&stored.remote_id,&stored.metadata,created_at);
    let query=format!("on _file_versions | insert {}",serde_json::to_string(&json).expect("file version serializes"));
    match execute_request(engine,QueryRequest::new(id,query)){
        QueryResponse::Ok{..}=>Ok(json),
        error@QueryResponse::Error{..}=>{let _=versions.delete(&stored.remote_id);Err(error)}
    }
}

pub(super) static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(1);

pub(super) fn resolve_file_context( engine:&Engine, id:RequestId, principal:&Principal, place_id:&str, instance_id:&str, write:bool )->Result<ExecutionContext,QueryResponse>{
    let context=resolve_query_execution_context(engine,id,principal,RequestedExecutionContext{place_id:place_id.to_owned(),app_instance_id:Some(instance_id.to_owned())})?;
    if write && !context.access.can_write(){
        return Err(QueryResponse::request_error(id,"authorization.denied","Place access is read-only for Files"));
    }
    Ok(context)
}

pub(super) fn file_entry_json(entry:&FileEntry)->JsonValue{
    let mut value=serde_json::json!({
        "fileId":entry.file_id.as_str(),"storeId":entry.store_id.as_str(),"remoteId":entry.remote_id,
         "name":entry.name,"kind":entry.kind.as_str(),"_place":entry.place_id,"_app_instance":entry.app_instance_id,
        "parentId":entry.parent_id.as_ref().map(|parent|parent.as_str()),
    });
    let object=value.as_object_mut().expect("file entry object");
    if let Some(size)=entry.metadata.size{object.insert("size".into(),JsonValue::from(size));}
    if let Some(v)=&entry.metadata.content_type{object.insert("contentType".into(),JsonValue::String(v.clone()));}
    if let Some(v)=&entry.metadata.etag{object.insert("etag".into(),JsonValue::String(v.clone()));}
    if let Some(v)=entry.metadata.created_at{object.insert("createdAt".into(),JsonValue::from(v));}
    if let Some(v)=entry.metadata.modified_at{object.insert("modifiedAt".into(),JsonValue::from(v));}
    value
}

pub(super) fn json_to_file_entry(value:&JsonValue)->Result<FileEntry,&'static str>{
    let object=value.as_object().ok_or("file metadata is not an object")?;
    let string=|key| object.get(key).and_then(JsonValue::as_str).map(str::to_owned);
    let kind=match string("kind").as_deref(){Some("file")=>crate::files::FileKind::File,Some("directory")=>crate::files::FileKind::Directory,_=>return Err("invalid file kind")};
    Ok(FileEntry{
        file_id:FileId::from(string("fileId").ok_or("missing fileId")?),
        store_id:StoreId::from(string("storeId").ok_or("missing storeId")?),
        remote_id:string("remoteId").ok_or("missing remoteId")?,
        parent_id:string("parentId").map(FileId::from),
        name:string("name").ok_or("missing name")?,kind,
        metadata:crate::files::FileMetadata{size:object.u64_field("size"),content_type:string("contentType"),etag:string("etag"),created_at:object.u64_field("createdAt"),modified_at:object.u64_field("modifiedAt")},
        place_id:string("_place").ok_or("missing _place")?,app_instance_id:string("_app_instance").ok_or("missing _app_instance")?,
    })
}

pub(super) fn load_file_entry_json(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str)->Result<JsonValue,QueryResponse>{
    let query=format!("on _files | where _place == {} and _app_instance == {} and fileId == {} | limit 1",query_string(place_id),query_string(instance_id),query_string(file_id));
    match execute_request(engine,QueryRequest::new(id,query)){
        QueryResponse::Ok{documents,..}=>documents.into_iter().next().ok_or_else(||QueryResponse::request_error(id,"file.not_found","File entry was not found")),
        error@QueryResponse::Error{..}=>Err(error),
    }
}
pub(super) fn load_file_entry(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str)->Result<FileEntry,QueryResponse>{
    let json=load_file_entry_json(engine,id,place_id,instance_id,file_id)?;
    json_to_file_entry(&json).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))
}
pub(super) fn list_file_children_raw(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,parent_id:Option<&str>)->Result<Vec<JsonValue>,QueryResponse>{
    let parent=parent_id.map_or_else(||"parentId == null".to_owned(),|value|format!("parentId == {}",query_string(value)));
    let query=format!("on _files | where _place == {} and _app_instance == {} and {parent} | sort name",query_string(place_id),query_string(instance_id));
    match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{documents,..}=>Ok(documents),error@QueryResponse::Error{..}=>Err(error)}
}

pub(super) fn list_file_entries(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,parent_id:Option<&str>)->Result<Vec<JsonValue>,QueryResponse>{
    Ok(list_file_children_raw(engine,id,place_id,instance_id,parent_id)?
        .into_iter()
        .filter(|document|document.bool_field("trashed")!=Some(true))
        .collect())
}

#[cfg(feature = "files")]
pub(super) fn list_trashed_file_entries(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str)->Result<Vec<JsonValue>,QueryResponse>{
    let query=format!("on _files | where _place == {} and _app_instance == {}",query_string(place_id),query_string(instance_id));
    match execute_request(engine,QueryRequest::new(id,query)){
        QueryResponse::Ok{documents,..}=>{
            let trashed_ids=documents.iter()
                .filter(|document|document.bool_field("trashed")==Some(true))
                .filter_map(|document|document.str_field("fileId"))
                .map(str::to_owned)
                .collect::<std::collections::BTreeSet<_>>();
            let mut entries=documents.into_iter()
                .filter(|document|{
                    if document.bool_field("trashed")!=Some(true){return false;}
                    match document.str_field("parentId"){
                        Some(parent)=>!trashed_ids.contains(parent),
                        None=>true,
                    }
                })
                .collect::<Vec<_>>();
            entries.sort_by_key(|document|std::cmp::Reverse(document.u64_field("trashedAt").unwrap_or(0)));
            Ok(entries)
        }
        error@QueryResponse::Error{..}=>Err(error),
    }
}
pub(super) fn file_entry_from_store(place_id:&str,instance_id:&str,parent_id:Option<&str>,stored:FileStoreEntry)->FileEntry{
    let sequence=NEXT_FILE_ID.fetch_add(1,Ordering::Relaxed);
    FileEntry{file_id:FileId::from(format!("file-{}-{sequence}",unix_time_millis())),store_id:StoreId::from(DEFAULT_FILES_STORE_ID),remote_id:stored.remote_id,parent_id:parent_id.map(FileId::from),name:stored.name,kind:stored.kind,metadata:stored.metadata,place_id:place_id.to_owned(),app_instance_id:instance_id.to_owned()}
}
pub(super) fn persist_file_entry(engine:&Engine,id:RequestId,entry:&FileEntry)->Result<JsonValue,QueryResponse>{
    let json=file_entry_json(entry);
    let query=format!("on _files | insert {}",serde_json::to_string(&json).expect("file metadata serializes"));
    match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{..}=>Ok(json),error@QueryResponse::Error{..}=>Err(error)}
}
pub(super) fn replace_file_entry(engine:&Engine,id:RequestId,entry:&FileEntry)->Result<JsonValue,QueryResponse>{
    let delete=format!("on _files | where _place == {} and _app_instance == {} and fileId == {} | delete",query_string(&entry.place_id),query_string(&entry.app_instance_id),query_string(entry.file_id.as_str()));
    match execute_request(engine,QueryRequest::new(id,delete)){QueryResponse::Ok{..}=>persist_file_entry(engine,id,entry),error@QueryResponse::Error{..}=>Err(error)}
}
#[cfg(feature = "files")]
pub(super) fn delete_file_metadata(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str)->Result<(),QueryResponse>{
    let query=format!("on _files | where _place == {} and _app_instance == {} and fileId == {} | delete",query_string(place_id),query_string(instance_id),query_string(file_id));
    match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{..}=>Ok(()),error@QueryResponse::Error{..}=>Err(error)}
}

pub(super) fn replace_file_json(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str,json:&JsonValue)->Result<JsonValue,QueryResponse>{
    let delete=format!("on _files | where _place == {} and _app_instance == {} and fileId == {} | delete",query_string(place_id),query_string(instance_id),query_string(file_id));
    match execute_request(engine,QueryRequest::new(id,delete)){
        QueryResponse::Ok{..}=>{
            let query=format!("on _files | insert {}",serde_json::to_string(json).expect("file metadata serializes"));
            match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{..}=>Ok(json.clone()),error@QueryResponse::Error{..}=>Err(error)}
        }
        error@QueryResponse::Error{..}=>Err(error),
    }
}

pub(super) const INTERNAL_TRASH_DIR: &str = ".og-trash";

pub(super) fn file_store_query_error(id:RequestId,error:FileStoreError)->QueryResponse{
    let code=match error{
        FileStoreError::NotFound=>"file.not_found",
        FileStoreError::AlreadyExists=>"file.already_exists",
        FileStoreError::InvalidName|FileStoreError::InvalidRemoteId=>"file.invalid",
        FileStoreError::NotDirectory=>"file.not_directory",
        FileStoreError::NotFile=>"file.not_file",
        FileStoreError::InvalidRange=>"file.invalid_range",
        FileStoreError::Unsupported(_)=>"file.unsupported",
        _=>"file.store_error",
    };
    QueryResponse::request_error(id,code,error.to_string())
}

pub(super) fn ensure_file_trash_root(store:&dyn FileStore,id:RequestId)->Result<String,QueryResponse>{
    let entries=store.list(None).map_err(|error|file_store_query_error(id,error))?;
    if let Some(entry)=entries.into_iter().find(|entry|entry.name==INTERNAL_TRASH_DIR){
        if entry.kind!=crate::files::FileKind::Directory{
            return Err(QueryResponse::request_error(id,"file.trash_invalid","internal trash path is not a directory"));
        }
        return Ok(entry.remote_id);
    }
    store.mkdir(None,INTERNAL_TRASH_DIR)
        .map(|entry|entry.remote_id)
        .map_err(|error|file_store_query_error(id,error))
}

pub(super) fn trash_file_entry(engine:&Engine,settings:&ConnectionSettings,id:RequestId,place_id:&str,instance_id:&str,file_id:&str)->Result<JsonValue,QueryResponse>{
    let mut json=load_file_entry_json(engine,id,place_id,instance_id,file_id)?;
    if json.bool_field("trashed")==Some(true){return Ok(json);}
    let entry=json_to_file_entry(&json).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))?;
    let store=scoped_native_file_store(settings,id,place_id,instance_id)?;
    let trash_root=ensure_file_trash_root(&store,id)?;
    let stored=store.move_entry(&entry.remote_id,Some(&trash_root),entry.file_id.as_str())
        .map_err(|error|file_store_query_error(id,error))?;

    let parent=json.get("parentId").cloned().unwrap_or(JsonValue::Null);
    let object=json.as_object_mut().ok_or_else(||QueryResponse::request_error(id,"file.invalid_record","File metadata is not an object"))?;
    object.insert("remoteId".to_owned(),JsonValue::String(stored.remote_id.clone()));
    object.insert("trashed".to_owned(),JsonValue::Bool(true));
    object.insert("trashedAt".to_owned(),JsonValue::from(unix_time_millis()));
    object.insert("trashedParentId".to_owned(),parent);
    if let Some(etag)=stored.metadata.etag.clone(){object.insert("etag".to_owned(),JsonValue::String(etag));}
    if let Some(modified)=stored.metadata.modified_at{object.insert("modifiedAt".to_owned(),JsonValue::from(modified));}

    let persisted=replace_file_json(engine,id,place_id,instance_id,file_id,&json)?;
    if entry.kind==crate::files::FileKind::Directory{
        let moved=json_to_file_entry(&persisted).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))?;
        sync_moved_children(engine,id,&store,place_id,instance_id,&moved)?;
    }
    Ok(persisted)
}

#[cfg(feature = "files")]
pub(super) fn file_is_trashed(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str)->Result<bool,QueryResponse>{
    Ok(load_file_entry_json(engine,id,place_id,instance_id,file_id)?.bool_field("trashed")==Some(true))
}

#[cfg(feature = "files")]
pub(super) fn restore_file_entry( engine:&Engine,settings:&ConnectionSettings,id:RequestId,place_id:&str,instance_id:&str,file_id:&str )->Result<JsonValue,QueryResponse>{
    let mut json=load_file_entry_json(engine,id,place_id,instance_id,file_id)?;
    if json.bool_field("trashed")!=Some(true){
        return Err(QueryResponse::request_error(id,"file.not_trashed","File entry is not in the trash"));
    }

    let entry=json_to_file_entry(&json).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))?;
    let old_parent=json.str_field("trashedParentId").map(str::to_owned);
    let parent=match old_parent.as_deref(){
        Some(parent_id)=>load_file_entry(engine,id,place_id,instance_id,parent_id)
            .ok()
            .filter(|_|load_file_entry_json(engine,id,place_id,instance_id,parent_id)
                .ok()
                .and_then(|parent|parent.bool_field("trashed"))!=Some(true)),
        None=>None,
    };
    let target_parent_id=parent.as_ref().map(|parent|parent.file_id.as_str().to_owned());
    let target_parent_remote=parent.as_ref().map(|parent|parent.remote_id.as_str());
    let store=scoped_native_file_store(settings,id,place_id,instance_id)?;
    let stored=store.move_entry(&entry.remote_id,target_parent_remote,&entry.name)
        .map_err(|error|file_store_query_error(id,error))?;

    if let Some(object)=json.as_object_mut(){
        object.insert("remoteId".to_owned(),JsonValue::String(stored.remote_id.clone()));
        object.insert("parentId".to_owned(),target_parent_id.map(JsonValue::String).unwrap_or(JsonValue::Null));
        object.insert("trashed".to_owned(),JsonValue::Bool(false));
        object.remove("trashedAt");
        object.remove("trashedParentId");
        if let Some(etag)=stored.metadata.etag.clone(){object.insert("etag".to_owned(),JsonValue::String(etag));}
        if let Some(modified)=stored.metadata.modified_at{object.insert("modifiedAt".to_owned(),JsonValue::from(modified));}
    }
    let persisted=replace_file_json(engine,id,place_id,instance_id,file_id,&json)?;
    if entry.kind==crate::files::FileKind::Directory{
        let moved=json_to_file_entry(&persisted).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))?;
        sync_moved_children(engine,id,&store,place_id,instance_id,&moved)?;
    }
    Ok(persisted)
}

#[cfg(feature = "files")]
pub(super) fn purge_file_versions( engine:&Engine,settings:&ConnectionSettings,id:RequestId,place_id:&str,instance_id:&str,file_id:&str )->Result<(),QueryResponse>{
    let versions=list_file_versions(engine,id,place_id,instance_id,file_id)?;
    if versions.is_empty(){return Ok(());}
    let store=scoped_native_version_store(settings,id,place_id,instance_id)?;
    for version in versions {
        if let Some(remote_id)=version.str_field("remoteId"){
            let _=store.delete(remote_id);
        }
        if let Some(version_id)=version.str_field("versionId"){
            delete_file_version_metadata(engine,id,place_id,instance_id,file_id,version_id)?;
        }
    }
    Ok(())
}

#[cfg(feature = "files")]
pub(super) fn permanently_delete_file_tree( engine:&Engine,settings:&ConnectionSettings,id:RequestId,place_id:&str,instance_id:&str,file_id:&str )->Result<(),QueryResponse>{
    let entry=load_file_entry(engine,id,place_id,instance_id,file_id)?;
    for child in child_file_entries(engine,id,place_id,instance_id,file_id)? {
        permanently_delete_file_tree(engine,settings,id,place_id,instance_id,child.file_id.as_str())?;
    }
    purge_file_versions(engine,settings,id,place_id,instance_id,file_id)?;
    let store=scoped_native_file_store(settings,id,place_id,instance_id)?;
    if entry.kind==crate::files::FileKind::Directory {
        // Children were removed first; delete handles the now-empty tree too.
        store.delete(&entry.remote_id).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    } else {
        store.delete(&entry.remote_id).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    }
    delete_file_metadata(engine,id,place_id,instance_id,file_id)
}

#[cfg(feature = "files")]
pub(super) fn child_file_entries(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,parent_id:&str)->Result<Vec<FileEntry>,QueryResponse>{
    list_file_children_raw(engine,id,place_id,instance_id,Some(parent_id))?.into_iter().map(|value|json_to_file_entry(&value).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))).collect()
}
#[cfg(feature = "files")]
pub(super) fn subtree_has_trashed_entries(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,parent_id:&str)->Result<bool,QueryResponse>{
    for child in list_file_children_raw(engine,id,place_id,instance_id,Some(parent_id))? {
        if child.bool_field("trashed")==Some(true){return Ok(true);}
        if child.str_field("kind")==Some("directory") {
            if let Some(file_id)=child.str_field("fileId") {
                if subtree_has_trashed_entries(engine,id,place_id,instance_id,file_id)?{return Ok(true);}
            }
        }
    }
    Ok(false)
}

pub(super) fn sync_moved_children(engine:&Engine,id:RequestId,store:&dyn FileStore,place_id:&str,instance_id:&str,parent:&FileEntry)->Result<(),QueryResponse>{
    let persisted=list_file_children_raw(engine,id,place_id,instance_id,Some(parent.file_id.as_str()))?;
    let remote=store.list(Some(&parent.remote_id)).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    for mut json in persisted {
        let child=json_to_file_entry(&json).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))?;
        let Some(found)=remote.iter().find(|candidate|candidate.name==child.name) else {
            return Err(QueryResponse::request_error(id,"file.store_mismatch","provider child is missing after move"));
        };
        if let Some(object)=json.as_object_mut(){
            object.insert("remoteId".to_owned(),JsonValue::String(found.remote_id.clone()));
            if let Some(etag)=&found.metadata.etag{object.insert("etag".to_owned(),JsonValue::String(etag.clone()));}
            if let Some(modified)=found.metadata.modified_at{object.insert("modifiedAt".to_owned(),JsonValue::from(modified));}
        }
        replace_file_json(engine,id,place_id,instance_id,child.file_id.as_str(),&json)?;
        if child.kind==crate::files::FileKind::Directory{
            let updated=FileEntry{
                file_id:child.file_id,store_id:child.store_id,remote_id:found.remote_id.clone(),
                parent_id:child.parent_id,name:child.name,kind:child.kind,metadata:found.metadata.clone(),
                place_id:place_id.to_owned(),app_instance_id:instance_id.to_owned()
            };
            sync_moved_children(engine,id,store,place_id,instance_id,&updated)?;
        }
    }
    Ok(())
}
#[cfg(feature = "files")]
pub(super) fn persist_copied_children(engine:&Engine,id:RequestId,store:&dyn FileStore,place_id:&str,instance_id:&str,parent:&FileEntry)->Result<(),QueryResponse>{
    let remote=store.list(Some(&parent.remote_id)).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    for stored in remote {
        let child=file_entry_from_store(place_id,instance_id,Some(parent.file_id.as_str()),stored);
        persist_file_entry(engine,id,&child)?;
        if child.kind==crate::files::FileKind::Directory{persist_copied_children(engine,id,store,place_id,instance_id,&child)?;}
    }
    Ok(())
}
