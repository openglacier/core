//! Local Files sync: materialize remote Files under a root and push local changes back.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;

#[derive(Clone, Debug)]
pub(super) struct FileSyncAppProjection {
    pub(super) name: String,
    pub(super) place_root_projection: bool,
}

#[derive(Clone, Debug)]
pub(super) struct FileSyncInstanceProjection {
    pub(super) app_id: String,
    pub(super) name: String,
}

pub(super) struct FileSyncSource<'a> {
    pub(super) settings: &'a ConnectionSettings,
    pub(super) engine: &'a Engine,
    pub(super) gateway: Option<NodeWebSocket>,
    pub(super) next_request_id: u64,
}

impl<'a> FileSyncSource<'a> {
    pub(super) fn connect(settings: &'a ConnectionSettings, engine: &'a Engine) -> Result<Self, String> {
        let gateway = if has_upstream_gateway(settings) {
            let credential = settings.upstream_identity.as_ref()
                .ok_or_else(|| "Files sync needs the node Identity to read from the upstream Gateway".to_owned())?;
            let (websocket, _endpoint) = connect_authenticated_gateway_client_any(
                settings,
                credential,
                settings.read_timeout,
                settings.write_timeout,
            )?;
            Some(websocket)
        } else {
            None
        };
        Ok(Self { settings, engine, gateway, next_request_id: 10_000 })
    }

    pub(super) fn next_request_id(&mut self) -> RequestId {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        RequestId::Number(id)
    }

    pub(super) fn gateway_data(&mut self, op: &str, data: JsonValue) -> Result<JsonValue, String> {
        let id = self.next_request_id();
        let request = OperationRequest::new(id, op, data);
        let websocket = self.gateway.as_mut().ok_or_else(|| "upstream Gateway is not connected".to_owned())?;
        let response = gateway_response_for_request(websocket, &request)?;
        if response.str_field("status") == Some("error") || response.get("error").is_some() {
            let error = response.get("error").cloned().unwrap_or(JsonValue::Null);
            return Err(format!("{op} failed upstream: {error}"));
        }
        response.get("data").cloned().ok_or_else(|| format!("{op} returned no data"))
    }

    pub(super) fn place_name(&mut self, place_id: &str) -> Result<String, String> {
        if self.gateway.is_some() {
            let data = self.gateway_data("place.get", serde_json::json!({"placeId": place_id}))?;
            let place = data.get("place").unwrap_or(&data);
            return place.str_field("name").filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| format!("Place {place_id} has no visible name"));
        }
        let id = self.next_request_id();
        load_place(self.engine, id, place_id)
            .map(|place| place.name)
            .map_err(query_response_message)
    }

    pub(super) fn place_has_sync_assignment(&mut self, place_id: &str, identity_id: &str, device_id: &str) -> Result<bool, String> {
        let assignments = if self.gateway.is_some() {
            self.gateway_data("place.resource.list", serde_json::json!({"placeId": place_id}))?.array_field("assignments").cloned()
                .ok_or_else(|| format!("place.resource.list returned no assignments for Place {place_id}"))?
        } else {
            let id = self.next_request_id();
            load_place(self.engine, id, place_id).map_err(query_response_message)?.resource_assignments
        };
        Ok(assignments.iter().any(|entry| {
            let entry_identity = entry.get("identityId").or_else(|| entry.get("nodeIdentityId")).and_then(JsonValue::as_str);
            let entry_device = entry.get("deviceId").or_else(|| entry.get("nodeDeviceId")).or_else(|| entry.get("nodeId")).and_then(JsonValue::as_str);
            let capability = entry.str_field("capability");
            let storage_role = resource_assignment_storage_role(entry);
            entry_identity == Some(identity_id) && entry_device == Some(device_id) && capability == Some("files") && storage_role == Some("sync")
        }))
    }

    pub(super) fn sync_place_ids(&mut self, identity_id: &str, device_id: &str) -> Result<Vec<String>, String> {
        if self.gateway.is_some() {
            // place.list already carries the authoritative resourceAssignments.
            // Resolve every sync-enabled Place in one round-trip instead of one
            // place.resource.list request per Place on every reconciliation.
            let places = self.gateway_data("place.list", serde_json::json!({}))?.array_field("places").cloned()
                .ok_or_else(|| "place.list returned no places array".to_owned())?;
            let mut result = Vec::new();
            for place in places {
                let Some(place_id) = place.get("placeId").or_else(|| place.get("id")).and_then(JsonValue::as_str) else { continue; };
                let assignments = place.array_field("resourceAssignments");
                let sync = assignments.is_some_and(|assignments| assignments.iter().any(|entry| {
                    let entry_identity = entry.get("identityId").or_else(|| entry.get("nodeIdentityId")).and_then(JsonValue::as_str);
                    let entry_device = entry.get("deviceId").or_else(|| entry.get("nodeDeviceId")).or_else(|| entry.get("nodeId")).and_then(JsonValue::as_str);
                    let capability = entry.str_field("capability");
                    let storage_role = resource_assignment_storage_role(entry);
                    entry_identity == Some(identity_id)
                        && entry_device == Some(device_id)
                        && capability == Some("files")
                        && storage_role == Some("sync")
                }));
                if sync { result.push(place_id.to_owned()); }
            }
            return Ok(result);
        }

        // Continuous sync is currently daemon-owned only for connected nodes.
        // Keep standalone/manual behaviour scoped to explicit selections.
        Ok(Vec::new())
    }

    pub(super) fn apps(&mut self) -> Result<HashMap<String, FileSyncAppProjection>, String> {
        let documents = if self.gateway.is_some() {
            self.gateway_data("app.list", serde_json::json!({}))?.array_field("apps").cloned()
                .ok_or_else(|| "app.list returned no apps array".to_owned())?
        } else {
            let id = self.next_request_id();
            match execute_request(self.engine, QueryRequest::new(id, "on _apps | where state == \"active\" | sort name")) {
                QueryResponse::Ok { documents, .. } => documents,
                response => return Err(query_response_message(response)),
            }
        };
        let mut apps = HashMap::new();
        for document in documents {
            let Some(app_id) = document.str_field("appId") else { continue; };
            let name = document.str_field("name").filter(|value| !value.is_empty()).unwrap_or(app_id).to_owned();
            let place_root_projection = file_sync_app_place_root_projection(&document);
            apps.insert(app_id.to_owned(), FileSyncAppProjection { name, place_root_projection });
        }
        Ok(apps)
    }

    pub(super) fn instances(&mut self, place_id: &str) -> Result<HashMap<String, FileSyncInstanceProjection>, String> {
        let documents = if self.gateway.is_some() {
            self.gateway_data("app.instance.list", serde_json::json!({"placeId": place_id}))?.array_field("instances").cloned()
                .ok_or_else(|| format!("app.instance.list returned no instances array for Place {place_id}"))?
        } else {
            let id = self.next_request_id();
            let query = format!("on _app_instances | where placeId == {} and state == \"active\" | sort createdAt", query_string(place_id));
            match execute_request(self.engine, QueryRequest::new(id, query)) {
                QueryResponse::Ok { documents, .. } => documents,
                response => return Err(query_response_message(response)),
            }
        };
        let mut instances = HashMap::new();
        for document in documents {
            let Some(instance_id) = document.str_field("instanceId") else { continue; };
            let Some(app_id) = document.str_field("appId") else { continue; };
            let name = document.str_field("name").filter(|value| !value.is_empty()).unwrap_or("Main").to_owned();
            instances.insert(instance_id.to_owned(), FileSyncInstanceProjection { app_id: app_id.to_owned(), name });
        }
        Ok(instances)
    }

    pub(super) fn file_children(&mut self, place_id: &str, instance_id: &str, parent_id: Option<&str>) -> Result<Vec<FileEntry>, String> {
        let documents = if self.gateway.is_some() {
            let data = self.gateway_data("file.list", serde_json::json!({
                "placeId": place_id,
                "instanceId": instance_id,
                "parentId": parent_id,
            }))?;
            data.as_array().cloned().ok_or_else(|| "file.list returned no file array".to_owned())?
        } else {
            let id = self.next_request_id();
            list_file_entries(self.engine, id, place_id, instance_id, parent_id)
                .map_err(query_response_message)?
        };
        documents.into_iter().map(|document| {
            json_to_file_entry(&document).map_err(|message| message.to_owned())
        }).collect()
    }

    pub(super) fn file_tree(&mut self, place_id: &str, instance_id: &str) -> Result<Vec<FileEntry>, String> {
        let mut entries = Vec::new();
        let mut parents = vec![None::<String>];
        while let Some(parent_id) = parents.pop() {
            let children = self.file_children(place_id, instance_id, parent_id.as_deref())?;
            for child in &children {
                if child.kind == crate::files::FileKind::Directory {
                    parents.push(Some(child.file_id.as_str().to_owned()));
                }
            }
            entries.extend(children);
        }
        Ok(entries)
    }

    pub(super) fn read_file_to_path(&mut self, entry: &FileEntry, destination: &Path) -> Result<u64, String> {
        if self.gateway.is_some() {
            let id = self.next_request_id();
            let request = OperationRequest::new(id, "file.read", serde_json::json!({
                "placeId": entry.place_id,
                "instanceId": entry.app_instance_id,
                "fileId": entry.file_id.as_str(),
                "offset": 0,
            }));
            let websocket = self.gateway.as_mut().expect("checked above");
            return gateway_raw_read_to_path(websocket, &request, destination);
        }
        let id = self.next_request_id();
        let store = scoped_native_file_store(self.settings, id, &entry.place_id, &entry.app_instance_id)
            .map_err(query_response_message)?;
        let mut reader = store.read(&entry.remote_id, None).map_err(|error| error.to_string())?;
        atomic_sync_write(destination, &mut reader)
    }

    pub(super) fn write_existing_from_path(&mut self, entry: &FileEntry, source_path: &Path) -> Result<FileEntry, String> {
        let size = fs::metadata(source_path).map_err(|error| error.to_string())?.len();
        if self.gateway.is_some() {
            let id = self.next_request_id();
            let request = OperationRequest::new(id, "file.write", serde_json::json!({
                "placeId": entry.place_id,
                "instanceId": entry.app_instance_id,
                "fileId": entry.file_id.as_str(),
                "size": size,
            }));
            let websocket = self.gateway.as_mut().expect("checked above");
            let data = gateway_raw_write_from_path(websocket, &request, source_path)?;
            let file = data.get("file").ok_or_else(|| "file.write returned no file metadata".to_owned())?;
            return json_to_file_entry(file).map_err(str::to_owned);
        }

        let id = self.next_request_id();
        archive_current_file(self.engine, self.settings, id, entry).map_err(query_response_message)?;
        let store = scoped_native_file_store(self.settings, id, &entry.place_id, &entry.app_instance_id)
            .map_err(query_response_message)?;
        let mut input = fs::File::open(source_path).map_err(|error| error.to_string())?;
        let stored = store.write(
            FileWrite {
                remote_id: Some(&entry.remote_id),
                parent_remote_id: None,
                name: &entry.name,
                content_type: entry.metadata.content_type.as_deref(),
                size: Some(size),
            },
            &mut input,
        ).map_err(|error| error.to_string())?;
        let updated = FileEntry {
            file_id: entry.file_id.clone(),
            store_id: entry.store_id.clone(),
            remote_id: stored.remote_id,
            parent_id: entry.parent_id.clone(),
            name: entry.name.clone(),
            kind: stored.kind,
            metadata: stored.metadata,
            place_id: entry.place_id.clone(),
            app_instance_id: entry.app_instance_id.clone(),
        };
        replace_file_entry(self.engine, id, &updated).map_err(query_response_message)?;
        self.publish_file_event(id, "file.updated", &updated);
        Ok(updated)
    }

    pub(super) fn create_file_from_path(&mut self, place_id: &str, instance_id: &str, parent_id: Option<&str>, name: &str, source_path: &Path) -> Result<FileEntry, String> {
        let size = fs::metadata(source_path).map_err(|error| error.to_string())?.len();
        if self.gateway.is_some() {
            let id = self.next_request_id();
            let request = OperationRequest::new(id, "file.write", serde_json::json!({
                "placeId": place_id,
                "instanceId": instance_id,
                "parentId": parent_id,
                "name": name,
                "size": size,
            }));
            let websocket = self.gateway.as_mut().expect("checked above");
            let data = gateway_raw_write_from_path(websocket, &request, source_path)?;
            let file = data.get("file").ok_or_else(|| "file.write returned no file metadata".to_owned())?;
            return json_to_file_entry(file).map_err(str::to_owned);
        }

        let id = self.next_request_id();
        let store = scoped_native_file_store(self.settings, id, place_id, instance_id)
            .map_err(query_response_message)?;
        let parent_remote = parent_id
            .map(|parent| load_file_entry(self.engine, id, place_id, instance_id, parent))
            .transpose().map_err(query_response_message)?
            .map(|entry| entry.remote_id);
        let mut input = fs::File::open(source_path).map_err(|error| error.to_string())?;
        let stored = store.write(FileWrite {
            remote_id: None,
            parent_remote_id: parent_remote.as_deref(),
            name,
            content_type: None,
            size: Some(size),
        }, &mut input).map_err(|error| error.to_string())?;
        let entry = file_entry_from_store(place_id, instance_id, parent_id, stored);
        persist_file_entry(self.engine, id, &entry).map_err(query_response_message)?;
        self.publish_file_event(id, "file.created", &entry);
        Ok(entry)
    }

    pub(super) fn mkdir(&mut self, place_id: &str, instance_id: &str, parent_id: Option<&str>, name: &str) -> Result<FileEntry, String> {
        if self.gateway.is_some() {
            let data = self.gateway_data("file.mkdir", serde_json::json!({
                "placeId": place_id,
                "instanceId": instance_id,
                "parentId": parent_id,
                "name": name,
            }))?;
            return json_to_file_entry(&data).map_err(str::to_owned);
        }

        let id = self.next_request_id();
        let store = scoped_native_file_store(self.settings, id, place_id, instance_id)
            .map_err(query_response_message)?;
        let parent_remote = parent_id
            .map(|parent| load_file_entry(self.engine, id, place_id, instance_id, parent))
            .transpose().map_err(query_response_message)?
            .map(|entry| entry.remote_id);
        let stored = store.mkdir(parent_remote.as_deref(), name).map_err(|error| error.to_string())?;
        let entry = file_entry_from_store(place_id, instance_id, parent_id, stored);
        persist_file_entry(self.engine, id, &entry).map_err(query_response_message)?;
        self.publish_file_event(id, "file.created", &entry);
        Ok(entry)
    }

    pub(super) fn move_entry(&mut self, entry: &FileEntry, parent_id: Option<&str>, name: &str) -> Result<FileEntry, String> {
        if self.gateway.is_some() {
            let data = self.gateway_data("file.move", serde_json::json!({
                "placeId": entry.place_id,
                "instanceId": entry.app_instance_id,
                "fileId": entry.file_id.as_str(),
                "parentId": parent_id,
                "name": name,
            }))?;
            return json_to_file_entry(&data).map_err(str::to_owned);
        }

        let id = self.next_request_id();
        let store = scoped_native_file_store(self.settings, id, &entry.place_id, &entry.app_instance_id)
            .map_err(query_response_message)?;
        let parent_remote = parent_id
            .map(|parent| load_file_entry(self.engine, id, &entry.place_id, &entry.app_instance_id, parent))
            .transpose().map_err(query_response_message)?
            .map(|parent| parent.remote_id);
        let stored = store.move_entry(&entry.remote_id, parent_remote.as_deref(), name).map_err(|error| error.to_string())?;
        let updated = FileEntry {
            file_id: entry.file_id.clone(),
            store_id: entry.store_id.clone(),
            remote_id: stored.remote_id,
            parent_id: parent_id.map(FileId::from),
            name: stored.name,
            kind: stored.kind,
            metadata: stored.metadata,
            place_id: entry.place_id.clone(),
            app_instance_id: entry.app_instance_id.clone(),
        };
        replace_file_entry(self.engine, id, &updated).map_err(query_response_message)?;
        if updated.kind == crate::files::FileKind::Directory {
            sync_moved_children(self.engine, id, &store, &entry.place_id, &entry.app_instance_id, &updated)
                .map_err(query_response_message)?;
        }
        self.publish_file_event(id, "file.moved", &updated);
        Ok(updated)
    }

    pub(super) fn delete_entry(&mut self, entry: &FileEntry) -> Result<(), String> {
        if self.gateway.is_some() {
            self.gateway_data("file.delete", serde_json::json!({
                "placeId": entry.place_id,
                "instanceId": entry.app_instance_id,
                "fileId": entry.file_id.as_str(),
            }))?;
            return Ok(());
        }
        let id = self.next_request_id();
        trash_file_entry(self.engine, self.settings, id, &entry.place_id, &entry.app_instance_id, entry.file_id.as_str())
            .map_err(query_response_message)?;
        self.publish_file_event(id, "file.deleted", entry);
        Ok(())
    }

    pub(super) fn publish_file_event(&self, id: RequestId, event_type: &str, entry: &FileEntry) {
        if let Ok(audience) = place_audience(self.engine, id, &entry.place_id) {
            publish_durable_event(self.engine, audience, event_type, serde_json::json!({
                "placeId": entry.place_id,
                "instanceId": entry.app_instance_id,
                "fileId": entry.file_id.as_str(),
                "name": entry.name,
            }));
        }
    }
}

impl Drop for FileSyncSource<'_> {
    fn drop(&mut self) {
        if let Some(mut websocket) = self.gateway.take() {
            close_node_websocket(&mut websocket);
        }
    }
}

pub(super) fn file_sync_app_place_root_projection(document: &JsonValue) -> bool {
    fn declared(definition: &JsonValue) -> bool {
        definition
            .get("files")
            .and_then(|files| files.get("projection"))
            .and_then(JsonValue::as_str)
            == Some("place-root")
    }

    if let Some(definition) = document.get("definition") {
        if declared(definition) { return true; }
        if let Some(serialized) = definition.as_str() {
            if let Ok(definition) = serde_json::from_str::<JsonValue>(serialized) {
                if declared(&definition) { return true; }
            }
        }
    }

    // Accept a flattened App Definition too; the sync projection depends on the
    // generic Files contract, never on a concrete App id or kind.
    declared(document)
}

pub(super) fn query_response_message(response: QueryResponse) -> String {
    match serde_json::to_string(&response) {
        Ok(value) => value,
        Err(_) => "OpenGlacier operation failed".to_owned(),
    }
}

pub(super) fn atomic_sync_write(destination: &Path, source: &mut dyn Read) -> Result<u64, String> {
    write_file_atomic(destination, &sync_temporary_path(destination), |output| io::copy(source, output))
        .map_err(|error| error.to_string())
}

pub(super) fn sync_temporary_path(destination: &Path) -> PathBuf {
    let name = destination.file_name().and_then(|value| value.to_str()).unwrap_or("file");
    destination.with_file_name(format!(".{name}.ogsync-{}.part", std::process::id()))
}

pub(super) fn replace_sync_file(temporary: &Path, destination: &Path) -> Result<(), String> {
    if destination.exists() {
        fs::remove_file(destination).map_err(|error| error.to_string())?;
    }
    fs::rename(temporary, destination).map_err(|error| error.to_string())
}

pub(super) fn file_sync_local_metadata(path: &Path) -> Result<(Option<u64>, Option<u64>), String> {
    let metadata = fs::metadata(path).map_err(|error| error.to_string())?;
    let size = metadata.is_file().then_some(metadata.len());
    let modified = metadata.modified().ok().and_then(system_time_millis);
    Ok((size, modified))
}

pub(super) fn file_sync_local_matches(path: &Path, entry: &FileSyncIndexEntry) -> bool {
    if entry.kind.as_deref() == Some("directory") {
        return path.is_dir();
    }
    let Ok((size, modified)) = file_sync_local_metadata(path) else { return false; };
    // Size + millisecond mtime is the hot-path change detector. The watcher is
    // only a trigger and every mutation is reconciled against remote metadata,
    // so re-reading hundreds of MiB just to prove a file is unchanged is both
    // unnecessary and disastrous for large-file sync performance.
    size == entry.local_size && modified == entry.local_modified_at
}

pub(super) fn file_sync_local_fingerprint(path: &Path) -> Result<String, String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() {
        return Err(format!("symbolic links are not supported by Files sync: {}", path.display()));
    }
    let mut hash = FNV1A64_OFFSET;
    if metadata.is_file() {
        // Move correlation must stay cheap for large files. Size + mtime is
        // name-independent and avoids reading the file contents a second time.
        hash = fnv1a64_continue(hash, b"F");
        hash = fnv1a64_continue(hash, &metadata.len().to_le_bytes());
    } else if metadata.is_dir() {
        // Never recursively fingerprint directories: the scanner already walks
        // every entry once, and recursive hashes make nested trees O(n²).
        hash = fnv1a64_continue(hash, b"D");
    } else {
        return Err(format!("unsupported filesystem entry in Files sync: {}", path.display()));
    }
    let modified = metadata.modified().ok().and_then(system_time_millis).unwrap_or_default();
    hash = fnv1a64_continue(hash, &modified.to_le_bytes());
    Ok(format!("fnv1a64-meta:{hash:016x}"))
}

#[derive(Clone, Debug)]
pub(super) struct FileSyncLocalEntry {
    pub(super) kind: &'static str,
    pub(super) size: Option<u64>,
    pub(super) modified_at: Option<u64>,
    pub(super) fingerprint: String,
}

pub(super) fn file_sync_scan_local_root(root: &Path, scan_root: &Path, output: &mut HashMap<PathBuf, FileSyncLocalEntry>) -> Result<(), String> {
    if !scan_root.exists() { return Ok(()); }
    let metadata = fs::symlink_metadata(scan_root).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() {
        return Err(format!("symbolic links are not supported by Files sync: {}", scan_root.display()));
    }
    if !metadata.is_dir() { return Ok(()); }
    let mut pending = vec![scan_root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut children = fs::read_dir(&directory).map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>().map_err(|error| error.to_string())?;
        children.sort_by_key(|entry| entry.file_name());
        for child in children {
            let path = child.path();
            let file_type = child.file_type().map_err(|error| error.to_string())?;
            if file_type.is_symlink() {
                return Err(format!("symbolic links are not supported by Files sync: {}", path.display()));
            }
            let relative_path = path.strip_prefix(root).map_err(|_| format!("sync path escaped configured root: {}", path.display()))?.to_path_buf();
            let (size, modified_at) = file_sync_local_metadata(&path)?;
            if file_type.is_dir() {
                let fingerprint = file_sync_local_fingerprint(&path)?;
                output.insert(relative_path, FileSyncLocalEntry { kind: "directory", size: None, modified_at, fingerprint });
                pending.push(path);
            } else if file_type.is_file() {
                let fingerprint = file_sync_local_fingerprint(&path)?;
                output.insert(relative_path, FileSyncLocalEntry { kind: "file", size, modified_at, fingerprint });
            } else {
                return Err(format!("unsupported filesystem entry in Files sync: {}", path.display()));
            }
        }
    }
    Ok(())
}

pub(super) fn file_sync_mutable_ids(entries: &[FileEntry], selection: &crate::files::FileSyncSelection) -> HashSet<String> {
    if selection.mode == FileSyncSelectionMode::All {
        return entries.iter().map(|entry| entry.file_id.as_str().to_owned()).collect();
    }
    let by_id: HashMap<String, &FileEntry> = entries.iter().map(|entry| (entry.file_id.as_str().to_owned(), entry)).collect();
    let selected: HashSet<&str> = selection.folder_ids.iter().map(String::as_str).collect();
    entries.iter().filter_map(|entry| {
        let mut current = Some(entry.file_id.as_str());
        let mut guard = 0usize;
        while let Some(file_id) = current {
            if selected.contains(file_id) { return Some(entry.file_id.as_str().to_owned()); }
            guard = guard.saturating_add(1);
            if guard > entries.len() { break; }
            current = by_id.get(file_id).and_then(|value| value.parent_id.as_ref().map(|parent| parent.as_str()));
        }
        None
    }).collect()
}

#[derive(Default)]
pub(super) struct FileSyncMutationSummary {
    pub(super) created: u64,
    pub(super) moved: u64,
    pub(super) deleted: u64,
    pub(super) conflicts: Vec<FileSyncIndexEntry>,
    pub(super) errors: Vec<JsonValue>,
}

pub(super) fn file_sync_remote_unchanged(previous: &FileSyncIndexEntry, logical: &FileEntry) -> bool {
    previous.remote_etag == logical.metadata.etag && previous.remote_modified_at == logical.metadata.modified_at
}

pub(super) fn file_sync_local_name(path: &Path, kind: &str) -> Result<String, String> {
    let name = path.file_name().and_then(|value| value.to_str()).filter(|value| !value.is_empty())
        .ok_or_else(|| format!("Files sync path has no portable UTF-8 name: {}", path.display()))?;
    let fallback = if kind == "directory" { "Folder" } else { "File" };
    if file_sync_projection_component(name, fallback) != name {
        return Err(format!("local name cannot be represented safely by Files sync: {name}"));
    }
    Ok(name.to_owned())
}

pub(super) fn file_sync_reconcile_local_mutations( source: &mut FileSyncSource<'_>, root: &Path, selection: &crate::files::FileSyncSelection, base: &Path, primary_files: bool, tree: &[FileEntry], paths: &HashMap<String, PathBuf>, index: &FileSyncIndex, ) -> Result<FileSyncMutationSummary, String> {
    let mutable_ids = file_sync_mutable_ids(tree, selection);
    let by_id: HashMap<String, FileEntry> = tree.iter().cloned().map(|entry| (entry.file_id.as_str().to_owned(), entry)).collect();
    let previous: HashMap<String, FileSyncIndexEntry> = index.entries.iter()
        .filter(|entry| entry.place_id == selection.place_id && entry.app_instance_id == selection.app_instance_id && mutable_ids.contains(&entry.file_id))
        .cloned().map(|entry| (entry.file_id.clone(), entry)).collect();

    let mut local = HashMap::<PathBuf, FileSyncLocalEntry>::new();
    if selection.mode == FileSyncSelectionMode::All {
        file_sync_scan_local_root(root, &root.join(base), &mut local)?;
        if primary_files {
            let reserved = base.join(APP_FILES_DIRECTORY);
            local.retain(|path, _| !path.starts_with(&reserved));
        }
    } else {
        for folder_id in &selection.folder_ids {
            if let Some(path) = paths.get(folder_id) {
                file_sync_scan_local_root(root, &root.join(path), &mut local)?;
                if let Some(directory) = root.join(path).strip_prefix(root).ok().map(Path::to_path_buf) {
                    if root.join(path).is_dir() {
                        let (size, modified_at) = file_sync_local_metadata(&root.join(path))?;
                        local.entry(directory).or_insert(FileSyncLocalEntry {
                            kind: "directory",
                            size,
                            modified_at,
                            fingerprint: file_sync_local_fingerprint(&root.join(path))?,
                        });
                    }
                }
            }
        }
    }

    let tracked_paths: HashSet<PathBuf> = previous.values().map(|entry| entry.materialized_path.clone()).collect();
    let remote_paths: HashSet<PathBuf> = paths.values().cloned().collect();
    let mut candidates: HashSet<PathBuf> = local.keys()
        .filter(|path| !tracked_paths.contains(*path) && !remote_paths.contains(*path))
        .cloned().collect();
    let mut path_to_id: HashMap<PathBuf, String> = previous.values()
        .filter(|entry| root.join(&entry.materialized_path).exists())
        .map(|entry| (entry.materialized_path.clone(), entry.file_id.clone())).collect();
    let mut handled_missing_prefixes = Vec::<PathBuf>::new();
    let mut summary = FileSyncMutationSummary::default();

    let mut missing: Vec<&FileSyncIndexEntry> = previous.values().filter(|entry| !root.join(&entry.materialized_path).exists()).collect();
    missing.sort_by_key(|entry| entry.materialized_path.components().count());
    for previous_entry in missing {
        if handled_missing_prefixes.iter().any(|prefix| previous_entry.materialized_path.starts_with(prefix)) { continue; }
        let Some(logical) = by_id.get(&previous_entry.file_id) else { continue; };
        if paths.get(&previous_entry.file_id).is_some_and(|path| path != &previous_entry.materialized_path) {
            // The logical object moved remotely. Remote projection wins here;
            // materialization below will move the unchanged tracked local copy.
            continue;
        }
        if !file_sync_remote_unchanged(previous_entry, logical) {
            let mut conflict = previous_entry.clone();
            conflict.state = FileSyncEntryState::Conflict;
            conflict.last_error = Some("local entry disappeared while the remote entry changed".to_owned());
            summary.conflicts.push(conflict);
            handled_missing_prefixes.push(previous_entry.materialized_path.clone());
            continue;
        }

        let expected_kind = previous_entry.kind.as_deref().unwrap_or(logical.kind.as_str());
        let mut matches = candidates.iter().filter_map(|path| {
            let candidate = local.get(path)?;
            if candidate.kind != expected_kind { return None; }
            let fingerprint_matches = previous_entry.local_fingerprint.as_ref()
                .is_some_and(|value| value.starts_with("fnv1a64-meta:") && value == &candidate.fingerprint);
            let legacy_file_match = expected_kind == "file"
                && !previous_entry.local_fingerprint.as_deref().is_some_and(|value| value.starts_with("fnv1a64-meta:"))
                && previous_entry.local_size == candidate.size && previous_entry.local_modified_at == candidate.modified_at;
            (fingerprint_matches || legacy_file_match).then_some(path.clone())
        }).collect::<Vec<_>>();
        matches.sort();
        if matches.len() == 1 {
            let candidate_path = matches.remove(0);
            let parent_path = candidate_path.parent().map(Path::to_path_buf).unwrap_or_default();
            let parent_id = if parent_path == base { None } else { path_to_id.get(&parent_path).map(String::as_str) };
            if parent_path != base && parent_id.is_none() {
                let mut conflict = previous_entry.clone();
                conflict.state = FileSyncEntryState::Conflict;
                conflict.last_error = Some("local move targets a parent that is not synchronized".to_owned());
                summary.conflicts.push(conflict);
                continue;
            }
            let name = match file_sync_local_name(&candidate_path, expected_kind) {
                Ok(name) => name,
                Err(error) => {
                    summary.errors.push(serde_json::json!({"placeId":selection.place_id,"instanceId":selection.app_instance_id,"fileId":previous_entry.file_id,"error":error}));
                    continue;
                }
            };
            match source.move_entry(logical, parent_id, &name) {
                Ok(_) => {
                    summary.moved = summary.moved.saturating_add(1);
                    candidates.remove(&candidate_path);
                    path_to_id.remove(&previous_entry.materialized_path);
                    path_to_id.insert(candidate_path.clone(), previous_entry.file_id.clone());
                    if expected_kind == "directory" {
                        handled_missing_prefixes.push(previous_entry.materialized_path.clone());
                        let descendants = previous.values().filter(|entry| entry.materialized_path.starts_with(&previous_entry.materialized_path) && entry.file_id != previous_entry.file_id).cloned().collect::<Vec<_>>();
                        for descendant in descendants {
                            if let Ok(suffix) = descendant.materialized_path.strip_prefix(&previous_entry.materialized_path) {
                                path_to_id.remove(&descendant.materialized_path);
                                path_to_id.insert(candidate_path.join(suffix), descendant.file_id);
                            }
                        }
                    }
                    continue;
                }
                Err(error) => {
                    summary.errors.push(serde_json::json!({"placeId":selection.place_id,"instanceId":selection.app_instance_id,"fileId":previous_entry.file_id,"error":error}));
                    continue;
                }
            }
        }

        if expected_kind == "directory"
            && !previous_entry.local_fingerprint.as_deref().is_some_and(|value| value.starts_with("fnv1a64-meta:"))
            && candidates.iter().any(|path| local.get(path).is_some_and(|entry| entry.kind == "directory")) {
            let mut conflict = previous_entry.clone();
            conflict.state = FileSyncEntryState::Conflict;
            conflict.last_error = Some("directory disappeared but its previous fingerprint is unavailable; refusing destructive reconciliation".to_owned());
            summary.conflicts.push(conflict);
            handled_missing_prefixes.push(previous_entry.materialized_path.clone());
            continue;
        }

        match source.delete_entry(logical) {
            Ok(()) => {
                summary.deleted = summary.deleted.saturating_add(1);
                handled_missing_prefixes.push(previous_entry.materialized_path.clone());
            }
            Err(error) => summary.errors.push(serde_json::json!({"placeId":selection.place_id,"instanceId":selection.app_instance_id,"fileId":previous_entry.file_id,"error":error})),
        }
    }

    let mut new_paths: Vec<PathBuf> = candidates.into_iter().collect();
    new_paths.sort_by_key(|path| path.components().count());
    for path in new_paths {
        let Some(candidate) = local.get(&path) else { continue; };
        if tracked_paths.contains(&path) || path_to_id.contains_key(&path) { continue; }
        let parent_path = path.parent().map(Path::to_path_buf).unwrap_or_default();
        let parent_id = if parent_path == base { None } else { path_to_id.get(&parent_path).map(String::as_str) };
        if parent_path != base && parent_id.is_none() { continue; }
        let name = match file_sync_local_name(&path, candidate.kind) {
            Ok(name) => name,
            Err(error) => {
                summary.errors.push(serde_json::json!({"placeId":selection.place_id,"instanceId":selection.app_instance_id,"path":path,"error":error}));
                continue;
            }
        };
        let created = if candidate.kind == "directory" {
            source.mkdir(&selection.place_id, &selection.app_instance_id, parent_id, &name)
        } else {
            source.create_file_from_path(&selection.place_id, &selection.app_instance_id, parent_id, &name, &root.join(&path))
        };
        match created {
            Ok(entry) => {
                summary.created = summary.created.saturating_add(1);
                path_to_id.insert(path, entry.file_id.as_str().to_owned());
            }
            Err(error) => summary.errors.push(serde_json::json!({"placeId":selection.place_id,"instanceId":selection.app_instance_id,"path":path,"error":error})),
        }
    }

    Ok(summary)
}

pub(super) fn file_sync_prune_empty_ancestors(mut path: Option<&Path>, root: &Path) {
    while let Some(directory) = path {
        if directory == root || !directory.starts_with(root) { break; }
        match fs::remove_dir(directory) {
            Ok(()) => path = directory.parent(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => path = directory.parent(),
            Err(_) => break,
        }
    }
}

pub(super) fn file_sync_entry_depth(entry: &FileEntry, by_id: &HashMap<String, FileEntry>) -> usize {
    let mut depth = 0usize;
    let mut current = entry.parent_id.as_ref().map(|value| value.as_str());
    let mut guard = 0usize;
    while let Some(parent_id) = current {
        depth = depth.saturating_add(1);
        guard = guard.saturating_add(1);
        if guard > by_id.len() { break; }
        current = by_id.get(parent_id).and_then(|parent| parent.parent_id.as_ref().map(|value| value.as_str()));
    }
    depth
}

pub(super) fn file_sync_selected_ids(entries: &[FileEntry], selection: &crate::files::FileSyncSelection) -> Result<HashSet<String>, String> {
    if selection.mode == FileSyncSelectionMode::All {
        return Ok(entries.iter().map(|entry| entry.file_id.as_str().to_owned()).collect());
    }
    let by_id: HashMap<String, &FileEntry> = entries.iter().map(|entry| (entry.file_id.as_str().to_owned(), entry)).collect();
    for folder_id in &selection.folder_ids {
        let folder = by_id.get(folder_id).ok_or_else(|| format!("selected Files folder {folder_id} no longer exists"))?;
        if folder.kind != crate::files::FileKind::Directory {
            return Err(format!("selected Files object {folder_id} is not a directory"));
        }
    }

    let selected: HashSet<&str> = selection.folder_ids.iter().map(String::as_str).collect();
    let mut materialized = HashSet::new();
    for entry in entries {
        let mut current = Some(entry.file_id.as_str());
        let mut included = false;
        let mut guard = 0usize;
        while let Some(file_id) = current {
            if selected.contains(file_id) { included = true; break; }
            guard = guard.saturating_add(1);
            if guard > entries.len() { break; }
            current = by_id.get(file_id).and_then(|value| value.parent_id.as_ref().map(|parent| parent.as_str()));
        }
        if included {
            materialized.insert(entry.file_id.as_str().to_owned());
        }
    }
    // Ancestor directories are materialized as containers so a selected nested
    // folder keeps its natural path from the instance root.
    for folder_id in &selection.folder_ids {
        let mut current = by_id.get(folder_id).and_then(|entry| entry.parent_id.as_ref().map(|parent| parent.as_str()));
        let mut guard = 0usize;
        while let Some(file_id) = current {
            materialized.insert(file_id.to_owned());
            guard = guard.saturating_add(1);
            if guard > entries.len() { break; }
            current = by_id.get(file_id).and_then(|value| value.parent_id.as_ref().map(|parent| parent.as_str()));
        }
    }
    Ok(materialized)
}

pub(super) fn file_sync_reserve_component(base: &Path, desired: String, stable_id: &str, claimed: &mut HashMap<String, String>) -> String {
    let candidate = base.join(&desired);
    let key = candidate.to_string_lossy().to_lowercase();
    match claimed.get(&key) {
        None => { claimed.insert(key, stable_id.to_owned()); desired }
        Some(existing) if existing == stable_id => desired,
        Some(_) => {
            let suffix = file_sync_projection_suffix(stable_id);
            let disambiguated = format!("{desired} ({suffix})");
            claimed.insert(base.join(&disambiguated).to_string_lossy().to_lowercase(), stable_id.to_owned());
            disambiguated
        }
    }
}

pub(super) fn file_sync_relative_paths( entries: &[FileEntry], included: &HashSet<String>, base: &Path, primary_files: bool, claimed: &mut HashMap<String, String>, ) -> HashMap<String, PathBuf> {
    let by_id: HashMap<String, FileEntry> = entries.iter().cloned().map(|entry| (entry.file_id.as_str().to_owned(), entry)).collect();
    let mut ordered: Vec<&FileEntry> = entries.iter().filter(|entry| included.contains(entry.file_id.as_str())).collect();
    ordered.sort_by_key(|entry| file_sync_entry_depth(entry, &by_id));
    let mut paths = HashMap::new();
    for entry in ordered {
        let parent = entry.parent_id.as_ref().and_then(|parent| paths.get(parent.as_str())).cloned().unwrap_or_else(|| base.to_path_buf());
        let fallback = if entry.kind == crate::files::FileKind::Directory { "Folder" } else { "File" };
        let mut name = file_sync_projection_component(&entry.name, fallback);
        if primary_files && entry.parent_id.is_none() && name.eq_ignore_ascii_case(APP_FILES_DIRECTORY) {
            name = PRIMARY_APPS_COLLISION_NAME.to_owned();
        }
        let name = file_sync_reserve_component(&parent, name, entry.file_id.as_str(), claimed);
        paths.insert(entry.file_id.as_str().to_owned(), parent.join(name));
    }
    paths
}

pub(super) fn file_sync_rebase_index_paths(index: &mut FileSyncIndex, old_prefix: &Path, new_prefix: &Path) {
    for entry in &mut index.entries {
        if let Ok(suffix) = entry.materialized_path.strip_prefix(old_prefix) {
            entry.materialized_path = new_prefix.join(suffix);
        }
    }
}

pub(super) fn file_sync_materialize_entry( source: &mut FileSyncSource<'_>, root: &Path, logical: &FileEntry, relative_path: PathBuf, index: &mut FileSyncIndex, ) -> Result<(FileSyncIndexEntry, bool, bool), String> {
    let key_matches = |entry: &&FileSyncIndexEntry| entry.place_id == logical.place_id && entry.app_instance_id == logical.app_instance_id && entry.file_id == logical.file_id.as_str();
    let destination = root.join(&relative_path);

    // File IDs are authoritative while they remain stable, but an upload/create
    // can replace the remote identity for a path that is already tracked locally.
    // In that case treating the local file as "untracked" creates a false
    // conflict even when it is byte-for-byte (size+mtime hot-path) the exact
    // local state recorded in our base index.
    //
    // Rebind by path only when it is provably safe: same Place/instance/path and
    // the current local object still matches that previous base entry. A genuinely
    // untracked occupant therefore continues to conflict.
    let existing = index.entries.iter().find(key_matches).cloned().or_else(|| {
        if !destination.exists() {
            return None;
        }
        index.entries.iter().find(|entry| {
            entry.place_id == logical.place_id
                && entry.app_instance_id == logical.app_instance_id
                && entry.materialized_path == relative_path
                && file_sync_local_matches(&destination, entry)
        }).cloned()
    });
    if let Some(previous) = existing.as_ref() {
        if previous.materialized_path != relative_path {
            let old_path = root.join(&previous.materialized_path);
            if old_path.exists() && !destination.exists() && file_sync_local_matches(&old_path, previous) {
                if let Some(parent) = destination.parent() { fs::create_dir_all(parent).map_err(|error| error.to_string())?; }
                fs::rename(&old_path, &destination).map_err(|error| error.to_string())?;
                file_sync_prune_empty_ancestors(old_path.parent(), root);
                if logical.kind == crate::files::FileKind::Directory {
                    file_sync_rebase_index_paths(index, &previous.materialized_path, &relative_path);
                }
            }
        }
    }

    if logical.kind == crate::files::FileKind::Directory {
        if destination.exists() && !destination.is_dir() {
            return Ok((FileSyncIndexEntry {
                place_id: logical.place_id.clone(), app_instance_id: logical.app_instance_id.clone(), file_id: logical.file_id.as_str().to_owned(),
                materialized_path: relative_path, kind: Some("directory".to_owned()), remote_etag: logical.metadata.etag.clone(), remote_modified_at: logical.metadata.modified_at,
                local_size: None, local_modified_at: None, local_fingerprint: None, state: FileSyncEntryState::Conflict,
                last_error: Some("a local file occupies the directory required by OpenGlacier".to_owned()),
            }, false, false));
        }
        fs::create_dir_all(&destination).map_err(|error| error.to_string())?;
        let (_, modified) = file_sync_local_metadata(&destination)?;
        return Ok((FileSyncIndexEntry {
            place_id: logical.place_id.clone(), app_instance_id: logical.app_instance_id.clone(), file_id: logical.file_id.as_str().to_owned(),
            materialized_path: relative_path, kind: Some("directory".to_owned()), remote_etag: logical.metadata.etag.clone(), remote_modified_at: logical.metadata.modified_at,
            local_size: None, local_modified_at: modified, local_fingerprint: None, state: FileSyncEntryState::Synced, last_error: None,
        }, false, false));
    }

    if let Some(previous) = existing.as_ref() {
        let tracked_path = root.join(&previous.materialized_path);
        if tracked_path.exists() && !file_sync_local_matches(&tracked_path, previous) {
            let remote_unchanged = previous.remote_etag == logical.metadata.etag
                && previous.remote_modified_at == logical.metadata.modified_at;
            if remote_unchanged {
                let updated = source.write_existing_from_path(logical, &tracked_path)?;
                let (size, modified) = file_sync_local_metadata(&tracked_path)?;
                return Ok((FileSyncIndexEntry {
                    place_id: updated.place_id.clone(), app_instance_id: updated.app_instance_id.clone(), file_id: updated.file_id.as_str().to_owned(),
                    materialized_path: previous.materialized_path.clone(), kind: Some("file".to_owned()), remote_etag: updated.metadata.etag.clone(), remote_modified_at: updated.metadata.modified_at,
                    local_size: size, local_modified_at: modified, local_fingerprint: None, state: FileSyncEntryState::Synced,
                    last_error: None,
                }, false, true));
            }
            return Ok((FileSyncIndexEntry {
                place_id: logical.place_id.clone(), app_instance_id: logical.app_instance_id.clone(), file_id: logical.file_id.as_str().to_owned(),
                materialized_path: previous.materialized_path.clone(), kind: Some("file".to_owned()), remote_etag: logical.metadata.etag.clone(), remote_modified_at: logical.metadata.modified_at,
                local_size: previous.local_size, local_modified_at: previous.local_modified_at, local_fingerprint: previous.local_fingerprint.clone(), state: FileSyncEntryState::Conflict,
                last_error: Some("local and remote content both changed since the last synchronization".to_owned()),
            }, false, false));
        }
    } else if destination.exists() {
        let (size, modified) = file_sync_local_metadata(&destination)?;
        return Ok((FileSyncIndexEntry {
            place_id: logical.place_id.clone(), app_instance_id: logical.app_instance_id.clone(), file_id: logical.file_id.as_str().to_owned(),
            materialized_path: relative_path, kind: Some("file".to_owned()), remote_etag: logical.metadata.etag.clone(), remote_modified_at: logical.metadata.modified_at,
            local_size: size, local_modified_at: modified, local_fingerprint: None, state: FileSyncEntryState::Conflict,
            last_error: Some("an untracked local file already occupies this OpenGlacier path".to_owned()),
        }, false, false));
    }

    let remote_unchanged = existing.as_ref().is_some_and(|previous| previous.remote_etag == logical.metadata.etag && previous.remote_modified_at == logical.metadata.modified_at);
    if remote_unchanged && destination.exists() && existing.as_ref().is_some_and(|previous| file_sync_local_matches(&destination, previous)) {
        let (size, modified) = file_sync_local_metadata(&destination)?;
        return Ok((FileSyncIndexEntry {
            place_id: logical.place_id.clone(), app_instance_id: logical.app_instance_id.clone(), file_id: logical.file_id.as_str().to_owned(),
            materialized_path: relative_path, kind: Some("file".to_owned()), remote_etag: logical.metadata.etag.clone(), remote_modified_at: logical.metadata.modified_at,
            local_size: size, local_modified_at: modified, local_fingerprint: None, state: FileSyncEntryState::Synced, last_error: None,
        }, false, false));
    }

    let bytes = source.read_file_to_path(logical, &destination)?;
    if logical.metadata.size.is_some_and(|expected| expected != bytes) {
        return Err(format!("downloaded {bytes} bytes for {} but metadata announced {}", logical.file_id, logical.metadata.size.unwrap_or_default()));
    }
    let (size, modified) = file_sync_local_metadata(&destination)?;
    Ok((FileSyncIndexEntry {
        place_id: logical.place_id.clone(), app_instance_id: logical.app_instance_id.clone(), file_id: logical.file_id.as_str().to_owned(),
        materialized_path: relative_path, kind: Some("file".to_owned()), remote_etag: logical.metadata.etag.clone(), remote_modified_at: logical.metadata.modified_at,
        local_size: size, local_modified_at: modified, local_fingerprint: None, state: FileSyncEntryState::Synced, last_error: None,
    }, true, false))
}

pub(super) fn run_file_sync_materialization(settings: &ConnectionSettings, engine: &Engine, config: &FileSyncConfig, index: &mut FileSyncIndex, identity_id: &str, device_id: &str, allow_local_mutations: bool) -> Result<JsonValue, String> {
    if !config.root.is_dir() {
        return Err(format!(
            "Files sync root {} is unavailable; refusing local deletions until the root is restored or reconfigured",
            config.root.display()
        ));
    }
    let mut source = FileSyncSource::connect(settings, engine)?;
    let apps = source.apps()?;

    // storageRole=sync is the Place-level switch. Folder selections are only
    // overrides. A newly assigned sync Place therefore defaults to syncing all
    // active App-instance file scopes, even before Desktop has saved a tree
    // selection. This also guarantees that an empty Place directory is created.
    let mut effective_selections = config.selections.clone();
    let sync_places = source.sync_place_ids(identity_id, device_id)?;
    let mut selected_places: HashSet<String> = sync_places.iter().cloned().collect();
    selected_places.extend(config.selections.iter().map(|selection| selection.place_id.clone()));
    let mut instance_cache = HashMap::<String, HashMap<String, FileSyncInstanceProjection>>::new();
    for place_id in &sync_places {
        let instances = source.instances(place_id)?;
        for instance_id in instances.keys() {
            if !effective_selections.iter().any(|selection| selection.place_id.as_str() == place_id.as_str() && selection.app_instance_id.as_str() == instance_id.as_str()) {
                effective_selections.push(crate::files::FileSyncSelection {
                    place_id: place_id.clone(),
                    app_instance_id: instance_id.clone(),
                    mode: FileSyncSelectionMode::All,
                    folder_ids: Vec::new(),
                });
            }
        }
        instance_cache.insert(place_id.clone(), instances);
    }

    let mut place_names = HashMap::new();
    let mut place_name_counts = HashMap::<String, usize>::new();
    for place_id in &selected_places {
        let name = file_sync_projection_component(&source.place_name(place_id)?, "Place");
        *place_name_counts.entry(name.to_lowercase()).or_insert(0) += 1;
        place_names.insert(place_id.clone(), name);
    }

    let mut claimed = HashMap::<String, String>::new();
    let mut seen = HashSet::<(String, String, String)>::new();
    let mut next_entries = Vec::<FileSyncIndexEntry>::new();
    let mut downloaded = 0u64;
    let mut uploaded = 0u64;
    let mut created = 0u64;
    let mut moved = 0u64;
    let mut deleted = 0u64;
    let mut materialized = 0u64;
    let mut conflicts = 0u64;
    let mut errors = Vec::<JsonValue>::new();

    for selection in &effective_selections {
        if !source.place_has_sync_assignment(&selection.place_id, identity_id, device_id)? {
            errors.push(serde_json::json!({
                "placeId": selection.place_id,
                "instanceId": selection.app_instance_id,
                "error": "this device is not assigned files.storageRole=sync for the Place"
            }));
            continue;
        }
        let raw_place_name = place_names.get(&selection.place_id).cloned().unwrap_or_else(|| "Place".to_owned());
        let place_name = if place_name_counts.get(&raw_place_name.to_lowercase()).copied().unwrap_or(0) > 1 {
            format!("{} ({})", raw_place_name, file_sync_projection_suffix(&selection.place_id))
        } else { raw_place_name };
        let place_base = PathBuf::from(file_sync_reserve_component(Path::new(""), place_name, &format!("place:{}", selection.place_id), &mut claimed));
        fs::create_dir_all(config.root.join(&place_base))
            .map_err(|error| format!("cannot prepare sync Place directory {}: {error}", config.root.join(&place_base).display()))?;
        let apps_namespace = place_base.join(APP_FILES_DIRECTORY);
        claimed.entry(apps_namespace.to_string_lossy().to_lowercase()).or_insert_with(|| format!("apps:{}", selection.place_id));

        if !instance_cache.contains_key(&selection.place_id) {
            instance_cache.insert(selection.place_id.clone(), source.instances(&selection.place_id)?);
        }
        let instances = instance_cache.get(&selection.place_id).expect("inserted above");
        let Some(instance) = instances.get(&selection.app_instance_id) else {
            errors.push(serde_json::json!({"placeId": selection.place_id, "instanceId": selection.app_instance_id, "error": "selected App instance is not active"}));
            continue;
        };
        let app = apps.get(&instance.app_id).cloned().unwrap_or(FileSyncAppProjection { name: instance.app_id.clone(), place_root_projection: false });
        let primary_files = app.place_root_projection;
        let base = if primary_files {
            place_base.clone()
        } else {
            let app_name = file_sync_projection_component(&app.name, "App");
            let app_dir = file_sync_reserve_component(&apps_namespace, app_name, &format!("app:{}", instance.app_id), &mut claimed);
            let app_base = apps_namespace.join(app_dir);
            let instance_name = file_sync_projection_component(&instance.name, "Main");
            let instance_dir = file_sync_reserve_component(&app_base, instance_name, &format!("instance:{}", selection.app_instance_id), &mut claimed);
            app_base.join(instance_dir)
        };

        let tree = match source.file_tree(&selection.place_id, &selection.app_instance_id) {
            Ok(tree) => tree,
            Err(error) => {
                errors.push(serde_json::json!({"placeId": selection.place_id, "instanceId": selection.app_instance_id, "error": error}));
                continue;
            }
        };
        let included = match file_sync_selected_ids(&tree, selection) {
            Ok(value) => value,
            Err(error) => {
                errors.push(serde_json::json!({"placeId": selection.place_id, "instanceId": selection.app_instance_id, "error": error}));
                continue;
            }
        };
        let mut paths = file_sync_relative_paths(&tree, &included, &base, primary_files, &mut claimed);
        let mutations = if allow_local_mutations {
            match file_sync_reconcile_local_mutations(&mut source, &config.root, selection, &base, primary_files, &tree, &paths, index) {
                Ok(value) => value,
                Err(error) => {
                    errors.push(serde_json::json!({"placeId":selection.place_id,"instanceId":selection.app_instance_id,"error":error}));
                    continue;
                }
            }
        } else {
            FileSyncMutationSummary::default()
        };
        created = created.saturating_add(mutations.created);
        moved = moved.saturating_add(mutations.moved);
        deleted = deleted.saturating_add(mutations.deleted);
        errors.extend(mutations.errors);
        let mut blocked_ids = HashSet::<String>::new();
        if !mutations.conflicts.is_empty() {
            let old_by_id: HashMap<String, FileEntry> = tree.iter().cloned().map(|entry| (entry.file_id.as_str().to_owned(), entry)).collect();
            for conflict in mutations.conflicts {
                blocked_ids.insert(conflict.file_id.clone());
                if conflict.kind.as_deref() == Some("directory") {
                    for candidate in &tree {
                        let mut current = candidate.parent_id.as_ref().map(|value| value.as_str());
                        let mut guard = 0usize;
                        while let Some(parent_id) = current {
                            if parent_id == conflict.file_id {
                                blocked_ids.insert(candidate.file_id.as_str().to_owned());
                                break;
                            }
                            guard = guard.saturating_add(1);
                            if guard > tree.len() { break; }
                            current = old_by_id.get(parent_id).and_then(|entry| entry.parent_id.as_ref().map(|value| value.as_str()));
                        }
                    }
                }
                seen.insert((conflict.place_id.clone(), conflict.app_instance_id.clone(), conflict.file_id.clone()));
                conflicts = conflicts.saturating_add(1);
                next_entries.push(conflict);
            }
        }

        let changed_remote = mutations.created > 0 || mutations.moved > 0 || mutations.deleted > 0;
        let tree = if changed_remote {
            match source.file_tree(&selection.place_id, &selection.app_instance_id) {
                Ok(value) => value,
                Err(error) => {
                    errors.push(serde_json::json!({"placeId":selection.place_id,"instanceId":selection.app_instance_id,"error":error}));
                    continue;
                }
            }
        } else { tree };
        let included = match file_sync_selected_ids(&tree, selection) {
            Ok(value) => value,
            Err(error) => {
                errors.push(serde_json::json!({"placeId":selection.place_id,"instanceId":selection.app_instance_id,"error":error}));
                continue;
            }
        };
        paths = file_sync_relative_paths(&tree, &included, &base, primary_files, &mut claimed);
        let by_id: HashMap<String, FileEntry> = tree.iter().cloned().map(|entry| (entry.file_id.as_str().to_owned(), entry)).collect();
        let mut ordered: Vec<&FileEntry> = tree.iter().filter(|entry| included.contains(entry.file_id.as_str()) && !blocked_ids.contains(entry.file_id.as_str())).collect();
        ordered.sort_by_key(|entry| file_sync_entry_depth(entry, &by_id));
        for logical in ordered {
            let Some(relative_path) = paths.get(logical.file_id.as_str()).cloned() else { continue; };
            let key = (logical.place_id.clone(), logical.app_instance_id.clone(), logical.file_id.as_str().to_owned());
            seen.insert(key.clone());
            match file_sync_materialize_entry(&mut source, &config.root, logical, relative_path, index) {
                Ok((entry, was_downloaded, was_uploaded)) => {
                    materialized = materialized.saturating_add(1);
                    if was_downloaded { downloaded = downloaded.saturating_add(1); }
                    if was_uploaded { uploaded = uploaded.saturating_add(1); }
                    if entry.state == FileSyncEntryState::Conflict { conflicts = conflicts.saturating_add(1); }
                    next_entries.push(entry);
                }
                Err(error) => {
                    errors.push(serde_json::json!({"placeId": logical.place_id, "instanceId": logical.app_instance_id, "fileId": logical.file_id.as_str(), "error": error}));
                    next_entries.push(FileSyncIndexEntry {
                        place_id: logical.place_id.clone(), app_instance_id: logical.app_instance_id.clone(), file_id: logical.file_id.as_str().to_owned(), materialized_path: paths.get(logical.file_id.as_str()).cloned().unwrap_or_default(),
                        kind: Some(logical.kind.as_str().to_owned()), remote_etag: logical.metadata.etag.clone(), remote_modified_at: logical.metadata.modified_at,
                        local_size: None, local_modified_at: None, local_fingerprint: None, state: FileSyncEntryState::Error, last_error: Some(error),
                    });
                }
            }
        }
    }

    // Entries that are no longer selected are removed only when their local
    // copy still matches the last synchronized state. Local modifications are
    // retained as conflicts until bidirectional reconciliation exists.
    let mut stale: Vec<FileSyncIndexEntry> = index.entries.iter().filter(|entry| {
        !seen.contains(&(entry.place_id.clone(), entry.app_instance_id.clone(), entry.file_id.clone()))
    }).cloned().collect();
    stale.sort_by_key(|entry| std::cmp::Reverse(entry.materialized_path.components().count()));
    for mut entry in stale {
        let path = config.root.join(&entry.materialized_path);
        if !path.exists() { continue; }
        if entry.kind.as_deref() == Some("file") {
            if file_sync_local_matches(&path, &entry) {
                if let Err(error) = fs::remove_file(&path) {
                    entry.state = FileSyncEntryState::Error; entry.last_error = Some(error.to_string()); next_entries.push(entry);
                } else {
                    file_sync_prune_empty_ancestors(path.parent(), &config.root);
                }
            } else {
                entry.state = FileSyncEntryState::Conflict;
                entry.last_error = Some("local file changed after it left the sync selection".to_owned());
                conflicts = conflicts.saturating_add(1);
                next_entries.push(entry);
            }
        } else if entry.kind.as_deref() == Some("directory") {
            match fs::remove_dir(&path) {
                Ok(()) => { file_sync_prune_empty_ancestors(path.parent(), &config.root); }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) if error.kind() == io::ErrorKind::DirectoryNotEmpty => {
                    entry.state = FileSyncEntryState::Conflict;
                    entry.last_error = Some("directory is no longer selected but still contains local content".to_owned());
                    conflicts = conflicts.saturating_add(1);
                    next_entries.push(entry);
                }
                Err(error) => { entry.state = FileSyncEntryState::Error; entry.last_error = Some(error.to_string()); next_entries.push(entry); }
            }
        }
    }

    for entry in &mut next_entries {
        if entry.state != FileSyncEntryState::Synced { continue; }
        let path = config.root.join(&entry.materialized_path);
        if !path.exists() { continue; }
        if let Ok((size, modified_at)) = file_sync_local_metadata(&path) {
            entry.local_size = size;
            entry.local_modified_at = modified_at;
        }
        if let Ok(fingerprint) = file_sync_local_fingerprint(&path) {
            entry.local_fingerprint = Some(fingerprint);
        }
    }
    next_entries.sort_by(|left, right| (&left.place_id, &left.app_instance_id, &left.file_id).cmp(&(&right.place_id, &right.app_instance_id, &right.file_id)));
    index.entries = next_entries;
    Ok(serde_json::json!({
        "materialized": materialized,
        "downloaded": downloaded,
        "uploaded": uploaded,
        "created": created,
        "moved": moved,
        "deleted": deleted,
        "conflicts": conflicts,
        "errors": errors,
        "projection": {"appsDirectory": APP_FILES_DIRECTORY, "primaryAppsCollisionDirectory": PRIMARY_APPS_COLLISION_NAME},
    }))
}


pub(super) fn file_sync_signal(runtime: &FileSyncRuntime, trigger: &str) {
    if let Some(sender) = runtime.trigger.as_ref() {
        let _ = sender.send(trigger.to_owned());
    }
}

pub(super) fn file_sync_event_relevant(event_type: &str) -> bool {
    if event_type == "backup.restored" {
        return true;
    }
    if event_type.starts_with("file.sync.") {
        return false;
    }
    event_type.starts_with("file.")
        || matches!(event_type, "place.created" | "place.updated" | "place.deleted")
        || event_type.starts_with("place.resource.")
        || event_type == "place.resources.updated"
        || event_type.starts_with("place.access.")
        || matches!(event_type, "app.created" | "app.updated" | "app.deleted")
        || event_type.starts_with("app.instance.")
}

pub(super) fn run_file_sync_once( settings: &ConnectionSettings, engine: &Engine, identity_id: &str, device_id: &str, ) -> Result<(u64, JsonValue), String> {
    let _run = settings
        .file_sync_run
        .lock()
        .map_err(|_| "Files sync execution lock is unavailable".to_owned())?;
    let (config, index_path) = {
        let runtime = settings
            .file_sync
            .lock()
            .map_err(|_| "Files sync local state is unavailable".to_owned())?;
        let config = runtime
            .config
            .clone()
            .ok_or_else(|| "configure a Files sync root before running synchronization".to_owned())?;
        (config, runtime.index_path.clone())
    };
    let mut index = match FileSyncIndex::load(&index_path) {
        Ok(Some(index)) => index,
        Ok(None) => FileSyncIndex::default(),
        Err(error) => return Err(format!("cannot read Files sync index: {error}")),
    };
    let run_at = unix_time_millis();
    let allow_local_mutations = !config.local_baseline_required;
    let result = run_file_sync_materialization(settings, engine, &config, &mut index, identity_id, device_id, allow_local_mutations)
        .and_then(|summary| {
            index
                .save(&index_path)
                .map_err(|error| format!("cannot persist Files sync index: {error}"))?;
            if config.local_baseline_required {
                let mut updated = config.clone();
                updated.local_baseline_required = false;
                let config_path = settings
                    .file_sync
                    .lock()
                    .map_err(|_| "Files sync local state is unavailable".to_owned())?
                    .config_path
                    .clone();
                updated
                    .save(&config_path)
                    .map_err(|error| format!("cannot persist Files sync configuration after root baseline: {error}"))?;
                if let Ok(mut runtime) = settings.file_sync.lock() {
                    runtime.config = Some(updated);
                }
            }
            Ok(summary)
        });
    if let Ok(mut runtime) = settings.file_sync.lock() {
        runtime.last_run_at = Some(run_at);
        runtime.last_run_error = result.as_ref().err().cloned();
    }
    result.map(|summary| (run_at, summary))
}

pub(super) fn start_file_sync_worker( settings: Arc<ConnectionSettings>, engine: Arc<Engine>, event_engine: Arc<EventEngine>, ) -> Option<thread::JoinHandle<()>> {
    if !settings.service_capabilities.contains(ServiceCapability::Files) {
        return None;
    }

    let (trigger_tx, trigger_rx) = std::sync::mpsc::channel::<String>();
    if let Ok(mut runtime) = settings.file_sync.lock() {
        runtime.trigger = Some(trigger_tx.clone());
        runtime.worker_running = true;
        runtime.remote_events = if has_upstream_gateway(&settings)
            && settings.service_capabilities.contains(ServiceCapability::Events)
        {
            "connecting".to_owned()
        } else if has_upstream_gateway(&settings) {
            "disabled".to_owned()
        } else {
            "local".to_owned()
        };
    }

    // Local Core events cover standalone operation and provider-side mutations.
    // They are only triggers: reconciliation remains the source of truth.
    {
        let subscription = event_engine.subscribe(vec![
            "file.*".to_owned(),
            "place.created".to_owned(), "place.updated".to_owned(), "place.deleted".to_owned(),
            "place.resource.*".to_owned(), "place.resources.updated".to_owned(), "place.access.*".to_owned(),
            "app.created".to_owned(), "app.updated".to_owned(), "app.deleted".to_owned(),
            "app.instance.*".to_owned(),
        ]);
        let sender = trigger_tx.clone();
        thread::Builder::new()
            .name("og-file-sync-events-local".to_owned())
            .spawn(move || {
                while let Ok(event) = subscription.recv() {
                    if file_sync_event_relevant(&event.event_type) {
                        let _ = sender.send(format!("core:{}", event.event_type));
                    }
                }
            })
            .ok();
    }

    // Remote authoritative events are relayed into the local EventEngine once per
    // connected ogd. The same local subscription above therefore covers both
    // provider-local and upstream mutations without opening a second event socket.

    // The native watcher never decides what changed. It only wakes the same
    // canonical reconciler used by remote Core events and file.sync.run.
    {
        let watch_settings = Arc::clone(&settings);
        let sender = trigger_tx.clone();
        thread::Builder::new()
            .name("og-file-sync-watcher".to_owned())
            .spawn(move || run_file_sync_watcher(watch_settings, sender))
            .ok();
    }

    let _ = trigger_tx.send("startup".to_owned());
    let worker_events = Arc::clone(&event_engine);
    thread::Builder::new()
        .name("og-file-sync-worker".to_owned())
        .spawn(move || {
            while let Ok(mut trigger) = trigger_rx.recv() {
                if let Ok(mut runtime) = settings.file_sync.lock() {
                    runtime.worker_pending = true;
                    runtime.last_trigger = Some(trigger.clone());
                }

                // Quiet-period debounce. Large copies or remote batches therefore
                // collapse to one reconciliation rather than one run per event.
                while let Ok(next) = trigger_rx.recv_timeout(Duration::from_millis(350)) {
                    trigger = next;
                    if let Ok(mut runtime) = settings.file_sync.lock() {
                        runtime.last_trigger = Some(trigger.clone());
                    }
                }

                let configured = settings.file_sync.lock().ok().is_some_and(|runtime| runtime.config.is_some());
                if !configured {
                    if let Ok(mut runtime) = settings.file_sync.lock() {
                        runtime.worker_pending = false;
                    }
                    continue;
                }

                let Some(credential) = settings.upstream_identity.as_ref() else {
                    // Standalone Core has no daemon-owned Identity. Manual file.sync.run
                    // remains available from an authenticated local session.
                    if let Ok(mut runtime) = settings.file_sync.lock() {
                        runtime.worker_pending = false;
                        runtime.last_run_error = Some("continuous sync is waiting for a daemon node Identity".to_owned());
                    }
                    continue;
                };

                worker_events.publish_global("file.sync.started", serde_json::json!({"trigger": trigger}));
                settings.file_sync_applying.store(true, Ordering::Release);
                settings.file_sync_watcher_dirty.store(false, Ordering::Release);
                let result = run_file_sync_once(&settings, &engine, &credential.identity_id, &credential.device_id);
                settings.file_sync_applying.store(false, Ordering::Release);
                let watcher_changed_during_run = settings.file_sync_watcher_dirty.swap(false, Ordering::AcqRel);
                if let Ok(mut runtime) = settings.file_sync.lock() {
                    runtime.worker_pending = false;
                }
                if watcher_changed_during_run {
                    if let Ok(runtime) = settings.file_sync.lock() {
                        file_sync_signal(&runtime, "filesystem.post_sync");
                    }
                }
                match result {
                    Ok((completed_at, summary)) => {
                        worker_events.publish_global("file.sync.completed", serde_json::json!({
                            "completedAt": completed_at,
                            "summary": summary,
                        }));
                    }
                    Err(error) => {
                        worker_events.publish_global("file.sync.failed", serde_json::json!({"error": error}));
                    }
                }
            }
        })
        .ok()
}

#[cfg(feature = "files-watch")]
pub(super) fn run_file_sync_watcher(settings: Arc<ConnectionSettings>, sender: std::sync::mpsc::Sender<String>) {
    let callback_sender = sender.clone();
    let applying = Arc::clone(&settings.file_sync_applying);
    let dirty = Arc::clone(&settings.file_sync_watcher_dirty);
    let mut watcher: RecommendedWatcher = match notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
        if let Ok(event) = result {
            // Access-only noise does not represent a local mutation.
            if matches!(event.kind, notify::EventKind::Access(_)) {
                return;
            }
            if applying.load(Ordering::Acquire) {
                // Remote materialization can generate thousands of native events
                // for one large file. Remember only that one verification pass is
                // needed after the current reconciliation.
                dirty.store(true, Ordering::Release);
                return;
            }
            if !event.paths.is_empty() && event.paths.iter().all(|path| {
                path.file_name().and_then(|name| name.to_str()).is_some_and(|name| name.contains(".ogsync-") && name.ends_with(".part"))
            }) {
                return;
            }
            let _ = callback_sender.send("filesystem.changed".to_owned());
        }
    }) {
        Ok(watcher) => watcher,
        Err(error) => {
            if let Ok(mut runtime) = settings.file_sync.lock() {
                runtime.local_watcher = format!("error: {error}");
            }
            return;
        }
    };

    let mut watched_root: Option<PathBuf> = None;
    loop {
        let root = settings
            .file_sync
            .lock()
            .ok()
            .and_then(|runtime| runtime.config.as_ref().map(|config| config.root.clone()));
        if root != watched_root {
            if let Some(previous) = watched_root.take() {
                let _ = watcher.unwatch(&previous);
            }
            match root {
                Some(root) => {
                    if !root.is_dir() {
                        if let Ok(mut runtime) = settings.file_sync.lock() {
                            runtime.local_watcher = "waiting_root".to_owned();
                            runtime.last_run_error = Some(format!(
                                "Files sync root {} is unavailable; restore or reconfigure it",
                                root.display()
                            ));
                        }
                    } else if let Err(error) = watcher.watch(&root, RecursiveMode::Recursive).map_err(io::Error::other) {
                        if let Ok(mut runtime) = settings.file_sync.lock() {
                            runtime.local_watcher = format!("error: {error}");
                        }
                    } else {
                        watched_root = Some(root);
                        if let Ok(mut runtime) = settings.file_sync.lock() {
                            runtime.local_watcher = "active".to_owned();
                        }
                        let _ = sender.send("filesystem.root.changed".to_owned());
                    }
                }
                None => {
                    if let Ok(mut runtime) = settings.file_sync.lock() {
                        runtime.local_watcher = "waiting_config".to_owned();
                    }
                }
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
}

#[cfg(not(feature = "files-watch"))]
pub(super) fn run_file_sync_watcher(settings: Arc<ConnectionSettings>, _sender: std::sync::mpsc::Sender<String>) {
    if let Ok(mut runtime) = settings.file_sync.lock() {
        runtime.local_watcher = "not_compiled".to_owned();
    }
}


#[derive(Debug)]
pub(super) struct FileSyncRuntime {
    pub(super) config_path: PathBuf,
    pub(super) index_path: PathBuf,
    pub(super) config: Option<FileSyncConfig>,
    pub(super) last_run_at: Option<u64>,
    pub(super) last_run_error: Option<String>,
    pub(super) worker_running: bool,
    pub(super) worker_pending: bool,
    pub(super) remote_events: String,
    pub(super) local_watcher: String,
    pub(super) last_trigger: Option<String>,
    pub(super) trigger: Option<std::sync::mpsc::Sender<String>>,
}

impl FileSyncRuntime {
    pub(super) fn disabled(configuration: &Configuration) -> Self {
        Self {
            config_path: configuration.files_sync_config_path.clone(),
            index_path: configuration.files_sync_index_path.clone(),
            config: None,
            last_run_at: None,
            last_run_error: None,
            worker_running: false,
            worker_pending: false,
            remote_events: "disabled".to_owned(),
            local_watcher: "disabled".to_owned(),
            last_trigger: None,
            trigger: None,
        }
    }
}

pub(super) fn load_file_sync_runtime(configuration: &Configuration) -> Result<FileSyncRuntime, DaemonError> {
    let config_path = configuration.files_sync_config_path.clone();
    let index_path = configuration.files_sync_index_path.clone();
    let mut config = FileSyncConfig::load(&config_path).map_err(|error| {
        DaemonError::Runtime(format!(
            "cannot load Files sync configuration {}: {error}",
            config_path.display()
        ))
    })?;

    if config.is_none() {
        if let Some(root) = configuration.files_sync_root.as_ref() {
            let bootstrap = FileSyncConfig::new(root.clone());
            fs::create_dir_all(&bootstrap.root).map_err(|source| {
                DaemonError::PrepareStorageDirectory {
                    path: bootstrap.root.clone(),
                    source,
                }
            })?;
            bootstrap.save(&config_path).map_err(|error| {
                DaemonError::Runtime(format!(
                    "cannot persist Files sync configuration {}: {error}",
                    config_path.display()
                ))
            })?;
            config = Some(bootstrap);
        }
    }

    if let Some(config) = config.as_ref() {
        if config.root.exists() && !config.root.is_dir() {
            return Err(DaemonError::Runtime(format!(
                "Files sync root {} exists but is not a directory",
                config.root.display()
            )));
        }
        debug::log( DebugTopic::Core, None, if config.root.is_dir() { format!("Files sync configured at {}", config.root.display()) } else { format!("Files sync root unavailable at {}; waiting for restore/reconfiguration", config.root.display()) }, );
    }

    Ok(FileSyncRuntime {
        config_path,
        index_path,
        config,
        last_run_at: None,
        last_run_error: None,
        worker_running: false,
        worker_pending: false,
        remote_events: "disabled".to_owned(),
        local_watcher: "starting".to_owned(),
        last_trigger: None,
        trigger: None,
    })
}
