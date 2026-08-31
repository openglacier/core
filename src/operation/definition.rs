//! Single source of truth for built-in operations.
//!
//! Each entry declares its wire name, access contract, execution mode, handler domain, wire transport, connection policy and typed payload once.
//! The catalog and router consume this list to generate their own views.

macro_rules! operation_definitions {
    ($consumer:ident) => {
        $consumer! {
            CORE_HEALTH => CoreHealth, "core.health", AccessPolicy::Public, ExecutionMode::Standard, HandlerKind::Core, TransportKind::Message, ConnectionKind::Shared, UncheckedInput;
            CORE_OPERATIONS => CoreOperations, "core.operations", AccessPolicy::Public, ExecutionMode::Standard, HandlerKind::Core, TransportKind::Message, ConnectionKind::Shared, EmptyInput;
            NODE_STATUS => NodeStatus, "node.status", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Core, TransportKind::Message, ConnectionKind::Shared, EmptyInput;
            PING => Ping, "ping", AccessPolicy::Public, ExecutionMode::Standard, HandlerKind::Core, TransportKind::Message, ConnectionKind::Shared, UncheckedInput;
            QUERY_EXECUTE => QueryExecute, "query.execute", AccessPolicy::Query, ExecutionMode::Query, HandlerKind::Query, TransportKind::MessageStream, ConnectionKind::Shared, QueryExecuteInput;
            QUERY_CONTEXT_RESOLVE => QueryContextResolve, "query.context.resolve", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, QueryContextResolveInput;
            AUTH_BEGIN => AuthBegin, "auth.begin", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, TransportKind::Message, ConnectionKind::Shared, AuthBeginInput;
            AUTH_COMPLETE => AuthComplete, "auth.complete", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, TransportKind::Message, ConnectionKind::Shared, ChallengeSignatureInput;
            AUTH_ENROLL_BEGIN => AuthEnrollBegin, "auth.enroll.begin", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, TransportKind::Message, ConnectionKind::Shared, AuthEnrollBeginInput;
            AUTH_ENROLL_COMPLETE => AuthEnrollComplete, "auth.enroll.complete", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, TransportKind::Message, ConnectionKind::Shared, ChallengeSignatureInput;
            AUTH_CLASSIC_REGISTER => AuthClassicRegister, "auth.classic.register", AccessPolicy::Authenticated, ExecutionMode::Authentication, HandlerKind::Authentication, TransportKind::Message, ConnectionKind::Shared, ClassicAuthRegisterInput;
            AUTH_CLASSIC_LOGIN => AuthClassicLogin, "auth.classic.login", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, TransportKind::Message, ConnectionKind::Shared, ClassicAuthLoginInput;
            EVENTS_SUBSCRIBE => EventsSubscribe, "events.subscribe", AccessPolicy::Permission { action: AuthorizationAction::EventsSubscribe, resource: "*" }, ExecutionMode::Subscription, HandlerKind::Subscription, TransportKind::Message, ConnectionKind::Persistent, EventsSubscribeInput;
            IDENTITY_REGISTER => IdentityRegister, "identity.register", AccessPolicy::Permission { action: AuthorizationAction::IdentityManage, resource: "_identities" }, ExecutionMode::Standard, HandlerKind::Identity, TransportKind::Message, ConnectionKind::Shared, IdentityRegisterInput;
            IDENTITY_OPEN => IdentityOpen, "identity.open", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, TransportKind::Message, ConnectionKind::Shared, IdentityOpenInput;
            IDENTITY_GET => IdentityGet, "identity.get", AccessPolicy::Authenticated, ExecutionMode::Authentication, HandlerKind::Authentication, TransportKind::Message, ConnectionKind::Shared, PasswordInput;
            IDENTITY_RENEW => IdentityRenew, "identity.renew", AccessPolicy::Authenticated, ExecutionMode::Authentication, HandlerKind::Authentication, TransportKind::Message, ConnectionKind::Shared, IdentityRenewInput;
            DEVICE_REGISTER => DeviceRegister, "device.register", AccessPolicy::DynamicPermission(AuthorizationAction::DeviceManage), ExecutionMode::Standard, HandlerKind::Device, TransportKind::Message, ConnectionKind::Shared, DeviceRegisterInput;
            DEVICE_LIST => DeviceList, "device.list", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Device, TransportKind::Message, ConnectionKind::Shared, EmptyInput;
            DEVICE_RENAME => DeviceRename, "device.rename", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Device, TransportKind::Message, ConnectionKind::Shared, DeviceRenameInput;
            DEVICE_REVOKE => DeviceRevoke, "device.revoke", AccessPolicy::DynamicPermission(AuthorizationAction::DeviceManage), ExecutionMode::Standard, HandlerKind::Device, TransportKind::Message, ConnectionKind::Shared, DeviceRevokeInput;
            DEVICE_IDENTIFY => DeviceIdentify, "device.identify", AccessPolicy::Authenticated, ExecutionMode::Authentication, HandlerKind::Authentication, TransportKind::Message, ConnectionKind::Shared, EmptyInput;
            PERMISSION_GRANT => PermissionGrant, "permission.grant", AccessPolicy::Permission { action: AuthorizationAction::PermissionManage, resource: "_permissions" }, ExecutionMode::Standard, HandlerKind::Permission, TransportKind::Message, ConnectionKind::Shared, PermissionGrantInput;
            PERMISSION_REVOKE => PermissionRevoke, "permission.revoke", AccessPolicy::Permission { action: AuthorizationAction::PermissionManage, resource: "_permissions" }, ExecutionMode::Standard, HandlerKind::Permission, TransportKind::Message, ConnectionKind::Shared, PermissionRevokeInput;
            SHARING_CREATE => SharingCreate, "sharing.create", AccessPolicy::Permission { action: AuthorizationAction::SharingManage, resource: "_sharings" }, ExecutionMode::Standard, HandlerKind::Sharing, TransportKind::Message, ConnectionKind::Shared, SharingCreateInput;
            SHARING_UPDATE => SharingUpdate, "sharing.update", AccessPolicy::Permission { action: AuthorizationAction::SharingManage, resource: "_sharings" }, ExecutionMode::Standard, HandlerKind::Sharing, TransportKind::Message, ConnectionKind::Shared, SharingUpdateInput;
            SHARING_DELETE => SharingDelete, "sharing.delete", AccessPolicy::Permission { action: AuthorizationAction::SharingManage, resource: "_sharings" }, ExecutionMode::Standard, HandlerKind::Sharing, TransportKind::Message, ConnectionKind::Shared, SharingDeleteInput;
            PLACE_CREATE => PlaceCreate, "place.create", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, PlaceCreateInput;
            PLACE_LIST => PlaceList, "place.list", AccessPolicy::Public, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, EmptyInput;
            PLACE_GET => PlaceGet, "place.get", AccessPolicy::Public, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, PlaceIdInput;
            PLACE_UPDATE => PlaceUpdate, "place.update", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, PlaceUpdateInput;
            PLACE_DELETE => PlaceDelete, "place.delete", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, PlaceDeleteInput;
            PLACE_ACCESS_LIST => PlaceAccessList, "place.access.list", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, PlaceIdInput;
            PLACE_ACCESS_SET => PlaceAccessSet, "place.access.set", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, PlaceAccessSetInput;
            PLACE_ACCESS_REMOVE => PlaceAccessRemove, "place.access.remove", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, PlaceAccessRemoveInput;
            PLACE_PUBLIC_SET => PlacePublicSet, "place.public.set", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, PlacePublicSetInput;
            PLACE_RESOURCE_LIST => PlaceResourceList, "place.resource.list", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, PlaceIdInput;
            PLACE_RESOURCE_SET => PlaceResourceSet, "place.resource.set", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, PlaceResourceSetInput;
            PLACE_RESOURCE_REMOVE => PlaceResourceRemove, "place.resource.remove", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, TransportKind::Message, ConnectionKind::Shared, PlaceResourceRemoveInput;
            APP_CREATE => AppCreate, "app.create", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, AppCreateInput;
            APP_LIST => AppList, "app.list", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, UncheckedInput;
            APP_GET => AppGet, "app.get", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, AppIdInput;
            APP_UPDATE => AppUpdate, "app.update", AccessPolicy::DynamicPermission(AuthorizationAction::AppManage), ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, AppUpdateInput;
            APP_DELETE => AppDelete, "app.delete", AccessPolicy::DynamicPermission(AuthorizationAction::AppManage), ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, AppDeleteInput;
            APP_INSTANCE_CREATE => AppInstanceCreate, "app.instance.create", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, AppInstanceCreateInput;
            APP_INSTANCE_LIST => AppInstanceList, "app.instance.list", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, PlaceIdInput;
            APP_INSTANCE_REMOVE => AppInstanceRemove, "app.instance.remove", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, AppInstanceRemoveInput;
            DATA_ANALYZE => DataAnalyze, "data.analyze", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, DataAnalyzeInput;
            DATA_IMPORT => DataImport, "data.import", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, DataImportInput;
            DATA_WORKER_RUN => DataWorkerRun, "data.worker.run", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::BinaryIn, ConnectionKind::Exclusive, DataWorkerRunInput;
            DATA_MAPPING_SAVE => DataMappingSave, "data.mapping.save", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, TransportKind::Message, ConnectionKind::Shared, DataMappingSaveInput;
            FILE_CAPABILITIES => FileCapabilities, "file.capabilities", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileScopeInput;
            FILE_SYNC_CONFIG_GET => FileSyncConfigGet, "file.sync.config.get", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, EmptyInput;
            FILE_SYNC_CONFIG_SET => FileSyncConfigSet, "file.sync.config.set", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileSyncConfigSetInput;
            FILE_SYNC_SELECTION_SET => FileSyncSelectionSet, "file.sync.selection.set", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileSyncSelectionSetInput;
            FILE_SYNC_SELECTION_REMOVE => FileSyncSelectionRemove, "file.sync.selection.remove", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileSyncSelectionRemoveInput;
            FILE_SYNC_STATUS => FileSyncStatus, "file.sync.status", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, EmptyInput;
            FILE_SYNC_RUN => FileSyncRun, "file.sync.run", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, EmptyInput;
            FILE_SYNC_FOLDERS => FileSyncFolders, "file.sync.folders", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileListInput;
            FILE_LIST => FileList, "file.list", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileListInput;
            FILE_STAT => FileStat, "file.stat", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileEntryInput;
            FILE_MKDIR => FileMkdir, "file.mkdir", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileMkdirInput;
            FILE_MOVE => FileMove, "file.move", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileMoveInput;
            FILE_COPY => FileCopy, "file.copy", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileMoveInput;
            FILE_DELETE => FileDelete, "file.delete", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileEntryInput;
            FILE_TRASH_LIST => FileTrashList, "file.trash.list", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileScopeInput;
            FILE_RESTORE => FileRestore, "file.restore", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileEntryInput;
            FILE_DELETE_PERMANENT => FileDeletePermanent, "file.delete.permanent", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileEntryInput;
            FILE_TRASH_EMPTY => FileTrashEmpty, "file.trash.empty", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileScopeInput;
            FILE_READ => FileRead, "file.read", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::BinaryOut, ConnectionKind::Exclusive, FileReadInput;
            FILE_WRITE => FileWrite, "file.write", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::BinaryIn, ConnectionKind::Exclusive, FileWriteInput;
            FILE_VERSIONS => FileVersions, "file.versions", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileEntryInput;
            FILE_VERSION_READ => FileVersionRead, "file.version.read", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::BinaryOut, ConnectionKind::Exclusive, FileVersionReadInput;
            FILE_VERSION_RESTORE => FileVersionRestore, "file.version.restore", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileVersionInput;
            FILE_VERSION_DELETE => FileVersionDelete, "file.version.delete", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, TransportKind::Message, ConnectionKind::Shared, FileVersionInput;
            COLLECTIONS_LIST => CollectionsList, "collections.list", AccessPolicy::Permission { action: AuthorizationAction::CollectionsList, resource: "*" }, ExecutionMode::Standard, HandlerKind::Collections, TransportKind::Message, ConnectionKind::Shared, CollectionsListInput;
            STORAGE_STATS => StorageStats, "storage.stats", AccessPolicy::Permission { action: AuthorizationAction::StorageStats, resource: "*" }, ExecutionMode::Standard, HandlerKind::Storage, TransportKind::Message, ConnectionKind::Shared, EmptyInput;
            BACKUP_CREATE => BackupCreate, "backup.create", AccessPolicy::Permission { action: AuthorizationAction::BackupManage, resource: "*" }, ExecutionMode::Standard, HandlerKind::Backup, TransportKind::Message, ConnectionKind::Shared, BackupNameInput;
            BACKUP_INSPECT => BackupInspect, "backup.inspect", AccessPolicy::Permission { action: AuthorizationAction::BackupManage, resource: "*" }, ExecutionMode::Standard, HandlerKind::Backup, TransportKind::Message, ConnectionKind::Shared, BackupNameInput;
            BACKUP_RESTORE => BackupRestore, "backup.restore", AccessPolicy::Permission { action: AuthorizationAction::BackupManage, resource: "*" }, ExecutionMode::Standard, HandlerKind::Backup, TransportKind::Message, ConnectionKind::Shared, BackupRestoreInput;
        }
    };
}

pub(crate) use operation_definitions;
