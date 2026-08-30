//! Single source of truth for built-in operations.
//!
//! Each entry declares its wire name, access contract, execution mode, handler domain and typed payload once.
//! The catalog and router consume this list to generate their own views.

macro_rules! operation_definitions {
    ($consumer:ident) => {
        $consumer! {
            CORE_HEALTH => CoreHealth, "core.health", AccessPolicy::Public, ExecutionMode::Standard, HandlerKind::Core, UncheckedInput;
            NODE_STATUS => NodeStatus, "node.status", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Core, EmptyInput;
            PING => Ping, "ping", AccessPolicy::Public, ExecutionMode::Standard, HandlerKind::Core, UncheckedInput;
            QUERY_EXECUTE => QueryExecute, "query.execute", AccessPolicy::Query, ExecutionMode::Query, HandlerKind::Query, QueryExecuteInput;
            QUERY_CONTEXT_RESOLVE => QueryContextResolve, "query.context.resolve", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, QueryContextResolveInput;
            AUTH_BEGIN => AuthBegin, "auth.begin", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, AuthBeginInput;
            AUTH_COMPLETE => AuthComplete, "auth.complete", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, ChallengeSignatureInput;
            AUTH_ENROLL_BEGIN => AuthEnrollBegin, "auth.enroll.begin", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, AuthEnrollBeginInput;
            AUTH_ENROLL_COMPLETE => AuthEnrollComplete, "auth.enroll.complete", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, ChallengeSignatureInput;
            AUTH_CLASSIC_REGISTER => AuthClassicRegister, "auth.classic.register", AccessPolicy::Authenticated, ExecutionMode::Authentication, HandlerKind::Authentication, ClassicAuthRegisterInput;
            AUTH_CLASSIC_LOGIN => AuthClassicLogin, "auth.classic.login", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, ClassicAuthLoginInput;
            EVENTS_SUBSCRIBE => EventsSubscribe, "events.subscribe", AccessPolicy::Permission { action: AuthorizationAction::EventsSubscribe, resource: "*" }, ExecutionMode::Subscription, HandlerKind::Subscription, EventsSubscribeInput;
            IDENTITY_REGISTER => IdentityRegister, "identity.register", AccessPolicy::Permission { action: AuthorizationAction::IdentityManage, resource: "_identities" }, ExecutionMode::Standard, HandlerKind::Identity, IdentityRegisterInput;
            IDENTITY_OPEN => IdentityOpen, "identity.open", AccessPolicy::Public, ExecutionMode::Authentication, HandlerKind::Authentication, IdentityOpenInput;
            IDENTITY_GET => IdentityGet, "identity.get", AccessPolicy::Authenticated, ExecutionMode::Authentication, HandlerKind::Authentication, PasswordInput;
            IDENTITY_RENEW => IdentityRenew, "identity.renew", AccessPolicy::Authenticated, ExecutionMode::Authentication, HandlerKind::Authentication, IdentityRenewInput;
            DEVICE_REGISTER => DeviceRegister, "device.register", AccessPolicy::DynamicPermission(AuthorizationAction::DeviceManage), ExecutionMode::Standard, HandlerKind::Device, DeviceRegisterInput;
            DEVICE_LIST => DeviceList, "device.list", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Device, EmptyInput;
            DEVICE_RENAME => DeviceRename, "device.rename", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Device, DeviceRenameInput;
            DEVICE_REVOKE => DeviceRevoke, "device.revoke", AccessPolicy::DynamicPermission(AuthorizationAction::DeviceManage), ExecutionMode::Standard, HandlerKind::Device, DeviceRevokeInput;
            DEVICE_IDENTIFY => DeviceIdentify, "device.identify", AccessPolicy::Authenticated, ExecutionMode::Authentication, HandlerKind::Authentication, EmptyInput;
            PERMISSION_GRANT => PermissionGrant, "permission.grant", AccessPolicy::Permission { action: AuthorizationAction::PermissionManage, resource: "_permissions" }, ExecutionMode::Standard, HandlerKind::Permission, PermissionGrantInput;
            PERMISSION_REVOKE => PermissionRevoke, "permission.revoke", AccessPolicy::Permission { action: AuthorizationAction::PermissionManage, resource: "_permissions" }, ExecutionMode::Standard, HandlerKind::Permission, PermissionRevokeInput;
            SHARING_CREATE => SharingCreate, "sharing.create", AccessPolicy::Permission { action: AuthorizationAction::SharingManage, resource: "_sharings" }, ExecutionMode::Standard, HandlerKind::Sharing, SharingCreateInput;
            SHARING_UPDATE => SharingUpdate, "sharing.update", AccessPolicy::Permission { action: AuthorizationAction::SharingManage, resource: "_sharings" }, ExecutionMode::Standard, HandlerKind::Sharing, SharingUpdateInput;
            SHARING_DELETE => SharingDelete, "sharing.delete", AccessPolicy::Permission { action: AuthorizationAction::SharingManage, resource: "_sharings" }, ExecutionMode::Standard, HandlerKind::Sharing, SharingDeleteInput;
            PLACE_CREATE => PlaceCreate, "place.create", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, PlaceCreateInput;
            PLACE_LIST => PlaceList, "place.list", AccessPolicy::Public, ExecutionMode::Standard, HandlerKind::Place, EmptyInput;
            PLACE_GET => PlaceGet, "place.get", AccessPolicy::Public, ExecutionMode::Standard, HandlerKind::Place, PlaceIdInput;
            PLACE_UPDATE => PlaceUpdate, "place.update", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, PlaceUpdateInput;
            PLACE_DELETE => PlaceDelete, "place.delete", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, PlaceDeleteInput;
            PLACE_ACCESS_LIST => PlaceAccessList, "place.access.list", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, PlaceIdInput;
            PLACE_ACCESS_SET => PlaceAccessSet, "place.access.set", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, PlaceAccessSetInput;
            PLACE_ACCESS_REMOVE => PlaceAccessRemove, "place.access.remove", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, PlaceAccessRemoveInput;
            PLACE_PUBLIC_SET => PlacePublicSet, "place.public.set", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, PlacePublicSetInput;
            PLACE_RESOURCE_LIST => PlaceResourceList, "place.resource.list", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, PlaceIdInput;
            PLACE_RESOURCE_SET => PlaceResourceSet, "place.resource.set", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, PlaceResourceSetInput;
            PLACE_RESOURCE_REMOVE => PlaceResourceRemove, "place.resource.remove", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::Place, PlaceResourceRemoveInput;
            APP_CREATE => AppCreate, "app.create", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, AppCreateInput;
            APP_LIST => AppList, "app.list", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, UncheckedInput;
            APP_GET => AppGet, "app.get", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, AppIdInput;
            APP_UPDATE => AppUpdate, "app.update", AccessPolicy::DynamicPermission(AuthorizationAction::AppManage), ExecutionMode::Standard, HandlerKind::App, AppUpdateInput;
            APP_DELETE => AppDelete, "app.delete", AccessPolicy::DynamicPermission(AuthorizationAction::AppManage), ExecutionMode::Standard, HandlerKind::App, AppDeleteInput;
            APP_INSTANCE_CREATE => AppInstanceCreate, "app.instance.create", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, AppInstanceCreateInput;
            APP_INSTANCE_LIST => AppInstanceList, "app.instance.list", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, PlaceIdInput;
            APP_INSTANCE_REMOVE => AppInstanceRemove, "app.instance.remove", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, AppInstanceRemoveInput;
            DATA_ANALYZE => DataAnalyze, "data.analyze", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, DataAnalyzeInput;
            DATA_IMPORT => DataImport, "data.import", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, DataImportInput;
            DATA_WORKER_RUN => DataWorkerRun, "data.worker.run", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, DataWorkerRunInput;
            DATA_MAPPING_SAVE => DataMappingSave, "data.mapping.save", AccessPolicy::Authenticated, ExecutionMode::Standard, HandlerKind::App, DataMappingSaveInput;
            FILE_CAPABILITIES => FileCapabilities, "file.capabilities", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileScopeInput;
            FILE_LIST => FileList, "file.list", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileListInput;
            FILE_STAT => FileStat, "file.stat", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileEntryInput;
            FILE_MKDIR => FileMkdir, "file.mkdir", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileMkdirInput;
            FILE_MOVE => FileMove, "file.move", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileMoveInput;
            FILE_COPY => FileCopy, "file.copy", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileMoveInput;
            FILE_DELETE => FileDelete, "file.delete", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileEntryInput;
            FILE_TRASH_LIST => FileTrashList, "file.trash.list", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileScopeInput;
            FILE_RESTORE => FileRestore, "file.restore", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileEntryInput;
            FILE_DELETE_PERMANENT => FileDeletePermanent, "file.delete.permanent", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileEntryInput;
            FILE_TRASH_EMPTY => FileTrashEmpty, "file.trash.empty", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileScopeInput;
            FILE_READ => FileRead, "file.read", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileReadInput;
            FILE_WRITE => FileWrite, "file.write", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileWriteInput;
            FILE_VERSIONS => FileVersions, "file.versions", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileEntryInput;
            FILE_VERSION_READ => FileVersionRead, "file.version.read", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileVersionReadInput;
            FILE_VERSION_RESTORE => FileVersionRestore, "file.version.restore", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileVersionInput;
            FILE_VERSION_DELETE => FileVersionDelete, "file.version.delete", AccessPolicy::Authenticated, ExecutionMode::File, HandlerKind::File, FileVersionInput;
            COLLECTIONS_LIST => CollectionsList, "collections.list", AccessPolicy::Permission { action: AuthorizationAction::CollectionsList, resource: "*" }, ExecutionMode::Standard, HandlerKind::Collections, CollectionsListInput;
            STORAGE_STATS => StorageStats, "storage.stats", AccessPolicy::Permission { action: AuthorizationAction::StorageStats, resource: "*" }, ExecutionMode::Standard, HandlerKind::Storage, EmptyInput;
            BACKUP_CREATE => BackupCreate, "backup.create", AccessPolicy::Permission { action: AuthorizationAction::BackupManage, resource: "*" }, ExecutionMode::Standard, HandlerKind::Backup, BackupNameInput;
            BACKUP_INSPECT => BackupInspect, "backup.inspect", AccessPolicy::Permission { action: AuthorizationAction::BackupManage, resource: "*" }, ExecutionMode::Standard, HandlerKind::Backup, BackupNameInput;
            BACKUP_RESTORE => BackupRestore, "backup.restore", AccessPolicy::Permission { action: AuthorizationAction::BackupManage, resource: "*" }, ExecutionMode::Standard, HandlerKind::Backup, BackupRestoreInput;
        }
    };
}

pub(crate) use operation_definitions;
