# Ajouter une opération à `og-core`

Cette note décrit la procédure avec l’architecture actuelle des opérations.

L’objectif de la refonte est qu’une opération soit décrite **une seule fois** pour tout ce qui est structurel :

- nom wire ;
- `OperationKind` ;
- politique d’autorisation ;
- mode d’exécution ;
- domaine de handler ;
- type de payload.

Le routeur et le catalogue consomment ensuite cette définition automatiquement.

> **État actuel**
>
> Le handler métier n’est pas encore un pointeur de fonction stocké dans la définition canonique.
> Pour une opération `Standard`, il faut donc encore ajouter son corps dans le handler de domaine correspondant dans `src/bin/ogd.rs`.
> En revanche, il n’est plus nécessaire d’ajouter manuellement un variant dans plusieurs enums ou de maintenir un second mapping dans le router.

---

## Les fichiers à modifier

Pour une opération standard avec un nouveau payload :

| Fichier | Obligatoire | Rôle |
|---|---:|---|
| `src/operation/definition.rs` | oui | Déclaration canonique de l’opération |
| `src/operation/payload.rs` | oui si nouveau payload | Type wire + validation/normalisation |
| `src/bin/ogd.rs` | oui actuellement | Corps métier dans le handler du domaine |
| `src/access/authorization.rs` | seulement si nouvelle action | Nouvelle `AuthorizationAction` |
| tests du domaine / router | recommandé | Validation du contrat et du comportement |

Normalement, **ne pas modifier** :

- `src/operation/catalog.rs`
- `src/operation/router.rs`
- l’enum `RoutedOperation`
- `OperationKind`

Ces éléments sont générés à partir de `operation_definitions!`.

---

# Exemple : ajouter `app.archive`

On veut ajouter :

```text
app.archive
```

Payload wire :

```json
{
  "appId": "my-app",
  "archivedAt": 1234567890
}
```

Règles :

- identité authentifiée ;
- permission dynamique `app.manage` sur `appId` ;
- exécution standard ;
- handler dans le domaine `App`.

---

## 1. Définir le payload

Dans :

```text
src/operation/payload.rs
```

Ajouter :

```rust
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppArchiveInput {
    pub app_id: String,

    #[serde(default)]
    pub archived_at: Option<u64>,
}
```

Puis déclarer sa validation :

```rust
validate_fields!(
    AppArchiveInput =>
        app_id: "appId"
);
```

Si la validation est plus complexe, implémenter directement :

```rust
impl OperationPayload for AppArchiveInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "appId", &self.app_id)?;

        // Exemple de normalisation supplémentaire :
        // self.some_field = self.some_field.trim().to_owned();

        Ok(())
    }
}
```

### Où doit vivre la validation ?

La règle est :

> Tout invariant propre au payload doit vivre dans `payload.rs`, pas dans le router.

Exemples :

- champ obligatoire ;
- chaîne non vide ;
- combinaison de champs interdite ;
- normalisation ;
- valeur par défaut logique ;
- déduplication d’une liste.

Le router appelle automatiquement :

```rust
input.validate(operation)?;
```

après la désérialisation.

---

## 2. Ajouter l’opération à la définition canonique

Dans :

```text
src/operation/definition.rs
```

Ajouter une entrée à `operation_definitions!` :

```rust
APP_ARCHIVE => AppArchive,
"app.archive",
AccessPolicy::DynamicPermission(AuthorizationAction::AppManage),
ExecutionMode::Standard,
HandlerKind::App,
AppArchiveInput;
```

Sous sa forme compacte :

```rust
APP_ARCHIVE => AppArchive, "app.archive",
    AccessPolicy::DynamicPermission(AuthorizationAction::AppManage),
    ExecutionMode::Standard,
    HandlerKind::App,
    AppArchiveInput;
```

Cette **unique entrée** alimente automatiquement :

- la constante `APP_ARCHIVE` ;
- `OperationKind::AppArchive` ;
- `OperationKind::ALL` ;
- le descriptor ;
- `operation_by_name("app.archive")` ;
- `RoutedOperation::AppArchive(Routed<AppArchiveInput>)` ;
- le décodage du payload ;
- la validation du payload ;
- `operation.execution_mode()` ;
- `operation.handler()`.

Il ne faut donc pas recréer manuellement ces éléments ailleurs.

---

# Choisir `AccessPolicy`

Les politiques actuelles sont :

```rust
pub enum AccessPolicy {
    Public,
    Authenticated,
    Query,
    Permission {
        action: AuthorizationAction,
        resource: &'static str,
    },
    DynamicPermission(AuthorizationAction),
}
```

## `Public`

Aucune identité requise.

Exemple :

```rust
AccessPolicy::Public
```

Utilisé notamment pour des opérations comme :

```text
core.health
ping
auth.begin
```

---

## `Authenticated`

Une identité authentifiée est nécessaire, mais l’accès métier est ensuite résolu par le handler.

```rust
AccessPolicy::Authenticated
```

Exemple typique :

```text
place.get
```

Le handler peut ensuite vérifier le rôle de l’identité dans le Place.

---

## `Permission`

Permission statique avec une ressource connue à l’avance.

```rust
AccessPolicy::Permission {
    action: AuthorizationAction::StorageStats,
    resource: "*",
}
```

Ou :

```rust
AccessPolicy::Permission {
    action: AuthorizationAction::PermissionManage,
    resource: "_permissions",
}
```

Cette politique peut être vérifiée par le preflight générique sans connaître le payload.

---

## `DynamicPermission`

La permission est connue, mais la ressource vient du payload ou du domaine.

```rust
AccessPolicy::DynamicPermission(
    AuthorizationAction::AppManage
)
```

Exemple :

```text
app.archive
```

où la ressource est :

```rust
input.app_id
```

Dans ce cas, le handler utilise le mécanisme d’autorisation dynamique existant du domaine App.

---

## `Query`

Réservé au pipeline :

```text
query.execute
```

L’accès dépend du plan/query et n’est pas une simple permission statique.

---

# Choisir `ExecutionMode`

Les modes actuels :

```rust
pub enum ExecutionMode {
    Standard,
    Query,
    Authentication,
    Subscription,
    File,
}
```

La majorité des nouvelles opérations métier doivent être :

```rust
ExecutionMode::Standard
```

Choisir un autre mode uniquement si l’opération appartient réellement à un protocole spécial :

| Mode | Usage |
|---|---|
| `Standard` | opération RPC métier classique |
| `Query` | exécution du langage de query |
| `Authentication` | handshake / credential flow |
| `Subscription` | connexion événementielle longue |
| `File` | opérations File, notamment streaming |

Ne pas créer un nouveau mode simplement pour organiser un domaine métier. Les domaines sont représentés par `HandlerKind`.

---

# Choisir `HandlerKind`

Les domaines actuels incluent notamment :

```rust
HandlerKind::Core
HandlerKind::Collections
HandlerKind::Storage
HandlerKind::Backup
HandlerKind::Identity
HandlerKind::Device
HandlerKind::Permission
HandlerKind::Sharing
HandlerKind::Place
HandlerKind::App
```

Pour notre exemple :

```rust
HandlerKind::App
```

Cela signifie :

> `app.archive` est une opération Standard gérée par le domaine App.

---

## 3. Implémenter le comportement

Dans l’état actuel, le corps métier des opérations standard se trouve dans le handler standard de :

```text
src/bin/ogd.rs
```

Repérer :

```rust
HandlerKind::App => match operation {
```

et ajouter :

```rust
RoutedOperation::AppArchive(Routed {
    id,
    input: AppArchiveInput {
        app_id,
        archived_at,
    },
}) => {
    let identity_id =
        identity_or_reject!(
            id,
            "an authenticated identity is required to archive an App"
        );

    authorize_resource_or_reject!(
        id,
        OperationKind::AppArchive,
        &app_id
    );

    let _ = or_reject!(
        load_app_definition(engine, id, &app_id)
    );

    let archived_at =
        archived_at.unwrap_or_else(unix_time_millis);

    let query = format!(
        "on _apps \
         | where appId == {} and state == \"active\" \
         | set state = \"archived\", archivedBy = {}, archivedAt = {archived_at}",
        query_string(&app_id),
        query_string(identity_id),
    );

    execute_publish_reply!(
        id,
        query,
        Audience::Global,
        "app.archived",
        serde_json::json!({
            "appId": app_id,
            "archivedBy": identity_id,
            "archivedAt": archived_at
        }),
        serde_json::json!({
            "appId": app_id,
            "state": "archived",
            "archivedBy": identity_id,
            "archivedAt": archived_at
        }),
    );
}
```

Le variant :

```rust
RoutedOperation::AppArchive(...)
```

existe automatiquement grâce à l’entrée de `definition.rs`.

---

# Ajouter une nouvelle action d’autorisation

Si l’opération peut réutiliser une action existante, **ne rien ajouter**.

Exemple :

```rust
AuthorizationAction::AppManage
```

convient à `app.archive`.

Si une nouvelle capacité est réellement nécessaire, par exemple :

```text
app.publish
```

modifier :

```text
src/access/authorization.rs
```

La définition des actions est déclarative :

```rust
authorization_actions! {
    // ...
    AppManage => "app.manage",
    AppPublish => "app.publish",
}
```

Puis l’utiliser dans l’opération :

```rust
AccessPolicy::DynamicPermission(
    AuthorizationAction::AppPublish
)
```

Éviter de créer une action ultra-spécifique par opération si plusieurs opérations représentent la même capacité métier.

---

# Cas où aucun nouveau payload n’est nécessaire

Réutiliser un type existant lorsque le contrat wire est réellement identique.

Par exemple, une opération prenant uniquement :

```json
{
  "appId": "..."
}
```

peut utiliser :

```rust
AppIdInput
```

Définition :

```rust
APP_SOMETHING => AppSomething,
"app.something",
AccessPolicy::DynamicPermission(AuthorizationAction::AppManage),
ExecutionMode::Standard,
HandlerKind::App,
AppIdInput;
```

Ne pas créer `AppSomethingInput` uniquement pour renommer le même champ.

Même principe pour :

- `EmptyInput`
- `PlaceIdInput`
- `FileEntryInput`
- `FileScopeInput`
- etc.

---

# `EmptyInput` vs `UncheckedInput`

La différence est importante.

## `EmptyInput`

Le payload doit respecter la structure vide attendue.

Utiliser lorsque l’opération n’accepte réellement aucun champ.

```rust
EmptyInput
```

## `UncheckedInput`

Le contenu wire n’est volontairement pas validé strictement.

Utilisé pour certains endpoints historiques comme :

```text
core.health
ping
app.list
```

Pour une nouvelle API, préférer généralement :

```rust
EmptyInput
```

afin de détecter les champs wire erronés.

---

# Ce que fait automatiquement le router

Après l’ajout dans `definition.rs`, `OperationRouter::route()` n’a pas besoin d’être modifié.

Il fait conceptuellement :

```rust
let kind = operation_by_name(request.op)?;

match kind {
    OperationKind::AppArchive => {
        let input =
            decode_payload::<AppArchiveInput>(request.data)?;

        input.validate("app.archive")?;

        RoutedOperation::AppArchive(
            Routed::new(request.id, input)
        )
    }

    // généré pour toutes les opérations
}
```

Ce dispatch est produit par :

```rust
operation_definitions!(dispatch_operations)
```

Il ne faut donc **pas ajouter de branche manuellement dans `router.rs`**.

---

# Tests recommandés

## Test de routage / validation

Ajouter au minimum un test qui vérifie :

```rust
let operation = OperationRouter::default()
    .route(OperationRequest {
        id: RequestId::Number(1),
        op: APP_ARCHIVE.to_owned(),
        data: serde_json::json!({
            "appId": "demo"
        }),
    })
    .unwrap();

assert!(matches!(
    operation,
    RoutedOperation::AppArchive(
        Routed {
            input: AppArchiveInput { .. },
            ..
        }
    )
));
```

Et un test payload invalide :

```rust
let result = OperationRouter::default()
    .route(OperationRequest {
        id: RequestId::Number(1),
        op: APP_ARCHIVE.to_owned(),
        data: serde_json::json!({
            "appId": ""
        }),
    });

assert!(result.is_err());
```

---

## Test d’autorisation

Tester au moins :

1. utilisateur sans permission ;
2. utilisateur avec permission correcte ;
3. ressource différente si `DynamicPermission`.

Pour notre exemple :

```text
app.manage / app-A
```

ne doit pas implicitement permettre :

```text
app.manage / app-B
```

si la politique de ressources est restrictive.

---

## Test métier

Vérifier :

- App inexistante ;
- App déjà archivée ;
- succès ;
- événement émis ;
- réponse wire ;
- mutation persistée.

---

# Checklist courte

Pour ajouter une opération standard :

```text
[ ] payload existant réutilisable ?
[ ] sinon : struct dans operation/payload.rs
[ ] OperationPayload impl / validation
[ ] entrée unique dans operation/definition.rs
[ ] AccessPolicy correcte
[ ] ExecutionMode correcte
[ ] HandlerKind correct
[ ] nouvelle AuthorizationAction seulement si nécessaire
[ ] corps métier dans le handler de domaine d’ogd.rs
[ ] test payload invalide
[ ] test routage
[ ] test autorisation
[ ] test métier
[ ] make checktest
```

---

# Le test d’architecture mental

Lors de l’ajout d’une opération, si tu te retrouves à vouloir modifier :

```text
catalog.rs
router.rs
RoutedOperation
OperationKind
```

**arrête-toi** : cela signifie probablement que tu recrées une source de vérité parallèle.

La déclaration structurante doit rester ici :

```text
src/operation/definition.rs
```

et être consommée par le reste.

---

# Résumé minimal

Pour une opération classique, les modifications normales sont :

```text
src/operation/payload.rs
src/operation/definition.rs
src/bin/ogd.rs
```

Éventuellement :

```text
src/access/authorization.rs
```

si une nouvelle action d’autorisation est nécessaire.

Le cœur de la définition tient en une ligne :

```rust
APP_ARCHIVE => AppArchive,
"app.archive",
AccessPolicy::DynamicPermission(AuthorizationAction::AppManage),
ExecutionMode::Standard,
HandlerKind::App,
AppArchiveInput;
```

Le reste du catalogue et du routage est dérivé automatiquement.
