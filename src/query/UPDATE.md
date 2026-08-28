Voici le déroulé que je recommande, fichier par fichier, pour mettre à jour `lookup`, `union` et `load` sans casser le moteur.

## 1. AST et modèle syntaxique

Premier bloc à modifier :

1. `ast.rs` ou le fichier qui contient `Stage`, `Pipeline`, `Lookup`, `Union`, `Load`
2. éventuellement `query.rs` ou `statement.rs` si `on/from` et les pipelines y sont définis

Objectif :

* rendre `Pipeline` récursif ;
* ajouter les stages composés ;
* ajouter `end`, `with`, `chunk`, `into` dans le modèle syntaxique ;
* définir les modes de chargement.

Forme cible approximative :

```rust
pub struct Pipeline {
    pub stages: Vec<Stage>,
}

pub enum Stage {
    Where(WhereStage),
    Lookup(LookupStage),
    Union(UnionStage),
    Load(LoadStage),
    Into(IntoStage),
    With(WithStage),
    Chunk(ChunkStage),
    // ...
}

pub struct LookupStage {
    pub source: Source,
    pub pipeline: Pipeline,
}

pub struct UnionStage {
    pub pipeline: Pipeline,
}

pub struct LoadStage {
    pub pipeline: Pipeline,
}

pub enum LoadMode {
    Replace,
    Update,
    Merge,
}
```

`end` ne doit probablement pas rester dans l’AST final : il sert au parseur à fermer un contexte.

---

## 2. Tokens du lexer

Ensuite :

3. `token.rs`
4. `lexer.rs`

Mots-clés à vérifier ou ajouter :

```text
lookup
union
load
with
replace
update
merge
chunk
into
end
```

Objectif :

* reconnaître tous les mots-clés ;
* ne pas traiter `end` comme un stage ordinaire ;
* préserver les retours à la ligne si le parseur en dépend ;
* garder `|` comme séparateur universel.

---

## 3. Parseur de pipeline

Puis le cœur du changement :

5. `parser.rs`
6. éventuellement `pipeline_parser.rs`
7. éventuellement `stage_parser.rs`

Objectif :

* rendre le parsing récursif ;
* ouvrir un sous-pipeline sur `lookup`, `union` ou `load` ;
* fermer ce sous-pipeline sur `end` ;
* permettre les imbrications.

La fonction centrale devrait ressembler conceptuellement à :

```rust
fn parse_pipeline(&mut self, stop_on_end: bool) -> Result<Pipeline, ParseError>
```

Logique :

```text
lookup -> parse lookup header
          parse nested pipeline until end

union  -> parse nested pipeline until end

load   -> parse nested pipeline until end

end    -> return current nested pipeline
```

Le parser devra aussi distinguer :

```text
on users
| load x, y with replace
```

de :

```text
on users
| load
    | with replace
    | chunk x
    | chunk y
| end
```

La forme compacte pourra être normalisée vers la forme composée.

---

## 4. Validation syntaxique et sémantique

Ensuite :

8. `validation.rs`
9. éventuellement `semantic_analyzer.rs`
10. éventuellement `diagnostics.rs`

Règles à ajouter :

### `lookup`

* `into` obligatoire ;
* `into` unique ;
* `into` placé dans le sous-pipeline ;
* la source du lookup doit être définie ;
* les alias parent et lookup doivent être accessibles.

### `union`

* le sous-pipeline doit produire des documents ;
* `into`, `with` et `chunk` ne sont pas valides directement dans un `union`, sauf s’ils appartiennent à un stage composé imbriqué.

### `load`

* `with` obligatoire ;
* `with` unique ;
* mode parmi `replace`, `update`, `merge` ;
* au moins un `chunk`, sauf si la forme compacte fournit déjà des documents ;
* `with` doit précéder les chunks ;
* `_id` obligatoire pour `update` et `merge`, au moins au moment de l’exécution ;
* `load` doit avoir une collection cible.

---

## 5. Normalisation

Puis :

11. `normalizer.rs`
12. éventuellement `normalized_ast.rs`

Objectif :

* supprimer les différences entre syntaxe compacte et composée ;
* transformer les sous-pipelines génériques en structures explicites ;
* extraire `into`, `with` et `chunk`.

Par exemple :

```rust
Stage::Load {
    pipeline: Pipeline {
        stages: [
            Stage::With(Replace),
            Stage::Chunk(x),
            Stage::Chunk(y),
        ],
    },
}
```

devient :

```rust
NormalizedStage::Load {
    mode: LoadMode::Replace,
    chunks: vec![x, y],
}
```

Pour `lookup` :

```rust
NormalizedStage::Lookup {
    source,
    pipeline: candidate_pipeline,
    target,
}
```

Le `IntoStage` disparaît du sous-pipeline normalisé et devient le champ `target`.

---

## 6. Plan logique

Ensuite, les fichiers déjà connus :

13. `logical_plan.rs`
14. `planner.rs`

Nouveaux nœuds probables :

```rust
LogicalPlan::Lookup {
    input: Box<LogicalPlan>,
    source: SourcePlan,
    pipeline: Box<LogicalPlan>,
    target: FieldPath,
}

LogicalPlan::Union {
    input: Box<LogicalPlan>,
    branch: Box<LogicalPlan>,
}

LogicalPlan::Load {
    target: CollectionName,
    segment: Option<Expression>,
    mode: LoadMode,
    chunks: Vec<Expression>,
}
```

Le planner devient récursif :

```rust
fn plan_pipeline(
    &self,
    pipeline: &NormalizedPipeline,
    scope: &PlanningScope,
) -> Result<LogicalPlan, PlanError>
```

Pour `lookup`, le scope enfant doit connaître :

* l’alias parent ;
* l’alias lookup ;
* les variables accessibles.

Pour `union`, le sous-pipeline est un pipeline autonome produisant des documents.

Pour `load`, le planner doit récupérer :

* la collection cible ;
* le filtre ou segment construit avant `load` ;
* le mode ;
* les chunks.

---

## 7. Plan physique

Puis :

15. `physical_plan.rs`
16. `lowerer.rs`

Nœuds physiques probables :

```rust
PhysicalPlan::Lookup {
    input: Box<PhysicalPlan>,
    branch: Box<PhysicalPlan>,
    target: FieldPath,
}

PhysicalPlan::Union {
    input: Box<PhysicalPlan>,
    branch: Box<PhysicalPlan>,
}

PhysicalPlan::Load {
    collection: CollectionName,
    segment: Option<CompiledExpression>,
    mode: LoadMode,
    chunks: Vec<CompiledExpression>,
}
```

Le lowerer transforme récursivement les plans enfants.

---

## 8. Exécution

Puis :

17. `executor.rs`
18. `runtime.rs`
19. éventuellement `execution_context.rs`

### Exécution de `lookup`

Pour chaque document parent :

1. créer un contexte contenant le document parent ;
2. exécuter le sous-pipeline lookup ;
3. collecter les documents ;
4. écrire le tableau dans `target` ;
5. émettre le document parent enrichi.

### Exécution de `union`

1. exécuter le flux principal ;
2. exécuter le sous-pipeline ;
3. concaténer les deux flux.

Il faudra décider si l’ordre est strictement :

```text
main puis union
```

Je partirais sur cette règle.

### Exécution de `load`

1. collecter ou recevoir les chunks ;
2. valider tous les documents ;
3. ouvrir une transaction ;
4. appliquer `replace`, `update` ou `merge` ;
5. commit ;
6. rollback à la moindre erreur.

---

## 9. Interface de stockage

C’est probablement le bloc le plus structurant après le parser :

20. `storage.rs`
21. éventuellement `collection.rs`
22. éventuellement `transaction.rs`
23. éventuellement l’implémentation mémoire ou disque correspondante

Méthodes probables :

```rust
trait Storage {
    fn replace_collection(
        &mut self,
        collection: &str,
        documents: Vec<Document>,
    ) -> Result<MutationResult, StorageError>;

    fn replace_segment(
        &mut self,
        collection: &str,
        predicate: &CompiledExpression,
        documents: Vec<Document>,
    ) -> Result<MutationResult, StorageError>;

    fn update_documents(
        &mut self,
        collection: &str,
        documents: Vec<Document>,
        guard: Option<&CompiledExpression>,
    ) -> Result<MutationResult, StorageError>;

    fn merge_documents(
        &mut self,
        collection: &str,
        documents: Vec<Document>,
        guard: Option<&CompiledExpression>,
    ) -> Result<MutationResult, StorageError>;
}
```

Il faudra peut-être aussi une abstraction transactionnelle explicite.

---

## 10. Résultat d’exécution et protocole

Enfin :

24. `execution_result.rs` ou équivalent
25. côté `ogd`, le fichier de réponse protocolaire
26. éventuellement les types de requête/réponse de `ogcli`

`lookup` et `union` produisent des documents.

`load` produit plutôt un résultat de mutation :

```rust
pub struct MutationResult {
    pub inserted: usize,
    pub updated: usize,
    pub deleted: usize,
    pub matched: usize,
}
```

Le protocole devra donc distinguer :

```rust
ExecutionResult::Documents(Vec<Document>)
ExecutionResult::Mutation(MutationResult)
```

---

# Ordre concret de travail

À notre rythme habituel, je suivrais exactement cet ordre :

```text
1. ast.rs
2. token.rs
3. lexer.rs
4. parser.rs
5. normalizer.rs
6. logical_plan.rs
7. planner.rs
8. physical_plan.rs
9. lowerer.rs
10. storage.rs
11. executor.rs
12. runtime.rs
13. tests parser
14. tests planner
15. tests executor
```

Les fichiers réellement nécessaires dépendront légèrement de ton arborescence, mais le chemin critique est :

```text
AST
→ lexer
→ parser récursif
→ normalisation
→ logical plan récursif
→ physical plan
→ storage mutations
→ executor
```

Le premier fichier à ouvrir est donc celui qui définit actuellement `Pipeline` et `Stage`.
