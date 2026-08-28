# App Runtime foundation pack — Core

- Place-owned App definitions use `ownerPlaceId`.
- Maintainers are persisted as a scalar JSON string (`maintainersJson`) to avoid array-expression limitations in the current `set` query stage.
- Place Owners may change maintainers; Maintainers may update the App definition but not its maintainer set.
- Query writes remain protected by Place + AppInstance scope and Place role, so Hub visibility is never the security boundary.
- `load` continues to be classified as a write mutation and is ready for a future bulk-import primitive.
