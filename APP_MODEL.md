
## App ownership and access contract

Place-created Apps are owned by the Place, not by the identity that originally created them. `_apps.ownerPlaceId` is the durable administrative owner; `createdBy` remains audit metadata. `_apps.maintainers` is reserved for identities that may manage one App without becoming Place Owners.

Runtime primitives/actions use the declarative access levels `read`, `write`, and `manage`. Hub uses these levels for UX gating; Core remains authoritative. Place Members are read-only, Residents read/write, Place Owners read/write/manage, and App Maintainers may manage the App definition. Ownership transfer remains a separate administrative concern and is not implied by `manage`.
