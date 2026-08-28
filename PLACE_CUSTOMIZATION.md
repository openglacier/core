# Place customization

Owners can now persist Place presentation and App ordering in og-core.

`place.update` supports `name`, `title`, `subtitle`, `colorScheme`, and `appOrder`.
The Hub exposes these presentation fields from the Place header. `appOrder` stores AppInstance IDs and is updated by drag and drop. Native fixed Place objects are deliberately not part of this order because they are expected to become Apps themselves.

Color schemes remain inside the Hub palette: `glacier`, `sage`, `amber`, and `graphite`.
