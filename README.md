# Git Larder

"A synonym of Git Storage"

## Wrapped NoSQL Built On Git

GitLarder is a dead-simple flat-file storage mechanism meant for data needing
strict version control with few writes. It's built straight on top of git and
works alongside it.

### Usage

A GitRecordFactory object can be instantiated with a repo path. The path should
point to a real git repository, and anything intended to be interacted with
using GitLarder should be simple json files in directories (called "models").
Any tree not intended to be pointed at with GitLarder should be shoved into a
.gitrecord_ignore file in the top level.

A GitRecord object can be instantiated from a dict, but the more likely usage is
to retrieve a class from the GitRecordFactory using get_model. The factory
finder methods can then be called on the generated GitRecord subclass without
having to explicitly pass in a model name.
