---
title: Nightly dependency test failed on {{ date | date('YYYY-MM-DD') }}
labels: nightly-failure, dependencies
assignees: strawgate
---

## Nightly Dependency Test Failure

The nightly dependency test workflow has failed. This likely indicates that a dependency has released a breaking change.

**Workflow run:** {{ env.WORKFLOW_URL }}

### Next Steps
1. Review the workflow run logs to identify which dependency/test failed
2. Check recent releases of dependencies for breaking changes
3. Update code or pin dependency versions as needed
