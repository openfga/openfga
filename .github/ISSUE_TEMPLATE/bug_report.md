---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

**Summary**

Describe the bug here.

- Steps to reproduce
- What did you expect to happen?
- What actually happened?

**OpenFGA version or commit**

If you know it, enter here.

**Store data**

If applicable, provide information about your authorization model, tuples, and the Check or ListObjects calls you're making. For example:

```yaml
model_file: |
  model
    schema 1.1
  type user
  type organization
    relations
      define member: [user]
tuples:
  - user: user:anne
    relation: member
    object: organization:openfga
  - user: user:bob
    relation: member
    object: organization:openfga
tests: # remove this if not a bug in Check or ListObjects API
  - name: test-1
    check:
      - user: user:anne
        object: organization:openfga
        assertions:
          member: true
    list_objects:
      - user: user:anne
        type: organization
        assertions:
          member:
            - organization:openfga
```

**Other data**

- How are you running OpenFGA? (As a binary, in Docker, in Kubernetes)
- What datastore are you using? (In memory, MySQL, Postgres)
- Are you running OpenFGA with any configuration overrides or with any of the flags mentioned in `./openfga run --help`?
- Do you have any logs or traces that could help us debug the problem?
