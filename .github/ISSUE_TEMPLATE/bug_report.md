---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

**Summary**

Describe the bug here. What did you expect to happen and what did you get that was unexpected?

**OpenFGA version or commit**

If you know it, enter here.

**Store data**

If applicable, provide information about your authorization model, tuples, and the Check or ListObjects calls you're making. For example:

```yaml
model-file: |
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
    list-objects:
      - user: user:anne
        type: organization
        assertions:
          member:
            - organization:openfga
```

**Other data**

- How are you running OpenFGA? (As a binary, in Docker, in Kubernetes)
- What datastore are you using? (In memory, MySQL, Postgres)
