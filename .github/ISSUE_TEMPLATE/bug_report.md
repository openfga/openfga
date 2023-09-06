---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

**Summary**
Please describe the bug here. What did you expect to happen (e.g. a request) and what did you get that was unexpected?

**OpenFGA version or commit**: 
Enter here.

**Store data**
Please provide information about your authorization model and tuples. For example:

```
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
```

**Other data**
- How are you running OpenFGA? (As a binary, in Docker, in Kubernetes)
- What data store are you using? (In memory, MySQL, Postgres)
