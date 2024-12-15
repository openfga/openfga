
Targets https://github.com/openid/authzen/blob/main/api/authorization-api-1_1_01.md

```
dist/openfga run --datastore-engine memory --authn-method preshared --authn-preshared-keys key1 
```

```
fga store import --file docs/authzen/authzen.fga.yaml --api-token key1
```

