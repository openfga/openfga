# OpenFGA AuthZen Implementation and Interop App

This branch includes an experimental implementation of the [AuthZen Authorization API 1.1 – draft 01](https://github.com/openid/authzen/blob/main/api/authorization-api-1_1_01.md).

It also includes a SQLite file with an OpenFGA store + tuples for the AuthZen Interop application.

## Deploying the branch to an EC2 instance

There's currently no Docker image for this branch. We are keeping deployment simple by just copying the binary to an EC2 instance.

- Build a linux/arm64 binary

```shell
GOOS=linux GOARCH=arm64 go build -o bin/openfga "./cmd/openfga" 
```

- Copy it to the EC2 instance with scp:
```
scp -i "openfga-authzen.pem" bin/openfga ec2-user@ec2-3-21-166-38.us-east-2.compute.amazonaws.com:/home/ec2-user/openfga
```

## Initializing an SQLite database for the AuthZen interop authorization model

We want to always use the same SQLite instance with the same Store ID to avoid having a new IDs generated each time we build/deploy the system:

```
openfga migrate --datastore-engine sqlite --datastore-uri openfga.sqlite
openfga run --datastore-engine sqlite --datastore-uri openfga.sqlite
fga store import --file docs/authzen/authzen.fga.yaml 
```

We can then copy the SQLite database to the EC2 instance:

```
scp -i "openfga-authzen.pem" ./openfga.sqlite ec2-user@ec2-3-21-166-38.us-east-2.compute.amazonaws.com:/home/ec2-user/openfga.sqlite
```

Note that the openfga.sqlite file is included in the branch so it does not need to be recreated each time.

## Starting OpenFGA in the EC2 instance

The simplest way to run OpenFGA in the EC2 instance is by executing the command below:

```
ssh -i "openfga-authzen.pem" ec2-user@ec2-3-21-166-38.us-east-2.compute.amazonaws.com

sudo pkill openfga
sudo ./openfga run --datastore-engine sqlite --datastore-uri openfga.sqlite  --http-addr 0.0.0.0:80  --authn-method preshared --authn-preshared-keys <key>
```

To test if it's working correctly you can try the `fga store list` command from a different host:

```
fga --api-url http://ec2-user@ec2-3-21-166-38.us-east-2.compute.amazonaws.com --api-token <key> store list 
```

## Running the Authzen Interop test suite

- Clone https://github.com/openid/authzen
- Go to the `interop/authzen-todo-backend` folder and follow the instructions in the readme

```
AUTHZEN_PDP_URL=http://ec2-3-21-166-38.us-east-2.compute.amazonaws.com/stores/01JG2JKG07BQE6PAK8ZZWYQ4YK
AUTHZEN_PDP_API_KEY="Bearer FbtmrFbuBOT/4R68B2s0pSYsPg5BI6dr+b7KX1OGwhs="
```
```
yarn test http://ec2-3-21-166-38.us-east-2.compute.amazonaws.com/stores/01JG2JKG07BQE6PAK8ZZWYQ4YK 1.1-preview
```

## Implementing the AuthZen Interop example with OpenFGA

TBD

## TODO

- Setup a domain / cname
- Properly wire up the API reference
- Check what happens with the error codes for check
- Add more tests for mapping
- Add experimental flag
- How to test the error code when one type is incorrect?
- Can we deploy this more efficiently?
- How to setup SSL? Nginx? Using the ELB?
- Test the full interop app
- Add support for mapping contextual tuples
- Add examples for ABAC
