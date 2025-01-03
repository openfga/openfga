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
openfga migrate --datastore-engine sqlite --datastore-uri authzen.sqlite
openfga run --datastore-engine sqlite --datastore-uri authzen.sqlite
fga store import --file docs/authzen/authzen.fga.yaml 
```

We can then copy the SQLite database to the EC2 instance:

```
scp -i "openfga-authzen.pem" ./authzen.sqlite ec2-user@ec2-3-21-166-38.us-east-2.compute.amazonaws.com:/home/ec2-user/authzen.sqlite
```

Note that the authzen.sqlite file is included in the branch so it does not need to be recreated each time and we can keep the Store ID.

## Starting OpenFGA in the EC2 instance

The simplest way to run OpenFGA in the EC2 instance is by executing the command below:

```
ssh -i "openfga-authzen.pem" ec2-user@ec2-3-21-166-38.us-east-2.compute.amazonaws.com

sudo pkill openfga
sudo ./openfga run --datastore-engine sqlite --datastore-uri authzen.sqlite  --http-addr 0.0.0.0:80  --authn-method preshared --authn-preshared-keys <key>
```

To test if it's working correctly you can try the `fga store list` command from a different host:

```
fga --api-url https://authzen-interop.openfga.dev --api-token <key> store list 
```

## Running the Authzen Interop test suite

- Clone https://github.com/openid/authzen
- Go to the `interop/authzen-todo-backend` folder and follow the instructions in the readme to build the project.

- Set the following environment variables:

```
export AUTHZEN_PDP_URL=https://authzen-interop.openfga.dev/stores/01JG9JGS4W0950VN17G8NNAH3C
export AUTHZEN_PDP_API_KEY="Bearer <key>"
```

- Before running the tests, you need to write some tuples that the test requires. They could already be present in the FGA store and in such a case you'll get errors:

```
fga --api-url https://authzen-interop.openfga.dev tuple write --file authzen.interop-tuples.yaml --store-id 01JG9JGS4W0950VN17G8NNAH3C  --api-token <key>
```

- Run the conformance test suite with the following command:

```
yarn test https://authzen-interop.openfga.dev/stores/01JG9JGS4W0950VN17G8NNAH3C 1.1-preview
```

## Running the AuthZen Interop App

- Follow the steps in /authzen/interop/authzen-todo-backend/README.MD
- Add the following environment variable:

```
export AUTHZEN_PDP_API_KEY='{"OpenFGA": "Bearer <key>"}'
```

- In `/authzen/interop/authzen-todo-backend` run `yarn start`

- Follow the steps in /authzen/interop/authzen-todo-application/README.MD
- In `/authzen/interop/authzen-todo-application/` run `yarn start`

## Implementing the AuthZen Interop example with OpenFGA
TBD

## TODO

- Deployment
    - Deploy with Docker

- AuthZen implementation
    - Check what happens with the error codes for check
    - Add more tests for mapping
    - Add experimental flag
    - How to test the error code when one type is incorrect?
    - Add support for mapping contextual tuples
    - Add examples for ABAC

- Authzen Interop App
    - Error handling for failing writes/fga writes?

