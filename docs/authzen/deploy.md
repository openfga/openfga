# OpenFGA AuthZen Implementation and Interop App

## Creating a Docker image with the AuthZEN Branch

The [GoReleaser](/.goreleaser.yaml) definition in this branch publishes a docker image to a `openfga-authzen` project in `ghcr.io/aaguiarz/openfga`. We tested this approach locally, not using Github actions yet.

## Deploying the AuthZEN branch to an AWS instance

We are currently using an AWS EC2 instance with Docker to run it and we configure it manually. 

- You can login to the EC2 instance using the `openfga-authzen.pem` key that's in the OpenFGA vault

```
ssh -i "~/aws/openfga-authzen.pem" ec2-user@ec2-3-21-166-38.us-east-2.compute.amazonaws.com
```

- If for some reason you need to restart the server, you'd need to stop it, remove it, and run it again:

```
docker stop openfga || true
docker rm openfga || true

docker run --name openfga \
  -d -p 80:8080 \
  --restart=always \
  --user "$(id -u):$(id -g)" \
  --env OPENFGA_DATASTORE_ENGINE=sqlite \
  --env OPENFGA_DATASTORE_URI=/data/authzen.sqlite \
  --env OPENFGA_PLAYGROUND_ENABLED=false \
  --env OPENFGA_AUTHN_METHOD=preshared \
  --env OPENFGA_AUTHN_PRESHARED_KEYS=<shared-key> \
  --volume authzen_data:/data \
   ghcr.io/aaguiarz/openfga run 

```
- The shared key used to run this OpenFGA server is the OpenFGA vault under the "OpenFGA Authzen shared key" name.

### SQLite database

We are using a SQLite database. When configuring Docker, we created a volume that's mounted into the docker instance. This does not need to be done again unless we want to change the directory: 

```
sudo mkdir -p /data
sudo chown -R ec2-user:ec2-user /data
docker volume create --driver local --opt type=none --opt o=bind --opt device="$HOME/data" authzen_data
```

The SQLite database is in the github repository, and we copy it manually to the VM to avoid having Store IDs being recreated:

```
scp -i "~/aws/openfga-authzen.pem" bin/openfga ec2-user@ec2-3-21-166-38.us-east-2.compute.amazonaws.com:/home/ec2-user/openfga
```

## Initializing an SQLite database for the AuthZen interop authorization model

We want to always use the same SQLite instance with the same Store ID to avoid having a new IDs generated each time we build/deploy the system:

To recreate the `authzen.sqlite` file, delete it, and create it again using the `openfga migrate` command, and then start the openfga instance with `openfga run`. 

```
openfga migrate --datastore-engine sqlite --datastore-uri authzen.sqlite
openfga run --datastore-engine sqlite --datastore-uri authzen.sqlite
```

To load it with the required models & data, run the following commands:

```
fga model write --store-id 01JG9JGS4W0950VN17G8NNAH3C --file authzen-todo.fga
fga tuple write --store-id 01JG9JGS4W0950VN17G8NNAH3C --file authzen-todo-tuples.yaml

fga model write --store-id 01JNW1803442023HVDKV03FB3A --file authzen-gateway.fga
fga tuple write --store-id 01JNW1803442023HVDKV03FB3A --file authzen-gateway-tuples.yaml
```

## Making model changes

Given that the OpenFGA server is deployed and the database has 'production' data (e.g. the Todo items added in https://todo.authzen-interop.net/), if we want to change the model, we should do it using the FGA API pointing to the production server, e.g

```
fga model write --api-url https://authzen-interop.openfga.dev --api-token <key> -store-id 01JG9JGS4W0950VN17G8NNAH3C --file authzen-todo.fga 
```