# Signal-Server Full Installation Guide

- [Roadmap! New readers probably start here](https://github.com/jtof-dev/Signal-Docker)
- Written for Signal-Server v9.81.0
- Documented with a Debian / Ubuntu EC2 instance in mind

## Dependencies

Required:
- `git`
- `java`
- `docker`
- `docker-compose`

Optional:
- `maven` (v3.8.6 or newer)
  - If on Debian, you may need to manually install a newer version

## Pre-Compilation

Signal-Server needs to be ran in an AWS EC2 instance for it to function. While you can configure locally and `scp` your files into the instance, this is a pain

- Create an account with AWS, then follow [these sections](docs/signal-server-configuration.md#aws-ec2) about making an EC2 instance and associating an elastic ip

## Compilation

### The easy way

```
git clone https://github.com/jtof-dev/Signal-Server.git
cd Signal-Server/scripts
# now choose surgery-compiler.sh or main-compiler.sh, though surgery-compiler is the one that will work currently
bash surgery-compiler.sh
```

Using the scripted compilers are recommended to ensure that the server is in the correct configuration (with or without `zkgroup`)

### Manually

- If you want to pull from signalapp's repo, you can run `git clone https://github.com/signalapp/Signal-Server` (mileage may vary on configuring a newer version), and if you want to specify v9.81.0, also run `git checkout 9c93d37`

Then compile with:

```
./mvnw clean install -DskipTests -Pexclude-spam-filter
```

Which uses the maven build script that comes bundled with Signal-Server. You can also install your own instance of maven and build using that:

```
mvn clean install -DskipTests -Pexclude-spam-filter
```

### Removing zkgroup dependencies manually

- This removes `zkgroup` (originally called from [libsignal](https://github.com/signalapp/libsignal)) which will let the server start

- In [`WhisperServerService.java`](service/src/main/java/org/whispersystems/textsecuregcm/WhisperServerService.java), comment out lines 639, 739-40, 773-777

- Alternatively, copy the `WhisperServerService.java` file from either the folder `intact` or `post-surgery` to `service/src/main/java/org/whispersystems/textsecuregcm` to either include or remove `zkgroup`

## Configuration

### Fill out `sample.yml` and `sample-secrets-bundle.yml`, located in `service/config/`

- Any configuration notes related to these two `.yml` files are located [here](docs/signal-server-configuration.md)

### Docker configuration

Note: this is only for docker containers bundled in `Signal-Server`, there are some other docker dependencies - check out [Other Self-Hosted Services](#other-self-hosted-services)

In order to for the redis-cluster to successfully start, it has to start without any modifications to the source [docker-compose.yml](https://github.com/bitnami/containers/blob/main/bitnami/redis-cluster/docker-compose.yml)

**The easy way**

```
cd scripts
bash docker-compose-first-run.sh
```

Note: this will delete any existing redis-cluster volumes generated in the `Signal-Server` folder

**Manually**

Check out [this README in Signal-Docker](https://github.com/jtof-dev/Signal-Docker/tree/main/redis-cluster)

## Starting the server

### The easy way

[Make sure you configure the repo for `quickstart.sh` first!](docs/signal-server-configuration.md#configuring-for-quickstartsh)

```
cd scripts
bash quickstart.sh
```

### Manually

Call your environmental variables (if not in `.bashrc`)

Make sure your AWS environmental variables are sorted out

Run redis:

```
sudo docker-compose up -d
```

Start the server with:

``` 
java -jar -Dsecrets.bundle.filename=service/config/sample-secrets-bundle.yml service/target/TextSecureServer-9.81.0.jar server service/config/sample.yml
```

## Running the server

To ping the server, try this command (or any of the ones listed when starting the server)

```
curl http://127.0.0.1:8080/v1/accounts/whoami
```

When running Signal-Server in a Docker container, replace port `8080` with port `7006`

## Other Self-Hosted Services

Once you get the server running without errors in EC2, there are a couple other services you need to set up

- [NGINX and Certbot to handle SSL certificates](https://github.com/jtof-dev/Signal-Docker/tree/main/nginx-certbot)

  - NGINX needs to be done first to generate certificates which will be used while configuring `registration-service`

- [Signalapp's registration-service](https://github.com/jtof-dev/Signal-Docker/tree/main/registration-service) to handle registering phone numbers

### Recloning

The [recloner.sh](scripts/recloner.sh) bash script moves the folder [personal-config](personal-config) up one level outside of `Signal-Server`, then reclones from this repository

- This is useful when a compilation fails, but hopefully everything is streamlined so you don't run into this (usually you can just `git pull` and call it a day)

```
cd scripts
bash recloner.sh
```

### Connecting the server to an Android app

- Check out [jtof-dev/Signal-Android](https://github.com/jtof-dev/Signal-Android)!

## To-Do

### General

### Running the server

- Make EC2 role and policy narrower

- Make the account crawler wait longer between runs

- Fix the redis-cluster error (doesn't show up every time but it complains about null values)

### Extra Credit

- Write scripts for AWS / Google Cloud cli