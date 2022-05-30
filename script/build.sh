#!/bin/bash
cd /home/longdv/project/signal-master/dev;mvn clean;mvn -DskipTests package
#java -jar service/target/TextSecureServer-0.0.0-dirty-SNAPSHOT.jar server service/config/config.yml
