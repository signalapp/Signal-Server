FROM openjdk:8-jre

EXPOSE 8080
EXPOSE 8081

COPY target/lib /usr/share/signal/lib

ARG CONFIG_FILE
COPY config/${CONFIG_FILE} /usr/share/signal/config.yml

ARG JAR_FILE
COPY target/${JAR_FILE} /usr/share/signal/Signal-Service.jar

ENTRYPOINT ["/usr/bin/java", "-server", "-Djava.awt.headless=true", "-Xmx2048m", "-Xss512k", "-jar", "/usr/share/signal/Signal-Service.jar", "server", "/usr/share/signal/config.yml"]

