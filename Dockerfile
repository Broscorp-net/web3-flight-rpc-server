# Build stage
FROM maven:3.9-eclipse-temurin-21-alpine AS builder

WORKDIR /build

# Copy pom files
COPY pom.xml .
COPY common/pom.xml common/
COPY server/pom.xml server/
COPY client/pom.xml client/
COPY backfill/pom.xml backfill/

# Download dependencies (cached layer)
RUN mvn dependency:go-offline -B

# Copy source code
COPY common/src common/src
COPY server/src server/src
COPY client/src client/src
COPY backfill/src backfill/src

# Build all modules
RUN mvn clean package -DskipTests

# Runtime stage -- glibc-based (RocksDB JNI native lib needs libstdc++/glibc).
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# One image bundles both executables; the deployment picks which one to run
# by passing the jar name as the container args (docker-compose `command:`,
# K8s `args:`). e.g. `server.jar` for the live Flight server, `backfill.jar`
# for the one-shot Argo Workflow that bulk-fills S3 from history.
COPY --from=builder /build/server/target/server.jar /app/server.jar
COPY --from=builder /build/backfill/target/backfill.jar /app/backfill.jar

ENV JAVA_OPTS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -Djava.net.preferIPv4Stack=true"
ENV FLIGHT_PORT=8815
ENV METRICS_PORT=9091

EXPOSE 8815 9091

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/$0"]
