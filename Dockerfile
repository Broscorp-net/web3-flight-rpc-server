# Build stage
FROM maven:3.9-eclipse-temurin-21-alpine AS builder

WORKDIR /build

# Copy pom files
COPY pom.xml .
COPY server/pom.xml server/
COPY client/pom.xml client/

# Download dependencies (cached layer)
RUN mvn dependency:go-offline -B

# Copy source code
COPY server/src server/src
COPY client/src client/src

# Build the project
RUN mvn clean package -DskipTests

# Runtime stage -- glibc-based (RocksDB JNI native lib needs libstdc++/glibc).
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY --from=builder /build/server/target/server.jar /app/server.jar

ENV JAVA_OPTS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -Djava.net.preferIPv4Stack=true"
ENV FLIGHT_PORT=8815
ENV METRICS_PORT=9091

EXPOSE 8815 9091

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/server.jar"]
