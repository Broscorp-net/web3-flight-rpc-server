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

# Runtime stage
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Copy the built JAR
COPY --from=builder /build/server/target/server.jar /app/server.jar

# Set JVM options for Arrow
ENV JAVA_OPTS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -Djava.net.preferIPv4Stack=true"

# Default environment variables
ENV FLIGHT_PORT=8815
ENV MAX_BLOCK_RANGE=500
ENV SLEEP_BEFORE_WEB3_REQUEST_ML_SEC=500

# Expose Flight RPC port
EXPOSE 8815

# Run the server
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/server.jar"]
