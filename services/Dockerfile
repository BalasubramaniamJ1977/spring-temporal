# ── Stage 1: Maven build ────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-21 AS build
ARG SERVICE_MODULE=payment-initiation
WORKDIR /build

# Copy parent POM + all module POMs for full dependency resolution
COPY pom.xml .
COPY common/pom.xml                common/pom.xml
COPY temporal-api/pom.xml          temporal-api/pom.xml
COPY temporal-worker/pom.xml       temporal-worker/pom.xml
COPY payment-initiation/pom.xml    payment-initiation/pom.xml
COPY routing/pom.xml               routing/pom.xml
COPY transformer/pom.xml           transformer/pom.xml
COPY payment-processor/pom.xml     payment-processor/pom.xml
COPY accounting-generator/pom.xml  accounting-generator/pom.xml
COPY posting/pom.xml               posting/pom.xml
COPY dispatcher/pom.xml            dispatcher/pom.xml
COPY response-handler/pom.xml      response-handler/pom.xml
COPY sanctions-screening/pom.xml   sanctions-screening/pom.xml
COPY fraud-analytics/pom.xml       fraud-analytics/pom.xml
COPY network-simulator/pom.xml     network-simulator/pom.xml
COPY settlement-processor/pom.xml  settlement-processor/pom.xml

# Resolve all deps and cache as a layer — only invalidated when a POM changes
RUN mvn dependency:go-offline --no-transfer-progress -q

# Copy source for common and temporal-api (always needed as shared libs) + the target service
COPY common/src       common/src
COPY temporal-api/src temporal-api/src
COPY ${SERVICE_MODULE}/src ${SERVICE_MODULE}/src

# Build common + service, skip tests
RUN mvn package -pl ${SERVICE_MODULE} -am --no-transfer-progress -DskipTests -q

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre
ARG SERVICE_MODULE=payment-initiation
WORKDIR /app

# Download OTEL Java agent (pinned version — change to upgrade)
RUN mkdir -p /otel \
 && apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && curl -fsSL \
      "https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.3.0/opentelemetry-javaagent.jar" \
      -o /otel/javaagent.jar \
 && apt-get purge -y --auto-remove curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/${SERVICE_MODULE}/target/${SERVICE_MODULE}-*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", \
  "-javaagent:/otel/javaagent.jar", \
  "-Dotel.instrumentation.logback-appender.experimental-log-attributes=true", \
  "-jar", "app.jar"]
