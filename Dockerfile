FROM eclipse-temurin:25-jdk AS build
WORKDIR /app
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle/ gradle/
# Resolve dependencies in their own layer so source edits don't re-download them
RUN ./gradlew --no-daemon dependencies
COPY src/ src/
RUN ./gradlew --no-daemon installDist

FROM eclipse-temurin:25-jre
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home app
USER app
WORKDIR /app
COPY --from=build /app/build/install/tessera-engine/ ./
EXPOSE 8080
ENTRYPOINT ["./bin/tessera-engine"]
