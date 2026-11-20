FROM docker.io/library/maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY common-events common-events
COPY order-service order-service
COPY inventory-service inventory-service
COPY payment-service payment-service
RUN mvn -B -ntp package -DskipTests
ARG SERVICE
RUN cp ${SERVICE}/target/${SERVICE}-0.1.0-SNAPSHOT.jar /app.jar

FROM docker.io/library/eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S app -G app && apk add --no-cache curl
WORKDIR /app
COPY --from=build /app.jar app.jar
USER app
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=6 CMD curl -fsS http://localhost:8080/actuator/health/readiness || exit 1
ENTRYPOINT ["java", "-jar", "app.jar"]
