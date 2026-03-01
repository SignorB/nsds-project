FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app

COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn dependency:go-offline -B

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn clean package -DskipTests

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /app/target/akka-services.jar ./app.jar

ENV SERVICE_NAME=""

ENTRYPOINT ["sh", "-c", "java -jar app.jar ${SERVICE_NAME}"]
