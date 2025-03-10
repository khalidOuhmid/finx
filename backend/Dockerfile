FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && apt-get install -y maven

WORKDIR /app
COPY demo .

CMD ["mvn", "spring-boot:run", "-Dspring-boot.run.profiles=dev"]
