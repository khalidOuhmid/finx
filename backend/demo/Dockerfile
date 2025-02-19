FROM eclipse-temurin:21-jdk-jammy

RUN apt-get update && apt-get install -y maven

WORKDIR /app
COPY . .

CMD ["mvn", "spring-boot:run", "-Dspring-boot.run.profiles=dev"]
