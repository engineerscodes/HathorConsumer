FROM openjdk:17.0.1-jdk-slim

WORKDIR /app

ENV DB_PASSWORD=naveen
ENV PRIVATE_KEY=766f6f72664c38514c6541633643706c357467726e4c4c765433487862546b4e

RUN apt-get update && apt-get install -y maven
COPY pom.xml ./
COPY src ./src

RUN mvn dependency:go-offline
RUN mvn clean package


CMD ["java", "-jar", "target/HathorConsumer-1.0.jar"]