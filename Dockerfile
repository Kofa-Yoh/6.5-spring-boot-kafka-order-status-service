FROM openjdk:17-oracle

ARG JAR_FILE=build/libs/*.jar
COPY ${JAR_FILE} order-status-server.jar

ENV SERVER_PORT=8081
ENV BOOTSTRAP_SERVERS=localhost:9092

CMD ["java", "-jar", "order-status-server.jar"]