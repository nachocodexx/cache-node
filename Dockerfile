FROM alpine:3.7
RUN apk add openjdk8
COPY ./target/scala-2.13/storagenode.jar /app/src/app.jar
WORKDIR /app/src
#ENTRYPOINT ["java", "-jar","app.jar"]
ENTRYPOINT ["java", "-cp","app.jar","mx.cinvestav.MainV5"]
