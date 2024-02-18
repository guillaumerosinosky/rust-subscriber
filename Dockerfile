FROM ubuntu:22.04
RUN apt-get update && apt-get install tini
RUN mkdir /app

#Target TCP
ENV TARGET Kafka
ENV HOST kafka-edge1
ENV PORT 9092
ENV TOPIC test
ENV GROUP_ID test
ENV PARTITIONS 0,1

WORKDIR /app
RUN ldd --version
COPY target/release/rust-subscriber /app/rust-subscriber
ENTRYPOINT ["/app/rust-subscriber"]