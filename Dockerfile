FROM alpine:3

RUN apk --no-cache add tzdata jq
COPY ./build/any2any /usr/local/bin/any2any

ENTRYPOINT ["/usr/local/bin/any2any"]
