FROM alpine:3

RUN apk --no-cache add tzdata jq python3 py3-pip

COPY ./build/any2any /usr/local/bin/any2any

ENTRYPOINT ["/usr/local/bin/any2any"]
