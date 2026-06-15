FROM alpine:3

ARG WITH_PYTHON=false

RUN apk --no-cache add tzdata jq && \
    if [ "$WITH_PYTHON" = "true" ]; then \
        apk --no-cache add python3 py3-pip; \
    fi

COPY ./build/any2any /usr/local/bin/any2any

ENTRYPOINT ["/usr/local/bin/any2any"]
