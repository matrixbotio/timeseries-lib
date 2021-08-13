FROM golang:1.16-buster as builder

COPY . /go/src

WORKDIR /go/src/src

RUN mkdir /app

RUN go get -v -t -d ./...

RUN go build -o /app/main


FROM gcr.io/distroless/base

COPY --from=builder /app /app

ENTRYPOINT [ "/app/main" ]
