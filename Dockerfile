FROM golang:1.14.3-alpine AS build

WORKDIR /app

ADD . /app

RUN go build -o main .

CMD ["/app/main"]