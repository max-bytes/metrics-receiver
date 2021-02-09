FROM golang:1.14.3-alpine AS build
WORKDIR /go/src/mhx.at/gitlab/landscape/metrics-receiver-ng
COPY . /go/src/mhx.at/gitlab/landscape/metrics-receiver-ng
RUN go mod vendor
# RUN echo $GOPATH
ARG version
RUN go build -ldflags "-X main.version=$version" -o /metrics-receiver-ng ./

FROM scratch AS bin
COPY --from=build /metrics-receiver-ng /
ENTRYPOINT ["./metrics-receiver-ng"]