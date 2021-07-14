FROM golang:1.14.3-alpine AS build
WORKDIR /go/src/github.com/maximiliancsuk/metrics-receiver
COPY . /go/src/github.com/maximiliancsuk/metrics-receiver

# add ca-certificates, later copy them to final image
RUN apk update && apk --no-cache add ca-certificates && update-ca-certificates

 # git needed for fetching (internal) go modules from git repository
RUN apk add git


# "patch" git config to add authentication for private go repository
ARG repoUser
ARG repoPass
ENV repoUser=$repoUser
ENV repoPass=$repoPass
RUN git config --global url."https://${repoUser}:${repoPass}@github.com/maximiliancsuk".insteadOf "https://github.com/maximiliancsuk"

RUN GOPRIVATE="github.com/maximiliancsuk/*" go mod vendor
ENV CGO_ENABLED=0
ARG version
RUN go build -ldflags "-X main.version=$version" -o /out/metrics-receiver ./cmd/metrics-receiver

FROM scratch AS bin
COPY --from=build /out/metrics-receiver /
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["/metrics-receiver"]
