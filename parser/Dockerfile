# syntax=docker/dockerfile:1.7
FROM golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go main_test.go ./
RUN go test ./...
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/binlog-parser-linux-amd64 .

FROM scratch
COPY --from=build /out/binlog-parser-linux-amd64 /binlog-parser-linux-amd64
ENTRYPOINT ["/binlog-parser-linux-amd64"]
