FROM golang:1.23.3-alpine

WORKDIR /app

COPY . .

RUN apk add --no-cache curl
RUN go build -o node main.go database.go tree.go multicast.go
RUN go build -o middleware middleware.go
RUN go build -o membership membership.go

EXPOSE 8080 8090 7946
