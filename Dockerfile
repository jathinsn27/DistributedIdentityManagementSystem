FROM golang:1.20

WORKDIR /app
COPY . .

RUN go mod init app && go build -o app

CMD ["./app"]
