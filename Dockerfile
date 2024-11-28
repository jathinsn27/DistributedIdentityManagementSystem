FROM golang:1.22

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY *.go .

RUN go build -o main .

EXPOSE 7946

CMD ["./main"]