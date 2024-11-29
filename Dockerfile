FROM golang:1.23.3-alpine

WORKDIR /app

COPY . .

RUN go build -o node main.go database.go
RUN go build -o middleware middleware.go

EXPOSE 8080 8090

CMD ["sh", "-c", "./node & ./middleware"]