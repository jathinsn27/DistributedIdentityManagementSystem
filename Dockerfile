FROM golang:1.23.3-alpine
WORKDIR /app
COPY . .
RUN go build -o main .
RUN go build -o middleware .
EXPOSE 8080 8090
CMD ["./main & ./middleware"]