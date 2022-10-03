FROM golang:1.18.5-alpine

# Install dependencies
RUN apk add --no-cache postgresql-client

# Copy source code
WORKDIR /app
COPY . .

# Build
RUN go build -o main main.go

CMD ["/app/main"]