FROM golang:alpine as build

WORKDIR /build

# Copy and download dependency using go mod
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy the code into the container
COPY . .

# Build the application
RUN go build -o main .

FROM alpine

COPY --from=build ./build/env.list /app/env.list
COPY --from=build /build/main /app/

WORKDIR /app

CMD ["chmod 755 env.list"]
CMD ["./main"]
