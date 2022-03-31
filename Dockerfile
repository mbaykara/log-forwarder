FROM golang:alpine as build 
WORKDIR /
COPY go.mod ./
RUN go mod download
COPY *.go ./
RUN go get .
RUN go build -o /usr/local/bin/app

FROM alpine:3.15
ENV AZURE_STORAGE_ACCOUNT_NAME=${AZURE_STORAGE_ACCOUNT_NAME} \
    AZURE_STORAGE_ACCOUNT_KEY=${AZURE_STORAGE_ACCOUNT_KEY}
COPY --from=build /usr/local/bin/app /usr/local/bin/app
CMD ["app"]
EXPOSE 8090