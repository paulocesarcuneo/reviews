FROM golang:latest
RUN apt update -y
RUN apt install -y netcat
WORKDIR /go
COPY . .
RUN mkdir -p /go/lib
ENV GOPATH /go/lib
RUN go build report.go
RUN go build fiftyPlusReviews.go
RUN go build fiftyReviewsFiveStars.go
RUN go build fiveReviewsSameText.go
RUN go build funnyCities.go
RUN go build histogram.go
RUN go build parser.go
RUN go build reviewCounter.go
RUN go build sameText.go
RUN go build stars5.go
RUN go build top10FunniestCities.go
RUN go build feeder.go


