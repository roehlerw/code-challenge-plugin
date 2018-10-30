FROM golang:1.11-stretch

WORKDIR /code-challenge-plugin

ADD go.mod .
ADD go.sum .

RUN go mod download

ADD ./plugin/ ./plugin
ADD ./data/ ./data

ADD host.go .

ENTRYPOINT ["go", "run", "host.go"]

# Build your implementation here

ADD ./goplugin ./goplugin

WORKDIR /code-challenge-plugin/goplugin
RUN go install

WORKDIR /code-challenge-plugin

# Put your implementation here
CMD ["goplugin"]
