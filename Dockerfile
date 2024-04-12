ARG GO_VERSION
FROM golang:${GO_VERSION} as golang-cacheprog

# copy out of /usr/local/go; can't use GOROOT
# TODO: there's probably something we can do with GOTOOLCHAIN, but I don't understand it well enough
RUN cp -r /usr/local/go /gocacheprog
RUN cd /gocacheprog/src && GOEXPERIMENT=cacheprog ./make.bash
RUN cp -r /gocacheprog/* /usr/local/go/
RUN go version | grep cacheprog
RUN rm -rf /gocacheprog

FROM golang-cacheprog as gocacheprog-s3
ADD . /workdir
RUN cd /workdir && go install . && rm -rf /workdir

# TODO: not sure this affects the EXEC
ENV GOCACHEPROG="gocacheprog-s3 -v=1"