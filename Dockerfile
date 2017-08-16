FROM ipfs/go-ipfs
MAINTAINER Salim Salaues <salimsalaues@gmail.com>
MAINTAINER Joseph Eftekhari <jdeftekhari@gmail.com>
MAINTAINER Hanu Prateek Kunduru

WORKDIR /usr/src/app

COPY . /usr/src/app

RUN apk update

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

EXPOSE 8080
