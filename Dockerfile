FROM scality/s3server:latest
MAINTAINER Salim Salaues <salimsalaues@gmail.com>
MAINTAINER Joseph Eftekhari <jdeftekhari@gmail.com>
MAINTAINER Hanu Prateek Kunduru <hanu.prateek@gmail.com>

WORKDIR /usr/src/app

COPY . /usr/src/app

VOLUME ["/usr/src/app/localData","/usr/src/app/localMetadata"]

CMD [ "npm", "start" ]

EXPOSE 8000
