FROM python:3.7.0-alpine3.8

#todo: Frage  ist alpine-sdk eine Möglichkeit, Entwicklungstools in alpine zu verwenden?
#FROM alpine:3.7
#RUN apk update &&apk add --update alpine-sdk
#RUN mkdir /app
#WORKDIR /app
#COPY . /app
#RUN mkdir bin
#RUN gcc -Wall hello.c -o bin/hello

#was wirklich helfen könnte, wäre ein Multistage-Build
#z.B. ganz kurz beschrieben in
#https://learning.oreilly.com/library/view/learn-docker-/9781788997027/41a4845a-e102-477d-b3c3-0ca1067e68fa.xhtml


#RUN addgroup --gid 55864 harvester
#RUN adduser -g harvester -u 55864   harvester

#RUN addgroup harvester
#RUN adduser -g harvester  harvester

ARG LIBRDKAFKA_NAME="librdkafka"
ARG LIBRDKAFKA_VER="0.11.6"

# Install librdkafka
RUN apk add --no-cache --virtual .fetch-deps \
      ca-certificates \
      libressl \
      curl \
      tar && \
\
    BUILD_DIR="$(mktemp -d)" && \
\
    curl -sLo "$BUILD_DIR/$LIBRDKAFKA_NAME.tar.gz" "https://github.com/edenhill/librdkafka/archive/v$LIBRDKAFKA_VER.tar.gz" && \
    mkdir -p $BUILD_DIR/$LIBRDKAFKA_NAME-$LIBRDKAFKA_VER && \
    tar \
      --extract \
      --file "$BUILD_DIR/$LIBRDKAFKA_NAME.tar.gz" \
      --directory "$BUILD_DIR/$LIBRDKAFKA_NAME-$LIBRDKAFKA_VER" \
      --strip-components 1 && \
\
    apk add --no-cache --virtual .build-deps \
      bash \
      g++ \
      libressl-dev \
      make \
      musl-dev \
      zlib-dev && \
\
    cd "$BUILD_DIR/$LIBRDKAFKA_NAME-$LIBRDKAFKA_VER" && \
    ./configure \
      --prefix=/usr && \
    make -j "$(getconf _NPROCESSORS_ONLN)" && \
    make install && \
\
    runDeps="$( \
      scanelf --needed --nobanner --recursive /usr/local \
        | awk '{ gsub(/,/, "\nso:", $2); print "so:" $2 }' \
        | sort -u \
        | xargs -r apk info --installed \
        | sort -u \
      )" && \
    apk add --no-cache --virtual .librdkafka-rundeps \
      $runDeps && \
\
    cd / && \
    apk del .fetch-deps .build-deps && \
    rm -rf $BUILD_DIR

LABEL maintainer="King Chung Huang <kchuang@ucalgary.ca>" \
org.label-schema.vcs-url="https://github.com/ucalgary/docker-python-librdkafka"

#RUN wget http://nl.alpinelinux.org/alpine/edge/main/x86_64/py-libxml2-2.9.8-r2.apk -O /var/cache/apk/py-lxml.apk
#RUN apk add --allow-untrusted /var/cache/apk/py-lxml.apk



RUN apk update && apk upgrade && pip install -U pip
RUN apk add --update alpine-sdk make gcc python3-dev python-dev libxslt-dev libxml2-dev libc-dev  libffi-dev zlib-dev \
py-pip bash && rm -rf /var/cache/apk/*


ENV CPLUS_INCLUDE_PATH /usr/local/include
ENV LIBRARY_PATH /usr/local/lib
ENV LD_LIBRARY_PATH /usr/local/lib


RUN mkdir -p /app/kafka_event_hub

WORKDIR /app

COPY requirements.txt /app


RUN pip install -r requirements.txt

#RUN chown -R harvester:harvester /app


COPY run.py /app


#was machen wir mit den Tests?
COPY kafka_event_hub /app/kafka_event_hub

USER 52799:52799


VOLUME /basedir
VOLUME /webdav


#ENTRYPOINT ["python3", "run.py", "/inrep"]
ENTRYPOINT ["python3"]

#now you can overwrite the --type parameter for the producer / consumer to be used as argument of the docker run command
# e.g. docker run --rm --name=keh --network host -v $(pwd)/test/configs/oai/idssg1.oai.yaml:/in kafka-event-hub --type=blabla
#todo:
# 1) find a solution for the network if Kafka is not running s docker container on local host
# 2) build docker-compose solution just for this scenario (kafka in container on localhost)

CMD ["--type=OAIProducer" ]

#howto start
#docker run --rm --name=keh --network host -v $(pwd)/test/configs/oai/idssg1.oai.yaml:/inrep kafka-event-hub --sharedconfig $(pwd)/test/configs/oai/oai.share.yaml

#docker run --rm --name=keh --network host -v $(pwd)/test/configs/oai/idsbb.oai.yaml:/inrep -v $(pwd)/test/configs/oai/oai.share.yaml:/inshare kafka-event-hub run.py --type=OAIProducerKafka --sharedconfig /inshare /inrep


#docker run --rm --name=keh -v $(pwd)/configs/idsbb.oai.yaml:/inrep -v $(pwd)/configs/oai.share.yaml:/inshare kafka-event-hub run.py --type=OAIProducerKafka --sharedconfig /inshare /inrep

#docker run --rm  -v $(pwd)/configs/oai/idsbb.oai.yaml:/inrep -v $(pwd)/configs/oai/oai.share.yaml:/inshare kafka-event-hub run.py --type=OAIProducerKafka --sharedconfig /inshare /inrep

#docker run --rm  -v $(pwd)/logs/producer/json:/logpath -v $(pwd)/configs/oai/idsbb.oai.yaml:/inrep -v $(pwd)/configs/oai/oai.share.yaml:/inshare kafka-event-hub run.py --type=OAIProducerKafka --sharedconfig /inshare /inrep

#Start mit logging
#docker container run --rm      -v $(pwd)/logpath:/logpath -v $(pwd)/configs/oai:/inrep -v $(pwd)/configs/logs:/logconfigpath -v $(pwd)/configs/oai:/inshare kafka-event-hub app/run.py --type=OAIProducerKafka --sharedconfig /inshare/cc.share.yaml /inrep/idsbb.oai.yaml
