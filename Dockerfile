FROM osgeo/gdal:alpine-small-latest

ENV TZ="Europe/Berlin"

RUN apk update && apk upgrade
# Build deps
RUN apk add --update --no-cache --virtual .build-deps g++ libc-dev linux-headers musl musl-dev make geos-dev
# Pillow deps
# zlib-dev numpy
# python-dev numpy
# c++ numpy 


RUN apk add --no-cache jpeg-dev zlib-dev libjpeg 
# Other stuff
RUN apk add --no-cache libffi-dev openssl-dev curl python3-dev py3-pip

RUN pip3 install -U pip
RUN pip3 install wheel

WORKDIR /install
WORKDIR /gtfsfilter
WORKDIR /build

COPY . .

# PYTHONUSERBASE
RUN pip3 install --prefix="/install" .

ENV AM_I_IN_A_DOCKER_CONTAINER Yes
RUN apk del .build-deps
