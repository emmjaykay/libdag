FROM ubuntu:17.04
ARG USER_PACKAGE

RUN apt-get update && apt-get -y upgrade
RUN apt-get install -y python3 python3-pip python3-dev build-essential 

RUN  apt-get install -y python3-numpy 

RUN mkdir $USER_PACKAGE
COPY ./$USER_PACKAGE/ /$USER_PACKAGE
COPY ./run_object.py /

RUN pip3 install -e /$USER_PACKAGE
