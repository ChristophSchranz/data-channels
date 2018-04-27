FROM landoop/fast-data-dev:cp4.0.0
MAINTAINER Christoph Schranz <christoph.schranz@salzburgresearch.at>

#RUN rm -rf /usr/share/landoop/sample-data \
#  && mkdir /usr/share/landoop/sample-data

RUN apk add --update \
    python \
    python-dev \
    py-pip \
    build-base \
  && pip install virtualenv \
  && rm -rf /var/cache/apk/*


RUN mkdir /home/src
WORKDIR /home/src
ADD src/requirements.txt /home/src
RUN pip install -r /home/src/requirements.txt

ADD src/dc-service.py /home/src
ADD src/run.sh /home/src

EXPOSE 2181 3030 3031 8081 8082 8083 9092
EXPOSE 3033

ENTRYPOINT ["/usr/local/bin/dumb-init", "--"]
CMD ["/home/src/run.sh"]
#CMD ["/usr/local/bin/setup-and-run.sh"]
