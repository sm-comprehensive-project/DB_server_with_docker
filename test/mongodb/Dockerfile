FROM mongo
WORKDIR /usr/src
RUN mkdir -p /usr/src/scripts

COPY replicaSet.js /usr/src/scripts/
COPY setup.sh /usr/src/scripts/

RUN chmod +x /usr/src/scripts/setup.sh

CMD ["/usr/src/scripts/setup.sh"]