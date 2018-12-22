FROM flink

ADD /target/scala-2.11/TChallenge-0.1.jar job.jar
ADD sample.csv sample.csv

COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
RUN chmod +x /docker-entrypoint.sh
EXPOSE 6123 8081
