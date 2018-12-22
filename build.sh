#!/bin/sh

JOBMANAGER_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})
docker cp target/scala-2.11/TChallenge-0.1.jar "$JOBMANAGER_CONTAINER":/job.jar
docker exec -t -i "$JOBMANAGER_CONTAINER" flink run /job.jar "$JOBMANAGER_CONTAINER"/sample.csv
