# An Application With Apache Flink

To build it first clone the repository
 `git clone git@github.com:alpersimsek1/flinkExs.git`

Then compile with sbt 

    sbt assembly

## Running The Jar
There are two different options in here. 
#### First option is running jar in local
1. Start Flink Cluster

	2. install flink with `brew install apache-flink`
	3. `cd /usr/local/Cellar/apache-flink/1.7.0/libexec/bin`
	4. `./start-cluster.sh`
	
2. Run the jar using flink command
	      This command takes two arguments. First argument is path of the sample.csv. Second argument is for the path of the outputs csv's.  

          flink run target/scala-2.11/TChallenge-0.1.jar /path/to/sample.csv /path/to/outputs`
          flink run target/scala-2.11/TChallenge-0.1.jar sample.csv outputs
            
#### Second option is using docker 
##### there are two different approach to use docker
First: 
    using docker-compose 
    cd to the project directory
    
    1. docker pull flink
    2. docker-compose up
    2. run build.sh which contains: 
         JOBMANAGER_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})
         docker cp path/to/jar "$JOBMANAGER_CONTAINER":/job.jar
         docker cp sample.csv  "$JOBMANAGER_CONTAINER":/sample.csv
         docker exec -t -i "$JOBMANAGER_CONTAINER" flink run /job.jar sample.csv
  
Second: 
    using Dockerfile
1. Build container using dockerfile`docker build -t flink:app .`
2. Run with `docker run flink:app jobmanager`
3. Execute with 
 ````
 JOBMANAGER_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})
 docker exec -t -i "$JOBMANAGER_CONTAINER" flink run job.jar sample.csv
 ````
4. Copy outputs from docker using 
````
 docker cp "$JOBMANAGER_CONTAINER":/outputs/ ~/Desktop/
```` 

Some challenges: 
    First I try to deploy to docker using java -jar job.jar 
    but this gives some problems. So I had to download flink to the containers. 
    
    
You can see my strategy by looking to the commits. 
Thank you. 