# Steps to run the Kafka application

After cloning the repository

Start the virtual enviroment
```
pipenv shell
```

Install the requirements
```
pipenv sync
```

Create Docker network.
Note - this is not needed for the Kafka Python example, but since
it is already included in the docker-compose.yml run it
```
docker network  create kafka-spark-network
```

Run Kafka services on Docker
```
cd docker/kafka/
docker compose up -d
```

Run the producer on one terminal
```
python producer.py
```

Run the consumer on another terminal
```
python consumer.py
```

Ctrl-C to stop both the producer and consumer

# Other CLIs

To create a topic on the CLI
```
./bin/kafka-topics.sh --create --topic demo_1 --bootstrap-server localhost:9092 --partitions 2
```

List Docker network
```
docker network ls
```

List Docker volume
```
docker volume ls
```

List Docker processes
```
docker ps
```

Delete Docker containers
```
docker rm -f $(docker ps -a -q)
```

Delete Docker volumes
```
docker volume rm $(docker volume ls -q)
```

To run Kafka on Spark, follow instructions [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_6_stream_processing/python/docker)

## Output from the application

For `green_rides`, in the first few partitions, PuLocationID 72 and 41 appear to occur more frequently. Whereas, for the `fhv_rides`
they are 48 and 138