INPUT_DATA_PATH = '../data/kafka/'
LOOKUP_FILE = '../data/taxi_zone_lookup.csv'

BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = {
    'green_rides': INPUT_DATA_PATH + 'green_tripdata_2019-01.csv.gz',
    'fhv_rides': INPUT_DATA_PATH + 'fhv_tripdata_2019-01.csv.gz',
}
KAFKA_AGG_TOPIC = 'rides_all'