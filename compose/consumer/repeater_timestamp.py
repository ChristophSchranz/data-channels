import os
import sys
import time
import json
import logging
from multiprocessing import Process
import requests
from datetime import datetime
from flask import Flask, jsonify
from redis import Redis
from logstash import TCPLogstashHandler
# confluent_kafka is based on librdkafka, details in requirements.txt
from confluent_kafka import Consumer, Producer, KafkaError


# Why using a kafka to logstash adapter, while there is a plugin?
# Because there are measurements, as well as observations valid as SensorThings result.
# Kafka Adapters seems to use only one topic
# ID mapping is pretty much straightforward with a python script


__date__ = "15 March 2018"
__version__ = "1.10"
__email__ = "christoph.schranz@salzburgresearch.at"
__status__ = "Development"
__desc__ = """This program forwards consumed messages from the kafka bus to to a new kafka topic in a specific time range."""


# kafka parameters
# topics and servers should be of the form: "topic1,topic2,..."
KAFKA_TOPICS = "SensorData"
NEW_KAFKA_TOPIC = "repeatedSensorData_2"
BOOTSTRAP_SERVERS_default = 'il061,il062,il063'

STARTTIME = "2018-03-23T08:30"
ENDTIME = "2018-03-28T23:59"


# "iot86" for local testing. In case of any data losses, temporarily use another group-id until all data is load.
KAFKA_GROUP_ID = "iot86_testwith ipn8"  # use il060 if used in docker swarm
# If executed locally with python, the KAFKA_GROUP_ID won't be changed
#KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', KAFKA_GROUP_ID)  # overwrite iot86 by envfile ID=il060
# if deployed in docker, the adapter will automatically use the entry in the .env file.

# logstash parameters
HOST_default = KAFKA_GROUP_ID  # 'il060'   # use the local endpoint: equals hostname
PORT_default = 5000
STATUS_FILE = "status.log"

# Sensorthings parameters
ST_SERVER = "http://il060:8082/v1.0/"
REFRESH_MAPPING_EVERY = 5 * 60  # in seconds

# webservice setup
app = Flask(__name__)
redis = Redis(host='redis', port=6379)


@app.route('/')
def print_adapter_status():
    """
    This function is called by a sebserver request and prints the current meta information.
    :return:
    """
    try:
        with open(STATUS_FILE) as f:
            adapter_status = json.loads(f.read())
    except FileNotFoundError:
        adapter_status = {"application": "kafka-repeater with timerange",
                          "status": "initialisation"}
    return jsonify(adapter_status)


class KafkaStAdapter:
    def __init__(self, enable_kafka_adapter, enable_sensorthings):
        self.enable_kafka_adapter = enable_kafka_adapter
        self.enable_sensorthings = enable_sensorthings
        if self.enable_sensorthings:
            self.id_mapping = self.full_st_id_map()

    def full_st_id_map(self):
        datastreams = requests.get(ST_SERVER + "Datastreams").json()
        id_mapping = dict()
        id_mapping["@iot.nextLink"] = datastreams.get("@iot.nextLink", None)
        id_mapping["value"] = dict()
        for stream in datastreams["value"]:
            stream_id = str(stream["@iot.id"])
            id_mapping["value"][stream_id] = {"name": stream["name"],
                                              "description": stream["description"]}
        return id_mapping

    def one_st_id_map(self, idn):
        stream = requests.get(ST_SERVER + "Datastreams(" + str(idn) + ")").json()
        stream_id = str(stream["@iot.id"])
        self.id_mapping["value"][stream_id] = {"name": stream["name"],
                                               "description": stream["description"]}

    def empty_id_mapping(self):
        datastreams = requests.get(ST_SERVER + "Datastreams").json()
        id_mapping = dict()
        id_mapping["@iot.nextLink"] = datastreams.get("@iot.nextLink", None)
        id_mapping["value"] = dict()
        return id_mapping

    def stream_kafka(self):
        """highest
        This function configures a kafka consumer and a logstash logger instance.
        :return
        """
        # Init logstash logging
        logging.basicConfig(level='WARNING')
        loggername_logs = 'kafka-repeater.logging'
        logger_logs = logging.getLogger(loggername_logs)
        logger_logs.setLevel(logging.INFO)
        #  use default and init Logstash Handler
        logstash_handler = TCPLogstashHandler(host=HOST_default,
                                              port=PORT_default,
                                              version=1)
        logger_logs.addHandler(logstash_handler)
        logger_logs.info('Added Logstash Logger for Logs with loggername: {}'.format(loggername_logs))

        # Init kafka consumer
        logging.basicConfig(level='WARNING')
        kafka_topics_str = os.getenv('KAFKA_TOPICS', KAFKA_TOPICS)
        kafka_topics = [topic.strip() for topic in kafka_topics_str.split(",") if len(topic) > 0]
        print(kafka_topics)
        logger_logs.info('Subscribed Kafka Topics: {}'.format(kafka_topics))
        #logger_logs.info('Producing Kafka Topic: {}'.format(NEW_KAFKA_TOPIC))

        # Init logstash logging for data
        logging.basicConfig(level='WARNING')
        loggername = KAFKA_GROUP_ID + '.' + kafka_topics_str
        logger = logging.getLogger(loggername)
        logger.setLevel(logging.INFO)

        # get bootstrap_servers from environment variable or use defaults and configure Consumer
        bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS', BOOTSTRAP_SERVERS_default)
        conf = {'bootstrap.servers': bootstrap_servers, 'group.id': KAFKA_GROUP_ID,
                'session.timeout.ms': 6000,
                'default.topic.config': {'auto.offset.reset': 'smallest'}}
        logger_logs.info('Subscribed Kafka Config: {}'.format(conf))

        # def on_assign(c, ps):
        #     for p in ps:
        #         p.offset = 0
        #     c.assign(ps)

        # Create Consumer if allowed:
        if self.enable_kafka_adapter:
            consumer = Consumer(**conf)
            consumer.subscribe(kafka_topics)  # , on_assign=on_assign)
        else:
            consumer = None

        conf = {'bootstrap.servers': BOOTSTRAP_SERVERS_default}
        producer = Producer(**conf)

        #  use default and init Logstash Handler
        logstash_handler = TCPLogstashHandler(host=HOST_default,
                                              port=PORT_default,
                                              version=1)
        logger.addHandler(logstash_handler)
        logger_logs.info('Added Logstash Logger for Data with loggername: {}'.format(loggername))

        # Check if Sensorthings server is reachable
        if self.enable_sensorthings:
            st_reachable = True
        else:
            st_reachable = False

        # Set status and write to shared file
        adapter_status = {
            "application": "db-adapter",
            "doc": __desc__,
            "status": "waiting for Logstash",
            "kafka input": {
                "configuration": conf,
                "subscribed topics": kafka_topics_str,
                "producing topic": NEW_KAFKA_TOPIC,
                "enabled kafka adapter": self.enable_kafka_adapter,
                "iot_ids": []
            },
            "logstash output": {
                "host": HOST_default,
                "port": PORT_default,
                "logger name for data": loggername,
                "logger name for logs": loggername_logs
            },
            "sensorthings mapping": {
                "enabled sensorthings": self.enable_sensorthings,
                "host": ST_SERVER,
                "reachable": st_reachable
            },
            "version": {
                "number": __version__,
                "build_date": __date__,
                "repository": "https://github.com/i-maintenance/DB-Adapter"
            }
        }
        with open(STATUS_FILE, "w") as f:
            f.write(json.dumps(adapter_status))
            logger_logs.info('Status of Adapter: {}'.format(adapter_status))

        # ready to stream flag
        adapter_status["status"] = "running"
        with open(STATUS_FILE, "w") as f:
            f.write(json.dumps(adapter_status))
        print("Adapter Status:", str(adapter_status))
        logger_logs.info('Ready to start')

        iot_ids = list()

        i = 0
        # Kafka 2 Logstash streaming
        if self.enable_kafka_adapter:
            running = True
            ts_refreshed_mapping = time.time()
            try:
                while running:
                    msg = consumer.poll(0.1)
                    if msg is None:
                        continue
                    if not msg.error():
                        data = json.loads(msg.value().decode('utf-8'))

                        if self.enable_sensorthings:
                            data_id = str(data['Datastream']['@iot.id'])
                            if data_id not in list(self.id_mapping['value'].keys()):
                                self.one_st_id_map(data_id)
                            data['Datastream']['name'] = self.id_mapping['value'][data_id]['name']
                            data['Datastream']['URI'] = ST_SERVER + "Datastreams(" + data_id + ")"

                        if i % 25 == 0:
                            print(data["phenomenonTime"], data["Datastream"])
                        i += 1

                        if data["phenomenonTime"] < STARTTIME:
                            # print("skipped", data["phenomenonTime"])
                            continue
                        elif data["phenomenonTime"] > ENDTIME:
                            # print("stopped", data["phenomenonTime"])
                            continue

                        # if iot_id not in range(11, 30):
                        #     continue
                        iot_id = data['Datastream']['@iot.id']
                        if iot_id not in [row[0] for row in iot_ids]:
                            iot_ids.append([iot_id, data["phenomenonTime"]])
                            iot_ids = sorted(iot_ids, key=lambda x: x[0])
                            print("Found new iot_id", iot_id, data["phenomenonTime"])
                            adapter_status["kafka input"]["iot_ids"] = iot_ids
                            with open(STATUS_FILE, "w") as f:
                                f.write(json.dumps(adapter_status))

                        # logger.info('', extra=data)
                        try:
                            producer.produce(NEW_KAFKA_TOPIC, json.dumps(data).encode('utf-8'),
                                             key=str(data['Datastream']['@iot.id']).encode('utf-8'))
                            producer.poll(0)  # using poll(0), as Eden Hill mentions it avoids BufferError: Local: Queue full
                            # pass
                        except:
                            logger_logs.exception(
                                "Exception while sending: {} \non kafka topic: {}".format(data, NEW_KAFKA_TOPIC))

                    elif msg.error().code() != KafkaError._PARTITION_EOF:
                        print(msg.error())
                        logger_logs.error('Exception in Kafka-Logstash Streaming: {}'.format(msg))

                    t = time.time()
                    if t - ts_refreshed_mapping > REFRESH_MAPPING_EVERY:
                        self.id_mapping = self.empty_id_mapping()
                        ts_refreshed_mapping = t
                    time.sleep(0.010)

            except Exception as error:
                logger_logs.error("Error in Kafka-Logstash Streaming: {}".format(error))
                adapter_status["status"] = "Last error occured at {}: Error msg: {}"\
                    .format(time.ctime(), str(error))
                logger_logs.warning('Status of Adapter: {}'.format(adapter_status))
                with open(STATUS_FILE, "w") as f:
                    f.write(json.dumps(adapter_status))


if __name__ == '__main__':
    # Load variables set by docker-compose, enable kafka as data input and sensorthings mapping by default
    enable_kafka_adapter = True
    if os.getenv('enable_kafka_adapter', "true") in ["false", "False", 0]:
        enable_kafka_adapter = False

    enable_sensorthings = True
    if os.getenv('enable_sensorthings', "true") in ["false", "False", 0]:
        enable_sensorthings = False

    # Create an kafka to logstash instance
    adapter_instance = KafkaStAdapter(enable_kafka_adapter, enable_sensorthings)

    # start kafka to logstash streaming in a subprocess
    kafka_streaming = Process(target=adapter_instance.stream_kafka, args=())
    kafka_streaming.start()

    app.run(host="0.0.0.0", debug=False, port=3031)
