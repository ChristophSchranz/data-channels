import os
import sys
import time
import json
import logging
from multiprocessing import Process
import requests
from flask import Flask, jsonify
from redis import Redis
from logstash import TCPLogstashHandler

# confluent_kafka is based on librdkafka, details in requirements.txt
#from confluent_kafka import Consumer, KafkaError

# Why using a kafka to logstash adapter, while there is a plugin?
# Because there are measurements, as well as observations valid as SensorThings result.
# Kafka Adapters seems to use only one topic
# ID mapping is pretty much straightforward with a python script


__date__ = "05 April 2018"
__version__ = "1.1"
__email__ = "christoph.schranz@salzburgresearch.at"
__status__ = "Development"
__desc__ = """This program manages kafka topics on the broker."""

#
# # kafka parameters
# # topics and servers should be of the form: "topic1,topic2,..."
# KAFKA_TOPICS = "SensorData"  # TODO can be set as env, Also works for the logstash pipeline index filter
# BOOTSTRAP_SERVERS_default = 'il061,il062,il063'
#
# # "iot86" for local testing. In case of any data losses, temporarily use another group-id until all data is load.
# KAFKA_GROUP_ID = "iot86"  # use il060 if used in docker swarm
# # If executed locally with python, the KAFKA_GROUP_ID won't be changed
# KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', KAFKA_GROUP_ID)  # overwrite iot86 by envfile ID=il060
# # if deployed in docker, the adapter will automatically use the entry in the .env file.
#
# # logstash parameters
# HOST_default = KAFKA_GROUP_ID  # 'il060'   # use the local endpoint: equals hostname
# PORT_default = 5000
STATUS_FILE = "status.log"
ZOOKEEPER_HOST = "localhost:2181"
NUMBER_OF_REPLICAS = 1

RETENTION_TIME = 6  # in months

# # Sensorthings parameters
# ST_SERVER = "http://il060:8082/v1.0/"
# REFRESH_MAPPING_EVERY = 5 * 60  # in seconds

# webservice setup
app = Flask(__name__)
redis = Redis(host='redis', port=6379)

# http://0.0.0.0:3033/
@app.route('/')
@app.route('/status')
def print_adapter_status():
    """
    This function is called by a sebserver request and prints the current meta information.
    :return:
    """
    try:
        with open(STATUS_FILE) as f:
            adapter_status = json.loads(f.read())
    except:
        adapter_status = {"application": "kafka-manager",
                          "status": "running",
                          "topics": list_topics()}
    return jsonify(adapter_status)

def list_topics():
    pipe = os.popen("kafka-topics --list --zookeeper {}".format(ZOOKEEPER_HOST))
    topics = pipe.read().split("\n")
    return topics

# respond to invalid request
@app.route('/create')
@app.route('/create/<company>')
@app.route('/create/<company>/<machine>')
def create_invalid(**kwargs):
    response = "creation of sensor must be of the form: '/create/<company>/<machine>/<sensor>'"
    return jsonify(response)


# TODO maybe a department layer or plant is needed
# http://0.0.0.0:3033/create/srfg/ultimaker/temp122
@app.route('/create/<company>/<machine>/<sensor>')
def create_topic(company, machine, sensor, persistence=2):
    cmd = """kafka-topics --create --zookeeper {zoo} --topic eu.{com}.{mac}.{sns} 
 --replication-factor {rep} --partitions 1 --config cleanup.policy=compact --config flush.ms=60000 
 --config retention.ms={ret}
""".format(zoo=ZOOKEEPER_HOST, com=company, mac=machine, sns=sensor, rep=NUMBER_OF_REPLICAS,
           ret=RETENTION_TIME*31*24*3600*1000).replace("\n", "")
    pipe = os.popen(cmd)
    response = pipe.read()
    if "\nCreated" in response:
        response = "Created topic" + response.split("\nCreated topic")[-1].\
            replace("\n", "").replace('"', "")  # [:-1]
    elif "Error" in response and "already exists." in response:
        # Topic already exists
        pass

    # print(cmd)
    return jsonify(response)

# http://0.0.0.0:3033/create/srfg/ultimaker/temp122
@app.route('/create_avro/<company>/<machine>/<sensor>')
def create_topic(company, machine, sensor, persistence=2):
    cmd = """kafka-topics --create --zookeeper {zoo} --topic eu.{com}.{mac}.{sns} 
 --replication-factor {rep} --partitions 1 --config cleanup.policy=compact --config flush.ms=60000 
 --config retention.ms={ret}
""".format(zoo=ZOOKEEPER_HOST, com=company, mac=machine, sns=sensor, rep=NUMBER_OF_REPLICAS,
           ret=RETENTION_TIME*31*24*3600*1000).replace("\n", "")
    pipe = os.popen(cmd)
    response = pipe.read()
    if "\nCreated" in response:
        response = "Created topic" + response.split("\nCreated topic")[-1].\
            replace("\n", "").replace('"', "")  # [:-1]
    elif "Error" in response and "already exists." in response:
        # Topic already exists
        pass

    # print(cmd)
    return jsonify(response)

if __name__ == '__main__':

    app.run(host="0.0.0.0", debug=False, port=3033)
