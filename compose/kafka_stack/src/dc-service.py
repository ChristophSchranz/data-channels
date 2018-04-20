import os
import sys
import time
import re
import json
import yaml
import logging
from multiprocessing import Process
import requests
from flask import Flask, jsonify, request
from redis import Redis
from logstash import TCPLogstashHandler

# confluent_kafka is based on librdkafka, details in requirements.txt
# from confluent_kafka import Consumer, KafkaError

# Why using a kafka to logstash adapter, while there is a plugin?
# Because there are measurements, as well as observations valid as SensorThings result.
# Kafka Adapters seems to use only one topic
# ID mapping is pretty much straightforward with a python script


__date__ = "20 April 2018"
__version__ = "1.2"
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
NUMBER_OF_PARTITIONS = 1

RETENTION_TIME = 6  # in months


logger = logging.getLogger('kafka_manager.logging')
logger.setLevel(os.getenv('LOG_LEVEL', logging.INFO))
console_logger = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(console_logger)
logger.info("Started data-channel service")

# Sensorthings parameters
# ST_SERVER = "http://localhost:8084/v1.0/"  # reachable only from host outside docker
ST_SERVER = "http://gost:8080/v1.0/"  # GOST server is reachable within kafka stack with that
# REFRESH_MAPPING_EVERY = 5 * 60  # in seconds

# webservice setup
app = Flask(__name__)
redis = Redis(host='redis', port=6379)
logger.info("Added flask API on port {}.".format(6379))


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
def create_invalid():
    response = "creation of sensor must be of the form: '/create/<company>/<machine>/<sensor>'"
    return jsonify(response)


# http://0.0.0.0:3033/create_sensor
# header: application/json
@app.route('/create_sensor', methods=['GET', 'POST'])
def create_topic_api():
    data = json.loads(request.data)
    company = data["sensor"].get("company", "")
    system = data["sensor"].get("system", "")
    sensor = data["sensor"].get("sensor", "")
    result = data["sensor"].get("result", "")
    topic_name = ".".join([str(i) for i in ["eu", company, system, sensor]])[:-1]
    logger.info("Added sensor with topic {}, result is of type {}.".format(topic_name, type(result)))

    cmd = """kafka-topics --create --zookeeper {zoo} --topic {tpc}
 --replication-factor {rep} --partitions 1 --config cleanup.policy=compact --config flush.ms=60000
 --config retention.ms={ret}
""".format(zoo=ZOOKEEPER_HOST, tpc=topic_name, rep=NUMBER_OF_REPLICAS,
           ret=RETENTION_TIME*31*24*3600*1000).replace("\n", "")
    pipe = os.popen(cmd)
    response = pipe.read()
    if "\nCreated" in response:
        response = "Created topic" + response.split("\nCreated topic")[-1].\
            replace("\n", "").replace('"', "")  # [:-1]
    elif "Error" in response and "already exists." in response:
        # Topic already exists
        logger.warning("Couldn't create sensor with topic {}, topic already exists.".format(topic_name))
        return jsonify({"Error, sensor already exists": str(response)}), 403
    else:
        logger.warning("Couldn't create sensor with topic {}".format(topic_name))
        return jsonify({"Couldn't create sensor with topic": str(response)}), 404
    logger.info("Added sensor with topic {}, result is of type {}.".format(topic_name, type(result)))
    logger.info(response)
    return jsonify(data)


def reassemble_kafka_from_st():
    time.sleep(20)  # wait for kafka and GOST server
    print("Trying to restore kafka topics from SensorThings")

    thing_id = 0
    stati = list()
    while True:
        thing_id += 1
        response = requests.request(
            "GET", ST_SERVER+"Things({id})?$expand=Locations,Sensor,Datastreams,Observations,ObservedProperty"
            .format(id=thing_id))
        if response.status_code not in [200]:
            break
        logger.info(response)
        thing = yaml.safe_load(response.text)

        channelID = thing.get("@iot.id")
        companyID = thing.get("properties").get("owner")

        topic_name = "eu.channelID_{chID}.companyID_{compID}".format(chID=channelID, compID=companyID)
        topic_name = re.sub("[^a-zA-Z.0-9_-]+", "", topic_name.replace(" ", "-"))
        logger.info(topic_name)

        status = create_topic(topic_name)
        stati.append(status)
        logger.info("dc-service successfully restored kafka topic {} from Sensorthings".format(topic_name))

    tracebacks = [status for status in stati if "Successfully created topic" != status]
    if tracebacks is not []:
        logger.warning("dc-service encountered errors while restoring topics: {}".format(tracebacks))


def get_topic_name(payload):
    # is of the form eu.owner.thingname, only alphanumerics and .-_ are allowed
    # topic_name = "eu."+str(payload["properties"]["owner"])+"."+str(payload["name"])

    # is of the form eu.ChannelID_<channelID>.CompanyID_<companyID>
    channelID = payload.get("@iot.id")
    companyID = payload.get("properties").get("owner")
    topic_name = "eu.ChannelID_{chID}.CompanyID_{compID}".format(chID=channelID, compID=companyID)
    topic_name = re.sub("[^a-zA-Z.0-9_-]+", "", topic_name.replace(" ", "-"))
    return topic_name


def create_topic(topic_name):
    cmd = """kafka-topics --create --zookeeper {zoo} --topic {tpc}
 --replication-factor {rep} --partitions {par} --config cleanup.policy=compact --config flush.ms=60000
 --config retention.ms={ret}
""".format(zoo=ZOOKEEPER_HOST, tpc=topic_name, par=NUMBER_OF_PARTITIONS, rep=NUMBER_OF_REPLICAS,
           ret=RETENTION_TIME*31*24*3600*1000).replace("\n", "")
    pipe = os.popen(cmd)
    response = pipe.read()
    print(response)
    if "\nCreated" in response:
        response = "Created topic" + response.split("\nCreated topic")[-1].\
            replace("\n", "").replace('"', "")
    elif "Error" in response and "already exists." in response:  # Topic already exists
        logger.warning("Couldn't create sensor with topic {}, topic already exists.".format(topic_name))
        return "Error, instance already exists"
    else:  # Misc Error
        logger.warning("Couldn't create sensor with topic {}".format(topic_name))
        return "Couldn't create instance with response: {}".format(response)

    logger.info(response)
    return "Successfully created topic"


# http://0.0.0.0:3033/submit_contract
# payload is the contract where sensors are specified
@app.route('/submit_contract', methods=['GET', 'POST'])
def submit_contract():
    logger.info("Received contract")
    # payload = json.loads(request.data)

    headers = {'content-type': 'application/json'}
    response = requests.request("POST", ST_SERVER + "Things", data=request.data, headers=headers)
    payload = json.loads(response.text)

    if response.status_code not in [200, 202]:
        logger.warning("Posting contract to SensorThings failed with status {}".format(response.status_code))
        logger.warning("Couldn't create instance {}".format(response.text))
        return jsonify({"Couldn't create instance {}".format(response.text)}), 409

    logger.info("Added topic with name {} to SensorThings".format(payload.get("name")))

    topic_name = get_topic_name(payload)
    status = create_topic(topic_name)
    logger.info("Created kafka topic with name: {}, returned status {}".format(topic_name, status))

    return jsonify(payload)


if __name__ == '__main__':
    restore_from_sensorthings = Process(target=reassemble_kafka_from_st, args=())
    restore_from_sensorthings.start()

    app.run(host="0.0.0.0", debug=False, port=3033)
