import os
import sys
import time
import re
import json
import yaml
import logging
from multiprocessing import Process
from datetime import datetime
import requests
from flask import Flask, jsonify, request
from redis import Redis
from logstash import TCPLogstashHandler

__date__ = "20 April 2018"
__version__ = "1.2"
__email__ = "christoph.schranz@salzburgresearch.at"
__status__ = "Development"
__desc__ = """The datachannel services manages kafka topics on the broker, clones the datachannel contract onto the provided 
functionality of SensorThings and provides API."""

# KAFKA_GROUP_ID = "iot86"  # use il060 if used in docker swarm
# # If executed locally with python, the KAFKA_GROUP_ID won't be changed
# KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', KAFKA_GROUP_ID)  # overwrite iot86 by envfile ID=il060
# # if deployed in docker, the adapter will automatically use the entry in the .env file.
#
# # logstash parameters
# HOST_default = KAFKA_GROUP_ID  # 'il060'   # use the local endpoint: equals hostname
# PORT_default = 5000

# Kafka parameters
STATUS_FILE = "status.log"
ZOOKEEPER_HOST = "localhost:2181"
NUMBER_OF_REPLICAS = 1
NUMBER_OF_PARTITIONS = 1
RETENTION_TIME = 6  # in months

# Sensorthings parameters
# ST_SERVER = "http://localhost:8084/v1.0/"  # reachable only from host outside docker
ST_SERVER = "http://gost:8080/v1.0/"  # GOST server is reachable within kafka stack with that
# REFRESH_MAPPING_EVERY = 5 * 60  # in seconds

logger = logging.getLogger('kafka_manager.logging')
logger.setLevel(os.getenv('LOG_LEVEL', logging.INFO))
console_logger = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(console_logger)
logger.info("Started data-channel service with hosts: {}, {}".format(ZOOKEEPER_HOST, ST_SERVER))

# webservice setup
app = Flask(__name__)
redis = Redis(host='redis', port=6379)
logger.info("Added flask API on port {}.".format(6379))

build_date = str(datetime.now())


def list_topics():
    """
    This function returns all kafka topics.
    :return: a list of registered topics
    """
    pipe = os.popen("kafka-topics --list --zookeeper {}".format(ZOOKEEPER_HOST))
    topics = pipe.read().split("\n")
    return topics


def get_adapter_status():
    try:
        with open(STATUS_FILE) as f:
            adapter_status = json.loads(f.read())
    except:
        adapter_status = {"application": "kafka-manager",
                          "status": "initialisation",
                          "build_date": build_date}
    return adapter_status


def reassemble_kafka_from_st():
    """
    When the broker is started, this function is called to restore the kafka topics from the stored
    SensorThings contracts.
    :return:
    """
    time.sleep(15)  # wait for kafka and GOST server (
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

        thing = yaml.safe_load(response.text)  # load safely because unicode may cause errors
        channel_id = thing.get("@iot.id")
        company_id = thing.get("properties").get("owner")

        topic_name = "eu.channelID_{chID}.companyID_{compID}".format(chID=channel_id, compID=company_id)
        topic_name = re.sub("[^a-zA-Z.0-9_-]+", "", topic_name.replace(" ", "-"))

        status = create_topic(topic_name)
        stati.append(status)

    logger.info("dc-service successfully restored all kafka topics from Sensorthings")
    adapter_status = get_adapter_status()
    adapter_status["status"] = "running"
    with open(STATUS_FILE, "w") as f:
        f.write(json.dumps(adapter_status))


def get_topic_name(payload):
    """
    Given a payload, this function creates an unique name that is valid for creating a kafka topic.
    :return: a kafka-topic name form eu.ChannelID_<channel_id>.CompanyID_<companyID>
    """
    channel_id = payload.get("@iot.id")
    company_id = payload.get("properties").get("owner")
    topic_name = "eu.channelID_{chID}.companyID_{compID}".format(chID=channel_id, compID=company_id)
    #  only alphanumerics and _.- are allowed
    topic_name = re.sub("[^a-zA-Z.0-9_-]+", "", topic_name.replace(" ", "-"))
    return topic_name


def create_topic(topic_name):
    """
    Given a kafka topic name, this function calls the internal cli to create a topic.
    :return: status string. Success, topic already exists or an other error message
    """
    cmd = """kafka-topics --create --zookeeper {zoo} --topic {tpc} 
--replication-factor {rep} --partitions {par} --config cleanup.policy=compact --config flush.ms=60000 
--config retention.ms={ret}
""".format(zoo=ZOOKEEPER_HOST, tpc=topic_name, par=NUMBER_OF_PARTITIONS, rep=NUMBER_OF_REPLICAS,
           ret=RETENTION_TIME*31*24*3600*1000).replace("\n", "")
    pipe = os.popen(cmd)
    response = pipe.read()

    if "\nCreated" in response:
        # Filter line of form: Created topic "eu.channelID_1.companyID_ownername".
        response = response.split("\n")[-2]
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
    """
    API for submitting a data-channel contract
    :return: Payload of the SensorThing call, or traceback code.
    """
    logger.info("Received new contract")

    adapter_status = get_adapter_status()
    if adapter_status["status"] != "running":
        logger.error("Can't submit contract during initialisation")
        return jsonify({"status": "Can't submit contract during initialisation"}), 503

    headers = {'content-type': 'application/json'}
    try:
        response = requests.request("POST", ST_SERVER + "Things", data=request.data, headers=headers)
    except:
        return jsonify({"status": "Couldn't parse input"}), 406

    if response.status_code not in [200, 201, 202]:
        logger.warning("Posting contract to SensorThings failed with status {}".format(response.status_code))
        logger.warning("Couldn't create instance {}".format(response.text))
        return jsonify({"Couldn't create instance {}".format(response.text)}), 409

    payload = yaml.safe_load(response.text)
    logger.info("Added contract with name {} to SensorThings".format(payload.get("name")))

    topic_name = get_topic_name(payload)
    status = create_topic(topic_name)
    logger.info("Created kafka topic with name {tpc} on system {sys}, returned status: {stat}"
                .format(tpc=topic_name, sys=payload.get("name"), stat=status))

    payload["@iot.dataChannelID"] = topic_name
    return jsonify(payload), 201


# http://0.0.0.0:3033/
@app.route('/')
@app.route('/status')
def print_adapter_status():
    """
    This function is called by a sebserver request and prints the current meta information.
    :return: status
    """
    adapter_status = get_adapter_status()
    adapter_status["topics"] = list_topics()
    with open(STATUS_FILE, "w") as f:
        f.write(json.dumps(adapter_status))
    # adapter_status["status"] = ["running" if not lock else "initialisation"][0]
    return jsonify(adapter_status)


if __name__ == '__main__':
    restore_from_sensorthings = Process(target=reassemble_kafka_from_st, args=())
    restore_from_sensorthings.start()

    app.run(host="0.0.0.0", debug=False, port=3033)
