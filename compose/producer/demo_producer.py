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

from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

BOOTSTRAP_SERVERS = 'il061,il062,il063'
SCHEMA_REGISTRY_HOST = 'http://127.0.0.1:8082'

value_schema_str = """
{
   "namespace": "my.test",
   "name": "value",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

key_schema_str = """
{
   "namespace": "my.test",
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "name",
       "type" : "string"
     }
   ]
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)
value = {"name": "Value"}
key = {"name": "Key"}

avroProducer = AvroProducer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'schema.registry.url': SCHEMA_REGISTRY_HOST
    }, default_key_schema=key_schema, default_value_schema=value_schema)

avroProducer.produce(topic='test-avro', value=value, key=key)
avroProducer.flush()
