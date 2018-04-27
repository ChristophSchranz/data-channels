# Data Channel Service for Streaming Data within or accross companies on nimble

Based on a specified contract negotiated between two
registered companies on nimble, we support data streaming
of machine data.

The DataChannel Service is composed from the following components:
* [Kafka Stack](https://kafka.apache.org/) version **4.0.0** (based on Kafka 0.11) with following subcomponents:
    * Kafka Brokers
    * Kafka Streams
    * Kafka Rest API
    * Custum Kafka-Topic Management service

* SensorThings Server [GOST](https://github.com/gost/server) for semantic description




## Contents

1. [Requirements](#requirements)
2. [Usage](#usage)
3. [Trouble-Shooting](#Trouble-shooting)


## Requirements

1. Install [Docker](https://www.docker.com/community-edition#/download) version **1.10.0+**
2. Install [Docker Compose](https://docs.docker.com/compose/install/) version **1.6.0+**
3. Clone this repository


## Usage

Using `docker-compose`:

```bash
cd compose
sudo docker-compose up --build -d
```

The flag `-d` stands for running it in background (detached mode):


Watch the logs with:

```bash
sudo docker-compose logs -f
```



### Kafka-Services:

List topic names:
```
http://hostname:8082/topics
```


Get kafka-stack status:
```
http://hostname:3033
```


### Producer services:
Submit a contract to create a new channel
```
http://hostname:3033/submit_contract
{
  "name": "Ultimaker 2, number11",
  "description": "A 3D printing system in the 3D printing farm.",
  "properties": {
		"owner": "Salzburg Research",
		"from": "2018-04-19T13:11:12+00:00",
		"to": "2018-06-09T13:11:12+00:00",
		"viewers": ["viewingcompany"],
    "monitoring_clauses": {
			"machined part": "partID",
			"machined order": "orderID"
		}
  },
  "Locations": [{
    "name": "IoT Lab",
    "description": "Salzburg Research",
    "encodingType": "application/vnd.geo+json",
    "location": {
      "type": "Point",
      "coordinates": [13.040631, 47.822782]
    }
  }],
  "Datastreams": [{
    "name": "Air Temperature DS",
    "description": "Datastream for recording temperature",
    "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
    "unitOfMeasurement": {
      "name": "Degree Celsius",
      "symbol": "degC",
      "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius"
    },
    "ObservedProperty": {
      "name": "Area Temperature",
      "description": "The degree or intensity of heat present in the area",
      "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#AreaTemperature"
    },
    "Sensor": {
      "name": "DHT22",
      "description": "DHT22 temperature sensor",
      "encodingType": "application/pdf",
      "metadata": "https://cdn-shop.adafruit.com/datasheets/DHT22.pdf"
    }
  },
								 {
    "name": "Nozzle Temperature DS",
    "description": "Datastream for recording temperature of the printe's nozzle",
    "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
    "unitOfMeasurement": {
      "name": "Degree Celsius",
      "symbol": "degC",
      "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius"
    },
    "ObservedProperty": {
      "name": "Nozzle Temperature",
      "description": "The degree or intensity of heat of the printer's nozzle",
      "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#AreaTemperature"
    },
    "Sensor": {
      "name": "PT100",
      "description": "PT100 temperature sensor",
      "encodingType": "application/pdf",
      "metadata": "http://irtfweb.ifa.hawaii.edu/~iqup/domeenv/PDF/pt100plat.pdf"
    }
  }]
}
```
This request will return an augmented payload. One important field is
"@iot.dataChannelID" which is an unique identifier for created data-channel.


Send Data on a channel
```
http://hostname:8082/topics/<@iot.dataChannelID>
{
  "records": [
    {
      "key": "998",
      "value": {"iot.id": 998,
	"phenomenonTime": "{% now 'iso-8601', '' %}",
	"resultTime": "{% now 'iso-8601', '' %}",
	"result": 1234.56}
    }
  ]
}
```


### Consumer Services:
Create a Consumer
```
http://hostname:8082/consumers/my-consumer-group
{
  "name": "my_consumer_json",
  "format": "json",
  "auto.offset.reset": "earliest",
  "auto.commit.enable": "false"
}
```


Subscribe Consumer to topics
```
http://hostname:8082/consumers/my-consumer-group/instances/my_consumer_json/subscription
{
  "topics": [
    "<@iot.dataChannelID>"
  ]
}
```

Get Data from Consumer
```GET
http://hostname:8082/consumers/my-consumer-group/instances/my_consumer_json/records?timeout=1000&max_bytes=300000
```


## Trouble-Shooting

* ERROR org.apache.kafka.common.errors.TopicExistsException: Topic 'eu.channelID_10.companyID_Salzburg-Research' already exists.
If data is sent before kafka topics are restored, an error will be returned.
