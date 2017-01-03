# queue_service


This is a simple queue service which can be used across various Node.JS applications to make them communicate via the redis "QUEUE" especially in the microservice architecture design for writing consolidated logs in a single place, maintain common information to be shared across multiple servers etc.

## Listed below some of the key features

- Very simple and flexible to use.
- Configurable rules and patterns.
- Configurable queue stores.
- Can be used in Monolithic, Microservices as well as Distributed architectures.
- Can be used for report logs, shared information for persistence etc.

## Install

```javascript
npm install queue_service
```

## Dependencies

```javascript
"dependencies": {
    "async": "2.1.4",
    "moment": "2.17.1",
    "redis": "2.6.3",
    "sanitation": "^1.0.2"
};
```

## Use

```javascript
let queue = require('queue_service');

let connectionConfig = {
    "redis" : {
        "queueConnector" : [
            {
                "host" : localhost,
                "port" : 6679
            }
        ],
        "serviceName" : "test_servie",
        "identifierSet" : ["log", "sharedData"]
    }
};

let queueHandler = new queue(connectionConfig);
```

## Emitter Applications

- Register the "service" to the queue along with "service actions identifiers".
- Define your custom "queue data", each queue data is identified by an unique identifier and key which the client can set for each "queue data" along with the intended target destination to consume.
- Execute the Queue CRUD operations using the exposed methods.

### Quick Example

```javascript
/**
* Push the data to queue
**/

let customData = {
    <key1> : <value1>,
    <key2> : <value2>,
    targetType : [<value1>, <value2>]
};

let queueData = {
    identifier : 'log',
    key : <unique_id>,
    value : customData
};

queueHandler.pushToQueue(queueData, (err, status) => {
    /**
    * Define your handler for error and success scenario
    **/
});
```

## Listener Applications

- Register the "service" to the queue along with "service actions identifiers".
- Watch for "queue data" in the registered stores for the specific identifier and the target destination to process it.
- Execute the Queue CRUD operations using the exposed methods.

### Quick Example

```javascript
/**
* Fetch all the keys along with their values from the queue for the specific identifier
**/

let fetchQueueData = {
	identifier : 'log'
};

queueHandler.readKeysAndValuesFromQueue(fetchQueueData, (err, result) => {
    /**
    * Define your handler for error and success scenario
    **/
});

/**
* Fetch all the keys from the queue for the specific identifier
**/

let fetchKeyFromQueueData = {
	identifier : 'log'
};

queueHandler.readKeysFromQueue(fetchKeyFromQueueData, (err, result) => {
    /**
    * Define your handler for error and success scenario
    **/
});

/**
* Fetch the value of the specific key from the queue for the specific identifier
**/

let fetchValueFromQueueData = {
	identifier : 'log',
	key : <unique_id>
};

queueHandler.readFromQueue(fetchValueFromQueueData, (err, result) => {
    /**
    * Define your handler for error and success scenario
    **/
});

/**
* Delete the record based on key from the queue for the specific identifier
**/

let deleteQueueData = {
	identifier : 'log'
	key : <unique_id>
};

queueHandler.deleteKeyFromQueue(deleteQueueData, (err, result) => {
    /**
    * Define your handler for error and success scenario
    **/
});
```