var config = {
    "constructor" : {
        "schema" : {
            "blank_value" : false,
            "elements" : {
                "redis" : {
                    "queueConnector" : [
                        {
                            "host" : "",
                            "port" : 0
                        }
                    ],
                    "serviceName" : "",
                    "identifierSet" : []
                }
            },
            "mandatory_elements" : ["queueConnector", "host", "port", "serviceName", "identifierSet"]
        }
    }
};

module.exports = config;
