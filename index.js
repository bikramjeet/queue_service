/* jshint node: true */
/* jshint esnext: true */
'use strict';

const async = require('async');
const moment = require('moment');
const config = require('./config');
const validator = require('sanitation');
const redisOperation = require('./redis_io');
const ValidQueueFields = require('./ValidateStoreFields');

/**
* Defines the handlers for pushing and retrieving the data from the queue, default constructor is used for initialization of the instance variable during instantiation of a class.
* @class QueueHandler
**/
class QueueHandler extends ValidQueueFields {
    
    /**
    * @callback cb Method to be called on complete.
    **/
    
    constructor(connectionConfig) {
        super();
        if (!connectionConfig || typeof connectionConfig !== "object" || connectionConfig instanceof Array || Object.keys(connectionConfig).length === 0) {
            throw new Error(`'Connection config' is either missing or not in the specified format`);
        }
        let validation = validator.paramsValidator(connectionConfig, config.constructor.schema.elements, config.constructor.schema.mandatory_elements, config.constructor.schema.blank_value);
        if(!validation.success) {
            throw new Error(validation.response.errorMsg);
        } else if (Object.keys(validation.elements).length === 0) {
            throw new Error(`Minimum one valid queue store is required`);
        }
        this.connectionHandler = {};
        this.servicesInfo = "registeredServices";
        let date = moment(new Date()).format("YYYY-MM-DDTHH:mm:ss");
        async.eachOf(validation.elements, (value, serviceStore, asyncEachCallback) => {
            switch(serviceStore.toLowerCase()) {
                case 'redis' :
                    this.connectionHandler.redis = {};
                    this.connectionHandler.redis.connection = redisOperation(value.queueConnector);
                    this.connectionHandler.redis.serviceName = value.serviceName;
                    this.connectionHandler.redis.identifierSet = [];
                    async.each(value.identifierSet, (identifierName, innerAsyncEachCallback) => {
                        if(identifierName.trim() === "") {
                            return innerAsyncEachCallback(null);
                        }
                        this.connectionHandler.redis.identifierSet.push(identifierName.trim());
                        async.waterfall([
                            (waterfallCallback) => {
                                this.connectionHandler.redis.connection.getHashObject(this.servicesInfo, `${value.serviceName}_${identifierName.trim()}`, (err, response) => {
                                    /**
                                    * Put any handler if required.
                                    **/
                                    waterfallCallback(err, response);
                                });
                            },
                            (result, waterfallCallback) => {
                                if(result) {
                                    return waterfallCallback(null, result);
                                }
                                this.connectionHandler.redis.connection.setHashObject(this.servicesInfo, `${value.serviceName}_${identifierName.trim()}`, date, (err, status) => {
                                    /**
                                    * Put any handler if required.
                                    **/
                                    waterfallCallback(null, status);
                                });
                            }
                        ], (err, response) => {
                            innerAsyncEachCallback(err);
                        });
                    }, (err) => {
                        asyncEachCallback(err);
                    });
                    break;
                default :
                    asyncEachCallback(`Not a valid queue store`);
                    break;
            }
        }, (err) => {
            if (err) {
                throw new Error(err);
            }
        });
    }
    
    /**
    * Inserts the data into the queue based on group.
    * @param {Object} queueData Holds the data to be queued. 
    * @param {String} queueData.identifier Unique name to group all the data under specific set while storing the information for log/push/event etc. 
    * @param {String} queueData.key Unique identifier under the specific group. 
    * @param {Object} queueData.value The data for the unique identifier. 
    * @param {Array} [queueData.store] The name of different connectors. 
    * @param {cb} [callback] The callback that handles the response.
    **/
    pushToQueue (queueData, callback) {
        let errMsg = "";
        let validInsertFields = this.validateQueueFields(queueData, callback);
        if(validInsertFields) {
            errMsg = validInsertFields;
        } else if (!queueData.value || typeof queueData.value !== "object" || Object.keys(queueData.value).length === 0) {
            errMsg = `'Value' is either missing or not in the specified format`;
        } else if (!queueData.value.targetType || !(queueData.value.targetType instanceof Array) || queueData.value.targetType.length === 0) {
            errMsg = `'Target type' is either missing or not in the specified format`;
        } else {
            async.eachOf(this.connectionHandler, (value, serviceStore, asyncEachCallback) => {
                if(queueData.store !== undefined && queueData.store.indexOf(serviceStore.toLowerCase()) === -1) {
                    return asyncEachCallback(null);
                }
                switch(serviceStore.toLowerCase()) {
                    case 'redis' :
                        if(value.identifierSet.indexOf(queueData.identifier) === -1) {
                            return asyncEachCallback(`This service is not registered with redis store for the '${queueData.identifier}' identifier`);
                        }
                        value.connection.setHashObject(queueData.identifier, queueData.key, queueData.value, (err, status) => {
                            asyncEachCallback(err);
                        });
                        break;
                    default :
                        asyncEachCallback(`Not a valid queue store`);
                        break;
                }
            }, (err) => {
                if (callback !== undefined) {
                    return callback(err);
                }
                if (err) {
                    throw new Error(err);
                } else {
                    return true;
                }
            });
            return;
        }
        if(callback !== undefined) {
            callback(errMsg, null);
        } else {
            throw new Error(errMsg);
        }
    }
    
    /**
    * Retrieves the data from the queue based on group and key.
    * @param {Object} readQueueData Holds the inputs based on which the data from key is to be fetched from the queue.
    * @param {String} readQueueData.identifier Group category name. 
    * @param {String} readQueueData.key Unique identifier under the specific group.
    * @param {Array} [readQueueData.store] The name of different connectors.
    * @param {cb} callback The callback that handles the response.
    **/
    readFromQueue (readQueueData, callback) {
        let validReadFields = this.validateQueueFields(readQueueData, callback);
        if(validReadFields) {
            return callback(validReadFields);
        }
        let result = {};
        async.eachOf(this.connectionHandler, (value, serviceStore, asyncEachCallback) => {
            if(readQueueData.store !== undefined && readQueueData.store.indexOf(serviceStore.toLowerCase()) === -1) {
                return asyncEachCallback(null);
            }
            switch(serviceStore.toLowerCase()) {
                case 'redis' :
                    if(value.identifierSet.indexOf(readQueueData.identifier) === -1) {
                        return asyncEachCallback(`This service is not registered with redis store for the '${readQueueData.identifier}' identifier`);
                    }
                    async.waterfall([
                        (waterfallCallback) => {
                            value.connection.getHashObject(readQueueData.identifier, readQueueData.key, (err, response) => {
                                if(response && response.length) {
                                    result.redis = response;
                                }
                                waterfallCallback(err, response);
                            });
                        },
                        (response, waterfallCallback) => {
                            if(!response) {
                                return waterfallCallback(null, response);
                            }
                            value.connection.setHashObject(this.servicesInfo, `${value.serviceName}_${readQueueData.identifier}`, (this.redisReadDate || moment(new Date()).format("YYYY-MM-DDTHH:mm:ss")), (err, status) => {
                                /**
                                * Put any handler if required.
                                **/
                                waterfallCallback(null, status);
                            });
                        }
                    ], (err, response) => {
                        asyncEachCallback(err);
                    });
                    break;
                default :
                    asyncEachCallback(`Not a valid queue store`);
                    break;
            }
        }, (err) => {
            return callback(err, result);
        });
    }
    
    /**
    * Retrieves the keys from the queue based on group.
    * @param {Object} readKeyQueueData Holds the inputs based on which the data is to be fetched from the queue.
    * @param {String} readKeyQueueData.identifier Group category name. 
    * @param {Array} [readKeyQueueData.store] The name of different connectors.
    * @param {cb} callback The callback that handles the response.
    **/
    readKeysFromQueue (readKeyQueueData, callback) {
        let validReadKeyFields = this.validateQueueFields(readKeyQueueData, callback, {'key' : true});
        if(validReadKeyFields) {
            return callback(validReadKeyFields);
        }
        let result = {};
        async.eachOf(this.connectionHandler, (value, serviceStore, asyncEachCallback) => {
            if(readKeyQueueData.store !== undefined && readKeyQueueData.store.indexOf(serviceStore.toLowerCase()) === -1) {
                return asyncEachCallback(null);
            }
            switch(serviceStore.toLowerCase()) {
                case 'redis' :
                    if(value.identifierSet.indexOf(readKeyQueueData.identifier) === -1) {
                        return asyncEachCallback(`This service is not registered with redis store for the '${readKeyQueueData.identifier}' identifier`);
                    }
                    value.connection.getHashKey(readKeyQueueData.identifier, (err, response) => {
                        if(response && response.length) {
                            result.redis = response;
                        }
                        asyncEachCallback(err);
                    });
                    break;
                default :
                    asyncEachCallback(`Not a valid queue store`);
                    break;
            }
        }, (err) => {
            return callback(err, result);
        });
    }
    
    /**
    * Retrieves the key and value from the queue based on group.
    * @param {Object} readKeyValQueueData Holds the inputs based on which the key and value data is to be fetched from the queue.
    * @param {String} readKeyValQueueData.identifier Group category name. 
    * @param {Array} [readKeyValQueueData.store] The name of different connectors.
    * @param {cb} callback The callback that handles the response.
    **/
    readKeysAndValuesFromQueue (readKeyValQueueData, callback) {
        let validReadKeyAndValueFields = this.validateQueueFields(readKeyValQueueData, callback, {'key' : true});
        if(validReadKeyAndValueFields) {
            return callback(validReadKeyAndValueFields);
        }
        let result = {};
        async.eachOf(this.connectionHandler, (value, serviceStore, asyncEachCallback) => {
            if(readKeyValQueueData.store !== undefined && readKeyValQueueData.store.indexOf(serviceStore.toLowerCase()) === -1) {
                return asyncEachCallback(null);
            }
            switch(serviceStore.toLowerCase()) {
                case 'redis' :
                    if(value.identifierSet.indexOf(readKeyValQueueData.identifier) === -1) {
                        return asyncEachCallback(`This service is not registered with redis store for the '${readKeyValQueueData.identifier}' identifier`);
                    }
                    async.waterfall([
                        (waterfallCallback) => {
                            value.connection.getHashKey(readKeyValQueueData.identifier, (err, response) => {
                                this.redisReadDate = moment(new Date()).format("YYYY-MM-DDTHH:mm:ss");
                                waterfallCallback(err, response);
                            });
                        },
                        (keyList, waterfallCallback) => {
                            if(!keyList || keyList.length === 0) {
                                return waterfallCallback(null, keyList);
                            }
                            result.redis = [];
                            async.each(keyList, (name, innerAsyncEachCallback) => {
                                let dataObj = Object.assign({}, readKeyValQueueData);
                                dataObj.key = name;
                                dataObj.store = ['redis'];
                                this.readFromQueue(dataObj, (err, response) => {
                                    if(response && response.redis && response.redis.length) {
                                        dataObj.value = response.redis;
                                        result.redis.push(dataObj);
                                    }
                                    innerAsyncEachCallback(err);
                                });
                            }, (err) => {
                                return waterfallCallback(err);
                            });
                        }
                    ], (err, response) => {
                        this.redisReadDate = null;
                        asyncEachCallback(err);
                    });
                    break;
                default :
                    asyncEachCallback(`Not a valid queue store`);
                    break;
            }
        }, (err) => {
            return callback(err, result);
        });
    }
    
    /**
    * Deletes the data from the queue based on group and key.
    * @param {Object} deleteQueueData Holds the inputs based on which the data is to be deleted from the queue.
    * @param {String} deleteQueueData.identifier Group category name. 
    * @param {String} deleteQueueData.key Unique identifier under the specific group.
    * @param {Array} [deleteQueueData.store] The name of different connectors.
    * @param {cb} [callback] The callback that handles the response.
    **/
    deleteKeyFromQueue (deleteQueueData, callback) {
        let validDeleteKeyFields = this.validateQueueFields(deleteQueueData, callback);
        if(validDeleteKeyFields) {
            return callback(validDeleteKeyFields);
        }
        let result = {};
        async.eachOf(this.connectionHandler, (value, serviceStore, asyncEachCallback) => {
            if(deleteQueueData.store !== undefined && deleteQueueData.store.indexOf(serviceStore.toLowerCase()) === -1) {
                return asyncEachCallback(null);
            }
            switch(serviceStore.toLowerCase()) {
                case 'redis' :
                    if(value.identifierSet.indexOf(deleteQueueData.identifier) === -1) {
                        return asyncEachCallback(`This service is not registered with redis store for the '${deleteQueueData.identifier}' identifier`);
                    }
                    value.connection.deleteHashKey(deleteQueueData.identifier, deleteQueueData.key, (err, response) => {
                        if(response) {
                            result.redis = response;
                        }
                        asyncEachCallback(err);
                    });
                    break;
                default :
                    asyncEachCallback(`Not a valid queue store`);
                    break;
            }
        }, (err) => {
            if(callback !== undefined) {
                return callback(err, result);
            }
            if (err) {
                throw new Error(err);
            } else {
                return true;
            }
        });
    }
}

module.exports = QueueHandler;