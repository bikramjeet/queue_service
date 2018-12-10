/* jshint node: true */
/* jshint esnext: true */
'use strict';

/**
* Defines the handlers for validating the queue fields along with the format of incoming data, default constructor is used for initialization of the instance variable during instantiation of a class.
* @class ValidateStoreFields
**/
class ValidateStoreFields {
    
    /**
    * @callback cb Method to be called on complete.
    **/
    
    constructor() {
        
    }
    
    /**
    * Validates the incoming fields to fetch the data from the queue store.
    * @param {Object} fields Holds the inputs based on which the data is to be fetched from the queue.
    * @param {String} fields.identifier Group category name.
    * @param {String} fields.key Unique identifier under the specific group.
    * @param {Array} [fields.store] The name of different connectors.
    * @param {Object} [skipFields] The keys to be skipped for mandatory checks.
    * @param {cb} callback The callback that handles the response.
    **/
    validateQueueFields (fields, callback, skipFields) {
        if(callback === undefined || typeof callback !== "function") {
            throw new Error(`Callback is expected and must be a function`);
        } else if (!fields || typeof fields !== "object" || Object.keys(fields).length === 0) {
            return `'Queue data' is either missing or not in the specified format`;
        } else if (!fields.identifier || typeof fields.identifier !== "string" || fields.identifier.trim() === "") {
            return `'Identifier' is either missing or not in the specified format`;
        } else if (((!!skipFields && !skipFields.key) || skipFields === undefined) && (!fields.key || typeof fields.key !== "string" || fields.key.trim() === "")) {
            return `'Key' is either missing or not in the specified format`;
        } else if (fields.store !== undefined && (!(fields.store instanceof Array) || fields.store.length === 0)) {
            return `'Store' value is either blank or not in the specified format`;
        } else {
            return false;
        }
    }
}

module.exports = ValidateStoreFields;