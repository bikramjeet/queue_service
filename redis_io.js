var redis = require("redis");
var redisClient = []; 
//client.on("error", function (err) {
	//console.log("Error " + err);
//});


/* setObject will store the key-value pair if its not
 * already their, and update the value if key is already present
 * key can be a json object or string, the function internally
 * sort & compress it, to use it as a key if its an object.
 * value must be a string value.
 * callback function will be called with two parameters 
 * which are error message & the sorted-compressed jsonObject/string used
 * as key in the current operation
 * */
function setObject(key, value, callback){
	var rediskey = (typeof key == 'object')?JSON.stringify(sort(key)):key;
	var i = redisClient.length;
	while (i--) {
		redisClient[i].set(rediskey,value, function(err, reply){
			if( i === 0 ){
				callback(err,rediskey);
			}
		});
		if( i === 0 )break;
	}
}


/* getObject will query redis using key
 * and return the value, if it is already exists.
 * key can be a json object or string, the function internally
 * sort & compress it, to use it as a key if its an object.
 * callback function will be called with two parameters 
 * which are error message & returned value 
 * of key  
 * */
function getObject(key, callback, i){
	var rediskey = (typeof key == 'object')?JSON.stringify(sort(key)):key;
	i = i || 0;
	redisClient[i].get(rediskey, function (err, reply) {
		if(err || reply === null ){
			i++;
			if(i < redisClient.length){
				getObject(key, callback, i );
				return;
			} 
			callback(err,null);
			return;
		}
		// Successfully retreived the value of fieldname from hashset;
		callback(null,reply);
	});
}


/* setHashObject will create a hashset using hashsetkey if its not
 * already their, and create a fieldname - value pair in the
 * hashset.
 * fieldname can be a json object or string, the function internally
 * sort & compress it, to use it as a fieldname if its an object.
 * hashsetkey & value must be a string value.
 * callback function will be called with two parameters 
 * which are error message & the sorted-compressed jsonObject/string used
 * as fieldname to store the value
 * */
function setHashObject(hashsetkey, fieldname, value, callback){
	var key = (typeof fieldname == 'object')?JSON.stringify(sort(fieldname)):fieldname;
	var value = (typeof value == 'object')?JSON.stringify(value):value;
	var i = redisClient.length;
	while (i--) {
		redisClient[i].hset([hashsetkey, key, value], function(err, reply){
			if( i === 0 ){
				callback(null,key);
			}
		});
		if( i === 0 )break;
	}
}


/* getHashObject will query redis using hashsetkey & fieldname 
 * and return the value of that field, if it is already exists.
 * fieldname can be a json object or string, the function internally
 * sort & compress it, to use it as a fieldname if its an object.
 * hashsetkey must be a string value.
 * callback function will be called with two parameters 
 * which are error message & returned value 
 * of fieldname, value will be null if hashkey or fieldname inside hashkey
 * does not exist.   
 * */
function getHashObject(hashsetkey, fieldname, callback, i){
	var key = (typeof fieldname == 'object')?JSON.stringify(sort(fieldname)):fieldname;
	i = i || 0;
	redisClient[i].hget(hashsetkey, key, function (err, reply) {
		if(err || reply === null ){
			i++;
			if(i < redisClient.length){
				getHashObject(hashsetkey, fieldname, callback, i);
				return;
			} 
			callback(err,null);
			return;
		}
		// Successfully retrieved the value of fieldname from hashset;
		callback(null,reply);
	});
}

function getHashKey(hashsetkey, callback, i){
	i = i || 0;
	redisClient[i].hkeys(hashsetkey, function (err, reply) {
		if(err || reply === null ){
			i++;
			if(i < redisClient.length){
				getHashKey(hashsetkey, callback, i);
				return;
			}
			callback(err,null);
			return;
		}
		// Successfully retrieved the fieldname from hashset;
		callback(null,reply);
	});
}

/* getAllKeys is used to retreive
 * all the keys currently indexed in the database.
 * callback function will be called with two parameter 
 * which is a error message & an array of all the returned keys
 * */
function getAllKeys(callback,i){
	i = i || 0;
	redisClient[i].keys('*',function(err,reply){
		if(err || reply === null ){
			i++;
			if(i < redisClient.length){
				getHashObject(hashsetkey, fieldname, callback, i );
				return;
			} 
			callback(err,null);
			return;
		}
		callback(null,reply);
	});
}


/* getAllValues is used to retreive the values of all keys
 * present in the Db in the form of an array .
 * It first call getAllKeys to get the list of all the keys & 
 * then retreive all the values with respect to them.
 * callback function will be called with two parameter 
 * which is a error message & an array of all the returned values
 * Please note, any hashkey or listkey will return a null value
 * */
function getAllValues(callback,i){
	var valArr =[];
	i = i || 0;
	getAllKeys(function(err, keyArr){
		if(err){
			callback(err,null);
			return;
		}
		redisClient[i].mget(keyArr,function(err,reply){
			if(err || reply === null ){
				i++;
				if(i < redisClient.length){
					getHashObject(hashsetkey, fieldname, callback, i );
					return;
				} 
				callback(err,null);
				return;
			}
			callback(null,reply);
		})
	});
}


/* delobject will delete the stored object using 
 * the input key.
 * key must be a json object for now, the function internally
 * sort & stringify it, to use it.
 * callback function will be called with two parameter 
 * which is a error message & status message 
 * of deletion operation 
 * */
function delObject(key, callback){
	//var rediskey = (typeof key == 'object')?sortCompress(key):key;
	var rediskey = (typeof key == 'object')?JSON.stringify(sort(key)):key;
	var i = redisClient.length;
	while (i--) {
		redisClient[i].del(rediskey,function(err,reply){
			//if(err){
				//callback(err,null);
				//return;
			//}
			if( i === 0 ){
				callback(null,true);
			}	
		});
		if( i === 0 )break;
	}
	
}


function deleteHashKey(hashsetkey, fieldname, callback, i){
    var key = (typeof fieldname == 'object')?JSON.stringify(sort(fieldname)):fieldname;
	i = i || 0;
	redisClient[i].hdel(hashsetkey, fieldname, function (err, reply) {
		if(err || reply === null ){
			i++;
			if(i < redisClient.length){
				deleteHashKey(hashsetkey, fieldname, callback, i);
				return;
			}
			callback(err,null);
			return;
		}
		// Successfully retrieved the fieldname from hashset;
		callback(null,reply);
	});
}


/* sort is used internally by library
 * functions to sort a jsonobject.
 * it take a json object as a input & returns the sorted 
 * json object
 * */
function sort(jsonobject){
	var sortArr = Object.keys(jsonobject).sort();
	var b ={};
	for(i=0; i< sortArr.length;i++){
		b[sortArr[i]] =  jsonobject[sortArr[i]];
	}
	return b;
}





/* Sample data used for test operations
 * ************************************
var testObj = { SENDING_LINE1: 'DAD NO HOME',    //jsonObject which will be used as a fieldname in the test process
 SENDING_LINE2: 'line2',
 STREET_ADDRESS: '677 800TH STREET',
 CITY: 'SACRAMENTO',
 STATE: 'CA',
 ZIP: '95814-0000',
 CORR_ID: '0018798205',
 COUNTY_ID: '34',
 LANG_CODE: 'EN',
 CORR_NUMBER: 'M44-350A',
 NUM_PAGES: '06',
 CORR_TYPE: 'A',
 CASE_NUMBER: 'E256616',
 PROG_CODE: 'CW',
 CASELOAD_NUMBER: 'CSLD',
 OFFICE_ID: '000001',
 SYSTEM_DATE: '2007-01-05' };

// value used for the fieldname in the test process
var objectUid = "291b347e2f7-9404-4d7b-aabf-32c9524a2b51" ;

//test hashsetkey used for the process
var batchkey = 'abc';

 * Sample call for all exported functions 
 * **************************************
 * getAllKeys(function(err,keys){if(err){console.log(err);return;}console.log(keys);});
 * getAllValues(function(err,vals){if(err){console.log(err);return;}console.log(vals);});
 * getObject(testObj, function(err, value){if(err){console.log(err);return;}console.log(value);});
 * setObject(testObj, objectUid, function(err, value){if(err){console.log(err);return;}console.log(value);});
 * setHashObject(batchkey, testObj, objectUid, function(err, value){if(err){console.log(err);return;} console.log(value);});
 * getHashObject(batchkey, testObj, function(err, value){if(err){console.log(err);return;} console.log(value);});
 * delObject(batchkey, function(err, msg){if(err){console.log(err);return;} console.log(msg);});
 * */
var methods = {};

methods.setObject = setObject ;
methods.getObject = getObject ;
methods.delObject = delObject ;
methods.getAllValues = getAllValues ;
methods.setHashObject = setHashObject ;
methods.getHashObject = getHashObject ;
methods.getHashKey = getHashKey ;
methods.deleteHashKey = deleteHashKey ;

module.exports = function (config) {
   if (Array.isArray(config)) {
	   var i = config.length;
	   while (i--) {
			var obj = config[i];
			var rClient = redis.createClient(obj.port,obj.host);
			if(obj.password) {
				rClient.auth(obj.password);
			}
			redisClient.push(rClient);
	   }
	   return methods;
   }
}
