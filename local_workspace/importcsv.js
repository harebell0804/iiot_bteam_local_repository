var fs = require('fs');
var mysql=require("mysql");
var uuid = require('node-uuid');
var sha1 = require('sha1');
var dateFormat = require('dateformat');

// csvtojson modules 宣告
var Converter = require("csvtojson").Converter;
var newConverter = new Converter({});

var db_config = { 
	host     : '52.68.237.64',
	user     : 'kayl',
	password : 'kkforever',
	database : 'icameraDB',
	port: '3306',
	// 無可用連線時是否等待pool連線釋放(預設為true) 
	waitForConnections : true, 
	// 連線池可建立的總連線數上限(預設最多為10個連線數) 
	connectionLimit : 10 
}; 


//var connection_icameraDB = mysql.createConnection(db_config);

var pool = mysql.createPool(db_config);
console.log('Read module OK');


var csvDataArray = [];
var csvDataErrorMessageArray = [];
var csvDataSuccessMessageArray = [];
var results = [];

// read from file 
// 利用 fs 讀取 csv 檔案並交給 csvtojson 解析
fs.createReadStream( 'mapping.csv' ).pipe( newConverter );
console.log('Read File OK');
main();

//main function
function final() {
	// 儲存成 JSON
	// fs.writeFile 使用 File System 的 writeFile 方法做儲存
	// 傳入三個參數（ 存檔名, 資料, 格式 ）
	//fs.writeFile( saveFileName, JSON.stringify( jsonArray ), 'utf8');

	console.log('CSV to JSON done !!');
	console.log('-------Error Message List:-------------');
	console.log(csvDataErrorMessageArray);
	console.log('---------------------------------------');
	console.log('-------Success Message List:-------------');
	console.log(csvDataSuccessMessageArray);
	console.log('---------------------------------------');

	//把資料寫入檔案
	fs.writeFile(__dirname+'/Error.txt',csvDataErrorMessageArray,function(error){ 
	    if(error){ //如果有錯誤，把訊息顯示並離開程式
	        console.log('檔案寫入錯誤');
	    }
	});

    fs.writeFile(__dirname+'/Success.txt',csvDataSuccessMessageArray,function(error){ 
	    if(error){ //如果有錯誤，把訊息顯示並離開程式
	        console.log('檔案寫入錯誤');
	    }
	});
}

function main() {

	var recordIndex = 0;
	// end_parsed will be emitted once parsing finished 
	// 當 csvtojson 結束解析的時候
	newConverter.on("end_parsed", function (jsonArray) {
	 
	    // 對 jsonArray 做處理
	    // here is your result jsonarray
	    console.log(jsonArray);
	    console.log('==================================');

	    for (var i=0; i<jsonArray.length;i++) {

	    	jsonData = jsonArray[i];

		    process(jsonData, i, function(result){
		    	results.push(result);
		    
			    if(results.length == jsonArray.length) {
			      final();
			    }
		    });
		};

		// 可開啟這行在 Command Line 觀看 data 內容
		//console.log(jsonArray); //here is your result jsonarray 
	});
};

function process(jsonData, recordIndex, callback){
		
	var cvsData = {};
	var csvDataErrorMessage = '';

	//1.check if empty columns
	for (var attribute in jsonData) {
		if(jsonData[attribute] === ''){
			csvDataErrorMessage = csvDataErrorMessage + "Record " + (recordIndex+1) + " has Empty column: " + attribute + '\r\n';
		}
		else{
			if(attribute!='latitude' && attribute!='longitude' && attribute!='address'){
				cvsData[attribute] = jsonData[attribute];
				//console.log("cvsData[" + attribute + ']: ' + cvsData[attribute]);
			}
		}
	}

	//2. no empty columns, check ac is actived
	if(csvDataErrorMessage!=''){
		csvDataErrorMessageArray.push(csvDataErrorMessage);
		callback(csvDataErrorMessage);
	}
	else{
		
		checkRegistryData(cvsData, function (err, checkRegistryDataResultObject){
			if (err) {
				console.log('ERROR!!! checkRegistryDataResult: ' +  JSON.stringify(checkRegistryDataResultObject));
			}
			else{
				
				if(checkRegistryDataResultObject['result'] === 'success'){

					console.log("checkRegistryDataResultObject success!");
					
					var timeStamp = Math.floor(Date.now());
					console.log("timeStamp: " + timeStamp);

					cvsData['cuid'] = uuid.v1(); //Generate a v1 (time-based) id
					cvsData['ckey'] = sha1(cvsData['cuid']+cvsData['ac']+timeStamp);
					//cvsData['boundDate'] = 'now()';
					cvsData['boundDate'] = dateFormat(new Date(), "yyyy-mm-dd HH:MM:ss");
					cvsData['type'] = '1';
					cvsData['desc'] = getDesc(jsonData);
					cvsData['mac'] = checkRegistryDataResultObject['mac'];

					//console.log("cvsData: " + JSON.stringify(cvsData));

					csvDataArray.push(cvsData);

					console.log('-------csv Data:-------------');
					console.log(csvDataArray);
					console.log('------------------------------');
					
					
					insertData(csvDataArray, function (err, content){
						if(err){
							csvDataErrorMessageArray.push(content);
							callback(content);
						}
						else{
							csvDataSuccessMessageArray.push("Record " + (recordIndex+1) + " " + content +"\r\n");
							callback(content);
						}
					});
					
					
				}else{
					csvDataErrorMessage = "Record " + (recordIndex+1) + ' Check Registry Data Fail : ' + checkRegistryDataResultObject['result'] +"\r\n";
					
					console.log('-------csv Data Error:--------');
					console.log(csvDataErrorMessage);
					console.log('------------------------------');
					
					csvDataErrorMessageArray.push(csvDataErrorMessage);
					callback(csvDataErrorMessage);
				}

			}
		});
	}
}

/*
example :
{"location":{"latitude":25.0339226,"longitude":121.56463680000002},"address":"110台灣台北市信義區信義路五段7號"}
*/
function getDesc(jsonData){
	var dataObject = {};
	var locationObject = {};
	
	locationObject['latitude'] = jsonData['latitude'];
	locationObject['longitude'] = jsonData['longitude'];
	
	dataObject['location'] = locationObject;
	dataObject['address'] = jsonData['address'];
	return JSON.stringify(dataObject);
}

/*insert data to icamra db*/
function checkRegistryData(cvsData, mainCallback){

	var checkResultObject = {};
	var checkResult = 'success';
	var macResult = '' ;

	// 取得連線池的連線 
	pool.getConnection(function(err, connection) { 

		if (err)  
		{ 
			// 取得可用連線出錯 
			console.log('----[checkRegistryData] when connecting to db:', err);
			connection.release(); 
			checkResult = '----[checkRegistryData] ERROR when connecting to db:' + err;

			checkResultObject['result'] = checkResult;
			checkResultObject['mac'] = macResult;
			console.log('checkResultObject:' + JSON.stringify(checkResultObject));
			mainCallback(err, checkResultObject);
		} 
		else 
		{ 
			//console.log('----[checkRegistryData] first connecting to db');
			// 成功取得可用連線  
			// Query Data from DB
			var sql = "SELECT * FROM registry "
			sql += " WHERE ac = " + mysql.escape(cvsData['ac']) ;

			//console.log('SQL: ' + sql);
			
			queryMac(connection, sql, function(err, content) {
		        if (err) {
			        //console.log(err);
			        connection.release(); 
			        checkResult = '----[checkRegistryData, queryMac] SQL: ' + sql + '\n' + 'ERROR: ' + err;

			        checkResultObject['result'] = checkResult;
					checkResultObject['mac'] = macResult;
					//console.log('checkResultObject:' + JSON.stringify(checkResultObject));
			        mainCallback(err, null);
		        } else {
			        macResult = content; 
			        checkResultObject['result'] = checkResult;
					checkResultObject['mac'] = macResult;
					//sconsole.log('checkResultObject:' + JSON.stringify(checkResultObject));
					mainCallback(null, checkResultObject);
		        }
		    });	
		} 
	}); 

	function queryMac(connection, sql, callback) {

		connection.query( sql, function(err, rows) {
			
			if (err)  
			{ 
				// 取得可用連線出錯 
				console.log('----[checkRegistryData, queryMac] ERROR when query data to db:', err);
				connection.release(); 
				checkResult = '----[checkRegistryData, queryMac] ERROR when query to db:' + err;
				callback(err, null);
			} 
			else 
			{ 
				// success
				console.log('[checkRegistryData, queryMac] success when query data from db');
				console.log(rows);

				if(rows.length==1){
					var object = rows[0];
					if(object['isActivated'] == '0'){
						console.log('isActivated: ' + object['isActivated']);
						if (object['mac']== null || object['mac']==='') {
							checkResult = 'Find ac in registry table, but no mac information';
						}
						else{
							macResult = object['mac']
						}
					}
					else{
						checkResult = 'Find ac in registry table, but ac( '+ cvsData['ac'] + ' ) is active!'
					}
				}
				else if(rows.length==0){
					checkResult = 'Can not Find ac( '+ cvsData['ac'] + ' )  in registry table!'
				}
				else if(rows.length>1){
					checkResult = 'Find multiple ac( '+ cvsData['ac'] + ' )  in registry table!'
				}
				else{
					checkResult = 'Find ac( '+ cvsData['ac'] + ' ) Error, please detail check it!';
				}

				// 釋放連線 
				connection.release(); 
			}

			//console.log('mac information: ' + macResult);
			callback(null,macResult);
		}); 
	}
}

/*insert data to icamra db*/
function insertData(jsonArray, mainCallback){

	var checkResult = 'success';
	//connection.connect();
	// 取得連線池的連線 
	pool.getConnection(function(err, connection) { 
	
		if (err)  
		{ 
			// 取得可用連線出錯 
			console.log('ERROR when connecting to db:', err);
			connection.release(); 
		} 
		else 
		{ 
			//console.log('first connecting to db');
			// 成功取得可用連線  
			// 處理 json data
			jsonArray.forEach(function(data) {

				var cols = [];
				var vals = [];

				for (var attribute in data) {
					cols.push(attribute);
					vals.push(data[attribute]);
				}

				// 新增 Data to DB
				var sql = "INSERT INTO camera "
				sql += " (" + mysql.escapeId(cols) + /*"," + mysql.escapeId('boundDate')+ */ ") VALUES ("
					+ mysql.escape(vals) + /*", now() " + */ ") ON DUPLICATE KEY UPDATE "
					+ mysql.escape(data) ;

				//console.log('SQL: ' + sql);

				// 更新 Data to DB
				// isActivated: 0, tsActivated: Tue Feb 03 2015 12:04:01 GMT+0800 (台北標準時間),
				var sql_updateRegistry = "UPDATE registry SET "
				sql_updateRegistry += "isActivated = '1' , tsActivated = " + mysql.escape(data['boundDate']) 
					+ " where ac = " + mysql.escape(data['ac'] );
				//console.log('SQL: ' + sql_updateRegistry);

				update(connection, sql, function(err, content) {
			        if (err) {
				        //console.log(err);
				        //connection.release(); 
				        checkResult = '----[insertData, updateCamera] SQL: ' + sql + '\n' + 'ERROR: ' + err;
				        mainCallback(err, checkResult);
			        } else {

			        	console.log('update registry');
				        update(connection, sql_updateRegistry, function(err, content) {
					        if (err) {
						        //console.log(err);
						        //connection.release(); 
						        checkResult = '----[insertData, updateRegistry] SQL: ' + sql_updateRegistry + '\n' + 'ERROR: ' + err;
						        //connection.release(); 
						        mainCallback(err, checkResult);
					        } else {
						        //connection.release(); 
								mainCallback(null, content);
					        }
					    });	
			        }
			    });	

			});

			connection.release(); 
		} 


		function update(connection, sql, callback) {

			connection.query( sql, function(err, rows) {

				console.log(sql);
				if (err)  
				{ 
					// 取得可用連線出錯
					callback(err, null);
				} 
				else 
				{ 
					console.log('success insert data to db');
					callback(null, 'success insert data to db');
				}
			});
		}


	}); 
}