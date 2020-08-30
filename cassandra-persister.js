var cassandraDb = require('cassandra-driver');
let client;
let SchemaJson = {};
let models = {};

/**
 * 
 * @param {Array<String>} contactPoints 
 * @param {String} datacenter 
 * @param {String} username 
 * @param {String} password 
 * @param {String} keyspace 
 * 
 * Creates a cassandra client for outside modules to run their queries on.
 * 
 */
let CassandraPersister = class CassandraPersister {

	constructor(contactPoints, datacenter, username, password, keyspace) {
		this.client = new cassandraDb.Client(
			{
				contactPoints: contactPoints,
				localDataCenter: datacenter,
				authProvider: new cassandraDb.auth.PlainTextAuthProvider(username, password),
				keyspace: keyspace
			}
		)
		client = this.client
	}

	testConnection() {

		// console.log('HELOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO', this.client.execute('show version') );

		return this.client.execute('SELECT cql_version FROM system.local');

		// return new Promise((resolve, reject) => {
		// 	this.client.ping({
		// 		requestTimeout: 1000
		// 	}).then((res) => {
		// 		resolve(true);
		// 	}).catch((error) => {
		// 		reject(error);
		// 	})
		// });
	}

	getClient() {
		return this.client
	}
}


/** 
 * SchemaBuilder class. 
 * @param {String} tableName 
 * @param {Array<String>} columnData 
 * Exists to expose the CRUD functionalities for the tables in cassandra.
 * Methods : 
 * 			save : save a record in a table.
 * 			fetch : get a record from the table.
 * 			update : update an existing record in the table.
 * 			delete : delete all records from the table as cassandra does not supports optional deletes.
 * 
 * For more info, read complete documentation. 
*/ 

let SchemaBuilder = class SchemaBuilder {

	// columnData = [];
	// tableName = '';

	constructor(tableName, columnData) {
		this.tableName = tableName;
		this.columnData = columnData;
	}

	save(data, callback) {
		var columns = '(';
		var values = '(';
		var query = 'INSERT INTO ' + this.tableName
		for (var i = 0; i < this.columnData.length; i++) {
			if (i < this.columnData.length - 1) {
				columns += this.columnData[i] + ', '
				values += '?, ';
			}
			else {
				columns += this.columnData[i] + ')'
				values += '?)'
			}
		}

		query += columns + ' VALUES' + values;

		return new Promise((resolve, reject) => {
			client.execute(query, data, { prepare: true }, (err, result) => {
				if (err) {
					reject(err);
				} else {
					callback(err, result);
					resolve(result);
				}
			});
		});
	}

	fetch(callback, condition_config) {
		var query = 'SELECT * FROM ' + this.tableName;

		if (condition_config) {
			var append_string = '';
			for (var key in condition_config) {
				if (key === 'where') {
					for (var conjunction in condition_config[key]) {
						if (conjunction === 'and') {
							for (var column in condition_config[key][conjunction]) {
								for (var condition in condition_config[key][conjunction][column]) {
									if (condition === 'gt') {
										if (append_string != '') {
											append_string = append_string + "and " + column + " > '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " > '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'lt') {
										if (append_string != '') {
											append_string = append_string + "and " + column + " < '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " < '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'eq') {
										if (append_string != '') {
											append_string = append_string + "and " + column + " = '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " = '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'gte') {
										if (append_string != '') {
											append_string = append_string + "and " + column + " >= '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " >= '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'lte') {
										if (append_string != '') {
											append_string = append_string + "and " + column + " <= '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " <= '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
								}
							}
						}
						if (conjunction === 'or') {
							for (var column in query_column_data) {
								for (var condition in condition_config[query_conditions[key]][query_appenders[conjunction]][column]) {
									if (condition === 'gt') {
										if (append_string != '') {
											append_string = append_string + "or " + column + " > '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " > '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'lt') {
										if (append_string != '') {
											append_string = append_string + "or " + column + " < '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " < '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'eq') {
										if (append_string != '') {
											append_string = append_string + "or " + column + " = '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " = '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'gte') {
										if (append_string != '') {
											append_string = append_string + "or " + column + " >= '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " >= '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'lte') {
										if (append_string != '') {
											append_string = append_string + "or " + column + " <= '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " <= '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
								}
							}
						}
					}
					append_string = 'where ' + append_string + ' allow filtering';
				}
			}

			query = query + ' ' + append_string;
		}

		return new Promise((resolve, reject) => {
			client.execute(query, (err, result) => {
				if (err) {
					reject(err);
				} else {
					callback(err, result);
					resolve(result);
				}
			});
		});

	}

	delete(callback, condition_config) {
		var query = '';

		if (condition_config) {
			query = 'TO BE WRITTEN'
		}
		else {
			query = 'TRUNCATE TABLE ' + this.tableName;
		}

		return new Promise((resolve, reject) => {
			client.execute(query, (err, result) => {
				if (err) {
					reject(err);
				} else {
					callback(err, result);
					resolve(result);
				}
			});
		});
	}

	update(columnData, data, callback, condition_config) {
		var query = 'UPDATE ' + this.tableName + ' set '
		for (var i = 0; i < columnData.length; i++) {
			if (i < columnData.length - 1) {
				if (Array.isArray(data[i])) {
					data[i] = JSON.stringify(data[i]);
					data[i] = data[i].replace(/"/g, "'");
					query += columnData[i] + "= " + data[i] + ","
				}
				else {
					query += columnData[i] + "= '" + data[i] + "',"
				}
			}
			else {
				if (Array.isArray(data[i])) {
					data[i] = JSON.stringify(data[i]);
					data[i] = data[i].replace(/"/g, "'");
					query += columnData[i] + "= " + data[i]
				}
				else {
					query += columnData[i] + "= '" + data[i] + "'"
				}
			}
		}
		if (condition_config) {
			var append_string = '';
			for (var key in condition_config) {
				if (key === 'where') {
					for (var conjunction in condition_config[key]) {
						if (conjunction === 'and') {
							for (var column in condition_config[key][conjunction]) {
								for (var condition in condition_config[key][conjunction][column]) {
									if (condition === 'gt') {
										if (append_string != '') {
											append_string = append_string + "and " + column + " > '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " > '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'lt') {
										if (append_string != '') {
											append_string = append_string + "and " + column + " < '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " < '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'eq') {
										if (append_string != '') {
											append_string = append_string + "and " + column + " = '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " = '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'gte') {
										if (append_string != '') {
											append_string = append_string + "and " + column + " >= '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " >= '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'lte') {
										if (append_string != '') {
											append_string = append_string + "and " + column + " <= '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " <= '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
								}
							}
						}
						if (conjunction === 'or') {
							for (var column in query_column_data) {
								for (var condition in condition_config[query_conditions[key]][query_appenders[conjunction]][column]) {
									if (condition === 'gt') {
										if (append_string != '') {
											append_string = append_string + "or " + column + " > '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " > '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'lt') {
										if (append_string != '') {
											append_string = append_string + "or " + column + " < '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " < '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'eq') {
										if (append_string != '') {
											append_string = append_string + "or " + column + " = '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " = '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'gte') {
										if (append_string != '') {
											append_string = append_string + "or " + column + " >= '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " >= '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
									else if (condition === 'lte') {
										if (append_string != '') {
											append_string = append_string + "or " + column + " <= '" + condition_config[key][conjunction][column][condition] + "'";
										}
										else {
											append_string = column + " <= '" + condition_config[key][conjunction][column][condition] + "'";
										}
									}
								}
							}
						}
					}
					append_string = 'where ' + append_string;
				}
			}

			query = query + ' ' + append_string;
		}
		return new Promise((resolve, reject) => {
			client.execute(query, (err, result) => {
				if (err) {
					reject(err);
				} else {
					callback(err, result);
					resolve(result);
				}
			});
		});
	}

}

/** 
 * Model class to initialize models for modules. creates the table for models. 
 * @param {String} tableName 
 * @param {JSON} tableData 
 * Takes 2 arguments : 
 * 	tableName : name of the table to be defined
 * 	tableData : a json object in following format : 
 * 				{
 * 					<column-name1> : {
 * 										type : <column-type>
 * 									},
 * 					<column-name2> : {
 * 										type : <column-type>
 * 									}
 * 				}
*/
let Model = class Model {

	constructor(tableName, tableData) {
		this.tableName = tableName;
		this.tableData = tableData;
	}

	// CREATES a table with given tablename and The primary key is always a column named id. It should be filled with a unique id.
	// uuid is recommended for if fields to avoid conflicts.
	createTable() {
		// Creates the table in cassandra DB

		var tableQuery = "CREATE TABLE IF NOT EXISTS " + this.tableName + " ( ";

		tableQuery += "id text PRIMARY KEY, ";

		var keys = Object.keys(this.tableData)
		for (var i = 0; i < keys.length; i++) {
			var columnName = keys[i];
			var columnType = this.tableData[columnName].type;

			if (i < keys.length - 1) {
				tableQuery += columnName + " " + columnType + ", "
			}
			else {
				tableQuery += columnName + " " + columnType + " );"
			}
		}

		// console.log(tableQuery);
		client.execute(tableQuery, (err,res) => {
			if(err) {
				throw Error("Error in creating table for model" + this.tableName + "\n" + err);
			}
		})

	}

}

/**
 * Exposes all the necessary functions to the files that require cassandra-persister.
 */
let Singleton = (() => {
	let instance;

	let createSchema = (tableName, columnData) => {
		if (SchemaJson[tableName]) {
			throw Error('Schema already defined')
		}
		else {
			SchemaJson[tableName] = new SchemaBuilder(tableName, columnData);
		}
	}

	let getSchema = (tableName) => {
		if (SchemaJson[tableName]) {
			return SchemaJson[tableName]
		}
		else {
			throw Error('Schema for the ' + tableName + ' does not exist!')
		}
	}

	let createInstance = (contactPoints, datacenter, username, password, keyspace) => {
		if (!instance) {
			instance = new CassandraPersister(contactPoints, datacenter, username, password, keyspace);
		}
		return instance;
	}

	let testConnection = () => {
		return instance.testConnection();
	}

	let saveModel = (modelName, modelJson) => {

		if (models[modelName]) {
			throw Error('Model already defined');
		}

		else {
			models[modelName] = new Model(modelName, modelJson);
			models[modelName].createTable();
			columnData = []
			console.log(modelJson);
			keys = Object.keys(modelJson);
			for (var key in keys) {
				columnData.push(keys[key])
			}
			createSchema(modelName, columnData);
		}
	}

	let getModel = (modelName) => {
		if (models[modelName]) {
			getSchema(modelName)
		}
		else {
			throw Error('Model for ' + modelName + ' doesnot exist!')
		}
	}

	return {
		createInstance: createInstance,
		getInstance: createInstance,
		// createSchema: createSchema,
		testConnection: testConnection,
		saveModel: saveModel,
		getModel: getModel
		// getSchema: getSchema
	}
})();



module.exports = Singleton;
