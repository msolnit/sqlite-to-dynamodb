#!/usr/bin/env node

var argv = require('optimist')
	.usage("Usage: $0 --region [DynamoDB region] --access-key [AWS access key] --secret-key [AWS secret key] --dynamo-table [DynamoDB table name] --file [SQLite file] --sqlite-table [SQLite table name]")
	.demand(["region", "access-key", "secret-key", "dynamo-table", "file", "sqlite-table"])
	.argv;

var dynamoRegion = argv["region"];
var dynamoAccessKey = argv["access-key"];
var dynamoSecretKey = argv["secret-key"];
var dynamoTableName = argv["dynamo-table"];

var sqliteFile = argv["file"];
var sqliteTableName = argv["sqlite-table"];

// TODO: Support ~/.aws/credentials file (user passes in a profile, or even that is optional)
// TODO: Support using a single --table parameter to specify both SQLite and DynamoDB tables

// Both of these TODOs put together would let you call with simply --file and --table

var AWS = require('aws-sdk');
AWS.config.update({
	region: dynamoRegion,
	accessKeyId: dynamoAccessKey,
	secretAccessKey: dynamoSecretKey });

var dynamoDB = new AWS.DynamoDB();

var fs = require("fs");

if (!fs.existsSync(sqliteFile)) {
	throw new Error(sqliteFile + " not found.");
}

fs.openSync(sqliteFile, "r");

var sqlite3 = require("sqlite3").verbose();
var db = new sqlite3.Database(sqliteFile);
var Batch = require("./batch.js");

try {
	// Per http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#batchWriteItem-property
	// No batch can have greater than 25 requests.
	var MAX_BATCH_SIZE = 25;
	var batch = new Batch(dynamoTableName);

	db.serialize(function() {
		db.each("SELECT * FROM " + sqliteTableName, function(err, row) {
			if (err) {
				throw err;
			};

			batch.addRow(row);

			if (batch.getLength() == MAX_BATCH_SIZE) {
				sendBatch(batch);
				batch = new Batch(dynamoTableName);
			}
		}, function(error, rowCount) {
			// Invoked when we've finished processing every row.

			// If there is any un-sent batch content, then send it now.
			if (batch.getLength() > 0) {
				sendBatch(batch);
			}
		});
	});
}
finally {
	db.close();
}

function sendBatch(batch) {
	console.log("Sending batch of " + batch.getLength() + " record(s).");

	var params = batch.getBatchWriteItemParams();

	// console.log(JSON.stringify(params));

	dynamoDB.batchWriteItem(params, function(err, data) {
		if (err) {
			throw err;
		}
		else {
			console.log(data);
		}
	});
}
