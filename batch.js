function Batch(tableName) {
	this.tableName = tableName;
	this.putRequests = [];
}

Batch.prototype.addRow = function(row) {
	var item = {};

	for (var column in row) {
		var value = row[column];

		if (value && value.length > 0) {
			item[column] = { "S": value };
		}
	}

	var request = {
		"PutRequest": {
			"Item": item
		}
	};

	this.putRequests.push(request);
}

Batch.prototype.getLength = function() {
	return this.putRequests.length;
}

Batch.prototype.getBatchWriteItemParams = function() {
	var tableRequests = {};
	tableRequests[this.tableName] = this.putRequests;

	var params = {
		"RequestItems": tableRequests
	};

	return params;
}

module.exports = Batch;
