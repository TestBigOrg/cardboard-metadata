var Dyno = require('dyno');

module.exports = function(config) {
    return function(records, callback) {
        var requestItems = records.map(function(record) {
            var change = {};
            change.before = record.dynamodb.OldImage ?
                Dyno.deserialize(JSON.stringify(record.dynamodb.OldImage)) : undefined;
            change.after = record.dynamodb.NewImage ?
                Dyno.deserialize(JSON.stringify(record.dynamodb.NewImage)) : undefined;
            change.action = record.eventName;
            return change;
        }).filter(function(change) {
            // make sure we're dealing with the feature object
            var idx = change.after ? change.after.key : change.before.key;
            return idx.split('!')[1] === 'feature';
        }).map(function(change) {
          // CONVERT TO WRITE ITEMS
        });

        if (requestItems.length === 0) return setTimeout(callback, 0);

        var params = { RequestItems: {} };
        params.RequestItems[config.metadataTable] = requestItems;

        config.dyno.batchWriteAll(params).sendAll(10, function(err, res) {
            if (err) return callback(err);
            if (res.UnprocessedItems.length > 0) return callback(new Error('Not all records were written'));
            callback();
        });
    }
}

