var queue = require('d3-queue').queue;
var Dyno = require('dyno');
var makeKey = require('./lib/make-key');
var zoomRange = require('./lib/zoom-range');

module.exports = function(config) {

    if (!config.dyno && (typeof config.metadataTable !== 'string' || config.metadataTable.length === 0)) throw new Error('"metadataTable" must be a string');
    if (!config.dyno && !config.region) throw new Error('No region set');
    if (!config.dyno) config.dyno = Dyno({table: config.metadataTable, region: config.region, endpoint: config.endpoint});
 
    var cardboardMetadata = {};

    /**
     * Create DynamoDB tables with Cardboard's schema
     * @param {function} callback - the callback function to handle the response
     */
    cardboardMetadata.createTable = function(callback) {
        var tableSchema = require('./lib/metadata-table.json');
        tableSchema.TableName = config.metadataTable;
        config.dyno.createTable(tableSchema, callback);
    };        

    cardboardMetadata.get = function(dataset, callback) {
        config.dyno.getItem({Key: makeKey(dataset) }, function(err, data) {
            if (err) return callback(err);
            if (data.Item === undefined) return callback(null, {});
            var info = {
                count: data.Item.count || 0,
                size: data.Item.size || 0,
                editcount: data.Item.editcount || 0,
                west: data.Item.west || 0,
                south: data.Item.south || 0,
                east: data.Item.east || 0,
                north: data.Item.north || 0,
                updated: data.Item.updated
            };
            var range = zoomRange(info.size, [info.west, info.south, info.east, info.north]);
            info.minzoom = range.min;
            info.maxzoom = range.max;
            callback(null, info);
        });
    };

    cardboardMetadata.adjustBounds = function(dataset, bounds, callback) {
        var key = makeKey(dataset);
        var q = queue();
        var labels = ['west', 'south', 'east', 'north'];

        bounds.forEach(function(bound, i) {
            var params = {
                Key: key,
                ExpressionAttributeNames: { '#attr': labels[i], '#u': 'updated' },
                ExpressionAttributeValues: { ':attr': bound, ':u': +new Date() },
                UpdateExpression: 'set #attr = :attr, #u = :u',
                ConditionExpression: 'attribute_not_exists(#attr) OR #attr ' + ( i < 2 ? '>' : '<') + ' :attr'
            };

            q.defer(function(done) {
                config.dyno.updateItem(params, function(err) {
                    if (err && err.message === 'The conditional request failed') return done();
                    if (err) return done(err);
                    done();
                });
            });
        });

        q.awaitAll(callback);
    };

    cardboardMetadata.adjustProperties = function(dataset, properties, callback) {
        var key = makeKey(dataset);
        var params = {
            Key: key,
            ExpressionAttributeNames: { '#u': 'updated', '#e': 'editcount' },
            ExpressionAttributeValues: { ':u': +new Date(), ':e': properties.edits || 1 },
            UpdateExpression: 'set #u = :u add #e :e'
        };

        Object.keys(properties).forEach(function(key, i) {
            if (key === 'edits') return;
            params.ExpressionAttributeNames['#' + i] = key;
            params.ExpressionAttributeValues[':' + i] = properties[key];
            params.UpdateExpression += ', #' + i + ' ' + ':' + i;
        });
        
        config.dyno.updateItem(params, callback);
    }

    cardboardMetadata.streamHandler = require('./lib/stream-handler')(config, cardboardMetadata);

    return cardboardMetadata;
}

