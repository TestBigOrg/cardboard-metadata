var Dyno = require('dyno');
var makeKey = require('./lib/make-key');
var zoomRange = require('./lib/zoom-range');

module.exports = function(config) {

    if (typeof config.metadataTable !== 'string' || config.metadataTable.length === 0) throw new Error('"metadataTable" must be a string');
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
                count: data.Item.count,     
                size: data.Item.size,     
                edits: data.Item.edits,
                west: data.Item.west,
                south: data.Item.south,
                east: data.Item.east,
                north: data.Item.north,
                updated: data.Item.updated
            };
            var range = zoomRange(info.size, [info.west, info.south, info.east, info.north]);
            info.minzoom = range.min;
            info.maxzoom = range.max;
            callback(null, info);
        });
    }

    cardboardMetadata.streamHandler = require('./lib/stream-handler')(config, cardboardMetadata);

    return cardboardMetadata;
}

