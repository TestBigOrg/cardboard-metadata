var Dyno = require('dyno');

module.exports = function(config) {

    if (typeof config.metadataTable !== 'string' || config.metadataTable.length === 0) throw new Error('"metadataTable" must be a string');
    if (!config.dyno && !config.region) throw new Error('No region set');
    if (!config.dyno) config.dyno = Dyno({table: config.metadataTable, region: config.region, endpoint: config.endpoint});
 
    var cardboardMetadata = {};

    /**
     * Create DynamoDB tables with Cardboard's schema
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.createTable = function(callback) {
        var tableSchema = require('./lib/metadata-table.json');
        tableSchema.TableName = config.metadataTable;
        config.dyno.createTable(tableSchema, callback);
    };        

    cardboardMetadata.streamHandler = require('./lib/stream-handler')(config);

    return cardboardMetadata;
}

