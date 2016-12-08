var Dyno = require('dyno');
var dynamodbTest = require('dynamodb-test');
var tape = require('tape');
var utils = require('cardboard/lib/utils')();

var metadataTableSpec = require('../lib/metadata-table.json');
var metadataTable = dynamodbTest(tape, 'cardboard-metadata', metadataTableSpec);

var CardboardMetadata = require('..');

var metadataConfig = {
    metadataTable: metadataTable.tableName,
    endpoint: 'http://localhost:4567',
    region: 'test'
};

metadataTable.start();

var states = require('./data/states.json').features.map(function(state) {
    state.id = state.properties.name.toLowerCase().replace(/ /g, '-') + '-state';
    return utils.toDatabaseRecord(state, 'default');
});

var countries = require('./data/countries.json').features.map(function(feature) {
    feature.id = feature.properties.name.toLowerCase().replace(/ /g, '-') + '-country';
    return utils.toDatabaseRecord(feature, 'default');
});

metadataTable.test('check adding on an empty db works', function(assert) {

});

metadataTable.close();

function toEvent(action, records) {
    return records.map(function(mainRecord) {
        var serialized = JSON.parse(Dyno.serialize(mainRecord));
        var record = { eventName: action };
        record.dynamodb = {};
        record.dynamodb.OldImage = action !== 'INSERT' ? serialized : undefined;
        record.dynamodb.NewImage = action !== 'REMOVE' ? serialized : undefined;
        return record;
    });
}
