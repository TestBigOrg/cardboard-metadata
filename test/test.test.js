var extent = require('@turf/bbox');
var Dyno = require('dyno');
var dynamodbTest = require('dynamodb-test');
var tape = require('tape');
var utils = require('cardboard/lib/utils');

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

metadataTable.test('[stream handler] check adding on an empty db works', function(assert) {
    var bounds = extent(require('./data/states.json')).map(function(v) {
        return parseFloat(v.toFixed(6));
    });
    var cardboardMetadata = CardboardMetadata(metadataConfig);
    var events = toEvent('INSERT', states);
    var start = +new Date();
    cardboardMetadata.streamHandler(events, function(err) {
        if (err) return assert.end(err, 'stream worked');
        cardboardMetadata.get('default', function(err, metadata) {
            if (err) return assert.end(err);
            assert.equal(metadata.count, states.length, 'has right count');
            assert.equal(metadata.editcount, states.length, 'has right num editcount');
            assert.equal(metadata.size, states.reduce(function(m, s) { return m + s.size; }, 0), 'has right size');
            assert.equal(metadata.west, bounds[0], 'has right west');
            assert.equal(metadata.south, bounds[1], 'has right south');
            assert.equal(metadata.east, bounds[2], 'has right east');
            assert.equal(metadata.north, bounds[3], 'has right north');
            assert.equal(new Date(metadata.updated) > start, true, 'is marked as updated');
            assert.end();
        });
    });
});

var seed = {key: 'default', count: countries.length+10, size: 10000000, west:0, east:0, south:0, north:0, editcount:0 };

metadataTable.test('[stream handler] check deleteing works', [seed], function(assert) {
    var cardboardMetadata = CardboardMetadata(metadataConfig);
    var events = toEvent('REMOVE', countries);
    var start = +new Date();
    cardboardMetadata.streamHandler(events, function(err) {
        if (err) return assert.end(err, 'stream worked');
        cardboardMetadata.get('default', function(err, metadata) {
            if (err) return assert.end(err);
            assert.equal(metadata.count, 10, 'has right count');
            assert.equal(metadata.editcount, countries.length, 'has right num editcount');
            assert.equal(metadata.size, seed.size - countries.reduce(function(m, s) { return m + s.size; }, 0), 'has right size');
            assert.equal(metadata.west, 0, 'no change on west');
            assert.equal(metadata.south, 0, 'no change on south');
            assert.equal(metadata.east, 0, 'no change on east');
            assert.equal(metadata.north, 0, 'no change on north');
            assert.equal(new Date(metadata.updated) > start, true, 'is marked as updated');
            assert.end();
        });
    });
});

metadataTable.test('[get] get an non-existing metadata doc', [seed], function(assert) {
    var cardboardMetadata = CardboardMetadata(metadataConfig);
    cardboardMetadata.get('nope-default', function(err, metadata) {
        if (err) return assert.end(err);
        assert.equal(Object.keys(metadata).length, 0, 'empty object');
        assert.end();
    });
});

metadataTable.test('[get] get an existing metadata doc', [seed], function(assert) {
    var cardboardMetadata = CardboardMetadata(metadataConfig);
    cardboardMetadata.get('default', function(err, metadata) {
        if (err) return assert.end(err);
        Object.keys(seed).forEach(function(key) {
            if (key === 'dataset' || key === 'key') return;
            assert.equal(metadata[key], seed[key], 'has '+key);     
        });
        assert.end();
    });
});

metadataTable.close();

function toEvent(action, records) {
    return {
        Records: records.map(function(mainRecord) {
            var serialized = JSON.parse(Dyno.serialize(mainRecord));
            var record = { eventName: action };
            record.dynamodb = {};
            record.dynamodb.OldImage = action !== 'INSERT' ? serialized : undefined;
            record.dynamodb.NewImage = action !== 'REMOVE' ? serialized : undefined;
            return record;
        })
    };
}
