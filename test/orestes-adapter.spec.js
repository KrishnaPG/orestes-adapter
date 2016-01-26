var _ = require('underscore');
var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);
var retry = require('bluebird-retry');
var expect = require('chai').expect;
var util = require('util');

var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;
var Juttle = require('juttle/lib/runtime').Juttle;
var Orestes = require('../lib');
var adapter_test_utils = require('./orestes-adapter-test-utils');
var orestes_test_utils = require('orestes/test/orestes-test-utils');
var remove = require('orestes/lib/orestes-remover').remove;

var adapter = Orestes({
    port: 9668,
    cassandra: {
        host: '127.0.0.1',
        native_transport_port: 9042
    },
    elasticsearch: {
        host: 'localhost',
        port: 9200
    },
    spaces: {
        default: {
            table_granularity_days: 1
        }
    }
}, Juttle);

Juttle.adapters.register(adapter.name, adapter);

describe('orestes source', function() {
    this.timeout(300000);

    afterEach(function() {
        return remove({space: 'default', keep_days: 0});
    });

    function write_read_check(points) {
        var write_program = util.format('emit -points %s | write orestes', JSON.stringify(points));
        return check_juttle({
            program: write_program
        })
        .then(function() {
            return retry(function() {
                var read_program = 'read orestes -from :10 years ago: -to :now:';
                return check_juttle({
                    program: read_program
                })
                .then(function(result) {
                    expect(result.sinks.table).deep.equal(points);
                });
            });
        });
    }

    it('writes and reads a point to Orestes', function() {
        var one_point = orestes_test_utils.generate_sample_data({count: 1});
        return write_read_check(one_point);
    });

    it('writes and reads multiple series to Orestes', function() {
        var points = orestes_test_utils.generate_sample_data({
            count: 1000,
            tags: {
                host: ['a', 'b', 'c'],
                pop: ['d', 'e', 'f', 'g'],
                bananas: ['one', 'two', 'three', 'four', 'five']
            }
        });

        return write_read_check(points);
    });

    it('reads using a time filter', function() {
        var points = orestes_test_utils.generate_sample_data({
            count: 1000,
            tags: {
                host: ['a', 'b', 'c'],
                pop: ['d', 'e', 'f', 'g'],
                bananas: ['one', 'two', 'three', 'four', 'five']
            }
        });

        var start_index = Math.floor(points.length / 2);
        var end_index = 2 * Math.floor(points.length / 3);

        return write_read_check(points)
            .then(function() {
                var start = points[start_index].time;
                var end = points[end_index].time;
                var read_program = util.format('read orestes -from :%s: -to :%s:', start, end);

                return check_juttle({
                    program: read_program
                });
            })
            .then(function(result) {
                expect(result.sinks.table).deep.equal(points.slice(start_index, end_index));
            });
    });

    it('reads with a tag filter', function() {
        var points = orestes_test_utils.generate_sample_data({
            count: 1000,
            tags: {
                host: ['a', 'b', 'c'],
                pop: ['d', 'e', 'f', 'g'],
                bananas: ['one', 'two', 'three', 'four', 'five']
            }
        });

        return write_read_check(points)
            .then(function() {
                var read_program = 'read orestes -last :hour: host = "a"';
                return check_juttle({
                    program: read_program
                });
            })
            .then(function(result) {
                var expected = points.filter(function(pt) {
                    return pt.host === 'a';
                });

                expect(result.sinks.table).deep.equal(expected);
            });
    });
});
