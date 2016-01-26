'use strict';

var Juttle = require('juttle/lib/runtime').Juttle;
var utils = require('./orestes-adapter-utils');
var Orestes = require('orestes/lib/orestes');
var AdapterWrite = require('juttle/lib/runtime/adapter-write');

var WRITE_FLUSH_THRESHOLD;

function init(config) {
    WRITE_FLUSH_THRESHOLD = config.WRITE_FLUSH_THRESHOLD || 100;
}

class WriteOrestes extends AdapterWrite {
    constructor(options, params) {
        super(options, params);
        this.eofs = 0;
        this.space = options.space || 'default';
        this.points = [];
        this.flush_promise = utils.startup();
    }
    write(points) {
        this.points = this.points.concat(points);
        if (this.points.length > WRITE_FLUSH_THRESHOLD) {
            this.flush();
        }
    }
    flush() {
        var points = this.points;
        this.points = [];

        this.flush_promise = this.flush_promise.then(() => {
            return Orestes.write(points, this.space);
        })
        .catch((err) => {
            this.trigger('error', err);
        });

        return this.flush_promise;
    }
    eof() {
        return this.flush().then(this.done);
    }
}

module.exports = {
    proc: WriteOrestes,
    init: init
};
