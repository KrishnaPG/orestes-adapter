var Juttle = require('juttle/lib/runtime').Juttle;
var utils = require('./orestes-adapter-utils');
var Orestes = require('orestes/lib/orestes');

var WRITE_FLUSH_THRESHOLD;

function init(config) {
    WRITE_FLUSH_THRESHOLD = config.WRITE_FLUSH_THRESHOLD || 100;
}

var Write = Juttle.proc.sink.extend({
    procName: 'orestes_write',
    initialize: function(options) {
        var self = this;
        this.eofs = 0;
        this.space = options.space || 'default';
        this.points = [];
    },
    process: function(points) {
        this.points = this.points.concat(points);
        if (this.points.length > WRITE_FLUSH_THRESHOLD) {
            this.flush();
        }
    },
    flush: function() {
        var self = this;
        var points = this.points;
        this.points = [];
        return utils.startup().then(function() {
            return Orestes.write(points, self.space);
        })
        .catch(function(err) {
            self.trigger('error', err);
        });
    },
    eof: function() {
        this.flush().then(this.done);
    }
});

module.exports = {
    proc: Write,
    init: init
};
