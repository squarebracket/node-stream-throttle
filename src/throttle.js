var inherits = require('util').inherits;
var Transform = require('stream').Transform;
var TokenBucket = require('limiter').TokenBucket;

/*
 * Throttle is a throttled stream implementing the stream.Transform interface.
 * Options:
 *    rate (mandatory): the throttling rate in bytes per second.
 *    chunksize (optional): the maximum chunk size into which larger writes are decomposed.
 * Any other options are passed to stream.Transform.
 */
function Throttle(opts, group) {
    if (group === undefined)
        group = new ThrottleGroup(opts);
    this.group = group;
    this.bucket = group.bucket;
    this.chunksize = group.chunksize;
    //this.bucket = new TokenBucket(opts.rate, opts.rate, 'second', null);
    //this.chunksize = opts.rate / 10;
    Transform.call(this, opts);
    this.on('end', () => {
      let asdf = this.group.throttles.indexOf(this);
      if (asdf > -1) {
        delete this.group.throttles[asdf];
      }
    });
}
inherits(Throttle, Transform);

Throttle.prototype._transform = function(chunk, encoding, done) {
    process(this, chunk, 0, done);
};

Throttle.prototype.setRate = function(rate) {
    //this._pendingRate = rate;
    this.group.setRate(rate);
};

function process(self, chunk, pos, done) {
    var slice = chunk.slice(pos, pos + 1024);
    //var slice = chunk.slice(pos, pos + self.chunksize);
    if (!slice.length) {
        // chunk fully consumed
        done();
        return;
    }
    if (self.group.changingRates) {
      console.log('waiting until rate change is over');
      setTimeout(() => {
        process(self, chunk, pos, done);
      });
      return;
    }
    //const chunksize = self.chunksize;
    const chunksize = 1024;
    //console.log(slice.length, chunksize, self.chunksize);
    var worked = self.bucket.removeTokens(slice.length, function(err) {
        if (err) {
            console.error('error', err);
            done(err);
            return;
        }
        self.push(slice);
        var end = pos + chunksize;
        //if (self._pendingRate) {
          //console.log(`changing rate from ${self.bucket.tokensPerInterval} to ${self._pendingRate}`);
          //self.bucket.tokensPerInterval = self._pendingRate;
          //self.chunksize = self._pendingRate / 10;
          //self._pendingRate = null;
        //}
        //if (self.group._pendingRate) {
          //console.log(`changing rate from ${self.bucket.tokensPerInterval} to ${self.group._pendingRate}`);
          //self.bucket.tokensPerInterval = self.group._pendingRate;
          //self.bucket.bucketSize = self.group._pendingRate;
          //self.chunksize = self.group._pendingRate / 10;
          //self.group._pendingRate = null;
        //}
        process(self, chunk, end, done);
    });
}

/*
 * ThrottleGroup throttles an aggregate of streams.
 * Options are the same as for Throttle.
 */
function ThrottleGroup(opts) {
    if (!(this instanceof ThrottleGroup))
        return new ThrottleGroup(opts);

    opts = opts || {};
    if (opts.rate === undefined)
        throw new Error('throttle rate is a required argument');
    if (typeof opts.rate !== 'number' || opts.rate <= 0)
        throw new Error('throttle rate must be a positive number');
    if (opts.chunksize !== undefined && (typeof opts.chunksize !== 'number' || opts.chunksize <= 0)) {
        throw new Error('throttle chunk size must be a positive number');
    }

    this.throttles = [];
    this.rate = opts.rate;
    this.originalChunksize = opts.chunksize;
    this.chunksize = this.originalChunksize || this.rate/20;
    this.bucket = new TokenBucket(this.rate / 20, this.rate, 1000, null);
}

/*
 * Create a new stream in the throttled group and returns it.
 * Any supplied options are passed to the Throttle constructor.
 */
ThrottleGroup.prototype.throttle = function(opts) {
    const throttle = new Throttle(opts, this);
    this.throttles.push(throttle);
    return throttle;
};

ThrottleGroup.prototype.setRate = function(rate) {
    this.changingRates = true;
    this.bucket.tokensPerInterval = rate;
    this.bucket.bucketSize = rate / 20;
    this.chunksize = this.originalChunksize || (rate / 20);
    this.throttles.forEach((throttle) => {
      throttle.chunksize = this.originalChunksize || (rate / 20);
    });
    this.changingRates = false;
};

module.exports = {
    Throttle: Throttle,
    ThrottleGroup: ThrottleGroup
};
