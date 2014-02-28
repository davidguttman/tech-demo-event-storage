var Level    = require('level')
var SubLevel = require('level-sublevel')
var Trigger  = require('level-trigger')

var port = process.env.PORT || 3010

var opts = {encoding: 'json', cacheSize: 256 * 1024 * 1024}
var db = SubLevel(Level('./db-' + port, opts))

var Events = module.exports.Events = db.sublevel('events')
var Pageviews = module.exports.Pageviews = db.sublevel('pageviews')

Pageviews.updateIf = function(key, val, test, cb) {
  var self = this
  cb = cb || function() {}

  this.get(key, function(err, existing) {
    if (test(val, existing)) {
      self.put(key, val, function(err) {
        if (err) { return cb(err) }
        cb(null, true)
      })
    } else {
      cb(null, false)
    }
  })
}

Trigger(Events, 'update-pageview', getKey, updatePageview)
Trigger(Pageviews, 'saved-pageview', getKey, onSavePageview)

function onSavePageview (pvid, cb) {
  cb()
  Pageviews.get(pvid, function(err, pv) {
    console.log(pvid, pv);
  })
}

function getKey (change) { return change.key }

function updatePageview (eventKey, cb) {
  Events.get(eventKey, function(err, event) {
    if (event.type === 'load') setMax(event, 'createdAt', 'ts')
    if (event.type === 'refAdvance') {
      var advanceKey = [event.pageviewId, 'advanced'].join('\xff')
      Pageviews.put(advanceKey, true)
    }
    
    setMax(event, 'tsDelta', 'duration')
    cb()
  })
}

function setMax (event, property, subkey) {
  var val = event[property]
  if (!val) return
  
  var key = [event.pageviewId, subkey].join('\xff')
  Pageviews.updateIf(key, val, function (v, existing) {
    if (!existing) return true
    return v > existing
  })
}
