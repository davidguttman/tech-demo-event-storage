var http = require('http')
var uuid = require('uuid')

var Level    = require('level')
var SubLevel = require('level-sublevel')
var Trigger  = require('level-trigger')

var port = process.env.PORT || 3010

var opts = {encoding: 'json', cacheSize: 256 * 1024 * 1024}
var db = SubLevel(Level('./db-' + port, opts))

var Events = db.sublevel('events')
var Pageviews = db.sublevel('pageviews')

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

Trigger(
  Events, 
  'update-pageview', 
  function (ch) {
    return ch.key
  },
  updatePageview
)

var nUpdates = 0
var lastUpdates = 0

setInterval(function() {
  console.log(port, nUpdates, nUpdates - lastUpdates)
  lastUpdates = nUpdates
}, 1000)

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

var server = http.createServer(function(req, res) {
  var buffer = ''
  req.on('data', function(chunk) { buffer += chunk })
  
  req.on('end', function() {
    res.writeHead(200, {'Content-Type': 'application/json'})
    res.end(JSON.stringify({ok:true}))
    storeEvent(buffer)
    nUpdates += 1
  })
})

function storeEvent (eventString) {
  var event = JSON.parse(eventString)
  var pvid = event.pageviewId
  var key = [pvid, uuid.v4()].join('\xff')
  Events.put(key, event)
}

server.listen(port)

console.log('Storage running on port:', port)