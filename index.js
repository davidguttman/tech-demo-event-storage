var http = require('http')
var uuid = require('uuid')
var es = require('event-stream')

var db = require('./db')
var Events = db.Events
var Pageviews = db.Pageviews

var port = process.env.PORT || 3010

var server = http.createServer(function(req, res) {
  if (req.url === '/event') return addEvent(req, res)
  if (req.url === '/pageviews') return getPageviews(req, res)
})

server.listen(port)

console.log('Storage running on port:', port)

function getPageviews (req, res) {
  res.writeHead(200, {'Content-Type': 'application/json'})
  var pvStream = Pageviews.createReadStream()
  var last = null
  var tr = es.through(function(data) {
    var key_prop = data.key.split('\xff')
    var pvid = key_prop[0]
    var property = key_prop[1]
    
    last = last || {pageviewId: pvid}
    
    if (last.pageviewId !== pvid) {
      this.queue(JSON.stringify(last) + '\n')
      last = {pageviewId: pvid}
    }

    last[property] = data.value
  })
  pvStream.pipe(tr).pipe(res)
}

function addEvent (req, res) {
  var buffer = ''
  req.on('data', function(chunk) { buffer += chunk })
  
  req.on('end', function() {
    res.writeHead(200, {'Content-Type': 'application/json'})
    res.end(JSON.stringify({ok:true}))
    storeEvent(buffer)
  })
}

function storeEvent (eventString) {
  var event = JSON.parse(eventString)
  var pvid = event.pageviewId
  var key = [pvid, uuid.v4()].join('\xff')
  Events.put(key, event)
}