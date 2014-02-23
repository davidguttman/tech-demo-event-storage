var Level    = require('level')
var pj = require('post-json')
var hq = require('hyperquest')
var es = require('event-stream')
var http = require('http')

// http.globalAgent.maxSockets = 1000

var db = Level('../db')

var rs = db.createReadStream({keys: false})

var nSent = 0
var lastSent = 0
// 94633
setInterval(function() {
  console.log(nSent, nSent - lastSent)
  lastSent = nSent
}, 1000)

var ws = es.map(function(val, cb) {  
  var i = nSent % 3
  if (nSent > -94633) {
    post('http://localhost:300'+i, val, cb)
  } else {
    cb()
  }
  nSent += 1
})

rs.pipe(ws)

function post (host, body, cb) {
  var opts = { host: 'localhost', port: 3000, path: '/', method: 'POST', agent: false }
  var req = http.request(opts, function(res) {
    res.setEncoding('utf8')
    res.on('end', cb)
  })
  req.end(body)
}