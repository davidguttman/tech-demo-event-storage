var Level    = require('level')
var hq = require('hyperquest')
var es = require('event-stream')

var db = Level('../db')

var rs = db.createReadStream({keys: false})
rs.on('error',function(err) {console.log('db err', err);})

var nSent = 0
var lastSent = 0
setInterval(function() {
  console.log(nSent, nSent - lastSent)
  lastSent = nSent
}, 1000)

var ws = es.map(function(val, cb) {  
  // var i = nSent % 3
  
  if (nSent > 1250236) {
    var req = hq.post('http://localhost:3000', function(err) {
      if (err) console.log('req', err)
    })
    req.end(val)
    cb()
    req.on('error', function(err) {console.log('err', err);})
  }

  nSent += 1
})

ws.on('error', function(err) {
  console.log('ws err', err);
})

rs.pipe(ws)

// process.on('uncaughtException', function(err) {
//   console.log('process err', err);
// })