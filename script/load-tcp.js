var Level    = require('level')
var pj = require('post-json')
var hq = require('hyperquest')
var es = require('event-stream')
var net = require('net')

var db = Level('../db')

var rs = db.createReadStream({keys: false, limit: -1})

var nSent = 0
var lastSent = 0
// 94633
setInterval(function() {
  console.log(nSent, nSent - lastSent)
  lastSent = nSent
}, 1000)

var client = net.connect({port:4000})

var tr = es.through(function(val) {
  nSent += 1
  // console.log('val', val);
  this.queue(val+'\n')
})

rs.pipe(tr).pipe(client)