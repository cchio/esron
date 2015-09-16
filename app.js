var express = require('express');
var app = express();
var server = require('http').Server(app);
var io = require('socket.io')(server);

require('./config.js');

var kafka;
while (!kafka) {
  try {
    var kafka = require('kafka-node'),
      HighLevelConsumer = kafka.HighLevelConsumer,
      client = new kafka.Client(config['zkServer']),
      consumer = new HighLevelConsumer(
          client,
          [
              { topic: config['kafkaTopic'] }
          ],
          {
              groupId: config['kafkaConsumerGroupId']
          }
      );
  } catch (e) {
    console.log(e);
  }
}

if (typeof String.prototype.startsWith != 'function') {
  String.prototype.startsWith = function (str){
    return this.slice(0, str.length) == str;
  };
}

app.use(express.static('public'));
server.listen(3000);

app.get('/', function (req, res) {
  res.sendfile(__dirname + '/index.html');
});

io.on('connection', function (socket) {
  socket.emit('attacks', { status: 'connected' });
  // socket.on('my other event', function (data) {
  //   console.log(data);
  // });
});

consumer.on('message', function (kafkaMsgObj) {
  msgWithHeader = kafkaMsgObj.value;
  if (!msgWithHeader.startsWith("[tel")) return;

  var i = msgWithHeader.indexOf("]");
  if (i < 0) return;

  var cid = msgWithHeader.substring(1, i).split(',')[1];
  if (!(cid in hqMap)) {
    console.log(cid);
    return;
  }
  var hq = hqMap[cid];

  var msg = msgWithHeader.substring(i+1);
  try {
    msgObj = JSON.parse(msg);
  } catch (e) {
    return;
  }

  if (!("tel_attacks" in msgObj) || !msgObj["tel_attacks"]) return;
  if (msgObj["tel_attacks"].length == 0) return;
  if (!("geoip" in msgObj) || !msgObj['geoip']) return;

  var city = msgObj['geoip']['city_name'];
  if (!city || city == "null") return;
  io.emit('attacks', {hq: hq, city: city})
});
