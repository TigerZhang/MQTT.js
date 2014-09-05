/**
 * Module dependencies
 */
var Connection = require('./connection')
  , events = require('events')
  , util = require('util')
  , async = require('async')
  , crypto = require('crypto')
    , druuid = require('druuid')
    , bignum = require('bignum');

/*
 type 类型列表
 0: topics
 1: payload
 2: platform 参数为int 目标用户终端手机的平台类型，如： android, ios, winphone 多个请使用逗号分隔（默认全部推送）
 3: time_to_live 参数为int 从消息推送时起，保存离线的时长。秒为单位。最多支持15天 (默认永久保留)
 4: time_delay 定时发送
 5: location 位置
 6: qos
 7: apn_json，包含以下键:
 expiration: number (maybe NULL)
 priority: number (maybe NULL)
 alert: string or dictionary (maybe NULL)
 dictionary: (暂时不做检查)
 badge: number (maybe NULL)
 sound: string (maybe NULL)
 content-available: number (maybe NULL)
 extra:  dictionary （自定义参数， maybe NULL)
 */
var EXT_CMD_GET_STATUS = 9;
var EXT_CMD_GET_STATUS_ACK = 10;

/* send a recvack to publisher after the first puback from a subscriber */
var EXT_CMD_PUB_RECVACK = 11;

var EXT_CMD_PUB_ACK = 12;

/**
 * Default options
 */
var defaultConnectOptions = {
  keepalive: 60,
  protocolId: 'MQIsdp',
  protocolVersion: 0x13,
  reconnectPeriod: 1000,
  clean: true,
  encoding: 'utf8'
};

var defaultId = function() {
  return 'mqttjs_' + crypto.randomBytes(8).toString('hex');
};

var nop = function(){};

/**
 * MqttClient constructor
 *
 * @param {Stream} stream - stream
 * @param {Object} [options] - connection options
 * (see Connection#connect)
 */
var MqttClient = module.exports =
function MqttClient(streamBuilder, options) {
  var that = this;

  if (!this instanceof MqttClient) {
    return new MqttClient(stream, options);
  }

  this.options = options || {};

  // Defaults
  for(var k in defaultConnectOptions) {
    if ('undefined' === typeof this.options[k]) {
      this.options[k] = defaultConnectOptions[k];
    } else {
      this.options[k] = options[k];
    }
  }

  this.options.clientId = this.options.clientId || defaultId();

  this.streamBuilder = streamBuilder;

  this._setupStream();

  // Ping timer, setup in _setupPingTimer
  this.pingTimer = null;
  // Is the client connected?
  this.connected = false;
  // Packet queue
  this.queue = [];
  // Are we intentionally disconnecting?
  this.disconnecting = false;
  // Reconnect timer
  this.reconnectTimer = null;
  // MessageIDs starting with 1
  if (this.options.protocolVersion == 0x13) {
      this.nextId = druuid.gen();
  } else {
      this.nextId = Math.floor(Math.random() * 65535);
  }


  this.publishTimer = null;
  this.toPublish = 0;

  // Inflight messages
  this.inflight = {
    puback: {},
    pubrec: {},
    pubcomp: {},
    suback: {},
    get: {},
    unsuback: {},
      recvack: {}
  };

  // Incoming messages
  this.incoming = {
    pubrel: {}
  };

  // Mark connected on connect
  this.on('connect', function() {
    this.connected = true;
  });

  // Mark disconnected on stream close
  this.on('close', function() {
    this.connected = false;
  });

  // Setup ping timer
  this.on('connect', this._setupPingTimer);

  // Send queued packets
  this.on('connect', function() {
    var queue = that.queue
      , length = queue.length;

    for (var i = 0; i < length; i += 1) {
      that._sendPacket(
        queue[i].type,
        queue[i].packet,
        queue[i].cb
      );
    }
    that.queue = [];
  });


  // Clear ping timer
  this.on('close', function () {
    if (that.pingTimer !== null) {
      clearInterval(that.pingTimer);
      that.pingTimer = null;
    }
  });

  // Setup reconnect timer on disconnect
  this.on('close', function() {
    that._setupReconnect();
  });

  events.EventEmitter.call(this);
};
util.inherits(MqttClient, events.EventEmitter);

/**
 * setup the event handlers in the inner stream.
 *
 * @api private
 */
MqttClient.prototype._setupStream = function() {
  var that = this;

  this._clearReconnect();

  this.stream = this.streamBuilder();

  // MqttConnection
  this.conn = this.stream.pipe(new Connection(this.options.protocolVersion));

  // Set encoding of incoming publish payloads
  if (this.options.encoding) {
    this.conn.setPacketEncoding(this.options.encoding);
  }

  // Suppress connection errors
  this.stream.on('error', nop);

  // Echo stream close
  this.stream.on('close', this.emit.bind(this, 'close'));

  // Send a connect packet on stream connect
  this.stream.on('connect', function () {
    that.conn.connect(that.options);
  });

  this.stream.on('secureConnect', function () {
    that.conn.connect(that.options);
  });

  // Handle incoming publish
  this.conn.on('publish', function (packet) {
    that._handlePublish(packet);
  });

  // one single handleAck function
  var handleAck = function (packet) {
    that._handleAck(packet);
  };

  // Handle incoming acks
  var acks = ['get', 'puback', 'pubrec', 'pubcomp', 'suback', 'unsuback'];

  acks.forEach(function (event) {
    that.conn.on(event, handleAck);
  });

  // Handle outgoing acks
  this.conn.on('pubrel', function (packet) {
    that._handlePubrel(packet);
  });

  // Handle connack
  this.conn.on('connack', function (packet) {
    that._handleConnack(packet);
  });

  // Handle pingresp
  this.conn.on('pingresp', function (packet) {
    that._handlePingresp(packet);
  });

  // Echo connection errors
  this.conn.on('error', this.emit.bind(this, 'error'));
};

MqttClient.prototype.publish_to_alias =
    function(alias, message, opts, callback) {
        var topic = ",yta/" + alias;
        this.publish(topic, message, opts, callback);
    }

/**
 * publish - publish <message> to <topic>
 *
 * @param {String} topic - topic to publish to
 * @param {String, Buffer} message - message to publish
 * @param {Object} [opts] - publish options, includes:
 *    {Number} qos - qos level to publish on
 *    {Boolean} retain - whether or not to retain the message
 * @param {Function} [callback] - function(err){}
 *    called when publish succeeds or fails
 * @returns {MqttClient} this - for chaining
 * @api public
 *
 * @example client.publish('topic', 'message');
 * @example
 *     client.publish('topic', 'message', {qos: 1, retain: true});
 * @example client.publish('topic', 'message', console.log);
 */
MqttClient.prototype.publish =
function(topic, message, opts, callback) {
  var packet;

  // .publish(topic, payload, cb);
  if ('function' === typeof opts) {
    callback = opts;
    opts = null;
  }

  // Default opts
  if(!opts) opts = {qos: 1, retain: false};

  callback = callback || nop;

    // Support messageId in opts
    var messageId = opts.messageId;
    if (!messageId) {
        messageId = this._nextId();
    } else if (this.options.protocolVersion == 0x13) {
        messageId = bignum(messageId);
    }

  packet = {
    topic: topic,
    payload: message,
    qos: opts.qos,
    retain: opts.retain,
//    messageId: this._nextId()
      messageId: messageId
  }

  this._sendPacket('publish', packet, function () {
    switch (opts.qos) {
      case 0:
        // Immediately callback
        callback();
        break;
      case 1:
        // Add to puback callbacks
        this.inflight.puback[packet.messageId] = callback;
        break;
      case 2:
        // Add to pubrec callbacks
        this.inflight.pubrec[packet.messageId] = callback;
        break
      default:
        break;
    }
  });

  return this;
};

/**
 * subscribe - subscribe to <topic>
 *
 * @param {String, Array} topic - topic(s) to subscribe to
 * @param {Object} [opts] - subscription options, includes:
 *    {Number} qos - subscribe qos level
 * @param {Function} [callback] - function(err, granted){} where:
 *    {Error} err - subscription error (none at the moment!)
 *    {Array} granted - array of {topic: 't', qos: 0}
 * @returns {MqttClient} this - for chaining
 * @api public
 * @example client.subscribe('topic');
 * @example client.subscribe('topic', {qos: 1});
 * @example client.subscribe('topic', console.log);
 */
MqttClient.prototype.subscribe =
function(topic, opts, callback) {
  var subs = [];

  // .subscribe('topic', callback)
  if ('function' === typeof opts) {
    callback = opts;
    opts = null;
  }

  // Defaults
  opts = opts || {qos: 1};
  callback = callback || nop;

  if ('string' === typeof topic) {
    subs.push({topic: topic, qos: opts.qos});
  } else if ('object' === typeof topic) {
    // TODO: harder array check
    for (var i = 0; i < topic.length; i += 1) {
      var t = topic[i];
      subs.push({topic: t, qos: opts.qos});
    }
  } else {
    // Error!
  }

  var packet = {
    subscriptions: subs,
    qos: 1,
    retain: false,
    dup: false,
    messageId: this._nextId()
  };

  this._sendPacket('subscribe', packet, function () {
    this.inflight.suback[packet.messageId] = {
      callback: callback,
      packet: packet
    };
  });

  return this;
};

/**
 * get - expand command: get
 *
 * @param {Object} [opts] - get options, includes:
 *    {Number} command - detailed command
 *    {String} data - command params
 * @param {Function} [callback] - function(err){}
 *    called when get succeeds or fails
 * @returns {MqttClient} this - for chaining
 * @api get
 *
 * @example client.get({command:0, data:"alias"});
 * @example client.get({command:0, data:"alias"}, console.log);
 */
MqttClient.prototype.get =
    function(opts, callback, recvack_callback) {
        // Defaults
        if(!opts){
            //TODO error
        }
        callback = callback || nop;
        recvack_callback = recvack_callback || nop;

        // Support messageId in opts
        var messageId = null;
        if (opts.data && opts.data.messageId) messageId = opts.data.messageId;

        if (!messageId) {
            messageId = this._nextId();
        } else {
            delete opts.data.messageId;

            if (this.options.protocolVersion == 0x13) {
                messageId = bignum(messageId);
            } else {
                messageId = parseInt(messageId);
            }
        }

        var packet = {
            command: opts.command,
            data: opts.data,
            topics: opts.topics,
            payload: opts.payload,
//            messageId: this._nextId()
            messageId: messageId
        };

        if (opts.publishedTimeout !== undefined) {
            var that = this;
            var publishTimeout = opts.publishedTimeout;
            this.toPublish = 1;
            if (opts.publishedMsgId !== undefined) {
                this.message_id = opts.publishedMsgId;
		delete opts.publishedMsgId;
            }
	    delete opts.publishedTimeout;
            this.publishTimer = setInterval(function () {
                that.publishTimeUp();
            }, publishTimeout);
        }

        this._sendPacket('get', packet, function () {
          //  console.log("get command msgid = " + packet.messageId);

            this.inflight.get[packet.messageId] = callback;

            this.inflight.recvack[packet.messageId] = recvack_callback;
        });

        return this;
    }

/**
 * set alias
 *
 * @param {String} alias - alias name
 * @param {Function} [callback] - function(err){}
 *    called when publish succeeds or fails

 */
MqttClient.prototype.set_alias =
    function(alias, callback) {
        this.publish(',yali', alias, {qos: 1}, callback);
    }

/**
 * get_alias - expand command: get
 *
 * @param {String} alias - alias name
 * @param {Function} [callback] - function(err, payload){}
 *    called when get_alias succeeds or fails
 * @returns {MqttClient} this - for chaining
 * @api get_alias
 *
 * @example client.get_alias(function(err, payload){console.log(payload)});
 */
MqttClient.prototype.get_alias =
    function( callback) {
        if(callback){
            var opts = {
                command: 1,
                data: "",
                protocolVersion:0x13
            };
            return this.get(opts, callback);
        }else{
            //TODO error
        }

    }

/**
 * get_state - expand command: get
 *
 * @param {String} alias - alias name
 * @param {Function} [callback] - function(err, payload){}
 *    called when get_state succeeds or fails
 * @returns {MqttClient} this - for chaining
 * @api get_state
 *
 * @example client.get_state("alias", function(err, payload){console.log(payload)});
 */
MqttClient.prototype.get_state =
    function(alias, callback) {
        if("string" === typeof alias){
            var opts = {
                command: 9,
                data: alias,
                protocolVersion:0x13
            };
            return this.get(opts, callback);
        }else{
            //TODO error
        }

    }

/**
 * where_now - expand command: get
 *
 * @param {String} alias - alias name
 * @param {Function} [callback] - function(err, payload){}
 *    called when where_now succeeds or fails
 * @returns {MqttClient} this - for chaining
 * @api where_now
 *
 * @example client.where_now("alias", function(err, payload){console.log(payload)});
 */
MqttClient.prototype.where_now =
    function(alias, callback) {
        if("string" === typeof alias){
            var opts = {
                command: 3,
                data: alias,
                protocolVersion:0x13
            };
            return this.get(opts, callback);
        }else{
            //TODO error
        }

    }

/**
 * here_now - expand command: get
 *
 * @param {String} topic - topic name
 * @param {Function} [callback] - function(err, payload){}
 *    called when where_now succeeds or fails
 * @returns {MqttClient} this - for chaining
 * @api here_now
 *
 * @example client.here_now("alias", function(err, payload){console.log(payload)});
 */
MqttClient.prototype.here_now =
    function(topic, callback) {
        if("string" === typeof topic){
            var opts = {
                command: 5,
                data: topic,
                protocolVersion:0x13
            };
            return this.get(opts, callback);
        }else{
            //TODO error
        }
    }

MqttClient.prototype.publish2_to_alias =
    function(alias, payload, opts, callback, recvack_callback) {
        var topic = ",yta/" + alias;
        this.publish2(topic, payload, opts, callback, recvack_callback);
    }

/**
 *
 * @param topics, multi topic separated by commas example: t1,t2,t3
 * @param payload
 * @param opts : json object {"time_reserve":3000, "time_delay":2000,"platform":0,}
 * @param callback
 * @returns {MqttClient}
 */
MqttClient.prototype.publish2 =
    function(topics, payload, opts, callback, recvack_callback) {

        if ('function' === typeof opts) {
            recvack_callback = callback;
            callback = opts;
            opts = null;
        }

        var opts2 = {
                command: 7,
                topics: topics,
                payload: payload,
                data: opts,
                protocolVersion:0x13
            };
            return this.get(opts2, callback, recvack_callback);

    }


function publish_callback(num,cb)
{
    this.num = num;
    this.callback = cb;

}


/**
 *
 * @param topics, multi topic separated by commas example: t1,t2,t3
 * @param payload
 * @param opts : json object {"time_reserve":3000, "time_delay":2000,"platform":0,}
 * @param callback
 * @returns {MqttClient}
 */
MqttClient.prototype.publish_enhancement =
    function(topics, payload, opts, callback) {
        var self = this;
        var topicArr =  topics.toString().split(",");
        var publish_cb = new publish_callback(topicArr.length, callback);

        async.eachSeries(topicArr,function (topic, cb) {
         //   console.log("topic = " +  topic);
            self.publish_opts(topic, payload, opts, publish_cb);
            cb(null);


        }), function (err, results){
            if (err) {

            } else {
               // return this.get(opts, callback);
            }
        }

    }

/**
 * unsubscribe - unsubscribe from topic(s)
 *
 * @param {String, Array} topic - topics to unsubscribe from
 * @param {Function} [callback] - callback fired on unsuback
 * @returns {MqttClient} this - for chaining
 * @api public
 * @example client.unsubscribe('topic');
 * @example client.unsubscribe('topic', console.log);
 */
MqttClient.prototype.unsubscribe = function(topic, callback) {
  callback = callback || nop;
  var packet = {messageId: this._nextId()};

  if ('string' === typeof topic) {
    packet.unsubscriptions = [topic];
  } else if ('object' === typeof topic && topic.length) {
    packet.unsubscriptions = topic;
  }

  this._sendPacket('unsubscribe', packet, function () {
    this.inflight.unsuback[packet.messageId] = callback;
  });

  return this;
};

/**
 * end - close connection
 *
 * @returns {MqttClient} this - for chaining
 * @api public
 */
MqttClient.prototype.end = function() {
  this.disconnecting = true;
  if (!this.connected) {
    this.once('connect', this._cleanUp.bind(this));
  } else {
    this._cleanUp();
  }

  return this;
};

/**
 * _reconnect - implement reconnection
 * @api privateish
 */
MqttClient.prototype._reconnect = function() {

  if (this.conn) {
    this.conn.removeAllListeners();
    delete this.conn;
  }

  this._setupStream();
};

/**
 * _setupReconnect - setup reconnect timer
 */
MqttClient.prototype._setupReconnect = function() {
  var that = this;

  if (!that.disconnecting && !that.reconnectTimer && (that.options.reconnectPeriod > 0)) {
    that.reconnectTimer = setInterval(function () {
      that._reconnect();
    }, that.options.reconnectPeriod);
  }
};

/**
 * _clearReconnect - clear the reconnect timer
 */
MqttClient.prototype._clearReconnect = function() {
  if (this.reconnectTimer) {
    clearInterval(this.reconnectTimer);
    this.reconnectTimer = false;
  }
};

MqttClient.prototype.publishTimeUp = function() {
    if (this.publishTimer) {
        if (this.toPublish == 1) {
            var mid = this.message_id;
            var cb = this.inflight.get[mid];

            var payload = {
                command: 8,
                status: 1,
                data: 'fail',
                msgId: mid
            };

            cb(1, payload);
            delete this.inflight['get'][mid];
        }
        clearInterval(this.publishTimer);
        this.publishTimer = false;
    }
};


/**
 * _cleanUp - clean up on connection end
 * @api private
 */
MqttClient.prototype._cleanUp = function() {
  this.conn.disconnect();
  this.stream.end();
  if (this.pingTimer !== null) {
    clearInterval(this.pingTimer);
    this.pingTimer = null;
  }
};

/**
 * _sendPacket - send or queue a packet
 * @param {String} type - packet type (see `protocol`)
 * @param {Object} packet - packet options
 * @param {Function} cb - callback when the packet is sent
 * @api private
 */

MqttClient.prototype._sendPacket = function(type, packet, cb) {
  if (this.connected) {
      packet.protocolVersion = this.options.protocolVersion;
      // console.log(packet);
    this.conn[type](packet);
    if (cb) cb.call(this);
  } else {
    this.queue.push({type: type, packet: packet, cb: cb});
  }
};

/**
 * _setupPingTimer - setup the ping timer
 *
 * @api private
 */
MqttClient.prototype._setupPingTimer = function() {
  var that = this;

  if (!this.pingTimer && this.options.keepalive) {
    this.pingResp = true;
    this.pingTimer = setInterval(function () {
        that._checkPing();
    }, this.options.keepalive * 1000);
  }
};

/**
 * _checkPing - check if a pingresp has come back, and ping the server again
 *
 * @api private
 */
MqttClient.prototype._checkPing = function () {
  if (this.pingResp) {
    this.pingResp = false;
    this.conn.pingreq();
  } else {
    this._cleanUp();
  }
};

/**
 * _handlePingresp - handle a pingresp
 *
 * @api private
 */
MqttClient.prototype._handlePingresp = function () {
  this.pingResp = true;
};

/**
 * _handleConnack
 *
 * @param {Object} packet
 * @api private
 */

MqttClient.prototype._handleConnack = function(packet) {
  var rc = packet.returnCode;

  // TODO: move to protocol
  var errors = [
    '',
    'Unacceptable protocol version',
    'Identifier rejected',
    'Server unavailable',
    'Bad username or password',
    'Not authorized'
  ];

  if (rc === 0) {
    this.emit('connect');
  } else if (rc > 0) {
    this.emit('error',
        new Error('Connection refused: ' + errors[rc]));
  }
};

/**
 * _handlePublish
 *
 * @param {Object} packet
 * @api private
 */

MqttClient.prototype._handlePublish = function(packet) {
  var topic = packet.topic
    , message = packet.payload
    , qos = packet.qos
    , mid = packet.messageId
    , retain = packet.retain;

  switch (qos) {
    case 0:
      this.emit('message', topic, message, packet);
      break;
    case 1:
      this.conn.puback({messageId: mid, protocolVersion: this.options.protocolVersion});
      this.emit('message', topic, message, packet);
      break;
    case 2:
      this.conn.pubrec({messageId: mid, protocolVersion: this.options.protocolVersion});
      this.incoming.pubrel[mid] = packet;
      break;
    default:
      break;
  }
};

/**
 * _handleAck
 *
 * @param {Object} packet
 * @api private
 */

MqttClient.prototype._handleAck = function(packet) {
  var mid = packet.messageId
    , type = packet.cmd
    , cb = this.inflight[type][mid]
      , recvack_callback = this.inflight["recvack"][mid];

    //
    // FIXME 兼容旧版代码
    //
    var ext_pub_ack = null;
    if (!cb && type == 'puback') {
        console.warn('got puback when calling publish2, it should not happended.')
        ext_pub_ack = this.inflight['get'][mid]
        cb = ext_pub_ack;
    }

//    console.log('type = ' + type + " msgid = " + mid);
//    console.log('command', packet.command);

    if (type == "get") {
        if (packet.command == EXT_CMD_PUB_RECVACK) {
            if (!recvack_callback) {
                console.warn("no recvack_callback, skip", mid);
            }
        } else {
            if (!cb) {
                // Server sent an ack in error, ignore it.
                console.warn("no callback available, skip", mid);

                return;
            }
        }
    }

    var error = null;

  // Process
  switch (type) {
    case "get":
        var payload = {
            command: packet.command,
            status: packet.status,
            data: packet.data,
            msgId: mid
      };
          if (payload.status != 0) error = payload.status;

        if(packet.command == 8 &&  cb instanceof  publish_callback) {
               cb.num --;
               if(cb.num == 0) {
                   var call = cb.callback;
                   if(call) {
                       call(error, payload);
                   }
               }
        } else if (packet.command == EXT_CMD_PUB_RECVACK) {
            if (recvack_callback) {
                recvack_callback(error, payload);
            }
        } else {
            cb(error, payload);
            this.toPublish = 0;
            if (this.publishTimer != null) {
                clearInterval(this.publishTimer);
                this.publishTimer = null;
            }
        }

      break;
    case 'puback':
      // Callback - we're done
        if (ext_pub_ack) {
            //
            // FIXME 兼容旧版代码
            //
            cb(0, {msgId: mid});
        } else {
            cb(mid);
        }
      break;
    case 'pubrec':
      // Pubrel and add to pubcomp list
      this.conn.pubrel(packet);
      this.inflight.pubcomp[mid] = cb;
      break;
    case 'pubcomp':
      // Callback - we're done
      cb();
      break;
    case 'suback':
      // TODO: RIDICULOUS HACK, PLEASE FIX
      var origSubs = cb.packet.subscriptions
        , cb = cb.callback
        , granted = packet.granted;

      for (var i = 0; i < granted.length; i += 1) {
        origSubs[i].qos = granted[i];
      }
      cb(null, origSubs);
      break;
    case 'unsuback':
      cb();
      break;
    default:
      // code
  }

  // Remove from queue
  delete this.inflight[type][mid];
    //
    // FIXME 兼容旧版代码
    //
    if (ext_pub_ack) {
        delete this.inflight['get'][mid];
    }
};

/**
 * _handlePubrel
 *
 * @param {Object} packet
 * @api private
 */

MqttClient.prototype._handlePubrel = function(packet) {
  var mid = packet.messageId
    , pub = this.incoming.pubrel[mid];

  if (!pub) this.emit('error', new Error('Unknown message id'));
  this.conn.pubcomp({messageId: mid});
  this.emit('message', pub.topic, pub.payload, pub);
};

/**
 * _nextId
 */
MqttClient.prototype._nextId = function() {
    if (this.options.protocolVersion == 0x13) {
        return druuid.gen();
    } else {
        var id = this.nextId++;
        // Ensure 16 bit unsigned int:
        if (id === 65535) {
            this.nextId = 1;
        }
        return id;
    }
};

MqttClient.prototype.getNextMessageId = function() {
    return this._nextId();
}
