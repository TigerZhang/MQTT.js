/* Parse - packet parsing */
var protocol = require('./protocol');
var bops = require('bops');
var utils = require('./parsing_utils');
var bignum = require('bignum');

var parser = module.exports;

parser.header = function(buf, packet) {
  var zero = bops.readUInt8(buf, 0);
  packet.cmd = protocol.types[zero >> protocol.CMD_SHIFT];
  packet.retain = (zero & protocol.RETAIN_MASK) !== 0;
  packet.qos = (zero >> protocol.QOS_SHIFT) & protocol.QOS_MASK;
  packet.dup = (zero & protocol.DUP_MASK) !== 0;
  return packet;
};

// return fixedHeader Length
parser.fixedHeaderLength = function(buf){
    var tmpPos = 1;  //skip first byte
    var byteCount = 1;
    var digit;

    if(buf.length <= 0){
        return 0;
    }

    do{
        digit = bops.readUInt8(buf, tmpPos++);
        byteCount ++;
    }while((digit & 128) && (byteCount < 6)); //max Length 5 byte

    return byteCount;
}

parser.connect = function(buf, packet) {
  parser._pos = 0;
  parser._len = buf.length;

  var protocolId // Protocol id
    , clientId // Client id
    , topic // Will topic
    , payload // Will payload
    , password // Password
    , username // Username
    , flags = {};
  
  // Parse protocol id
  protocolId = parser.parse_string(buf);
  if (protocolId === null) return new Error('Parse error - cannot parse protocol id');
  packet.protocolId = protocolId;

  // Parse protocol version number
  if(parser._pos > parser._len) return null;
  packet.protocolVersion = bops.readUInt8(buf, parser._pos);
  parser._pos += 1;
  
  // Parse connect flags
  flags.username = (bops.readUInt8(buf, parser._pos) & protocol.USERNAME_MASK);
  flags.password = (bops.readUInt8(buf, parser._pos) & protocol.PASSWORD_MASK);
  flags.will = (bops.readUInt8(buf, parser._pos) & protocol.WILL_FLAG_MASK);
  
  if(flags.will) {
    packet.will = {};
    packet.will.retain = (bops.readUInt8(buf, parser._pos) & protocol.WILL_RETAIN_MASK) !== 0;
    packet.will.qos = (bops.readUInt8(buf, parser._pos) & protocol.WILL_QOS_MASK) >> protocol.WILL_QOS_SHIFT;
  }
  
  packet.clean = (bops.readUInt8(buf, parser._pos) & protocol.CLEAN_SESSION_MASK) !== 0;
  parser._pos += 1;
  
  // Parse keepalive
  packet.keepalive = this.parse_num(buf);
  if(packet.keepalive === null) return null;
  
  // Parse client ID
  clientId = this.parse_string(buf);
  if(clientId === null) return new Error('Parse error - cannot parse client id');
  packet.clientId = clientId;

  if(flags.will) {
    // Parse will topic
    topic = this.parse_string(buf);
    if(topic === null) return new Error('Parse error - cannot parse will topic');
    packet.will.topic = topic;

    // Parse will payload
    payload = this.parse_string(buf);
    if(payload === null) return new Error('Parse error - cannot parse will payload');
    packet.will.payload = payload;
  }
  
  // Parse username
  if(flags.username) {
    username = this.parse_string(buf);
    if(username === null) return new Error('Parse error - cannot parse username');
    packet.username = username;
  }
  
  // Parse password
  if(flags.password) {
    password = this.parse_string(buf);
    if(password === null) return ;
    packet.password = password;
  }
  
  return packet;
};

parser.connack = function(buf, packet) {
  parser._len = buf.length;
  parser._pos = 0;
    
  packet.returnCode = parser.parse_num(buf);
  if(packet.returnCode === null) return new Error('Parse error - cannot parse return code');
  
  return packet;
};

parser.publish = function(buf, packet, encoding, opts) {
  parser._len = buf.length;
  parser._pos = 0;
  var topic;
    var opts
    if ('object' === typeof encoding) {
       opts  = encoding;
    } else {
        opts = opts || {};
    }

    // Parse topic name
  topic = parser.parse_string(buf);
  if(topic === null) return new Error('Parse error - cannot parse topic');
  packet.topic = topic;

  // Parse message ID
  if (packet.qos > 0) {
      if (opts.protocolVersion == protocol.VERSION13) {
          packet.messageId = parser.parse_message_id64(buf);
      } else {
          packet.messageId = parser.parse_message_id16(buf);
      }
    if(packet.messageId === null) return new Error('Parse error - cannot parse message id');
  }

  utils.parseEncodedPayload(parser, buf, encoding, packet);
  
  return packet; 
}

// Parse puback, pubrec, pubrel, pubcomp and suback
parser.puback =
parser.pubrec =
parser.pubrel =
parser.pubcomp =
parser.unsuback = function (buf, packet, encoding, opts) {
  parser._len = buf.length;
  parser._pos = 0;
    var opts
    if ('object' === typeof encoding) {
        opts  = encoding;
    } else {
        opts = opts || {};
    }


//  packet.messageId = parser.parse_num(buf, parser._len, parser._pos);
    if (opts.protocolVersion == protocol.VERSION13) {
        packet.messageId = parser.parse_message_id64(buf);
    } else {
        packet.messageId = parser.parse_message_id16(buf);
    }
  if (packet.messageId === null) return new Error('Parse error - cannot parse message id');

  return packet;
};

parser.subscribe = function(buf, packet, encoding, opts) {
  parser._len = buf.length;
  parser._pos = 0;
    var opts
    if ('object' === typeof encoding) {
        opts  = encoding;
    } else {
        opts = opts || {};
    }

  packet.subscriptions = [];

  // Parse message ID
//  packet.messageId = parser.parse_num(buf);
    if (opts.protocolVersion == protocol.VERSION13) {
        packet.messageId = parser.parse_message_id64(buf);
    } else {
        packet.messageId = parser.parse_message_id16(buf);
    }
  if (packet.messageId === null) return new Error('Parse error - cannot parse message id');
  
  while(parser._pos < parser._len) {
    var topic
      , qos;
    
    // Parse topic
    topic = parser.parse_string(buf);
    if(topic === null) return new Error('Parse error - cannot parse topic');

    // Parse QoS
    // TODO: possible failure location
    qos = buf[parser._pos++];
    
    // Push pair to subscriptions
    packet.subscriptions.push({topic: topic, qos: qos});
  }
  return packet;
};

parser.suback = function(buf, packet, encoding, opts) {
  parser._len = buf.length;
  parser._pos = 0;

    var opts
    if ('object' === typeof encoding) {
        opts  = encoding;
    } else {
        opts = opts || {};
    }

  packet.granted = [];
  
  // Parse message ID
//  packet.messageId = parser.parse_num(buf, parser._len, parser._pos);
    if (opts.protocolVersion == protocol.VERSION13) {
        packet.messageId = parser.parse_message_id64(buf);
    } else {
        packet.messageId = parser.parse_message_id16(buf);
    }
  if(packet.messageId === null) return new Error('Parse error - cannot parse message id');
  
  // Parse granted QoSes
  while(parser._pos < parser._len) {
    packet.granted.push(buf[parser._pos++]);
  }

  return packet;
};

parser.unsubscribe = function(buf, packet, encoding, opts) {
  parser._len = buf.length;
  parser._pos = 0;
    var opts
    if ('object' === typeof encoding) {
        opts  = encoding;
    } else {
        opts = opts || {};
    }

  packet.unsubscriptions = [];

  // Parse message ID
//  packet.messageId = parser.parse_num(buf, parser._len, parser._pos);
    if (opts.protocolVersion == protocol.VERSION13) {
        packet.messageId = parser.parse_message_id64(buf);
    } else {
        packet.messageId = parser.parse_message_id16(buf);
    }
  if(packet.messageId === null) return new Error('Parse error - cannot parse message id');
  
  while(parser._pos < parser._len) {
    var topic;
    
    // Parse topic
    topic = parser.parse_string(buf, parser._len, parser._pos);
    if(topic === null) return new Error('Parse error - cannot parse topic');

    // Push topic to unsubscriptions
    packet.unsubscriptions.push(topic);
  }

  return packet;
};

parser.reserved = function(buf, packet) {
  return new Error("reserved is not a valid command");
};

//if opts.ack -> parse getAck
parser.get = function(buf, packet, encoding, opts) {
    parser._len = buf.length;
    parser._pos = 0;
    var opts
    if ('object' === typeof encoding) {
        opts  = encoding;
    } else {
        opts = opts || {};
    }

    // Parse message ID
    if (opts.protocolVersion == protocol.VERSION13) {
        packet.messageId = parser.parse_message_id64(buf);
    } else {
        packet.messageId = parser.parse_message_id16(buf);
    }
    if (packet.messageId === null) return new Error('Parse error - cannot parse message id');

    // Parse command
    packet.command = parser.parse_byte(buf);
    if(packet.command === null) return new Error('Parse error - cannot parse command');

    // Parse status
    if(opts.ack){
        packet.status = parser.parse_byte(buf);
        if(packet.status === null) return new Error('Parse error - cannot parse status');
    }

    if (packet.command === 7) {
        packet.data = parser.parse_tlv(buf);

    } else {
    // Parse data
    packet.data = parser.parse_string(buf);
    if(packet.data === null) return new Error('Parse error - cannot parse data');
    }

    return packet;
};

var empties = ['pingreq', 'pingresp', 'disconnect'];

parser.pingreq =
parser.pingresp =
parser.disconnect = function (buf, packet) {
  return packet;
};

parser.parse_num = function(buf) {
  if(2 > parser._pos + parser._len) return null;

  var result = bops.readUInt16BE(buf, parser._pos);
  parser._pos += 2;
  return result;
}

parser.parse_byte = function(buf) {
    if(1 > parser._pos + parser._len) return null;

    var result = bops.readUInt8(buf, parser._pos);
    parser._pos += 1;
    return result;
}

parser.parse_message_id64 = function(buf) {
    if (8 > parser._pos + parser._len) return null;

    var higher = bops.readUInt32BE(buf, parser._pos);
    parser._pos += 4;
    var lower = bops.readUInt32BE(buf, parser._pos);
    parser._pos += 4;

    var h = bignum(higher).shiftLeft(32);
    return h.or(lower);
//    var l = bignum(lower & 0xFFFFFFFF);
//    var result = (higher << 32) + (lower & 0xFFFFFFFF);
//    return result;
}

parser.parse_message_id16 = function(buf) {
    var id = parser.parse_num(buf);

    return bignum(id);
}

parser.parse_string = function(buf) {
  var length = parser.parse_num(buf)
    , result;

  if(length === null || length + parser._pos > parser._len) return null;

  result = utils.toString(parser, buf, length);

  parser._pos += length;

  return result;
}

parser.parse_tlv = function(buf) {
    var dataLen = parser.parse_num(buf);
    if(dataLen === null || dataLen === 0) {
        return null;
    }
    var data ={};
    while(parser._pos < parser._len) {

        var key = parser.parse_byte(buf);
       // var lenth = parser.parse_num(buf);
        var value = parser.parse_string(buf);
        data[key] = value;
    }
    return data;
}
