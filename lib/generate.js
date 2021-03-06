var protocol = require('./protocol');
var bops = require('bops');
var bignum = require('bignum');

function check_message_id(version, message_id) {
    if (message_id.constructor) {
        if (message_id.constructor.name == 'BigNum') {
            return true;
        }
    }

    if (version != protocol.VERSION13) {
        if (typeof message_id === 'number') {
            return true;
        }
    }

    return false;
}

// Connect
module.exports.connect = function(opts) {
  var opts = opts || {}
    , protocolId = opts.protocolId
    , protocolVersion = opts.protocolVersion
    , will = opts.will
    , clean = opts.clean
    , keepalive = opts.keepalive
    , clientId = opts.clientId
    , username = opts.username
    , password = opts.password;

  var length = 0;

  // Must be a string and non-falsy
  if(!protocolId || typeof protocolId !== "string") { 
    return new Error('Invalid protocol id');
  } else {
    length += protocolId.length + 2;
  }

  // Must be a 1 byte number
  if (!protocolVersion || 
      'number' !== typeof protocolVersion ||
      protocolVersion > 255 ||
      protocolVersion < 0) {

    return new Error('Invalid protocol version');
  } else {
    length += 1;
  }
  
  // Must be a non-falsy string
  if(!clientId ||
      'string' !== typeof clientId) {
    return new Error('Invalid client id');
  } else {
    length += clientId.length + 2;
  }
  
  // Must be a two byte number
  if ('number' !== typeof keepalive ||
      keepalive < 0 ||
      keepalive > 65535) {
    return new Error('Invalid keepalive');
  } else {
    length += 2;
  }

  // Connect flags
  length += 1;

  // If will exists...
  if (will) {
    // It must be an object
    if ('object' !== typeof will) {
      return new Error('Invalid will');
    } 
    // It must have topic typeof string
    if (!will.topic || 'string' !== typeof will.topic) {
      return new Error('Invalid will - invalid topic');
    } else {
      length += will.topic.length + 2;
    }
    // TODO: should be a buffer
    // It must have payload typeof string
    if (!will.payload || 'string' !== typeof will.payload) {
      return new Error('Invalid will - invalid payload');
    } else {
      length += will.payload.length + 2;
    }
  }

  // Username
  if (username && 'string' !== typeof username) {
    return new Error('Invalid username');
  } else if (username) {
    length += username.length + 2;
  } 

  // Password
  if (password && 'string' !== typeof password) {
    return new Error('Invalid password');
  } else if (password) {
    length += password.length + 2;
  }

  var buffer = bops.create(1 + calc_length_length(length) + length)
    , pos = 0;

  // Generate header
  bops.writeUInt8(buffer, protocol.codes['connect'] << protocol.CMD_SHIFT, pos++);

  // Generate length
  pos += write_length(buffer, pos, length);
  
  // Generate protocol ID
  pos += write_string(buffer, pos, protocolId);
  bops.writeUInt8(buffer, protocolVersion, pos++);
  
  // Connect flags
  var flags = 0;
  flags |= username ? protocol.USERNAME_MASK : 0;
  flags |= password ? protocol.PASSWORD_MASK : 0;
  flags |= (will && will.retain) ? protocol.WILL_RETAIN_MASK : 0;
  flags |= (will && will.qos) ? 
    will.qos << protocol.WILL_QOS_SHIFT : 0;
  flags |= will ? protocol.WILL_FLAG_MASK : 0;
  flags |= clean ? protocol.CLEAN_SESSION_MASK : 0;

  bops.writeUInt8(buffer, flags, pos++);
  
  // Keepalive
  pos += write_number(buffer, pos, keepalive);
  
  // Client ID
  pos += write_string(buffer, pos, clientId);
  
  // Will
  if (will) {
    pos += write_string(buffer, pos, will.topic);
    pos += write_string(buffer, pos, will.payload);
  }
  
  // Username and password
  if (username) pos += write_string(buffer, pos, username);
  if (password) pos += write_string(buffer, pos, password);

  return buffer;
};

// Connack
module.exports.connack = function(opts) {
  var opts = opts || {}
      version = opts.protocolVersion
    , rc = opts.returnCode;


  // Check return code
  if ('number' !== typeof rc) {
    return new Error('Invalid return code');
  }

  var buffer = bops.create(4)
    , pos = 0;

  bops.writeUInt8(buffer, protocol.codes['connack'] << protocol.CMD_SHIFT, pos++);
  pos += write_length(buffer, pos, 2);
  pos += write_number(buffer, pos, rc);

  return buffer;
};

// Publish
var empty = bops.create(0);
module.exports.publish = function(opts) {
  var opts = opts || {}
    , dup = opts.dup ? protocol.DUP_MASK : 0
    , qos = opts.qos
    , retain = opts.retain ? protocol.RETAIN_MASK : 0
    , topic = opts.topic
    , payload = opts.payload || empty
    , id = opts.messageId
      , version = opts.protocolVersion;

  var length = 0;

  // Topic must be a non-empty string
  if (!topic || 'string' !== typeof topic) {
    return new Error('Invalid topic');
  } else {
    length += topic.length + 2;
  }

  // get the payload length
  if (!bops.is(payload)) {
    length += Buffer.byteLength(payload);
  } else {
    length += payload.length;
  }
  
  // Message id must a number if qos > 0
//  if (qos && 'number' !== typeof id) {
    if (qos && !check_message_id(version, id)) {
    return new Error('Invalid message id')
  } else if (qos) {
      if (version == protocol.VERSION13) {
          length += 8;
      } else {
          length += 2;
      }
  }

  var buffer = bops.create(1 + calc_length_length(length) + length)
    , pos = 0;

  // Header
  buffer[pos++] = 
    protocol.codes['publish'] << protocol.CMD_SHIFT |
    dup |
    qos << protocol.QOS_SHIFT |
    retain;

  // Remaining length
  pos += write_length(buffer, pos, length);

  // Topic
  pos += write_string(buffer, pos, topic);

  // Message ID
  if (qos > 0) {
      if (version == protocol.VERSION13) {
          pos += write_message_id_len64(buffer, pos, id);
      } else {
          pos += write_number(buffer, pos, id);
      }
  }

  // Payload
  if (!bops.is(payload)) {
    write_string_no_pos(buffer, pos, payload);
  } else {
    write_buffer(buffer, pos, payload);
  }

  return buffer;
};

/* Puback, pubrec, pubrel and pubcomp */
var gen_pubs = function(opts, type) {
  var opts = opts || {}
    , id = opts.messageId
    , dup = (opts.dup && type === 'pubrel') ? protocol.DUP_MASK : 0
    , qos = type === 'pubrel' ? 1 : 0
      , version = opts.protocolVersion;

  var length = 0;

  // Check message ID
//  if ('number' !== typeof id) {
    if (!check_message_id(version, id)) {
    return new Error('Invalid message id');
  } else {
    if (version == protocol.VERSION13) {
        length += 8;
    } else {
        length += 2;
    }
  }

  var buffer = bops.create(1 + calc_length_length(length) + length)
    , pos = 0;

  // Header
  buffer[pos++] = 
    protocol.codes[type] << protocol.CMD_SHIFT |
    dup |
    qos << protocol.QOS_SHIFT;

  // Length
  pos += write_length(buffer, pos, length);

  // Message ID
    if (version == protocol.VERSION13) {
        pos += write_message_id_len64(buffer, pos, id);
    } else {
        pos += write_number(buffer, pos, id);
    }

  return buffer;
}

var pubs = ['puback', 'pubrec', 'pubrel', 'pubcomp'];

for (var i = 0; i < pubs.length; i++) {
  module.exports[pubs[i]] = function(pubType) {
    return function(opts) {
      return gen_pubs(opts, pubType);
    }
  }(pubs[i]);
}

/* Subscribe */
module.exports.subscribe = function(opts) {
  var opts = opts || {}
    , dup = opts.dup ? protocol.DUP_MASK : 0
    , qos = opts.qos || 0
    , id = opts.messageId
    , subs = opts.subscriptions
      , version = opts.protocolVersion;

  var length = 0;

  // Check mid
//  if ('number' !== typeof id) {
    if (!check_message_id(version, id)) {
    return new Error('Invalid message id');
  } else {
      if (version == protocol.VERSION13) {
          length += 8;
      } else {
          length += 2;
      }
  }
  // Check subscriptions
  if ('object' === typeof subs && subs.length) {
    for (var i = 0; i < subs.length; i += 1) {
      var topic = subs[i].topic
        , qos = subs[i].qos;

      if ('string' !== typeof topic) {
        return new Error('Invalid subscriptions - invalid topic');
      }
      if ('number' !== typeof qos) {
        return new Error('Invalid subscriptions - invalid qos');
      }

      length += topic.length + 2 + 1;
    }
  } else {
    return new Error('Invalid subscriptions');
  }

  var buffer = bops.create(1 + calc_length_length(length) + length)
    , pos = 0;

  // Generate header
  bops.writeUInt8(buffer,
    protocol.codes['subscribe'] << protocol.CMD_SHIFT | 
    dup | 
    1 << protocol.QOS_SHIFT, pos++);

  // Generate length
  pos += write_length(buffer, pos, length);

  // Generate message ID
    if (version == protocol.VERSION13) {
        pos += write_message_id_len64(buffer, pos, id);
    } else {
        pos += write_number(buffer, pos, id);
    }

  // Generate subs
  for (var i = 0; i < subs.length; i++) {
    var sub = subs[i]
      , topic = sub.topic
      , qos = sub.qos;

    // Write topic string
    pos += write_string(buffer, pos, topic);
    // Write qos
    bops.writeUInt8(buffer, qos, pos++);
  } 

  return buffer;
};

/* Suback */
module.exports.suback = function(opts) {
  var opts = opts || {}
    , id = opts.messageId
    , granted = opts.granted
      , version = opts.protocolVersion;

  var length = 0;

  // Check message id
//  if ('number' !== typeof id) {
    if (!check_message_id(version, id)) {
    return new Error('Invalid message id');
  } else {
      if (version == protocol.VERSION13) {
          length += 8;
      } else {
          length += 2;
      }
  }
  // Check granted qos vector
  if ('object' === typeof granted && granted.length) {
    for (var i = 0; i < granted.length; i += 1) {
      if ('number' !== typeof granted[i]) {
        return new Error('Invalid qos vector');
      }
      length += 1;
    }
  } else {
    return new Error('Invalid qos vector');
  }

  var buffer = bops.create(1 + calc_length_length(length) + length)
    , pos = 0;

  // Header
  bops.writeUInt8(buffer, protocol.codes['suback'] << protocol.CMD_SHIFT, pos++);

  // Length
  pos += write_length(buffer, pos, length);

  // Message ID
    if (version == protocol.VERSION13) {
        pos += write_message_id_len64(buffer, pos, id);
    } else {
        pos += write_number(buffer, pos, id);
    }

  // Subscriptions
  for (var i = 0; i < granted.length; i++) {
    bops.writeUInt8(buffer, granted[i], pos++);
  }

  return buffer;
};

/* Unsubscribe */
module.exports.unsubscribe = function(opts) {
  var opts = opts || {}
    , id = opts.messageId
    , dup = opts.dup ? protocol.DUP_MASK : 0
    , unsubs = opts.unsubscriptions
      version = opts.protocolVersion;

  var length = 0;

  // Check message id
//  if ('number' !== typeof id) {
    if (!check_message_id(version, id)) {
    return new Error('Invalid message id');
  } else {
      if (version == protocol.VERSION13) {
          length += 8;
      } else {
          length += 2;
      }
  }
  // Check unsubs
  if ('object' === typeof unsubs && unsubs.length) {
    for (var i = 0; i < unsubs.length; i += 1) {
      if ('string' !== typeof unsubs[i]) {
        return new Error('Invalid unsubscriptions');
      }
      length += unsubs[i].length + 2;
    }
  } else {
    return new Error('Invalid unsubscriptions');
  }

  var buffer = bops.create(1 + calc_length_length(length) + length)
    , pos = 0;

  // Header
  buffer[pos++] = 
    protocol.codes['unsubscribe'] << protocol.CMD_SHIFT |
    dup | 
    1 << protocol.QOS_SHIFT;

  // Length
  pos += write_length(buffer, pos, length);

  // Message ID
    if (version == protocol.VERSION13) {
        pos += write_message_id_len64(buffer, pos, id);
    } else {
        pos += write_number(buffer, pos, id);
    }

  // Unsubs
  for (var i = 0; i < unsubs.length; i++) {
    pos += write_string(buffer, pos, unsubs[i]);
  }

  return buffer;
};
  
/* Unsuback */
/* Note: uses gen_pubs since unsuback is the same as suback */
module.exports.unsuback = function(type) {
  return function(opts) {
    return gen_pubs(opts, type);
  }
}('unsuback');

/* Pingreq, pingresp, disconnect */
var empties = ['pingreq', 'pingresp', 'disconnect'];

for (var i = 0; i < empties.length; i++) {
  module.exports[empties[i]] = function(type) {
    return function(opts) {
      var buf = bops.create(2);
      buf[0] = protocol.codes[type] << 4;
      buf[1] = 0;
      return buf;
    }
  }(empties[i]);
}

// if opts.status -> generate getAck
module.exports.get = function(opts) {
    var opts = opts || {}
        , id = opts.messageId
        , command = opts.command
        , status = opts.status
        , data = opts.data
        , topics = opts.topics
        , payload = opts.payload
        , version = opts.protocolVersion;

    var length = 0;

    // Message id must a number if qos > 0
    if (!check_message_id(version, id)) {
        return new Error('Invalid message id')
    } else {
        if (version == protocol.VERSION13) {
            length += 8;
        } else {
            length += 2;
        }
    }

    if ('number' !== typeof command) {
        return new Error('Invalid get - invalid command');
    } else {
        length += 1;
    }


    // status could be 0
    if (status != undefined && 'number' !== typeof status) {
        return new Error('Invalid get - invalid status');
    } else if (status != undefined) {
        length += 1;
    }
    if(command == 7) {

        if(topics == undefined) {
            return new Error('Invalid publish - invalid topics');
        }
        if(topics == payload) {
            return new Error('Invalid publish - invalid payload');
        }
        data = json2buf(topics,payload,data);
        length += data.length;
    } else {
        length += data.length + 2;
    }

    var buffer = bops.create(1 + calc_length_length(length) + length)
        , pos = 0;
   // console.log("buffer length = " + buffer.length);

    // Header
    bops.writeUInt8(buffer, protocol.codes['get'] << protocol.CMD_SHIFT, pos++);

    // Length
    pos += write_length(buffer, pos, length);

    // Message ID
    if (version == protocol.VERSION13) {
        pos += write_message_id_len64(buffer, pos, id);
    } else {
        pos += write_number(buffer, pos, id);
    }

    // command
    bops.writeUInt8(buffer, command, pos++);

    // status
    if (status != undefined) {
        bops.writeUInt8(buffer, status, pos++);
    }


    if (command == 7) {
        write_buffer(buffer, pos, data);
     } else {
        write_string(buffer, pos, data);
    }


    return buffer;
};

/**
 * calc_length_length - calculate the length of the remaining
 * length field
 *
 * @api private
 */

function calc_length_length(length) {
  if (length >= 0 && length < 128) {
    return 1;
  } else if (length >= 128 && length < 16384) {
    return 2;
  } else if (length >= 16384 && length < 2097152) {
    return 3;
  } else if (length >= 2097152 && length < 268435456) {
    return 4;
  } else {
    return 0;
  }
};

/**
 * write_length - write an MQTT style length field to the buffer
 *
 * @param <Buffer> buffer - destination
 * @param <Number> pos - offset
 * @param <Number> length - length (>0)
 * @returns <Number> number of bytes written
 *
 * @api private
 */

function write_length(buffer, pos, length) {
  var digit = 0
    , origPos = pos;
  
  do {
    digit = length % 128 | 0
    length = length / 128 | 0;
    if (length > 0) {
        digit = digit | 0x80;
    }
    bops.writeUInt8(buffer, digit, pos++);
  } while (length > 0);
  
  return pos - origPos;
};

/**
 * write_string - write a utf8 string to the buffer
 *
 * @param <Buffer> buffer - destination
 * @param <Number> pos - offset
 * @param <String> string - string to write
 * @return <Number> number of bytes written
 *
 * @api private
 */

function write_string(buffer, pos, string) {
  var strlen = Buffer.byteLength(string);
  write_number(buffer, pos, strlen);

  write_string_no_pos(buffer, pos + 2, string);

  return strlen + 2;
};

function write_string_no_pos(buffer, pos, string) {
  if (Buffer.isBuffer(buffer)) {
    buffer.write(string, pos);
  } else {
    var bufString = bops.from(string, 'utf8');
    bops.copy(bufString, buffer, pos, 0, bufString.length);
  }
}

/**
 * write_buffer - write buffer to buffer
 *
 * @param <Buffer> buffer - dest buffer
 * @param <Number> pos - offset
 * @param <Buffer> src - source buffer
 * @return <Number> number of bytes written
 *
 * @api private
 */

function write_buffer(buffer, pos, src) {
  bops.copy(src, buffer, pos); 
  return src.length;
}

function is_bignum(number) {
//    console.log("typeof(number): " + typeof number + " " + number)
    if (number && typeof number === 'Object') {
        if (number.constructor.name == 'BigNum') {
            return true;
        }
    }

    return false;
}

/**
 * write_number - write a two byte number to the buffer
 *
 * @param <Buffer> buffer - destination
 * @param <Number> pos - offset
 * @param <String> number - number to write
 * @return <Number> number of bytes written
 *
 * @api private
 */
function write_number(buffer, pos, number) {
    if (is_bignum(number)) {
//        console.log(number + "is bignum");
        number = number.toNumber();
    }
//    console.log("number = " + number);
//    console.log("number >> 8 = " + (number >> 8));
  bops.writeUInt8(buffer, (number >> 8) & 0x00FF , pos);
  bops.writeUInt8(buffer, number & 0x00FF, pos + 1);
  
  return 2;
};

/**
 * write_message_id_len64 - write a 64bit number to the buffer
 * @param <Buffer> buffer - destination
 * @param <Number> pos - offset
 * @param <String> number - number to write
 * @return <Number> number of bytes written
 */
function write_message_id_len64(buffer, pos, number) {
//    var b = bignum(number);
    bops.writeUInt32BE(buffer, number.shiftRight(32), pos);
    bops.writeUInt32BE(buffer, number.and(0xFFFFFFFF), pos + 4);

    return 8;
};

function json2buf(topics,payload,data) {
    if(data == undefined) {
        data = {};
    }
    data["topics"] = topics;
    data["payload"] = payload;
    var lenth = 0;
    lenth +=2;
    for(var key in data) {
        lenth +=3;
      //  console.log(key + ": " + data[key]);
        var value = data[key];
        var strlen = 0;
        switch (typeof value){
            case "number":
            case "string":
                strlen = Buffer.byteLength(value.toString());
                break;
            case "object":  // for json, example apn_json
                strlen = Buffer.byteLength(JSON.stringify(value));
                break;
            default :
                strlen = 0;
                break
        }
        lenth += strlen;
    }
    var buff = bops.create(lenth), pos = 0;
    pos += write_number(buff, pos, lenth);
    for(var key in data) {
       if(key2commad(key)){
           // console.log(key + ": " + data[key]);
           bops.writeUInt8(buff, key2commad(key), pos++);
           var value = data[key];
           switch(typeof value){
               case "string":
               case "number":
                   inc_len = write_string(buff, pos, value.toString());
                   pos += inc_len;
                   break;
               case "object":  // for json
                   inc_len = write_string(buff, pos, JSON.stringify(value))
                   pos += inc_len
                   break;
               default:
                   break
           }
       }
    }
    return buff;
};

function key2commad(key) {
    return protocol.extend_codes[key];
}
