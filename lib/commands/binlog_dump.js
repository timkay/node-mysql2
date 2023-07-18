'use strict';

const Command = require('./command');
const Packets = require('../packets');

const eventParsers = [];

class BinlogEventHeader {
  constructor(packet) {
    this.timestamp = packet.readInt32();
    this.eventType = packet.readInt8();
    this.serverId = packet.readInt32();
    this.eventSize = packet.readInt32();
    this.logPos = packet.readInt32();
    this.flags = packet.readInt16();
  }
}

class BinlogDump extends Command {
  constructor(opts) {
    super();
    // this.onResult = callback;
    this.opts = opts;
  }

  start(packet, connection) {
    const newPacket = new Packets.BinlogDump(this.opts);
    connection.writePacket(newPacket.toPacket(1));
    return BinlogDump.prototype.binlogData;
  }

  binlogData(packet) {
    // ok - continue consuming events
    // error - error
    // eof - end of binlog
    if (packet.isEOF()) {
      this.emit('eof');
      return null;
    }
    // binlog event header
    packet.readInt8();
    const header = new BinlogEventHeader(packet);
    const EventParser = eventParsers[header.eventType];
    let event;
    if (EventParser) {
      event = new EventParser(packet);
    } else {
      event = {
        name: 'UNKNOWN'
      };
    }
    event.header = header;
    this.emit('event', event);
    return BinlogDump.prototype.binlogData;
  }
}

class RotateEvent {
  constructor(packet) {
    this.pposition = packet.readInt32();
    // TODO: read uint64 here
    packet.readInt32(); // positionDword2
    this.nextBinlog = packet.readString();
    this.name = this.constructor.name;
  }
}

class IntvarEvent {
    constructor(packet) {
        this.type = packet.readInt8() === 1? 'LAST_INSERT_ID': 'auto_increment';
        this.value = packet.readInt32();
        // TODO: read uint64 here
        packet.readInt32();
        this.name = this.constructor.name;
    }
}

class FormatDescriptionEvent {
  constructor(packet) {
    this.binlogVersion = packet.readInt16();
    this.serverVersion = packet.readString(50).replace(/\u0000.*/, ''); // eslint-disable-line no-control-regex
    this.createTimestamp = packet.readInt32();
    this.eventHeaderLength = packet.readInt8(); // should be 19
    this.eventsLength = packet.readBuffer();
    this.name = this.constructor.name;
  }
}

class QueryEvent {
  constructor(packet) {
    const parseStatusVars = require('../packets/binlog_query_statusvars.js');
    this.slaveProxyId = packet.readInt32();
    this.executionTime = packet.readInt32();
    const schemaLength = packet.readInt8();
    this.errorCode = packet.readInt16();
    const statusVarsLength = packet.readInt16();
    const statusVars = packet.readBuffer(statusVarsLength);
    this.schema = packet.readString(schemaLength);
    packet.readInt8(); // should be zero
    this.statusVars = parseStatusVars(statusVars);
    this.query = packet.readString();
    this.name = this.constructor.name;
  }
}

class XidEvent {
  constructor(packet) {
    //this.binlogVersion = packet.readInt16();
    this.xid = packet.readInt64();
    this.name = this.constructor.name;
  }
}

// See https://mariadb.com/kb/en/table_map_event/
class TableMapEvent {
  constructor(packet) {
    this.tableId = packet.readInt32();
    packet.readInt16(); // two more bytes of tableId
    packet.readInt16();
    const schemaLength = packet.readInt8();
    this.schema = packet.readString(schemaLength);
    packet.readInt8();
    const tableNameLength = packet.readInt8();
    this.tableName = packet.readString(tableNameLength);
    packet.readInt8();
    // columnTypes
    // metadataLength
    // metadata
    // bitsNull
    // more metadata
    this.name = this.constructor.name;
  }
}

class AnnotateRowsEvent {
    constructor(packet) {
        this.name = this.constructor.name;
    }
}

class BinlogCheckpointEvent {
    constructor(packet) {
        this.name = this.constructor.name;
    }
}

class GtidEvent {
    constructor(packet) {
        this.name = this.constructor.name;
    }
}

class GtidListEvent {
    constructor(packet) {
        this.name = this.constructor.name;
    }
}

class WriteRowsEvent {
    constructor(packet) {
        this.tableId = packet.readInt32();
        packet.readInt16();
        this.name = this.constructor.name;
    }
}

class UpdateRowsEvent {
    constructor(packet) {
        this.tableId = packet.readInt32();
        packet.readInt16();
        this.name = this.constructor.name;
    }
}

class DeleteRowsEvent {
    constructor(packet) {
        this.tableId = packet.readInt32();
        packet.readInt16();
        this.name = this.constructor.name;
    }
}

eventParsers[2] = QueryEvent;
eventParsers[4] = RotateEvent;
eventParsers[5] = IntvarEvent;
eventParsers[15] = FormatDescriptionEvent;
eventParsers[16] = XidEvent;
eventParsers[19] = TableMapEvent;
eventParsers[23] = WriteRowsEvent;
eventParsers[24] = UpdateRowsEvent;
eventParsers[25] = DeleteRowsEvent;
eventParsers[30] = WriteRowsEvent;
eventParsers[31] = UpdateRowsEvent;
eventParsers[32] = DeleteRowsEvent;

eventParsers[161] = AnnotateRowsEvent;

eventParsers[161] = BinlogCheckpointEvent;
eventParsers[162] = GtidEvent;
eventParsers[163] = GtidListEvent;

eventParsers[166] = WriteRowsEvent;
eventParsers[167] = UpdateRowsEvent;
eventParsers[168] = DeleteRowsEvent;

module.exports = BinlogDump;
