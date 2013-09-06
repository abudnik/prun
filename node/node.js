var fs = require('fs');

function getBytes( x ) {
    var bytes = new Buffer(4);
    var i = 0;
    do {
        bytes[i++] = x & (255);
        x = x>>8;
    } while ( i < 4 );
    return bytes;
}

var NODE_SCRIPT_EXEC_FAILED = -5;
var errCode = 0;

try {
    var shmemPath = process.argv[3];
    var scriptLen = parseInt( process.argv[4] );
    var shmemOffset = parseInt( process.argv[5] );
    var taskId = process.argv[6];
    var numTasks = process.argv[7];

    var shmem = fs.openSync(shmemPath, "r");

    var buffer = new Buffer(scriptLen);
    fs.readSync(shmem, buffer, shmemOffset, scriptLen, null);

    var data = buffer.toString("utf8", 0, buffer.length);

    var inject = "var taskId=" + taskId + ";\n";
    inject += "var numTasks=" + numTasks + ";\n";

    eval(inject+data);
} catch( e ) {
    errCode = NODE_SCRIPT_EXEC_FAILED;
    console.log(e);
}

try {
    var fifoName = process.argv[2];
    var fifo = fs.openSync(fifoName, "w");
    fs.writeSync( fifo, getBytes( errCode ), 0, 4, null );
    fs.close( fifo );
} catch( e ) {
    console.log(e);
}
