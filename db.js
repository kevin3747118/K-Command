const mysql = require("mysql");
const config = require("./config.json")

const moduleUtil = exports;

try {
    var conn = mysql.createConnection({
        host: config.DATABASE.HOST,
        port: config.DATABASE.PORT,
        user: config.DATABASE.DB_USER,
        password: config.DATABASE.DB_PWD,
        database: config.DATABASE.DB_NAME,
    });
    conn.connect((err) => {
        if (err) {
            console.log(err);
            return;
        }
    })
} catch (err) {
    console.log(err)
}

moduleUtil.execSQL = function(sqlStr, parms) {
    //parms must be array
    conn.query(sqlStr, parms, (err, rows) => {
        if (err) console.log(err)
        return rows;
    });
    // conn.end();
}

// module.exports = execSQL;