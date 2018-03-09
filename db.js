const mysql = require("mysql");
const Promise = require('promise');
const config = require("./config.json")

const moduleUtil = exports;

function getConn() {
    try {
        var conn = mysql.createConnection({
            host: config.DATABASE.REMOTE_HOST,
            port: config.DATABASE.PORT,
            user: config.DATABASE.DB_USER,
            password: config.DATABASE.DB_PWD,
            database: config.DATABASE.DB_NAME,
        })
        return conn;
    } catch (err) {
        console.log(err)
    }
}


moduleUtil.execSQL = async function (sqlStr, parms) {
    //parms must be array
    let conn = await getConn();
    return new Promise((resolve, reject) => {
        conn.query(sqlStr, parms, (err, rows) => {
            if (err) reject(err)
            else resolve(rows)
        })
    })
}


// moduleUtil.execSQL = function(sqlStr, parms, cb) {
//     //parms must be array
//     conn.query(sqlStr, parms, (err, rows) => {
//         if (err) console.log(err)
//         if (cb) {
//             cb(rows);
//         } else {
//             return rows;
//         }
//         // return rows;
//     });
//     // conn.end();
// }

// module.exports = execSQL;