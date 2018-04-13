const mysql = require("mysql");
const Promise = require("promise");
const config = require("./config.json");
const crypto = require("crypto");
const request = require("request");

const moduleUtil = exports;

function getConn() {
  try {
    var conn = mysql.createConnection({
      host: config.DATABASE.REMOTE_HOST,
      port: config.DATABASE.PORT,
      user: config.DATABASE.DB_USER,
      password: config.DATABASE.DB_PWD,
      database: config.DATABASE.DB_NAME,
    });
    return conn;
  } catch (err) {
    console.log(err);
  }
}


moduleUtil.execSQL = async function (sqlStr, parms) {
  //parms must be array
  let conn = await getConn();
  return new Promise((resolve, reject) => {
    conn.query(sqlStr, parms, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
};


moduleUtil.dateToDbStr = function (d, mins) {
  if (mins > 0) d.setMinutes(d.getMinutes() + mins);
  return String("0000" + d.getFullYear()).slice(-4) + "/" +
    String("00" + (d.getMonth() + 1)).slice(-2) + "/" +
    String("00" + d.getDate()).slice(-2) + " " +
    String("00" + d.getHours()).slice(-2) + ":" +
    String("00" + d.getMinutes()).slice(-2) + ":" +
    String("00" + d.getSeconds()).slice(-2);
};


moduleUtil.genRandomKeys = function () {
  const keyId = crypto.randomBytes(7).toString("hex");
  return keyId;
}


module.reqAPI = function () {
  return new Promise((resolve, reject) => {
    let options = {
      url: config.LOCKAPI["REMOTE"] + config.LOCK["LOCK_ID"] + cmd,
      agentOptions: {
        rejectUnauthorized: false
      }
    };
    request.get(options, (err, res, bodys) => {
      console.log(bodys)
    })
  })
}




