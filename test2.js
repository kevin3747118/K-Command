
const crypto = require("crypto");

let parm = {'key': ''};


async function genKeys() {
  return new Promise((resolve, reject) => {
    const keyId = crypto.randomBytes(7).toString("hex");
    parm.key = keyId;
    console.log(parm)
    resolve();
  });
}


async function a() {
  return new Promise((resolve, reject) => {
    console.log('1')
    resolve();
  })
}

async function main() {
  while (true) {
    await genKeys()
    await a()
    // console.log('@@')
    // setTimeout(() => {
    //   console.log('waiting...')
    // }, 20000);
  }
}

main()


connection.beginTransaction(function(err) {
  if (err) { throw err; }
  connection.query('INSERT INTO posts SET title=?', title, function (error, results, fields) {
    if (error) {
      return connection.rollback(function() {
        throw error;
      });
    }

    var log = 'Post ' + results.insertId + ' added';

    connection.query('INSERT INTO log SET data=?', log, function (error, results, fields) {
      if (error) {
        return connection.rollback(function() {
          throw error;
        });
      }
      connection.commit(function(err) {
        if (err) {
          return connection.rollback(function() {
            throw err;
          });
        }
        console.log('success!');
      });
    });
  });
});