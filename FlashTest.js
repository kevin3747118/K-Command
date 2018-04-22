const util = require("./util.js");
const async = require("async");
const config = require("./config.json");
const fs = require("fs");
const crypto = require("crypto");


// const cmdK0 = new LockCommand({
//                         lockPlaceId : self._id, 
//                         cmd:"k0",
//                         sendData : sendData,
//                         k0Index : -2,
//                         status : cmdStatus
//                     })



let parm = { "key": "", "count": 0 };
let mailOptions = {
  from: `"${config.EMAIL.EMAIL_FROM_DESC}" <${config.EMAIL.EMAIL_FROM_ADDR}>`,
  to: `kevin@alzk.com.tw;lintungwei@gmail.com;vincenthpchou@gmail.com;gavin@alzk.com.tw;cavin@alzk.com.tw`,
  subject: ``
};
let start = 0;
let dirname = __dirname;



//record counts
async function record() {
  parm.count += 1;
  fs.writeFile(dirname + "/" + config.RECORD_FILE, parm.count, (err) => {
    if (err) throw err;
  });
  if (parm.count / 3000 === 0) {
    mailOptions.subject = `Flash Memory Write : ${parm.count} times (No Content)`;
    await util.Mailer(mailOptions);
  }
}


async function genKeys() {
  return new Promise((resolve, reject) => {
    const keyId = crypto.randomBytes(7).toString("hex");
    parm.key = keyId;
    console.log(parm)
    resolve();
  });
}



function genDates(flag) {
  let date = new Date();
  let dataNow = util.dateToDbStr(date);
  if (flag) {
    return dataNow.replace(date.getFullYear(), date.getFullYear() + flag);
  }
  else return dataNow;
}



function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}



async function sqlStuff() {
  await util.execSQL(`insert into alzk.ordkeys (_id, keytype, keystatus, onetimepass, createdate, expiredate, timecontrol, lastreporttime) 
        values (?, ?, ?, ?, ?, ?, ?, ?)`, [parm.key, "Tenant", 0, "N", genDates(), genDates(1), "[]", genDates()]);
  await util.execSQL(`insert into alzk.keyareas values (?, ?, ?, ?)`, [config.AREAS.AREA_ID, parm.key, "[]", "N"]);
}



async function initial() {
  return new Promise(async (resolve, reject) => {
    let count = 0;
    let cmd = [];

    // if (start === 0) {
    //   await genKeys();
    //   await sqlStuff();
    //   start = 1;
    // }

    while (count == 0) {
      let results = await util.execSQL(`select * from alzk.lockcommands where lockplaceid = ? and status in (2) and cmd in ("k2")`, [config.LOCKPLACES.LOCKPLACE_ID]);
      if (results.length !== 0) {
        console.log("waiting...");
        await sleep(2000);
      } else {
        count = 1;
      }
    }
    await record();
    let delKey = await util.execSQL(`select keyid from alzk.keyareas where areaid = ?`, [config.AREAS.AREA_ID]);
    let delParm = [];
    if (delKey.length != 0) {
      delKey.forEach(ele => {
        delParm.push(ele.keyid);
        util.execSQL(`delete from alzk.ordkeys where _id = ?`, [ele.keyid]);
        util.execSQL(`delete from alzk.keyareas where keyid = ?`, [ele.keyid]);
      });
    }
    console.log('delete finished')
    resolve()
  })
}


async function main() {
  while(true) {
    try {
      await initial();
      await genKeys();
      await sqlStuff();
      await sleep(9000);
    } catch (err) {
      let mailOptions = {
        from: `"${config.EMAIL.EMAIL_FROM_DESC}" <${config.EMAIL.EMAIL_FROM_ADDR}>`,
        to: `kevin@alzk.com.tw;`,
        subject: err
      };
      await util.Mailer(mailOptions);
    }

  }

  // while (true) {
  //   try {
  //     await initial();
  //     await genKeys();
  //     await sqlStuff();
  //   } catch (err) {
  //     console.log(err)
  //   }
  // }
}

main();