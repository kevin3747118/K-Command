const util = require('./db.js')
const async = require('async');
const config = require("./config.json")
// const Promise = require('promise');
const crypto = require("crypto");

/*
1. generate virtual key
2. save key to lock command

const cmdK0 = new LockCommand({
                        lockPlaceId : self._id, 
                        cmd:'k0',
                        sendData : sendData,
                        k0Index : -2,
                        status : cmdStatus
                    })

*/

// function genKeys() {
//     var text = "";
//     var possible = "abcdefghijklmnopqrstuvwxyz0123456789";

//     for (var i = 0; i < 15; i++){
//       text += possible.charAt(Math.floor(Math.random() * possible.length));
//   }
//   return text;
// }

let parm = { 'key': '' }


function genKeys() {
    return new Promise((resolve, reject) => {
        const keyId = crypto.randomBytes(7).toString('hex');
        parm.key = keyId;
        resolve(keyId)
    })
}


function genDates(flag) {
    let date = new Date()
    let dataNow = util.dateToDbStr(date)
    if (flag) {
        return dataNow.replace(date.getFullYear(), date.getFullYear() + flag)
    }
    else return dataNow
}

// status = 3

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


async function sqlStuff() {
    await util.execSQL(`insert into alzk.ordkeys (_id, keytype, keystatus, onetimepass, createdate, expiredate, timecontrol, lastreporttime) 
        values (?, ?, ?, ?, ?, ?, ?, ?)`, [parm.key, 'Tenant', 0, 'N', genDates(), genDates(1), '[]', genDates()])
    await util.execSQL(`insert into alzk.keyareas values (?, ?, ?, ?)`, [config.AREAS.AREA_ID, parm.key, '[]', 'N']);
}


async function initial() {
    let results = await util.execSQL(`select * from alzk.lockcommands where lockplaceid = ? and status != 0 and cmd in ('k2')`, [config.LOCKPLACES.LOCKPLACE_ID])
    results.forEach(element => {
        parm[element.cmd] = element.status
    });
    //while condition
    if (Object.values(parm).indexOf(1) > -1 || Object.values(parm).indexOf(2) > -1) {
        console.log('waiting... ')
        console.log(parm)
        await sleep(4000)
        initial()
    }
    else {
        let delKey = await util.execSQL(`select keyid from alzk.keyareas where areaid = ?`, [config.AREAS.AREA_ID])
        let delParm = []
        if (delKey.length != 0) {
            delKey.forEach(ele => {
                console.log('1')
                delParm.push(ele.keyid)
                util.execSQL(`delete from alzk.ordkeys where _id = ?`, [ele.keyid]);
                util.execSQL(`delete from alzk.keyareas where keyid = ?`, [ele.keyid])
            })
        } else {
            console.log('2')
            await genKeys()
        }

        // console.log(delParm)
        // await util.execSQL(`delete from alzk.ordkeys where _id in (?)`, [delParm])
        // await util.execSQL(`delete from alzk.keyareas where keyid in (?)`, [delParm])
        // console.log('@')
        // await genKeys()
    }

}

//紀錄次數
function record() {

}


async function main() {
    await initial()
    await sqlStuff()
    console.log('done')
}

main()