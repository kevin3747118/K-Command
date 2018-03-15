const dbUtil = require('./db.js')
const async = require('async');
const fs = require('fs');
const config = require("./config.json")
const request = require("request")

/*
1. {cardType: {tenant: 1, staff: 2, vendor: 3, guest: 4, temp: 5, master: 6}}
2. {areaTC: {no: 0, yes: 1}}
3. {keyTC: {no: 0, yes: 1}}
4. {accessRule: {key+lockplace: 0, key: 1, lockplace: 2}}
5. {k0: {no: 0, yes: 1}}
6. {k1: {no: 0, yes: 1}}
issue date改成前一天
*/

const results = [{
    'CARDTYPE': 'Tenant', 'LOCKMODE': 'Normal', 'AREATC': 'NO',
    'KEYTC': 'NO', 'ACCESSRULE': 'key+lockplace',
    'EXPECT_CMD': { 'k1': 0 },
    'REAL_CMD': [],
    'RESULT': '',
    'ERROR_MSG': ''
}]

function dateToDbStr(d) {
    return String('0000' + d.getFullYear()).slice(-4) + "/" +
        String('00' + (d.getMonth() + 1)).slice(-2) + "/" +
        String('00' + d.getDate()).slice(-2) + " " +
        String('00' + d.getHours()).slice(-2) + ":" +
        String('00' + d.getMinutes()).slice(-2) + ":" +
        String('00' + d.getSeconds()).slice(-2);
}


function TC(flag) {
    let date = new Date();
    let dateHours = date.getHours();
    let dateDay = date.getDay();
    const tcData = []

    if (flag == 1) {
        for (var i = 1; i < 8; ++i) {
            var tcArr = new Array(24).fill(0);
            if (i == dateDay) {
                tcArr[dateHours] = 1;
            }
            tcData.push(tcArr);
        }
    }
    return tcData
}



function accessRule(cardType, status) {
    let readFile = JSON.parse(fs.readFileSync(config.LOCKPARAMETERS_FILE))
    readFile.ACCESS_RULE['Unit'][cardType] = status;
    fs.writeFileSync(config.LOCKPARAMETERS_FILE, JSON.stringify(readFile, null, '\t'))
}



function reqLockApi(element, cmd, kArr) {
    return new Promise((resolve, reject) => {
        request(config.LOCKAPI["REMOTE"] + config.LOCK['LOCK_ID'] + cmd, (err, res, bodys) => {
            let body = JSON.parse(bodys)
            // console.log(body)
            if (!err && body.cmd) {
                if (body.cmd.includes('k') && !body.index) {
                    element.REAL_CMD.push(body.cmd)
                    if (element.EXPECT_CMD.hasOwnProperty(body.cmd)) {
                        element.EXPECT_CMD[body.cmd] = 1;
                    }
                    // if (!element.EXPECT_CMD.hasOwnProperty(body.cmd)) {
                    //     //test case EXPECT_CMD is different from admin server
                    //     element.RESULT = 'FAIL';
                    //     element.ERROR_MSG = 'Expect command is different from admin server sent'
                    // }
                }
                resolve(reqLockApi(element, body.cmd + "?status=ok", kArr))
            } else {
                resolve('No Command to Do !')
            }
        })
    })
}



async function initial() {
    let results = await dbUtil.execSQL(`select * from alzk.lockplaces where areaid=?`, [config.AREAS.AREA_ID]);
    let lastkeyids = results[0].lastkeyids.replace(`"` + config.KEYS.KEY_ID2 + `", `, ``);
    let lastdelids = `[]`;
    let lastinsids = `[]`;
    await dbUtil.execSQL(`update alzk.lockplaces set lastkeyids=?, lastdelids=?, lastinsids=? where areaid=?`,
        [lastkeyids, lastdelids, lastinsids, config.AREAS.AREA_ID])
}



function sqlStuff(arr) {
    for (let key in arr) {
        switch (key) {
            case 'CARDTYPE':
                // var cardType = config[key][arr[key]];
                var cardType = arr['CARDTYPE'];
                dbUtil.execSQL(`update ordkeys set keytype=? where _id=?`,
                    [cardType, config.KEYS.KEY_ID2])
                break;
            case 'LOCKMODE':
                config.LOCKSETTINGS['w0']['mode'] = config[key][arr[key]];
                dbUtil.execSQL(`update lockplaces set locksettings=? where _id=?`,
                    [JSON.stringify(config.LOCKSETTINGS), config.AREAS.AREA_ID])
                break;
            case 'AREATC':
                dbUtil.execSQL(`update areas set timecontrol=? where _id=?`,
                    [JSON.stringify(TC(config[key][arr[key]])), config.AREAS.AREA_ID]);
                break;
            case 'KEYTC':
                dbUtil.execSQL(`update keyareas set timecontrol=? where keyid=?`,
                    [JSON.stringify(TC(config[key][arr[key]])), config.KEYS.KEY_ID2]);
                break;
            case 'ACCESSRULE':
                accessRule(cardType, config[key][arr[key]])
                break;
        }
    }
}


async function main(results) {
    await initial();
    results.forEach(async (ele) => {
        await sqlStuff(ele)
        await reqLockApi(ele, 'e0')
        if (Object.values(ele.EXPECT_CMD).includes(0) || ele.REAL_CMD.length != Object.keys(ele.EXPECT_CMD).length) {
            ele.RESULT = 'FAIL';
            ele.ERROR_MSG = 'Expected command is different from admin server sent'
        }
        else {
            ele.RESULT = 'PASS'
        }
        let dbParms = [ele.CARDTYPE, ele.LOCKMODE, ele.AREATC, ele.KEYTC,
        ele.ACCESSRULE, JSON.stringify(Object.keys(ele.EXPECT_CMD)),
        JSON.stringify(ele.REAL_CMD), ele.RESULT, ele.ERROR_MSG, dateToDbStr(new Date())]

        dbUtil.execSQL(`insert into cmd_test.test_results (cardtype, lockmode, areatc,
                        keytc, accessrule, expectcmd, realcmd, testresult, errormsg, date)
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, dbParms)
        console.log(dbParms)
    });
}

main(results)


// recursive without promise&async
// function reqLockApi(element, cmd, kArr) {
//     request.get(config.LOCKAPI["REMOTE"] + config.LOCK['LOCK_ID'] + cmd, (err, response, bodys) => {
//         let body = JSON.parse(bodys)
//         if (!err && body.cmd) {
//             if (body.cmd.includes('k') && !body.index) {
//                 // console.log(element.EXPECT_CMD)
//                 console.log(body.cmd)
//                 if (element.EXPECT_CMD.hasOwnProperty(body.cmd)) {
//                     element.EXPECT_CMD[body.cmd] = 1;
//                 } else if (!element.EXPECT_CMD.hasOwnProperty(body.cmd)) {
//                     console.log('fail 1!')
//                 } else {
//                     console.log('fail 2!')
//                 }
//             }
//             reqLockApi(element, body.cmd + "?status=ok", kArr)
//         } else {
//             console.log('No Command to Do !')
//         }   
//     })
// }



