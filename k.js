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

const results = [{ 'CARDTYPE': 1, 'LOCKMODE': 'Normal', 'AREATC': 'NO', 'KEYTC': 'NO', 'ACCESSRULE': 'key+lockplace', 'OUTPUT': ['k0', 'k2'] }]


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
    request.get(config.LOCKAPI["REMOTE"] + config.LOCK['LOCK_ID'] + cmd, (err, response, bodys) => {
        let body = JSON.parse(bodys)
        let count = 0;
        // console.log(body)
        if (!err && body.cmd) {
            if (body.cmd.includes('k') && !body.index) {
                // console.log(element.OUTPUT)
                console.log(body.cmd)
                if (element.OUTPUT.includes(body.cmd)) {
                    console.log('pass !')
                } else {
                    console.log('fail !')
                }
            }
            reqLockApi(element, body.cmd + "?status=ok", kArr)
        } else {
            console.log('No Command to Do !')
        }
    })
    // return await request.get(config.LOCKAPI["REMOTE"] + config.LOCK['LOCK_ID'] + cmd, parms)
}



function sqlStuff(arr) {
    for (let key in arr) {
        if (key != 'OUTPUT') {
            switch (key) {
                case 'CARDTYPE':
                    var cardType = config[key][arr[key]];
                    dbUtil.execSQL(`update ordkeys set keytype=? where _id=?`,
                        [config[key][arr[key]], config.KEYS.KEY_ID])
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
                        [JSON.stringify(TC(config[key][arr[key]])), config.KEYS.KEY_ID]);
                    break;
                case 'ACCESSRULE':
                    accessRule(cardType, config[key][arr[key]])
                    break;
            }
        }
    }
}



function main(results) {
    results.forEach(element => {
        sqlStuff(element);
        reqLockApi(element, 'e0')
    });
}



// function keyTC() {
//     conn.query(`select * from alzk.ordkeys where _id=?`, ['fa000000000001'], (err, rows) => {
//         console.log(rows)
//         conn.end()
//     })
//     // conn.query(`update alzk.ordkeys set keytype = '@@' where _id =` confi)
// }


main(results)






