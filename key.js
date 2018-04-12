const util = require("./db.js");
const async = require("async");
const config = require("./config.json");
const fs = require("fs");
const crypto = require("crypto");

/*
1. generate virtual key
2. save key to lock command

const cmdK0 = new LockCommand({
                        lockPlaceId : self._id, 
                        cmd:"k0",
                        sendData : sendData,
                        k0Index : -2,
                        status : cmdStatus
                    })

*/

let parm = { "key": "", "count": 0 };
let dirname = __dirname;

function genKeys() {
	return new Promise((resolve, reject) => {
		const keyId = crypto.randomBytes(7).toString("hex");
		parm.key = keyId;
		// resolve(keyId)
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
	let count = 0;
	while (count == 0) {
		let results = await util.execSQL(`select * from alzk.lockcommands where lockplaceid = ? and status != 0 and cmd in ("k2")`, [config.LOCKPLACES.LOCKPLACE_ID]);
		results.forEach(element => {
			parm[element.cmd] = element.status;
		});
		if (Object.values(parm).indexOf(1) > -1 || Object.values(parm).indexOf(2) > -1) {
			console.log("waiting...");
			await sleep(5000);
		} else {
			let delKey = await util.execSQL(`select keyid from alzk.keyareas where areaid = ?`, [config.AREAS.AREA_ID]);
			let delParm = [];
			if (delKey.length != 0) {
				delKey.forEach(ele => {
					delParm.push(ele.keyid);
					util.execSQL(`delete from alzk.ordkeys where _id = ?`, [ele.keyid]);
					util.execSQL(`delete from alzk.keyareas where keyid = ?`, [ele.keyid]);
				});
				await genKeys();
				count = 1;
			} else {
				await genKeys();
				count = 1;
			}
		}
	}

}


//record counts
async function record() {
	parm.count += 1;
	fs.writeFile(dirname + "/" + config.RECORD_FILE, parm.count, (err) => {
		if (err) throw err;
	});
}


async function main() {
	for (let i = 0; i <= 4; i++) {
		await initial();
		await sqlStuff();
		await record();
		//
	}
}

main();