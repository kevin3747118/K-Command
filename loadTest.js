const util = require("./util.js");
const async = require("async");
const config = require("./config.json");

// keyStatus:"0"
// keyType:"Tenant"
// oneTimePass:"N"
// ownerCompanyName:""
// ownerEmail:""
// ownerFirstName:""
// ownerLastName:""
// ownerMiddleName:""
// ownerName:""
// ownerPhone:""
// ownerPosition:""
// ownerUserId:""

// keyAreas:Array(4) [Object, Object, Object, …]
// length:4
// __proto__:Array(0) [, …]
// 0:Object {areaId: "100000"}
// 1:Object {areaId: "10000"}
// 2:Object {areaId: "1", timeControl: Array(8)}
// 3:Object {areaId: "3"}

// _id:"045043b2075980"
// expireDate:"2019/04/13 00:00:00"

// this.createDate = alzkUtil.dateToDbStr(new Date());
// this.expireDate = '';
// this.lastReportTime = '';
// this.timeControl = [];


```
2. 初始發 400 張卡，持續更換、新增 guest/staff/vendor/temp/unit 卡，直到上限值，expire date 為發卡時間後的 30 分鐘，強迫 k2
3. 初始發 200 張卡，持續更換、新增 guest/staff/vendor/temp/unit 卡，直到上限值，expire date 為發卡時間後的 30 分鐘，強迫 k2
```

async function genKeyInfo(idArr) {
  // 根據卡別，組合卡別相對應資訊
  return new Promise((resolve, reject) => {
    let keyObj = config.KEYOBJ;
    keyObj._id = util.genRandomKeys();
    keyObj.expireDate = util.dateToDbStr(new Date(), 40);
    keyObj.createDate = util.dateToDbStr(new Date());
    keyObj.keyAreas = [];
    keyObj.timeControl = [];
    idArr.forEach((id) => {
      keyObj.keyAreas.push({'areaId': id['_id'].toString()})
    });
    resolve(keyObj);
  })
}


async function getArea() {
  let results = await util.execSQL(`SELECT _id FROM alzk.areas WHERE RAND() <= 0.046 limit ` + config.AREA_RETURN_LIMIT + `;`);
  return results;
}

// function 隨機選出 guest/staff/vendor/temp/unit 卡


async function main() {
  let idArr = await getArea();
  let keyObj = await genKeyInfo(idArr);
  console.log(keyObj)
}

main()
