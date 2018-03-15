const dbUtil = require('./db.js')
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

function genKeys() {
    return new Promise((resolve, reject) => {
        const keyId = crypto.randomBytes(7).toString('hex');
        resolve(keyId)
    })
}

async function main() {
   let a = await genKeys()
   console.log(a)
}

main()