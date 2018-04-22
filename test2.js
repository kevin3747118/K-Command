
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
