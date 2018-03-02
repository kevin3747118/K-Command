// [4,2,3]
// [4,2,1]

var checkPossibility = function (nums) {
    let nums2 = nums;
    let status
    let status2 = true;
    for (let i = 0; i < nums.length; i++) {
        if (nums[i] >= nums[i + 1]) {
            nums[i] = nums[i + 1] - 1;
            status = false;
            break;
        }
    };
    if (!status) {
        for (let i = 0; i < nums.length; i++) {
            if (nums[i] >= nums[i + 1]) {
                status2 = false;
                return false;
            } 
        };
    } else {
        return true
    }
    if (status2) return true
};


a = [1,3,2]

checkPossibility(a)

// class Polygon {
//     constructor(height) {
//         this.height = height;
//         // this.width = width;
//     }
//     // Getter
//     set setHeight(height) {
//         this.height = height;
//     }
//     get getHeight() {
//         return this.height;
//     }
//     // get area() {
//     //     return this.calcArea();
//     // }
//     // // Method
//     // calcArea() {
//     //     return this.height * this.width;
//     // }
// }

// // let square = new Polygon(13, 10);
// let sq = new Polygon('@@')

// sq.setHeight = 55
// console.log(sq.getHeight)
// // console.log(square.height); //100
