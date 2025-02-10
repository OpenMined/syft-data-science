import pkg from "./compiled.js";
const { Addition } = pkg;

// example code
// Addition = root.lookupType("Addition");
let message = Addition.create({ a: 1, b: 2 });
console.log(message)
let buffer  = Addition.encode(message).finish();
console.log(buffer)
let decoded = Addition.decode(buffer);
console.log(decoded); // Addition { a: 1, b: 2 }