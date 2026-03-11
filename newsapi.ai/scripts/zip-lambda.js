const fs = require("fs");
const path = require("path");
const archiver = require("archiver");

const file = process.argv[2];
const zipName = process.argv[3];

if (!file || !zipName) {
  console.error("Usage: node zip-lambda.js <file> <zipname>");
  process.exit(1);
}

const outputPath = path.resolve(zipName);
const inputPath = path.resolve(file);

const output = fs.createWriteStream(outputPath);
const archive = archiver("zip", { zlib: { level: 9 } });

output.on("close", () => {
  console.log(`Created ${zipName} (${archive.pointer()} bytes)`);
});

archive.on("error", (err) => {
  throw err;
});

archive.pipe(output);
archive.file(inputPath, { name: path.basename(inputPath) });

archive.finalize();