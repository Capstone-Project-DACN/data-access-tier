// services/areaMinioService.js
const { log } = require("console");
const Minio = require("minio");
const stream = require("stream");
const { promisify } = require("util");
const finished = promisify(stream.finished);

const minioClient = new Minio.Client({
  endPoint: process.env.MINIO_ENDPOINT || "13.251.38.153",
  port: parseInt(process.env.MINIO_PORT || "9000"),
  useSSL: process.env.MINIO_USE_SSL === "true",
  accessKey: process.env.MINIO_ACCESS_KEY || "myminioadmin",
  secretKey: process.env.MINIO_SECRET_KEY || "myminioadmin",
});
async function listObject(bucketName, paths) {
  for (const path of paths) {
    const objectsStream = minioClient.listObjects(bucketName, path, false);
    await new Promise((resolve, reject) => {
      objectsStream.on("data", async (obj) => {
        console.log(obj);
      });

      objectsStream.on("end", () => {
        resolve();
      });
    });
  }
}

async function getRawObjectsByPaths(bucketName, paths) {
  const results = [];
  const seenPaths = new Set();

  for (const path of paths) {
    const objectsStream = minioClient.listObjects(bucketName, path, false);
    await new Promise((resolve, reject) => {
      objectsStream.on("data", async (obj) => {
        if (obj.name.endsWith(".json") && !seenPaths.has(obj.name)) {
          try {
            const dataStream = await minioClient.getObject(
              bucketName,
              obj.name
            );
            const chunks = [];
            for await (const chunk of dataStream) {
              chunks.push(chunk);
            }
            const dataBuffer = Buffer.concat(chunks);
            const dataContent = dataBuffer.toString("utf-8");

            results.push({
              id: obj.name,
              data: JSON.parse(dataContent),
            });
            console.log(results)
            seenPaths.add(obj.name);
          } catch (err) {
            console.error(`Error reading object ${obj.name}:`, err.message);
          }
        }
      });

      objectsStream.on("error", (err) => {
        console.error(`Error listing objects with prefix ${path}:`, err);
        // Don't reject as we want to continue with other paths
      });

      objectsStream.on("end", () => {
        resolve();
      });
    });
  }

  // console.log(results)
  return results;
}

async function getObject() {
  const bucketName = "household";
  const obj = { name: "household-HCMC-Q1-0/2025-04-13/6.json" };
  const dataStream = await minioClient.getObject(bucketName, obj.name);
  const chunks = [];
  for await (const chunk of dataStream) {
    chunks.push(chunk);
  }
  const dataBuffer = Buffer.concat(chunks);
  const dataContent = dataBuffer.toString("utf-8");
  console.log(dataContent);
}

// const bucketName = "household";
// const paths = ["household-HCMC-Q1-0/2025-04-13.json"];
// listObject(bucketName, paths);
// getRawObjectsByPaths(bucketName, paths);




// const path= "household-HCMC-Q1-0/2025-04-13/6.json"
// getObject()
// const timeStr="2025-04-13T06:25:30Z"
// const timeStrDate = new Date(timeStr)
// timeStrDate.setUTCHours(0, 0, 0)
// timeStrDate.setHours(23, 59, 59, 999);
// const dateStr = timeStrDate.toISOString().split('T')[0]; // YYYY-MM-DD
// console.log(dateStr);
// console.log(timeStrDate.getTime());


// async function listTopLevelFolders(city) {
//   try {
//     const objectsStream = minioClient.listObjectsV2("ward", "", true);

//     const topLevelFolders = new Set();

//     objectsStream.on("data", (obj) => {
//       if (obj.name.includes("/")) {
//         const folder = obj.name.split("/")[0] + "/";
//         if (folder.includes(`${city}`)) {
//           topLevelFolders.add(folder);
//         }
//       }
//     });

//     objectsStream.on("error", (err) => {
//       console.error("Error listing objects:", err);
//     });

//     objectsStream.on("end", () => {
//       console.log("Top-level folders:", Array.from(topLevelFolders));
//     });
//   } catch (err) {
//     console.error("Error:", err);
//   }
// }
// listTopLevelFolders('HCMC');

async function getFileFromMinIO() {
  // Set up bucket name and object name
  const bucketName = "predict";
  const objectName = "electricity_forecast_q10_jun_dec_2025.csv";
  
  try {
    // Get the object as a stream
    const dataStream = await minioClient.getObject(bucketName, objectName);
    
    // Convert stream to Buffer
    const chunks = [];
    return new Promise((resolve, reject) => {
      dataStream.on('data', (chunk) => {
        chunks.push(chunk);
      });
      
      dataStream.on('end', () => {
        const buffer = Buffer.concat(chunks);
        const csvContent = buffer.toString('utf-8');
        
        // Parse CSV and format data
        const formattedData = parseCSVToRequestedFormat(csvContent);
        resolve(formattedData);
      });
      
      dataStream.on('error', (err) => {
        reject(err);
      });
    });
  } catch (err) {
    console.error("Error retrieving file from MinIO:", err);
    throw err;
  }
}

function parseCSVToRequestedFormat(csvContent) {
  // Split by lines and remove any empty lines
  const lines = csvContent.trim().split('\n');
  
  // Skip header line and parse data rows
  const result = [];
  
  for (let i = 1; i < lines.length; i++) {
    const [date_part, daily_usage] = lines[i].split(',');
    
    const dateObj = new Date(date_part);
    const date_part_utc = dateObj.getTime();
    
    result.push({
      date_part,
      daily_usage: parseFloat(daily_usage),
      date_part_utc
    });
  }
  
  return result;
}

async function main(){
  let res= await getFileFromMinIO()
  console.log(res)
}

main()