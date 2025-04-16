const Minio = require("minio");

const minioClient = new Minio.Client({
  endPoint: process.env.MINIO_ENDPOINT || 'localhost',
  port: parseInt(process.env.MINIO_PORT || '9000'),
  useSSL: process.env.MINIO_USE_SSL === 'true',
  accessKey: process.env.MINIO_ACCESS_KEY || 'myminioadmin',
  secretKey: process.env.MINIO_SECRET_KEY || 'myminioadmin'
});

function getChartData(deviceId, timeStart, timeEnd, timeSlot) {
  if (!deviceId || !timeStart || !timeEnd || !timeSlot) {
    throw new Error("Missing required parameters");
  }
  // timeStart= new Date(timeStart)
  // timeEnd= new Date(timeEnd)

  //get file path
  const paths = getFilePath(deviceId, timeStart, timeEnd, timeSlot);

  //get objects by path

}

function getFilePath(deviceId, timeStart, timeEnd, timeSlot) {
  timeStart = new Date(timeStart);
  timeEnd = new Date(timeEnd);
  let startDateStr=""
  let endDateStr=""
  const paths = [];

  switch (timeSlot) {
    case "1m":
      endDateStr = timeEnd.toISOString().split("T")[0];
      while (timeStart <= timeEnd) {
        startDateStr = timeStart.toISOString().split("T")[0];
        startHour = timeStart.getUTCHours();
        const isSameDay = startDateStr === endDateStr;
        const endHour = isSameDay ? timeEnd.getUTCHours() : 23;
        for (let h = startHour; h <= endHour; h++) {
          paths.push(`${deviceId}/${startDateStr}/${h}/`);
        }
        timeStart.setUTCDate(timeStart.getUTCDate() + 1);
        timeStart.setUTCHours(0, 0, 0);
      }
      break;

    case "1h":
      endDateStr = timeEnd.toISOString().split("T")[0];
      while (timeStart <= timeEnd) {
        startDateStr = timeStart.toISOString().split("T")[0];
        startHour = timeStart.getUTCHours();
        const isSameDay = startDateStr === endDateStr;
        const endHour = isSameDay ? timeEnd.getUTCHours() : 23;
        for (let h = startHour; h <= endHour; h++) {
          //those Path that not exist in minio will not be listed by listObject
          paths.push(`${deviceId}/${startDateStr}/${h}.json`);
        }
        timeStart.setUTCDate(timeStart.getUTCDate() + 1);
        timeStart.setUTCHours(0, 0, 0);
      }
      break;
    
    case "1d":
      timeStart.setUTCHours(0, 0, 0, 0);
      timeEnd.setUTCHours(0, 0, 0, 0);
      while (timeStart <= timeEnd) {
        startDateStr = timeStart.toISOString().split("T")[0];
        paths.push(`${deviceId}/${startDateStr}.json`);
        timeStart.setUTCDate(timeStart.getUTCDate() + 1);
      }
      break;
  }

  return paths;
}

// console.log(getFilePath("dev001", "2025-04-13T22:00:00Z", "2025-04-14T01:00:00Z", "1m")

// )

// console.log(getFilePath("abc", "2025-04-13T00:00:00Z", "2025-04-13T23:59:59Z", "1m"));
// console.log(getFilePath("sensor9", "2025-04-10T12:00:00Z", "2025-04-12T03:00:00Z", "1m")
// )

async function getRawObjectsByPaths(bucketName, paths) {
  const results = [];
  const seenPaths = new Set();
  
  for (const path of paths) {
    // First, collect all objects that match the path
    const objectsToProcess = [];
    
    const objectsStream = minioClient.listObjects(bucketName, path, false);
    await new Promise((resolve, reject) => {
      objectsStream.on('data', (obj) => {
        if (obj.name.endsWith('.json') && !seenPaths.has(obj.name)) {
          objectsToProcess.push(obj.name);
          seenPaths.add(obj.name);
        }
      });
      
      objectsStream.on('error', (err) => {
        console.error(`Error listing objects with prefix ${path}:`, err);
        resolve(); // Continue with other paths
      });
      
      objectsStream.on('end', () => {
        resolve();
      });
    });
    
    // Then process all collected objects sequentially
    for (const objectName of objectsToProcess) {
      try {
        const dataStream = await minioClient.getObject(bucketName, objectName);
        const chunks = [];
        for await (const chunk of dataStream) {
          chunks.push(chunk);
        }
        const dataBuffer = Buffer.concat(chunks);
        const dataContent = dataBuffer.toString('utf-8');
        
        results.push({
          id: objectName,
          data: JSON.parse(dataContent)
        });
      } catch (err) {
        console.error(`Error reading object ${objectName}:`, err.message);
      }
    }
  }
  console.log(results)
  return results;
}

const paths=getFilePath("household-HCMC-Q1-0","2025-04-12T08:25:30Z","2025-04-12T12:25:30Z",'1h')
const obj=getRawObjectsByPaths('household',paths)
// console.log(obj)
// function extractAndTransformDataFromObjects(objects,timeSlot) {
//   const dataByTimestamp = {};
//   for (const obj of objects) {
//     if (!obj.data.timestamp.endsWith('Z')) {
//       obj.data.timestamp += 'Z';
//     }

//     let timestamp = new Date(obj.data.timestamp) ;

//     switch (timeSlot){
//       case '1m':
//         timestamp.setUTCSeconds(0, 0);
//       case '1h':
//         timestamp.setUTCMinutes(0, 0, 0);
//       case '1d': 
//       timestamp.setUTCHours(0, 0, 0, 0);
//     }
//     const timeKey = timestamp.getTime();
    
//     // Only keep the latest record for each timestamp
//     if (!dataByTimestamp[timeKey]) {
//       dataByTimestamp[timeKey] = {
//         timestamp,
//         value: obj.data.electricity_usage_kwh
//       };
//     }
//   }
//   // console.log(dataByTimestamp)
//   return dataByTimestamp
// }


// function generateTimeRanges(startDate, endDate, timeSlot) {
//   const timeRanges = [];
//   startDate = new Date(startDate) ;
//   endDate = new Date(endDate) ;
//   let currentTime = ""
//   let intervalMs = 0
//   switch (timeSlot){
//     case '1m':
//       startDate.setUTCSeconds(0, 0)
//       endDate.setUTCSeconds(0, 0)
//       currentTime=startDate
//       intervalMs = 60 * 1000
//       while (currentTime <= endDate) {
//         timeRanges.push(currentTime.getTime());
//         currentTime = new Date(currentTime.getTime() + intervalMs);
//       }

//     case '1h':
//       startDate.setUTCMinutes(0, 0, 0)
//       endDate.setUTCMinutes(0, 0, 0)
//       currentTime=startDate
//       intervalMs = 60 * 60 * 1000
//       while (currentTime <= endDate) {
//         timeRanges.push(currentTime.getTime());
//         currentTime = new Date(currentTime.getTime() + intervalMs);
//       }

//     case '1d':
//       startDate.setUTCHours(0, 0, 0, 0)
//       endDate.setUTCHours(0, 0, 0, 0)
//       currentTime=startDate
//       intervalMs = 24 * 60 * 60 * 1000
//       while (currentTime <= endDate) {
//         timeRanges.push(currentTime.getTime());
//         currentTime = new Date(currentTime.getTime() + intervalMs);
//       }
//   }
//   return timeRanges
// }
// function createChartDataPoints(timeRanges, dataByTimestamp) {
//   const chartData = [];
//   let dataPoint ={}
//   timeRanges.forEach(timePoint =>{
//     dataPoint = {
//       x: timePoint,
//       y: dataByTimestamp[timePoint]?.value | 0,
//       x_utc_timestamp: new Date(timePoint)
//     };
//     chartData.push(dataPoint)
//   })
//   return chartData
// }

// async function main(){
//   const startDate="2025-04-11T06:00:00Z"
//   const endDate="2025-04-13T09:00:00Z"
//   const timeSlot= "1d"
//   const paths=getFilePath("household-HCMC-Q1-0",startDate,endDate,timeSlot)
//   const objects= await getRawObjectsByPaths('household',paths)
//   const dataByTimestamps = extractAndTransformDataFromObjects(objects,timeSlot)
//   const timeRanges = generateTimeRanges(startDate,endDate,timeSlot)
//   const chartData= createChartDataPoints(timeRanges,dataByTimestamps)
//   console.log(chartData)
//   return chartData

//   //=======

//   // const paths=getFilePath("household-HCMC-Q1-0","2025-04-12T05:00:00Z","2025-04-13T09:00:00Z",'1d')
//   // // console.log(paths)
//   // const bucketName = "household";
//   // // const paths = ["household-HCMC-Q1-0/2025-04-13.json"];
//   // const obj=await getRawObjectsByPaths(bucketName,paths)
//   // console.log(obj)
// }
// // main()
// console.log(main())

// console.log(generateTimeRanges("2025-04-13T08:00:26.687Z", "2025-04-13T08:03:02.000Z", "1m"))
