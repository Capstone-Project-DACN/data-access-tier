const Minio = require("minio");

const minioClient = new Minio.Client({
  endPoint: process.env.MINIO_ENDPOINT || "localhost",
  port: parseInt(process.env.MINIO_PORT || "9000"),
  useSSL: process.env.MINIO_USE_SSL === "true",
  accessKey: process.env.MINIO_ACCESS_KEY || "myminioadmin",
  secretKey: process.env.MINIO_SECRET_KEY || "myminioadmin",
});

function getFilePaths(deviceId, timeStart, timeEnd, timeSlot) {
  timeStart = new Date(timeStart);
  timeEnd = new Date(timeEnd);
  let startDateStr = "";
  let endDateStr = "";
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

async function getRawObjectsByPaths(bucket, paths) {
  const results = [];
  const seenPaths = new Set();

  for (const path of paths) {
    // First, collect all objects that match the path
    const objectsToProcess = [];

    const objectsStream = minioClient.listObjects(bucket, path, false);
    await new Promise((resolve, reject) => {
      objectsStream.on("data", (obj) => {
        if (obj.name.endsWith(".json") && !seenPaths.has(obj.name)) {
          objectsToProcess.push(obj.name);
          seenPaths.add(obj.name);
        }
      });

      objectsStream.on("error", (err) => {
        console.error(`Error listing objects with prefix ${path}:`, err);
        resolve(); // Continue with other paths
      });

      objectsStream.on("end", () => {
        resolve();
      });
    });

    // Then process all collected objects sequentially
    for (const objectName of objectsToProcess) {
      try {
        const dataStream = await minioClient.getObject(bucket, objectName);
        const chunks = [];
        for await (const chunk of dataStream) {
          chunks.push(chunk);
        }
        const dataBuffer = Buffer.concat(chunks);
        const dataContent = dataBuffer.toString("utf-8");

        results.push({
          id: objectName,
          data: JSON.parse(dataContent),
        });
      } catch (err) {
        console.error(`Error reading object ${objectName}:`, err.message);
      }
    }
  }

  return results;
}

function extractAndTransformDataFromObjects(objects, timeSlot) {
  const dataByTimestamp = {};
  for (const obj of objects) {
    if (!obj.data.timestamp.endsWith("Z")) {
      obj.data.timestamp += "Z";
    }

    let timestamp = new Date(obj.data.timestamp);

    switch (timeSlot) {
      case "1m":
        timestamp.setUTCSeconds(0, 0);
        break;
      case "1h":
        timestamp.setUTCMinutes(0, 0, 0);
        break;

      case "1d":
        timestamp.setUTCHours(0, 0, 0, 0);
        break;
    }
    const timeKey = timestamp.getTime();

    if (!dataByTimestamp[timeKey]) {
      dataByTimestamp[timeKey] = {
        timestamp,
        value:
          obj.data.type == "HouseholdData"
            ? {
                electricity_usage: obj.data.electricity_usage_kwh,
                voltage: obj.data.voltage,
                current: obj.data.current,
              }
            : {
                electricity_usage: obj.data.total_electricity_usage_kwh,
              },
      };
    }
  }
  return dataByTimestamp;
}

function generateTimeRanges(timeStart, timeEnd, timeSlot) {
  const timeRanges = [];
  timeStart = new Date(timeStart);
  timeEnd = new Date(timeEnd);
  let currentTime = "";
  let intervalMs = 0;
  switch (timeSlot) {
    case "1m":
      timeStart.setUTCSeconds(0, 0);
      timeEnd.setUTCSeconds(0, 0);
      currentTime = timeStart;
      intervalMs = 60 * 1000;
      while (currentTime <= timeEnd) {
        timeRanges.push(currentTime.getTime());
        currentTime = new Date(currentTime.getTime() + intervalMs);
      }
      break;

    case "1h":
      timeStart.setUTCMinutes(0, 0, 0);
      timeEnd.setUTCMinutes(0, 0, 0);
      currentTime = timeStart;
      intervalMs = 60 * 60 * 1000;
      while (currentTime <= timeEnd) {
        timeRanges.push(currentTime.getTime());
        currentTime = new Date(currentTime.getTime() + intervalMs);
      }
      break;

    case "1d":
      timeStart.setUTCHours(0, 0, 0, 0);
      timeEnd.setUTCHours(0, 0, 0, 0);
      currentTime = timeStart;
      intervalMs = 24 * 60 * 60 * 1000;
      while (currentTime <= timeEnd) {
        timeRanges.push(currentTime.getTime());
        currentTime = new Date(currentTime.getTime() + intervalMs);
      }
      break;
  }
  return timeRanges;
}
function createChartDataPoints(timeRanges, dataByTimestamp, sortOrder) {
  const chartData = [];
  let dataPoint = {};
  timeRanges.forEach((timePoint) => {
    const dataValue = dataByTimestamp[timePoint]?.value;

    // Start with the common properties
    const dataPoint = {
      x: timePoint,
      x_utc_timestamp: new Date(timePoint),
      electricity_usage: dataValue?.electricity_usage || 0,
    };

    // If it's HouseholdData, add the additional fields
    if (dataValue && dataValue.voltage !== undefined) {
      dataPoint.voltage = dataValue.voltage;
      dataPoint.current = dataValue.current;
    }

    chartData.push(dataPoint);
  });

  if (sortOrder == -1) {
    return chartData.sort((a, b) => {
      return b.x - a.x;
    });
  }

  return chartData;
}

exports.getChartData = async (
  bucket,
  deviceId,
  timeStart,
  timeEnd,
  timeSlot,
  sortOrder
) => {
  try {
    // Validate parameters
    if (!deviceId || !timeStart || !timeEnd || !timeSlot) {
      throw new Error("Missing required parameters");
    }
    const paths = getFilePaths(deviceId, timeStart, timeEnd, timeSlot);

    // Get objects that match our paths
    const objects = await getRawObjectsByPaths(bucket, paths);

    // Extract and organize data by timestamp
    const dataByTimestamp = extractAndTransformDataFromObjects(
      objects,
      timeSlot
    );

    // Generate time slots
    const timeRanges = generateTimeRanges(timeStart, timeEnd, timeSlot);

    // Create chart data points
    const chartData = createChartDataPoints(
      timeRanges,
      dataByTimestamp,
      sortOrder
    );

    return {
      device_id: deviceId,
      data: chartData,
    };
  } catch (err) {
    console.error("Error getting chart data:", err);
    throw err;
  }
};

async function listTopLevelFolders(city) {
  try {
    const objectsStream = minioClient.listObjectsV2("ward", "", true);

    const topLevelFolders = new Set();

    
    await new Promise((resolve, reject) => {
      objectsStream.on("data", (obj) => {
        if (obj.name.includes("/")) {
          const folder = obj.name.split("/")[0];
          if (folder.includes(`${city}`)) {
            topLevelFolders.add(folder);
          }
        }
      });
  
      objectsStream.on("error", (err) => {
        console.error("Error listing objects:", err);
        resolve()
      });
  
      objectsStream.on("end", () => {
        resolve()
      });
    });
    
    return topLevelFolders
  } catch (err) {
    console.error("Error:", err);
  }
}
function getElectricityUsage(dataByTimestamp) {
  // If there's no data or only one entry, we can't calculate usage
  const timeKeys = Object.keys(dataByTimestamp);
  if (timeKeys.length <= 1) {
    return {
      usage:0,
      reason: "have data points <= 1"
    }
  }

  // Sort the timestamps
  const sortedTimeKeys = timeKeys.map(Number).sort((a, b) => a - b);
  
  // Get the oldest and newest readings
  const oldestKey = sortedTimeKeys[0];
  const newestKey = sortedTimeKeys[sortedTimeKeys.length - 1];
  
  const oldestReading = dataByTimestamp[oldestKey];
  const newestReading = dataByTimestamp[newestKey];
  
  // Calculate the difference in electricity usage
  const startValue = oldestReading.value.electricity_usage;
  const endValue = newestReading.value.electricity_usage;
  const usage = endValue - startValue;
  
  return {
    usage: usage,
    startValue: startValue,
    endValue: endValue,
    startTime: oldestReading.timestamp,
    endTime: newestReading.timestamp,
    start_utc: new Date(oldestReading.timestamp),
    end_utc: new Date(newestReading.timestamp)
  };
}

exports.getChartCityUsage = async (city,timeStart,timeEnd) => {
  const allDeviceInCity = await listTopLevelFolders(city)
  const timeSlot='1d'
  const bucket="ward"
  const districtUsageElectricity=[]
  for (const device of allDeviceInCity){
    const paths=getFilePaths( device, timeStart,timeEnd,timeSlot)
    const objects= await getRawObjectsByPaths(bucket,paths)
    const dataByTimestamp = extractAndTransformDataFromObjects(
      objects,
      timeSlot
    );
    const usage =getElectricityUsage(dataByTimestamp)
    const district = device.split("-")[2]
    districtUsageElectricity.push({
      district,
      ...usage
    })
  }
  return districtUsageElectricity
};
