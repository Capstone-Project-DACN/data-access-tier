/**
 * Parse time string into Date object and components
 * @param {string} timeStr - Time string in format 'YYYY-MM-DD-HH-MM-SS' or ISO format
 * @returns {Object} - Object with parsed Date and components
 */
const Minio = require("minio");

const minioClient = new Minio.Client({
  endPoint: process.env.MINIO_ENDPOINT || 'localhost',
  port: parseInt(process.env.MINIO_PORT || '9000'),
  useSSL: process.env.MINIO_USE_SSL === 'true',
  accessKey: process.env.MINIO_ACCESS_KEY || 'myminioadmin',
  secretKey: process.env.MINIO_SECRET_KEY || 'myminioadmin'
});


function parseTimeString(timeStr) {
  // Handle different formats: 2025-12-01-16-00-00 or 2025-12-01T16:00:00
  let year, month, day, hour, minute, second;
  let date;

    // Format: 2025-12-01T16:00:00
    try {
      date = new Date(timeStr);
      if (isNaN(date.getTime())) {
        throw new Error("Invalid date");
      }
      year = date.getFullYear();
      month = date.getMonth();
      day = date.getDate();
      hour = date.getHours();
      minute = date.getMinutes();
      second = date.getSeconds();
    } catch (e) {
      throw new Error(`Invalid time format: ${timeStr}`);
    }

  if (!date || isNaN(date.getTime())) {
    throw new Error(`Invalid time format: ${timeStr}`);
  }

  return {
    date,
    components: {
      year,
      month: month + 1, // Convert back to 1-indexed for path construction
      day,
      hour,
      minute,
      second
    },
    formatted: {
      date: `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`,
      hour: String(hour).padStart(2, '0'),
      minute: String(minute).padStart(2, '0')
    }
  };
}
  /**
   * Calculate interval in milliseconds based on time slot
   * @param {string} timeSlot - Time slot interval ('1m', '1h', '1d', '1M')
   * @returns {Object} - Interval info with milliseconds and type
   */
  function calculateIntervalInfo(timeSlot) {
    const info = {
      ms: 0,
      type: ''
    };
    
    switch (timeSlot) {
      case '1m':
        info.ms = 60 * 1000; // 1 minute
        info.type = 'minute';
        break;
      case '1h':
        info.ms = 60 * 60 * 1000; // 1 hour
        info.type = 'hour';
        break;
      case '1d':
        info.ms = 24 * 60 * 60 * 1000; // 1 day
        info.type = 'day';
        break;
      case '1M': 
        // Approximate month (30 days)
        info.ms = 30 * 24 * 60 * 60 * 1000;
        info.type = 'month';
        break;
      default:
        throw new Error(`Unsupported time slot: ${timeSlot}`);
    }
    
    return info;
  }
  
  /**
   * Get the appropriate file paths based on time range and interval
   * @param {string} deviceId - Device ID
   * @param {Object} startTime - Parsed start time
   * @param {Object} endTime - Parsed end time
   * @param {string} intervalType - Type of interval ('minute', 'hour', 'day', 'month')
   * @returns {Array} - Array of file path patterns to match, if minutes, it return path by hours
   */
  //household-HCMC-Q1-0/2025-04-13/6 , household-HCMC-Q1-0/2025-04-13/7
  function getFilePaths(deviceId, startTime, endTime, intervalType) {
    const paths = [];
    
    // If we're looking at minute granularity, use minute-level files
    if (intervalType === 'minute') {
      // Handle date by date
      let currentDate = new Date(startTime.date);
      const endDate = new Date(endTime.date);
      
      // Set time to midnight for day-by-day iteration
      currentDate.setHours(0, 0, 0, 0);
      endDate.setHours(23, 59, 59, 999);
      
      while (currentDate <= endDate) {
        const dateStr = currentDate.toISOString().split('T')[0]; // YYYY-MM-DD
        
        // For minutes, we need to look at each hour's folder
        if (currentDate.getTime() === startTime.date.setHours(0,0,0,0)) {
          // On the start day, begin from the start hour
          const startHour = startTime.components.hour;
          for (let h = startHour; h < 24; h++) {
            paths.push(`${deviceId}/${dateStr}/${h}`);
          }
        } else if (currentDate.getTime() === endTime.date.setHours(0,0,0,0)) {
          // On the end day, only go up to the end hour
          const endHour = endTime.components.hour;
          for (let h = 0; h <= endHour; h++) {
            paths.push(`${deviceId}/${dateStr}/${h}`);
          }
        } else {
          // For days in between, include all hours
          for (let h = 0; h < 24; h++) {
            paths.push(`${deviceId}/${dateStr}/${h}`);
          }
        }
        
        // Move to next day
        currentDate.setDate(currentDate.getDate() + 1);
      }
    } 
    // If we're looking at hour granularity or larger, use hour or day-level files
    else if (intervalType === 'hour') {
      // For hour granularity, use hour-level files
      let currentDate = new Date(startTime.date);
      const endDate = new Date(endTime.date);
      
      // Set time to midnight for day-by-day iteration
      currentDate.setHours(0, 0, 0, 0);
      endDate.setHours(23, 59, 59, 999);
      
      while (currentDate <= endDate) {
        const dateStr = currentDate.toISOString().split('T')[0]; // YYYY-MM-DD
        // Include the date path to get hour files (deviceId/date/*.json)
        paths.push(`${deviceId}/${dateStr}`);
        
        // Move to next day
        currentDate.setDate(currentDate.getDate() + 1);
      }
    }
    // For day or month granularity, use day-level files
    else {
      // For coarser granularity, just list the day-level files
      let currentDate = new Date(startTime.date);
      const endDate = new Date(endTime.date);
      
      // Set time to midnight for day-by-day iteration
      currentDate.setHours(0, 0, 0, 0);
      endDate.setHours(23, 59, 59, 999);
      
      // Get all the date-level files in the range
      while (currentDate <= endDate) {
        const dateStr = currentDate.toISOString().split('T')[0]; // YYYY-MM-DD
        paths.push(`${deviceId}/${dateStr}.json`);
        
        // Move to next day
        currentDate.setDate(currentDate.getDate() + 1);
      }
    }
    
    return paths;
  }
  
  /**
   * Get objects from MinIO that match the given paths
   * @param {Object} minioClient - MinIO client
   * @param {string} bucketName - Bucket name
   * @param {Array} paths - Array of path patterns to fetch
   * @returns {Promise<Array>} - Array of objects with data
   */
  async function getObjectsByPaths( bucketName, paths) {
    const results = [];
    const seenPaths = new Set(); // To avoid duplicate processing
    
    for (const path of paths) {
      try {
        // Check if this is a direct file path or a prefix
        if (path.endsWith('.json')) {
          // Direct file path
          try {
            // Check if file exists
            await minioClient.statObject(bucketName, path);
            
            // Get the file
            if (!seenPaths.has(path)) {
              const dataStream = await minioClient.getObject(bucketName, path);
              const chunks = [];
              for await (const chunk of dataStream) {
                chunks.push(chunk);
              }
              const dataBuffer = Buffer.concat(chunks);
              const dataContent = dataBuffer.toString('utf-8');
              
              results.push({
                id: path,
                data: dataContent
              });
              
              seenPaths.add(path);
            }
          } catch (err) {
            // File might not exist, which is okay
            console.log(`File not found or error: ${path}`, err.message);
          }
        } else {
          // Prefix - list objects with this prefix
          const objectsStream = minioClient.listObjects(bucketName, path, false);
          
          await new Promise((resolve, reject) => {
            objectsStream.on('data', async (obj) => {
              // Only process JSON files and avoid duplicates
              if (obj.name.endsWith('.json') && !seenPaths.has(obj.name)) {
                try {
                  const dataStream = await minioClient.getObject(bucketName, obj.name);
                  const chunks = [];
                  for await (const chunk of dataStream) {
                    chunks.push(chunk);
                  }
                  const dataBuffer = Buffer.concat(chunks);
                  const dataContent = dataBuffer.toString('utf-8');
                  
                  results.push({
                    id: obj.name,
                    data: dataContent
                  });
                  
                  seenPaths.add(obj.name);
                } catch (err) {
                  console.error(`Error reading object ${obj.name}:`, err.message);
                }
              }
            });
            
            objectsStream.on('error', (err) => {
              console.error(`Error listing objects with prefix ${path}:`, err);
              // Don't reject as we want to continue with other paths
            });
            
            objectsStream.on('end', () => {
              resolve();
            });
          });
        }
      } catch (err) {
        console.error(`Error processing path ${path}:`, err);
        // Continue with other paths
      }
    }
    
    return results;
  }
  
  /**
   * Extract data from objects based on time range
   * @param {Array} objects - MinIO objects with data
   * @param {Date} startDate - Start date
   * @param {Date} endDate - End date
   * @returns {Object} - Data organized by timestamp
   */
  function extractDataFromObjects(objects, startDate, endDate) {
    const dataByTimestamp = {};
    
    for (const obj of objects) {
      try {
        if (!obj.data || typeof obj.data !== 'string' || obj.data.trim() === '') {
          continue;
        }
        
        // Parse JSON
        let data;
        try {
          data = JSON.parse(obj.data);
        } catch (e) {
          console.error(`Failed to parse JSON from ${obj.id}:`, e.message);
          continue;
        }
        
        // Handle single object or array
        const records = Array.isArray(data) ? data : [data];
        
        for (const record of records) {
          let timestamp;
          
          // Extract timestamp
          if (record.timestamp) {
            timestamp = new Date(record.timestamp);
          } else if (record.formatted_timestamp) {
            if (record.formatted_timestamp.includes(' ')) {
              // Format: "2025-04-13 08:36:53"
              timestamp = new Date(record.formatted_timestamp.replace(' ', 'T'));
            } else {
              // Try other formats
              timestamp = new Date(record.formatted_timestamp);
            }
          } else {
            // Try to extract from file path: household/device-id/YYYY-MM-DD/HH/MM.json
            const parts = obj.id.split('/');
            const dateIndex = parts.findIndex(part => /^\d{4}-\d{2}-\d{2}$/.test(part));
            
            if (dateIndex >= 0) {
              const dateStr = parts[dateIndex];
              const hour = dateIndex + 1 < parts.length ? parts[dateIndex + 1] : '00';
              const minuteMatch = dateIndex + 2 < parts.length ? parts[dateIndex + 2].match(/^(\d+)\.json$/) : null;
              const minute = minuteMatch ? minuteMatch[1] : '00';
              
              timestamp = new Date(`${dateStr}T${hour.padStart(2, '0')}:${minute.padStart(2, '0')}:00`);
            }
          }
          
          if (!timestamp) {
            console.warn(`Could not extract timestamp from record in ${obj.id}`);
            continue;
          }
          
          // Check if it's within our time range
          if (timestamp >= startDate && timestamp <= endDate) {
            const timeKey = timestamp.getTime();
            
            // Only keep the latest record for each timestamp
            if (!dataByTimestamp[timeKey] || 
                (dataByTimestamp[timeKey].timestamp < timestamp)) {
              dataByTimestamp[timeKey] = {
                timestamp,
                value: record.electricity_usage_kwh
              };
            }
          }
        }
      } catch (err) {
        console.error(`Error processing object ${obj.id}:`, err);
      }
    }
    
    return dataByTimestamp;
  }
  

  //[1744525530000,1744525531000,1744525532000,...] 1m
  /**
   * Generate time slots between start and end time
   * @param {Date} startDate - Start date
   * @param {Date} endDate - End date
   * @param {number} intervalMs - Interval in milliseconds
   * @returns {Array} - Array of Date objects representing time slots
   */
  function generateTimeSlots(startDate, endDate, intervalMs) {
    const timeSlots = [];
    let currentTime = new Date(startDate);
    
    while (currentTime <= endDate) {
      timeSlots.push(new Date(currentTime));
      currentTime = new Date(currentTime.getTime() + intervalMs);
    }
    
    return timeSlots;
  }
  
  /**
   * Create chart data points from time slots and data
   * @param {Array} timeSlots - Array of time slot Date objects
   * @param {Object} dataByTimestamp - Data organized by timestamp
   * @returns {Array} - Formatted chart data points
   */
  function createChartDataPoints(timeSlots, dataByTimestamp) {
    const chartData = [];
    let lastValue = null;
  
    // Convert dataByTimestamp keys to numbers and sort them
    const dataTimeKeys = Object.keys(dataByTimestamp).map(Number).sort((a, b) => a - b);
  
    timeSlots.forEach(slotTime => {
      const slotKey = slotTime.getTime();
      
      // Find the closest data point before or at this time slot
      // Binary search would be ideal, but for simplicity use a linear search
      let closestData = null;
      let closestTime = 0;
      
      for (const timeKey of dataTimeKeys) {
        if (timeKey <= slotKey && timeKey > closestTime) {
          closestTime = timeKey;
          closestData = dataByTimestamp[timeKey];
        } else if (timeKey > slotKey) {
          // Since the array is sorted, we can break once we go past our slot
          break;
        }
      }
  
      // If no data found for this slot, use the last known value
      const dataPoint = {
        x_value: slotTime.toISOString(),
        y_value: closestData ? closestData.value : lastValue
      };
      
      // Update lastValue if we have data
      if (dataPoint.y_value !== null) {
        lastValue = dataPoint.y_value;
      }
      
      chartData.push(dataPoint);
    });
    
    return chartData;
  }
  
  /**
   * Get time series chart data for a specific device
   * @param {string} bucketName - Name of the bucket
   * @param {string} deviceId - Device ID to retrieve data for
   * @param {string} timeStart - Start time in format 'YYYY-MM-DD-HH-MM-SS'
   * @param {string} timeEnd - End time in format 'YYYY-MM-DD-HH-MM-SS'
   * @param {string} timeSlot - Time slot interval ('1m', '1h', '1d', '1M')
   * @param {Object} minioClient - MinIO client instance
   * @returns {Promise<Object>} - Formatted chart data
   */
  exports.getChartData = async (bucketName, deviceId, timeStart, timeEnd, timeSlot) => {
    try {
      // Validate parameters
      if (!deviceId || !timeStart || !timeEnd || !timeSlot ) {
        throw new Error('Missing required parameters');
      }
  
      // Parse time parameters
      const startTimeInfo = parseTimeString(timeStart);
      const endTimeInfo = parseTimeString(timeEnd);
      
      // Calculate interval information
      const intervalInfo = calculateIntervalInfo(timeSlot);
      
      // Get file paths to check based on time range and granularity
      const filePaths = getFilePaths(
        deviceId,
        startTimeInfo,
        endTimeInfo,
        intervalInfo.type
      );
      
      // Get objects that match our paths
      const objects = await getObjectsByPaths( bucketName, filePaths);
      
      // Extract and organize data by timestamp
      const dataByTimestamp = extractDataFromObjects(
        objects,
        startTimeInfo.date,
        endTimeInfo.date
      );
      
      // Generate time slots
      const timeSlots = generateTimeSlots(
        startTimeInfo.date,
        endTimeInfo.date,
        intervalInfo.ms
      );
      
      // Create chart data points
      const chartData = createChartDataPoints(timeSlots, dataByTimestamp);
      
      return {
        device_id: deviceId,
        data: chartData
      };
    } catch (err) {
      console.error('Error getting chart data:', err);
      throw err;
    }
  };