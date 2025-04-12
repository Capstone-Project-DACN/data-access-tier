// services/minioService.js
const Minio = require('minio');
const stream = require('stream');
const { promisify } = require('util');
const finished = promisify(stream.finished);

// Create MinIO client
const minioClient = new Minio.Client({
  endPoint: process.env.MINIO_ENDPOINT || 'localhost',
  port: parseInt(process.env.MINIO_PORT || '9000'),
  useSSL: process.env.MINIO_USE_SSL === 'true',
  accessKey: process.env.MINIO_ACCESS_KEY || 'myminioadmin',
  secretKey: process.env.MINIO_SECRET_KEY || 'myminioadmin'
});

/**
 * Get all objects for a specific household ID
 * @param {string} bucketName - Name of the bucket
 * @param {string} householdId - Household ID to retrieve data for
 * @returns {Promise<Array>} - Array of objects with their data
 */
exports.getObjectsByHouseholdId = async (bucketName, householdId) => {
  try {
    // Ensure bucket exists
    const bucketExists = await minioClient.bucketExists(bucketName);
    if (!bucketExists) {
      throw new Error(`Bucket ${bucketName} does not exist`);
    }

    // Create the prefix pattern for this household
    const prefix = `${householdId}/`;

    // List all objects with the given prefix
    const objectsStream = minioClient.listObjects(bucketName, prefix, true);

    const objectsWithData = [];
    const pendingPromises = [];

    return new Promise((resolve, reject) => {
      objectsStream.on('data', (obj) => {
        // For each object found, create a promise to retrieve its data
        const dataPromise = (async () => {
          try {
            // Get object metadata
            const stat = await minioClient.statObject(bucketName, obj.name);

            // Get object content
            const dataStream = await minioClient.getObject(bucketName, obj.name);

            // Read the stream into a buffer
            const chunks = [];
            for await (const chunk of dataStream) {
              chunks.push(chunk);
            }
            const dataBuffer = Buffer.concat(chunks);

            // Convert buffer to string (assuming CSV or text data)
            const dataContent = dataBuffer.toString('utf-8');

            // Add to our results array
            objectsWithData.push({
              id: obj.name,
              size: obj.size,
              lastModified: obj.lastModified,
              etag: obj.etag,
              metadata: stat.metaData,
              contentType: stat.metaData && stat.metaData['content-type'],
              data: dataContent
            });
          } catch (err) {
            console.error(`Error retrieving object ${obj.name}:`, err);
            // Still include the object without content if there's an error
            objectsWithData.push({
              id: obj.name,
              size: obj.size,
              lastModified: obj.lastModified,
              etag: obj.etag,
              error: err.message
            });
          }
        })();

        pendingPromises.push(dataPromise);
      });

      objectsStream.on('error', (err) => {
        reject(err);
      });

      objectsStream.on('end', async () => {
        try {
          // Wait for all data retrieval operations to complete
          await Promise.all(pendingPromises);

          // Sort objects by lastModified date (newest first)
          objectsWithData.sort((a, b) => {
            return new Date(b.lastModified) - new Date(a.lastModified);
          });

          resolve(objectsWithData);
        } catch (err) {
          reject(err);
        }
      });
    });
  } catch (err) {
    throw err;
  }
};

/**
 * Process household energy data and organize by date
 * @param {Array} minioObjects - Array of objects from MinIO containing energy data
 * @param {string} targetDate - Date to filter in format "YYYY-MM-DD" or null for all dates
 * @param {string} timeFormat - "timestamp" for full timestamp or "hour" to group by hour
 * @param {boolean} latestOnly - If true, only keep latest record for each time group
 * @returns {Object} - Organized data structure with statistics
 */
/**
 * Process household energy data and organize by date
 * @param {Array} minioObjects - Array of objects from MinIO containing energy data
 * @param {string} targetDate - Date to filter in format "YYYY-MM-DD" or null for all dates
 * @param {string} timeFormat - "timestamp", "hour", "date", "month", or "year" to group by time unit
 * @param {boolean} latestOnly - If true, only keep latest record for each time group
 * @returns {Object} - Organized data structure with statistics
 */
exports.processHouseholdData = (minioObjects, targetDate = null, timeFormat = "hour", latestOnly = true) => {


  // Skip empty files or those without data
  const validFiles = minioObjects.filter(obj =>
      obj.data &&
      typeof obj.data === 'string' &&
      obj.data.trim().length > 0 &&
      !obj.id.includes('_SUCCESS') && // Skip _SUCCESS files
      !obj.id.includes('_temporary') // Skip temporary files
  );

  // Parse all CSV data
  const allRecords = [];

  validFiles.forEach(file => {
    // Get date from filename (format: hcmc-q1-0/2025-04-07-15-50-25/...)
    const pathParts = file.id.split('/');
    let dateTimePart = null;

    // Find the part that looks like a date-time (contains multiple hyphens)
    for (const part of pathParts) {
      if (part.match(/\d{4}-\d{2}-\d{2}/)) {
        dateTimePart = part;
        break;
      }
    }

    if (!dateTimePart) return; // Skip if no date found

    // Extract date and time
    const [year, month, day, hour, minute, second] = dateTimePart.split('-');
    const fileDate = `${year}-${month}-${day}`;
    const fileTime = `${hour}:${minute}:${second}`;

    // Skip if we're filtering by date and this doesn't match
    if (targetDate && fileDate !== targetDate) return;

    // Parse CSV content
    const lines = file.data.trim().split('\n');
    if (lines.length <= 1) return; // Skip if only header or empty

    const headers = lines[0].split(',');

    // Process each data row
    for (let i = 1; i < lines.length; i++) {
      const values = lines[i].split(',');
      if (values.length !== headers.length) continue; // Skip malformed rows

      const record = {};
      headers.forEach((header, index) => {
        // Convert numeric fields to numbers
        if (['electricity_usage_kwh', 'voltage', 'current'].includes(header)) {
          record[header] = parseFloat(values[index]);
        } else {
          record[header] = values[index];
        }
      });

      // Add file metadata
      record._file = {
        id: file.id,
        lastModified: file.lastModified,
        size: file.size
      };

      // Add file date and time if not already present
      if (!record.formatted_timestamp || record.formatted_timestamp.trim() === '') {
        record.formatted_timestamp = `${fileDate} ${fileTime}`.replace(/-/g, '-');
      }

      // Replace any timestamps that use spaces with standard format
      if (record.formatted_timestamp.includes(' ')) {
        // If the format is like "2025-04-07 16-44-01", convert to "2025-04-07-16-44-01"
        const parts = record.formatted_timestamp.split(' ');
        if (parts[1].includes('-')) {
          record.formatted_timestamp = parts[0] + '-' + parts[1].replace(/-/g, '-');
        }
      }

      // Extract time components from timestamp for grouping
      let dateParts = { year: '', month: '', day: '', hour: '', date: '' };

      if (record.formatted_timestamp.includes(' ')) {
        // Format: "2025-04-07 16:44:01" or "2025-04-07 16-44-01"
        const parts = record.formatted_timestamp.split(' ');
        const datePortion = parts[0].split('-'); // ["2025", "04", "07"]
        const timePortion = parts[1].replace(/-/g, ':').split(':'); // ["16", "44", "01"]

        dateParts.year = datePortion[0];
        dateParts.month = datePortion[1];
        dateParts.day = datePortion[2];
        dateParts.hour = timePortion[0];
        dateParts.date = parts[0];
      } else if (record.formatted_timestamp.includes('-')) {
        // Format: "2025-04-07-16-44-01"
        const parts = record.formatted_timestamp.split('-');
        dateParts.year = parts[0];
        dateParts.month = parts[1];
        dateParts.day = parts[2];
        dateParts.hour = parts[3];
        dateParts.date = `${parts[0]}-${parts[1]}-${parts[2]}`;
      }

      // Save all time components to the record
      record.year = dateParts.year;
      record.month = dateParts.month;
      record.day = dateParts.day;
      record.date = dateParts.date;
      record.hour = dateParts.hour;

      allRecords.push(record);
    }
  });

  // Group records by time format according to requested format
  const groupedData = {};

  allRecords.forEach(record => {
    let groupKey;

    // Determine group key based on requested time format
    switch (timeFormat) {
      case 'year':
        groupKey = record.year;
        break;
      case 'month':
        groupKey = `${record.year}-${record.month}`;
        break;
      case 'date':
        groupKey = record.date;
        break;
      case 'hour':
        groupKey = `${record.date} ${record.hour}:00`;
        break;
      case 'timestamp':
      default:
        groupKey = record.formatted_timestamp;
        break;
    }

    if (!groupedData[groupKey]) {
      groupedData[groupKey] = [];
    }

    groupedData[groupKey].push(record);
  });

  // Get only the latest record in each group if requested
  // Get only the latest record in each group if requested
  if (latestOnly) {
    // Process each time group to keep only the latest record
    Object.keys(groupedData).forEach(timeKey => {
      const records = groupedData[timeKey];

      // Sort by the formatted_timestamp in the data itself (not by file metadata)
      records.sort((a, b) => {
        // Newer timestamps should come first (descending order)
        return b.formatted_timestamp.localeCompare(a.formatted_timestamp);
      });

      // Keep only the latest record
      groupedData[timeKey] = [records[0]];
    });
  }

  // Calculate statistics for each time group
  const result = {
    household_id: allRecords.length > 0 ? allRecords[0].device_id : null,
    device_id: allRecords.length > 0 ? allRecords[0].device_id : null,
    date: targetDate || 'all',
    timeFormat: timeFormat,
    latestOnly: latestOnly,
    totalReadings: allRecords.length,
    filteredReadings: Object.values(groupedData).reduce((sum, group) => sum + group.length, 0),
    timePoints: Object.keys(groupedData).length,
    sortOrder: "latest to oldest",
    data: {}
  };

  // Calculate statistics for each time group - sort from newest to oldest
  Object.keys(groupedData).sort((a, b) => {
    // For consistent sorting across all time formats
    return timeFormat === 'timestamp' || timeFormat === 'hour'
        ? new Date(b) - new Date(a)  // Timestamp and hour can be compared directly
        : b.localeCompare(a);        // For year/month/date formats, reverse string comparison
  }).forEach(timeKey => {
    const records = groupedData[timeKey];

    // For latestOnly mode, don't need to calculate stats since there's only one record
    if (latestOnly) {
      const record = records[0];
      result.data[timeKey] = {
        count: 1,
        electricity_usage_kwh: record.electricity_usage_kwh,
        voltage: record.voltage,
        current: record.current,
        sample: record,
        file: {
          id: record._file.id,
        }
      };
      delete record._file;
    } else {
      // Calculate statistics for all records in the group
      const stats = {
        count: records.length,
        electricity_usage_kwh: {
          min: Math.min(...records.map(r => r.electricity_usage_kwh)),
          max: Math.max(...records.map(r => r.electricity_usage_kwh)),
          avg: records.reduce((sum, r) => sum + r.electricity_usage_kwh, 0) / records.length
        },
        voltage: {
          min: Math.min(...records.map(r => r.voltage)),
          max: Math.max(...records.map(r => r.voltage)),
          avg: records.reduce((sum, r) => sum + r.voltage, 0) / records.length
        },
        current: {
          min: Math.min(...records.map(r => r.current)),
          max: Math.max(...records.map(r => r.current)),
          avg: records.reduce((sum, r) => sum + r.current, 0) / records.length
        },
        samples: records
      };

      result.data[timeKey] = stats;
    }
  });

  return result;
};

/*
 * Get and process household data for a specific date
 * @param {string} bucketName - Name of the bucket
 * @param {string} householdId - Household ID to retrieve data for
 * @param {string} targetDate - Date in format 'YYYY-MM-DD' or null for all dates
 * @param {string} timeFormat - Group by 'hour' or 'timestamp'
 * @param {string} outputFormat - 'json', 'report', or 'csv'
 * @returns {Promise<Object|string>} - Processed data in requested format
 */
exports.getAndProcessHouseholdData = async (bucketName, householdId, targetDate = null, timeFormat = 'hour',latestOnly) => {
  try {
    // Get all objects for the household
    const objects = await exports.getObjectsByHouseholdId(bucketName, householdId);

    // Process the data
    const processedData = exports.processHouseholdData(objects, targetDate, timeFormat,latestOnly);

    return processedData;
  } catch (err) {
    throw err;
  }
};