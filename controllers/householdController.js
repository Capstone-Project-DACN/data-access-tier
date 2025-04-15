// controllers/minioController.js
const minioService = require('../services/householdService.js');


exports.getProcessedHouseholdData = async (req, res, next) => {
  try {
    const bucketName = req.query.bucket || 'household';
    const householdId = req.params.householdId;
    const targetDate = req.query.date || null; // YYYY-MM-DD format
    const timeFormat = req.query.timeFormat || 'hour'; // 'hour' or 'timestamp'
    const latestOnly= req.query.latestOnly === 'true' || req.query.latestOnly === '1';
    if (!householdId) {
      return res.status(400).json({
        success: false,
        message: 'Household ID is required'
      });
    }

    const result = await minioService.getAndProcessHouseholdData(
        bucketName,
        householdId,
        targetDate,
        timeFormat,
        latestOnly
    );

    res.json({
      success: true,
      householdId: householdId,
      date: targetDate || 'all',
      data: result
    });
  } catch (err) {
    next(err);
  }
};

exports.getChartData = async (req, res) => {
  try {
    const deviceId = req.query.device_id;
    const timeStart = req.query.time_start;
    const timeEnd = req.query.time_end;
    const timeSlot = req.query.time_slot || '1h';
    
    // Validate required parameters
    if (!deviceId || !timeStart || !timeEnd) {
      return res.status(400).json({
        error: 'Missing required parameters',
        message: 'Please provide device_id, time_start, and time_end parameters'
      });
    }
    
    // Validate time slot format
    if (!['1m', '1h', '1d'].includes(timeSlot)) {
      return res.status(400).json({
        error: 'Invalid time_slot parameter',
        message: 'time_slot must be one of: 1m, 1h, 1d'
      });
    }
    
    // Get chart data from minio service
    const chartData = await minioService.getChartData(
      'household', // bucket name - adjust if needed
      deviceId,
      timeStart,
      timeEnd,
      timeSlot,
    );
    
    res.json(chartData);
  } catch (err) {
    console.error('Error in chart endpoint:', err);
    res.status(500).json({
      error: 'Internal server error',
      message: err.message
    });
  }
};
