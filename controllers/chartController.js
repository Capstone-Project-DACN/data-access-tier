const chartService = require('../services/chartService.js');

exports.getChartData = async (req, res) => {
    try {
      const bucket = req.query.bucket || 'household';
      const deviceId = req.query.device_id;
      const timeStart = req.query.time_start;
      const timeEnd = req.query.time_end;
      const timeSlot = req.query.time_slot || '1h';
      const sortOrder = req.query.sortOrder;
      
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
      const chartData = await chartService.getChartData(
        bucket, // bucket name - adjust if needed
        deviceId,
        timeStart,
        timeEnd,
        timeSlot,
        sortOrder
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
  