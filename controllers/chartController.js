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
exports.getChartCityUsage = async (req, res) => {
    try {
      const city = req.query.city;
      const timeStart = req.query.time_start;
      const timeEnd = req.query.time_end;
      
      // Validate required parameters
      if (!city || !timeStart || !timeEnd) {
        return res.status(400).json({
          error: 'Missing required parameters',
          message: 'Please provide city, time_start, and time_end parameters'
        });
      }
      
      const chartData = await chartService.getChartCityUsage(
        city, 
        timeStart,
        timeEnd
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
exports.getChartCityDaily= async(req,res) =>{
  try {
    const deviceId = req.query.device_id;
    const timeStart = req.query.time_start;
    const timeEnd = req.query.time_end;
    const multiplyBy= req.query.multiplyBy | 1000
    // Validate required parameters
    if (!deviceId || !timeStart || !timeEnd) {
      return res.status(400).json({
        error: 'Missing required parameters',
        message: 'Please provide device_Id, time_start, and time_end parameters'
      });
    }
    
    const startDate = new Date(timeStart);
    const endDate = new Date(timeEnd);

    // Check if dates are valid
    if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
      return res.status(400).json({
        error: 'Invalid date format',
        message: 'Please provide valid date formats for time_start and time_end'
      });
    }

    const timeDifference = endDate.getTime() - startDate.getTime();
    const dayDifference = timeDifference / (1000 * 3600 * 24);

    // Verify that end date is after start date by at least 1 day
    if (timeDifference <= 0 || dayDifference < 1) {
      return res.status(400).json({
        error: 'Invalid time range',
        message: 'time_end must be greater than time_start by at least 1 day'
      });
    }

    const chartData = await chartService.getChartCityDaily(
      deviceId, 
      timeStart,
      timeEnd,
      multiplyBy
    );
    
    res.json(chartData);
  } catch (err) {
    console.error('Error in chart endpoint:', err);
    res.status(500).json({
      error: 'Internal server error',
      message: err.message
    });
  }
}

exports.predictDaily= async(req,res) =>{
  try {
    const deviceId = 'area-HCMC-Q10';
    const timeStart = req.query.time_start;
    const timeEnd = req.query.time_end;
    const multiplyBy= req.query.multiplyBy | 1000
    // Validate required parameters
    if (!deviceId || !timeStart || !timeEnd) {
      return res.status(400).json({
        error: 'Missing required parameters',
        message: 'Please provide device_Id, time_start, and time_end parameters'
      });
    }
    
    const startDate = new Date(timeStart);
    const endDate = new Date(timeEnd);

    // Check if dates are valid
    if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
      return res.status(400).json({
        error: 'Invalid date format',
        message: 'Please provide valid date formats for time_start and time_end'
      });
    }

    const chartData = await chartService.predictDaily(
      deviceId, 
      timeStart,
      timeEnd,
      multiplyBy
    );
    
    res.json(chartData);
  } catch (err) {
    console.error('Error in chart endpoint:', err);
    res.status(500).json({
      error: 'Internal server error',
      message: err.message
    });
  }
}