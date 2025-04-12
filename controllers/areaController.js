// controllers/minioController.js
const areaService = require('../services/areaService.js');


exports.getProcessedAreaData = async (req, res, next) => {
  try {
    const bucketName = req.query.bucket || 'ward';
    const householdId = req.params.areaId;
    const targetDate = req.query.date || null; // YYYY-MM-DD format
    const timeFormat = req.query.timeFormat || 'hour'; // 'hour' or 'timestamp', 'month','date','year'
    const latestOnly= req.query.latestOnly === 'true' || req.query.latestOnly === '1';
    if (!householdId) {
      return res.status(400).json({
        success: false,
        message: 'Household ID is required'
      });
    }

    const result = await areaService.getAndProcessAreaData(
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

