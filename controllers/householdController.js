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

