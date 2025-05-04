// routes/minioRoutes.js
const express = require('express');
const router = express.Router();
const householdController = require('../controllers/householdController');
const chartController = require('../controllers/chartController');
const areaController = require('../controllers/areaController');
router.get('/household/:householdId', householdController.getProcessedHouseholdData);
router.get('/area/:areaId', areaController.getProcessedAreaData);
router.get('/chart/', chartController.getChartData) 
router.get('/chart/usage', chartController.getChartCityUsage)
router.get('/chart/daily', chartController.getChartCityDaily)
router.get('/chart/predict-daily', chartController.predictDaily)

module.exports = router;