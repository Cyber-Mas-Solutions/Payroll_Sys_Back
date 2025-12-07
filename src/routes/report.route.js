// reports
const express = require('express');
const { requireAuth, requireRole } = require('../middleware/auth');
const { getMonthlyTotalData, getSalaryRange, compensateTrend, getDeductionsByType, getBonusesByType, getAllowancesByType, getEmployeeInsights, getAllDepartments } = require('../controllers/report.controller');

const router = express.Router();

router.use(requireAuth, requireRole('HR'));

router.get("/payroll/month", getMonthlyTotalData);
router.get('/payroll/salary-range', getSalaryRange);


router.get('/payroll/trends', compensateTrend);

router.get('/deductions/by-type', getDeductionsByType);
router.get('/allowances/by-type', getAllowancesByType);
router.get('/bonuses/by-type', getBonusesByType);

router.get('/employees', getEmployeeInsights);
router.get('/departments', getAllDepartments);

module.exports = router;