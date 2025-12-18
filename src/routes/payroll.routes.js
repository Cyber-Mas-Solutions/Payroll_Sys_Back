// src/routes/payroll.routes.js
const express = require('express');
const ctrl = require('../controllers/payroll.controller');
const { requireAuth, requireRole } = require('../middleware/auth');

const router = express.Router();

// Protect all payroll routes (HR/Finance roles)
router.use(requireAuth, requireRole(['HR', 'Finance']));

// Payroll processing endpoints
router.get('/employee-payroll-data', ctrl.getEmployeePayrollData);
router.get('/generate-payslip-pdf', ctrl.generatePaySlipPDF);
router.post('/process-salary-transfer', ctrl.processSalaryTransfer);
router.get('/department-payroll-summary', ctrl.getDepartmentPayrollSummary);
router.get('/payroll-transfers', ctrl.getPayrollTransfers);
router.get('/export-payroll-csv', ctrl.exportPayrollCSV);
router.get('/available-months', ctrl.getAvailableMonths);

// Test endpoint for debugging
router.get('/test-debug', (req, res) => {
  console.log('Test endpoint called by user:', req.user);
  res.json({
    ok: true,
    message: 'Payroll test endpoint working',
    user: req.user,
    timestamp: new Date().toISOString()
  });
});

module.exports = router;