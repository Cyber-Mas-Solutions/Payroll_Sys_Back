// src/routes/leave.routes.js
const express = require('express');
const { body } = require('express-validator'); // Import express-validator
const validate = require('../middleware/validate'); // 
const ctrl = require('../controllers/leave.controller');
const { requireAuth, requireRole } = require('../middleware/auth');

const router = express.Router();

// Protect all leave endpoints (HR only, like attendance)
router.use(requireAuth, requireRole('HR'));

// Leave requests
router.post('/requests', ctrl.createRequest);
router.get('/requests', ctrl.listRequests);
router.post('/requests/:id/decide', ctrl.decideRequest);

// Status / calendar / summary
router.get('/status', ctrl.statusList);
router.get('/calendar', ctrl.calendarFeed);
router.get('/summary', ctrl.summary);

// 🔹 Employee balances for EmployeeLeaves.jsx
router.get('/balances', ctrl.employeeBalances);

router.get('/calendar', ctrl.calendarFeed);

// Special / restricted dates for leave calendar
router.post('/calendar/restrictions', ctrl.saveRestriction);
router.delete('/calendar/restrictions/:id', ctrl.deleteRestriction);

// ===========================================================
// 🔹 NEW: Grade-Based Leave Rules
// ===========================================================
router.get('/rules', ctrl.getRules);
router.post(
    '/rules',
    [
        body('grade_id').isInt({ min: 1 }).withMessage('Invalid Grade ID'),
        body('annual_limit').isFloat({ min: 0 }).withMessage('Annual limit must be a positive number'),
        body('medical_limit').isFloat({ min: 0 }).withMessage('Medical limit must be a positive number'),
    ],
    validate, // Apply validation middleware
    ctrl.saveRule
);



module.exports = router;
