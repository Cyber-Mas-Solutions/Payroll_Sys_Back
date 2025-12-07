const express = require('express');
const ctrl = require('../controllers/attendance.controller');
const { requireAuth, requireRole } = require('../middleware/auth');

const router = express.Router();

// Protect all attendance endpoints
router.use(requireAuth, requireRole('HR'));

// Timetable routes
router.get('/timetables', ctrl.getTimetables);
router.post('/timetables', ctrl.createTimetable);
router.put('/timetables/:id', ctrl.updateTimetable);
router.delete('/timetables/:id', ctrl.deleteTimetable);

// Employee timetable assignment
router.post('/timetables/assign', ctrl.assignTimetable);

// Attendance records
router.get('/attendance', ctrl.getAttendanceRecords);
router.get('/attendance/employee/:employeeId', ctrl.getEmployeeAttendance);
router.post('/attendance/checkin', ctrl.checkIn);
router.post('/attendance/checkout', ctrl.checkOut);

// Attendance adjustments
router.get('/adjustments', ctrl.getAdjustments);
router.post('/adjustments', ctrl.createAdjustment);
router.put('/adjustments/:id/approve', ctrl.approveAdjustment);

// Reports
router.get('/reports/absence', ctrl.getAbsenceReport);
router.get('/reports/checkin-checkout', ctrl.getCheckinCheckoutReport);

module.exports = router;