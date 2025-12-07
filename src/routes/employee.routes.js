// src/routes/employee.routes.js
const express = require('express');
const { body } = require('express-validator');
const validate = require('../middleware/validate');
const upload = require('../middleware/upload');
const ctrl = require('../controllers/employee.controller');
const { requireAuth, requireRole } = require('../middleware/auth');

const router = express.Router();

// protect all employee routes (HR only)
router.use(requireAuth, requireRole('HR'));

const addValidations = [
  body('first_name').isString().isLength({ min: 1 }),
  body('last_name').optional({ nullable: true }).isString(),
  body('email').isEmail(),
  body('status').optional().isString(),
  body('dob').optional({ nullable: true }).isISO8601(),
  body('appointment_date').optional({ nullable: true }).isISO8601(),
  body('basic_salary').optional({ nullable: true }).isNumeric(),
];

const updateValidations = [
  body('email').optional({ nullable: true }).isEmail(),
  body('dob').optional({ nullable: true }).isISO8601(),
  body('appointment_date').optional({ nullable: true }).isISO8601(),
  body('basic_salary').optional({ nullable: true }).isNumeric(),
];

const uploadFields = upload.fields([
  { name: 'profilePhoto', maxCount: 1 },
  { name: 'documents', maxCount: 10 },
  { name: 'bankDocument', maxCount: 1 },
]);

router.post('/', uploadFields, addValidations, validate, ctrl.createEmployee);
router.get('/', ctrl.getEmployees);

router.get('/performance-overview', ctrl.getPerformanceOverview);
router.post('/performance-reviews', ctrl.addPerformanceReview);
router.get('/training-overview', ctrl.getTrainingOverview);
router.post('/training-records', ctrl.addTrainingRecord);

router.get('/:id', ctrl.getEmployeeById);
router.put('/:id', uploadFields, updateValidations, validate, ctrl.updateEmployee);
router.delete('/:id', ctrl.deleteEmployee);

// document mgmt (NOTE: base path is /api/employees)
router.delete('/:id/documents/:docId', ctrl.deleteEmployeeDocument);
router.put('/:id/documents/:docId', upload.single('file'), ctrl.replaceEmployeeDocument);

// PERFORMANCE & TRAINING OVERVIEW
router.get('/performance-overview', ctrl.getPerformanceOverview);
router.post('/performance-reviews', ctrl.addPerformanceReview);

router.get('/training-overview', ctrl.getTrainingOverview);
router.post('/training-records', ctrl.addTrainingRecord);


module.exports = router;
