const express = require('express');
const { body } = require('express-validator');
const validate = require('../middleware/validate');
const ctrl = require('../controllers/auth.controller');
const { requireAuth } = require('../middleware/auth');

const router = express.Router();

router.post(
  '/login',
  [body('email').isEmail(), body('password').isString().isLength({ min: 6 })],
  validate,
  ctrl.login
);

router.get('/me', requireAuth, ctrl.me);

module.exports = router;

