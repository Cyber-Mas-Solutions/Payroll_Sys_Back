const express = require('express');
const { requireAuth } = require('../middleware/auth');
const ctrl = require('../controllers/auditlog.controller');

const router = express.Router();
router.get('/', requireAuth, ctrl.getauditLogs);
router.get('/:id', requireAuth, ctrl.getauditLogById);

module.exports = router;
