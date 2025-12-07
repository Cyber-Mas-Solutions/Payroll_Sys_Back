// src/routes/contracts.routes.js
const express = require('express');
const upload = require('../middleware/upload');
const { requireAuth, requireRole } = require('../middleware/auth');
const ctrl = require('../controllers/contracts.controller');

const router = express.Router();

// HR only
router.use(requireAuth, requireRole('HR'));

// List all documents (with optional filters)
router.get('/', ctrl.listDocuments);

// Upload one or more documents for an employee
router.post('/', upload.array('files', 10), ctrl.uploadDocuments);

// Delete a document
router.delete('/:id', ctrl.deleteDocument);

module.exports = router;
