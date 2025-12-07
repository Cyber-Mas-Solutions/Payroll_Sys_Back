// src/controllers/contracts.controller.js
const pool = require('../config/db');
const path = require('path');
const fs = require('fs');

function baseUrl(req) {
  return process.env.BACKEND_PUBLIC_URL || `${req.protocol}://${req.get('host')}`;
}
function toUrl(req, filePath) {
  if (!filePath) return null;
  const filename = String(filePath).split(/[/\\]/).pop();
  return `${baseUrl(req)}/uploads/${filename}`;
}
function kindFromType(t = '') {
  if (t.startsWith('image/')) return 'image';
  if (t === 'application/pdf') return 'pdf';
  return 'file';
}

// GET /api/contracts-docs?employee_id=&category=
exports.listDocuments = async (req, res) => {
  try {
    const { employee_id, category } = req.query;

    const params = [];
    let where = 'WHERE 1=1';

    if (employee_id) {
      where += ' AND d.employee_id = ?';
      params.push(Number(employee_id));
    }
    if (category) {
      where += ' AND d.category = ?';
      params.push(category);
    }

    const [rows] = await pool.query(
      `
      SELECT
        d.id,
        d.employee_id,
        d.category,
        d.file_name,
        d.file_path,
        d.file_type,
        d.status,
        d.uploaded_by_user_id,
        d.uploaded_at,
        e.full_name AS employee_name,
        e.employee_code
      FROM contracts_docs d
      JOIN employees e ON e.id = d.employee_id
      ${where}
      ORDER BY d.uploaded_at DESC, d.id DESC
      `,
      params
    );

    const data = rows.map((r) => ({
      id: r.id,
      employee_id: r.employee_id,
      employee_name: r.employee_name,
      employee_code: r.employee_code,
      category: r.category,
      file_name: r.file_name,
      url: toUrl(req, r.file_path),
      kind: kindFromType(r.file_type),
      status: r.status || 'Active',
      uploaded_at: r.uploaded_at,
      uploaded_by_user_id: r.uploaded_by_user_id,
    }));

    res.json({ ok: true, data });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: 'Failed to load contract documents' });
  }
};

// POST /api/contracts-docs
// form-data: employee_id, category, files[]
exports.uploadDocuments = async (req, res) => {
  try {
    const { employee_id, category } = req.body;
    const files = req.files || [];

    if (!employee_id) {
      return res.status(400).json({ ok: false, message: 'employee_id is required' });
    }
    if (!files.length) {
      return res.status(400).json({ ok: false, message: 'At least one file is required' });
    }

    const userId = (req.user && req.user.id) || null;

    for (const f of files) {
      await pool.query(
        `
        INSERT INTO contracts_docs
          (employee_id, category, file_name, file_path, file_type, status, uploaded_by_user_id)
        VALUES (?,?,?,?,?,?,?)
        `,
        [
          Number(employee_id),
          category || null,
          f.originalname,
          f.path.replace(/\\/g, '/'),
          f.mimetype,
          'Active',
          userId,
        ]
      );
    }

    res.status(201).json({ ok: true, message: 'Documents uploaded' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: 'Failed to upload documents' });
  }
};

// DELETE /api/contracts-docs/:id
exports.deleteDocument = async (req, res) => {
  try {
    const id = Number(req.params.id);

    const [[row]] = await pool.query(
      'SELECT file_path FROM contracts_docs WHERE id = ?',
      [id]
    );

    if (!row) {
      return res.status(404).json({ ok: false, message: 'Document not found' });
    }

    await pool.query('DELETE FROM contracts_docs WHERE id = ?', [id]);

    // best-effort delete actual file
    try {
      fs.unlinkSync(path.resolve(row.file_path));
    } catch (e) {
      // ignore
    }

    res.json({ ok: true, message: 'Document deleted' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: 'Failed to delete document' });
  }
};
