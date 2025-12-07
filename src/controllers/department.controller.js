// src/controllers/department.controller.js
const pool = require('../config/db');

exports.list = async (_req, res) => {
  const [rows] = await pool.query('SELECT id, name FROM departments ORDER BY name ASC');
  res.json({ ok: true, data: rows });
};
