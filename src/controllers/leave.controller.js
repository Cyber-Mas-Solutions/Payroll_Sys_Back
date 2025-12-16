// src/controllers/leave.controller.js
const pool = require('../config/db');
const dayjs = require('dayjs');

// Constants based on user requirements
const WORK_HOURS_PER_DAY = 9.0;
const LEAVE_TYPE_ANNUAL_ID = 1; // Personal (Annual)
const LEAVE_TYPE_MEDICAL_ID = 2; // Sick (Medical)

// Helper functions (Unchanged)
function calculateFullDays(start, end) {
    if (!start || !end) return 0;
    const s = dayjs(start);
    const e = dayjs(end);
    if (s.isAfter(e)) return 0;
    return e.diff(s, 'day') + 1; 
}

function computeDurationHours(startDate, endDate, startTime, endTime) {
  // Using 9 hours as the full day standard
  const s = dayjs(`${startDate}${startTime ? ' ' + startTime : ' 09:00'}`);
  const e = dayjs(`${endDate}${endTime ? ' ' + endTime : ' 18:00'}`);

  const h = Math.max(0, e.diff(s, 'minute')) / 60;
  return Number(h.toFixed(2));
}

// -----------------------------------------------------------------------------------
// CREATE REQUEST 
// -----------------------------------------------------------------------------------
// -----------------------------------------------------------------------------------
// CREATE REQUEST 
// -----------------------------------------------------------------------------------
exports.createRequest = async (req, res) => {
  const {
    employee_id, leave_type_id, start_date, end_date,
    start_time, end_time, reason, duration_hours: manual_duration_hours // Capture from FE
  } = req.body;

  const [empRows] = await pool.query(
    'SELECT id, department_id FROM employees WHERE id = ?',
    [employee_id]
  );
  const emp = empRows[0];
  if (!emp) return res.status(404).json({ ok:false, message: 'Employee not found' });

  // FIX: Check if manual_duration_hours is provided AND not null. 
  // If it is null (like for multi-day requests from FE), calculate it.
  let duration_hours = (manual_duration_hours !== null && manual_duration_hours !== undefined) 
    ? manual_duration_hours 
    : computeDurationHours(start_date, end_date, start_time, end_time);
    
  const attachment_path = req.file ? req.file.path.replace(/\\/g,'/') : null;

  const [result] = await pool.query(
    `INSERT INTO leave_requests
     (employee_id, leave_type_id, start_date, end_date, start_time, end_time,
      duration_hours, department_id, reason, attachment_path, status, created_by_user_id)
     VALUES (?,?,?,?,?,?,?,?,?,?, 'PENDING', ?)`,
    [
      employee_id, leave_type_id, start_date, end_date, start_time || null, end_time || null,
      duration_hours, emp.department_id || null, reason || null, attachment_path, req.user.id
    ]
  );

  res.status(201).json({ ok:true, id: result.insertId, duration_hours });
};

exports.listRequests = async (req, res) => {
  const page = Math.max(1, parseInt(req.query.page || '1', 10));
  const pageSize = Math.min(100, Math.max(1, parseInt(req.query.pageSize || '10', 10)));
  const offset = (page - 1) * pageSize;

  const { status, search, department_id, from, to } = req.query;

  const filters = [];
  const params = [];

  if (status) { filters.push('lr.status = ?'); params.push(status); }
  if (department_id) { filters.push('lr.department_id = ?'); params.push(Number(department_id)); }
  if (from) { filters.push('lr.start_date >= ?'); params.push(from); }
  if (to) { filters.push('lr.end_date <= ?'); params.push(to); }
  if (search) {
    filters.push('(e.full_name LIKE ? OR e.email LIKE ?)');
    params.push(`%${search}%`, `%${search}%`);
  }
  const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

  const [rows] = await pool.query(
    `SELECT lr.*, e.full_name, e.employee_code, d.name AS department_name, lt.name AS leave_type
     FROM leave_requests lr
     JOIN employees e ON e.id = lr.employee_id
     LEFT JOIN departments d ON d.id = lr.department_id
     JOIN leave_types lt ON lt.id = lr.leave_type_id
     ${where}
     ORDER BY lr.created_at DESC
     LIMIT ? OFFSET ?`,
    [...params, pageSize, offset]
  );

  const [[{ count }]] = await pool.query(
    `SELECT COUNT(*) AS count
     FROM leave_requests lr
     JOIN employees e ON e.id = lr.employee_id
     ${where}`,
    params
  );

  res.json({ ok:true, page, pageSize, total: count, data: rows });
};

// -----------------------------------------------------------------------------------
// DECIDE REQUEST (CRITICAL UNPAID LEAVE TRIGGER)
// -----------------------------------------------------------------------------------
exports.decideRequest = async (req, res) => {
  const id = Number(req.params.id);
  const { action, note } = req.body;
  const conn = await pool.getConnection(); 

  try {
    await conn.beginTransaction(); // Start transaction

    const [[lr]] = await conn.query('SELECT * FROM leave_requests WHERE id=?', [id]);
    if (!lr) {
        await conn.rollback();
        return res.status(404).json({ ok:false, message: 'Request not found' });
    }
    if (lr.status !== 'PENDING' && action !== 'RESPOND') {
        await conn.rollback();
        return res.status(400).json({ ok:false, message: 'Already decided' });
    }

    let newStatus = lr.status;
    if (action === 'APPROVE') newStatus = 'APPROVED';
    if (action === 'REJECT') newStatus = 'REJECTED';

    await conn.query(
      `UPDATE leave_requests SET
         status = ?,
         decided_by_user_id = ?,
         decided_at = NOW(),
         decision_note = COALESCE(?, decision_note)
       WHERE id = ?`,
      [newStatus, req.user.id, note || null, id]
    );

    // 💡 FIX 2: Automatic Unpaid Leave Creation Logic
    if (action === 'APPROVE') {
      const year = dayjs(lr.start_date).year();
      // Calculate days used by this request (e.g., 9 hours -> 1.00 day, 4 hours -> 0.44 days)
      const daysUsedByRequest = (Number(lr.duration_hours) / WORK_HOURS_PER_DAY); 

      // 1. Update/Insert into leave_balances (using fixed conversion)
      await conn.query(
        `INSERT INTO leave_balances (employee_id, leave_type_id, year, entitled_days, used_days)
         VALUES (?,?,?,?,?)
         ON DUPLICATE KEY UPDATE used_days = used_days + VALUES(used_days)`,
        [lr.employee_id, lr.leave_type_id, year, 0, daysUsedByRequest.toFixed(2)]
      );
      
      // 2. Fetch employee grade and leave rules
      const [[emp]] = await conn.query('SELECT grade_id FROM employees WHERE id = ?', [lr.employee_id]);
      const [[rules]] = await conn.query('SELECT annual_limit, medical_limit FROM leave_rules WHERE grade_id = ?', [emp?.grade_id || 0]);

      // 3. Get TOTAL current usage after this approval
      const [updatedBalances] = await conn.query(
          `SELECT lb.leave_type_id, SUM(lb.used_days) AS total_used 
           FROM leave_balances lb 
           WHERE lb.employee_id = ? AND lb.year = ? 
           GROUP BY lb.leave_type_id`, 
           [lr.employee_id, year]
      );

      const annualLimit = Number(rules?.annual_limit || 0);
      const medicalLimit = Number(rules?.medical_limit || 0);
      
      let leaveTypeMap = {};
      for (const bal of updatedBalances) {
          leaveTypeMap[bal.leave_type_id] = Number(bal.total_used);
      }
      
      const currentAnnualUsed = leaveTypeMap[LEAVE_TYPE_ANNUAL_ID] || 0; 
      const currentMedicalUsed = leaveTypeMap[LEAVE_TYPE_MEDICAL_ID] || 0; 

      let exceededDays = 0;
      let reason = '';
      
      // Check Annual Leave (ID 1)
      if (annualLimit > 0 && lr.leave_type_id === LEAVE_TYPE_ANNUAL_ID && currentAnnualUsed > annualLimit) {
          exceededDays = currentAnnualUsed - annualLimit;
          reason = `Annual Leave limit (${annualLimit} days) exceeded by ${exceededDays.toFixed(2)} days by this request.`;
      }
      
      // Check Medical Leave (ID 2)
      if (medicalLimit > 0 && lr.leave_type_id === LEAVE_TYPE_MEDICAL_ID && currentMedicalUsed > medicalLimit) {
          exceededDays = currentMedicalUsed - medicalLimit;
          reason = `Medical Leave limit (${medicalLimit} days) exceeded by ${exceededDays.toFixed(2)} days by this request.`;
      }

      // 4. Create Unpaid Leave record if limits exceeded (with a small margin for float errors)
      if (exceededDays > 0.01) { 
          // Note: total_days for unpaid_leaves is the EXCESS amount.
          await conn.query(
              `INSERT INTO unpaid_leaves (employee_id, start_date, end_date, total_days, reason, status)
               VALUES (?, ?, ?, ?, ?, 'Pending')`, // Set status to 'Pending' for HR review
              [lr.employee_id, lr.start_date, lr.end_date, exceededDays.toFixed(2), reason] 
          );
      }
    }

    await conn.commit(); // Commit transaction
    res.json({ ok:true, message: action === 'RESPOND' ? 'Response saved' : `Request ${newStatus.toLowerCase()}` });

  } catch (err) {
    await conn.rollback(); // Rollback on error
    console.error(err);
    res.status(500).json({ ok:false, message: err.message || 'Failed to process leave request' });

  } finally {
    conn.release();
  }
};

exports.statusList = async (req, res) => {
  // Re-use listRequests for now
  await exports.listRequests(req, res);
};

// -----------------------------------------------------------------------------------
// CALENDAR FEED (Fixed to return hours as 1 decimal, as per previous discussion)
// -----------------------------------------------------------------------------------
exports.calendarFeed = async (req, res) => {
  const { from, to } = req.query;

  try {
    const [rows] = await pool.query(
      `SELECT
         lr.id,
         lr.employee_id,
         e.employee_code,
         e.full_name,
         lt.name AS leave_type,
         lr.start_date,
         lr.end_date,
         lr.status,
         lr.duration_hours
       FROM leave_requests lr
       JOIN employees e ON e.id = lr.employee_id
       JOIN leave_types lt ON lt.id = lr.leave_type_id
       WHERE lr.status = 'APPROVED'
         AND lr.end_date >= ? AND lr.start_date <= ?
       ORDER BY lr.start_date ASC`,
      [from, to]
    );

    const events = rows.map(r => ({
      id: r.id,
      employee_id: r.employee_id,
      employee_code: r.employee_code,
      full_name: r.full_name,
      leave_type: r.leave_type,
      start_date: r.start_date,
      end_date: r.end_date,
      status: r.status,
      duration_hours: r.duration_hours,

      // backward-compatible fields
      title: `${r.full_name} - ${r.leave_type}`,
      start: r.start_date,
      end: r.end_date,
      // Send hours rounded to 1 decimal place.
      hours: Number(r.duration_hours).toFixed(1),
    }));

    // also return restrictions from calendar_restrictions if you added that:
    const [restrictionRows] = await pool.query(
      `SELECT id, date, type, reason
       FROM calendar_restrictions
       WHERE date >= ? AND date <= ?
       ORDER BY date ASC`,
      [from, to]
    );

    res.json({ ok: true, events, restrictions: restrictionRows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: err.message });
  }
};


// 隼 Create / update a restriction for a date
exports.saveRestriction = async (req, res) => {
  try {
    const { date, type, reason } = req.body;
    if (!date || !type) {
      return res.status(400).json({ ok: false, message: 'date and type are required' });
    }

    // Upsert by unique date
    const [result] = await pool.query(
      `INSERT INTO calendar_restrictions (date, type, reason, created_by_user_id)
       VALUES (?,?,?,?)
       ON DUPLICATE KEY UPDATE
         type = VALUES(type),
         reason = VALUES(reason),
         updated_at = CURRENT_TIMESTAMP`,
      [date, type, reason || null, req.user?.id || null]
    );

    // fetch the row back (so we get id)
    const [rows] = await pool.query(
      `SELECT id, date, type, reason
       FROM calendar_restrictions
       WHERE date = ?`,
      [date]
    );

    res.json({ ok: true, data: rows[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: err.message });
  }
};

// 隼 Delete restriction by date or id
exports.deleteRestriction = async (req, res) => {
  try {
    const { id } = req.params;    // /calendar/restrictions/:id
    const { date } = req.query;   // OR /calendar/restrictions?date=YYYY-MM-DD

    if (!id && !date) {
      return res.status(400).json({ ok: false, message: 'id or date is required' });
    }

    let result;
    if (id) {
      [result] = await pool.query(
        'DELETE FROM calendar_restrictions WHERE id = ?',
        [id]
      );
    } else {
      [result] = await pool.query(
        'DELETE FROM calendar_restrictions WHERE date = ?',
        [date]
      );
    }

    res.json({ ok: true, affectedRows: result.affectedRows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: err.message });
  }
};



exports.summary = async (req, res) => {
  const year = parseInt(req.query.year || String(dayjs().year()), 10);

  const [byType] = await pool.query(
    `SELECT lt.name AS leave_type, SUM(lr.duration_hours) AS hours
     FROM leave_requests lr
     JOIN leave_types lt ON lt.id = lr.leave_type_id
     WHERE YEAR(lr.start_date) = ? AND lr.status='APPROVED'
     GROUP BY lt.name
     ORDER BY lt.name ASC`,
    [year]
  );

  const today = dayjs().format('YYYY-MM-DD');
  const [[{ onLeaveToday }]] = await pool.query(
    `SELECT COUNT(*) AS onLeaveToday
     FROM leave_requests
     WHERE status='APPROVED' AND start_date <= ? AND end_date >= ?`,
    [today, today]
  );

  res.json({
    ok: true,
    year,
    onLeaveToday,
    byType
  });
};

// -----------------------------------------------------------------------------------
// EMPLOYEE BALANCES (Overview Tab)
// -----------------------------------------------------------------------------------
exports.employeeBalances = async (req, res) => {
  try {
    const year = parseInt(req.query.year || String(dayjs().year()), 10);
    const page = Math.max(1, parseInt(req.query.page || '1', 10));
    const pageSize = Math.min(100, Math.max(1, parseInt(req.query.pageSize || '50', 10)));
    const offset = (page - 1) * pageSize;

    const { department_id, search } = req.query;

    const filters = [];
    const params = [];

    if (department_id) {
      filters.push('e.department_id = ?');
      params.push(Number(department_id));
    }
    if (search) {
      filters.push('(e.full_name LIKE ? OR e.employee_code LIKE ?)');
      params.push(`%${search}%`, `%${search}%`);
    }
    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

    // 💡 FIX 5: Use grade-based rules for entitlements and specific IDs for usage.
    const [rows] = await pool.query(
      `SELECT
          e.id AS employee_id,
          e.employee_code,
          e.full_name,
          COALESCE(d.name, e.department_name) AS department_name,
          lr.annual_limit AS annualTotal,     -- From leave_rules
          lr.medical_limit AS medicalTotal,    -- From leave_rules
          
          -- Sum used days for Personal/Annual (ID 1)
          SUM(CASE WHEN lb.leave_type_id = ${LEAVE_TYPE_ANNUAL_ID} AND lb.year = ? THEN lb.used_days ELSE 0 END) AS annualUsed,
          
          -- Sum used days for Sick/Medical (ID 2)
          SUM(CASE WHEN lb.leave_type_id = ${LEAVE_TYPE_MEDICAL_ID} AND lb.year = ? THEN lb.used_days ELSE 0 END) AS medicalUsed
          
       FROM employees e
       LEFT JOIN departments d ON d.id = e.department_id
       LEFT JOIN leave_rules lr ON lr.grade_id = e.grade_id  
       LEFT JOIN leave_balances lb
         ON lb.employee_id = e.id AND lb.year = ?
       LEFT JOIN leave_types lt
         ON lt.id = lb.leave_type_id
       ${where}
       GROUP BY 
         e.id, e.employee_code, e.full_name, d.name, e.department_name, 
         lr.annual_limit, lr.medical_limit
       ORDER BY e.full_name ASC
       LIMIT ? OFFSET ?`,
      [year, year, year, ...params, pageSize, offset]
    );

    
    const [countRows] = await pool.query(
      `SELECT COUNT(*) AS count
       FROM employees e
       ${where}`,
      params
    );
    const total = countRows[0]?.count || 0;

    const data = rows.map(r => ({
      employee_id: r.employee_id,
      employee_code: r.employee_code,
      name: r.full_name,
      department: r.department_name || 'N/A',
      // Data in DAYS format for frontend display (using Annual=1, Medical=2)
      annualUsed: Number(r.annualUsed || 0),
      annualTotal: Number(r.annualTotal || 0),
      medicalUsed: Number(r.medicalUsed || 0), 
      medicalTotal: Number(r.medicalTotal || 0),
      halfDay1: '0 / 0',
      halfDay2: '0 / 0',
    }));

    res.json({ ok:true, page, pageSize, total, data, year });

  } catch (err) {
    console.error(err);
    res.status(500).json({ ok:false, message: err.message });
  }
};


// -----------------------------------------------------------------------------------
// LEAVE RULES (UNCHANGED)
// -----------------------------------------------------------------------------------

exports.getRules = async (req, res) => {
  try {
    const [rows] = await pool.query(
      `SELECT 
         g.grade_id, 
         g.grade_name,
         lr.annual_limit,
         lr.medical_limit
       FROM grades g
       LEFT JOIN leave_rules lr ON lr.grade_id = g.grade_id
       ORDER BY g.grade_id ASC`
    );

    const data = rows.filter(r => r.grade_name);
    res.json({ ok: true, data });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: 'Failed to fetch leave rules' });
  }
};

exports.saveRule = async (req, res) => {
  const { grade_id, annual_limit, medical_limit } = req.body;

  if (!grade_id || annual_limit === undefined || medical_limit === undefined) {
    return res.status(400).json({ ok: false, message: 'Grade ID, annual limit, and medical limit are required' });
  }

  try {
    const [result] = await pool.query(
      `INSERT INTO leave_rules (grade_id, annual_limit, medical_limit)
       VALUES (?,?,?)
       ON DUPLICATE KEY UPDATE 
         annual_limit = VALUES(annual_limit), 
         medical_limit = VALUES(medical_limit),
         updated_at = CURRENT_TIMESTAMP`,
      [grade_id, annual_limit, medical_limit]
    );

    const [rows] = await pool.query(
      `SELECT lr.id, lr.grade_id, lr.annual_limit, lr.medical_limit, g.grade_name
       FROM leave_rules lr
       JOIN grades g ON g.grade_id = lr.grade_id
       WHERE lr.grade_id = ?`,
      [grade_id]
    );

    res.json({ ok: true, data: rows[0], message: 'Leave rule saved successfully' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: 'Failed to save leave rule' });
  }
};

// ===================== EXPORTS =====================
module.exports = {
  calculateFullDays,
  computeDurationHours,
  createRequest: exports.createRequest,
  listRequests: exports.listRequests,
  decideRequest: exports.decideRequest,
  statusList: exports.statusList,
  calendarFeed: exports.calendarFeed,
  summary: exports.summary,
  employeeBalances: exports.employeeBalances,
  saveRestriction: exports.saveRestriction,
  deleteRestriction: exports.deleteRestriction,
  getRules: exports.getRules,
  saveRule: exports.saveRule,
};