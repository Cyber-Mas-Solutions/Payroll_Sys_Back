// src/controllers/leave.controller.js
const pool = require('../config/db');
const dayjs = require('dayjs');

// Helper to determine the number of full days between two dates (inclusive)
// NOTE: This assumes whole days and ignores weekends/holidays for simplicity 
// in this file, as holiday/weekend logic should be centralized elsewhere.
function calculateFullDays(start, end) {
    if (!start || !end) return 0;
    const s = dayjs(start);
    const e = dayjs(end);
    if (s.isAfter(e)) return 0;
    // Difference in days + 1 (to be inclusive)
    return e.diff(s, 'day') + 1; 
}

function computeDurationHours(startDate, endDate, startTime, endTime) {
  // 💡 FIX: Default full day duration changed from 8 to 9 hours based on requirement.
  const s = dayjs(`${startDate}${startTime ? ' ' + startTime : ' 09:00'}`);
  const e = dayjs(`${endDate}${endTime ? ' ' + endTime : ' 18:00'}`);

  // Prevent negatives
  const h = Math.max(0, e.diff(s, 'minute')) / 60;
  return Number(h.toFixed(2));
}

exports.createRequest = async (req, res) => {
  const {
    employee_id, leave_type_id, start_date, end_date,
    start_time, end_time, reason, duration_hours: manual_duration_hours // 💡 CHANGE 2: Capture explicit duration (4.0 for half-day) from FE
  } = req.body;

  const [empRows] = await pool.query(
    'SELECT id, department_id FROM employees WHERE id = ?',
    [employee_id]
  );
  const emp = empRows[0];
  if (!emp) return res.status(404).json({ ok:false, message: 'Employee not found' });

  // 💡 CHANGE 2: Prioritize manual duration provided by frontend (e.g., 4.0 for half-day)
  let duration_hours = manual_duration_hours;
  
  if (duration_hours === null || duration_hours === undefined) {
      // Fallback calculation for date ranges (assumes 9hr blocks)
      duration_hours = computeDurationHours(start_date, end_date, start_time, end_time); 
  }
  
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

exports.decideRequest = async (req, res) => {
  const id = Number(req.params.id);
  const { action, note } = req.body;
  const conn = await pool.getConnection(); // Use connection for transaction

  

  const [[lr]] = await pool.query('SELECT * FROM leave_requests WHERE id=?', [id]);
  if (!lr) return res.status(404).json({ ok:false, message: 'Request not found' });
  if (lr.status !== 'PENDING' && action !== 'RESPOND') {
    return res.status(400).json({ ok:false, message: 'Already decided' });
  }

  let newStatus = lr.status;
  if (action === 'APPROVE') newStatus = 'APPROVED';
  if (action === 'REJECT') newStatus = 'REJECTED';

  await pool.query(
    `UPDATE leave_requests SET
       status = ?,
       decided_by_user_id = ?,
       decided_at = NOW(),
       decision_note = COALESCE(?, decision_note)
     WHERE id = ?`,
    [newStatus, req.user.id, note || null, id]
  );

  // Update leave_balances on approve
  if (action === 'APPROVE') {
    const year = dayjs(lr.start_date).year();
    // 💡 CHANGE 2: Convert hours to days using 9 hours as a full day.
    const daysUsed = (Number(lr.duration_hours) / 9.0).toFixed(2); 

    await pool.query(
      `INSERT INTO leave_balances (employee_id, leave_type_id, year, entitled_days, used_days)
       VALUES (?,?,?,?,?)
       ON DUPLICATE KEY UPDATE used_days = used_days + VALUES(used_days)`,
      [lr.employee_id, lr.leave_type_id, year, 0, daysUsed]
    );
  }

  res.json({ ok:true, message: action === 'RESPOND' ? 'Response saved' : `Request ${newStatus.toLowerCase()}` });
};

exports.statusList = async (req, res) => {
  // Re-use listRequests for now
  await exports.listRequests(req, res);
};

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
      hours: r.duration_hours,
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

// 隼 NEW: Employee leave balances for EmployeeLeaves.jsx (Overview Tab)
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

    const [rows] = await pool.query(
      `SELECT
          e.id AS employee_id,
          e.employee_code,
          e.full_name,
          COALESCE(d.name, e.department_name) AS department_name,
          lr.annual_limit AS annualEntitlement,     -- 💡 CHANGE 3: Entitlement from Grade Rule
          lr.medical_limit AS medicalEntitlement,    -- 💡 CHANGE 3: Entitlement from Grade Rule (Medical)
          
          -- Sum used days for 'Personal' leave type (ID 1), derived from hours/9
          SUM(CASE WHEN lt.name = 'Personal' AND lb.year = ? THEN lb.used_days ELSE 0 END) AS annualUsed,
          
          -- Sum used days for 'Sick' leave type (ID 2), derived from hours/9
          SUM(CASE WHEN lt.name = 'Sick' AND lb.year = ? THEN lb.used_days ELSE 0 END) AS medicalUsed
          
       FROM employees e
       LEFT JOIN departments d ON d.id = e.department_id
       LEFT JOIN leave_rules lr ON lr.grade_id = e.grade_id  -- 💡 CHANGE 3: Join with leave_rules for entitlements
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
      [year, year, year, ...params, pageSize, offset] // Pass year for the SUM CASE
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
      
      // Annual Leave (using rule limits)
      annualUsed: Number(r.annualUsed || 0),
      annualTotal: Number(r.annualEntitlement || 0),
      
      // Medical Leave (using rule limits) 💡 CHANGE 3: Renamed fields
      medicalUsed: Number(r.medicalUsed || 0),
      medicalTotal: Number(r.medicalEntitlement || 0),

      // Half day columns remain static / placeholder
      halfDay1: '0 / 0', 
      halfDay2: '0 / 0',
    }));

    res.json({ ok:true, page, pageSize, total, data, year });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok:false, message: err.message });
  }
};

// =========================================================================
// 隼 Leave Rules (getRules, saveRule) are unchanged as the API path is correct
// =========================================================================

/**
 * Fetches all existing leave rules along with all grades.
 */
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

    // Filter out rows where grade_name is null if the grades table isn't clean
    const data = rows.filter(r => r.grade_name);

    res.json({ ok: true, data });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: 'Failed to fetch leave rules' });
  }
};

/**
 * Inserts or updates a leave rule for a specific grade.
 */
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

    // Fetch the saved rule back to confirm success, including auto-generated ID if created
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