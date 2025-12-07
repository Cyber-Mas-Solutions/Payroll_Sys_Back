// src/controllers/salary.controller.js
const pool = require('../config/db');
const PDFDocument = require('pdfkit');
const logEvent = require('../utils/event');
const logAudit = require('../utils/audit');

//new compensation adjusmnt

// === Utils (date helpers for compensation) ===

// month helpers for compensation flows 

const endOfMonth = (yyyyMM) => {
  // yyyyMM = 'YYYY-MM'
  const [Y, M] = yyyyMM.split('-').map(Number);
  const d = new Date(Y, M, 0); // last day
  return d.toISOString().slice(0,10);
};

const startOfMonth = (yyyyMM) => {
  const [Y, M] = yyyyMM.split('-').map(Number);
  const d = new Date(Y, M - 1, 1);
  return d.toISOString().slice(0,10);
};

// 
//===========================================

// static grades fallback

const GRADES_FALLBACK = [
  { grade_id: 1, grade_name: 'A' },
  { grade_id: 2, grade_name: 'B' },
  { grade_id: 3, grade_name: 'C' },
];

//GRADES - used by overtime section 

const getGrades = async (_req, res) => {
  try {
    const [rows] = await pool.query('SELECT grade_id, grade_name FROM grades ORDER BY grade_id');
    if (!rows || !rows.length) return res.json(GRADES_FALLBACK);
    return res.json(rows);
  } catch (err) {
    console.error(err);
    logEvent({
      level: 'error', event_type: "GET_GRADES",
      user_id: req.user?.id || null,
      event_details: {},
      status: "FAILURE",
      error_message: err.message,
      req
    })
    return res.json(GRADES_FALLBACK);
  }
}

const gradeNameOf = async (gradeId) => {
  try {
    const [[row]] = await pool.query(
      'SELECT grade_name FROM grades WHERE grade_id = ? LIMIT 1',
      [gradeId]
    );
    return row?.grade_name || (GRADES_FALLBACK, find(g => g.gradeId == gradeId)?.grade_name ?? null);
  } catch {
    return GRADES_FALLBACK, find(g => g.grade_id == gradeId)?.grade_name ?? null;
  }
};

/* =============================================================
   OVERTIME (Grades, Rules, Adjustments)
   Matches DB schema in your dump:
   - grades (grade_id, grade_name)
   - overtime_rule (rule_id, grade_id [UNIQUE], ot_rate, max_ot_hours, created_at)
   - overtime_adjustments (adjustment_id, employee_id, grade_id, ot_hours, ot_rate, adjustment_reason, created_at)
   - v_overtime_adjustments (includes ot_amount = ot_hours * ot_rate)
   ============================================================= */




// === Get latest overtime rule for a grade
const getOvertimeRulesByGrade = async (req, res) => {
  try {
    const { gradeId } = req.params;
    const [rows] = await pool.query(
      `SELECT rule_id, grade_id, ot_rate, max_ot_hours, created_at
         FROM overtime_rule
        WHERE grade_id = ?
        ORDER BY rule_id DESC
        LIMIT 1`,
      [gradeId]
    );
    res.json(rows[0] || null);
  } catch (err) {
    console.error('getOvertimeRulesByGrade error:', err);
    logEvent({
      level: 'error', event_type: "GET_OVER_TIME_RULES_BY_GRADE_ERROR",
      user_id: user?.id || null,
      event_details: {
        email,
        ip: ip_address,
        error: err
      },
      error_message: err.message
    })
    res.status(500).json({ message: 'Failed to fetch overtime rules' });
  }
};
//****************************************************** */
// === Upsert overtime rule for a grade (create or update current)
const upsertOvertimeRule = async (req, res) => {
  try {
    const { grade_id, ot_rate, max_ot_hours } = req.body;
    if (!grade_id || ot_rate == null || max_ot_hours == null) {
      return res.status(400).json({ message: 'grade_id, ot_rate, max_ot_hours are required' });
    }

    // Because grade_id is UNIQUE (uq_overtime_rule_grade), use INSERT ... ON DUPLICATE KEY UPDATE
    await pool.query(
      `INSERT INTO overtime_rule (grade_id, ot_rate, max_ot_hours, created_at)
       VALUES (?, ?, ?, NOW())
       ON DUPLICATE KEY UPDATE
         ot_rate = VALUES(ot_rate),
         max_ot_hours = VALUES(max_ot_hours),
         created_at = NOW()`
      , [grade_id, ot_rate, max_ot_hours]
    );
    logAudit({
      user_id: req.user.id,
      action_type: "UPSERT_OVERTIME_RULE",
      target_table: "overtime_rule",
      target_id: grade_id,
      before_state: null,   // we skip state diff here
      after_state: { grade_id, ot_rate, max_ot_hours },
      req,
      status: "SUCCESS"
    });

    res.json({ message: 'Overtime rule saved' });
  } catch (err) {
    console.error('upsertOvertimeRules error:', err);

    logAudit({
      user_id: req.user.id,
      action_type: "UPSERT_OVERTIME_RULE",
      target_table: "overtime_rule",
      target_id: req.body.grade_id ?? null,
      before_state: null,
      after_state: null,
      req,
      status: "FAILURE",
      error_message: err.message
    }).catch(() => { });

    res.status(500).json({ message: 'Failed to save overtime rule' });
  }
};


// === Employee quick search (by name or id)
const searchEmployees = async (req, res) => {
  try {
    const q = (req.query.search || '').trim();
    const like = `%${q}%`;
    const [rows] = await pool.query(
      `SELECT e.id AS employee_id, e.full_name, e.grade_id, g.grade_name
         FROM employees e
         LEFT JOIN grades g ON g.grade_id = e.grade_id
        WHERE e.full_name LIKE ? OR CAST(e.id AS CHAR) LIKE ?
        ORDER BY e.full_name
        LIMIT 50`,
      [like, like]
    );
    res.json(rows);
  } catch (err) {
    console.error('searchEmployees error:', err);
    res.status(500).json({ message: 'Failed to search employees' });
  }
};

// ****************************************************

// === Create an overtime adjustment
const createOvertimeAdjustment = async (req, res) => {
  try {
    let { employee_id, grade_id, ot_hours, ot_rate, adjustment_reason } = req.body || {};

    if (!employee_id || ot_hours == null) {
      return res.status(400).json({ ok: false, message: 'employee_id and ot_hours are required' });
    }

    // Validate employee and infer grade if not provided
    const [[emp]] = await pool.query(
      'SELECT id AS employee_id, grade_id FROM employees WHERE id = ? LIMIT 1',
      [employee_id]
    );
    if (!emp) return res.status(404).json({ ok: false, message: 'Employee not found' });
    if (!grade_id) grade_id = emp.grade_id || null;

    // Fetch rule if rate missing
    if (ot_rate == null) {
      const [[rule]] = await pool.query(
        `SELECT ot_rate, max_ot_hours
           FROM overtime_rule
          WHERE grade_id = ?
          ORDER BY rule_id DESC
          LIMIT 1`,
        [grade_id]
      );
      if (!rule || rule.ot_rate == null) {
        return res.status(400).json({ ok: false, message: 'No overtime rule found for this grade. Set a rule first.' });
      }
      ot_rate = rule.ot_rate;

      if (rule.max_ot_hours != null && Number(ot_hours) > Number(rule.max_ot_hours)) {
        return res.status(400).json({ ok: false, message: `OT hours exceed monthly cap (${rule.max_ot_hours})` });
      }
    }

    const [result] = await pool.query(
      `INSERT INTO overtime_adjustments (employee_id, grade_id, ot_hours, ot_rate, adjustment_reason)
       VALUES (?, ?, ?, ?, ?)`,
      [employee_id, grade_id || null, Number(ot_hours), Number(ot_rate), adjustment_reason || null]
    );

    // AUDIT LOG (important)
    logAudit({
      action: "ADD_OVERTIME_ADJUSTMENT",
      target_table: "overtime_adjustments",
      target_id: result.insertId,
      user_id: req.user?.id || null,
      record_id: employee_id,
      change_details: {
        employee_id,
        grade_id,
        ot_hours,
        ot_rate,
        adjustment_reason
      }
    });

    return res.json({ ok: true, message: 'Overtime adjustment saved' });

  } catch (err) {
    console.error('createOvertimeAdjustment error:', err);
    res.status(500).json({ ok: false, message: 'Failed to save overtime adjustment' });
  }
};


// === List overtime adjustments by grade (uses view for ot_amount)
const listOvertimeAdjustmentsByGrade = async (req, res) => {
  try {
    const { gradeId } = req.params;
    const { from, to } = req.query;

    const where = ['oa.grade_id = ?'];
    const params = [gradeId];

    if (from) { where.push('DATE(oa.created_at) >= ?'); params.push(from); }
    if (to) { where.push('DATE(oa.created_at) <= ?'); params.push(to); }

    const [rows] = await pool.query(
      `SELECT
         oa.adjustment_id,
         oa.employee_id,
         e.full_name,
         oa.grade_id,
         g.grade_name,
         oa.ot_hours,
         oa.ot_rate,
         (oa.ot_hours * oa.ot_rate) AS ot_amount,
         oa.adjustment_reason,
         oa.created_at
       FROM overtime_adjustments oa
       JOIN employees e ON e.id = oa.employee_id
       LEFT JOIN grades g ON g.grade_id = oa.grade_id
       WHERE ${where.join(' AND ')}
       ORDER BY oa.created_at DESC
       LIMIT 500`,
      params
    );

    return res.json(rows);
  } catch (err) {
    console.error('listOvertimeAdjustmentsByGrade error:', err);
    logEvent({
      level: 'error', event_type: "GET_OVER_TIME_ADJUSTMENT_BY_GRADE",
      user_id: req.user.id,
      error_message: err.message,
      event_details: { err }
    })
    res.status(500).json({ ok: false, message: 'Failed to load overtime entries' });
  }
};

// === List overtime adjustments by employee (uses view)
const listOvertimeAdjustmentsByEmployee = async (req, res) => {
  try {
    const { employeeId } = req.params;
    const { from, to } = req.query;

    const where = ['oa.employee_id = ?'];
    const params = [employeeId];

    if (from) { where.push('DATE(oa.created_at) >= ?'); params.push(from); }
    if (to) { where.push('DATE(oa.created_at) <= ?'); params.push(to); }

    const [rows] = await pool.query(
      `SELECT
         oa.adjustment_id,
         oa.employee_id,
         e.full_name,
         oa.grade_id,
         g.grade_name,
         oa.ot_hours,
         oa.ot_rate,
         (oa.ot_hours * oa.ot_rate) AS ot_amount,
         oa.adjustment_reason,
         oa.created_at
       FROM overtime_adjustments oa
       JOIN employees e ON e.id = oa.employee_id
       LEFT JOIN grades g ON g.grade_id = oa.grade_id
       WHERE ${where.join(' AND ')}
       ORDER BY oa.created_at DESC
       LIMIT 500`,
      params
    );

    return res.json(rows);
  } catch (err) {
    console.error('listOvertimeAdjustmentsByEmployee error:', err);
    logEvent({
      level: 'error', event_type: "GET_OVER_TIME_ADJUSTMENT_BY_EMPLOYEE",
      user_id: req.user.id,
      error_message: err.message,
      event_details: { err }
    })
    res.status(500).json({ ok: false, message: 'Failed to fetch employee OT adjustments' });
  }
};



// ===================== EMPLOYEE SEARCH (advanced) =====================
const searchEmployeesAdvanced = async (req, res) => {
  try {
    const {
      q = '',
      department_id,
      department,
      grade_id,
      grade,
      limit = 500,
      offset = 0,
    } = req.query;

    const clauses = [];
    const params = [];

    if (q) {
      clauses.push('(e.full_name LIKE ? OR e.employee_code LIKE ? OR CAST(e.id AS CHAR) LIKE ?)');
      params.push(`%${q}%`, `%${q}%`, `%${q}%`);
    }
    if (department_id) { clauses.push('e.department_id = ?'); params.push(Number(department_id)); }
    if (department)    { clauses.push('d.name LIKE ?');       params.push(`%${department}%`); }
    if (grade_id)      { clauses.push('e.grade_id = ?');      params.push(Number(grade_id)); }
    if (grade)         { clauses.push('g.grade_name = ?');    params.push(grade); }

    const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';

    const [rows] = await pool.query(
      `
      SELECT 
        e.id AS employee_id,
        e.employee_code,
        e.full_name,
        COALESCE(d.name, e.department_name) AS department_name,
        e.grade_id,
        g.grade_name,
        s.basic_salary
      FROM employees e
      LEFT JOIN departments d ON d.id = e.department_id
      LEFT JOIN grades g ON g.grade_id = e.grade_id
      LEFT JOIN salaries s ON s.employee_id = e.id
      ${where}
      ORDER BY e.full_name
      LIMIT ? OFFSET ?
      `,
      [...params, Number(limit), Number(offset)]
    );

    res.json({ ok:true, data: rows });
  } catch (err) {
    console.error('searchEmployeesAdvanced error:', err);
    logEvent({
      level: 'error', event_type: "SEARCH_EMPLOYEE_ADVANCES",
      user_id: req.user.id,
      error_message: err.message,
      event_details: { err }
    })
    res.status(500).json({ ok:false, message:'Failed to search employees' });
  }
};




// ===================== DEPARTMENTS LIST =====================
const listDepartments = async (_req, res) => {
  try {
    const [rows] = await pool.query('SELECT id, name FROM departments ORDER BY name');
    res.json({ ok:true, data: rows });
  } catch (err) {
    console.error('listDepartments error:', err);
    logEvent({
      level: 'info', event_type: "LIST_DEPARTMENTS",
      user_id: _req.user.id,
      error_message: err.message,
      event_details: { err }
    })
    res.status(500).json({ ok:false, message:'Failed to fetch departments' });
  }
};

// ===================== COMPENSATION PREVIEW =====================
// body: { type, mode, amount, percent, month, note, employee_ids: number[] }

const previewCompensation = async (req, res) => {
  try {
    const { type, mode, amount, percent, month, note, employee_ids } = req.body;

    if (!type || !mode || !month || !Array.isArray(employee_ids) || employee_ids.length === 0) {
      return res.status(400).json({ ok:false, message:'type, mode, month, employee_ids required' });
    }
    if (mode === 'fixed' && (amount == null || isNaN(Number(amount)))) {
      return res.status(400).json({ ok:false, message:'amount required for fixed mode' });
    }
    if (mode === 'percent' && (percent == null || isNaN(Number(percent)))) {
      return res.status(400).json({ ok:false, message:'percent required for percent mode' });
    }

    const [rows] = await pool.query(
      `
      SELECT e.id AS employee_id, e.full_name, s.basic_salary
      FROM employees e
      LEFT JOIN salaries s ON s.employee_id = e.id
      WHERE e.id IN (${employee_ids.map(()=>'?').join(',')})
      `,
      employee_ids
    );

    const items = rows.map(r => {
      const basic = Number(r.basic_salary || 0);
      const computed = mode === 'fixed'
        ? Number(amount)
        : (Number(percent) / 100) * basic;

      return {
        employee_id: r.employee_id,
        name: r.full_name,
        basic_salary: basic,
        computed_amount: Number(computed.toFixed(2))
      };
    });

    const total = items.reduce((a,b)=>a + b.computed_amount, 0);

    res.json({
      ok:true,
      meta: { type, mode, month, note },
      items,
      total: Number(total.toFixed(2))
    });
  } catch (err) {
    console.error('previewCompensation error:', err);
    logEvent({
      level: 'error', event_type: "PREVIEW_COMPENSATION",
      user_id: req.user.id,
      error_message: err.message,
      event_details: { err }
    })
    res.status(500).json({ ok:false, message:'Failed to preview compensation' });
  }
};

// ===================== COMPENSATION APPLY (BULK) =====================
// body: { type, mode, amount, percent, month, note, employee_ids: number[], category? }
const applyCompensation = async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const { type, mode, amount, percent, month, note, employee_ids, category } = req.body;

    if (!type || !mode || !month || !Array.isArray(employee_ids) || employee_ids.length === 0) {
      return res.status(400).json({ ok:false, message:'type, mode, month, employee_ids required' });
    }
    if (mode === 'fixed' && (amount == null || isNaN(Number(amount)))) {
      return res.status(400).json({ ok:false, message:'amount required for fixed mode' });
    }
    if (mode === 'percent' && (percent == null || isNaN(Number(percent)))) {
      return res.status(400).json({ ok:false, message:'percent required for percent mode' });
    }

    const effectiveDate = endOfMonth(month);     // for bonuses
    const effFrom = startOfMonth(month);         // for allowance window
    const effTo   = endOfMonth(month);

    // Fetch employees and salaries
    const [rows] = await conn.query(
      `SELECT e.id AS employee_id, e.full_name, s.basic_salary
       FROM employees e
       LEFT JOIN salaries s ON s.employee_id = e.id
       WHERE e.id IN (${employee_ids.map(() => '?').join(',')})`,
      employee_ids
    );

    const calc = (basic) => mode === 'fixed'
      ? Number(amount)
      : (Number(percent) / 100) * Number(basic || 0);

    await conn.beginTransaction();

    let insertSql = '', insertValues = [], auditLogs = [];

    if (type === 'Bonus' || type === 'Arrears' || type === 'Correction') {
      insertSql = `INSERT INTO bonuses (employee_id, amount, reason, effective_date, created_by) VALUES `;
      rows.forEach((r, i) => {
        const val = Number(calc(r.basic_salary).toFixed(2));
        insertSql += `(?,?,?,?,?)${i < rows.length - 1 ? ',' : ''}`;
        insertValues.push(r.employee_id, val, note || type, effectiveDate, req.user?.id || null);
        auditLogs.push({
          user_id: req.user?.id || null,
          action_type: 'INSERT',
          target_table: 'bonuses',
          after_state: { employee_id: r.employee_id, amount: val, reason: note || type, effective_date: effectiveDate }
        });
      });
    } else if (type === 'Allowance') {
      insertSql = `INSERT INTO allowances (employee_id, name, category, amount, taxable, frequency, effective_from, effective_to, status, created_at) VALUES `;
      rows.forEach((r, i) => {
        const val = Number(calc(r.basic_salary).toFixed(2));
        insertSql += `(?,?,?,?,?,?,?,?, 'Active', NOW())${i < rows.length - 1 ? ',' : ''}`;
        insertValues.push(r.employee_id, note || 'One-time allowance', category || null, val, 0, 'Monthly', effFrom, effTo);
        auditLogs.push({
          user_id: req.user?.id || null,
          action_type: 'INSERT',
          target_table: 'allowances',
          after_state: { employee_id: r.employee_id, name: note || 'One-time allowance', category, amount: val, effective_from: effFrom, effective_to: effTo }
        });
      });
    } else if (type === 'Reimbursement') {
      insertSql = `INSERT INTO reimbursements (employee_id, category, amount, month, year, approved_by, created_at) VALUES `;
      const [Y, M] = month.split('-').map(Number);
      rows.forEach((r, i) => {
        const val = Number(calc(r.basic_salary).toFixed(2));
        insertSql += `(?,?,?,?,?,?, NOW())${i < rows.length - 1 ? ',' : ''}`;
        insertValues.push(r.employee_id, category || 'Other', val, M, Y, req.user?.id || null);
        auditLogs.push({
          user_id: req.user?.id || null,
          action_type: 'INSERT',
          target_table: 'reimbursements',
          after_state: { employee_id: r.employee_id, category, amount: val, month: M, year: Y }
        });
      });
    } else {
      throw new Error('Unsupported type');
    }

    // Bulk insert
    if (insertSql) await conn.query(insertSql, insertValues);

    // Bulk audit logs
    if (auditLogs.length) {
      const auditSql = `INSERT INTO audit_logs (user_id, action_type, target_table, after_state, status, created_at) VALUES ` +
        auditLogs.map(() => `(?,?,?,?, 'SUCCESS', NOW())`).join(',');
      const auditParams = [];
      auditLogs.forEach(l => {
        auditParams.push(l.user_id, l.action_type, l.target_table, JSON.stringify(l.after_state));
      });
      await conn.query(auditSql, auditParams);
    }

    await conn.commit();
    res.json({ ok:true, message:`Applied ${type} to ${employee_ids.length} employee(s)` });

  } catch (err) {
    await (conn?.rollback?.());
    console.error('applyCompensation error:', err);
    logAudit({
      user_id: req.user?.id || null,
      action_type: 'COMPENSATION_APPLY',
      target_table: 'compensation_batch',
      target_id: null,
      status: 'FAILED',
      error_message: err.message,
      after_state: { ...req.body }
    });
    res.status(500).json({ ok:false, message:'Failed to apply compensation' });
  } finally {
    conn?.release?.();
  }
};

/* ===================== LISTS (for grids) ===================== */

const listAllowances = async (req, res) => {
  try {
    const { employee_id } = req.query; // optional

    const query = employee_id
      ? `
        SELECT 
          id, employee_id, 
          name AS description,
          category, amount, taxable, frequency,
          effective_from, effective_to,
          status, created_at, updated_at
        FROM allowances
        WHERE employee_id=?
        ORDER BY created_at DESC
      `
      : `
        SELECT 
          a.id, a.employee_id, 
          a.name AS description,
          a.category, a.amount, a.taxable, a.frequency,
          a.effective_from, a.effective_to,
          a.status, a.created_at, a.updated_at,
          e.full_name
        FROM allowances a
        JOIN employees e ON e.id = a.employee_id
        ORDER BY a.created_at DESC
      `;

    const params = employee_id ? [employee_id] : [];
    const [rows] = await pool.query(query, params);
    res.json({ ok: true, data: rows });

  } catch (err) {
    console.error(err);
    logEvent({
      level: 'error', event_type: "GET_ALLOWANCE_LIST_FAILED",
      user_id: req.user?.id,
      severity: "ERROR",
      event_details: { error: err.message }
    })
    res.status(500).json({ ok: false, message: 'Failed to load allowances' });
  }
};

const listOvertime = async (req, res) => {
  try {
    const { employee_id } = req.query;
    const [rows] = employee_id
      ? await pool.query(
          `SELECT * FROM v_overtime_adjustments WHERE employee_id=? ORDER BY created_at DESC`,
          [employee_id]
        )
      : await pool.query(`
          SELECT * FROM v_overtime_adjustments ORDER BY created_at DESC
        `);
    res.json({ ok: true, data: rows });
  } catch (err) {
    console.error(err);
    logEvent({
      level: 'error', event_type: "GET_OVERTIME_LIST_FAILED",
      user_id: req.user?.id,
      severity: "ERROR",
      event_details: { error: err.message }
    })
    res.status(500).json({ ok: false, message: 'Failed to load overtime/adjustments' });
  }
};

/* ===================== DEDUCTIONS ===================== */

const listDeductions = async (req, res) => {
  try {
    const { month, year } = req.query;
    let where = '1=1';
    const params = [];

    if (month && year) {
      where += ' AND MONTH(d.effective_date)=? AND YEAR(d.effective_date)=?';
      params.push(Number(month), Number(year));
    }

    const [rows] = await pool.query(
      `SELECT d.id, d.employee_id, e.full_name AS employee_name,
              d.name, d.type, d.basis, d.percent, d.amount,
              d.status, d.effective_date, d.created_at, d.updated_at
         FROM deductions d
         JOIN employees e ON e.id = d.employee_id
        WHERE ${where}
        ORDER BY d.effective_date DESC, d.id DESC`,
      params
    );

    res.json({ ok: true, data: rows });
  } catch (err) {
    console.error(err);
    logEvent({
      level: 'error', event_type: "GET_DEDUCTIONS_LIST_FAILED",
      user_id: req.user?.id,
      severity: "ERROR",
      event_details: { error: err.message }
    })
    res.status(500).json({ ok: false, message: 'Failed to load deductions' });
  }
};

const createDeduction = async (req, res) => {
  try {
    const {
      employee_id,
      name,
      type,            // 'Tax' | 'Statutory' | 'Insurance' | 'Loan' | 'Other'
      basis,           // 'Fixed' | 'Percent'
      percent,         // if basis = Percent
      amount,          // if basis = Fixed
      effective_date,
      status = 'Active',
    } = req.body;

    if (!employee_id || !name || !type || !basis || !effective_date) {
      return res.status(400).json({ ok: false, message: 'Missing required fields' });
    }
    if (basis === 'Percent' && (percent == null || isNaN(percent))) {
      return res.status(400).json({ ok: false, message: 'Percent is required for Percent basis' });
    }
    if (basis === 'Fixed' && (amount == null || isNaN(amount))) {
      return res.status(400).json({ ok: false, message: 'Amount is required for Fixed basis' });
    }

    const [result] = await pool.query(
      `INSERT INTO deductions
        (employee_id, name, type, basis, percent, amount, effective_date, status, created_at, updated_at)
       VALUES (?,?,?,?,?,?,?,?,NOW(),NOW())`,
      [employee_id, name, type, basis, percent ?? null, amount ?? null, effective_date, status]
    );
    logAudit({
      user_id: req.user.id,
      action_type: "CREATE_DEDUCTION",
      target_table: "deductions",
      target_id: result.insertId,
      before_state: null,
      after_state: {
        employee_id,
        name,
        type,
        basis,
        percent: percent ?? null,
        amount: amount ?? null,
        effective_date,
        status
      },
      req,
      status: "SUCCESS"
    });

    res.json({ ok: true, id: result.insertId, message: 'Deduction saved' });
  } catch (err) {
    console.error(err);
    logAudit({
      user_id: req.user?.id || null,
      action_type: "CREATE_DEDUCTION",
      target_table: "deductions",
      target_id: null,
      before_state: null,
      after_state: req.body,
      req,
      status: "FAILURE"
    })

    res.status(500).json({ ok: false, message: 'Failed to save deduction' });
  }
};

// get one deduction by id
const getDeductionById = async (req, res) => {
  try {
    const { id } = req.params;
    const [[row]] = await pool.query(
      `SELECT id, employee_id, name, type, basis, percent, amount, status, effective_date
         FROM deductions WHERE id=?`,
      [id]
    );
    if (!row) return res.status(404).json({ ok: false, message: 'Not Found' });
    res.json({ ok: true, data: row });
  } catch (err) {
    console.error(err);
    logEvent({
      level: 'error', event_type: "GET_DEDUCTION_BYID",
      user_id: req.user.id,
      error_message: err.message,
      event_details: { err }
    })
    res.status(500).json({ ok: false, message: 'Failed to load deduction' });
  }
};

// update deduction
const updateDeduction = async (req, res) => {
  try {
    const { id } = req.params;
    const {
      employee_id,
      name,
      type,
      basis,
      percent,
      amount,
      effective_date,
      status = 'Active',
    } = req.body;

    if (!employee_id || !name || !type || !basis || !effective_date) {
      return res.status(400).json({ ok: false, message: 'Missing required fields' });
    }

    const [[before]] = await pool.query(`SELECT * FROM deductions WHERE id = ?`, [id]);
    if (!before) {
      return res.status(404).json({ ok: false, message: 'Deduction not found' });
    }

    await pool.query(
      `UPDATE deductions
          SET employee_id=?, name=?, type=?, basis=?, percent=?, amount=?, effective_date=?, status=?, updated_at=NOW()
        WHERE id=?`,
      [
        employee_id,
        name,
        type,
        basis,
        basis === 'Percent' ? percent : null,
        basis === 'Fixed' ? amount : null,
        effective_date,
        status,
        id,
      ]
    );

    const [[after]] = await pool.query(`SELECT * FROM deductions WHERE id = ?`, [id]);

    logAudit({
      user_id: req.user.id,
      action_type: "UPDATE_DEDUCTION",
      target_table: "deductions",
      target_id: id,
      before_state: before,
      after_state: after,
      req,
      status: "SUCCESS"
    });

    res.json({ ok: true, message: 'Deduction Updated' });
  } catch (err) {
    console.error(err);

    try {
      logAudit({
        user_id: req.user?.id || null,
        action_type: "UPDATE_DEDUCTION",
        target_table: "deductions",
        target_id: req.params.id,
        before_state: null,
        after_state: req.body,
        req,
        status: "FAILURE"
      });
    } catch (e) { }

    res.status(500).json({ ok: false, message: 'Failed to Update deduction' });
  }
};

// delete deduction
const deleteDeduction = async (req, res) => {
  try {
    const { id } = req.params;
    const [[before]] = await pool.query(`SELECT * FROM deductions WHERE id = ?`, [id]);
    if (!before) {
      return res.status(404).json({ ok: false, message: 'Deduction not found' });
    }

    await pool.query('DELETE FROM deductions WHERE id=?', [id]);

    logAudit({
      user_id: req.user.id,
      action_type: "DELETE_DEDUCTION",
      target_table: "deductions",
      target_id: id,
      before_state: before,
      after_state: null,
      req,
      status: "SUCCESS"
    });

    res.json({ ok: true, message: 'Deduction deleted' });
  } catch (err) {
    console.error(err);
    logAudit({
      user_id: req.user?.id || null,
      action_type: "DELETE_DEDUCTION",
      target_table: "deductions",
      target_id: req.params.id,
      before_state: null,
      after_state: null,
      req,
      status: "FAILURE"
    });

    res.status(500).json({ ok: false, message: 'Failed to delete deduction' });
  }
};


//***************************************************** */

// allowances ================================================


/* ========== BASIC SALARY (also used by percent preview) ========== */

const getBasicSalary = async (req, res) => {
  try {
    const { employee_id } = req.query;

    if (!employee_id) return res.status(400).json({ ok: false, message: 'employee_id required' });
    const [[row]] = await pool.query('SELECT basic_salary FROM salaries WHERE employee_id=?', [employee_id]);

    if (!row) {
      return res.status(404).json({ ok: false, message: 'Employee salary not found' });
    }

    res.json({ basic_salary: row?.basic_salary || 0 });
  } catch (err) {
    console.error('getBasicSalary error:', err);
    logEvent({
      level: 'error', event_type: "GET_OVERTIME_LIST_FAILED",
      user_id: req.user?.id || null,
      severity: "ERROR",
      event_details: { error: err.message }
    })
    res.status(500).json({ ok: false, message: 'Failed to fetch basic salary' });
  }
};

const setBasicSalary = async (req, res) => {
  const { employee_id, basic_salary } = req.body;
  if (!employee_id || basic_salary == null) {
    return res.status(400).json({ ok: false, message: 'employee_id and basic_salary required' });
  }
  await pool.query(
    `INSERT INTO salaries (employee_id, basic_salary)
     VALUES (?, ?)
     ON DUPLICATE KEY UPDATE basic_salary=VALUES(basic_salary), updated_at=NOW()`,
    [employee_id, basic_salary]
  );
  res.json({ ok: true, message: 'Basic salary set' });
};

/* ===================== CREATES (forms) ===================== */

// Add Allowance
const addAllowance = async (req, res) => {
  try {
    const {
      employee_id,
      description,                 // mapped to DB column `name`
      category = null,
      amount,
      taxable = 0,
      frequency = 'Monthly',
      effective_from = null,
      effective_to = null,
      status = 'Active',
    } = req.body;

    if (!employee_id || !description || amount == null) {
      return res.status(400).json({ ok: false, message: 'employee_id, description, amount are required' });
    }

    const [result] = await pool.query(
      `INSERT INTO allowances
        (employee_id, name, category, amount, taxable, frequency, effective_from, effective_to, status, created_at)
       VALUES (?,?,?,?,?,?,?,?,?, NOW())`,
      [employee_id, description, category, amount, Number(taxable) ? 1 : 0, frequency, effective_from, effective_to, status]
    );

    const allowanceId = result.insertId;

    logAudit({
      user_id: req.user.id,
      action_type: "CREATE",
      target_table: "allowances",
      target_id: allowanceId,
      before_state: null,
      after_state: {
        employee_id,
        description,
        category,
        amount,
        taxable: Number(taxable) ? 1 : 0,
        frequency,
        effective_from,
        effective_to,
        status
      },
      req,
      status: "SUCCESS"
    }).catch(console.error);

    res.json({ ok: true, message: 'Allowance added' });

  } catch (err) {
    console.error(err);

    logAudit({
      user_id: req.user?.id,
      action_type: "CREATE",
      target_table: "allowances",
      target_id: null,
      before_state: null,
      after_state: req.body,
      req,
      status: "FAILURE"
    }).catch(console.error);

    res.status(500).json({ ok: false, message: 'Failed to add allowance' });
  }
};

// ===================== ALLOWANCE CRUD METHODS =====================
// ADD THESE METHODS AFTER addAllowance AND BEFORE addBonus

// Get allowance by ID
const getAllowanceById = async (req, res) => {
  try {
    const { id } = req.params;
    
    const [[allowance]] = await pool.query(
      `SELECT 
         a.id, a.employee_id, 
         a.name AS description,
         a.category, a.amount, a.taxable, a.frequency,
         a.effective_from, a.effective_to,
         a.status, a.created_at, a.updated_at,
         e.full_name
       FROM allowances a
       JOIN employees e ON e.id = a.employee_id
       WHERE a.id = ?`,
      [id]
    );
    
    if (!allowance) {
      return res.status(404).json({ ok: false, message: 'Allowance not found' });
    }
    
    res.json({ ok: true, data: allowance });
  } catch (err) {
    console.error('getAllowanceById error:', err);
    res.status(500).json({ ok: false, message: 'Failed to fetch allowance' });
  }
};

// Update allowance
const updateAllowance = async (req, res) => {
  try {
    const { id } = req.params;
    const {
      employee_id,
      description,
      category = null,
      amount,
      taxable = 0,
      frequency = 'Monthly',
      effective_from = null,
      effective_to = null,
      status = 'Active',
    } = req.body;

    if (!employee_id || !description || amount == null) {
      return res.status(400).json({ ok: false, message: 'employee_id, description, amount are required' });
    }

    const [result] = await pool.query(
      `UPDATE allowances 
       SET employee_id = ?, name = ?, category = ?, amount = ?, 
           taxable = ?, frequency = ?, effective_from = ?, effective_to = ?, 
           status = ?, updated_at = NOW()
       WHERE id = ?`,
      [
        employee_id, 
        description, 
        category, 
        amount, 
        Number(taxable) ? 1 : 0, 
        frequency, 
        effective_from, 
        effective_to, 
        status, 
        id
      ]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ ok: false, message: 'Allowance not found' });
    }

    logAudit({
      user_id: req.user.id,
      action_type: "UPDATE_ALLOWANCE",
      target_table: "allowances",
      target_id: result.insertId,
      description: `Allowance updated (id=${id}, employee_id=${employee_id}, amount=${amount})`
    });

    res.json({ ok: true, message: 'Allowance updated successfully' });
  } catch (err) {
    console.error('updateAllowance error:', err);
    res.status(500).json({ ok: false, message: 'Failed to update allowance' });
  }
};

// Delete allowance
const deleteAllowance = async (req, res) => {
  try {
    const { id } = req.params;
    const [[before]] = await pool.query('SELECT * FROM allowances WHERE id = ?', [id]);
    if (!before) {
      return res.status(404).json({ ok: false, message: 'Allowance not found' });
    }

    const [result] = await pool.query('DELETE FROM allowances WHERE id = ?', [id]);
    // Add audit log
    logAudit({
      user_id: req.user.id,
      action_type: "DELETE_ALLOWANCE",
      target_table: "allowances",
      target_id: id,
      before_state: before,
      after_state: null,
      req,
      status: "SUCCESS"
    });
    res.json({ ok: true, message: 'Allowance deleted successfully' });
  } catch (err) {
    console.error('deleteAllowance error:', err);

    if (req.user) {
      logAudit({
        user_id: req.user.id,
        action_type: "DELETE_ALLOWANCE",
        target_table: "allowances",
        target_id: req.params.id,
        before_state: null,
        after_state: null,
        req,
        status: "FAILURE"
      });
    }

    res.status(500).json({ ok: false, message: 'Failed to delete allowance' });
  }
};

// (Removed old addOvertimeAdjustment that referenced non-existent columns)

const addBonus = async (req, res) => {
  const { employee_id, amount, reason, effective_date } = req.body;
  if (!employee_id || !amount || !effective_date)
    return res.status(400).json({ ok: false, message: 'employee_id, amount, effective_date required' });

  await pool.query(
    'INSERT INTO bonuses (employee_id, amount, reason, effective_date, created_by) VALUES (?,?,?,?,?)',
    [employee_id, amount, reason || null, effective_date, req.user?.id || null]
  );
  res.json({ ok: true, message: 'Bonus added' });
};

/* ===================== EARNINGS GRID ===================== */

const listEarnings = async (req, res) => {
  try {
  const { month, year } = req.query; // optional
  const [emps] = await pool.query(`
    SELECT e.id, e.full_name, d.name AS department, s.basic_salary
    FROM employees e
    LEFT JOIN departments d ON d.id = e.department_id
    LEFT JOIN salaries s ON s.employee_id = e.id
    WHERE e.status='Active'
  `);




  // **************************************************
  
  // Build allowance/overtime/bonus maps with optional period filter
  const params = [];
  let allowWhere = "WHERE status='Active'";
  let otWhere = 'WHERE 1=1'; // created_at based
  let bonusWhere = 'WHERE 1=1';

  if (month && year) {
    // Use effective_from/effective_to for allowances period inclusion
    const first = new Date(Number(year), Number(month) - 1, 1);
    const last = new Date(Number(year), Number(month), 0); // end of month
    const firstStr = first.toISOString().slice(0, 10);
    const lastStr = last.toISOString().slice(0, 10);

    allowWhere += ' AND (effective_from IS NULL OR effective_from <= ?)';
    allowWhere += ' AND (effective_to IS NULL OR effective_to >= ?)';
    params.push(lastStr, firstStr);

    otWhere += ' AND MONTH(created_at)=? AND YEAR(created_at)=?';
    bonusWhere += ' AND MONTH(effective_date)=? AND YEAR(effective_date)=?';
    params.push(Number(month), Number(year), Number(month), Number(year));
  }

  const [allowMapRows] = await pool.query(
    `SELECT employee_id, SUM(amount) AS total FROM allowances ${allowWhere} GROUP BY employee_id`,
    params.slice(0, allowWhere.includes('?') ? 2 : 0)
  );

  const restParams = month && year ? [Number(month), Number(year)] : [];
  const [otMapRows] = await pool.query(
    `SELECT employee_id, SUM(ot_hours * ot_rate) AS total
       FROM overtime_adjustments
      ${otWhere}
      GROUP BY employee_id`,
    restParams
  );
  const [bonusMapRows] = await pool.query(
    `SELECT employee_id, SUM(amount) AS total FROM bonuses ${bonusWhere} GROUP BY employee_id`,
    restParams
  );

  const toMap = (rows) => rows.reduce((m, r) => (m[r.employee_id] = Number(r.total || 0), m), {});
  const A = toMap(allowMapRows);
  const O = toMap(otMapRows);
  const B = toMap(bonusMapRows);

  const data = emps.map(e => {
    const basic = Number(e.basic_salary || 0);
    const overtime = O[e.id] || 0;
    const bonus = B[e.id] || 0;
    const allowances = A[e.id] || 0;
    const gross = basic + overtime + bonus + allowances;
    return {
      employee_id: e.id,
      name: e.full_name,
      department: e.department || '',
      basic_salary: basic,
      overtime, bonus, allowances,
      gross
    };
  });

  res.json({ ok: true, data });

  } catch (err) {
    console.error('listEarnings error:', err);
    logEvent({
      level: 'error', event_type: "GET_EARNING_LIST_FAILED",
      user_id: req.user?.id || null,
      severity: "ERROR",
      event_details: {
        month: req.query.month || 'all',
        year: req.query.year || 'all',
        error: err.message
      }
    })

    res.status(500).json({ ok: false, message: 'Failed to fetch earnings' });
  }
};

/* ===================== MONTH SUMMARY & RUN ===================== */

// ===================== MONTH SUMMARY & RUN =====================
// CHANGE: robust version that avoids large "IN (...)" lists and still supports filters.
//         We pull the filtered employees, then aggregate month-wide and pick from maps.
const monthSummary = async (req, res) => {
  const { month, year, q = '', department_id, department, grade_id, grade } = req.query;
  if (!month || !year) return res.status(400).json({ ok:false, message:'month, year required' });

  try {
    // --- Filters for EMPLOYEE list
    const clauses = ['e.status = "Active"'];
    const params  = [];

    if (q) { clauses.push('(e.full_name LIKE ? OR e.employee_code LIKE ? OR CAST(e.id AS CHAR) LIKE ?)'); params.push(`%${q}%`, `%${q}%`, `%${q}%`); }
    if (department_id) { clauses.push('e.department_id = ?'); params.push(Number(department_id)); }
    if (department)    { clauses.push('d.name LIKE ?');       params.push(`%${department}%`); }
    if (grade_id)      { clauses.push('e.grade_id = ?');      params.push(Number(grade_id)); }
    if (grade)         { clauses.push('g.grade_name = ?');    params.push(grade); }

    const whereSql = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';

    // Pull filtered employees with identity/org/basic salary
    const [emps] = await pool.query(`
      SELECT 
        e.id                AS employee_id,
        e.employee_code,
        e.full_name,
        e.grade_id,
        g.grade_name,
        d.name              AS department_name,
        s.basic_salary
      FROM employees e
      LEFT JOIN departments d ON d.id = e.department_id
      LEFT JOIN grades g      ON g.grade_id = e.grade_id
      LEFT JOIN salaries s    ON s.employee_id = e.id
      ${whereSql}
      ORDER BY e.full_name
    `, params);

    if (!emps.length) return res.json({ ok:true, data: [] });

    // --- Time bounds for the selected month
    const first = new Date(Number(year), Number(month) - 1, 1);
    const last  = new Date(Number(year), Number(month), 0);
    const firstStr = first.toISOString().slice(0,10);
    const lastStr  = last.toISOString().slice(0,10);

    // --- Build ID set for fast lookups (not used in SQL; we’ll filter in JS)
    const idSet = new Set(emps.map(e => e.employee_id));

    // --- Aggregations (month-wide), then filter in JS

    // Allowances active in the month (period-inclusive)
    const [allowAll] = await pool.query(`
      SELECT employee_id, SUM(amount) AS total
      FROM allowances
      WHERE status='Active'
        AND (effective_from IS NULL OR effective_from <= ?)
        AND (effective_to   IS NULL OR effective_to   >= ?)
      GROUP BY employee_id
    `, [lastStr, firstStr]);

    // Overtime strictly in the month (by created_at)
    const [otAll] = await pool.query(`
      SELECT employee_id, SUM(ot_hours * ot_rate) AS total
      FROM overtime_adjustments
      WHERE MONTH(created_at)=? AND YEAR(created_at)=?
      GROUP BY employee_id
    `, [Number(month), Number(year)]);

    // Bonuses effective in the month
    const [bonusAll] = await pool.query(`
      SELECT employee_id, SUM(amount) AS total
      FROM bonuses
      WHERE MONTH(effective_date)=? AND YEAR(effective_date)=?
      GROUP BY employee_id
    `, [Number(month), Number(year)]);

    // Deductions up to that month/year (same logic)
    const [dedAll] = await pool.query(`
      SELECT employee_id, SUM(
        CASE WHEN basis='Percent' AND percent IS NOT NULL
             THEN (percent/100.0) * COALESCE((SELECT basic_salary FROM salaries WHERE employee_id=deductions.employee_id LIMIT 1), 0)
             ELSE amount
        END
      ) AS total
      FROM deductions
      WHERE status='Active'
        AND MONTH(effective_date)<=? AND YEAR(effective_date)<=?
      GROUP BY employee_id
    `, [Number(month), Number(year)]);

    // Convert rows to maps and keep only the filtered employees
    const toMap = (rows) => {
      const m = Object.create(null);
      for (const r of rows) {
        if (idSet.has(r.employee_id)) m[r.employee_id] = Number(r.total || 0);
      }
      return m;
    };
    const A = toMap(allowAll);
    const O = toMap(otAll);
    const B = toMap(bonusAll);
    const D = toMap(dedAll);

    // Compose final dataset
    const data = emps.map(e => {
      const basic       = Number(e.basic_salary || 0);
      const allowances  = A[e.employee_id] || 0;
      const overtime    = O[e.employee_id] || 0;
      const bonus       = B[e.employee_id] || 0;
      const gross       = basic + allowances + overtime + bonus;
      const totalDeductions = D[e.employee_id] || 0;
      const net = gross - totalDeductions;

      return {
        employee_id     : e.employee_id,
        employee_code   : e.employee_code,
        full_name       : e.full_name,
        department_name : e.department_name || '',
        grade_id        : e.grade_id,
        grade_name      : e.grade_name || '',
        basic,
        allowances,
        overtime,
        bonus,
        gross,
        totalDeductions,
        net
      };
    });

    res.json({ ok:true, data });
  } catch (err) {
    console.error('monthSummary error:', err);
    res.status(500).json({ ok:false, message:'Failed to build month summary' });
  }
};


//=======================================================

const runPayrollForMonth = async (req, res) => {
  const { month, year } = req.body;
  if (!month || !year) return res.status(400).json({ ok:false, message:'month, year required' });

  // reuse summary
  const fakeReq = { query: { month, year } };
  const fakeRes = { json: (p) => p };
  const { data } = await monthSummary(fakeReq, fakeRes);

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    for (const r of data) {
      await conn.query(`
        INSERT INTO payroll_cycles (employee_id, period_month, period_year, gross_earnings, total_deductions, net_salary, generated_at)
        VALUES (?,?,?,?,?,?, NOW())
      `, [r.employee_id, month, year, r.gross, r.totalDeductions, r.net]);
    }
    await conn.commit();
  } catch (e) {
    await conn.rollback();
    console.error(e);
    logEvent({
      level: 'error', event_type: "RUN_PAYROLL_FAILED",
      user_id: req.user?.id || null,
      severity: "ERROR",
      event_details: { error: e.message, month, year }
    })
    return res.status(500).json({ ok: false, message: 'Failed to run payroll' });
  } finally {
    conn.release();
  }

  res.json({ ok:true, message: `Payroll run stored for ${month}/${year}`, count: data.length });
};

/* ===================== PAYSLIP ===================== */

const generatePayslip = async (req, res) => {
  try {
  const { employee_id, month, year } = req.query;
  if (!employee_id || !month || !year) {
    return res.status(400).json({ ok: false, message: 'employee_id, month, year required' });
  }

  const [[emp]] = await pool.query('SELECT full_name, email FROM employees WHERE id=?', [employee_id]);
  if (!emp) return res.status(404).json({ ok: false, message: 'Employee not found' });

  const [[salary]] = await pool.query('SELECT basic_salary FROM salaries WHERE employee_id=?', [employee_id]);

  // Allowances active in the month
  const first = new Date(Number(year), Number(month) - 1, 1);
  const last = new Date(Number(year), Number(month), 0);
  const firstStr = first.toISOString().slice(0,10);
  const lastStr = last.toISOString().slice(0,10);

  const [allowances] = await pool.query(
    `SELECT name, amount FROM allowances
      WHERE employee_id=? AND status='Active'
        AND (effective_from IS NULL OR effective_from <= ?)
        AND (effective_to IS NULL OR effective_to >= ?)`,
    [employee_id, lastStr, firstStr]
  );

  const [deductions] = await pool.query(
    `SELECT name, basis, percent, amount
       FROM deductions
      WHERE employee_id=? AND status='Active'
        AND MONTH(effective_date)<=? AND YEAR(effective_date)<=?`,
    [employee_id, Number(month), Number(year)]
  );

  // Overtime in the month (by created_at)
  const [otRows] = await pool.query(
    `SELECT ot_hours, ot_rate, adjustment_reason, created_at
       FROM overtime_adjustments
      WHERE employee_id=? AND MONTH(created_at)=? AND YEAR(created_at)=?`
    , [employee_id, Number(month), Number(year)]
  );

  const otTotal = otRows.reduce((a, r) => a + Number(r.ot_hours || 0) * Number(r.ot_rate || 0), 0);
  const basic = Number(salary?.basic_salary || 0);
  const allowTotal = allowances.reduce((a, b) => a + Number(b.amount || 0), 0);

  const dedTotal = await (async () => {
    let sum = 0;
    for (const d of deductions) {
      if (d.basis === 'Percent' && d.percent != null) sum += (Number(d.percent) / 100) * basic;
      else sum += Number(d.amount || 0);
    }
    return sum;
  })();

  const gross = basic + allowTotal + otTotal;
  const net = gross - dedTotal;

  await pool.query(
    'INSERT INTO payroll_cycles (employee_id, period_month, period_year, gross_earnings, total_deductions, net_salary, generated_at) VALUES (?,?,?,?,?,?, NOW())',
    [employee_id, month, year, gross, dedTotal, net]
  );

    logAudit({
      user_id: req.user?.id || null,
      action_type: 'GENERATE_PAYSLIP',
      target_table: 'payroll_cycles',
      target_id: payrollResult.insertId,
      after_state: { employee_id, month, year, gross, total_deductions: dedTotal, net },
    });

  const doc = new PDFDocument();
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `inline; filename="payslip_${emp.full_name}_${month}_${year}.pdf"`);
  doc.pipe(res);

  doc.fontSize(18).text('Payslip', { align: 'center' });
  doc.moveDown();
  doc.fontSize(12).text(`Employee: ${emp.full_name}`);
  doc.text(`Email: ${emp.email}`);
  doc.text(`Period: ${month}/${year}`);
  doc.moveDown();
  doc.text(`Basic Salary: ${basic.toFixed(2)}`);
  allowances.forEach(a => doc.text(`Allowance - ${a.name}: ${Number(a.amount).toFixed(2)}`));
  otRows.forEach(o => doc.text(`Overtime - ${Number(o.ot_hours)}h × ${Number(o.ot_rate).toFixed(2)} = ${(Number(o.ot_hours)*Number(o.ot_rate)).toFixed(2)}${o.adjustment_reason ? ` (${o.adjustment_reason})` : ''}`));
  deductions.forEach(d => {
    if (d.basis === 'Percent' && d.percent != null) {
      doc.text(`Deduction - ${d.name} (${Number(d.percent)}% of basic): -${((Number(d.percent)/100)*basic).toFixed(2)}`);
    } else {
      doc.text(`Deduction - ${d.name}: -${Number(d.amount || 0).toFixed(2)}`);
    }
  });
  doc.moveDown();
  doc.text(`Gross: ${gross.toFixed(2)}`);
  doc.text(`Total Deductions: ${dedTotal.toFixed(2)}`);
  doc.text(`Net Salary: ${net.toFixed(2)}`, { underline: true });
  doc.end();

  } catch (err) {
    console.error('generatePayslip error:', err);
    logEvent({
      level: 'error', event_type: "GENERATE_PAYSLIP_FAILED",
      user_id: req.user?.id || null,
      severity: "ERROR",
      event_details: { error: err.message, query: req.query }
    })
    res.status(500).json({ ok: false, message: 'Failed to generate payslip' });
  }
};

/* ===================== EXPORTS ===================== */

module.exports = {
  // OT API
  getGrades,
  getOvertimeRulesByGrade,
  gradeNameOf,
  upsertOvertimeRule,
  searchEmployees,
  createOvertimeAdjustment,
  listOvertimeAdjustmentsByGrade,
  listOvertimeAdjustmentsByEmployee,

  // lists
  listAllowances,
  listDeductions,
  listOvertime,

  // creates
  createDeduction,
  addAllowance,
  getAllowanceById,
  updateAllowance,
  deleteAllowance,
  addBonus,

  // single item ops for deductions
  getDeductionById,
  updateDeduction,
  deleteDeduction,

  // salary helpers
  setBasicSalary,
  getBasicSalary,

  // earnings/summary/run
  listEarnings,
  monthSummary,
  runPayrollForMonth,

  // payslip
  generatePayslip,

    // NEW: compensation + search
  searchEmployeesAdvanced,
  listDepartments,
  previewCompensation,
  applyCompensation,



};
