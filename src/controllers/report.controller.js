const { level } = require('winston');
const pool = require('../config/db');
const logEvent = require('../utils/event');

const getMonthlyTotalData = async (req, res) => {
  try {
    const month = parseInt(req.query.month, 10) || new Date().getMonth() + 1;
    const year = parseInt(req.query.year, 10) || new Date().getFullYear();
    const departmentId = req.query.departmentId ? parseInt(req.query.departmentId, 10) : null;

    let sql = `
      SELECT 
        SUM(total_net_salary) AS net_salary,
        SUM(total_allowances) AS allowances,
        SUM(total_deductions) AS deductions,
        SUM(total_bonuses) AS bonuses,
        SUM(total_reimbursements) AS reimbursements
      FROM department_financial_summary
      WHERE period_month = ? AND period_year = ?
    `;
    const params = [month, year];
    if (departmentId) {
      sql += ` AND department_id = ?`;
      params.push(departmentId);
    }

    const [rows] = await pool.query(sql, params);
    const totals = rows?.[0] || {};

    res.json({
      period: `${year}-${String(month).padStart(2, '0')}`,
      net_salary: totals.net_salary || 0,
      allowances: totals.allowances || 0,
      deductions: totals.deductions || 0,
      bonuses: totals.bonuses || 0,
      reimbursements: totals.reimbursements || 0
    });

  } catch (err) {
    console.error("Error in getMonthlyTotalData:", err);
    logEvent({
      level: 'error', event_type: "GET_PAYROLL_SUMMARY", user_id: req.user?.id || null, event_details: { err }, error_message: err.message
    })
    res.status(500).json({ error: "Database query failed" });
  }
};

const getSalaryRange = async (req, res) => {
  try {
    const sql = `
      SELECT basic_salary
      FROM salaries;
    `;
    const [rows] = await pool.query(sql);
    res.json(rows.map(r => r.basic_salary));
  } catch (err) {
    console.error('Failed to fetch salaries:', err);
    logEvent({level:'error',  event_type: "GET_SALARY_LIST",
      user_id: req.user?.id || null,
      event_details: { err },
      error_message: err.message })
    res.status(500).json({ error: 'Database query failed' });
  }
};

const compensateTrend = async (req, res) => {
  try {
    // Number of months from query, default to 6
    const months = parseInt(req.query.months, 10) || 6;

    const sql = `
      SELECT 
        period_year,
        period_month,
        SUM(total_net_salary) AS net_salary,
        SUM(total_allowances) AS allowances,
        SUM(total_deductions) AS deductions,
        SUM(total_bonuses) AS bonuses,
        SUM(total_reimbursements) AS reimbursements
      FROM department_financial_summary
      WHERE STR_TO_DATE(CONCAT(period_year, '-', period_month, '-01'), '%Y-%m-%d') >= DATE_SUB(CURDATE(), INTERVAL ? MONTH)
      GROUP BY period_year, period_month
      ORDER BY period_year, period_month
    `;

    const [rows] = await pool.query(sql, [months]);
    res.json(rows);
  } catch (err) {
    console.error("Error in compensateTrend:", err);
    logEvent({level:'error',  event_type: "GET_COMPENSATE_TRENDS",
      user_id: req.user?.id || null,
      event_details: { err },
      error_message: err.message})
    res.status(500).json({ error: 'Database query failed' });
  }
};

const getDeductionsByType = async (req, res) => {
  try {
    const year = parseInt(req.query.year) || new Date().getFullYear();
    const departmentId = req.query.departmentId ? parseInt(req.query.departmentId, 10) : null;

    let sql = `
      SELECT d.type, SUM(d.amount) AS total_amount
      FROM deductions d
      INNER JOIN employees e ON d.employee_id = e.id
      WHERE YEAR(d.effective_date) = ?
    `;

    const params = [year];

    if (departmentId) {
      sql += ` AND e.department_id = ?`;
      params.push(departmentId);
    }

    sql += ` GROUP BY d.type`;

    const [rows] = await pool.query(sql, params);
    res.json(rows);
  } catch (err) {
    console.error('Error fetching deductions by type:', err);
    logEvent({
      level: 'error', event_type: "GET_DEDUCTION_BY_TYPE",
      user_id: req.user?.id || null,
      event_details: { err },
      error_message: err.message
    })
    res.status(500).json({ error: 'Database query failed' });
  }
};

const getAllowancesByType = async (req, res) => {
  try {
    const month = parseInt(req.query.month, 10) || new Date().getMonth() + 1;
    const year = parseInt(req.query.year, 10) || new Date().getFullYear();
    const departmentId = req.query.departmentId ? parseInt(req.query.departmentId, 10) : null;

    let sql = `
      SELECT a.category AS type, SUM(a.amount) AS total_amount
      FROM allowances a
      INNER JOIN employees e ON a.employee_id = e.id
      WHERE MONTH(a.effective_from) = ? AND YEAR(a.effective_from) = ?
    `;

    const params = [month, year];

    if (departmentId) {
      sql += ` AND e.department_id = ?`;
      params.push(departmentId);
    }

    sql += ` GROUP BY a.category`;

    const [rows] = await pool.query(sql, params);
    res.json(rows);
  } catch (err) {
    console.error('Error fetching allowances by type:', err);
    logEvent({
      level: 'error', event_type: "GET_ALLOWANCE_BY_TYPE",
      user_id: req.user?.id || null,
      event_details: { err },
      error_message: err.message
    })

    res.status(500).json({ error: 'Database query failed' });
  }
};

const getBonusesByType = async (req, res) => {
  try {
    const year = parseInt(req.query.year, 10) || new Date().getFullYear();
    const quarter = parseInt(req.query.quarter, 10);
    const departmentId = req.query.departmentId ? parseInt(req.query.departmentId, 10) : null;

    let sql = `
      SELECT b.reason AS type, SUM(b.amount) AS total_amount
      FROM bonuses b
      INNER JOIN employees e ON b.employee_id = e.id
      WHERE YEAR(b.effective_date) = ?
    `;

    const params = [year];

    if (quarter >= 1 && quarter <= 4) {
      const startMonth = (quarter - 1) * 3 + 1;
      const endMonth = startMonth + 2;
      sql += ` AND MONTH(b.effective_date) BETWEEN ? AND ?`;
      params.push(startMonth, endMonth);
    }

    if (departmentId) {
      sql += ` AND e.department_id = ?`;
      params.push(departmentId);
    }

    sql += ` GROUP BY b.reason`;

    const [rows] = await pool.query(sql, params);
    res.json(rows);
  } catch (err) {
    console.error('Error fetching bonuses by type:', err);
    logEvent({
      level: 'error', event_type: "GET_BONUS_BY_TYPE",
      user_id: req.user?.id || null,
      event_details: { err },
      error_message: err.message
    })
    res.status(500).json({ error: 'Database query failed' });
  }
};


//get employee insights with optional department filter
const getEmployeeInsights = async (req, res) => {
  try {
    const { departmentId } = req.query;

    // Prepare department filter and query parameters
    const deptFilter = departmentId ? 'WHERE department_id = ?' : '';
    const queryParams = departmentId ? [departmentId] : [];

    // Get total employees
    const [[{ total_employees }]] = await pool.query(
      `SELECT COUNT(*) AS total_employees FROM employees ${deptFilter}`,
      queryParams
    );

    // Get total departments
    const [[{ total_departments }]] = await pool.query(
      'SELECT COUNT(*) AS total_departments FROM departments'
    );

    // Get gender-wise employee count
    const [genderCounts] = await pool.query(
      `SELECT gender, COUNT(*) AS total FROM employees ${deptFilter} GROUP BY gender`,
      queryParams
    );

    // Get employment type count
    const [typeCounts] = await pool.query(
      `SELECT employment_type AS type, COUNT(*) AS total FROM employees ${deptFilter} GROUP BY employment_type`,
      queryParams
    );

    // Get employee grade count
    const [gradeCounts] = await pool.query(
      `SELECT grade, COUNT(*) AS total FROM employees ${deptFilter} GROUP BY grade`,
      queryParams
    );

    res.json({
      total_employees,
      total_departments,
      gender: genderCounts,
      types: typeCounts,
      grades: gradeCounts
    });

  } catch (error) {
    console.error('Error fetching employee insights:', error);
    logEvent({
      level: 'error', event_type: "GET_EMPLOYEE_SUMMARY",
      user_id: req.user?.id || null,
      event_details: { err },
      error_message: err.message
    })
    res.status(500).json({ ok: false, message: 'Database query failed' });
  }
};

// Controller to fetch all departments
const getAllDepartments = async (req, res) => {
  try {
    const [departments] = await pool.query('SELECT * FROM departments');
    res.json(departments);
  } catch (error) {
    console.error('Error fetching departments:', error);
    res.status(500).json({ error: 'Database query failed' });
  }
};

module.exports = {
  getMonthlyTotalData, getSalaryRange, compensateTrend, getAllowancesByType, getBonusesByType, getDeductionsByType, getEmployeeInsights, getAllDepartments
};
