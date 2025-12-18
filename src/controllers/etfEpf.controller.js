// src/controllers/etfEpf.controller.js
const pool = require('../config/db');
const logAudit = require('../utils/audit');
const logEvent = require('../utils/event');

// ===================== ETF/EPF MANAGEMENT =====================

/**
 * Utility function to aggregate gross salary components for a given employee and period.
 * Gross Salary for EPF calculation = Basic Salary + Allowances + Overtime + Bonuses
 */
const getGrossComponentsForPeriod = async (employee_id, year, month) => {
  // Use dates for inclusive period filtering
  const periodEndStr = new Date(Number(year), Number(month), 0).toISOString().slice(0, 10);
  const periodStartStr = `${year}-${String(month).padStart(2, '0')}-01`; 

  // 1. Fetch Basic Salary (using the latest record)
  const [[salaryRecord]] = await pool.query(`
    SELECT basic_salary FROM salaries WHERE employee_id = ? ORDER BY id DESC LIMIT 1
  `, [employee_id]).catch(() => [[]]);
  
  const basicSalary = Number(salaryRecord?.basic_salary || 0);

  // 2. Aggregate monthly components (Allowances, Overtime, Bonuses)
  const [components] = await pool.query(`
    SELECT
      (
        -- Allowances active in this period (effective_from <= period_end AND effective_to >= period_start)
        SELECT COALESCE(SUM(amount), 0.00) FROM allowances
        WHERE employee_id = ? 
          AND status = 'Active' 
          AND (effective_from IS NULL OR effective_from <= ?)
          AND (effective_to IS NULL OR effective_to >= ?)
      ) AS allowances_sum,
      (
        -- Overtime earned in this period (created_at in this month)
        SELECT COALESCE(SUM(ot_hours * ot_rate), 0.00) FROM overtime_adjustments
        WHERE employee_id = ?
          AND MONTH(created_at) = ? AND YEAR(created_at) = ?
      ) AS overtime_sum,
      (
        -- Bonuses effective in this period (effective_date in this month)
        SELECT COALESCE(SUM(amount), 0.00) FROM bonuses
        WHERE employee_id = ?
          AND MONTH(effective_date) = ? AND YEAR(effective_date) = ?
      ) AS compensation_sum
  `, [
    // Parameters for Allowances
    employee_id, periodEndStr, periodStartStr, 
    // Parameters for Overtime
    employee_id, month, year, 
    // Parameters for Compensation/Bonus
    employee_id, month, year,
  ]);

  const { allowances_sum, overtime_sum, compensation_sum } = components[0];

  const allowances = Number(allowances_sum);
  const overtime = Number(overtime_sum);
  const compensation = Number(compensation_sum);

  // Gross Salary for EPF = Basic + All Fixed/Variable Earnings (Allowances + Overtime + Bonuses)
  const grossForEpf = basicSalary + allowances + overtime + compensation;

  return {
    basic_salary: basicSalary,
    allowances_sum: allowances,
    overtime_sum: overtime,
    compensation_sum: compensation,
    gross_salary_for_epf: grossForEpf,
  };
};

// ===================== ETF/EPF PROCESSING FLOW =====================

/**
 * Get list of employees with all required data for ETF/EPF processing for a given month.
 * Logic includes filtering by joining date (must have joined on or before the period).
 */
const getEtfEpfProcessList = async (req, res) => {
  try {
    const { year, month } = req.query;
    const numYear = Number(year);
    const numMonth = Number(month);

    if (!numYear || !numMonth) return res.status(400).json({ ok: false, message: 'Year and Month required' });

    const [rows] = await pool.query(`
      SELECT 
        e.id AS employee_id,
        e.employee_code,
        e.full_name,
        e.joining_date,
        d.name AS department_name,
        COALESCE(s.basic_salary, 0) AS basic_salary,
        COALESCE(ee.epf_contribution_rate, 8.00) AS epf_rate,
        COALESCE(ee.employer_epf_rate, 12.00) AS employer_epf_rate,
        COALESCE(ee.etf_contribution_rate, 3.00) AS etf_rate
      FROM employees e
      LEFT JOIN departments d ON d.id = e.department_id
      LEFT JOIN salaries s ON e.id = s.employee_id AND s.id = (
          SELECT MAX(id) FROM salaries WHERE employee_id = e.id
      )
      LEFT JOIN employee_etf_epf ee ON ee.employee_id = e.id
      WHERE e.status = 'Active' 
        AND (YEAR(e.joining_date) < ? OR (YEAR(e.joining_date) = ? AND MONTH(e.joining_date) <= ?))
      ORDER BY e.full_name
    `, [numYear, numYear, numMonth]);

    const results = rows.map(record => {
      const basic = Number(record.basic_salary);
      const epfEmployee = (basic * record.epf_rate) / 100;
      const epfEmployer = (basic * record.employer_epf_rate) / 100;
      const etfAmount = (basic * record.etf_rate) / 100;

      return {
        ...record,
        basic_salary: basic.toFixed(2),
        epf_employee_amount: epfEmployee.toFixed(2),
        epf_employer_share: epfEmployer.toFixed(2),
        etf_employer_contribution: etfAmount.toFixed(2),
        total_statutory: (epfEmployee + epfEmployer + etfAmount).toFixed(2)
      };
    });

    res.json({ ok: true, data: results });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: 'Failed to load process list' });
  }
};

/**
 * Process (record) the calculated ETF/EPF payments for selected employees.
 * This inserts a transaction into `payroll_etf_epf_transactions`.
 */
const processEtfEpfPayments = async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const { month, year, employee_ids } = req.body;
    const user_id = req.user.id; 
    const numYear = Number(year);
    const numMonth = Number(month);

    if (!numMonth || !numYear || !employee_ids || employee_ids.length === 0) {
      return res.status(400).json({ ok: false, message: 'Invalid payload: month, year, and employee_ids required' });
    }

    await conn.beginTransaction();
    const processed = [];

    for (const employee_id of employee_ids) {
      // A. Check if already processed
      const [[existing]] = await conn.query(
        'SELECT id FROM payroll_etf_epf_transactions WHERE employee_id = ? AND period_year = ? AND period_month = ?',
        [employee_id, numYear, numMonth]
      );

      if (existing) {
        console.log(`Employee ${employee_id} already processed for ${numYear}-${numMonth}. Skipping.`);
        continue;
      }

      // B. Fetch rates
      const [[rates]] = await conn.query(
        'SELECT epf_number, etf_number, COALESCE(epf_contribution_rate, 8.00) AS epf_rate, COALESCE(employer_epf_rate, 12.00) AS employer_epf_rate, COALESCE(etf_contribution_rate, 3.00) AS etf_rate FROM employee_etf_epf WHERE employee_id = ?',
        [employee_id]
      );

      if (!rates) {
        logEvent({ level: 'warn', event_type: "ETF_EPF_RATES_MISSING", user_id, severity: "WARNING", event_details: { employee_id, month, year } });
        continue; // Skip employees without EPF/ETF configuration
      }

      // C. Re-fetch Gross Salary for transactional integrity
      const grossComponents = await getGrossComponentsForPeriod(employee_id, numYear, numMonth);
      const gross = grossComponents.gross_salary_for_epf;
      
      if(gross <= 0) {
         logEvent({ level: 'warn', event_type: "ETF_EPF_GROSS_ZERO", user_id, severity: "WARNING", event_details: { employee_id, month, year } });
         continue;
      }

      // D. Calculate Contributions
      const employeeEpf = (gross * rates.epf_rate) / 100;
      const employerEpf = (gross * rates.employer_epf_rate) / 100;
      const employerEtf = (gross * rates.etf_rate) / 100;
      
      // E. Insert transaction record (Column names aligned with provided SQL schema)
      const [resIns] = await conn.query(
        `INSERT INTO payroll_etf_epf_transactions 
          (employee_id, period_year, period_month, gross_salary, 
           employee_epf_amount, epf_employer_share, employer_etf_amount, processed_by)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [employee_id, numYear, numMonth, gross.toFixed(2), 
         employeeEpf.toFixed(2), employerEpf.toFixed(2), employerEtf.toFixed(2), user_id]
      );

      processed.push(employee_id);
    }

    await conn.commit();
    logAudit({ 
        user_id, 
        action_type: "PROCESS_ETF_EPF_PAYMENT", 
        target_table: "payroll_etf_epf_transactions", 
        target_id: processed.join(','), 
        after_state: { month: numMonth, year: numYear, processed }, 
        req, 
        status: "SUCCESS" 
    });
    res.json({ ok: true, message: `Successfully processed ${processed.length} records.`, processed_count: processed.length });
  } catch (err) {
    await conn.rollback();
    console.error('processEtfEpfPayments error:', err);
    logEvent({ level: 'error', event_type: "PROCESS_ETF_EPF_FAILED", user_id: req.user?.id || null, severity: "ERROR", error_message: err.message, event_details: { error: err.message, body: req.body } });
    res.status(500).json({ ok: false, message: 'Failed to process ETF/EPF payments' });
  } finally {
    conn.release();
  }
};

// ===================== EXISTING FUNCTIONS (REMAINS UNCHANGED) =====================

// Get all employees with ETF/EPF details
const getEtfEpfRecords = async (req, res) => {
  try {
    const [rows] = await pool.query(`
      SELECT 
        e.id AS employee_id,
        e.employee_code,
        e.full_name,
        e.designation,
        e.created_at AS emp_created_at, 
        d.name AS department,
        e.epf_no AS employee_epf_no,
        ee.epf_number AS etf_epf_epf_number,
        ee.etf_number,
        ee.epf_effective_date,
        ee.etf_effective_date,
        ee.epf_status,
        ee.etf_status,
        ee.epf_contribution_rate,
        ee.employer_epf_rate,
        ee.etf_contribution_rate,
        ee.id AS etf_epf_id,
        ee.created_at,
        ee.updated_at,
        CASE 
          WHEN ee.id IS NOT NULL THEN 'Yes' 
          ELSE 'No' 
        END AS has_etf_epf_record
      FROM employees e
      LEFT JOIN departments d ON d.id = e.department_id
      LEFT JOIN employee_etf_epf ee ON ee.employee_id = e.id
      WHERE e.status = 'Active'
      ORDER BY e.full_name
    `);

    // Transform data: Force effective dates to be emp_created_at if null
    const transformedData = rows.map(row => ({
      id: row.etf_epf_id || null,
      employee_id: row.employee_id,
      employee_code: row.employee_code,
      full_name: row.full_name,
      designation: row.designation,
      department: row.department,
      
      epf_number: row.etf_epf_epf_number || row.employee_epf_no,
      etf_number: row.etf_number,
      
      // LOGIC CHANGE: If specific effective date is null, use employee creation date
      epf_effective_date: row.epf_effective_date || row.emp_created_at,
      etf_effective_date: row.etf_effective_date || row.emp_created_at,
      
      epf_status: row.epf_status || 'Not Set',
      etf_status: row.etf_status || 'Not Set',
      epf_contribution_rate: row.epf_contribution_rate,
      employer_epf_rate: row.employer_epf_rate,
      etf_contribution_rate: row.etf_contribution_rate,
      has_etf_epf_record: row.has_etf_epf_record
    }));

    res.json({ ok: true, data: transformedData });
  } catch (err) {
    console.error('getEtfEpfRecords error:', err);
    res.status(500).json({ ok: false, message: 'Failed to fetch ETF/EPF records' });
  }
};

// Get Payment History for View Button
const getEmployeePaymentHistory = async (req, res) => {
  try {
    const { employeeId } = req.params;
    const [rows] = await pool.query(`
      SELECT 
        id, 
        basic_salary, 
        effective_date,
        (basic_salary * 0.08) as epf_emp,
        (basic_salary * 0.12) as epf_employer,
        (basic_salary * 0.03) as etf
      FROM salaries 
      WHERE employee_id = ? 
      ORDER BY effective_date DESC
    `, [employeeId]);
    res.json({ ok: true, data: rows });
  } catch (err) {
    res.status(500).json({ ok: false, message: 'Failed to fetch history' });
  }
};

// Get ETF/EPF record by ID
const getEtfEpfById = async (req, res) => {
  try {
    const { id } = req.params;
    const [[record]] = await pool.query(`
      SELECT ee.*, e.full_name, e.employee_code, e.epf_no AS employee_epf_no, d.name AS department
      FROM employee_etf_epf ee
      JOIN employees e ON e.id = ee.employee_id
      LEFT JOIN departments d ON d.id = e.department_id
      WHERE ee.id = ?
    `, [id]);

    if (!record) return res.status(404).json({ ok: false, message: 'Not found' });
    res.json({ ok: true, data: record });
  } catch (err) {
    res.status(500).json({ ok: false, message: 'Failed to fetch record' });
  }
};

// Create ETF/EPF record
const createEtfEpfRecord = async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const {
      employee_id, epf_number, etf_number, 
      epf_effective_date, etf_effective_date,
      epf_status = 'Active', etf_status = 'Active',
      epf_contribution_rate = 8.00, employer_epf_rate = 12.00, etf_contribution_rate = 3.00
    } = req.body;

    if (!employee_id) return res.status(400).json({ ok: false, message: 'Employee ID required' });

    await conn.beginTransaction();

    const [[existing]] = await conn.query('SELECT id FROM employee_etf_epf WHERE employee_id = ?', [employee_id]);
    if (existing) {
      await conn.rollback();
      return res.status(400).json({ ok: false, message: 'Record already exists' });
    }

    const [[emp]] = await conn.query('SELECT epf_no, created_at FROM employees WHERE id = ?', [employee_id]);
    const finalEpf = epf_number || emp?.epf_no;
    
    // Fallback to created_at if dates are missing
    const finalEpfDate = epf_effective_date || (emp ? emp.created_at : null);
    const finalEtfDate = etf_effective_date || (emp ? emp.created_at : null);

    if (!finalEpf) {
      await conn.rollback();
      return res.status(400).json({ ok: false, message: 'EPF number required' });
    }

    const [resIns] = await conn.query(
      `INSERT INTO employee_etf_epf 
        (employee_id, epf_number, etf_number, epf_effective_date, etf_effective_date, 
         epf_status, etf_status, epf_contribution_rate, employer_epf_rate, etf_contribution_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [employee_id, finalEpf, etf_number, finalEpfDate, finalEtfDate, epf_status, etf_status, epf_contribution_rate, employer_epf_rate, etf_contribution_rate]
    );

    await conn.commit();
    logAudit({ user_id: req.user.id, action_type: "CREATE_ETF_EPF", target_table: "employee_etf_epf", target_id: resIns.insertId, before_state: null, after_state: req.body, req, status: "SUCCESS" });
    res.json({ ok: true, message: 'Created successfully', id: resIns.insertId });
  } catch (err) {
    await conn.rollback();
    res.status(500).json({ ok: false, message: 'Failed to create' });
  } finally {
    conn.release();
  }
};

// Update ETF/EPF record
const updateEtfEpfRecord = async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const { id } = req.params;
    const {
      epf_number, etf_number, epf_effective_date, etf_effective_date,
      epf_status, etf_status, epf_contribution_rate, employer_epf_rate, etf_contribution_rate
    } = req.body;

    await conn.beginTransaction();
    const [[before]] = await conn.query('SELECT * FROM employee_etf_epf WHERE id = ?', [id]);
    if (!before) { await conn.rollback(); return res.status(404).json({ ok: false }); }

    await conn.query(
      `UPDATE employee_etf_epf 
        SET epf_number = COALESCE(?, epf_number), 
            etf_number = COALESCE(?, etf_number), 
            epf_effective_date = COALESCE(?, epf_effective_date), 
            etf_effective_date = COALESCE(?, etf_effective_date),
            epf_status = COALESCE(?, epf_status), 
            etf_status = COALESCE(?, etf_status),
            epf_contribution_rate = COALESCE(?, epf_contribution_rate),
            employer_epf_rate = COALESCE(?, employer_epf_rate),
            etf_contribution_rate = COALESCE(?, etf_contribution_rate),
            updated_at = NOW()
        WHERE id = ?`,
      [epf_number, etf_number, epf_effective_date, etf_effective_date, epf_status, etf_status, epf_contribution_rate, employer_epf_rate, etf_contribution_rate, id]
    );

    await conn.commit();
    logAudit({ user_id: req.user.id, action_type: "UPDATE_ETF_EPF", target_table: "employee_etf_epf", target_id: id, before_state: before, after_state: req.body, req, status: "SUCCESS" });
    res.json({ ok: true, message: 'Updated successfully' });
  } catch (err) {
    await conn.rollback();
    res.status(500).json({ ok: false, message: 'Failed to update' });
  } finally {
    conn.release();
  }
};

// Delete ETF/EPF record
const deleteEtfEpfRecord = async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const { id } = req.params;
    await conn.beginTransaction();
    const [[record]] = await conn.query('SELECT * FROM employee_etf_epf WHERE id = ?', [id]);
    if (!record) { await conn.rollback(); return res.status(404).json({ok:false}); }
    await conn.query('DELETE FROM employee_etf_epf WHERE id = ?', [id]);
    await conn.commit();
    logAudit({ user_id: req.user.id, action_type: "DELETE_ETF_EPF", target_table: "employee_etf_epf", target_id: id, before_state: record, after_state: null, req, status: "SUCCESS" });
    res.json({ ok: true, message: 'Deleted' });
  } catch(e) {
    await conn.rollback(); 
    res.status(500).json({ok:false});
  } finally { 
    conn.release(); 
  }
};

// Calculate contributions
const calculateContributions = async (req, res) => {
  try {
    const { employeeId, basicSalary } = req.body;
    if (!employeeId || !basicSalary) return res.status(400).json({ ok: false });

    const [[rec]] = await pool.query(`SELECT epf_contribution_rate, employer_epf_rate, etf_contribution_rate FROM employee_etf_epf WHERE employee_id = ?`, [employeeId]);
    if (!rec) return res.status(404).json({ ok: false, message: 'No record found' });

    const basic = Number(basicSalary);
    const employeeEpf = (basic * rec.epf_contribution_rate) / 100;
    const employerEpf = (basic * rec.employer_epf_rate) / 100;
    const employerEtf = (basic * rec.etf_contribution_rate) / 100;

    res.json({ ok: true, data: { basic_salary: basic, employee_epf_contribution: employeeEpf.toFixed(2), employer_epf_contribution: employerEpf.toFixed(2), employer_etf_contribution: employerEtf.toFixed(2), total_epf: (employeeEpf+employerEpf).toFixed(2) }});
  } catch (err) {
    res.status(500).json({ ok: false });
  }
};

// Helper function for calculations
const calculateContributionsHelper = (basic_salary, rates) => {
  const basic = Number(basic_salary || 0);

  // Use default rates if employee-specific rates are not set
  const epf_contribution_rate = Number(rates.epf_contribution_rate || 8); // Employee EPF: 8% default
  const employer_epf_rate = Number(rates.employer_epf_rate || 12); // Employer EPF: 12% default
  const etf_contribution_rate = Number(rates.etf_contribution_rate || 3); // Employer ETF: 3% default

  const employeeEpf = (basic * epf_contribution_rate) / 100;
  const employerEpf = (basic * employer_epf_rate) / 100;
  const employerEtf = (basic * etf_contribution_rate) / 100;

  return {
    employeeEpf,
    employerEpf,
    employerEtf,
    grossForEpf: basic, // Based on your current requirement: Basic Salary
  };
};

/**
 * Get processed payment summary by period
 */
const getPaymentSummary = async (req, res) => {
  try {
    const [rows] = await pool.query(`
      SELECT 
        period_year,
        period_month,
        COUNT(DISTINCT employee_id) AS employee_count,
        SUM(gross_salary) AS total_basic_salary,
        SUM(employee_epf_amount) AS total_employee_epf,
        SUM(epf_employer_share) AS total_employer_epf,
        SUM(employer_etf_amount) AS total_employer_etf,
        MAX(processed_at) AS last_processed_date
      FROM payroll_etf_epf_transactions
      GROUP BY period_year, period_month
      ORDER BY period_year DESC, period_month DESC
    `);
    
    res.json({ ok: true, data: rows });
  } catch (err) {
    console.error('getPaymentSummary error:', err);
    res.status(500).json({ ok: false, message: 'Failed to fetch payment summary' });
  }
};

/**
 * Get detailed payment history for a specific period
 */
const getPaymentHistory = async (req, res) => {
  try {
    const { year, month } = req.query;
    
    if (!year || !month) {
      return res.status(400).json({ ok: false, message: 'Year and month required' });
    }
    
    const [rows] = await pool.query(`
      SELECT 
        pt.id,
        pt.employee_id,
        e.full_name,
        e.employee_code,
        d.name AS department_name,
        ee.epf_number,
        ee.etf_number,
        pt.gross_salary AS basic_salary,
        pt.employee_epf_amount AS employee_epf_contribution,
        pt.epf_employer_share AS employer_epf_contribution,
        pt.employer_etf_amount AS employer_etf_contribution,
        pt.processed_at AS process_date,
        pt.processed_by
      FROM payroll_etf_epf_transactions pt
      JOIN employees e ON e.id = pt.employee_id
      LEFT JOIN departments d ON d.id = e.department_id
      LEFT JOIN employee_etf_epf ee ON ee.employee_id = e.id
      WHERE pt.period_year = ? AND pt.period_month = ?
      ORDER BY e.full_name
    `, [year, month]);
    
    // Calculate totals
    const totals = rows.reduce((acc, row) => ({
      totalEmployeeEpf: acc.totalEmployeeEpf + Number(row.employee_epf_contribution || 0),
      totalEmployerEpf: acc.totalEmployerEpf + Number(row.employer_epf_contribution || 0),
      totalEmployerEtf: acc.totalEmployerEtf + Number(row.employer_etf_contribution || 0)
    }), { totalEmployeeEpf: 0, totalEmployerEpf: 0, totalEmployerEtf: 0 });
    
    res.json({ 
      ok: true, 
      data: rows,
      totals 
    });
  } catch (err) {
    console.error('getPaymentHistory error:', err);
    res.status(500).json({ ok: false, message: 'Failed to fetch payment history' });
  }
};

// Process payment function
const processPayment = async (req, res) => {
  const connection = await pool.getConnection();
  try {
    const { year, month, employeeData } = req.body;

    if (!year || !month || !Array.isArray(employeeData) || employeeData.length === 0) {
      return res.status(400).json({ ok: false, message: 'Invalid payload: year, month, and employeeData array are required.' });
    }

    const periodYear = Number(year);
    const periodMonth = Number(month);
    
    await connection.beginTransaction();

    const insertPromises = employeeData.map(emp => {
      const { id, epf_number, etf_number, basic_salary, grossForEpf, employeeEpf, employerEpf, employerEtf } = emp;

      // Ensure required fields are present and valid
      if (!id || !basic_salary || !employeeEpf || !employerEpf || !employerEtf) {
        throw new Error(`Invalid data for employee ID ${id}`);
      }
      
      // Use REPLACE INTO or ON DUPLICATE KEY UPDATE to handle reprocessing
      const query = `
        INSERT INTO employee_etf_epf_payments 
        (employee_id, epf_number, etf_number, period_year, period_month, basic_salary, gross_for_epf, employee_epf_contribution, employer_epf_contribution, employer_etf_contribution)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE 
          epf_number=VALUES(epf_number), etf_number=VALUES(etf_number), basic_salary=VALUES(basic_salary), gross_for_epf=VALUES(gross_for_epf), 
          employee_epf_contribution=VALUES(employee_epf_contribution), employer_epf_contribution=VALUES(employer_epf_contribution), employer_etf_contribution=VALUES(employer_etf_contribution), 
          process_date=CURRENT_TIMESTAMP
      `;
      
      return connection.query(query, [
        id, epf_number, etf_number, periodYear, periodMonth, 
        basic_salary, grossForEpf, employeeEpf, employerEpf, employerEtf
      ]);
    });

    await Promise.all(insertPromises);
    await connection.commit();
    
    // Log audit event
    logAudit({
      level: 'info',
      user_id: req.user.id,
      action_type: 'PROCESS_ETF_EPF',
      target_table: 'employee_etf_epf_payments',
      target_id: null,
      before_state: { year, month },
      after_state: { count: employeeData.length },
      status: 'SUCCESS',
      req
    });

    res.status(200).json({ ok: true, message: `${employeeData.length} ETF/EPF payment records processed for ${periodYear}-${periodMonth}.` });

  } catch (err) {
    await connection.rollback();
    console.error('processPayment error:', err);
    logEvent({ 
      level: 'error', 
      event_type: "PROCESS_ETF_EPF_FAILED", 
      user_id: req.user?.id || null, 
      req, 
      extra: { error: err.message, payload: req.body }
    });
    res.status(500).json({ ok: false, message: 'Failed to process ETF/EPF payments.' });
  } finally {
    connection.release();
  }
};

// Test endpoint
const testProcessList = async (req, res) => {
  try {
    console.log('🧪 Test endpoint called');
    
    // Return test data
    const testData = [
      {
        employee_id: 1,
        full_name: 'John Doe',
        employee_code: 'EMP001',
        department_name: 'IT Department',
        basic_salary: 50000,
        epf_employee_amount: 4000,
        epf_employer_share: 6000,
        etf_employer_contribution: 1500,
        total_statutory: 11500
      },
      {
        employee_id: 2,
        full_name: 'Jane Smith',
        employee_code: 'EMP002',
        department_name: 'HR Department',
        basic_salary: 45000,
        epf_employee_amount: 3600,
        epf_employer_share: 5400,
        etf_employer_contribution: 1350,
        total_statutory: 10350
      }
    ];
    
    return res.json({ 
      ok: true, 
      data: testData,
      message: 'Test data loaded successfully'
    });
  } catch (err) {
    console.error('Test endpoint error:', err);
    return res.status(500).json({ 
      ok: false, 
      message: 'Test endpoint error' 
    });
  }
};

// Get employees without ETF/EPF
const getEmployeesWithoutEtfEpf = async (req, res) => {
  const [rows] = await pool.query("SELECT id, full_name FROM employees");
  res.json({ok:true, data:rows});
};

// Module exports - ALL functions must be listed here
module.exports = {
  getEtfEpfRecords,
  getEtfEpfById,
  createEtfEpfRecord,
  updateEtfEpfRecord,
  deleteEtfEpfRecord,
  getEmployeesWithoutEtfEpf,
  calculateContributions,
  getEmployeePaymentHistory,
  
  // Process functions
  getProcessList: getEtfEpfProcessList, 
  processPayment,
  getPaymentSummary,
  getPaymentHistory,
  
  // Test function
  testProcessList,
  
  // Aliases for compatibility
  getEtfEpfProcessList: getEtfEpfProcessList, 
  processEtfEpfPayments: processEtfEpfPayments
};