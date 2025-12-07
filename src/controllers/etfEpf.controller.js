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

    // 1. Fetch active employees and their ETF/EPF settings
    const [employeeRecords] = await pool.query(`
      SELECT 
        e.id AS employee_id,
        e.employee_code,
        e.full_name,
        e.joining_date,
        d.name AS department_name, /* Fetch department name for display */
        COALESCE(ee.epf_number, e.epf_no) AS epf_number, /* Use employee_etf_epf if available, fallback to employees.epf_no */
        ee.etf_number,
        COALESCE(ee.epf_contribution_rate, 8.00) AS epf_contribution_rate,
        COALESCE(ee.employer_epf_rate, 12.00) AS employer_epf_rate,
        COALESCE(ee.etf_contribution_rate, 3.00) AS etf_contribution_rate,
        tet.id AS transaction_id
      FROM employees e
      LEFT JOIN departments d ON d.id = e.department_id /* Join for department name */
      LEFT JOIN employee_etf_epf ee ON ee.employee_id = e.id
      LEFT JOIN payroll_etf_epf_transactions tet ON tet.employee_id = e.id AND tet.period_year = ? AND tet.period_month = ?
      WHERE e.status = 'Active' 
      ORDER BY e.full_name
    `, [numYear, numMonth]);

    const results = [];
    for (const record of employeeRecords) {
      // 2. Filter by eligibility (joined on or before the period month)
      if (record.joining_date) {
        const joinDate = new Date(record.joining_date);
        const joinY = joinDate.getFullYear();
        const joinM = joinDate.getMonth() + 1; // 1-based month

        // Skip employee if they joined after the selected period month/year
        if (numYear < joinY || (numYear === joinY && numMonth < joinM)) {
          continue; 
        }
      }

      // 3. Get Gross Salary Components for the period
      const grossComponents = await getGrossComponentsForPeriod(record.employee_id, numYear, numMonth);
      const grossForEpf = grossComponents.gross_salary_for_epf;
      
      // Calculate contributions based on the fetched gross salary
      const epfEmpRate = Number(record.epf_contribution_rate || 8);
      const epfCompRate = Number(record.employer_epf_rate || 12);
      const etfRate = Number(record.etf_contribution_rate || 3);
      
      const epfEmployee = (grossForEpf * epfEmpRate) / 100;
      const epfEmployer = (grossForEpf * epfCompRate) / 100;
      const etfAmount = (grossForEpf * etfRate) / 100;

      results.push({
        ...record,
        ...grossComponents, 
        gross_salary_for_epf: grossForEpf.toFixed(2),
        // Add calculated amounts for frontend display
        epf_employee_amount: epfEmployee.toFixed(2),
        epf_employer_share: epfEmployer.toFixed(2),
        etf_employer_contribution: etfAmount.toFixed(2),
        
        is_processed: record.transaction_id !== null,
      });
    }

    res.json({ ok: true, data: results });
  } catch (err) {
    console.error('getEtfEpfProcessList error:', err);
    logEvent({ level: 'error', event_type: "GET_ETF_EPF_PROCESS_LIST_FAILED", user_id: req.user?.id || null, severity: "ERROR", error_message: err.message, event_details: { query: req.query, error: err.message } });
    // IMPORTANT: Return a standard 500 error message. This will ensure the frontend shows the correct error message.
    res.status(500).json({ ok: false, message: `Failed to load ETF/EPF processing list. Internal Error: ${err.message}` });
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
    // Assumes payroll_cycles table exists. Returns empty array if query fails (table missing).
    const [rows] = await pool.query(`
      SELECT 
        id,
        period_year,
        period_month,
        generated_at as payment_date,
        total_deductions, 
        net_salary
      FROM payroll_cycles
      WHERE employee_id = ?
      ORDER BY period_year DESC, period_month DESC
    `, [employeeId]).catch(() => []); 

    res.json({ ok: true, data: rows });
  } catch (err) {
    console.error('getHistory error:', err);
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

// Delete
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
    await conn.rollback(); res.status(500).json({ok:false});
  } finally { conn.release(); }
};

// Calculate
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


const getEmployeesWithoutEtfEpf = async (req, res) => {
  const [rows] = await pool.query("SELECT id, full_name FROM employees");
  res.json({ok:true, data:rows});
};

module.exports = {
  getEtfEpfRecords,
  getEtfEpfById,
  createEtfEpfRecord,
  updateEtfEpfRecord,
  deleteEtfEpfRecord,
  getEmployeesWithoutEtfEpf,
  calculateContributions,
  getEmployeePaymentHistory,
  // === NEW EXPORTS FOR PROCESSING ===
  getEtfEpfProcessList, 
  processEtfEpfPayments,
};