// src/controllers/payroll.controller.js
const pool = require('../config/db');
const logAudit = require('../utils/audit');
const logEvent = require('../utils/event');
const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');

/**
 * Helper to calculate gross salary components
 */
const calculateGrossSalary = async (employee_id, year, month) => {
  try {
    console.log(`calculateGrossSalary for employee ${employee_id}, ${year}-${month}`);
    
    // Basic salary
    const [[salary]] = await pool.query(
      'SELECT basic_salary FROM salaries WHERE employee_id = ? ORDER BY id DESC LIMIT 1',
      [employee_id]
    );
    console.log('Salary query result:', salary);
    const basicSalary = Number(salary?.basic_salary || 0);
    console.log('Basic salary:', basicSalary);

    // Period for allowance filtering
    const periodEnd = new Date(year, month, 0).toISOString().slice(0, 10);
    const periodStart = `${year}-${String(month).padStart(2, '0')}-01`;
    console.log('Period:', periodStart, 'to', periodEnd);

    // Allowances
    const [allowances] = await pool.query(
      `SELECT COALESCE(SUM(amount), 0) as total FROM allowances 
       WHERE employee_id = ? AND status = 'Active'
       AND (effective_from IS NULL OR effective_from <= ?)
       AND (effective_to IS NULL OR effective_to >= ?)`,
      [employee_id, periodEnd, periodStart]
    );
    console.log('Allowances result:', allowances);

    // Overtime
    const [overtime] = await pool.query(
      `SELECT COALESCE(SUM(ot_hours * ot_rate), 0) as total FROM overtime_adjustments
       WHERE employee_id = ? AND MONTH(created_at) = ? AND YEAR(created_at) = ?`,
      [employee_id, month, year]
    );
    console.log('Overtime result:', overtime);

    // Bonuses
    const [bonuses] = await pool.query(
      `SELECT COALESCE(SUM(amount), 0) as total FROM bonuses
       WHERE employee_id = ? AND MONTH(effective_date) = ? AND YEAR(effective_date) = ?`,
      [employee_id, month, year]
    );
    console.log('Bonuses result:', bonuses);

    const allowancesTotal = Number(allowances[0]?.total || 0);
    const overtimeTotal = Number(overtime[0]?.total || 0);
    const bonusTotal = Number(bonuses[0]?.total || 0);

    console.log('Totals:', {
      basic: basicSalary,
      allowances: allowancesTotal,
      overtime: overtimeTotal,
      bonus: bonusTotal,
      gross: basicSalary + allowancesTotal + overtimeTotal + bonusTotal
    });

    return {
      basic_salary: basicSalary,
      allowances: allowancesTotal,
      overtime: overtimeTotal,
      bonuses: bonusTotal,
      gross_salary: basicSalary + allowancesTotal + overtimeTotal + bonusTotal
    };
  } catch (err) {
    console.error('calculateGrossSalary error:', err);
    throw err;
  }
};

/**
 * Get payroll data for payslip (simplified version)
 */
exports.getEmployeePayrollData = async (req, res) => {
  try {
    const { employee_id, month, year } = req.query;
    
    console.log('=== getEmployeePayrollData START ===');
    console.log('Params:', { employee_id, month, year });
    
    if (!employee_id || !month || !year) {
      console.log('Missing parameters');
      return res.status(400).json({ 
        ok: false, 
        message: 'employee_id, month, and year are required' 
      });
    }

    // Get employee info
    console.log('Fetching employee info...');
    const [[employee]] = await pool.query(
      `SELECT 
        e.id, 
        e.employee_code, 
        e.full_name, 
        e.email, 
        e.designation,
        d.name as department
       FROM employees e
       LEFT JOIN departments d ON d.id = e.department_id
       WHERE e.id = ?`,
      [employee_id]
    );
    
    console.log('Employee found:', employee);

    if (!employee) {
      console.log('Employee not found');
      return res.status(404).json({ ok: false, message: 'Employee not found' });
    }

    // Calculate earnings
    console.log('Calculating earnings...');
    const earnings = await calculateGrossSalary(employee_id, year, month);
    console.log('Earnings:', earnings);
    
    // Calculate deductions
    console.log('Calculating deductions...');
    const deductions = await calculateDeductions(employee_id, year, month);
    console.log('Deductions:', deductions);
    
    // Get ETF/EPF rates
    console.log('Fetching ETF/EPF rates...');
    const [[etfEpf]] = await pool.query(
      `SELECT 
        COALESCE(epf_contribution_rate, 8.00) as employee_epf_rate,
        COALESCE(employer_epf_rate, 12.00) as employer_epf_rate,
        COALESCE(etf_contribution_rate, 3.00) as employer_etf_rate
       FROM employee_etf_epf WHERE employee_id = ?`,
      [employee_id]
    );
    console.log('ETF/EPF rates:', etfEpf);

    const basicSalary = earnings.basic_salary;
    const employerEpf = (basicSalary * Number(etfEpf?.employer_epf_rate || 12.00)) / 100;
    const employerEtf = (basicSalary * Number(etfEpf?.employer_etf_rate || 3.00)) / 100;
    
    // Prepare earnings breakdown
    const earningsBreakdown = [
      { description: 'Basic Salary', amount: earnings.basic_salary },
      ...(earnings.allowances > 0 ? [{ description: 'Allowances', amount: earnings.allowances }] : []),
      ...(earnings.overtime > 0 ? [{ description: 'Overtime', amount: earnings.overtime }] : []),
      ...(earnings.bonuses > 0 ? [{ description: 'Bonuses', amount: earnings.bonuses }] : [])
    ];

    // Prepare deductions breakdown
    const deductionsBreakdown = [
      ...deductions.regular_deductions.map(d => ({
        description: d.name,
        amount: d.calculated_amount
      })),
      ...(deductions.epf_deduction > 0 ? [{ 
        description: 'Employee EPF Contribution', 
        amount: deductions.epf_deduction 
      }] : []),
      ...(deductions.unpaid_leave_deduction > 0 ? [{ 
        description: 'Unpaid Leave Deduction', 
        amount: deductions.unpaid_leave_deduction 
      }] : [])
    ];

    const grossSalary = earnings.gross_salary;
    const totalDeductions = deductions.total_deductions;
    const netSalary = grossSalary - totalDeductions;

    const responseData = {
      ok: true,
      data: {
        employee: {
          ...employee,
          basic_salary: basicSalary
        },
        period: {
          year: parseInt(year),
          month: parseInt(month),
          month_name: new Date(year, month - 1).toLocaleString('default', { month: 'long' })
        },
        earnings: {
          breakdown: earningsBreakdown,
          total: grossSalary
        },
        deductions: {
          breakdown: deductionsBreakdown,
          total: totalDeductions
        },
        employer_contributions: {
          epf: employerEpf,
          etf: employerEtf
        },
        summary: {
          gross_salary: grossSalary,
          total_deductions: totalDeductions,
          net_salary: netSalary
        }
      }
    };

    console.log('=== Response Data ===');
    console.log(JSON.stringify(responseData, null, 2));
    console.log('=== getEmployeePayrollData END ===');
    
    res.json(responseData);

  } catch (err) {
    console.error('getEmployeePayrollData error:', err);
    console.error('Error stack:', err.stack);
    res.status(500).json({ 
      ok: false, 
      message: 'Failed to fetch payroll data',
      error: err.message 
    });
  }
};

/**
 * Helper to calculate deductions
 */
const calculateDeductions = async (employee_id, year, month) => {
  try {
    // Get basic salary for percentage-based deductions
    const [[salary]] = await pool.query(
      'SELECT basic_salary FROM salaries WHERE employee_id = ? ORDER BY id DESC LIMIT 1',
      [employee_id]
    );
    const basicSalary = Number(salary?.basic_salary || 0);

    // Regular deductions (excluding EPF)
    const [regularDeductions] = await pool.query(
      `SELECT 
        name,
        basis,
        COALESCE(amount, 0) as fixed_amount,
        COALESCE(percent, 0) as percent,
        CASE 
          WHEN basis = 'Percent' THEN (COALESCE(percent, 0) / 100) * ?
          ELSE COALESCE(amount, 0)
        END as calculated_amount
       FROM deductions
       WHERE employee_id = ? AND status = 'Active'
       AND MONTH(effective_date) = ? AND YEAR(effective_date) = ?
       AND name NOT LIKE '%EPF%'`,
      [basicSalary, employee_id, month, year]
    );

    // Unpaid leave deductions (from unpaid_leaves table)
    const [unpaidLeaves] = await pool.query(
      `SELECT COALESCE(SUM(deduction_amount), 0) as total FROM unpaid_leaves
       WHERE employee_id = ? AND status = 'Processed'
       AND MONTH(updated_at) = ? AND YEAR(updated_at) = ?`,
      [employee_id, month, year]
    );

    // EPF deduction (employee contribution)
    const [[epfConfig]] = await pool.query(
      'SELECT COALESCE(epf_contribution_rate, 8.00) as rate FROM employee_etf_epf WHERE employee_id = ?',
      [employee_id]
    );
    const epfRate = Number(epfConfig?.rate || 8.00);
    const epfDeduction = (basicSalary * epfRate) / 100;

    // Sum all deductions
    let totalRegular = 0;
    regularDeductions.forEach(d => {
      totalRegular += Number(d.calculated_amount || 0);
    });

    const unpaidLeaveTotal = Number(unpaidLeaves[0]?.total || 0);
    const totalDeductions = totalRegular + unpaidLeaveTotal + epfDeduction;

    return {
      regular_deductions: regularDeductions,
      regular_total: totalRegular,
      unpaid_leave_deduction: unpaidLeaveTotal,
      epf_deduction: epfDeduction,
      total_deductions: totalDeductions
    };
  } catch (err) {
    console.error('calculateDeductions error:', err);
    throw err;
  }
};

/**
 * Get employee payroll details for payslip
 */
exports.getEmployeePayrollDetails = async (req, res) => {
  try {
    const { employee_id, month, year } = req.query;

    if (!employee_id || !month || !year) {
      return res.status(400).json({ 
        ok: false, 
        message: 'employee_id, month, and year are required' 
      });
    }

    // Get employee info
    const [[employee]] = await pool.query(
      `SELECT 
        e.id, 
        e.employee_code, 
        e.full_name, 
        e.email, 
        e.nic,
        d.name as department,
        e.designation
       FROM employees e
       LEFT JOIN departments d ON d.id = e.department_id
       WHERE e.id = ?`,
      [employee_id]
    );

    if (!employee) {
      return res.status(404).json({ ok: false, message: 'Employee not found' });
    }

    // Calculate earnings
    const earnings = await calculateGrossSalary(employee_id, year, month);

    // Calculate deductions
    const deductions = await calculateDeductions(employee_id, year, month);

    // Get ETF/EPF employer contributions
    const [[etfEpf]] = await pool.query(
      `SELECT 
        COALESCE(epf_contribution_rate, 8.00) as employee_epf_rate,
        COALESCE(employer_epf_rate, 12.00) as employer_epf_rate,
        COALESCE(etf_contribution_rate, 3.00) as employer_etf_rate
       FROM employee_etf_epf WHERE employee_id = ?`,
      [employee_id]
    );

    const basicSalary = earnings.basic_salary;
    const employerEpf = (basicSalary * Number(etfEpf?.employer_epf_rate || 12.00)) / 100;
    const employerEtf = (basicSalary * Number(etfEpf?.employer_etf_rate || 3.00)) / 100;

    // Prepare earnings breakdown
    const earningsBreakdown = [
      { description: 'Basic Salary', amount: earnings.basic_salary, type: 'salary' },
      { description: 'Allowances', amount: earnings.allowances, type: 'allowance' },
      { description: 'Overtime', amount: earnings.overtime, type: 'overtime' },
      { description: 'Bonuses', amount: earnings.bonuses, type: 'bonus' }
    ].filter(item => item.amount > 0);

    // Prepare deductions breakdown
    const deductionsBreakdown = [
      ...deductions.regular_deductions.map(d => ({
        description: d.name,
        amount: d.calculated_amount,
        type: 'regular'
      })),
      { description: 'EPF Contribution', amount: deductions.epf_deduction, type: 'epf' },
      { description: 'Unpaid Leave', amount: deductions.unpaid_leave_deduction, type: 'unpaid_leave' }
    ].filter(item => item.amount > 0);

    const netPay = earnings.gross_salary - deductions.total_deductions;

    res.json({
      ok: true,
      data: {
        employee,
        period: {
          year,
          month,
          month_name: new Date(year, month - 1).toLocaleString('default', { month: 'long' })
        },
        earnings: {
          breakdown: earningsBreakdown,
          total: earnings.gross_salary
        },
        deductions: {
          breakdown: deductionsBreakdown,
          total: deductions.total_deductions
        },
        employer_contributions: {
          epf: employerEpf,
          etf: employerEtf
        },
        net_pay: netPay,
        summary: {
          gross_salary: earnings.gross_salary,
          total_deductions: deductions.total_deductions,
          net_salary: netPay
        }
      }
    });

  } catch (err) {
    console.error('getEmployeePayrollDetails error:', err);
    logEvent({
      level: 'error',
      event_type: 'GET_PAYROLL_DETAILS_ERROR',
      user_id: req.user?.id,
      severity: 'ERROR',
      error_message: err.message,
      event_details: req.query
    });
    res.status(500).json({ ok: false, message: 'Failed to fetch payroll details' });
  }
};

/**
 * Generate PDF payslip
 */
exports.generatePaySlipPDF = async (req, res) => {
  try {
    const { employee_id, month, year } = req.query;

    if (!employee_id || !month || !year) {
      return res.status(400).json({ 
        ok: false, 
        message: 'employee_id, month, and year are required' 
      });
    }

    // Get payroll details
    const payrollData = await exports.getEmployeePayrollDetails(
      { query: { employee_id, month, year }, user: req.user },
      { json: (data) => data }
    );

    if (!payrollData.ok) {
      return res.status(404).json(payrollData);
    }

    const data = payrollData.data;
    const doc = new PDFDocument({ margin: 50 });

    // Set response headers
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="payslip_${data.employee.employee_code}_${year}_${month}.pdf"`
    );

    doc.pipe(res);

    // Header
    doc.fontSize(20).text('ABC Corporation', { align: 'center' });
    doc.fontSize(12).text('123 Business Ave, Galle road, Colombo 03', { align: 'center' });
    doc.moveDown();
    doc.fontSize(16).text('PAY SLIP', { align: 'center', underline: true });
    doc.moveDown();

    // Company and Pay Period
    doc.fontSize(10);
    doc.text(`Pay Period: ${data.period.month_name} ${data.period.year}`);
    doc.text(`Generated: ${new Date().toLocaleDateString()}`);
    doc.moveDown();

    // Employee Details
    doc.fontSize(12).text('Employee Details', { underline: true });
    doc.fontSize(10);
    doc.text(`Employee ID: ${data.employee.employee_code}`);
    doc.text(`Name: ${data.employee.full_name}`);
    doc.text(`Department: ${data.employee.department}`);
    doc.text(`Designation: ${data.employee.designation}`);
    doc.moveDown();

    // Earnings Table
    doc.fontSize(12).text('Earnings', { underline: true });
    let earningsY = doc.y;
    
    doc.fontSize(10);
    data.earnings.breakdown.forEach((item, index) => {
      doc.text(item.description, 50, earningsY + (index * 20));
      doc.text(`Rs ${item.amount.toFixed(2)}`, 400, earningsY + (index * 20), { align: 'right' });
    });
    
    // Earnings Total
    const earningsEndY = earningsY + (data.earnings.breakdown.length * 20);
    doc.fontSize(11).font('Helvetica-Bold');
    doc.text('Total Earnings', 50, earningsEndY + 10);
    doc.text(`Rs ${data.earnings.total.toFixed(2)}`, 400, earningsEndY + 10, { align: 'right' });
    
    doc.moveDown(2);

    // Deductions Table
    const deductionsY = doc.y;
    doc.fontSize(12).text('Deductions', { underline: true });
    doc.fontSize(10).font('Helvetica');
    
    data.deductions.breakdown.forEach((item, index) => {
      doc.text(item.description, 50, deductionsY + 20 + (index * 20));
      doc.text(`Rs ${item.amount.toFixed(2)}`, 400, deductionsY + 20 + (index * 20), { align: 'right' });
    });
    
    // Deductions Total
    const deductionsEndY = deductionsY + 20 + (data.deductions.breakdown.length * 20);
    doc.fontSize(11).font('Helvetica-Bold');
    doc.text('Total Deductions', 50, deductionsEndY + 10);
    doc.text(`Rs ${data.deductions.total.toFixed(2)}`, 400, deductionsEndY + 10, { align: 'right' });
    
    doc.moveDown(2);

    // Net Pay
    doc.fontSize(14).font('Helvetica-Bold');
    doc.text('NET PAY:', 50, doc.y);
    doc.text(`Rs ${data.net_pay.toFixed(2)}`, 400, doc.y, { align: 'right', color: 'green' });
    
    doc.moveDown();

    // Employer Contributions
    doc.fontSize(11).text('Employer Contributions:', { underline: true });
    doc.fontSize(10);
    doc.text(`EPF (Employer): Rs ${data.employer_contributions.epf.toFixed(2)}`);
    doc.text(`ETF: Rs ${data.employer_contributions.etf.toFixed(2)}`);

    // Footer
    doc.moveDown(3);
    doc.fontSize(9).text('This is a computer generated payslip. No signature required.', { align: 'center' });

    doc.end();

    // Log audit
    logAudit({
      user_id: req.user?.id,
      action_type: 'GENERATE_PAYSLIP_PDF',
      target_table: 'payroll',
      target_id: employee_id,
      before_state: null,
      after_state: { employee_id, month, year },
      req,
      status: 'SUCCESS'
    });

  } catch (err) {
    console.error('generatePaySlipPDF error:', err);
    logEvent({
      level: 'error',
      event_type: 'GENERATE_PAYSLIP_PDF_ERROR',
      user_id: req.user?.id,
      severity: 'ERROR',
      error_message: err.message
    });
    res.status(500).json({ ok: false, message: 'Failed to generate PDF' });
  }
};

/**
 * Process salary transfer (mark as paid)
 */
exports.processSalaryTransfer = async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const { employee_ids, month, year, payment_date } = req.body;
    const user_id = req.user.id;

    if (!employee_ids || !Array.isArray(employee_ids) || employee_ids.length === 0) {
      return res.status(400).json({ ok: false, message: 'employee_ids array required' });
    }
    if (!month || !year) {
      return res.status(400).json({ ok: false, message: 'month and year required' });
    }

    await conn.beginTransaction();
    const processed = [];

    for (const employee_id of employee_ids) {
      // Check if already processed
      const [[existing]] = await conn.query(
        'SELECT id FROM payroll_transfers WHERE employee_id = ? AND period_year = ? AND period_month = ?',
        [employee_id, year, month]
      );

      if (existing) {
        console.log(`Salary already transferred for employee ${employee_id} for ${year}-${month}`);
        continue;
      }

      // Calculate payroll details
      const payrollData = await exports.getEmployeePayrollDetails(
        { query: { employee_id, month, year }, user: req.user },
        { json: (data) => data }
      );

      if (!payrollData.ok) {
        logEvent({
          level: 'warn',
          event_type: 'SALARY_TRANSFER_SKIP',
          user_id,
          severity: 'WARNING',
          event_details: { employee_id, month, year, reason: 'Payroll data not found' }
        });
        continue;
      }

      const data = payrollData.data;

      // Insert transfer record
      const [transferResult] = await conn.query(
        `INSERT INTO payroll_transfers 
          (employee_id, period_year, period_month, gross_salary, total_deductions, net_salary, 
           payment_date, processed_by, status)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Completed')`,
        [
          employee_id,
          year,
          month,
          data.summary.gross_salary,
          data.summary.total_deductions,
          data.summary.net_salary,
          payment_date || new Date().toISOString().slice(0, 10),
          user_id
        ]
      );

      processed.push({
        employee_id,
        transfer_id: transferResult.insertId,
        net_salary: data.summary.net_salary
      });
    }

    await conn.commit();

    logAudit({
      user_id,
      action_type: 'PROCESS_SALARY_TRANSFER',
      target_table: 'payroll_transfers',
      target_id: processed.map(p => p.transfer_id).join(','),
      before_state: null,
      after_state: { month, year, processed_count: processed.length },
      req,
      status: 'SUCCESS'
    });

    res.json({
      ok: true,
      message: `Salary transfer processed for ${processed.length} employee(s)`,
      processed,
      processed_count: processed.length
    });

  } catch (err) {
    await conn.rollback();
    console.error('processSalaryTransfer error:', err);
    logEvent({
      level: 'error',
      event_type: 'PROCESS_SALARY_TRANSFER_ERROR',
      user_id: req.user?.id,
      severity: 'ERROR',
      error_message: err.message
    });
    res.status(500).json({ ok: false, message: 'Failed to process salary transfer' });
  } finally {
    conn.release();
  }
};

/**
 * Get payroll summary for a department
 */
exports.getDepartmentPayrollSummary = async (req, res) => {
  try {
    const { department_id, month, year } = req.query;

    if (!month || !year) {
      return res.status(400).json({ ok: false, message: 'month and year required' });
    }

    let whereClause = 'e.status = "Active"';
    const params = [month, year];

    if (department_id && department_id !== 'all') {
      whereClause += ' AND e.department_id = ?';
      params.unshift(department_id);
    }

    const [employees] = await pool.query(
      `SELECT 
        e.id as employee_id,
        e.employee_code,
        e.full_name,
        d.name as department_name,
        e.designation
       FROM employees e
       LEFT JOIN departments d ON d.id = e.department_id
       WHERE ${whereClause}
       ORDER BY e.full_name`,
      department_id && department_id !== 'all' ? [department_id] : []
    );

    const summary = [];
    let departmentTotalGross = 0;
    let departmentTotalDeductions = 0;
    let departmentTotalNet = 0;

    for (const emp of employees) {
      const payrollData = await exports.getEmployeePayrollDetails(
        { query: { employee_id: emp.employee_id, month, year }, user: req.user },
        { json: (data) => data }
      );

      if (payrollData.ok) {
        const data = payrollData.data;
        summary.push({
          employee_id: emp.employee_id,
          employee_code: emp.employee_code,
          full_name: emp.full_name,
          department: emp.department_name,
          designation: emp.designation,
          gross_salary: data.summary.gross_salary,
          total_deductions: data.summary.total_deductions,
          net_salary: data.summary.net_salary
        });

        departmentTotalGross += data.summary.gross_salary;
        departmentTotalDeductions += data.summary.total_deductions;
        departmentTotalNet += data.summary.net_salary;
      }
    }

    res.json({
      ok: true,
      data: {
        summary,
        totals: {
          total_employees: summary.length,
          total_gross_salary: departmentTotalGross,
          total_deductions: departmentTotalDeductions,
          total_net_salary: departmentTotalNet
        }
      }
    });

  } catch (err) {
    console.error('getDepartmentPayrollSummary error:', err);
    logEvent({
      level: 'error',
      event_type: 'GET_DEPARTMENT_PAYROLL_SUMMARY_ERROR',
      user_id: req.user?.id,
      severity: 'ERROR',
      error_message: err.message
    });
    res.status(500).json({ ok: false, message: 'Failed to get department payroll summary' });
  }
};

/**
 * Get all payroll transfers for a period
 */
exports.getPayrollTransfers = async (req, res) => {
  try {
    const { month, year, status } = req.query;

    let where = '1=1';
    const params = [];

    if (month && year) {
      where += ' AND pt.period_month = ? AND pt.period_year = ?';
      params.push(Number(month), Number(year));
    }

    if (status) {
      where += ' AND pt.status = ?';
      params.push(status);
    }

    const [transfers] = await pool.query(
      `SELECT 
        pt.*,
        e.employee_code,
        e.full_name,
        d.name as department_name
       FROM payroll_transfers pt
       JOIN employees e ON e.id = pt.employee_id
       LEFT JOIN departments d ON d.id = e.department_id
       WHERE ${where}
       ORDER BY pt.payment_date DESC, pt.id DESC`,
      params
    );

    res.json({ ok: true, data: transfers });
  } catch (err) {
    console.error('getPayrollTransfers error:', err);
    res.status(500).json({ ok: false, message: 'Failed to fetch payroll transfers' });
  }
};

// Add this function to payroll.controller.js

/**
 * Get available months with payroll data
 */
exports.getAvailableMonths = async (req, res) => {
  try {
    const { employee_id, department_id } = req.query;
    
    let query = `
      SELECT DISTINCT 
        YEAR(created_at) as year, 
        MONTH(created_at) as month
      FROM (
        -- From allowances
        SELECT created_at FROM allowances WHERE status = 'Active'
        UNION ALL
        -- From overtime adjustments
        SELECT created_at FROM overtime_adjustments
        UNION ALL
        -- From bonuses
        SELECT effective_date as created_at FROM bonuses
        UNION ALL
        -- From deductions
        SELECT effective_date as created_at FROM deductions WHERE status = 'Active'
        UNION ALL
        -- From unpaid leaves
        SELECT updated_at as created_at FROM unpaid_leaves WHERE status = 'Processed'
      ) as combined_data
      WHERE created_at IS NOT NULL
    `;
    
    const params = [];
    
    if (employee_id) {
      query = `
        SELECT DISTINCT 
          YEAR(created_at) as year, 
          MONTH(created_at) as month
        FROM (
          -- From allowances for specific employee
          SELECT created_at FROM allowances WHERE employee_id = ? AND status = 'Active'
          UNION ALL
          -- From overtime adjustments for specific employee
          SELECT created_at FROM overtime_adjustments WHERE employee_id = ?
          UNION ALL
          -- From bonuses for specific employee
          SELECT effective_date as created_at FROM bonuses WHERE employee_id = ?
          UNION ALL
          -- From deductions for specific employee
          SELECT effective_date as created_at FROM deductions WHERE employee_id = ? AND status = 'Active'
          UNION ALL
          -- From unpaid leaves for specific employee
          SELECT updated_at as created_at FROM unpaid_leaves WHERE employee_id = ? AND status = 'Processed'
        ) as combined_data
        WHERE created_at IS NOT NULL
      `;
      params.push(employee_id, employee_id, employee_id, employee_id, employee_id);
    }
    
    query += ' ORDER BY year DESC, month DESC';
    
    const [months] = await pool.query(query, params);
    
    // Format months for frontend
    const formattedMonths = months.map(row => {
      const date = new Date(row.year, row.month - 1, 1);
      return {
        value: `${row.year}-${String(row.month).padStart(2, '0')}`,
        label: date.toLocaleString('default', { month: 'long' }) + ' ' + row.year,
        year: row.year,
        month: row.month
      };
    });
    
    // Also add current and next month if not already present
    const currentDate = new Date();
    const currentMonth = {
      value: `${currentDate.getFullYear()}-${String(currentDate.getMonth() + 1).padStart(2, '0')}`,
      label: currentDate.toLocaleString('default', { month: 'long' }) + ' ' + currentDate.getFullYear()
    };
    
    const nextDate = new Date(currentDate.getFullYear(), currentDate.getMonth() + 1, 1);
    const nextMonth = {
      value: `${nextDate.getFullYear()}-${String(nextDate.getMonth() + 1).padStart(2, '0')}`,
      label: nextDate.toLocaleString('default', { month: 'long' }) + ' ' + nextDate.getFullYear()
    };
    
    // Add current and next month if not already in list
    const allMonths = [...formattedMonths];
    if (!allMonths.some(m => m.value === currentMonth.value)) {
      allMonths.unshift(currentMonth);
    }
    if (!allMonths.some(m => m.value === nextMonth.value)) {
      allMonths.unshift(nextMonth);
    }
    
    res.json({ ok: true, data: allMonths });
    
  } catch (err) {
    console.error('getAvailableMonths error:', err);
    res.status(500).json({ ok: false, message: 'Failed to fetch available months' });
  }
};

/**
 * Export payroll data to CSV
 */
exports.exportPayrollCSV = async (req, res) => {
  try {
    const { month, year, department_id } = req.query;

    const summary = await exports.getDepartmentPayrollSummary(
      { query: { month, year, department_id }, user: req.user },
      { json: (data) => data }
    );

    if (!summary.ok) {
      return res.status(400).json(summary);
    }

    const data = summary.data.summary;
    const totals = summary.data.totals;

    // Create CSV
    const headers = ['Employee ID', 'Employee Code', 'Name', 'Department', 'Designation', 'Gross Salary', 'Total Deductions', 'Net Salary'];
    const csvRows = [headers.join(',')];

    data.forEach(row => {
      csvRows.push([
        row.employee_id,
        row.employee_code,
        `"${row.full_name}"`,
        `"${row.department}"`,
        `"${row.designation}"`,
        row.gross_salary.toFixed(2),
        row.total_deductions.toFixed(2),
        row.net_salary.toFixed(2)
      ].join(','));
    });

    // Add totals row
    csvRows.push(['', '', '', '', 'TOTAL:', totals.total_gross_salary.toFixed(2), totals.total_deductions.toFixed(2), totals.total_net_salary.toFixed(2)].join(','));

    const csvContent = csvRows.join('\n');

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="payroll_${year}_${month}.csv"`);
    res.send(csvContent);

  } catch (err) {
    console.error('exportPayrollCSV error:', err);
    res.status(500).json({ ok: false, message: 'Failed to export payroll data' });
  }
};


/**
 * Get payroll summary for dashboard
 */
exports.getPayrollSummary = async (req, res) => {
  try {
    const { month, year } = req.query;
    
    const currentDate = new Date();
    const currentMonth = month || currentDate.getMonth() + 1;
    const currentYear = year || currentDate.getFullYear();
    
    // Get total employees count
    const [[employeeCount]] = await pool.query(
      'SELECT COUNT(*) as count FROM employees WHERE status = "Active"'
    );
    
    // Calculate total gross, net, and deductions for the month
    let grossTotal = 0;
    let deductionsTotal = 0;
    let netTotal = 0;
    
    // Get all active employees
    const [employees] = await pool.query(
      'SELECT id FROM employees WHERE status = "Active"'
    );
    
    for (const emp of employees) {
      const earnings = await calculateGrossSalary(emp.id, currentYear, currentMonth);
      const deductions = await calculateDeductions(emp.id, currentYear, currentMonth);
      
      grossTotal += earnings.gross_salary;
      deductionsTotal += deductions.total_deductions;
      netTotal += (earnings.gross_salary - deductions.total_deductions);
    }
    
    // Get previous month for comparison
    const prevMonth = currentMonth === 1 ? 12 : currentMonth - 1;
    const prevYear = currentMonth === 1 ? currentYear - 1 : currentYear;
    let prevGrossTotal = 0;
    
    for (const emp of employees) {
      const earnings = await calculateGrossSalary(emp.id, prevYear, prevMonth);
      prevGrossTotal += earnings.gross_salary;
    }
    
    // Calculate percentage change
    const grossChange = prevGrossTotal > 0 ? 
      ((grossTotal - prevGrossTotal) / prevGrossTotal * 100).toFixed(1) : 0;
    
    res.json({
      ok: true,
      data: {
        totalEmployees: employeeCount.count,
        grossSalary: grossTotal.toFixed(2),
        netSalary: netTotal.toFixed(2),
        totalDeductions: deductionsTotal.toFixed(2),
        grossChange: grossChange > 0 ? `↑ ${grossChange}%` : `↓ ${Math.abs(grossChange)}%`,
        grossTrend: grossChange > 0 ? 'up' : 'down',
        pendingCases: 0, // You can calculate this based on your business logic
      }
    });
    
  } catch (err) {
    console.error('getPayrollSummary error:', err);
    res.status(500).json({ ok: false, message: 'Failed to get payroll summary' });
  }
};

/**
 * Get payroll processing status
 */
exports.getPayrollStatus = async (req, res) => {
  try {
    const { month, year } = req.query;
    
    const currentDate = new Date();
    const currentMonth = month || currentDate.getMonth() + 1;
    const currentYear = year || currentDate.getFullYear();
    
    // Check if payroll has been run for this month
    const [payrollRun] = await pool.query(
      'SELECT COUNT(*) as count FROM payroll_cycles WHERE period_year = ? AND period_month = ?',
      [currentYear, currentMonth]
    );
    
    const hasPayrollRun = payrollRun[0].count > 0;
    
    // Check if salary transfers have been processed
    const [transfers] = await pool.query(
      'SELECT COUNT(*) as count FROM payroll_transfers WHERE period_year = ? AND period_month = ?',
      [currentYear, currentMonth]
    );
    
    const hasTransfers = transfers[0].count > 0;
    
    // Get number of pending bank transfers
    const [pending] = await pool.query(
      'SELECT COUNT(*) as count FROM payroll_transfers WHERE status = "Pending" AND period_year = ? AND period_month = ?',
      [currentYear, currentMonth]
    );
    
    res.json({
      ok: true,
      data: {
        calculation: hasPayrollRun ? 'Completed' : 'Not Started',
        approval: hasPayrollRun ? 'Completed' : 'Not Started',
        bankTransfer: hasTransfers ? (pending[0].count > 0 ? 'In Progress' : 'Completed') : 'Not Started',
        completion: hasTransfers && pending[0].count === 0 ? 'Completed' : 'Not Started',
        step: hasPayrollRun ? (hasTransfers ? (pending[0].count > 0 ? 3 : 4) : 2) : 1,
        totalSteps: 4
      }
    });
    
  } catch (err) {
    console.error('getPayrollStatus error:', err);
    res.status(500).json({ ok: false, message: 'Failed to get payroll status' });
  }
};

/**
 * Get employees with payroll details for transfer overview
 */
exports.getPayrollTransferOverview = async (req, res) => {
  try {
    const { month, year, page = 1, limit = 10 } = req.query;
    
    if (!month || !year) {
      return res.status(400).json({ ok: false, message: 'Month and year required' });
    }
    
    const offset = (page - 1) * limit;
    
    // Get total count
    const [[totalCount]] = await pool.query(
      'SELECT COUNT(*) as count FROM employees WHERE status = "Active"'
    );
    
    // Get paginated employees
    const [employees] = await pool.query(
      `SELECT 
        e.id, 
        e.employee_code, 
        e.full_name, 
        e.phone, 
        d.name as department
       FROM employees e
       LEFT JOIN departments d ON d.id = e.department_id
       WHERE e.status = 'Active'
       ORDER BY e.full_name
       LIMIT ? OFFSET ?`,
      [parseInt(limit), parseInt(offset)]
    );
    
    const result = [];
    
    for (const emp of employees) {
      // Calculate payroll details
      const earnings = await calculateGrossSalary(emp.id, year, month);
      const deductions = await calculateDeductions(emp.id, year, month);
      
      // Check if salary has been transferred
      const [[transfer]] = await pool.query(
        'SELECT status FROM payroll_transfers WHERE employee_id = ? AND period_year = ? AND period_month = ?',
        [emp.id, year, month]
      );
      
      const netSalary = earnings.gross_salary - deductions.total_deductions;
      
      result.push({
        id: emp.id,
        name: emp.full_name,
        employee_code: emp.employee_code,
        phone: emp.phone || 'N/A',
        department: emp.department || 'N/A',
        gross_salary: earnings.gross_salary.toFixed(2),
        net_salary: netSalary.toFixed(2),
        deductions: deductions.total_deductions.toFixed(2),
        bank_status: transfer?.status || 'Pending'
      });
    }
    
    res.json({
      ok: true,
      data: result,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: totalCount.count,
        totalPages: Math.ceil(totalCount.count / limit)
      }
    });
    
  } catch (err) {
    console.error('getPayrollTransferOverview error:', err);
    res.status(500).json({ ok: false, message: 'Failed to get transfer overview' });
  }
};

/**
 * Initiate salary bank transfer
 */
exports.initiateBankTransfer = async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const { employee_ids, month, year } = req.body;
    const user_id = req.user.id;
    
    if (!employee_ids || !Array.isArray(employee_ids) || employee_ids.length === 0) {
      return res.status(400).json({ ok: false, message: 'employee_ids array required' });
    }
    if (!month || !year) {
      return res.status(400).json({ ok: false, message: 'month and year required' });
    }
    
    await conn.beginTransaction();
    const processed = [];
    
    for (const employee_id of employee_ids) {
      // Check if already transferred
      const [[existing]] = await conn.query(
        'SELECT id, status FROM payroll_transfers WHERE employee_id = ? AND period_year = ? AND period_month = ?',
        [employee_id, year, month]
      );
      
      if (existing && existing.status === 'Completed') {
        console.log(`Salary already transferred for employee ${employee_id}`);
        continue;
      }
      
      // Get payroll details
      const earnings = await calculateGrossSalary(employee_id, year, month);
      const deductions = await calculateDeductions(employee_id, year, month);
      const netSalary = earnings.gross_salary - deductions.total_deductions;
      
      if (existing) {
        // Update existing transfer
        await conn.query(
          `UPDATE payroll_transfers 
           SET gross_salary = ?, total_deductions = ?, net_salary = ?, 
               status = 'Processing', processed_by = ?, updated_at = NOW()
           WHERE id = ?`,
          [earnings.gross_salary, deductions.total_deductions, netSalary, user_id, existing.id]
        );
      } else {
        // Insert new transfer
        await conn.query(
          `INSERT INTO payroll_transfers 
            (employee_id, period_year, period_month, gross_salary, total_deductions, net_salary, 
             status, processed_by, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 'Processing', ?, NOW(), NOW())`,
          [employee_id, year, month, earnings.gross_salary, deductions.total_deductions, netSalary, user_id]
        );
      }
      
      processed.push(employee_id);
    }
    
    await conn.commit();
    
    logAudit({
      user_id,
      action_type: 'INITIATE_BANK_TRANSFER',
      target_table: 'payroll_transfers',
      target_id: processed.join(','),
      before_state: null,
      after_state: { month, year, processed_count: processed.length },
      req,
      status: 'SUCCESS'
    });
    
    res.json({
      ok: true,
      message: `Bank transfer initiated for ${processed.length} employee(s)`,
      processed_count: processed.length
    });
    
  } catch (err) {
    await conn.rollback();
    console.error('initiateBankTransfer error:', err);
    logEvent({
      level: 'error',
      event_type: 'INITIATE_BANK_TRANSFER_ERROR',
      user_id: req.user?.id,
      severity: 'ERROR',
      error_message: err.message
    });
    res.status(500).json({ ok: false, message: 'Failed to initiate bank transfer' });
  } finally {
    conn.release();
  }
};


// Add these optimized functions to payroll.controller.js

/**
 * OPTIMIZED: Get payroll summary for dashboard (bulk calculation)
 */
exports.getPayrollSummaryOptimized = async (req, res) => {
  try {
    const { month, year } = req.query;
    
    const currentDate = new Date();
    const currentMonth = month ? parseInt(month) : currentDate.getMonth() + 1;
    const currentYear = year ? parseInt(year) : currentDate.getFullYear();
    
    // Get period dates for filtering
    const periodStart = `${currentYear}-${String(currentMonth).padStart(2, '0')}-01`;
    const periodEnd = new Date(currentYear, currentMonth, 0).toISOString().slice(0, 10);
    
    // Get all active employees with their basic salary in ONE query
    const [employees] = await pool.query(`
      SELECT 
        e.id as employee_id,
        e.full_name,
        COALESCE(s.basic_salary, 0) as basic_salary
      FROM employees e
      LEFT JOIN salaries s ON s.employee_id = e.id 
        AND s.id = (SELECT MAX(id) FROM salaries WHERE employee_id = e.id)
      WHERE e.status = 'Active'
    `);
    
    if (employees.length === 0) {
      return res.json({
        ok: true,
        data: {
          totalEmployees: 0,
          grossSalary: '0.00',
          netSalary: '0.00',
          totalDeductions: '0.00',
          grossChange: '0%',
          grossTrend: 'up',
          pendingCases: 0
        }
      });
    }
    
    const employeeIds = employees.map(e => e.employee_id);
    
    // Bulk calculate allowances in ONE query
    const [allowancesResult] = await pool.query(`
      SELECT 
        employee_id,
        COALESCE(SUM(amount), 0) as total_allowances
      FROM allowances
      WHERE employee_id IN (?)
        AND status = 'Active'
        AND (effective_from IS NULL OR effective_from <= ?)
        AND (effective_to IS NULL OR effective_to >= ?)
      GROUP BY employee_id
    `, [employeeIds, periodEnd, periodStart]);
    
    // Bulk calculate overtime in ONE query
    const [overtimeResult] = await pool.query(`
      SELECT 
        employee_id,
        COALESCE(SUM(ot_hours * ot_rate), 0) as total_overtime
      FROM overtime_adjustments
      WHERE employee_id IN (?)
        AND YEAR(created_at) = ? 
        AND MONTH(created_at) = ?
      GROUP BY employee_id
    `, [employeeIds, currentYear, currentMonth]);
    
    // Bulk calculate bonuses in ONE query
    const [bonusesResult] = await pool.query(`
      SELECT 
        employee_id,
        COALESCE(SUM(amount), 0) as total_bonuses
      FROM bonuses
      WHERE employee_id IN (?)
        AND YEAR(effective_date) = ? 
        AND MONTH(effective_date) = ?
      GROUP BY employee_id
    `, [employeeIds, currentYear, currentMonth]);
    
    // Bulk calculate deductions in ONE query (excluding EPF for now)
    const [deductionsResult] = await pool.query(`
      SELECT 
        employee_id,
        COALESCE(SUM(
          CASE 
            WHEN basis = 'Percent' AND percent IS NOT NULL 
            THEN (percent/100) * COALESCE((SELECT basic_salary FROM salaries WHERE employee_id = d.employee_id ORDER BY id DESC LIMIT 1), 0)
            ELSE COALESCE(amount, 0)
          END
        ), 0) as total_deductions
      FROM deductions d
      WHERE employee_id IN (?)
        AND status = 'Active'
        AND YEAR(effective_date) = ? 
        AND MONTH(effective_date) = ?
        AND name NOT LIKE '%EPF%'
      GROUP BY employee_id
    `, [employeeIds, currentYear, currentMonth]);
    
    // Bulk calculate EPF deductions in ONE query
    const [epfResult] = await pool.query(`
      SELECT 
        ee.employee_id,
        COALESCE(s.basic_salary, 0) as basic_salary,
        COALESCE(ee.epf_contribution_rate, 8.00) as epf_rate,
        COALESCE(ee.employer_epf_rate, 12.00) as employer_epf_rate,
        COALESCE(ee.etf_contribution_rate, 3.00) as etf_rate
      FROM employee_etf_epf ee
      LEFT JOIN salaries s ON s.employee_id = ee.employee_id 
        AND s.id = (SELECT MAX(id) FROM salaries WHERE employee_id = ee.employee_id)
      WHERE ee.employee_id IN (?)
    `, [employeeIds]);
    
    // Bulk calculate unpaid leave deductions in ONE query
    const [unpaidLeaveResult] = await pool.query(`
      SELECT 
        employee_id,
        COALESCE(SUM(deduction_amount), 0) as total_unpaid_leave
      FROM unpaid_leaves
      WHERE employee_id IN (?)
        AND status = 'Processed'
        AND YEAR(updated_at) = ? 
        AND MONTH(updated_at) = ?
      GROUP BY employee_id
    `, [employeeIds, currentYear, currentMonth]);
    
    // Convert arrays to maps for O(1) lookups
    const allowancesMap = {};
    allowancesResult.forEach(item => {
      allowancesMap[item.employee_id] = Number(item.total_allowances);
    });
    
    const overtimeMap = {};
    overtimeResult.forEach(item => {
      overtimeMap[item.employee_id] = Number(item.total_overtime);
    });
    
    const bonusesMap = {};
    bonusesResult.forEach(item => {
      bonusesMap[item.employee_id] = Number(item.total_bonuses);
    });
    
    const deductionsMap = {};
    deductionsResult.forEach(item => {
      deductionsMap[item.employee_id] = Number(item.total_deductions);
    });
    
    const epfMap = {};
    epfResult.forEach(item => {
      const basicSalary = Number(item.basic_salary || 0);
      const epfRate = Number(item.epf_rate || 8.00);
      epfMap[item.employee_id] = (basicSalary * epfRate) / 100;
    });
    
    const unpaidLeaveMap = {};
    unpaidLeaveResult.forEach(item => {
      unpaidLeaveMap[item.employee_id] = Number(item.total_unpaid_leave);
    });
    
    // Calculate totals in JavaScript (fast)
    let totalGross = 0;
    let totalDeductions = 0;
    let totalNet = 0;
    
    employees.forEach(emp => {
      const basic = Number(emp.basic_salary || 0);
      const allowances = allowancesMap[emp.employee_id] || 0;
      const overtime = overtimeMap[emp.employee_id] || 0;
      const bonuses = bonusesMap[emp.employee_id] || 0;
      
      const gross = basic + allowances + overtime + bonuses;
      const regularDeductions = deductionsMap[emp.employee_id] || 0;
      const epfDeduction = epfMap[emp.employee_id] || 0;
      const unpaidLeave = unpaidLeaveMap[emp.employee_id] || 0;
      
      const totalEmployeeDeductions = regularDeductions + epfDeduction + unpaidLeave;
      
      totalGross += gross;
      totalDeductions += totalEmployeeDeductions;
      totalNet += (gross - totalEmployeeDeductions);
    });
    
    // Get previous month totals (using same optimized approach)
    const prevMonth = currentMonth === 1 ? 12 : currentMonth - 1;
    const prevYear = currentMonth === 1 ? currentYear - 1 : currentYear;
    
    const [prevOvertime] = await pool.query(`
      SELECT COALESCE(SUM(ot_hours * ot_rate), 0) as total_overtime
      FROM overtime_adjustments
      WHERE YEAR(created_at) = ? AND MONTH(created_at) = ?
    `, [prevYear, prevMonth]);
    
    const prevGross = totalGross - (overtimeResult.reduce((sum, item) => sum + item.total_overtime, 0)) 
      + (prevOvertime[0]?.total_overtime || 0);
    
    const grossChange = prevGross > 0 ? 
      ((totalGross - prevGross) / prevGross * 100).toFixed(1) : 0;
    
    res.json({
      ok: true,
      data: {
        totalEmployees: employees.length,
        grossSalary: totalGross.toFixed(2),
        netSalary: totalNet.toFixed(2),
        totalDeductions: totalDeductions.toFixed(2),
        grossChange: `${grossChange > 0 ? '↑' : '↓'} ${Math.abs(grossChange)}%`,
        grossTrend: grossChange > 0 ? 'up' : 'down',
        pendingCases: 0
      }
    });
    
  } catch (err) {
    console.error('getPayrollSummaryOptimized error:', err);
    res.status(500).json({ ok: false, message: 'Failed to get payroll summary' });
  }
};

/**
 * OPTIMIZED: Get payroll transfer overview (bulk calculation)
 */
exports.getPayrollTransferOverviewOptimized = async (req, res) => {
  try {
    const { month, year, page = 1, limit = 10 } = req.query;
    
    if (!month || !year) {
      return res.status(400).json({ ok: false, message: 'Month and year required' });
    }
    
    const currentMonth = parseInt(month);
    const currentYear = parseInt(year);
    const offset = (parseInt(page) - 1) * parseInt(limit);
    
    const periodStart = `${currentYear}-${String(currentMonth).padStart(2, '0')}-01`;
    const periodEnd = new Date(currentYear, currentMonth, 0).toISOString().slice(0, 10);
    
    // Get total count
    const [[totalCount]] = await pool.query(
      'SELECT COUNT(*) as count FROM employees WHERE status = "Active"'
    );
    
    // Get paginated employees with their data
    const [employees] = await pool.query(`
      SELECT 
        e.id as employee_id,
        e.employee_code,
        e.full_name,
        e.phone,
        d.name as department,
        COALESCE(s.basic_salary, 0) as basic_salary
      FROM employees e
      LEFT JOIN departments d ON d.id = e.department_id
      LEFT JOIN salaries s ON s.employee_id = e.id 
        AND s.id = (SELECT MAX(id) FROM salaries WHERE employee_id = e.id)
      WHERE e.status = 'Active'
      ORDER BY e.full_name
      LIMIT ? OFFSET ?
    `, [parseInt(limit), offset]);
    
    if (employees.length === 0) {
      return res.json({
        ok: true,
        data: [],
        pagination: {
          page: parseInt(page),
          limit: parseInt(limit),
          total: totalCount.count,
          totalPages: Math.ceil(totalCount.count / limit)
        }
      });
    }
    
    const employeeIds = employees.map(e => e.employee_id);
    
    // Bulk fetch all required data
    const [allowances] = await pool.query(`
      SELECT employee_id, COALESCE(SUM(amount), 0) as total
      FROM allowances 
      WHERE employee_id IN (?)
        AND status = 'Active'
        AND (effective_from IS NULL OR effective_from <= ?)
        AND (effective_to IS NULL OR effective_to >= ?)
      GROUP BY employee_id
    `, [employeeIds, periodEnd, periodStart]);
    
    const [overtime] = await pool.query(`
      SELECT employee_id, COALESCE(SUM(ot_hours * ot_rate), 0) as total
      FROM overtime_adjustments
      WHERE employee_id IN (?) 
        AND YEAR(created_at) = ? 
        AND MONTH(created_at) = ?
      GROUP BY employee_id
    `, [employeeIds, currentYear, currentMonth]);
    
    const [bonuses] = await pool.query(`
      SELECT employee_id, COALESCE(SUM(amount), 0) as total
      FROM bonuses
      WHERE employee_id IN (?) 
        AND YEAR(effective_date) = ? 
        AND MONTH(effective_date) = ?
      GROUP BY employee_id
    `, [employeeIds, currentYear, currentMonth]);
    
    const [deductions] = await pool.query(`
      SELECT employee_id, COALESCE(SUM(
        CASE 
          WHEN basis = 'Percent' AND percent IS NOT NULL 
          THEN (percent/100) * COALESCE((SELECT basic_salary FROM salaries WHERE employee_id = d.employee_id ORDER BY id DESC LIMIT 1), 0)
          ELSE COALESCE(amount, 0)
        END
      ), 0) as total
      FROM deductions d
      WHERE employee_id IN (?)
        AND status = 'Active'
        AND YEAR(effective_date) = ? 
        AND MONTH(effective_date) = ?
        AND name NOT LIKE '%EPF%'
      GROUP BY employee_id
    `, [employeeIds, currentYear, currentMonth]);
    
    const [epfData] = await pool.query(`
      SELECT 
        ee.employee_id,
        COALESCE(s.basic_salary, 0) as basic_salary,
        COALESCE(ee.epf_contribution_rate, 8.00) as epf_rate
      FROM employee_etf_epf ee
      LEFT JOIN salaries s ON s.employee_id = ee.employee_id 
        AND s.id = (SELECT MAX(id) FROM salaries WHERE employee_id = ee.employee_id)
      WHERE ee.employee_id IN (?)
    `, [employeeIds]);
    
    const [unpaidLeaves] = await pool.query(`
      SELECT employee_id, COALESCE(SUM(deduction_amount), 0) as total
      FROM unpaid_leaves
      WHERE employee_id IN (?) 
        AND status = 'Processed'
        AND YEAR(updated_at) = ? 
        AND MONTH(updated_at) = ?
      GROUP BY employee_id
    `, [employeeIds, currentYear, currentMonth]);
    
    const [transfers] = await pool.query(`
      SELECT employee_id, status 
      FROM payroll_transfers 
      WHERE employee_id IN (?) 
        AND period_year = ? 
        AND period_month = ?
    `, [employeeIds, currentYear, currentMonth]);
    
    // Create lookup maps
    const createMap = (arr, keyField, valueField) => {
      const map = {};
      arr.forEach(item => {
        map[item[keyField]] = Number(item[valueField] || 0);
      });
      return map;
    };
    
    const allowancesMap = createMap(allowances, 'employee_id', 'total');
    const overtimeMap = createMap(overtime, 'employee_id', 'total');
    const bonusesMap = createMap(bonuses, 'employee_id', 'total');
    const deductionsMap = createMap(deductions, 'employee_id', 'total');
    const unpaidLeaveMap = createMap(unpaidLeaves, 'employee_id', 'total');
    
    const epfMap = {};
    epfData.forEach(item => {
      const basicSalary = Number(item.basic_salary || 0);
      const epfRate = Number(item.epf_rate || 8.00);
      epfMap[item.employee_id] = (basicSalary * epfRate) / 100;
    });
    
    const transferStatusMap = {};
    transfers.forEach(item => {
      transferStatusMap[item.employee_id] = item.status;
    });
    
    // Build result array
    const result = employees.map(emp => {
      const basic = Number(emp.basic_salary || 0);
      const allowances = allowancesMap[emp.employee_id] || 0;
      const overtime = overtimeMap[emp.employee_id] || 0;
      const bonuses = bonusesMap[emp.employee_id] || 0;
      
      const gross = basic + allowances + overtime + bonuses;
      const regularDeductions = deductionsMap[emp.employee_id] || 0;
      const epfDeduction = epfMap[emp.employee_id] || 0;
      const unpaidLeave = unpaidLeaveMap[emp.employee_id] || 0;
      
      const totalDeductions = regularDeductions + epfDeduction + unpaidLeave;
      const netSalary = gross - totalDeductions;
      
      return {
        id: emp.employee_id,
        name: emp.full_name,
        employee_code: emp.employee_code,
        phone: emp.phone || 'N/A',
        department: emp.department || 'N/A',
        gross_salary: gross.toFixed(2),
        net_salary: netSalary.toFixed(2),
        deductions: totalDeductions.toFixed(2),
        bank_status: transferStatusMap[emp.employee_id] || 'Pending'
      };
    });
    
    res.json({
      ok: true,
      data: result,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: totalCount.count,
        totalPages: Math.ceil(totalCount.count / limit)
      }
    });
    
  } catch (err) {
    console.error('getPayrollTransferOverviewOptimized error:', err);
    res.status(500).json({ ok: false, message: 'Failed to get transfer overview' });
  }
};

/**
 * OPTIMIZED: Get payroll status (fast)
 */
exports.getPayrollStatusOptimized = async (req, res) => {
  try {
    const { month, year } = req.query;
    
    const currentDate = new Date();
    const currentMonth = month ? parseInt(month) : currentDate.getMonth() + 1;
    const currentYear = year ? parseInt(year) : currentDate.getFullYear();
    
    // Use EXISTS instead of COUNT for faster checks
    const [hasPayrollRun] = await pool.query(`
      SELECT EXISTS(
        SELECT 1 FROM payroll_cycles 
        WHERE period_year = ? AND period_month = ?
      ) as has_run
    `, [currentYear, currentMonth]);
    
    const [hasTransfers] = await pool.query(`
      SELECT EXISTS(
        SELECT 1 FROM payroll_transfers 
        WHERE period_year = ? AND period_month = ?
      ) as has_transfers
    `, [currentYear, currentMonth]);
    
    const [pendingCount] = await pool.query(`
      SELECT COUNT(*) as count FROM payroll_transfers 
      WHERE status = 'Pending' AND period_year = ? AND period_month = ?
    `, [currentYear, currentMonth]);
    
    const calculation = hasPayrollRun[0].has_run ? 'Completed' : 'Not Started';
    const bankTransfer = hasTransfers[0].has_transfers ? 
      (pendingCount[0].count > 0 ? 'In Progress' : 'Completed') : 'Not Started';
    const approval = hasPayrollRun[0].has_run ? 'Completed' : 'Not Started';
    const completion = hasTransfers[0].has_transfers && pendingCount[0].count === 0 ? 'Completed' : 'Not Started';
    
    let step = 1;
    if (hasPayrollRun[0].has_run) step = 2;
    if (hasTransfers[0].has_transfers) step = 3;
    if (hasTransfers[0].has_transfers && pendingCount[0].count === 0) step = 4;
    
    res.json({
      ok: true,
      data: {
        calculation,
        approval,
        bankTransfer,
        completion,
        step,
        totalSteps: 4
      }
    });
    
  } catch (err) {
    console.error('getPayrollStatusOptimized error:', err);
    res.status(500).json({ ok: false, message: 'Failed to get payroll status' });
  }
};