import pool from "../config/db.js";

export const createFinancialSummaryEventCore = async () => {
  try {
    // Drop first
    await pool.query("DROP EVENT IF EXISTS update_department_financial_summary");

    // Then create
    const createSQL = `
      CREATE EVENT update_department_financial_summary
      ON SCHEDULE EVERY 1 DAY
      STARTS CURRENT_TIMESTAMP + INTERVAL 5 SECOND
      DO
      BEGIN
          /* Payroll */
          INSERT INTO department_financial_summary (department_id, period_year, period_month, total_net_salary)
          SELECT e.department_id, pc.period_year, pc.period_month, SUM(pc.net_salary)
          FROM payroll_cycles pc
          JOIN employees e ON e.id = pc.employee_id
          GROUP BY e.department_id, pc.period_year, pc.period_month
          ON DUPLICATE KEY UPDATE total_net_salary = VALUES(total_net_salary);

          /* Allowances */
          INSERT INTO department_financial_summary (department_id, period_year, period_month, total_allowances)
          SELECT e.department_id, YEAR(a.effective_from), MONTH(a.effective_from), SUM(a.amount)
          FROM allowances a
          JOIN employees e ON e.id = a.employee_id
          GROUP BY e.department_id, YEAR(a.effective_from), MONTH(a.effective_from)
          ON DUPLICATE KEY UPDATE total_allowances = VALUES(total_allowances);

          /* Deductions */
          INSERT INTO department_financial_summary (department_id, period_year, period_month, total_deductions)
          SELECT e.department_id, YEAR(d.effective_date), MONTH(d.effective_date), SUM(d.amount)
          FROM deductions d
          JOIN employees e ON e.id = d.employee_id
          GROUP BY e.department_id, YEAR(d.effective_date), MONTH(d.effective_date)
          ON DUPLICATE KEY UPDATE total_deductions = VALUES(total_deductions);

          /* Bonuses */
          INSERT INTO department_financial_summary (department_id, period_year, period_month, total_bonuses)
          SELECT e.department_id, YEAR(b.effective_date), MONTH(b.effective_date), SUM(b.amount)
          FROM bonuses b
          JOIN employees e ON e.id = b.employee_id
          GROUP BY e.department_id, YEAR(b.effective_date), MONTH(b.effective_date)
          ON DUPLICATE KEY UPDATE total_bonuses = VALUES(total_bonuses);

          /* Reimbursements */
          INSERT INTO department_financial_summary (department_id, period_year, period_month, total_reimbursements)
          SELECT e.department_id, YEAR(r.month), MONTH(r.month), SUM(r.amount)
          FROM reimbursements r
          JOIN employees e ON e.id = r.employee_id
          GROUP BY e.department_id, YEAR(r.month), MONTH(r.month)
          ON DUPLICATE KEY UPDATE total_reimbursements = VALUES(total_reimbursements);
      END
    `;

    await pool.query(createSQL);
    console.log("✅ Event created successfully!");
  } catch (err) {
    console.error("❌ Error creating event:", err.message);
  }
};
