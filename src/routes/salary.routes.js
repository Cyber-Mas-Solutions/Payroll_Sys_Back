const express = require('express');
const ctrl = require('../controllers/salary.controller');
const { requireAuth, requireRole } = require('../middleware/auth');
const etfEpfCtrl = require('../controllers/etfEpf.controller');

const router = express.Router();

// protect all salary endpoints
router.use(requireAuth, requireRole('HR'));

// overtime rule + adjustment 
router.get('/grades', ctrl.getGrades);

//rule 
router.get('/overtime/rules/:gradeId', ctrl.getOvertimeRulesByGrade);
router.post('/overtime/rules', ctrl.upsertOvertimeRule);

//adjustemnts
router.get('/overtime/adjustments/grade/:gradeId', ctrl.listOvertimeAdjustmentsByGrade);
router.post('/overtime/adjustments', ctrl.createOvertimeAdjustment);

// basic salary
router.post('/basic', ctrl.setBasicSalary);
router.get('/basic', ctrl.getBasicSalary);

// allowances
router.post('/allowance', ctrl.addAllowance);
router.get('/allowances', ctrl.listAllowances);
router.get('/allowance/:id', ctrl.getAllowanceById);        // ADD THIS
router.put('/allowance/:id', ctrl.updateAllowance);         // ADD THIS  
router.delete('/allowance/:id', ctrl.deleteAllowance);      // ADD THIS

// deductions
router.get('/deductions', ctrl.listDeductions);
router.post('/deductions', ctrl.createDeduction);

router.get('/deductions/:id', ctrl.getDeductionById);
router.put('/deductions/:id', ctrl.updateDeduction);
router.delete('/deductions/:id', ctrl.deleteDeduction);

// overtime / adjustments

//compensation 
// --- Employees advanced search + departments ---
router.get('/employees', ctrl.searchEmployeesAdvanced);
router.get('/departments', ctrl.listDepartments);

// --- Compensation (preview & apply) ---
router.post('/compensation/preview', ctrl.previewCompensation);
router.post('/compensation/apply',   ctrl.applyCompensation);


// bonuses
router.post('/bonus', ctrl.addBonus);

// earnings grid
router.get('/earnings', ctrl.listEarnings);

// ETF/EPF routes
router.get('/etf-epf', etfEpfCtrl.getEtfEpfRecords);
router.get('/etf-epf/employees-without', etfEpfCtrl.getEmployeesWithoutEtfEpf);

router.get('/etf-epf/:employeeId/history', etfEpfCtrl.getEmployeePaymentHistory);
router.get('/etf-epf/:id', etfEpfCtrl.getEtfEpfById);
router.post('/etf-epf', etfEpfCtrl.createEtfEpfRecord);
router.put('/etf-epf/:id', etfEpfCtrl.updateEtfEpfRecord);
router.delete('/etf-epf/:id', etfEpfCtrl.deleteEtfEpfRecord);
router.post('/etf-epf/calculate', etfEpfCtrl.calculateContributions);



// month summary / run payroll
router.get('/summary', ctrl.monthSummary);
router.post('/run', ctrl.runPayrollForMonth);

// payslip
router.get('/payslip', ctrl.generatePayslip);



module.exports = router;
