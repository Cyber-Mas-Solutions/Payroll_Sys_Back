const pool = require('../config/db');

exports.getTimetables = async (req, res) => {
    try {
        const [timetables] = await pool.query('SELECT * FROM timetables ORDER BY created_at DESC');
        res.json({ ok: true, data: timetables });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

exports.createTimetable = async (req, res) => {
    try {
        const { name, check_in_start, check_in_end, check_out_start, check_out_end, grace_period_start, grace_period_end, type } = req.body;
        
        const [result] = await pool.query(
            'INSERT INTO timetables (name, check_in_start, check_in_end, check_out_start, check_out_end, grace_period_start, grace_period_end, type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            [name, check_in_start, check_in_end, check_out_start, check_out_end, grace_period_start, grace_period_end, type]
        );
        
        res.json({ ok: true, message: 'Timetable created successfully', id: result.insertId });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

exports.updateTimetable = async (req, res) => {
    try {
        const { id } = req.params;
        const { name, check_in_start, check_in_end, check_out_start, check_out_end, grace_period_start, grace_period_end, type } = req.body;
        
        await pool.query(
            'UPDATE timetables SET name = ?, check_in_start = ?, check_in_end = ?, check_out_start = ?, check_out_end = ?, grace_period_start = ?, grace_period_end = ?, type = ? WHERE id = ?',
            [name, check_in_start, check_in_end, check_out_start, check_out_end, grace_period_start, grace_period_end, type, id]
        );
        
        res.json({ ok: true, message: 'Timetable updated successfully' });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

exports.deleteTimetable = async (req, res) => {
    try {
        const { id } = req.params;
        
        await pool.query('DELETE FROM timetables WHERE id = ?', [id]);
        
        res.json({ ok: true, message: 'Timetable deleted successfully' });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

// Employee timetable assignment
exports.assignTimetable = async (req, res) => {
    try {
        const { employee_id, timetable_id, effective_date } = req.body;
        
        // Remove any existing assignment for this employee
        await pool.query('DELETE FROM employee_timetables WHERE employee_id = ?', [employee_id]);
        
        // Add new assignment
        await pool.query(
            'INSERT INTO employee_timetables (employee_id, timetable_id, effective_date) VALUES (?, ?, ?)',
            [employee_id, timetable_id, effective_date]
        );
        
        res.json({ ok: true, message: 'Timetable assigned successfully' });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

// Attendance Records
exports.getAttendanceRecords = async (req, res) => {
    try {
        const { startDate, endDate, employeeId, department, status } = req.query;
        
        let query = `
            SELECT ar.*, e.full_name, e.department_name, e.employee_code, e.designation
            FROM attendance_records ar 
            JOIN employees e ON ar.employee_id = e.id 
            WHERE 1=1
        `;
        const params = [];
        
        if (startDate) {
            query += ' AND ar.date >= ?';
            params.push(startDate);
        }
        if (endDate) {
            query += ' AND ar.date <= ?';
            params.push(endDate);
        }
        if (employeeId) {
            query += ' AND ar.employee_id = ?';
            params.push(employeeId);
        }
        if (department) {
            query += ' AND e.department_name = ?';
            params.push(department);
        }
        if (status) {
            query += ' AND ar.status = ?';
            params.push(status);
        }
        
        query += ' ORDER BY ar.date DESC, e.full_name';
        
        const [records] = await pool.query(query, params);
        res.json({ ok: true, data: records });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

exports.getEmployeeAttendance = async (req, res) => {
    try {
        const { employeeId } = req.params;
        const { startDate, endDate } = req.query;
        
        let query = `
            SELECT * FROM attendance_records 
            WHERE employee_id = ?
        `;
        const params = [employeeId];
        
        if (startDate) {
            query += ' AND date >= ?';
            params.push(startDate);
        }
        if (endDate) {
            query += ' AND date <= ?';
            params.push(endDate);
        }
        
        query += ' ORDER BY date DESC';
        
        const [records] = await pool.query(query, params);
        res.json({ ok: true, data: records });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

exports.checkIn = async (req, res) => {
    try {
        const { employee_id, date, check_in_time, notes } = req.body;
        
        // Check if record already exists for today
        const [existing] = await pool.query(
            'SELECT * FROM attendance_records WHERE employee_id = ? AND date = ?',
            [employee_id, date]
        );
        
        if (existing.length > 0) {
            // Update existing record
            await pool.query(
                'UPDATE attendance_records SET check_in_time = ?, notes = ? WHERE employee_id = ? AND date = ?',
                [check_in_time, notes, employee_id, date]
            );
        } else {
            // Create new record
            await pool.query(
                'INSERT INTO attendance_records (employee_id, date, check_in_time, notes) VALUES (?, ?, ?, ?)',
                [employee_id, date, check_in_time, notes]
            );
        }
        
        res.json({ ok: true, message: 'Check-in recorded successfully' });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

exports.checkOut = async (req, res) => {
    try {
        const { employee_id, date, check_out_time, notes } = req.body;
        
        // Update existing record
        await pool.query(
            'UPDATE attendance_records SET check_out_time = ?, notes = COALESCE(?, notes) WHERE employee_id = ? AND date = ?',
            [check_out_time, notes, employee_id, date]
        );
        
        res.json({ ok: true, message: 'Check-out recorded successfully' });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

// Attendance Adjustments
exports.getAdjustments = async (req, res) => {
    try {
        const { status, employeeId } = req.query;
        
        let query = `
            SELECT aa.*, e.full_name, e.department_name, e.employee_code,
                   u.name as approved_by_name
            FROM attendance_adjustments aa
            JOIN employees e ON aa.employee_id = e.id
            LEFT JOIN users u ON aa.approved_by = u.id
            WHERE 1=1
        `;
        const params = [];
        
        if (status) {
            query += ' AND aa.status = ?';
            params.push(status);
        }
        if (employeeId) {
            query += ' AND aa.employee_id = ?';
            params.push(employeeId);
        }
        
        query += ' ORDER BY aa.created_at DESC';
        
        const [adjustments] = await pool.query(query, params);
        res.json({ ok: true, data: adjustments });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

exports.createAdjustment = async (req, res) => {
  try {
    const { employee_id, adjustment_date, adjustment_type, adjusted_time, reason } = req.body;

    // 1) Save the adjustment record (for audit trail)
    const [result] = await pool.query(
      `
        INSERT INTO attendance_adjustments
          (employee_id, adjustment_date, adjustment_type, adjusted_time, reason, status)
        VALUES (?, ?, ?, ?, ?, ?)
      `,
      [employee_id, adjustment_date, adjustment_type, adjusted_time, reason, "Pending"]
    );

    // 2) Apply the change to the main attendance_records table
    // (this is what AttendanceOverview is reading)
    if (adjustment_type === "Check-in") {
      await pool.query(
        "UPDATE attendance_records SET check_in_time = ? WHERE employee_id = ? AND date = ?",
        [adjusted_time, employee_id, adjustment_date]
      );
    } else if (adjustment_type === "Check-out") {
      await pool.query(
        "UPDATE attendance_records SET check_out_time = ? WHERE employee_id = ? AND date = ?",
        [adjusted_time, employee_id, adjustment_date]
      );
    } else if (adjustment_type === "Full Day") {
      
      await pool.query(
        "UPDATE attendance_records SET status = ? WHERE employee_id = ? AND date = ?",
        ["Present", employee_id, adjustment_date]
      );
    } else if (adjustment_type === "Half Day") {
      await pool.query(
        "UPDATE attendance_records SET status = ? WHERE employee_id = ? AND date = ?",
        ["Half Day", employee_id, adjustment_date]
      );
    }



    res.json({
      ok: true,
      message: "Attendance adjustment applied successfully",
      id: result.insertId,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ ok: false, message: error.message });
  }
};


exports.approveAdjustment = async (req, res) => {
    try {
        const { id } = req.params;
        const { status, decision_note } = req.body;
        const approved_by = req.user.id; // Assuming you have user info in req.user
        
        await pool.query(
            'UPDATE attendance_adjustments SET status = ?, approved_by = ?, decision_note = ? WHERE id = ?',
            [status, approved_by, decision_note, id]
        );
        
        res.json({ ok: true, message: `Adjustment ${status.toLowerCase()} successfully` });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

// Reports
exports.getAbsenceReport = async (req, res) => {
    try {
        const { startDate, endDate, department, employeeName } = req.query;
        
        let query = `
            SELECT 
                e.employee_code AS empNo, 
                e.id AS employee_id,
                ar.date,    
                e.full_name,
                COALESCE(e.department_name, d.name) AS department_name,
                e.designation,
                ar.status,
                ar.notes
                , e.calling_name
                , e.working_office
                , e.branch AS branch_name
            FROM attendance_records ar
            JOIN employees e ON ar.employee_id = e.id
            LEFT JOIN departments d ON e.department_id = d.id
            WHERE ar.status IN ('Absent', 'Half Day')
        `;
        const params = [];
        
        if (startDate) {
            query += ' AND ar.date >= ?';
            params.push(startDate);
        }
        if (endDate) {
            query += ' AND ar.date <= ?';
            params.push(endDate);
        }
        if (department) {
            query += ' AND e.department_name = ?';
            params.push(department);
        }
        if (employeeName) {
            query += ' AND e.full_name LIKE ?';
            params.push(`%${employeeName}%`);
        }
        
        query += ' ORDER BY ar.date DESC, e.full_name';
        
        const [records] = await pool.query(query, params);
        res.json({ ok: true, data: records });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
    }
};

// Reports
exports.getCheckinCheckoutReport = async (req, res) => {
  try {
    const { startDate, endDate, department } = req.query;

    let query = `
      SELECT 
        ar.date,
        ar.check_in_time,
        ar.check_out_time,
        ar.total_hours,
        ar.overtime_hours,
        ar.status,

        e.id AS employee_id,
        e.employee_code,      -- 🔥 keep the real employee code
        e.full_name,
        e.department_name,
        e.designation
      FROM attendance_records ar
      JOIN employees e ON ar.employee_id = e.id
      WHERE ar.check_in_time IS NOT NULL
    `;
    const params = [];

    if (startDate) {
      query += ' AND ar.date >= ?';
      params.push(startDate);
    }
    if (endDate) {
      query += ' AND ar.date <= ?';
      params.push(endDate);
    }
    if (department) {
      query += ' AND e.department_name = ?';
      params.push(department);
    }

    query += ' ORDER BY ar.date DESC, e.full_name';

    const [records] = await pool.query(query, params);
    return res.json({ ok: true, data: records });
  } catch (error) {
    console.error('getCheckinCheckoutReport error:', error);
    return res.status(500).json({ ok: false, message: error.message });
  }
};
