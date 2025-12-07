// src/controllers/employee.controller.js
const pool = require('../config/db');
const path = require('path');
const fs = require('fs');
const logAudit = require('../utils/audit');
const logEvent = require('../utils/event');

function baseUrl(req) {
  return process.env.BACKEND_PUBLIC_URL || `${req.protocol}://${req.get('host')}`;
}

function toUrl(req, filePath) {
  if (!filePath) return null;

  // normalize
  const normalized = String(filePath).replace(/\\/g, '/');

  // If DB has full path like ".../uploads/12345_pic.jpg"
  const idx = normalized.toLowerCase().lastIndexOf('/uploads/');
  if (idx !== -1) {
    const rel = normalized.slice(idx + 1); // "uploads/12345_pic.jpg"
    return `${baseUrl(req)}/${rel}`;
  }

  // New style: DB has only filename "12345_pic.jpg"
  const filename = normalized.split('/').pop();
  return `${baseUrl(req)}/uploads/${filename}`;
}

function kindFromType(t = '') {
  if (t.startsWith('image/')) return 'image';
  if (t === 'application/pdf') return 'pdf';
  return 'file';
}

function extractCategory(file_name = '') {
  // we store "Category::original.ext" for personal documents (no DB change needed)
  const m = String(file_name).match(/^([^:]+)::/);
  return m ? m[1] : null;
}

function stripCategoryPrefix(name = '') {
  return String(name).replace(/^[^:]+::/, '');
}

// map grade letter → grade_id
function gradeToId(grade) {
  if (!grade) return null;
  const g = String(grade).trim().toUpperCase();
  if (g === 'A') return 1;
  if (g === 'B') return 2;
  if (g === 'C') return 3;
  return null;
}

// helper: get or create department by name
async function getOrCreateDepartmentId(conn, deptName) {
  if (!deptName) return null;
  const name = String(deptName).trim();
  if (!name) return null;

  const [exist] = await conn.query('SELECT id FROM departments WHERE name = ?', [name]);
  if (exist.length) return exist[0].id;

  const [ins] = await conn.query('INSERT INTO departments (name) VALUES (?)', [name]);
  return ins.insertId;
}

exports.createEmployee = async (req, res) => {
  const {
    first_name, last_name, initials, calling_name,
    email, personal_email, country_code, phone,
    gender, dob, marital_status, nationality, religion, nic,
    address_permanent, address_temporary,

    appointment_date,
    department, designation, working_office, branch, employment_type,
    basic_salary, status = 'Active', supervisor, grade, designated_emails, epf_no,etf_no,

    kin_name, relationship, kin_nic, kin_dob,
  } = req.body;

  // only store file *name* in DB, not full disk path
  const profilePhotoFile = req.files?.profilePhoto?.[0];
  const profilePhotoPath = profilePhotoFile ? profilePhotoFile.filename : null;

  const generalDocs = Array.isArray(req.files?.documents) ? req.files.documents : [];
  const bankDoc = req.files?.bankDocument?.[0];

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    const department_id = await getOrCreateDepartmentId(conn, department);
    const grade_id = gradeToId(grade);

    const full_name =
      [first_name, last_name].filter(Boolean).join(' ').trim() ||
      calling_name ||
      email;

    const [empIns] = await conn.query(
      `INSERT INTO employees
       (employee_code, full_name, email, personal_email, phone, country_code,
        department_id, designation, status, joining_date, appointment_date,
        address, address_permanent, address_temporary, emergency_contact,
        profile_photo_path, created_by_user_id,
        first_name, last_name, initials, calling_name,
        gender, dob, marital_status, nationality, religion, nic,
        working_office, branch, employment_type, supervisor, grade, designated_emails, etf_no, grade_id)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [
        null,                       // employee_code
        full_name,
        email || null,
        personal_email || null,
        phone || null,
        country_code || null,
        department_id,
        designation || null,
        status || 'Active',
        appointment_date || null,   // joining_date
        appointment_date || null,   // appointment_date
        address_permanent || null,  // address
        address_permanent || null,
        address_temporary || null,
        null,                       // emergency_contact
        profilePhotoPath || null,
        (req.user && req.user.id) || 1,
        first_name || null,
        last_name || null,
        initials || null,
        calling_name || null,
        gender || null,
        dob || null,
        marital_status || null,
        nationality || null,
        religion || null,
        nic || null,
        working_office || null,
        branch || null,
        employment_type || null,
        supervisor || null,
        grade || null,
        designated_emails || null,
        etf_no || null,
        grade_id || null,
      ]
    );
    const employeeId = empIns.insertId;

    if (epf_no) {
      await conn.query(
        `INSERT INTO employee_etf_epf (employee_id, epf_number, epf_status) 
         VALUES (?, ?, 'Active')
         ON DUPLICATE KEY UPDATE epf_number = VALUES(epf_number)`,
        [employeeId, epf_no]
      );
      
      // OPTIONAL: Keep local employees.epf_no in sync if legacy systems need it
      await conn.query('UPDATE employees SET epf_no = ? WHERE id = ?', [epf_no, employeeId]);
    }

    if (basic_salary && !Number.isNaN(Number(basic_salary))) {
      await conn.query(
        'INSERT INTO salaries (employee_id, basic_salary) VALUES (?, ?)',
        [employeeId, Number(basic_salary)]
      );
    }

    if (kin_name || relationship || kin_nic || kin_dob) {
      await conn.query(
        'INSERT INTO employee_kin (employee_id, kin_name, relationship, kin_nic, kin_dob) VALUES (?,?,?,?,?)',
        [employeeId, kin_name || null, relationship || null, kin_nic || null, kin_dob || null]
      );
    }

    const { account_number, account_name, bank_name, branch_name } = req.body;
    if (account_number || account_name || bank_name || branch_name) {
      await conn.query(
        `INSERT INTO employee_bank_accounts
          (employee_id, account_number, account_name, bank_name, branch_name)
         VALUES (?,?,?,?,?)`,
        [employeeId, account_number || null, account_name || null, bank_name || null, branch_name || null]
      );
    }

    // ---- DOCUMENTS ----
    // Frontend sends documents[] plus document_type_0, document_type_1, ... and total_documents
    const totalDocs = Number(req.body.total_documents || generalDocs.length || 0);
    // totalDocs is kept for future checks if needed

    for (let i = 0; i < generalDocs.length; i++) {
      const f = generalDocs[i];
      const typeKey = `document_type_${i}`;
      const category = (req.body[typeKey] || '').trim(); // e.g. "NIC Copy"
      const original = f.originalname;
      const storedName = category ? `${category}::${original}` : original;
      await conn.query(
        'INSERT INTO employee_documents (employee_id, file_name, file_path, file_type) VALUES (?,?,?,?)',
        [employeeId, storedName, f.path.replace(/\\/g, '/'), f.mimetype]
      );
    }

    // Bank document: store with category "Bank Document"
    if (bankDoc) {
      const original = bankDoc.originalname;
      const storedName = `Bank Document::${original}`;
      await conn.query(
        'INSERT INTO employee_documents (employee_id, file_name, file_path, file_type) VALUES (?,?,?,?)',
        [employeeId, storedName, bankDoc.path.replace(/\\/g, '/'), bankDoc.mimetype]
      );
    }

    await conn.commit();

    const after_state = {
      employee_id: employeeId,
      full_name,
      email,
      department_id,
      designation,
      employment_type,
      basic_salary: Number(basic_salary) || null,
      supervisor,
      grade,
      has_documents: generalDocs.length > 0 || !!bankDoc,
      has_kin: !!kin_name,
      has_bank_account: !!account_number
    };

    logAudit({
      user_id: req.user.id,
      action_type: "CREATE_EMPLOYEE",
      target_table: "employees",
      target_id: employeeId,
      before_state: null,
      after_state,
      req,
      status: "SUCCESS"
    });


    res.status(201).json({ ok: true, id: employeeId, message: 'Employee created' });
  } catch (e) {
    await conn.rollback();
    console.error(e);
    logAudit({
      user_id: req.user?.id || null,
      action_type: "CREATE_EMPLOYEE",
      target_table: "employees",
      target_id: null,
      before_state: null,
      after_state: { employee_id: employeeId, full_name, epf_no, etf_no },
      req,
      status: "FAILURE",
      error_message: e.message
    }).catch(() => { });
    res.status(500).json({ ok: false, message: 'Failed to create employee' });
  } finally {
    conn.release();
  }
};

exports.getEmployees = async (req, res) => {
  try {
    const [rows] = await pool.query(
      `SELECT e.*, d.name AS department_name
     FROM employees e
     LEFT JOIN departments d ON d.id = e.department_id
     ORDER BY e.created_at DESC`
    );
    // add profile_photo_url for listing
    for (const r of rows) {
      r.profile_photo_url = r.profile_photo_path ? toUrl(req, r.profile_photo_path) : null;
    }
    res.json({ ok: true, data: rows });

  } catch (error) {
    logEvent({ level: "error", event_type: "GET_EMPLOYEES_FAILURE", user_id: req.user?.id || null, event_details: { error }, error_message: error.message })
    res.status(500).json({ ok: false, message: "Failed to fetch employees" });
  }
};

exports.getEmployeeById = async (req, res) => {
  try {
    const id = Number(req.params.id);
    const [[emp]] = await pool.query(
      `SELECT e.*, d.name AS department_name
     FROM employees e
     LEFT JOIN departments d ON d.id = e.department_id
     WHERE e.id = ?`,
      [id]
    );
    if (!emp) return res.status(404).json({ ok: false, message: 'Not found' });

    // FETCH EPF from separate table
    const [[epfData]] = await pool.query(
      'SELECT epf_number FROM employee_etf_epf WHERE employee_id = ?', 
      [id]
    );
    // Overwrite emp.epf_no with the data from the specific table if available
    if(epfData && epfData.epf_number) {
      emp.epf_no = epfData.epf_number;
    }

    const [docs] = await pool.query(
      'SELECT id, file_name, file_path, file_type, uploaded_at FROM employee_documents WHERE employee_id = ? ORDER BY id DESC',
      [id]
    );
    const [[kin]] = await pool.query('SELECT * FROM employee_kin WHERE employee_id = ? LIMIT 1', [id]);
    const [[bank]] = await pool.query('SELECT * FROM employee_bank_accounts WHERE employee_id = ? LIMIT 1', [id]);
    const [[sal]] = await pool.query('SELECT basic_salary FROM salaries WHERE employee_id = ? ORDER BY id DESC LIMIT 1', [id]);

    emp.kin = kin || null;
    emp.bank_account = bank || null;
    emp.basic_salary = sal ? sal.basic_salary : null;

    emp.documents = (docs || []).map(d => {
      const url = toUrl(req, d.file_path);
      const category = extractCategory(d.file_name);
      const cleanName = stripCategoryPrefix(d.file_name);
      return {
        ...d,
        file_name: cleanName,
        file_path: d.file_path,
        url,
        kind: kindFromType(d.file_type),
        doc_category: category,            // <-- used by FE
      };
    });

    emp.profile_photo_url = emp.profile_photo_path ? toUrl(req, emp.profile_photo_path) : null;

    res.json({ ok: true, data: emp });
  } catch (error) {
    logEvent({ level: "error", event_type: "GET_EMPLOYEE_FAILED", user_id: req.user?.id || null, event_details: { error }, error_message: error.message })

    res.status(500).json({ ok: false, message: "Failed to fetch employee" });
  }
};

exports.updateEmployee = async (req, res) => {
  const id = Number(req.params.id);

  const body = req.body || {};
  const profilePhotoFile = req.files?.profilePhoto?.[0];
  const profilePhotoPath = profilePhotoFile ? profilePhotoFile.filename : undefined;
  const generalDocs = Array.isArray(req.files?.documents) ? req.files.documents : [];
  const bankDoc = req.files?.bankDocument?.[0];

  const conn = await pool.getConnection();

  let before_state = null;
  try {
    await conn.beginTransaction();
    // Get BEFORE state
    const [beforeRows] = await conn.query('SELECT * FROM employees WHERE id = ?', [id]);
    if (!beforeRows.length) {
      await conn.rollback();
      return res.status(404).json({ ok: false, message: 'Not found' });
    }
    const before_state = beforeRows[0];

    let department_id = null;
    if (body.department || body.department_name) {
      department_id = await getOrCreateDepartmentId(conn, body.department || body.department_name);
    }

    let full_name = undefined;
    if (body.first_name || body.last_name) {
      const fn = body.first_name || '';
      const ln = body.last_name || '';
      full_name = [fn, ln].filter(Boolean).join(' ').trim() || undefined;
    }

    // derive grade_id either from explicit grade_id or from grade letter
    let grade_id_val = undefined;
    if (Object.prototype.hasOwnProperty.call(body, 'grade_id')) {
      grade_id_val = body.grade_id ? Number(body.grade_id) || null : null;
    } else if (body.grade) {
      grade_id_val = gradeToId(body.grade);
    }

    const fields = {
      employee_code: body.employee_code ?? null,
      full_name,
      email: body.email,
      personal_email: body.personal_email,
      phone: body.phone,
      country_code: body.country_code,
      department_id,
      designation: body.designation,
      status: body.status,
      joining_date: body.appointment_date,
      appointment_date: body.appointment_date,
      address: body.address_permanent,
      address_permanent: body.address_permanent,
      address_temporary: body.address_temporary,
      profile_photo_path: profilePhotoPath,

      first_name: body.first_name,
      last_name: body.last_name,
      initials: body.initials,
      calling_name: body.calling_name,
      gender: body.gender,
      dob: body.dob,
      marital_status: body.marital_status,
      nationality: body.nationality,
      religion: body.religion,
      nic: body.nic,

      working_office: body.working_office,
      branch: body.branch,
      employment_type: body.employment_type,
      supervisor: body.supervisor,
      grade: body.grade,
      grade_id: grade_id_val,
      designated_emails: body.designated_emails,

      etf_no: body.etf_no,
      epf_no: body.epf_no,

      kin_name: body.kin_name,
      kin_nic: body.kin_nic,
      kin_dob: body.kin_dob,
      relationship: body.relationship
    };

    const setParts = [];
    const setVals = [];
    for (const [k, v] of Object.entries(fields)) {
      if (v === undefined) continue;
      setParts.push(`${k} = COALESCE(?, ${k})`);
      setVals.push(v || null);
    }
    setVals.push(id);

    if (setParts.length) {
      const [r] = await conn.query(`UPDATE employees SET ${setParts.join(', ')} WHERE id = ?`, setVals);
      if (!r.affectedRows) {
        await conn.rollback();
        return res.status(404).json({ ok: false, message: 'Not found' });
      }
    }

    // UPDATE EPF in the Separate Table
    if (body.epf_no) {
      // Check if record exists
      const [[exist]] = await conn.query('SELECT id FROM employee_etf_epf WHERE employee_id = ?', [id]);
      if (exist) {
        await conn.query('UPDATE employee_etf_epf SET epf_number = ? WHERE employee_id = ?', [body.epf_no, id]);
      } else {
        await conn.query('INSERT INTO employee_etf_epf (employee_id, epf_number) VALUES (?, ?)', [id, body.epf_no]);
      }
    }

    if (body.basic_salary !== undefined && body.basic_salary !== null && body.basic_salary !== '') {
      await conn.query('INSERT INTO salaries (employee_id, basic_salary) VALUES (?, ?)', [id, Number(body.basic_salary)]);
    }

    // ---- DOCUMENTS ----
    const totalDocs = Number(req.body.total_documents || generalDocs.length || 0);
    // totalDocs is kept for future checks if needed

    for (let i = 0; i < generalDocs.length; i++) {
      const f = generalDocs[i];
      const typeKey = `document_type_${i}`;
      const category = (req.body[typeKey] || '').trim();
      const original = f.originalname;
      const storedName = category ? `${category}::${original}` : original;

      await conn.query(
        'INSERT INTO employee_documents (employee_id, file_name, file_path, file_type) VALUES (?,?,?,?)',
        [id, storedName, f.path.replace(/\\/g, '/'), f.mimetype]
      );
    }

    if (bankDoc) {
      const original = bankDoc.originalname;
      const storedName = `Bank Document::${original}`;
      await conn.query(
        'INSERT INTO employee_documents (employee_id, file_name, file_path, file_type) VALUES (?,?,?,?)',
        [id, storedName, bankDoc.path.replace(/\\/g, '/'), bankDoc.mimetype]
      );
    }

    await conn.commit();

    // Get AFTER state
    const [afterRows] = await conn.query('SELECT * FROM employees WHERE id = ?', [id]);
    const after_state = afterRows[0];

    // Audit log
    logAudit({
      user_id: req.user.id,
      action_type: "UPDATE_EMPLOYEE",
      target_table: "employees",
      target_id: id,
      before_state: before_state,
      after_state: after_state,
      req,
      status: "SUCCESS"
    });

    res.json({ ok: true, message: 'Updated' });
  } catch (e) {
    await conn.rollback();
    console.error(e);
    logAudit({
      user_id: req.user.id,
      action_type: "UPDATE_EMPLOYEE",
      target_table: "employees",
      target_id: id,
      before_state: before_state || null,
      after_state: null,
      req,
      status: "FAILURE",
      error_message: e.message
    }).catch(() => { });
    res.status(500).json({ ok: false, message: 'Failed to update employee' });
  } finally {
    conn.release();
  }
};

exports.deleteEmployee = async (req, res) => {
  const conn = await pool.getConnection();
  const id = Number(req.params.id);
  try {
    await conn.beginTransaction();

    const [[before_state]] = await conn.query('SELECT * FROM employees WHERE id = ?', [id]);
    if (!before_state) throw new Error('Not found');

    await conn.query('DELETE FROM salaries WHERE employee_id = ?', [id]);
    await conn.query('DELETE FROM employee_kin WHERE employee_id = ?', [id]);
    await conn.query('DELETE FROM employee_bank_accounts WHERE employee_id = ?', [id]);
    await conn.query('DELETE FROM employee_documents WHERE employee_id = ?', [id]);
    await conn.query('DELETE FROM employees WHERE id = ?', [id]);

    logAudit({
      user_id: req.user.id,
      action_type: "DELETE_EMPLOYEE",
      target_table: "employees",
      target_id: id,
      before_state,
      after_state: null,
      req,
      status: "SUCCESS"
    });

    await conn.commit();
    res.json({ ok: true, message: 'Deleted' });
  } catch (err) {
    await conn.rollback();
    console.error(err);
    logAudit({
      user_id: req.user.id,
      action_type: "DELETE_EMPLOYEE",
      target_table: "employees",
      target_id: id,
      before_state: before_state || null,
      after_state: null, // fixed typo
      req,
      status: "FAILURE",
      error_message: e.message
    }).catch(() => { });
    res.status(500).json({ ok: false, message: 'Failed to delete employee' });
  } finally {
    conn.release();
  }

};

// ---------- NEW: per-document delete/replace ----------

exports.deleteEmployeeDocument = async (req, res) => {
  const empId = Number(req.params.id);
  const docId = Number(req.params.docId);

  let before_state = null;

  try {
    const [[doc]] = await pool.query(
      'SELECT file_path FROM employee_documents WHERE id = ? AND employee_id = ?',
      [docId, empId]
    );
    if (!doc) return res.status(404).json({ ok: false, message: 'Document not found' });


    before_state = doc;

    await pool.query('DELETE FROM employee_documents WHERE id = ? AND employee_id = ?', [docId, empId]);

    try {
      fs.unlinkSync(path.resolve(doc.file_path));
    } catch (_) { }

    // Audit (SUCCESS)
    logAudit({
      user_id: req.user?.id || null,
      action_type: "DELETE_EMPLOYEE_DOCUMENT",
      target_table: "employee_documents",
      target_id: docId,
      before_state,
      after_state: null,
      req,
      status: "SUCCESS"
    });

    res.json({ ok: true, message: 'Document deleted' });

  } catch (e) {
    console.error(e);

    // Audit (FAILURE)
    logAudit({
      user_id: req.user?.id || null,
      action_type: "DELETE_EMPLOYEE_DOCUMENT",
      target_table: "employee_documents",
      target_id: docId,
      before_state,
      after_state: null,
      req,
      status: "FAILURE",
      error_message: e.message
    }).catch(() => { });

    res.status(500).json({ ok: false, message: 'Failed to delete document' });
  }
};


exports.replaceEmployeeDocument = async (req, res) => {
  const empId = Number(req.params.id);
  const docId = Number(req.params.docId);
  const f = req.file;

  if (!f) return res.status(400).json({ ok: false, message: 'file required' });

  let before_state = null;
  let after_state = null;

  try {
    const [[doc]] = await pool.query(
      'SELECT file_path FROM employee_documents WHERE id = ? AND employee_id = ?',
      [docId, empId]
    );
    if (!doc) return res.status(404).json({ ok: false, message: 'Document not found' });


    before_state = doc;

    // Update file
    await pool.query(
      'UPDATE employee_documents SET file_name = ?, file_path = ?, file_type = ? WHERE id = ? AND employee_id = ?',
      [f.originalname, f.path.replace(/\\/g, '/'), f.mimetype, docId, empId]
    );


    // Fetch after state
    const [[updatedDoc]] = await pool.query(
      'SELECT * FROM employee_documents WHERE id = ? AND employee_id = ?',
      [docId, empId]
    );
    after_state = updatedDoc;

    try {
      fs.unlinkSync(path.resolve(doc.file_path));
    } catch (_) { }


    // Audit (SUCCESS)
    logAudit({
      user_id: req.user?.id || null,
      action_type: "REPLACE_EMPLOYEE_DOCUMENT",
      target_table: "employee_documents",
      target_id: docId,
      before_state,
      after_state,
      status: "SUCCESS",
      req
    });

    res.json({ ok: true, message: 'Document replaced' });

  } catch (e) {
    console.error(e);

    // Audit (FAILURE)
    logAudit({
      user_id: req.user?.id || null,
      action_type: "REPLACE_EMPLOYEE_DOCUMENT",
      target_table: "employee_documents",
      target_id: docId,
      before_state,
      after_state: null,
      status: "FAILURE",
      error_message: e.message,
      req
    }).catch(() => { });

    res.status(500).json({ ok: false, message: 'Failed to replace document' });
  }
};


// ================= PERFORMANCE & TRAINING =================

// Latest performance review per employee (for overview page)
exports.getPerformanceOverview = async (req, res) => {
  try {
    const [rows] = await pool.query(`
      SELECT 
        e.id AS employee_id,
        e.employee_code,
        e.full_name,
        e.designation,
        d.name AS department_name,
        pr.rating,
        pr.review_date,
        pr.reviewer_name,
        pr.strengths,
        pr.improvements
      FROM employees e
      LEFT JOIN departments d ON d.id = e.department_id
      LEFT JOIN employee_performance_reviews pr
        ON pr.id = (
          SELECT pr2.id 
          FROM employee_performance_reviews pr2
          WHERE pr2.employee_id = e.id
          ORDER BY pr2.review_date DESC, pr2.id DESC
          LIMIT 1
        )
      WHERE e.status = 'Active'
      ORDER BY e.full_name ASC
    `);

    res.json({ ok: true, data: rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: 'Failed to load performance overview' });
  }
};

// Create a new performance review
exports.addPerformanceReview = async (req, res) => {
  try {
    const {
      employee_id,
      review_date,      // 'YYYY-MM-DD'
      reviewer_name,
      rating,           // 0 - 5, can have decimals
      strengths,
      improvements,
    } = req.body;

    const [result] = await pool.query(
      `INSERT INTO employee_performance_reviews
        (employee_id, review_date, reviewer_name, rating, strengths, improvements)
       VALUES (?,?,?,?,?,?)`,
      [
        employee_id,
        review_date || new Date().toISOString().slice(0, 10),
        reviewer_name || null,
        rating || 0,
        strengths || null,
        improvements || null,
      ]
    );

    res.status(201).json({ ok: true, id: result.insertId, message: 'Performance review added' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: 'Failed to add performance review' });
  }
};

// Latest/active training record per employee
exports.getTrainingOverview = async (req, res) => {
  try {
    const [rows] = await pool.query(`
      SELECT
        e.id AS employee_id,
        e.employee_code,
        e.full_name,
        e.designation,
        d.name AS department_name,
        tr.training_course,
        tr.status,
        tr.start_date,
        tr.end_date,
        tr.score,
        tr.certificate
      FROM employees e
      LEFT JOIN departments d ON d.id = e.department_id
      LEFT JOIN employee_training_records tr
        ON tr.id = (
          SELECT tr2.id
          FROM employee_training_records tr2
          WHERE tr2.employee_id = e.id
          ORDER BY 
            CASE tr2.status
              WHEN 'In Progress' THEN 1
              WHEN 'Scheduled' THEN 2
              WHEN 'Completed' THEN 3
              ELSE 4
            END,
            tr2.start_date DESC,
            tr2.id DESC
          LIMIT 1
        )
      WHERE e.status = 'Active'
      ORDER BY e.full_name ASC
    `);

    res.json({ ok: true, data: rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: 'Failed to load training overview' });
  }
};

// Assign / schedule training
exports.addTrainingRecord = async (req, res) => {
  try {
    const {
      employee_id,
      training_course,
      status,       // 'Scheduled' | 'In Progress' | 'Completed' | 'Cancelled'
      start_date,   // 'YYYY-MM-DD'
      end_date,     // 'YYYY-MM-DD'
      score,        // string like '95%' or null
      certificate,  // 'Available' | 'Not Available'
    } = req.body;

    if (!employee_id || !training_course) {
      return res.status(400).json({ ok: false, message: 'employee_id and training_course are required' });
    }

    const [result] = await pool.query(
      `INSERT INTO employee_training_records
        (employee_id, training_course, status, start_date, end_date, score, certificate)
       VALUES (?,?,?,?,?,?,?)`,
      [
        employee_id,
        training_course,
        status || 'Scheduled',
        start_date || null,
        end_date || null,
        score || null,
        certificate || 'Not Available',
      ]
    );

    res.status(201).json({ ok: true, id: result.insertId, message: 'Training record added' });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, message: 'Failed to add training record' });
  }
};
