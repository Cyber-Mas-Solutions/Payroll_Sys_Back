const pool = require('../config/db');
const logEvent = require('../utils/event');

exports.getauditLogs = async (req, res) => {
    try {
        const start = new Date(req.query.start);
        const end = new Date(req.query.end);

        // Optionally force end to end-of-day
        end.setHours(23, 59, 59, 999);
        const [auditlogs] = await pool.query(`
                SELECT 
                    audit_logs.audit_id,
                    audit_logs.action_type,
                    audit_logs.action_time,
                    audit_logs.target_id,
                    audit_logs.target_table,
                    audit_logs.status,
                    audit_logs.error_message,
                    users.id AS user_id,
                    users.name,
                    users.email
                FROM audit_logs
                INNER JOIN users ON audit_logs.user_id = users.id
                WHERE DATE(audit_logs.action_time) BETWEEN ? AND ?
                ORDER BY audit_logs.action_time DESC
            `, [start, end]);

        res.json({ ok: true, data: auditlogs });
    } catch (error) {
        res.status(500).json({ ok: false, message: error.message });
        logEvent({ level:'error', event_type:'GET_AUDITLOGS_FAIL', user_id: req.user?.id, req, extra: { error_message: error.message } });
        console.error(error);
    }
};

exports.getauditLogById = async (req, res) => {
    try {
        const auditId = req.params.id;
        const [auditlogs] = await pool.query(
            `SELECT 
             audit_logs.audit_id,
                    audit_logs.action_type,
                    audit_logs.action_time,
                    audit_logs.target_id,
                    audit_logs.target_table,
                    audit_logs.status,
                    audit_logs.error_message,
                    audit_logs.before_state,
                    audit_logs.after_state,
                    audit_logs.different,
                    users.id AS user_id,
                    users.name,
                    users.email
            FROM audit_logs 
            INNER JOIN users ON audit_logs.user_id = users.id WHERE audit_id = ?`, [auditId]);
        if (auditlogs.length === 0) {
            return res.status(404).json({ ok: false, message: 'Audit log not found' });
        }   
        res.json({ ok: true, data: auditlogs[0] });
    }   
    catch (error) {
        res.status(500).json({ ok: false, message: error.message });
        logEvent({ level:'error', event_type:'GET_AUDITLOG_BY_ID_FAIL', user_id: req.user?.id, req, extra: { error_message: error.message } });
        console.error(error);
    }
};
