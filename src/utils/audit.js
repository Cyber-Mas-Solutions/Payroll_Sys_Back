const pool = require('../config/db');
const { audit: auditLogger } = require('../logger/logger');
const getClientIP = require('../utils/getClientIP')
const { getDifferences } = require('../utils/getDifference');

async function logAudit({ level = "info", user_id, action_type, target_table, target_id,
    before_state = null, after_state = null, status = 'SUCCESS', error_message = null, req = null }) {

    const ip = req ? getClientIP(req) : null;

    const diff = getDifferences(before_state, after_state);
    const logPayload = {
        message: `${action_type} on ${target_table}#${target_id} by user ${user_id}`,
        timestamp: new Date().toISOString(),
        actor: { id: user_id },
        action: action_type,
        target: { table: target_table, id: target_id },
        change: diff,
        before_state, 
        after_state,
        ip, 
        status, 
        error_message
    };

    // Log to Winston
    const logLevel = ['error','warn','info','verbose','debug','silly'].includes(level) ? level : 'info';
    if (auditLogger && typeof auditLogger[logLevel] === 'function') {
        auditLogger[logLevel](logPayload);
    } else {
        console.error('Audit logger not available:', logPayload);
    }

    try {
        const sql = `
      INSERT INTO audit_logs
      (user_id, action_type, target_table, target_id, before_state, after_state, ip_address, status, error_message, different)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
        const params = [
            user_id,
            action_type,
            target_table,
            target_id,
            before_state ? JSON.stringify(before_state) : null,
            after_state ? JSON.stringify(after_state) : null,
            ip,
            status,
            error_message,
            JSON.stringify(diff)  
        ];


        console.log("afczdvfzdsvf")

        await pool.query(sql, params);
    } catch (err) {
        console.error('Failed to insert audit log into DB', err);
    }
}

module.exports = logAudit;

