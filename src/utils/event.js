const pool = require('../config/db'); 
const { event: eventLogger } = require('../logger/logger');
const getClientIP = require('./getClientIP');

async function logEvent({ level = 'info', event_type, user_id = null, email = null, req = null, extra = {} }) {
  
  const ip = req ? getClientIP(req) : null;
  const ua = req ? req.headers['user-agent'] || null : null;

  const logPayload = {
    timestamp: new Date().toISOString(),
    action: event_type,
    actor: { id: user_id, email, ip, user_agent: ua },
    details: { ...extra }
  };

  // Log to Winston
  if (eventLogger && typeof eventLogger[level] === 'function') {
    eventLogger[level](logPayload);
  } else {
    console.error('Invalid logger level:', level, logPayload);
  }

  // Insert into DB
  try {
    const sql = `
      INSERT INTO event_logs
      (event_type, user_id, severity, event_details, created_at)
      VALUES (?, ?, ?, ?, NOW())
    `;
    const params = [
      event_type,
      user_id,
      level.toUpperCase(),
      JSON.stringify(logPayload)
    ];

    await pool.query(sql, params);
  } catch (err) {
    console.error('Failed to insert event log into DB', err);
  }
}

module.exports = logEvent;
