const { event: eventLogger } = require('../logger/logger');
const getClientIP = require('./getClientIP');

function extractActor(req, user_id) {
  return {
    id: req?.user?.id ?? user_id ?? null,
    ip: req ? getClientIP(req) : null,
    user_agent: req?.headers["user-agent"] ?? null
  };
}

async function logEvent({ level = 'info', event_type, user_id = null, req = null, extra = {} }) {

  const actor = extractActor(req, user_id);

  const logPayload = {
    actor,
    action: event_type,
    ip: req?.ip || null,
    extra,
    timestamp: new Date().toISOString(),
  };

  // Log to Winston
  if (eventLogger && typeof eventLogger[level] === 'function') {
    eventLogger[level](logPayload);
  } else {
    console.error('Invalid logger level:', level, logPayload);
  }

}

module.exports = logEvent;
