// src/middleware/auth.js
const jwt = require('jsonwebtoken');
const logEvent = require('../utils/event');

function requireAuth(req, res, next) {
  const hdr = req.headers.authorization || '';
  const token = hdr.startsWith('Bearer ') ? hdr.slice(7) : null;
  
  if (!token) {
    logEvent({
      level: 'error', 
      event_type: "AUTH_FAILURE", 
      req, 
      extra: { reason: "Missing token" } 
    });
    return res.status(401).json({ ok: false, message: 'Missing token' });
  }
  
  try {
    const payload = jwt.verify(token, `super_secret_change_me`);
    req.user = payload; // { id, role, name, email }
    next();
  } catch (e) {
    logEvent({
      level: 'error', 
      event_type: "AUTH_FAILURE", 
      user_id: null, 
      req, 
      extra: { reason: "Invalid/expired token", error: e.message } 
    });
    return res.status(401).json({ ok: false, message: 'Invalid/expired token' });
  }
}

// UPDATED: Function now accepts string or array
function requireRole(allowedRoles) {
  return (req, res, next) => {
    if (!req.user) {
      logEvent({
        level: 'error', 
        event_type: "AUTH_FAILURE", 
        user_id: null, 
        req, 
        extra: { reason: "No user in request" }
      });
      return res.status(403).json({ ok: false, message: 'Forbidden - No user' });
    }
    
    // Convert single role to array for consistent handling
    const roles = Array.isArray(allowedRoles) ? allowedRoles : [allowedRoles];
    
    if (!roles.includes(req.user.role)) {
      logEvent({
        level: 'error', 
        event_type: "AUTH_FAILURE", 
        user_id: req.user.id, 
        req, 
        extra: { 
          reason: "Insufficient permissions", 
          userRole: req.user.role, 
          requiredRoles: roles 
        }
      });
      return res.status(403).json({ 
        ok: false, 
        message: `Forbidden - Required role: ${roles.join(' or ')}` 
      });
    }
    
    next();
  };
}

module.exports = { requireAuth, requireRole };