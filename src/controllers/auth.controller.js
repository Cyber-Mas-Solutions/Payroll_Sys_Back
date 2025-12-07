const pool = require('../config/db');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const logEvent = require('../utils/event');

exports.login = async (req, res) => {
  const { email, password } = req.body;
  let user = null;

  try {
  const [rows] = await pool.query('SELECT * FROM users WHERE email = ?', [email]);
  const user = rows[0];

    if (!user) {
      logEvent({ level: 'error', event_type: "LOGIN_FAILURE", email, req });
      return res.status(401).json({ ok: false, message: 'Invalid credentials' });
    }

    const ok = await bcrypt.compare(password, user.password_hash);
    if (!ok) {
      logEvent({ level: 'error', event_type: "LOGIN_FAILURE", user_id: user.id, email, req });
      return res.status(401).json({ ok: false, message: 'Invalid credentials' });
    }

    const token = jwt.sign(
      { id: user.id, role: user.role, name: user.name, email: user.email },
      process.env.JWT_SECRET || 'super_secret_change_me',
      { expiresIn: process.env.JWT_EXPIRES_IN || '8h' }
    );

    logEvent({ level: 'info', event_type: "LOGIN_SUCCESS", user_id: user.id, email, req });


  res.json({
    ok: true,
    token,
    user: { id: user.id, name: user.name, email: user.email, role: user.role }
  });
  } catch (err) {
    console.error(err);

    logEvent({ level: 'error', event_type: "LOGIN_ERROR", user_id: user?.id, email, req, extra: { error_message: err.message } });
    return res.status(500).json({ ok: false, message: 'Server error' });
  }
};

exports.me = async (req, res) => {
  res.json({ ok:true, user: req.user });
};
