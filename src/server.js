// src/server.js
require('dotenv').config();
const app = require('./app');
const pool = require('./config/db');
const logEvent = require('./utils/event');

const PORT = process.env.PORT || 4000;

async function start() {
  try {
    await pool.query('SELECT 1');
    app.listen(PORT, () => console.log('API on', PORT));
    logEvent({level:'info',event_type: `APP_IS_RUNNING_ON_${PORT}`})
  } catch (e) {
    console.error('Cannot connect to MySQL', e);
    process.exit(1);
  }
}
start();
