const pool = require('../config/db');
const { createFinancialSummaryEventCore } = require('./createEvent');

async function initializeMySQLEvents() {
  try {
    // ✅ Enable MySQL event scheduler
    await pool.query('SET GLOBAL event_scheduler = ON;');
    console.log('🕒 MySQL event scheduler enabled.');

    // ✅ Create or refresh department financial summary event
    await createFinancialSummaryEventCore();

    console.log('✅ MySQL events initialized successfully.');
  } catch (err) {
    console.error('❌ Failed to initialize MySQL events:', err.message);
  }
}

module.exports = { initializeMySQLEvents };
