require('dotenv').config();
const pool = require('../src/config/db');
const bcrypt = require('bcryptjs');

(async () => {
  try {
    const name = 'HR Admin';
    const email = 'hr@company.com';
    const password = 'Admin@123'; // change later
    const hash = await bcrypt.hash(password, 10);

    await pool.query(
      'INSERT INTO users (name, email, password_hash, role) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE role=VALUES(role)',
      [name, email, hash, 'HR']
    );
    console.log(`Seeded HR user:\n  email: ${email}\n  password: ${password}`);
    process.exit(0);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
