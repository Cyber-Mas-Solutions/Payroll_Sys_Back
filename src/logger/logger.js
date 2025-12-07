const dotenv = require('dotenv');
const path = require('path');
const fs = require('fs');
const { createLogger, format, transports } = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');

dotenv.config({ path: path.resolve(__dirname, '../../.env') });

// Ensure log directory exists
const logDirectory = process.env.LOG_DIR || path.resolve('logs');
if (!fs.existsSync(logDirectory)) {
  fs.mkdirSync(logDirectory, { recursive: true });
}

// Common console transport (DRY)
const consoleTransport = new transports.Console({
  format: format.combine(
    format.colorize(),
    format.printf(({ level, message, timestamp, stack }) =>
      stack ? `[${timestamp}] ${level}: ${stack}` : `[${timestamp}] ${level}: ${message}`
    )
  ),
});

// Factory function to create loggers
const createDailyLogger = ({ name, filename, maxFiles = '30d' }) => {
  const dailyRotateTransport = new DailyRotateFile({
    filename: path.join(logDirectory, filename),
    datePattern: 'YYYY-MM-DD',
    zippedArchive: true,
    maxSize: '20m',
    maxFiles,
    level: 'info',
    handleExceptions: true,
    format: format.combine(
      format.timestamp(),
      format.errors({ stack: true }),
      format.json()
    ),
  });

  return createLogger({
    level: 'info',
    format: format.combine(
      format.timestamp(),
      format.errors({ stack: true }),
      format.json()
    ),
    transports: [consoleTransport, dailyRotateTransport],
    exceptionHandlers: [dailyRotateTransport],
    exitOnError: false,
  });
};

// Create separate loggers
const eventLogger = createDailyLogger({ name: 'event', filename: '%DATE%-events.log', maxFiles: '60d' });
const auditLogger = createDailyLogger({ name: 'audit', filename: '%DATE%-audits.log', maxFiles: '3650d' });

// Export clean interface
module.exports = {
  event: eventLogger,
  audit: auditLogger,
};
