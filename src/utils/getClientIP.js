function normalizeIP(ip) {
  if (ip === '::1') return '127.0.0.1';
  if (ip && ip.startsWith('::ffff:')) return ip.replace('::ffff:', '');
  return ip;
}

function getClientIP(req) {
  const rawIP =
    req.headers['x-forwarded-for']?.split(',')[0] ||
    req.body.ip ||
    'unknown';
  // return normalizeIP(rawIP);
  return normalizeIP(rawIP);
}

module.exports = getClientIP;
