const https = require('https');

const rooms = new Map();

// Helper to push to public relay for multi-region Vercel Edge syncing
function mirrorToPublicRelay(room, signal) {
  try {
    const signalPayload = typeof signal === 'string' ? signal : JSON.stringify(signal);
    const postData = JSON.stringify({ message: signalPayload });
    const req = https.request(`https://ntfy.sh/hyperdrop-v6-${room}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData)
      },
      timeout: 3000
    }, (res) => {
      res.resume();
    });
    req.on('error', () => {});
    req.write(postData);
    req.end();
  } catch (e) {}
}

module.exports = (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Content-Type', 'application/json');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  let body = req.body || {};
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch (e) {}
  }

  const room = (req.query.room || body.room || '').toUpperCase().trim();
  if (!room) {
    return res.status(400).json({ error: 'Room code required' });
  }

  if (!rooms.has(room)) {
    rooms.set(room, { signals: [], lastActive: Date.now() });
  }

  const roomData = rooms.get(room);
  roomData.lastActive = Date.now();

  if (req.method === 'POST') {
    const signal = body.signal || body;
    if (signal) {
      signal.id = signal.id || ('sig_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6));
      signal.timestamp = Date.now();
      roomData.signals.push(signal);
      if (roomData.signals.length > 100) {
        roomData.signals.shift();
      }
      mirrorToPublicRelay(room, signal);
    }
    return res.status(200).json({ success: true, signalId: signal ? signal.id : null });
  }

  if (req.method === 'GET') {
    const since = parseInt(req.query.since || '0', 10);
    const newSignals = roomData.signals.filter(s => s.timestamp > since);
    return res.status(200).json({ room, signals: newSignals, serverTime: Date.now() });
  }

  return res.status(405).json({ error: 'Method not allowed' });
};
