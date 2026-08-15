const fs = require('fs');
const http = require('http');
const path = require('path');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

// Determine static public directory
const publicDir = fs.existsSync(path.join(__dirname, 'public', 'index.html'))
  ? path.join(__dirname, 'public')
  : __dirname;

// In-memory unified room store (Shared between WebSockets & HTTP fallback)
// roomCode -> { host: ws/null, joiner: ws/null, signals: [], lastActive: Date.now() }
const rooms = new Map();

// Helper to generate 6-character random room code
function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 6; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return rooms.has(code) ? generateRoomCode() : code;
}

app.use(express.json());

app.all('/api/signal', (req, res) => {
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
    rooms.set(room, { host: null, joiner: null, signals: [], lastActive: Date.now() });
  }

  const roomData = rooms.get(room);
  roomData.lastActive = Date.now();

  if (req.method === 'POST') {
    const signal = body.signal || body;
    if (signal) {
      signal.id = signal.id || ('sig_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6));
      signal.timestamp = Date.now();
      roomData.signals.push(signal);
      if (roomData.signals.length > 100) roomData.signals.shift();
    }
    return res.status(200).json({ success: true, signalId: signal ? signal.id : null });
  }

  if (req.method === 'GET') {
    const since = parseInt(req.query.since || '0', 10);
    const newSignals = roomData.signals.filter(s => s.timestamp > since);
    return res.status(200).json({ room, signals: newSignals, serverTime: Date.now() });
  }

  return res.status(405).json({ error: 'Method not allowed' });
});

// Serve static frontend files
app.use(express.static(publicDir));

// Catch-all route to serve index.html for all page requests (including room query params)
app.use((req, res) => {
  const indexPath = path.join(publicDir, 'index.html');
  res.sendFile(indexPath);
});

const os = require('os');

// Helper to get local Wi-Fi / Ethernet IPv4 address for mobile pairing (ignoring 169.254 APIPA addresses)
function getLocalIpAddress() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal && !iface.address.startsWith('169.254.')) {
        return iface.address;
      }
    }
  }
  return null;
}

const localIp = getLocalIpAddress();

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.roomCode = null;
  ws.role = null; // 'host' or 'joiner'

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (messageText) => {
    try {
      const data = JSON.parse(messageText.toString());
      const { type, roomCode, payload } = data;

      switch (type) {
        case 'create_room': {
          const code = generateRoomCode();
          ws.roomCode = code;
          ws.role = 'host';

          let room = rooms.get(code);
          if (!room) {
            room = { host: ws, joiner: null, signals: [], lastActive: Date.now() };
            rooms.set(code, room);
          } else {
            room.host = ws;
          }

          ws.send(JSON.stringify({
            type: 'room_created',
            roomCode: code,
            role: 'host',
            localIp: localIp,
            port: PORT
          }));
          console.log(`[Room Created] ${code}`);
          break;
        }

        case 'join_room': {
          const code = (roomCode || '').toUpperCase().trim();
          let room = rooms.get(code);

          if (!room) {
            if (code.length === 6) {
              room = { host: null, joiner: ws, signals: [], lastActive: Date.now() };
              rooms.set(code, room);
            } else {
              ws.send(JSON.stringify({
                type: 'error',
                message: 'Room code not found or has expired.'
              }));
              return;
            }
          } else {
            room.joiner = ws;
          }

          ws.roomCode = code;
          ws.role = 'joiner';

          // Notify joiner
          ws.send(JSON.stringify({
            type: 'room_joined',
            roomCode: code,
            role: 'joiner'
          }));

          // Notify host if host is connected via WebSocket
          if (room.host && room.host.readyState === WebSocket.OPEN) {
            room.host.send(JSON.stringify({
              type: 'peer_joined',
              peerRole: 'joiner'
            }));
          }

          console.log(`[Room Joined] ${code} by Joiner`);
          break;
        }

        case 'offer':
        case 'answer':
        case 'ice_candidate':
        case 'transfer_signal': {
          if (!ws.roomCode) return;
          const room = rooms.get(ws.roomCode);
          if (!room) return;

          const signalPayload = {
            type: type === 'ice_candidate' ? 'ICE_CANDIDATE' : type.toUpperCase(),
            senderRole: ws.role,
            id: 'ws_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6),
            timestamp: Date.now()
          };
          if (type === 'offer') signalPayload.offer = payload;
          else if (type === 'answer') signalPayload.answer = payload;
          else if (type === 'ice_candidate') signalPayload.candidate = payload;
          else signalPayload.payload = payload;

          room.signals.push(signalPayload);
          if (room.signals.length > 100) room.signals.shift();

          const target = ws.role === 'host' ? room.joiner : room.host;
          if (target && target.readyState === WebSocket.OPEN) {
            target.send(JSON.stringify({
              type: type,
              payload: payload,
              senderRole: ws.role
            }));
          }
          break;
        }

        case 'leave_room': {
          cleanUpClient(ws);
          break;
        }

        default:
          console.warn(`[Unknown Message Type]`, type);
      }
    } catch (err) {
      console.error('[Message Parse Error]', err);
    }
  });

  ws.on('close', () => {
    cleanUpClient(ws);
  });

  ws.on('error', (err) => {
    console.error('[WebSocket Error]', err);
    cleanUpClient(ws);
  });
});

function cleanUpClient(ws) {
  if (!ws.roomCode) return;
  const room = rooms.get(ws.roomCode);
  if (room) {
    const target = ws.role === 'host' ? room.joiner : room.host;
    if (target && target.readyState === WebSocket.OPEN) {
      target.send(JSON.stringify({
        type: 'peer_left',
        role: ws.role
      }));
    }

    if (ws.role === 'host') {
      room.host = null;
    } else if (ws.role === 'joiner') {
      room.joiner = null;
    }

    // Delete room if empty
    if (!room.host && !room.joiner) {
      rooms.delete(ws.roomCode);
      console.log(`[Room Cleaned] ${ws.roomCode}`);
    }
  }
  ws.roomCode = null;
  ws.role = null;
}

// Heartbeat ping interval to keep sockets alive across routers
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('close', () => {
  clearInterval(heartbeat);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 HyperDrop P2P Server listening on 0.0.0.0:${PORT}`);
});
