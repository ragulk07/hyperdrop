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

// In-memory room store
// roomCode -> { host: ws, joiner: ws }
const rooms = new Map();

// Helper to generate 6-character random room code
function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // Avoid easily confused chars
  let code = '';
  for (let i = 0; i < 6; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return rooms.has(code) ? generateRoomCode() : code;
}

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
          rooms.set(code, { host: ws, joiner: null });

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
          const room = rooms.get(code);

          if (!room) {
            ws.send(JSON.stringify({
              type: 'error',
              message: 'Room code not found or has expired.'
            }));
            return;
          }

          if (room.joiner && room.joiner.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
              type: 'error',
              message: 'Room is already full (max 2 devices per session).'
            }));
            return;
          }

          ws.roomCode = code;
          ws.role = 'joiner';
          room.joiner = ws;

          // Notify joiner
          ws.send(JSON.stringify({
            type: 'room_joined',
            roomCode: code,
            role: 'joiner'
          }));

          // Notify host that joiner has arrived
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
