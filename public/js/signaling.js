class SignalingClient {
  constructor() {
    this.ws = null;
    this.listeners = new Map();
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 3;
    this.roomCode = null;
    this.role = null;
    this.isConnected = false;
  }

  connect() {
    return new Promise((resolve, reject) => {
      const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${protocol}//${location.host}`;

      console.log('[Signaling] Connecting to:', wsUrl);

      let connectionTimer = setTimeout(() => {
        if (!this.isConnected) {
          if (this.ws) {
            try { this.ws.close(); } catch (e) {}
          }
          reject(new Error('WebSocket server unavailable'));
        }
      }, 8000);

      try {
        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
          if (connectionTimer) clearTimeout(connectionTimer);
          console.log('[Signaling] WebSocket Connected');
          this.isConnected = true;
          this.reconnectAttempts = 0;
          this.emit('connected');
          resolve();
        };

        this.ws.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            this.handleMessage(data);
          } catch (err) {
            console.error('[Signaling] WebSocket message parse error:', err);
          }
        };

        this.ws.onerror = (err) => {
          if (connectionTimer) clearTimeout(connectionTimer);
          if (!this.isConnected) {
            reject(err);
          }
          this.emit('error', err);
        };

        this.ws.onclose = () => {
          if (connectionTimer) clearTimeout(connectionTimer);
          console.warn('[Signaling] WebSocket Disconnected');
          this.isConnected = false;
          this.emit('disconnected');
        };
      } catch (err) {
        if (connectionTimer) clearTimeout(connectionTimer);
        reject(err);
      }
    });
  }

  attemptReconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 5000);
      console.log(`[Signaling] Reconnecting in ${delay}ms (Attempt ${this.reconnectAttempts})`);
      setTimeout(() => {
        this.connect().then(() => {
          if (this.roomCode && this.role) {
            if (this.role === 'host') {
              this.send({ type: 'create_room' });
            } else {
              this.send({ type: 'join_room', roomCode: this.roomCode });
            }
          }
        }).catch(err => console.error('[Signaling] Reconnect failed:', err));
      }, delay);
    }
  }

  handleMessage(data) {
    const { type, roomCode, role, payload, message } = data;

    switch (type) {
      case 'room_created':
        this.roomCode = roomCode;
        this.role = role;
        this.localIp = data.localIp;
        this.port = data.port;
        this.emit('room_created', { roomCode, role, localIp: data.localIp, port: data.port });
        break;

      case 'room_joined':
        this.roomCode = roomCode;
        this.role = role;
        this.emit('room_joined', { roomCode, role });
        break;

      case 'peer_joined':
        this.emit('peer_joined', { peerRole: data.peerRole });
        break;

      case 'peer_left':
        this.emit('peer_left', { role: data.role });
        break;

      case 'offer':
        this.emit('offer', payload);
        break;

      case 'answer':
        this.emit('answer', payload);
        break;

      case 'ice_candidate':
        this.emit('ice_candidate', payload);
        break;

      case 'error':
        this.emit('server_error', message);
        break;

      default:
        console.log('[Signaling] Unhandled message:', data);
    }
  }

  send(messageObj) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(messageObj));
    } else {
      console.warn('[Signaling] WebSocket is not open to send:', messageObj);
    }
  }

  createRoom() {
    this.send({ type: 'create_room' });
  }

  joinRoom(roomCode) {
    this.send({ type: 'join_room', roomCode });
  }

  sendOffer(offer) {
    this.send({ type: 'offer', payload: offer });
  }

  sendAnswer(answer) {
    this.send({ type: 'answer', payload: answer });
  }

  sendIceCandidate(candidate) {
    this.send({ type: 'ice_candidate', payload: candidate });
  }

  on(event, handler) {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, []);
    }
    this.listeners.get(event).push(handler);
  }

  emit(event, data) {
    if (this.listeners.has(event)) {
      this.listeners.get(event).forEach(handler => handler(data));
    }
  }
}

window.SignalingClient = SignalingClient;
