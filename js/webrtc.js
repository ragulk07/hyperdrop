class P2PManager {
  constructor(signalingClient = null) {
    this.signaling = signalingClient;
    this.pc = null;
    this.dataChannel = null;
    this.eventSource = null;
    this.pollInterval = null;
    this.joinRetryInterval = null;
    this.lastMsgTime = 0;
    this.lastSignalTimestamp = 0;
    this.processedMsgIds = new Set();
    this.remoteIceCandidates = [];
    this.pendingLocalIceCandidates = [];
    this.iceDebounceTimer = null;
    this.roomCode = null;
    this.role = null; // 'host' or 'joiner'
    this.clientId = 'client_' + Math.random().toString(36).substr(2, 8);
    this.listeners = new Map();
    this.transportType = 'Direct P2P';
    this.customIceConfig = null;

    this.loadCustomIceConfig();
    this.setupMobileVisibilityListener();

    if (this.signaling) {
      this.bindSignalingEvents();
    }
  }

  setupMobileVisibilityListener() {
    document.addEventListener('visibilitychange', () => {
      if (!document.hidden && this.roomCode) {
        console.log('[WebRTC Mobile] Tab became visible, triggering immediate signal check...');
        this.pollSignalsOnce();
      }
    });
  }

  bindSignalingEvents() {
    this.signaling.on('room_created', ({ roomCode, role }) => {
      this.roomCode = roomCode;
      this.role = role;
      this.emit('room_created', { roomCode, role });
    });

    this.signaling.on('room_joined', ({ roomCode, role }) => {
      this.roomCode = roomCode;
      this.role = role;
      this.emit('room_joined', { roomCode, role });
    });

    this.signaling.on('peer_joined', () => {
      this.emit('peer_joined', { peerRole: 'joiner' });
      if (this.role === 'host') {
        this.initPeerConnection(true);
      }
    });

    this.signaling.on('offer', async (offer) => {
      if (this.role === 'joiner') {
        this.emit('status_update', 'WebRTC OFFER received. Creating ANSWER...');
        await this.initPeerConnection(false);
        await this.pc.setRemoteDescription(new RTCSessionDescription(offer));
        await this.drainRemoteIceCandidates();

        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);
        this.signaling.sendAnswer(answer);
        this.emit('status_update', 'WebRTC ANSWER sent. Negotiating P2P routes...');
      }
    });

    this.signaling.on('answer', async (answer) => {
      if (this.role === 'host' && this.pc) {
        this.emit('status_update', 'WebRTC ANSWER received. Finalizing P2P routes...');
        await this.pc.setRemoteDescription(new RTCSessionDescription(answer));
        await this.drainRemoteIceCandidates();
      }
    });

    this.signaling.on('ice_candidate', async (candidate) => {
      if (candidate) {
        if (this.pc && this.pc.remoteDescription && this.pc.remoteDescription.type) {
          try {
            await this.pc.addIceCandidate(candidate);
          } catch (e) {
            console.warn('[WebRTC] addIceCandidate error:', e);
          }
        } else {
          this.remoteIceCandidates.push(candidate);
        }
      }
    });

    this.signaling.on('peer_left', () => {
      this.emit('peer_left');
      this.close();
    });

    this.signaling.on('server_error', (msg) => {
      this.emit('error', msg);
    });
  }

  loadCustomIceConfig() {
    try {
      const saved = localStorage.getItem('hyperdrop_ice_config');
      if (saved) {
        this.customIceConfig = JSON.parse(saved);
      }
    } catch (e) {
      console.warn('[WebRTC] Failed to load custom ICE config:', e);
    }
  }

  saveCustomIceConfig(config) {
    this.customIceConfig = config;
    try {
      localStorage.setItem('hyperdrop_ice_config', JSON.stringify(config));
    } catch (e) {
      console.warn('[WebRTC] Failed to save custom ICE config:', e);
    }
  }

  getIceServers() {
    const defaultStunServers = [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
      { urls: 'stun:stun2.l.google.com:19302' },
      { urls: 'stun:stun3.l.google.com:19302' },
      { urls: 'stun:stun4.l.google.com:19302' },
      { urls: 'stun:global.stun.twilio.com:3478' },
      { urls: 'stun:stun.services.mozilla.com' }
    ];

    const defaultTurnServers = [
      {
        urls: 'turn:openrelay.metered.ca:80',
        username: 'openrelayproject',
        credential: 'openrelayproject'
      },
      {
        urls: 'turn:openrelay.metered.ca:443',
        username: 'openrelayproject',
        credential: 'openrelayproject'
      },
      {
        urls: 'turn:openrelay.metered.ca:443?transport=tcp',
        username: 'openrelayproject',
        credential: 'openrelayproject'
      },
      {
        urls: 'turns:openrelay.metered.ca:443?transport=tcp',
        username: 'openrelayproject',
        credential: 'openrelayproject'
      }
    ];

    if (this.customIceConfig && this.customIceConfig.turnUrl) {
      const customTurnServer = {
        urls: this.customIceConfig.turnUrl,
        username: this.customIceConfig.username || undefined,
        credential: this.customIceConfig.credential || undefined
      };
      return [...defaultStunServers, customTurnServer, ...defaultTurnServers];
    }

    return [...defaultStunServers, ...defaultTurnServers];
  }

  generateRoomCode() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let code = '';
    for (let i = 0; i < 6; i++) {
      code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
  }

  async pollSignalsOnce() {
    if (!this.roomCode) return;

    const parseAndDispatch = (payload) => {
      if (payload && payload.senderId !== this.clientId) {
        const msgId = payload.id || payload.msgId || (payload.type + '_' + payload.senderId);
        if (!this.processedMsgIds.has(msgId)) {
          this.processedMsgIds.add(msgId);
          if (this.processedMsgIds.size > 200) {
            const firstItem = this.processedMsgIds.values().next().value;
            this.processedMsgIds.delete(firstItem);
          }
          this.handleSignalingMessage(payload);
        }
      }
    };

    // 1. Same-Domain Vercel Serverless API
    try {
      const res = await fetch(`/api/signal?room=${this.roomCode}&since=${this.lastSignalTimestamp}`);
      const contentType = res.headers.get('content-type') || '';
      if (res.ok && contentType.includes('application/json')) {
        const data = await res.json();
        if (Array.isArray(data.signals)) {
          for (const sig of data.signals) {
            if (sig.timestamp > this.lastSignalTimestamp) {
              this.lastSignalTimestamp = sig.timestamp;
            }
            parseAndDispatch(sig);
          }
        }
      }
    } catch (e) {}

    // 2. Simultaneous Public Relay Backup (For Cross-Region Mobile Connections)
    try {
      const ntfyUrl = `https://ntfy.sh/hyperdrop-v6-${this.roomCode}/json?poll=1&since=10m`;
      const ntfyRes = await fetch(ntfyUrl);
      if (ntfyRes.ok) {
        const text = await ntfyRes.text();
        const lines = text.split('\n');
        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const raw = JSON.parse(line);
            if (raw.event === 'message' && raw.message) {
              const payload = typeof raw.message === 'string' ? JSON.parse(raw.message) : raw.message;
              parseAndDispatch(payload);
            }
          } catch (e) {}
        }
      }
    } catch (e) {}
  }

  connectSignalStream(onReady) {
    if (!this.roomCode) return;

    if (this.eventSource) this.eventSource.close();
    if (this.pollInterval) clearInterval(this.pollInterval);

    this.lastSignalTimestamp = 0;

    this.pollInterval = setInterval(() => {
      this.pollSignalsOnce();
    }, 1000);

    // Initial immediate poll
    this.pollSignalsOnce();

    if (onReady) onReady();
  }

  async sendSignal(msg) {
    if (this.signaling && this.signaling.isConnected) {
      if (msg.type === 'OFFER') this.signaling.sendOffer(msg.offer);
      else if (msg.type === 'ANSWER') this.signaling.sendAnswer(msg.answer);
      else if (msg.type === 'ICE_CANDIDATE') this.signaling.sendIceCandidate(msg.candidate);
      else if (msg.type === 'ICE_CANDIDATES' && Array.isArray(msg.candidates)) {
        msg.candidates.forEach(c => this.signaling.sendIceCandidate(c));
      }
      return;
    }

    if (!this.roomCode) return;
    msg.senderId = this.clientId;
    msg.msgId = 'msg_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);

    // Send to same-domain Vercel API
    try {
      await fetch('/api/signal', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ room: this.roomCode, signal: msg })
      });
    } catch (e) {}

    // Also send to Public Relay Backup
    try {
      const topic = `hyperdrop-v6-${this.roomCode}`;
      const payloadString = JSON.stringify(msg);
      await fetch(`https://ntfy.sh/${topic}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: payloadString })
      });
    } catch (e) {}
  }

  queueLocalIceCandidate(candidate) {
    if (!candidate || !candidate.candidate) return;

    if (!this.pendingLocalIceCandidates) {
      this.pendingLocalIceCandidates = [];
    }
    this.pendingLocalIceCandidates.push(candidate);

    if (!this.iceDebounceTimer) {
      this.iceDebounceTimer = setTimeout(() => {
        this.flushLocalIceCandidates();
      }, 200);
    }
  }

  flushLocalIceCandidates() {
    if (this.iceDebounceTimer) {
      clearTimeout(this.iceDebounceTimer);
      this.iceDebounceTimer = null;
    }
    if (this.pendingLocalIceCandidates && this.pendingLocalIceCandidates.length > 0) {
      const candidates = [...this.pendingLocalIceCandidates];
      this.pendingLocalIceCandidates = [];
      console.log('[WebRTC Mobile] Flushing batched ICE candidates count:', candidates.length);
      this.sendSignal({ type: 'ICE_CANDIDATES', candidates });
    }
  }

  createRoom() {
    this.close();
    if (this.signaling && this.signaling.isConnected) {
      this.signaling.createRoom();
    } else {
      const code = this.generateRoomCode();
      this.roomCode = code;
      this.role = 'host';
      this.connectSignalStream(() => {
        console.log('[WebRTC Host] Room Created:', code);
        this.emit('room_created', { roomCode: code, role: 'host' });
      });
    }
  }

  joinRoom(roomCode) {
    this.close();
    let code = (roomCode || '').toUpperCase().trim();

    if (code.includes('ROOM=')) {
      try {
        const urlParams = new URLSearchParams(code.split('?')[1] || code);
        code = (urlParams.get('room') || code).toUpperCase().trim();
      } catch (e) {}
    }

    if (!code || code.length !== 6) {
      this.emit('error', 'Please enter a valid 6-character room code.');
      return;
    }

    this.roomCode = code;
    this.role = 'joiner';

    if (this.signaling && this.signaling.isConnected) {
      this.signaling.joinRoom(code);
    } else {
      this.emit('status_update', `Joining room ${code}...`);
      this.connectSignalStream(async () => {
        console.log('[WebRTC Joiner] Joining Room:', code);
        this.emit('room_joined', { roomCode: code, role: 'joiner' });
        this.emit('status_update', 'Sending JOIN announcement to Host...');
        await this.sendSignal({ type: 'JOIN' });

        // Start JOIN retry loop every 1.5s until OFFER is received
        if (this.joinRetryInterval) clearInterval(this.joinRetryInterval);
        this.joinRetryInterval = setInterval(async () => {
          if (this.role === 'joiner' && !this.pc) {
            console.log('[WebRTC Mobile] Retrying JOIN announcement...');
            await this.sendSignal({ type: 'JOIN' });
          } else {
            clearInterval(this.joinRetryInterval);
            this.joinRetryInterval = null;
          }
        }, 1500);
      });
    }
  }

  async handleSignalingMessage(msg) {
    console.log('[WebRTC Mobile] Received signal message:', msg.type);

    switch (msg.type) {
      case 'JOIN': {
        if (this.role === 'host') {
          console.log('[WebRTC Host] Received JOIN request from peer');
          this.emit('peer_joined', { peerRole: 'joiner' });
          this.emit('status_update', 'Peer joined. Initiating WebRTC OFFER...');
          await this.initPeerConnection(true);
        }
        break;
      }

      case 'OFFER': {
        if (this.role === 'joiner') {
          console.log('[WebRTC Joiner] Received OFFER from host');
          if (this.joinRetryInterval) {
            clearInterval(this.joinRetryInterval);
            this.joinRetryInterval = null;
          }
          this.emit('status_update', 'WebRTC OFFER received. Creating ANSWER...');
          await this.initPeerConnection(false);
          await this.pc.setRemoteDescription(new RTCSessionDescription(msg.offer));
          await this.drainRemoteIceCandidates();

          const answer = await this.pc.createAnswer();
          await this.pc.setLocalDescription(answer);
          await this.sendSignal({ type: 'ANSWER', answer });
          this.emit('status_update', 'WebRTC ANSWER sent. Negotiating P2P routes...');
        }
        break;
      }

      case 'ANSWER': {
        if (this.role === 'host' && this.pc) {
          console.log('[WebRTC Host] Received ANSWER from joiner');
          this.emit('status_update', 'WebRTC ANSWER received. Finalizing P2P routes...');
          await this.pc.setRemoteDescription(new RTCSessionDescription(msg.answer));
          await this.drainRemoteIceCandidates();
        }
        break;
      }

      case 'ICE_CANDIDATES': {
        if (Array.isArray(msg.candidates)) {
          console.log('[WebRTC Mobile] Received batched ICE candidates count:', msg.candidates.length);
          for (const candidate of msg.candidates) {
            if (candidate && candidate.candidate) {
              if (this.pc && this.pc.remoteDescription && this.pc.remoteDescription.type) {
                try {
                  await this.pc.addIceCandidate(new RTCIceCandidate(candidate));
                } catch (e) {
                  console.warn('[WebRTC Mobile] Add ICE candidate error:', e);
                }
              } else {
                this.remoteIceCandidates.push(candidate);
              }
            }
          }
          await this.drainRemoteIceCandidates();
        }
        break;
      }
    }
  }

  async drainRemoteIceCandidates() {
    if (!this.pc || !this.pc.remoteDescription) return;
    while (this.remoteIceCandidates.length > 0) {
      const candidate = this.remoteIceCandidates.shift();
      if (candidate) {
        try {
          await this.pc.addIceCandidate(candidate);
        } catch (e) {
          console.warn('[WebRTC Mobile] Draining ICE candidate error:', e);
        }
      }
    }
  }

  async initPeerConnection(isInitiator) {
    if (this.pc && (this.pc.connectionState === 'connecting' || this.pc.connectionState === 'connected')) {
      console.log('[WebRTC Mobile] PeerConnection already active, skipping re-init');
      return;
    }

    console.log('[WebRTC Mobile] Initializing RTCPeerConnection (Initiator:', isInitiator, ')');

    const config = {
      iceServers: this.getIceServers(),
      iceCandidatePoolSize: 10,
      iceTransportPolicy: (this.customIceConfig && this.customIceConfig.forceTurn) ? 'relay' : 'all',
      bundlePolicy: 'max-bundle',
      rtcpMuxPolicy: 'require',
      sdpSemantics: 'unified-plan'
    };

    if (this.pc) {
      try { this.pc.close(); } catch (e) {}
    }

    this.pc = new RTCPeerConnection(config);
    this.remoteIceCandidates = [];
    this.pendingLocalIceCandidates = [];

    this.pc.onicecandidate = (event) => {
      if (event.candidate && event.candidate.candidate) {
        if (this.signaling && this.signaling.isConnected) {
          this.signaling.sendIceCandidate(event.candidate);
        } else {
          this.queueLocalIceCandidate(event.candidate);
        }
      } else if (!event.candidate) {
        if (!this.signaling || !this.signaling.isConnected) {
          this.flushLocalIceCandidates();
        }
      }
    };

    this.pc.oniceconnectionstatechange = () => {
      console.log('[WebRTC Mobile] ICE Connection State:', this.pc.iceConnectionState);
      if (this.pc.iceConnectionState === 'checking') {
        this.emit('status_update', 'Exchanging ICE candidates (STUN/TURN)...');
      } else if (this.pc.iceConnectionState === 'connected' || this.pc.iceConnectionState === 'completed') {
        this.emit('status_update', 'P2P Route Established! Opening DataChannel...');
      } else if (this.pc.iceConnectionState === 'failed') {
        console.warn('[WebRTC Mobile] Direct STUN route failed, restarting ICE with TURN Relay...');
        this.emit('status_update', 'Retrying with TURN Relay fallback...');
        if (this.pc.restartIce) {
          this.pc.restartIce();
        }
      }
    };

    this.pc.onconnectionstatechange = () => {
      console.log('[WebRTC Mobile] PeerConnection State:', this.pc.connectionState);
      if (this.pc.connectionState === 'disconnected' || this.pc.connectionState === 'failed') {
        this.emit('channel_close');
        this.emit('peer_left');
      }
    };

    if (isInitiator) {
      this.dataChannels = [];
      const dcCount = 8;
      for (let i = 0; i < dcCount; i++) {
        const dc = this.pc.createDataChannel(`fileTransfer_${i}`, { ordered: true });
        this.setupDataChannel(dc, i);
      }

      const offer = await this.pc.createOffer();
      await this.pc.setLocalDescription(offer);
      await this.sendSignal({ type: 'OFFER', offer });
    } else {
      this.dataChannels = [];
      this.pc.ondatachannel = (event) => {
        console.log('[WebRTC Mobile] DataChannel received by Joiner:', event.channel.label);
        this.setupDataChannel(event.channel, this.dataChannels.length);
      };
    }
  }

  setupDataChannel(channel, index = 0) {
    channel.binaryType = 'arraybuffer';
    channel.bufferedAmountLowThreshold = 256 * 1024; // 256KB threshold for instant buffer recovery
    this.dataChannels[index] = channel;
    if (index === 0) this.dataChannel = channel;

    const handleOpen = () => {
      console.log(`[WebRTC Mobile] DataChannel ${channel.label} OPEN & READY!`);
      if (index === 0) {
        this.checkTransportType();
        this.emit('channel_open');
      }
    };

    if (channel.readyState === 'open') {
      handleOpen();
    } else {
      channel.onopen = handleOpen;
    }

    channel.onmessage = (event) => {
      this.emit('data_received', event.data);
    };

    channel.onbufferedamountlow = () => {
      this.emit('buffered_amount_low', { channelIndex: index });
    };

    channel.onclose = () => {
      console.warn(`[WebRTC Mobile] DataChannel ${channel.label} Closed`);
      if (index === 0) {
        this.emit('channel_close');
        this.emit('peer_left');
      }
    };

    channel.onerror = (err) => {
      console.error(`[WebRTC Mobile] DataChannel ${channel.label} Error:`, err);
      this.emit('channel_error', err);
    };
  }

  async checkTransportType() {
    if (!this.pc) return;
    try {
      const stats = await this.pc.getStats();
      stats.forEach(report => {
        if (report.type === 'candidate-pair' && report.state === 'succeeded') {
          const localCandidate = stats.get(report.localCandidateId);
          const remoteCandidate = stats.get(report.remoteCandidateId);
          
          if (localCandidate && remoteCandidate) {
            const isRelayed = localCandidate.candidateType === 'relay' || remoteCandidate.candidateType === 'relay';
            this.transportType = isRelayed ? 'TURN Relay' : 'Direct P2P (LAN/WAN)';
            console.log('[WebRTC Mobile] Transport type detected:', this.transportType);
            this.emit('transport_type_detected', this.transportType);
          }
        }
      });
    } catch (e) {
      console.warn('[WebRTC Mobile] Failed to get candidate pair stats:', e);
    }
  }

  sendData(data, channelIndex = 0) {
    const targetChannel = (this.dataChannels && this.dataChannels[channelIndex]) ? this.dataChannels[channelIndex] : this.dataChannel;
    if (targetChannel && targetChannel.readyState === 'open') {
      targetChannel.send(data);
      return true;
    }
    return false;
  }

  getBufferedAmount(channelIndex = 0) {
    const targetChannel = (this.dataChannels && this.dataChannels[channelIndex]) ? this.dataChannels[channelIndex] : this.dataChannel;
    return targetChannel ? targetChannel.bufferedAmount : 0;
  }

  getTotalBufferedAmount() {
    if (!this.dataChannels || this.dataChannels.length === 0) return this.getBufferedAmount(0);
    return this.dataChannels.reduce((sum, ch) => sum + (ch ? ch.bufferedAmount : 0), 0);
  }

  isChannelReady() {
    return (this.dataChannel && this.dataChannel.readyState === 'open') ||
           (this.dataChannels && this.dataChannels.some(ch => ch && ch.readyState === 'open'));
  }

  close() {
    if (this.dataChannel) {
      this.dataChannel.close();
      this.dataChannel = null;
    }
    if (this.pc) {
      this.pc.close();
      this.pc = null;
    }
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }
    if (this.joinRetryInterval) {
      clearInterval(this.joinRetryInterval);
      this.joinRetryInterval = null;
    }
    this.processedMsgIds.clear();
    this.remoteIceCandidates = [];
    this.pendingLocalIceCandidates = [];
    if (this.iceDebounceTimer) {
      clearTimeout(this.iceDebounceTimer);
      this.iceDebounceTimer = null;
    }
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

window.P2PManager = P2PManager;
