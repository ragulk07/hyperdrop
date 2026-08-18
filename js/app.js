document.addEventListener('DOMContentLoaded', () => {
  // Instantiate Core Services (Serverless + WebSocket Server Dual Engine)
  const signaling = window.SignalingClient ? new SignalingClient() : null;
  const p2p = new P2PManager(signaling);
  const transfer = new FileTransferManager(p2p);
  window.p2p = p2p;
  window.transfer = transfer;

  // DOM Elements
  const connectionBadge = document.getElementById('connectionBadge');
  const badgeText = document.getElementById('badgeText');
  
  // Tabs & Views
  const createTabBtn = document.getElementById('createTabBtn');
  const joinTabBtn = document.getElementById('joinTabBtn');
  const createRoomView = document.getElementById('createRoomView');
  const joinRoomView = document.getElementById('joinRoomView');

  // Room Actions
  const createRoomBtn = document.getElementById('createRoomBtn');
  const roomCodeDisplay = document.getElementById('roomCodeDisplay');
  const roomCodeValue = document.getElementById('roomCodeValue');
  const copyCodeBtn = document.getElementById('copyCodeBtn');
  const copyLinkBtn = document.getElementById('copyLinkBtn');
  const showQrBtn = document.getElementById('showQrBtn');
  const joinCodeInput = document.getElementById('joinCodeInput');
  const joinRoomBtn = document.getElementById('joinRoomBtn');

  // Connection Info
  const peerStatusVal = document.getElementById('peerStatusVal');
  const channelStatusVal = document.getElementById('channelStatusVal');
  const protocolVal = document.getElementById('protocolVal');

  // Dropzone & Queue
  const dropzone = document.getElementById('dropzone');
  const fileInput = document.getElementById('fileInput');
  const folderInput = document.getElementById('folderInput');
  const browseFilesBtn = document.getElementById('browseFilesBtn');
  const browseFolderBtn = document.getElementById('browseFolderBtn');
  const queueSection = document.getElementById('queueSection');
  const queueCount = document.getElementById('queueCount');
  const queueList = document.getElementById('queueList');
  const clearQueueBtn = document.getElementById('clearQueueBtn');
  const sendFilesBtn = document.getElementById('sendFilesBtn');

  // Dashboard & Progress
  const transferDashboard = document.getElementById('transferDashboard');
  const transferStatusBadge = document.getElementById('transferStatusBadge');
  const transferFileName = document.getElementById('transferFileName');
  const transferSizeText = document.getElementById('transferSizeText');
  const progressBarFill = document.getElementById('progressBarFill');
  const progressPercent = document.getElementById('progressPercent');
  const speedVal = document.getElementById('speedVal');
  const etaVal = document.getElementById('etaVal');
  const pauseTransferBtn = document.getElementById('pauseTransferBtn');
  const resumeTransferBtn = document.getElementById('resumeTransferBtn');
  const cancelTransferBtn = document.getElementById('cancelTransferBtn');

  // Received History
  const receivedCount = document.getElementById('receivedCount');
  const historyEmpty = document.getElementById('historyEmpty');
  const receivedList = document.getElementById('receivedList');

  // Modals
  const qrModal = document.getElementById('qrModal');
  const closeQrModal = document.getElementById('closeQrModal');
  const qrContainer = document.getElementById('qrContainer');
  const qrLinkText = document.getElementById('qrLinkText');

  const settingsModal = document.getElementById('settingsModal');
  const openSettingsBtn = document.getElementById('openSettingsBtn');
  const closeSettingsModal = document.getElementById('closeSettingsModal');
  const turnServerInput = document.getElementById('turnServerInput');
  const turnUserInput = document.getElementById('turnUserInput');
  const turnPassInput = document.getElementById('turnPassInput');
  const forceTurnCheck = document.getElementById('forceTurnCheck');
  const saveIceBtn = document.getElementById('saveIceBtn');
  const resetIceBtn = document.getElementById('resetIceBtn');

  const toastContainer = document.getElementById('toastContainer');

  // --- INITIALIZATION ---
  if (signaling) {
    signaling.connect().then(() => {
      updateConnectionBadge('connected', 'WebSocket Server Active');
      checkUrlForRoomCode();
    }).catch(() => {
      updateConnectionBadge('connected', 'HTTPS Signaling Active');
      checkUrlForRoomCode();
    });
  } else {
    updateConnectionBadge('connected', 'HTTPS Signaling Active');
    checkUrlForRoomCode();
  }

  // --- ROOM HANDSHAKE BINDINGS ---
  createTabBtn.addEventListener('click', () => {
    createTabBtn.classList.add('active');
    joinTabBtn.classList.remove('active');
    createRoomView.classList.add('active');
    joinRoomView.classList.remove('active');
  });

  joinTabBtn.addEventListener('click', () => {
    joinTabBtn.classList.add('active');
    createTabBtn.classList.remove('active');
    joinRoomView.classList.add('active');
    createRoomView.classList.remove('active');
  });

  createRoomBtn.addEventListener('click', () => {
    p2p.createRoom();
  });

  joinCodeInput.addEventListener('input', () => {
    joinCodeInput.value = joinCodeInput.value.toUpperCase();
  });

  joinCodeInput.addEventListener('keyup', (e) => {
    if (e.key === 'Enter') {
      triggerJoin();
    }
  });

  joinRoomBtn.addEventListener('click', () => {
    triggerJoin();
  });

  function triggerJoin() {
    const code = joinCodeInput.value.trim().toUpperCase();
    if (code.length === 6 || code.includes('ROOM=')) {
      peerStatusVal.textContent = `Connecting to room ${code}...`;
      updateConnectionBadge('connecting', 'Connecting...');
      p2p.joinRoom(code);
    } else {
      showToast('Please enter a valid 6-character room code', 'error');
    }
  }

  p2p.on('status_update', (msg) => {
    if (p2p.isChannelReady()) {
      peerStatusVal.textContent = 'Connected to Device';
      peerStatusVal.className = 'info-val text-success';
      return;
    }
    peerStatusVal.textContent = msg;
    if (msg.includes('Established') || msg.includes('Connected')) {
      peerStatusVal.className = 'info-val text-success';
    } else if (msg.includes('Error') || msg.includes('failed')) {
      peerStatusVal.className = 'info-val text-danger';
    } else {
      peerStatusVal.className = 'info-val text-warning';
    }
  });

  p2p.on('room_created', ({ roomCode }) => {
    roomCodeValue.textContent = roomCode;
    roomCodeDisplay.classList.remove('hidden');
    peerStatusVal.textContent = 'Waiting for peer device to join...';
    peerStatusVal.className = 'info-val text-warning';
    showToast(`Room created: ${roomCode}`, 'success');
  });

  p2p.on('room_joined', ({ roomCode }) => {
    peerStatusVal.textContent = `Room ${roomCode} code accepted. Connecting...`;
    peerStatusVal.className = 'info-val text-warning';
    showToast(`Connecting to room: ${roomCode}`, 'info');
  });

  p2p.on('peer_joined', () => {
    peerStatusVal.textContent = 'Peer joined! Establishing P2P DataChannel...';
    peerStatusVal.className = 'info-val text-warning';
    showToast('Pairing device connected!', 'success');
  });

  p2p.on('peer_left', () => {
    peerStatusVal.textContent = 'Peer disconnected.';
    peerStatusVal.className = 'info-val text-danger';
    channelStatusVal.textContent = 'Closed';
    channelStatusVal.className = 'info-val text-danger';
    updateConnectionBadge('disconnected', 'Peer Left');
    sendFilesBtn.disabled = true;
    showToast('Paired device left the room', 'error');
  });

  p2p.on('error', (msg) => {
    peerStatusVal.textContent = `Error: ${msg}`;
    peerStatusVal.className = 'info-val text-danger';
    updateConnectionBadge('disconnected', 'Connection Failed');
    showToast(msg, 'error');
  });

  // --- WEBRTC EVENT BINDINGS ---
  p2p.on('channel_open', () => {
    updateConnectionBadge('connected', 'P2P Connected');
    channelStatusVal.textContent = 'Active (End-to-End Encrypted)';
    channelStatusVal.className = 'info-val text-success';
    peerStatusVal.textContent = 'Connected to Device';
    peerStatusVal.className = 'info-val text-success';
    sendFilesBtn.disabled = false;
    showToast('Direct P2P Data Channel Ready!', 'success');
  });

  p2p.on('channel_close', () => {
    updateConnectionBadge('disconnected', 'Disconnected');
    channelStatusVal.textContent = 'Inactive';
    channelStatusVal.className = 'info-val text-danger';
    sendFilesBtn.disabled = true;
  });

  p2p.on('transport_type_detected', (type) => {
    protocolVal.textContent = type;
    if (type.includes('TURN')) {
      updateConnectionBadge('relayed', 'Relayed (TURN)');
    } else {
      updateConnectionBadge('connected', 'Direct P2P');
    }
  });

  // --- DRAG & DROP & FILE SELECTION ---
  browseFilesBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    fileInput.click();
  });

  browseFolderBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    folderInput.click();
  });

  fileInput.addEventListener('change', (e) => {
    if (e.target.files.length > 0) {
      transfer.addFilesToSendQueue(e.target.files);
      fileInput.value = '';
    }
  });

  folderInput.addEventListener('change', (e) => {
    if (e.target.files.length > 0) {
      transfer.addFilesToSendQueue(e.target.files);
      folderInput.value = '';
    }
  });

  dropzone.addEventListener('click', () => fileInput.click());

  ['dragenter', 'dragover'].forEach(eventName => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.add('dragover');
    });
  });

  ['dragleave', 'drop'].forEach(eventName => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.remove('dragover');
    });
  });

  dropzone.addEventListener('drop', (e) => {
    const files = e.dataTransfer.files;
    if (files.length > 0) {
      transfer.addFilesToSendQueue(files);
    }
  });

  // --- QUEUE RENDERING & ACTIONS ---
  transfer.on('status_toast', ({ type, message }) => {
    showToast(message, type);
  });

  transfer.on('queue_updated', (queue) => {
    queueCount.textContent = queue.length;
    if (queue.length > 0) {
      queueSection.classList.remove('hidden');
      renderQueue(queue);
    } else {
      queueSection.classList.add('hidden');
    }
  });

  function renderQueue(queue) {
    queueList.innerHTML = '';
    queue.forEach(item => {
      const el = document.createElement('div');
      el.className = 'queue-item';
      el.innerHTML = `
        <div class="item-main">
          <svg class="item-icon" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><polyline points="13 2 13 9 20 9"></polyline></svg>
          <span class="item-name">${escapeHtml(item.name)}</span>
        </div>
        <div class="item-meta">
          <span class="item-size">${formatBytes(item.size)}</span>
          <button class="icon-btn remove-item-btn" data-id="${item.id}" title="Remove">&times;</button>
        </div>
      `;
      queueList.appendChild(el);
    });

    queueList.querySelectorAll('.remove-item-btn').forEach(btn => {
      btn.addEventListener('click', (e) => {
        const id = e.target.getAttribute('data-id');
        transfer.removeQueueItem(id);
      });
    });
  }

  clearQueueBtn.addEventListener('click', () => {
    transfer.clearQueue();
  });

  sendFilesBtn.addEventListener('click', () => {
    if (!p2p.isChannelReady()) {
      showToast('P2P DataChannel is not connected yet!', 'error');
      return;
    }
    transfer.startSending();
  });

  // --- TRANSFER PROGRESS & DASHBOARD ---
  let dashboardRafId = null;
  let latestProgressArgs = null;

  function scheduleDashboardUpdate(filename, statusText, currentBytes, totalBytes, percent, speedBps, etaSec) {
    latestProgressArgs = [filename, statusText, currentBytes, totalBytes, percent, speedBps, etaSec];
    if (!dashboardRafId) {
      dashboardRafId = requestAnimationFrame(() => {
        dashboardRafId = null;
        if (latestProgressArgs) {
          showDashboard(...latestProgressArgs);
        }
      });
    }
  }

  transfer.on('send_progress', (data) => {
    scheduleDashboardUpdate(data.task.name, 'Sending...', data.bytesSent, data.totalBytes, data.percent, data.speedBps, data.etaSeconds);
  });

  transfer.on('receive_start', (task) => {
    scheduleDashboardUpdate(task.name, 'Receiving...', 0, task.size, 0, 0, 0);
  });

  transfer.on('receive_progress', (data) => {
    scheduleDashboardUpdate(data.task.name, 'Receiving...', data.bytesReceived, data.totalBytes, data.percent, data.speedBps, data.etaSeconds);
  });

  function showDashboard(filename, statusText, currentBytes, totalBytes, percent, speedBps, etaSec) {
    transferDashboard.classList.remove('hidden');
    transferStatusBadge.textContent = statusText;
    transferFileName.textContent = filename;
    transferSizeText.textContent = `${formatBytes(currentBytes)} / ${formatBytes(totalBytes)}`;
    progressBarFill.style.width = `${percent.toFixed(1)}%`;
    progressPercent.textContent = `${percent.toFixed(1)}%`;

    const speedMBs = (speedBps / (1024 * 1024)).toFixed(2);
    speedVal.textContent = `${speedMBs} MB/s`;

    if (etaSec > 0) {
      const hours = Math.floor(etaSec / 3600);
      const mins = Math.floor((etaSec % 3600) / 60);
      const secs = etaSec % 60;
      let etaStr = 'ETA: ';
      if (hours > 0) {
        etaStr += `${hours}h ${mins}m remaining`;
      } else if (mins > 0) {
        etaStr += `${mins}m ${secs}s remaining`;
      } else {
        etaStr += `${secs}s remaining`;
      }
      etaVal.textContent = etaStr;
    } else {
      etaVal.textContent = 'ETA: --';
    }
  }

  transfer.on('send_complete', (task) => {
    resetDashboardControls();
    transferDashboard.classList.add('hidden');
    showToast(`Successfully sent ${task.name}`, 'success');
  });

  transfer.on('receive_complete', (file) => {
    resetDashboardControls();
    transferDashboard.classList.add('hidden');
    addReceivedFileToList(file);
    showToast(`Received ${file.name}`, 'success');
  });

  transfer.on('transfer_status_change', ({ status }) => {
    transferStatusBadge.textContent = status;
  });

  transfer.on('transfer_cancelled', () => {
    resetDashboardControls();
    transferDashboard.classList.add('hidden');
    showToast('Transfer cancelled', 'error');
  });

  function resetDashboardControls() {
    pauseTransferBtn.classList.remove('hidden');
    resumeTransferBtn.classList.add('hidden');
  }

  pauseTransferBtn.addEventListener('click', () => {
    transfer.pauseTransfer();
    pauseTransferBtn.classList.add('hidden');
    resumeTransferBtn.classList.remove('hidden');
  });

  resumeTransferBtn.addEventListener('click', () => {
    transfer.resumeTransfer();
    resumeTransferBtn.classList.add('hidden');
    pauseTransferBtn.classList.remove('hidden');
  });

  cancelTransferBtn.addEventListener('click', () => {
    transfer.cancelTransfer();
  });

  // --- RECEIVED LIST RENDERING ---
  function addReceivedFileToList(file) {
    historyEmpty.classList.add('hidden');
    receivedCount.textContent = transfer.receivedFiles.length;

    const el = document.createElement('div');
    el.className = 'received-item';
    el.innerHTML = `
      <div class="received-main">
        <span class="received-name">${escapeHtml(file.name)}</span>
        <span class="received-meta">${formatBytes(file.size)} • ${file.time}</span>
      </div>
      <button class="btn btn-secondary btn-sm save-file-btn">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
        Save
      </button>
    `;

    const saveBtn = el.querySelector('.save-file-btn');
    saveBtn.addEventListener('click', () => {
      triggerDirectDownload(file.blob || file.url, file.name);
    });

    receivedList.prepend(el);
  }

  async function triggerDirectDownload(blobOrUrl, fileName) {
    let url = '';
    let shouldRevoke = false;

    if (blobOrUrl instanceof Blob) {
      url = URL.createObjectURL(blobOrUrl);
      shouldRevoke = true;
    } else if (typeof blobOrUrl === 'string') {
      url = blobOrUrl;
    }

    const a = document.createElement('a');
    a.style.display = 'none';
    a.href = url;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();

    setTimeout(() => {
      if (a.parentNode) {
        a.parentNode.removeChild(a);
      }
      if (shouldRevoke) {
        URL.revokeObjectURL(url);
      }
    }, 500);
  }

  // --- SHARE & QR CODE ---
  function getPairingUrl(roomCode) {
    return `${location.origin}${location.pathname}?room=${roomCode}`;
  }

  copyCodeBtn.addEventListener('click', () => {
    const code = roomCodeValue.textContent;
    if (code && code !== '------') {
      navigator.clipboard.writeText(code);
      showToast('Room code copied to clipboard!', 'success');
    }
  });

  copyLinkBtn.addEventListener('click', () => {
    const code = roomCodeValue.textContent;
    if (code && code !== '------') {
      const link = getPairingUrl(code);
      navigator.clipboard.writeText(link);
      showToast('Pairing link copied to clipboard!', 'success');
    }
  });

  showQrBtn.addEventListener('click', () => {
    const code = roomCodeValue.textContent;
    if (!code || code === '------') return;

    const link = getPairingUrl(code);
    qrContainer.innerHTML = '';
    qrLinkText.textContent = link;

    if (window.QRCode) {
      new QRCode(qrContainer, {
        text: link,
        width: 170,
        height: 170,
        colorDark: "#040914",
        colorLight: "#ffffff",
        correctLevel: QRCode.CorrectLevel.H
      });
    } else {
      qrContainer.textContent = 'QR Code Generator unavailable';
    }

    qrModal.classList.remove('hidden');
  });

  closeQrModal.addEventListener('click', () => qrModal.classList.add('hidden'));

  // --- SETTINGS MODAL ---
  openSettingsBtn.addEventListener('click', () => {
    const config = p2p.customIceConfig || {};
    turnServerInput.value = config.turnUrl || 'turn:openrelay.metered.ca:80';
    turnUserInput.value = config.username || 'openrelayproject';
    turnPassInput.value = config.credential || 'openrelayproject';
    forceTurnCheck.checked = !!config.forceTurn;
    settingsModal.classList.remove('hidden');
  });

  closeSettingsModal.addEventListener('click', () => settingsModal.classList.add('hidden'));

  saveIceBtn.addEventListener('click', () => {
    const config = {
      turnUrl: turnServerInput.value.trim(),
      username: turnUserInput.value.trim(),
      credential: turnPassInput.value.trim(),
      forceTurn: forceTurnCheck.checked
    };
    p2p.saveCustomIceConfig(config);
    settingsModal.classList.add('hidden');
    showToast('TURN Settings saved for next connection!', 'success');
  });

  resetIceBtn.addEventListener('click', () => {
    p2p.saveCustomIceConfig(null);
    turnServerInput.value = 'turn:openrelay.metered.ca:80';
    turnUserInput.value = 'openrelayproject';
    turnPassInput.value = 'openrelayproject';
    forceTurnCheck.checked = false;
    showToast('ICE configuration reset to default OpenRelay TURN servers', 'info');
  });

  // --- HELPER FUNCTIONS ---
  function updateConnectionBadge(state, text) {
    connectionBadge.className = `connection-badge ${state}`;
    badgeText.textContent = text;
  }

  function checkUrlForRoomCode() {
    const params = new URLSearchParams(window.location.search);
    const room = params.get('room');
    if (room && room.length === 6) {
      joinTabBtn.click();
      joinCodeInput.value = room.toUpperCase();
      setTimeout(() => p2p.joinRoom(room.toUpperCase()), 800);
    }
  }

  function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    toastContainer.appendChild(toast);
    setTimeout(() => toast.remove(), 4000);
  }

  function formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0.00 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    const val = bytes / Math.pow(k, i);
    return (Math.round(val * 100) / 100).toFixed(2) + ' ' + sizes[i];
  }

  function escapeHtml(str) {
    return str.replace(/[&<>"']/g, (m) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;'
    })[m]);
  }
});
