class FileTransferManager {
  constructor(p2pManager) {
    this.p2p = p2pManager;
    this.DEFAULT_CHUNK_SIZE = 256 * 1024; // 256KB per chunk for high throughput WebRTC SCTP
    this.MAX_BUFFER_PER_CHANNEL = 1024 * 1024; // 1MB backpressure threshold per channel
    this.MAX_PREFETCH_CHUNKS = 64; // Up to 16MB ring buffer pre-fetched into memory (Motrix style)

    this.sendQueue = [];
    this.currentSendTask = null;
    this.currentReceiveTask = null;
    this.receivedFiles = [];
    this.listeners = new Map();

    // Multi-channel round robin & prefetching
    this.currentChannelIndex = 0;
    this.prefetchMap = new Map(); // chunkIndex -> ArrayBuffer packet
    this.nextReadChunkIndex = 0;
    this.isPrefetching = false;

    // Speed tracking
    this.speedTimer = null;
    this.bytesInWindow = 0;
    this.lastSpeedCheck = Date.now();
    this.currentSpeedBps = 0;

    this.bindP2PEvents();
  }

  bindP2PEvents() {
    this.p2p.on('data_received', (data) => {
      if (typeof data === 'string') {
        try {
          const message = JSON.parse(data);
          this.handleControlMessage(message);
        } catch (e) {
          console.error('[FileTransfer] JSON parse error:', e);
        }
      } else if (data instanceof ArrayBuffer) {
        this.handleBinaryChunk(data);
      }
    });

    this.p2p.on('buffered_amount_low', () => {
      if (this.currentSendTask) {
        this.currentSendTask.isWaitingForBuffer = false;
        this.pumpSendQueue();
      }
    });

    this.p2p.on('channel_close', () => {
      if (this.currentSendTask) {
        this.currentSendTask.isPaused = true;
        this.emit('transfer_paused', { id: this.currentSendTask.id, reason: 'Disconnected' });
      }
    });
  }

  // --- SENDER SIDE ---

  async addFilesToSendQueue(fileList) {
    const items = Array.from(fileList);
    
    // Check if it's a folder drop or multi-file selection with webkitRelativePath
    const hasFolderStructure = items.some(f => f.webkitRelativePath && f.webkitRelativePath.includes('/'));

    if (hasFolderStructure && window.JSZip) {
      this.emit('status_toast', { type: 'info', message: 'Zipping folder client-side...' });
      const folderName = items[0].webkitRelativePath.split('/')[0] || 'folder';
      const zip = new JSZip();

      for (const file of items) {
        zip.file(file.webkitRelativePath, file);
      }

      const zipBlob = await zip.generateAsync({ type: 'blob' }, (metadata) => {
        this.emit('zip_progress', metadata.percent.toFixed(1));
      });

      const zipFile = new File([zipBlob], `${folderName}.zip`, { type: 'application/zip' });
      this.enqueueFile(zipFile);
    } else {
      for (const file of items) {
        this.enqueueFile(file);
      }
    }
  }

  enqueueFile(file) {
    const id = 'ft_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
    const chunkSize = file.size > 10 * 1024 * 1024 ? 256 * 1024 : 128 * 1024;
    const totalChunks = Math.ceil(file.size / chunkSize);
    
    const task = {
      id,
      file,
      name: file.name,
      size: file.size,
      type: file.type || 'application/octet-stream',
      chunkSize,
      totalChunks,
      currentChunkIndex: 0,
      isPaused: false,
      isCancelled: false,
      isWaitingForBuffer: false
    };

    this.sendQueue.push(task);
    this.emit('queue_updated', this.sendQueue);
  }

  clearQueue() {
    this.sendQueue = [];
    this.emit('queue_updated', this.sendQueue);
  }

  removeQueueItem(id) {
    this.sendQueue = this.sendQueue.filter(t => t.id !== id);
    this.emit('queue_updated', this.sendQueue);
  }

  startSending() {
    if (this.currentSendTask || this.sendQueue.length === 0) return;
    this.currentSendTask = this.sendQueue.shift();
    this.emit('queue_updated', this.sendQueue);

    console.log('[FileTransfer] Starting send task with Motrix pipeline engine:', this.currentSendTask.name);

    // Reset prefetch queue and read pointers
    this.prefetchMap.clear();
    this.nextReadChunkIndex = 0;
    this.isPrefetching = false;

    // Send FILE_START header JSON
    const header = {
      type: 'FILE_START',
      id: this.currentSendTask.id,
      name: this.currentSendTask.name,
      size: this.currentSendTask.size,
      totalChunks: this.currentSendTask.totalChunks,
      chunkSize: this.currentSendTask.chunkSize,
      fileType: this.currentSendTask.type
    };

    this.p2p.sendData(JSON.stringify(header));
    this.startSpeedTracker();

    // Start background prefetching and initial network pump
    this.fillPrefetchQueue();
    this.pumpSendQueue();
  }

  async fillPrefetchQueue() {
    const task = this.currentSendTask;
    if (!task || task.isPaused || task.isCancelled) return;
    if (this.isPrefetching) return;
    this.isPrefetching = true;

    try {
      while (
        task === this.currentSendTask &&
        !task.isPaused &&
        !task.isCancelled &&
        this.nextReadChunkIndex < task.totalChunks &&
        this.prefetchMap.size < this.MAX_PREFETCH_CHUNKS
      ) {
        const chunkIndex = this.nextReadChunkIndex++;
        const start = chunkIndex * task.chunkSize;
        const end = Math.min(start + task.chunkSize, task.size);
        const slice = task.file.slice(start, end);

        let rawBuffer;
        if (slice.arrayBuffer) {
          rawBuffer = await slice.arrayBuffer();
        } else {
          rawBuffer = await this.readBlobAsArrayBuffer(slice);
        }

        if (!this.currentSendTask || this.currentSendTask.id !== task.id || task.isPaused || task.isCancelled) {
          break;
        }

        // Pack packet: [4 bytes uint32 index] + [payload]
        const packet = new Uint8Array(4 + rawBuffer.byteLength);
        const indexView = new DataView(packet.buffer);
        indexView.setUint32(0, chunkIndex, false); // Big endian
        packet.set(new Uint8Array(rawBuffer), 4);

        this.prefetchMap.set(chunkIndex, packet.buffer);
      }
    } catch (err) {
      console.error('[FileTransfer] Prefetch read error:', err);
    } finally {
      this.isPrefetching = false;
      // Trigger pump to send any newly pre-fetched chunks immediately
      this.pumpSendQueue();
    }
  }

  pumpSendQueue() {
    const task = this.currentSendTask;
    if (!task || task.isPaused || task.isCancelled) return;

    if (task.currentChunkIndex >= task.totalChunks) {
      // Completed sending all chunks
      console.log('[FileTransfer] Finished sending file:', task.name);
      this.p2p.sendData(JSON.stringify({ type: 'FILE_END', id: task.id }), 0);
      this.emit('send_complete', task);
      this.currentSendTask = null;
      this.prefetchMap.clear();
      this.stopSpeedTracker();
      
      // Auto process next in queue
      if (this.sendQueue.length > 0) {
        setTimeout(() => this.startSending(), 100);
      }
      return;
    }

    const numChannels = (this.p2p.dataChannels && this.p2p.dataChannels.length > 0) ? this.p2p.dataChannels.length : 1;

    while (task.currentChunkIndex < task.totalChunks) {
      const chunkIndex = task.currentChunkIndex;
      const packetBuffer = this.prefetchMap.get(chunkIndex);

      if (!packetBuffer) {
        // Chunk not pre-fetched yet; trigger background prefetch
        task.isWaitingForBuffer = true;
        this.fillPrefetchQueue();
        break;
      }

      // Find next open DataChannel with buffer below threshold
      let targetChannelIdx = -1;
      for (let i = 0; i < numChannels; i++) {
        const idx = (this.currentChannelIndex + i) % numChannels;
        if (this.p2p.getBufferedAmount(idx) < this.MAX_BUFFER_PER_CHANNEL) {
          targetChannelIdx = idx;
          this.currentChannelIndex = (idx + 1) % numChannels;
          break;
        }
      }

      if (targetChannelIdx === -1) {
        // All data channels full, backpressure waiting
        task.isWaitingForBuffer = true;
        break;
      }

      const sent = this.p2p.sendData(packetBuffer, targetChannelIdx);
      if (sent) {
        this.prefetchMap.delete(chunkIndex);
        task.currentChunkIndex++;

        const payloadSize = packetBuffer.byteLength - 4;
        this.bytesInWindow += payloadSize;

        const bytesSent = Math.min(task.currentChunkIndex * task.chunkSize, task.size);
        const percent = Math.min(100, (bytesSent / task.size) * 100);
        
        this.emit('send_progress', {
          task,
          bytesSent,
          totalBytes: task.size,
          percent,
          speedBps: this.currentSpeedBps,
          etaSeconds: this.calculateETA(task.size - bytesSent)
        });
      } else {
        task.isWaitingForBuffer = true;
        break;
      }
    }

    // Keep prefetch queue filled
    this.fillPrefetchQueue();
  }

  // Backward compatibility alias for readAndSendNextChunk
  readAndSendNextChunk() {
    this.pumpSendQueue();
  }

  readBlobAsArrayBuffer(blob) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = (e) => resolve(e.target.result);
      reader.onerror = (err) => reject(err);
      reader.readAsArrayBuffer(blob);
    });
  }

  pauseTransfer() {
    if (this.currentSendTask) {
      this.currentSendTask.isPaused = true;
      this.p2p.sendData(JSON.stringify({ type: 'TRANSFER_PAUSE', id: this.currentSendTask.id }));
      this.stopSpeedTracker();
      this.emit('transfer_status_change', { status: 'Paused' });
    } else if (this.currentReceiveTask) {
      this.p2p.sendData(JSON.stringify({ type: 'TRANSFER_PAUSE', id: this.currentReceiveTask.id }));
      this.stopSpeedTracker();
      this.emit('transfer_status_change', { status: 'Paused' });
    }
  }

  resumeTransfer() {
    if (this.currentSendTask && this.currentSendTask.isPaused) {
      this.currentSendTask.isPaused = false;
      this.p2p.sendData(JSON.stringify({
        type: 'TRANSFER_RESUME',
        id: this.currentSendTask.id
      }));
      this.startSpeedTracker();
      this.emit('transfer_status_change', { status: 'Transferring...' });
      this.fillPrefetchQueue();
      this.pumpSendQueue();
    } else if (this.currentReceiveTask) {
      this.p2p.sendData(JSON.stringify({ type: 'TRANSFER_RESUME', id: this.currentReceiveTask.id }));
      this.startSpeedTracker();
      this.emit('transfer_status_change', { status: 'Transferring...' });
    }
  }

  cancelTransfer() {
    if (this.currentSendTask) {
      this.currentSendTask.isCancelled = true;
      this.p2p.sendData(JSON.stringify({ type: 'TRANSFER_CANCEL', id: this.currentSendTask.id }));
      const cancelledTask = this.currentSendTask;
      this.currentSendTask = null;
      this.prefetchMap.clear();
      this.stopSpeedTracker();
      this.emit('transfer_cancelled', cancelledTask);
    } else if (this.currentReceiveTask) {
      this.p2p.sendData(JSON.stringify({ type: 'TRANSFER_CANCEL', id: this.currentReceiveTask.id }));
      const cancelledTask = this.currentReceiveTask;
      this.currentReceiveTask = null;
      this.stopSpeedTracker();
      this.emit('transfer_cancelled', cancelledTask);
    }
  }

  // --- RECEIVER SIDE ---

  handleControlMessage(msg) {
    switch (msg.type) {
      case 'FILE_START': {
        console.log('[FileTransfer] Receiving file start:', msg.name);
        this.currentReceiveTask = {
          id: msg.id,
          name: msg.name,
          size: msg.size,
          totalChunks: msg.totalChunks,
          chunkSize: msg.chunkSize || (128 * 1024),
          fileType: msg.fileType,
          chunks: new Array(msg.totalChunks),
          receivedChunkCount: 0,
          receivedBytes: 0,
          startTime: Date.now()
        };
        this.startSpeedTracker();
        this.emit('receive_start', this.currentReceiveTask);
        break;
      }

      case 'FILE_END': {
        if (this.currentReceiveTask && this.currentReceiveTask.id === msg.id) {
          console.log('[FileTransfer] Receive complete:', this.currentReceiveTask.name);
          this.finishReceiveFile(this.currentReceiveTask);
        }
        break;
      }

      case 'TRANSFER_PAUSE': {
        this.stopSpeedTracker();
        if (this.currentSendTask) {
          this.currentSendTask.isPaused = true;
        }
        this.emit('transfer_status_change', { status: 'Paused' });
        break;
      }

      case 'TRANSFER_RESUME': {
        this.startSpeedTracker();
        if (this.currentSendTask && this.currentSendTask.isPaused) {
          this.currentSendTask.isPaused = false;
          this.fillPrefetchQueue();
          this.pumpSendQueue();
        }
        this.emit('transfer_status_change', { status: 'Transferring...' });
        break;
      }

      case 'TRANSFER_CANCEL': {
        this.stopSpeedTracker();
        if (this.currentSendTask && this.currentSendTask.id === msg.id) {
          this.currentSendTask.isCancelled = true;
          const cancelled = this.currentSendTask;
          this.currentSendTask = null;
          this.prefetchMap.clear();
          this.emit('transfer_cancelled', cancelled);
        } else if (this.currentReceiveTask && this.currentReceiveTask.id === msg.id) {
          const cancelled = this.currentReceiveTask;
          this.currentReceiveTask = null;
          this.emit('transfer_cancelled', cancelled);
        }
        break;
      }
    }
  }

  handleBinaryChunk(arrayBuffer) {
    if (!this.currentReceiveTask) return;

    const dataView = new DataView(arrayBuffer);
    const chunkIndex = dataView.getUint32(0, false); // Big endian
    
    // Zero-copy array buffer payload reference
    const chunkPayload = arrayBuffer.slice ? arrayBuffer.slice(4) : new Uint8Array(arrayBuffer, 4);

    const task = this.currentReceiveTask;
    if (!task.chunks[chunkIndex]) {
      task.chunks[chunkIndex] = chunkPayload;
      task.receivedChunkCount++;
      task.receivedBytes += (arrayBuffer.byteLength - 4);
      this.bytesInWindow += (arrayBuffer.byteLength - 4);

      const percent = Math.min(100, (task.receivedBytes / task.size) * 100);
      
      this.emit('receive_progress', {
        task,
        bytesReceived: task.receivedBytes,
        totalBytes: task.size,
        percent,
        speedBps: this.currentSpeedBps,
        etaSeconds: this.calculateETA(task.size - task.receivedBytes)
      });
    }
  }

  finishReceiveFile(task) {
    this.stopSpeedTracker();
    
    // Cleanly filter out any missing or sparse undefined chunks to avoid blob corruption
    const validChunks = [];
    for (let i = 0; i < task.totalChunks; i++) {
      if (task.chunks[i] !== undefined) {
        validChunks.push(task.chunks[i]);
      } else {
        console.warn(`[FileTransfer] Missing chunk index ${i} for file ${task.name}`);
      }
    }

    const mimeType = task.fileType || 'application/octet-stream';
    const fileBlob = new Blob(validChunks, { type: mimeType });
    const downloadUrl = URL.createObjectURL(fileBlob);

    const receivedItem = {
      id: task.id,
      name: task.name,
      size: task.size,
      type: mimeType,
      blob: fileBlob,
      url: downloadUrl,
      time: new Date().toLocaleTimeString()
    };

    this.receivedFiles.unshift(receivedItem);
    this.currentReceiveTask = null;

    this.emit('receive_complete', receivedItem);
  }

  // --- SPEED & ETA COMPUTATION ---

  startSpeedTracker() {
    this.stopSpeedTracker();
    this.bytesInWindow = 0;
    this.lastSpeedCheck = Date.now();

    this.speedTimer = setInterval(() => {
      const now = Date.now();
      const elapsedSeconds = (now - this.lastSpeedCheck) / 1000;
      if (elapsedSeconds > 0) {
        const instantBps = this.bytesInWindow / elapsedSeconds;
        // Smooth exponential moving average
        this.currentSpeedBps = this.currentSpeedBps > 0 
          ? (this.currentSpeedBps * 0.7) + (instantBps * 0.3) 
          : instantBps;

        this.bytesInWindow = 0;
        this.lastSpeedCheck = now;
      }
    }, 500);
  }

  stopSpeedTracker() {
    if (this.speedTimer) {
      clearInterval(this.speedTimer);
      this.speedTimer = null;
    }
    this.currentSpeedBps = 0;
  }

  calculateETA(remainingBytes) {
    if (this.currentSpeedBps <= 0 || remainingBytes <= 0) return 0;
    return Math.ceil(remainingBytes / this.currentSpeedBps);
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

window.FileTransferManager = FileTransferManager;
