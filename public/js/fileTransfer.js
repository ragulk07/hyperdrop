class FileTransferManager {
  constructor(p2pManager) {
    this.p2p = p2pManager;
    this.CHUNK_SIZE = 128 * 1024; // 128KB per chunk for high throughput
    this.MAX_BUFFER_AMOUNT = 4 * 1024 * 1024; // 4MB backpressure threshold to prevent starvation

    this.sendQueue = [];
    this.currentSendTask = null;
    this.currentReceiveTask = null;
    this.receivedFiles = [];
    this.listeners = new Map();

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
      if (this.currentSendTask && this.currentSendTask.isWaitingForBuffer) {
        this.currentSendTask.isWaitingForBuffer = false;
        this.readAndSendNextChunk();
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
    const totalChunks = Math.ceil(file.size / this.CHUNK_SIZE);
    
    const task = {
      id,
      file,
      name: file.name,
      size: file.size,
      type: file.type || 'application/octet-stream',
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

    console.log('[FileTransfer] Starting send task:', this.currentSendTask.name);

    // Send FILE_START header JSON
    const header = {
      type: 'FILE_START',
      id: this.currentSendTask.id,
      name: this.currentSendTask.name,
      size: this.currentSendTask.size,
      totalChunks: this.currentSendTask.totalChunks,
      fileType: this.currentSendTask.type
    };

    this.p2p.sendData(JSON.stringify(header));
    this.startSpeedTracker();
    this.readAndSendNextChunk();
  }

  async readAndSendNextChunk() {
    const task = this.currentSendTask;
    if (!task || task.isPaused || task.isCancelled) return;

    if (task.currentChunkIndex >= task.totalChunks) {
      // Completed sending all chunks
      console.log('[FileTransfer] Finished sending file:', task.name);
      this.p2p.sendData(JSON.stringify({ type: 'FILE_END', id: task.id }));
      this.emit('send_complete', task);
      this.currentSendTask = null;
      this.stopSpeedTracker();
      
      // Auto process next in queue
      if (this.sendQueue.length > 0) {
        setTimeout(() => this.startSending(), 500);
      }
      return;
    }

    // Backpressure check
    if (this.p2p.getBufferedAmount() > this.MAX_BUFFER_AMOUNT) {
      task.isWaitingForBuffer = true;
      return;
    }

    const start = task.currentChunkIndex * this.CHUNK_SIZE;
    const end = Math.min(start + this.CHUNK_SIZE, task.size);
    const blobSlice = task.file.slice(start, end);

    try {
      const rawBuffer = blobSlice.arrayBuffer ? await blobSlice.arrayBuffer() : await this.readBlobAsArrayBuffer(blobSlice);
      
      if (!this.currentSendTask || this.currentSendTask.id !== task.id) return;
      if (task.isPaused || task.isCancelled) return;

      const chunkIndex = task.currentChunkIndex;

      // Pack [4 bytes uint32 index] + [payload]
      const packet = new Uint8Array(4 + rawBuffer.byteLength);
      const indexView = new DataView(packet.buffer);
      indexView.setUint32(0, chunkIndex, false); // Big endian
      packet.set(new Uint8Array(rawBuffer), 4);

      const sent = this.p2p.sendData(packet.buffer);
      if (sent) {
        task.currentChunkIndex++;
        this.bytesInWindow += rawBuffer.byteLength;

        const bytesSent = Math.min(task.currentChunkIndex * this.CHUNK_SIZE, task.size);
        const percent = Math.min(100, (bytesSent / task.size) * 100);
        
        this.emit('send_progress', {
          task,
          bytesSent,
          totalBytes: task.size,
          percent,
          speedBps: this.currentSpeedBps,
          etaSeconds: this.calculateETA(task.size - bytesSent)
        });

        // Continue loop
        this.readAndSendNextChunk();
      } else {
        // Retry shortly if buffered
        setTimeout(() => this.readAndSendNextChunk(), 10);
      }
    } catch (err) {
      console.error('[FileTransfer] Chunk read error:', err);
    }
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
      this.readAndSendNextChunk();
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
          this.readAndSendNextChunk();
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
    const chunkPayload = arrayBuffer.slice(4);

    const task = this.currentReceiveTask;
    if (!task.chunks[chunkIndex]) {
      task.chunks[chunkIndex] = chunkPayload;
      task.receivedChunkCount++;
      task.receivedBytes += chunkPayload.byteLength;
      this.bytesInWindow += chunkPayload.byteLength;

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
