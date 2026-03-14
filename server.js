const http = require('http');
const crypto = require('crypto');
const net = require('net');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const HOST = '0.0.0.0';

// WebSocket UUID for Sec-WebSocket-Accept
const WS_MAGIC_STRING = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// Sessions Map: roomCode (string) -> { gameClient: socket, phoneClient: socket, lastOrientation: string }
const sessions = new Map();

// Generate 4-digit room code
function generateRoomCode() {
  let code;
  do {
    code = Math.floor(1000 + Math.random() * 9000).toString();
  } while (sessions.has(code));
  return code;
}

// Create HTTP server to serve static files
const server = http.createServer((req, res) => {
  let filePath = path.join(__dirname, req.url === '/' ? 'game.html' : req.url);
  
  // Basic security check to prevent directory traversal
  if (filePath.indexOf(__dirname) !== 0) {
    res.writeHead(403);
    return res.end('Forbidden');
  }
  
  // Default to 404
  fs.stat(filePath, (err, stats) => {
    if (err || !stats.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('404 Not Found');
      return;
    }
    
    const ext = path.extname(filePath);
    let contentType = 'text/plain';
    if (ext === '.html') contentType = 'text/html';
    else if (ext === '.css') contentType = 'text/css';
    else if (ext === '.js') contentType = 'application/javascript';
    
    // Add necessary headers for SharedArrayBuffer (sometimes needed by MediaPipe/WASM under the hood)
    res.writeHead(200, {
      'Content-Type': contentType,
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp'
    });
    fs.createReadStream(filePath).pipe(res);
  });
});


// Handle upgrade requests
server.on('upgrade', (req, socket, head) => {
  const pathname = req.url;
  
  let isGame = false;
  let isPhone = false;
  let roomCode = null;

  if (pathname === '/game') {
    isGame = true;
  } else if (pathname.startsWith('/phone/')) {
    isPhone = true;
    roomCode = pathname.replace('/phone/', '');
  } else {
    socket.destroy();
    return;
  }

  if (isPhone && (!roomCode || roomCode.length !== 4 || !sessions.has(roomCode))) {
    // Invalid or non-existent room code
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }

  // Handle WebSocket Handshake
  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.destroy();
    return;
  }

  const acceptKey = crypto
    .createHash('sha1')
    .update(key + WS_MAGIC_STRING)
    .digest('base64');

  const headers = [
    'HTTP/1.1 101 Switching Protocols',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Accept: ${acceptKey}`
  ];

  socket.write(headers.join('\r\n') + '\r\n\r\n');

  // Session Management
  let session;

  if (isGame) {
    roomCode = generateRoomCode();
    session = {
      gameClient: socket,
      phoneClient: null,
      lastOrientation: null
    };
    sessions.set(roomCode, session);
    console.log(`[+] Room ${roomCode} created by Game`);
    
    // Send the generated room code to the game client
    setImmediate(() => {
      sendMessage(socket, JSON.stringify({ type: 'room_code', code: roomCode }));
    });
  } else if (isPhone) {
    session = sessions.get(roomCode);
    if (session.phoneClient) {
      session.phoneClient.destroy();
    }
    session.phoneClient = socket;
    console.log(`[+] Phone joined Room ${roomCode}`);
    
    // Notify game
    if (session.gameClient) {
      sendMessage(session.gameClient, JSON.stringify({ type: 'phone_connected' }));
      // Send cached orientation if available
      if (session.lastOrientation) {
        sendMessage(session.gameClient, session.lastOrientation);
      }
    }
  }

  // WebSocket Frame Parsing
  let buffer = Buffer.alloc(0);

  socket.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    
    while (buffer.length >= 2) {
      const firstByte = buffer[0];
      const secondByte = buffer[1];
      
      const fin = (firstByte & 0x80) === 0x80;
      const opcode = firstByte & 0x0f;
      const masked = (secondByte & 0x80) === 0x80;
      let payloadLength = secondByte & 0x7f;
      
      let headerLength = 2;
      
      if (payloadLength === 126) {
        if (buffer.length < 4) return;
        payloadLength = buffer.readUInt16BE(2);
        headerLength = 4;
      } else if (payloadLength === 127) {
        if (buffer.length < 10) return;
        const high = buffer.readUInt32BE(2);
        const low = buffer.readUInt32BE(6);
        payloadLength = (high * Math.pow(2, 32)) + low;
        headerLength = 10;
      }
      
      if (masked) {
        headerLength += 4;
      }
      
      const frameLength = headerLength + payloadLength;
      
      if (buffer.length < frameLength) {
        return;
      }
      
      const frame = buffer.slice(0, frameLength);
      buffer = buffer.slice(frameLength);
      
      // Handle close
      if (opcode === 0x8) {
        socket.destroy();
        return;
      }
      
      // Handle ping
      if (opcode === 0x9) {
        const pong = Buffer.alloc(2);
        pong[0] = 0x8a;
        pong[1] = 0x00;
        socket.write(pong);
        continue;
      }
      
      // Text frame
      if (opcode === 0x1) {
        let payload = Buffer.alloc(payloadLength);
        if (masked) {
          const maskingKey = frame.slice(headerLength - 4, headerLength);
          for (let i = 0; i < payloadLength; i++) {
            payload[i] = frame[headerLength + i] ^ maskingKey[i % 4];
          }
        } else {
          payload = frame.slice(headerLength);
        }
        
        const message = payload.toString('utf8');
        
        try {
          const msgJson = JSON.parse(message);
          if (msgJson.type === 'orientation' && isPhone) {
            session.lastOrientation = message;
          }
        } catch (e) {
          // Ignore invalid JSON
        }
        
        // Relay logic within the specific session room
        if (isPhone && session.gameClient) {
          sendMessage(session.gameClient, message);
        } else if (isGame && session.phoneClient) {
          sendMessage(session.phoneClient, message);
        }
      }
    }
  });

  socket.on('close', () => {
    if (isPhone) {
      session.phoneClient = null;
      console.log(`[-] Phone left Room ${roomCode}`);
      if (session.gameClient) {
        sendMessage(session.gameClient, JSON.stringify({ type: 'phone_disconnected' }));
      }
    } else if (isGame) {
      session.gameClient = null;
      console.log(`[-] Game left Room ${roomCode}`);
      if (session.phoneClient) {
        sendMessage(session.phoneClient, JSON.stringify({ type: 'game_disconnected' }));
      }
      // If the game client disconnects, we destroy the room entirely
      sessions.delete(roomCode);
      console.log(`[x] Room ${roomCode} destroyed`);
    }
  });

  socket.on('error', (err) => {
    console.error(`Socket error on Room ${roomCode}:`, err.message);
  });
});

// Write text message back to socket
function sendMessage(socket, text) {
  if (socket.destroyed) return;
  
  const payload = Buffer.from(text, 'utf8');
  let headerLength;
  let secondByte = payload.length;

  if (payload.length <= 125) {
    headerLength = 2;
  } else if (payload.length <= 65535) {
    headerLength = 4;
    secondByte = 126;
  } else {
    headerLength = 10;
    secondByte = 127;
  }

  const frame = Buffer.alloc(headerLength + payload.length);
  frame[0] = 0x81; // FIN + Text Opcode
  frame[1] = secondByte; // Unmasked

  if (payload.length > 125 && payload.length <= 65535) {
    frame.writeUInt16BE(payload.length, 2);
  } else if (payload.length > 65535) {
    const high = Math.floor(payload.length / Math.pow(2, 32));
    const low = payload.length % Math.pow(2, 32);
    frame.writeUInt32BE(high, 2);
    frame.writeUInt32BE(low, 6);
  }

  payload.copy(frame, headerLength);
  
  try {
    socket.write(frame);
  } catch (err) {
    console.error('Failed to send message:', err.message);
  }
}

server.listen(PORT, HOST, () => {
  console.log('====================================');
  console.log('🏏 POV Cricket Relay Server started');
  console.log(`📡 Listening on: http://0.0.0.0:${PORT}`);
  console.log('====================================');
  console.log('Waiting for connections...');
  console.log('Open the browser to the host address to start a game room.');
});

