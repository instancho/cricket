const http = require('http');
const crypto = require('crypto');
const net = require('net');
const fs = require('fs');

const PORT = 8080;
const HOST = '0.0.0.0';

// WebSocket UUID for Sec-WebSocket-Accept
const WS_MAGIC_STRING = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// Connections
let phoneClient = null;
let gameClient = null;

// Cache last orientation to send to game immediately on connect
let lastOrientation = null;

// Create HTTP server
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('POV Cricket Server is running.\nConnect phone to /phone and game to /game via WebSockets.');
});

// Handle upgrade requests
server.on('upgrade', (req, socket, head) => {
  const pathname = req.url;
  
  if (pathname !== '/phone' && pathname !== '/game') {
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

  // Client connected successfully
  const isPhone = pathname === '/phone';
  
  if (isPhone) {
    if (phoneClient) phoneClient.destroy();
    phoneClient = socket;
    console.log('[+] Phone connected');
    
    // Notify game
    if (gameClient) {
      sendMessage(gameClient, JSON.stringify({ type: 'phone_connected' }));
    }
  } else {
    if (gameClient) gameClient.destroy();
    gameClient = socket;
    console.log('[+] Game connected');
    
    // Send cached orientation if available
    if (lastOrientation) {
      sendMessage(gameClient, lastOrientation);
    }

    // Notify phone
    if (phoneClient) {
      sendMessage(phoneClient, JSON.stringify({ type: 'game_connected' }));
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
        if (buffer.length < 4) return; // Wait for more data
        payloadLength = buffer.readUInt16BE(2);
        headerLength = 4;
      } else if (payloadLength === 127) {
        if (buffer.length < 10) return; // Wait for more data
        // Read 64-bit Big Int length, Node.js readBigUInt64BE
        const high = buffer.readUInt32BE(2);
        const low = buffer.readUInt32BE(6);
        // Note: bitwise operators in JS limit strictly to 32 bits, but for huge payloads > 4GB we don't care here
        payloadLength = (high * Math.pow(2, 32)) + low;
        headerLength = 10;
      }
      
      if (masked) {
        headerLength += 4; // Masking key is 4 bytes
      }
      
      const frameLength = headerLength + payloadLength;
      
      if (buffer.length < frameLength) {
        return; // Wait for full frame
      }
      
      // We have a full frame
      const frame = buffer.slice(0, frameLength);
      buffer = buffer.slice(frameLength); // Keep remaining data for next iteration
      
      // Handle close
      if (opcode === 0x8) {
        socket.destroy();
        return;
      }
      
      // Handle ping
      if (opcode === 0x9) {
        const pong = Buffer.alloc(2);
        pong[0] = 0x8a; // FIN + Pong
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
            lastOrientation = message;
          }
        } catch (e) {
          // Ignore invalid JSON
        }
        
        // Relay logic
        if (isPhone && gameClient) {
          sendMessage(gameClient, message);
        } else if (!isPhone && phoneClient) {
          sendMessage(phoneClient, message);
        }
      }
    }
  });

  socket.on('close', () => {
    if (isPhone) {
      phoneClient = null;
      console.log('[-] Phone disconnected');
      if (gameClient) {
        sendMessage(gameClient, JSON.stringify({ type: 'phone_disconnected' }));
      }
    } else {
      gameClient = null;
      console.log('[-] Game disconnected');
      if (phoneClient) {
        sendMessage(phoneClient, JSON.stringify({ type: 'game_disconnected' }));
      }
    }
  });

  socket.on('error', (err) => {
    console.error(`Socket error on ${pathname}:`, err.message);
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
  console.log(`Phone Controller URL -> http://<YOUR_LOCAL_IP>:${PORT}/phone`);
  console.log(`Game Client URL      -> http://<YOUR_LOCAL_IP>:${PORT}/game`);
  console.log('Run `ifconfig` or `ipconfig` to find your local IP address.');
});
