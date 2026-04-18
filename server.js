const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// ─── HTTP Server (serves frontend) ───
const server = http.createServer((req, res) => {
  let filePath = path.join(__dirname, 'public', req.url === '/' ? 'index.html' : req.url);
  const ext = path.extname(filePath);
  const mimeTypes = {
    '.html': 'text/html',
    '.js': 'application/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.svg': 'image/svg+xml'
  };

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not Found');
      return;
    }
    res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'text/plain' });
    res.end(data);
  });
});

// ─── WebSocket Signaling Server ───
const wss = new WebSocketServer({ server });

// Peer registry: publicKey -> { ws, nickname, carrots, contributions, joinedAt }
const peers = new Map();

// Chunk registry: chunkHash -> Set of publicKey who store it
const chunkRegistry = new Map();

// Resource catalog: resourceId -> metadata
const resourceCatalog = new Map();

// Generate a short ID for display
function shortId(pubKey) {
  return pubKey.slice(0, 8);
}

function broadcast(data, excludeWs) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client !== excludeWs && client.readyState === 1) {
      client.send(msg);
    }
  });
}

function getPeerList() {
  const list = [];
  for (const [pubKey, peer] of peers) {
    list.push({
      publicKey: pubKey,
      nickname: peer.nickname,
      carrots: peer.carrots,
      contributions: peer.contributions,
      joinedAt: peer.joinedAt,
      online: true
    });
  }
  return list;
}

wss.on('connection', (ws) => {
  let myPublicKey = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      // ─── Registration ───
      case 'register': {
        myPublicKey = msg.publicKey;
        peers.set(myPublicKey, {
          ws,
          nickname: msg.nickname || shortId(myPublicKey),
          carrots: 100, // starting carrots
          contributions: 0,
          joinedAt: Date.now()
        });

        // Tell the new peer their info + full peer list
        ws.send(JSON.stringify({
          type: 'registered',
          publicKey: myPublicKey,
          carrots: 100,
          peers: getPeerList(),
          resources: Array.from(resourceCatalog.values())
        }));

        // Announce to everyone
        broadcast({
          type: 'peer_joined',
          peer: {
            publicKey: myPublicKey,
            nickname: msg.nickname || shortId(myPublicKey),
            carrots: 100,
            contributions: 0,
            joinedAt: Date.now(),
            online: true
          }
        }, ws);
        break;
      }

      // ─── WebRTC Signaling ───
      case 'offer':
      case 'answer':
      case 'ice-candidate': {
        const target = peers.get(msg.target);
        if (target && target.ws.readyState === 1) {
          target.ws.send(JSON.stringify({
            ...msg,
            from: myPublicKey
          }));
        }
        break;
      }

      // ─── Chunk Registration (distributed storage) ───
      case 'announce_chunk': {
        const { chunkHash, size } = msg;
        if (!chunkRegistry.has(chunkHash)) {
          chunkRegistry.set(chunkHash, new Set());
        }
        chunkRegistry.get(chunkHash).add(myPublicKey);

        // Reward carrots for contributing storage
        const peer = peers.get(myPublicKey);
        if (peer) {
          const reward = Math.ceil(size / 1024); // 1 carrot per KB
          peer.carrots += reward;
          peer.contributions += size;
          ws.send(JSON.stringify({
            type: 'carrot_update',
            carrots: peer.carrots,
            reason: `存储奖励 +${reward}🥕`
          }));
        }
        break;
      }

      // ─── Chunk Discovery ───
      case 'find_chunk': {
        const holders = chunkRegistry.get(msg.chunkHash);
        ws.send(JSON.stringify({
          type: 'chunk_holders',
          chunkHash: msg.chunkHash,
          holders: holders ? Array.from(holders) : []
        }));
        break;
      }

      // ─── Resource Publishing ───
      case 'publish_resource': {
        const resource = {
          id: msg.resourceId,
          name: msg.name,
          size: msg.size,
          chunks: msg.chunks,
          publisher: myPublicKey,
          publisherName: peers.get(myPublicKey)?.nickname,
          publishedAt: Date.now(),
          downloads: 0,
          boosted: false
        };
        resourceCatalog.set(msg.resourceId, resource);
        broadcast({ type: 'new_resource', resource });
        break;
      }

      // ─── Boost Resource (carrot bidding) ───
      case 'boost_resource': {
        const peer = peers.get(myPublicKey);
        const resource = resourceCatalog.get(msg.resourceId);
        if (peer && resource && peer.carrots >= msg.amount) {
          peer.carrots -= msg.amount;
          resource.boosted = true;
          resource.boostAmount = (resource.boostAmount || 0) + msg.amount;
          resource.boostedBy = myPublicKey;

          ws.send(JSON.stringify({
            type: 'carrot_update',
            carrots: peer.carrots,
            reason: `竞价加速 -${msg.amount}🥕`
          }));

          broadcast({
            type: 'resource_boosted',
            resourceId: msg.resourceId,
            boostAmount: resource.boostAmount,
            boostedBy: myPublicKey
          });
        }
        break;
      }

      // ─── Relay message (fallback when no direct P2P) ───
      case 'relay': {
        const target = peers.get(msg.target);
        if (target && target.ws.readyState === 1) {
          target.ws.send(JSON.stringify({
            type: 'relay',
            from: myPublicKey,
            payload: msg.payload
          }));
          // Reward relay node
          const relayPeer = peers.get(myPublicKey);
          if (relayPeer) {
            relayPeer.carrots += 1;
          }
        }
        break;
      }

      // ─── Direct message (via server relay) ───
      case 'dm': {
        const target = peers.get(msg.target);
        if (target && target.ws.readyState === 1) {
          target.ws.send(JSON.stringify({
            type: 'dm',
            from: myPublicKey,
            fromName: peers.get(myPublicKey)?.nickname,
            content: msg.content,
            timestamp: Date.now()
          }));
        }
        break;
      }

      // ─── Broadcast message ───
      case 'broadcast_msg': {
        broadcast({
          type: 'broadcast_msg',
          from: myPublicKey,
          fromName: peers.get(myPublicKey)?.nickname,
          content: msg.content,
          timestamp: Date.now()
        }, ws);
        break;
      }

      // ─── Nickname update ───
      case 'set_nickname': {
        const peer = peers.get(myPublicKey);
        if (peer) {
          peer.nickname = msg.nickname;
          broadcast({
            type: 'peer_updated',
            publicKey: myPublicKey,
            nickname: msg.nickname
          });
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    if (myPublicKey && peers.has(myPublicKey)) {
      peers.delete(myPublicKey);
      broadcast({
        type: 'peer_left',
        publicKey: myPublicKey
      });
    }
  });
});

server.listen(PORT, () => {
  console.log(`\n🐰 兔子洞网络已启动`);
  console.log(`📡 WebSocket: ws://localhost:${PORT}`);
  console.log(`🌐 前端页面: http://localhost:${PORT}`);
  console.log(`\n等待兔子们入洞...\n`);
});
