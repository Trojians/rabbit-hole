const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// ─── Persistent Storage ───
const DATA_DIR = path.join(__dirname, 'data');
const PEERS_FILE = path.join(DATA_DIR, 'peers.json');
const RESOURCES_FILE = path.join(DATA_DIR, 'resources.json');
const CHUNKS_DIR = path.join(DATA_DIR, 'chunks');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(CHUNKS_DIR)) fs.mkdirSync(CHUNKS_DIR, { recursive: true });

function loadJson(filePath, fallback) {
  try { return JSON.parse(fs.readFileSync(filePath, 'utf8')); } catch { return fallback; }
}

function saveJson(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
}

// Chunk persistence: each chunk stored as data/chunks/<hash>.json
function saveChunk(hash, data) {
  try {
    fs.writeFileSync(path.join(CHUNKS_DIR, hash + '.json'), JSON.stringify(data));
  } catch (e) {
    console.error(`[chunk] 保存失败: ${hash.slice(0,12)}...`, e.message);
  }
}

function loadChunk(hash) {
  try {
    return JSON.parse(fs.readFileSync(path.join(CHUNKS_DIR, hash + '.json'), 'utf8'));
  } catch { return null; }
}

// Load persisted data
const persistedPeers = loadJson(PEERS_FILE, {});  // publicKey -> { carrots, contributions, nickname }
const persistedResources = loadJson(RESOURCES_FILE, {});  // resourceId -> resource metadata (no chunk data)

// Load chunks from disk into memory cache
const chunkDataStore = new Map();
try {
  const chunkFiles = fs.readdirSync(CHUNKS_DIR).filter(f => f.endsWith('.json'));
  for (const file of chunkFiles) {
    const hash = file.replace('.json', '');
    const data = loadChunk(hash);
    if (data) chunkDataStore.set(hash, data);
  }
  console.log(`[启动] 从磁盘加载了 ${chunkDataStore.size} 个碎片`);
} catch (e) {
  console.error('[启动] 加载碎片失败:', e.message);
}

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

// Restore resource catalog from disk
for (const [id, res] of Object.entries(persistedResources)) {
  resourceCatalog.set(id, res);
}

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
        const saved = persistedPeers[myPublicKey] || {};
        const carrots = saved.carrots ?? 100;
        const contributions = saved.contributions ?? 0;
        const nickname = msg.nickname || saved.nickname || shortId(myPublicKey);

        peers.set(myPublicKey, {
          ws,
          nickname,
          carrots,
          contributions,
          joinedAt: Date.now()
        });

        // Persist
        persistedPeers[myPublicKey] = {
          carrots,
          contributions,
          nickname
        };
        saveJson(PEERS_FILE, persistedPeers);

        // Tell the new peer their info + full peer list
        ws.send(JSON.stringify({
          type: 'registered',
          publicKey: myPublicKey,
          carrots,
          peers: getPeerList(),
          resources: Array.from(resourceCatalog.values())
        }));

        // Announce to everyone
        broadcast({
          type: 'peer_joined',
          peer: {
            publicKey: myPublicKey,
            nickname,
            carrots,
            contributions,
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
        const { chunkHash, size, data } = msg;
        if (!chunkRegistry.has(chunkHash)) {
          chunkRegistry.set(chunkHash, new Set());
        }
        chunkRegistry.get(chunkHash).add(myPublicKey);

        // Store chunk data on server for relay
        if (data) {
          chunkDataStore.set(chunkHash, data);
          saveChunk(chunkHash, data);
          if (chunkDataStore.size % 10 === 0) {
            console.log(`[announce_chunk] 已存储 ${chunkDataStore.size} 个碎片`);
          }
        } else {
          console.log(`[announce_chunk] 警告: chunk ${chunkHash.slice(0,12)}... 没有 data 字段`);
        }

        // Reward carrots for contributing storage
        const peer = peers.get(myPublicKey);
        if (peer) {
          const reward = Math.ceil(size / 1024); // 1 carrot per KB
          peer.carrots += reward;
          peer.contributions += size;

          // Persist
          if (persistedPeers[myPublicKey]) {
            persistedPeers[myPublicKey].carrots = peer.carrots;
            persistedPeers[myPublicKey].contributions = peer.contributions;
            saveJson(PEERS_FILE, persistedPeers);
          }

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
        const chunkData = chunkDataStore.get(msg.chunkHash);
        ws.send(JSON.stringify({
          type: 'chunk_holders',
          chunkHash: msg.chunkHash,
          holders: holders ? Array.from(holders) : [],
          hasData: !!chunkData
        }));
        break;
      }

      // ─── Fetch chunk data from server ───
      case 'fetch_chunk': {
        let chunkData = chunkDataStore.get(msg.chunkHash);
        if (!chunkData) {
          // Fallback: try loading from disk
          chunkData = loadChunk(msg.chunkHash);
          if (chunkData) {
            chunkDataStore.set(msg.chunkHash, chunkData);
          }
        }
        console.log(`[fetch_chunk] hash=${msg.chunkHash.slice(0,12)}... found=${!!chunkData} storeSize=${chunkDataStore.size}`);
        ws.send(JSON.stringify({
          type: 'chunk_data',
          chunkHash: msg.chunkHash,
          data: chunkData || null
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
        persistedResources[msg.resourceId] = resource;
        saveJson(RESOURCES_FILE, persistedResources);
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

          // Persist
          if (persistedPeers[myPublicKey]) {
            persistedPeers[myPublicKey].carrots = peer.carrots;
            saveJson(PEERS_FILE, persistedPeers);
          }
          persistedResources[msg.resourceId] = resource;
          saveJson(RESOURCES_FILE, persistedResources);

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
            if (persistedPeers[myPublicKey]) {
              persistedPeers[myPublicKey].carrots = relayPeer.carrots;
              saveJson(PEERS_FILE, persistedPeers);
            }
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
          if (persistedPeers[myPublicKey]) {
            persistedPeers[myPublicKey].nickname = msg.nickname;
            saveJson(PEERS_FILE, persistedPeers);
          }
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


