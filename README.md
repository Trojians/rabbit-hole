# 🐰 兔子洞 — Rabbit Hole

去中心化 P2P 通信网络。灵感来自《现实编程协会》。

## 功能

- 🔑 公钥身份系统（ECDSA P-256，无需注册）
- 💬 实时加密聊天（广播 + 私聊）
- 📡 WebRTC P2P 直连 + WebSocket 中继
- 📦 文件分片分布式存储（SHA-256 校验）
- 🥕 胡萝卜积分经济系统
- 🔥 资源竞价加速机制
- 🗺️ 节点网络可视化

## 快速启动

```bash
npm install
node server.js
```

浏览器打开 http://localhost:3000

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| PORT | 3000 | 监听端口 |

## 部署

### 方式一：直接运行

```bash
PORT=8080 node server.js
```

### 方式二：PM2 守护进程

```bash
npm install -g pm2
pm2 start server.js --name rabbit-hole
pm2 save
pm2 startup
```

### 方式三：Docker

```bash
docker build -t rabbit-hole .
docker run -d -p 3000:3000 --name rabbit-hole rabbit-hole
```

### 方式四：Docker Compose

```bash
docker compose up -d
```

### 方式五：systemd（Linux）

```bash
sudo cp rabbit-hole.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rabbit-hole
```

## Nginx 反代（推荐）

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

然后 `certbot` 加 HTTPS 即可。

## 技术架构

```
Browser A ←--WebRTC P2P--→ Browser B
    ↕                          ↕
WebSocket ←→ Server (信令+中继) ←→ WebSocket
```

- 信令服务器：WebSocket，负责节点发现和 WebRTC 握手
- 数据通道：WebRTC DataChannel，浏览器直连
- 中继回退：P2P 连不上时走服务器转发

## 项目结构

```
rabbit-hole/
├── server.js              # Node.js 服务器（WebSocket + HTTP）
├── public/
│   └── index.html         # 前端 SPA（全功能单文件）
├── package.json
├── Dockerfile
├── docker-compose.yml
├── rabbit-hole.service    # systemd 服务文件
└── README.md
```
