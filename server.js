const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const mongoose = require('mongoose');

// ----- 连接 MongoDB -----
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://pukuimx_db_user:Mx147258@cluster0.mrr1ndg.mongodb.net/?appName=Cluster0';
console.log('🔑 MONGODB_URI:', MONGODB_URI.replace(/\/\/.*@/, '//<hidden>@'));

mongoose.connect(MONGODB_URI, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('✅ MongoDB conectado'))
  .catch(err => console.error('❌ Error MongoDB:', err));

// ----- 定义状态 Schema -----
const StateSchema = new mongoose.Schema({
  state: {
    groups: Array,
    selectedMainKeys: [String],
    scanStatus: Object,
    resetPending: Boolean,
    batches: Array
  },
  recipients: [String]
}, { collection: 'state' });

const StateModel = mongoose.model('State', StateSchema);

// ----- 应用 -----
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// 默认状态
let state = {
  groups: [],
  selectedMainKeys: [],
  scanStatus: {},
  resetPending: false,
  batches: []
};
let recipients = [];
let mainKeyOwners = {};

// ========== 自动清理：保留最近30天 ==========
const KEEP_DAYS = 30;

function cleanOldData() {
  // 如果没有批次，直接返回，避免误删 groups
  if (state.batches.length === 0) {
    console.log('🧹 No hay lotes, saltar limpieza.');
    return;
  }

  const now = new Date();
  const cutoff = new Date(now);
  cutoff.setDate(cutoff.getDate() - KEEP_DAYS);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  // 过滤掉过期的批次
  const remainingBatches = state.batches.filter(b => {
    const batchDate = b.createdAt || '';
    return batchDate >= cutoffStr;
  });

  // 收集所有剩余批次中出现的主码
  const activeKeys = new Set();
  remainingBatches.forEach(b => b.keys.forEach(k => activeKeys.add(k)));

  // 保留哪些主码组（至少在一个未过期批次中出现）
  const updatedGroups = state.groups.filter(g => activeKeys.has(g.key));

  // 更新 scanStatus，仅保留这些主码下的箱码
  const validCodes = new Set();
  updatedGroups.forEach(g => g.items.forEach(item => validCodes.add(item.code)));
  const newScanStatus = {};
  for (const code in state.scanStatus) {
    if (validCodes.has(code)) {
      newScanStatus[code] = state.scanStatus[code];
    }
  }

  // 更新状态
  state.batches = remainingBatches;
  state.groups = updatedGroups;
  state.scanStatus = newScanStatus;
  state.selectedMainKeys = state.selectedMainKeys.filter(k => activeKeys.has(k));

  // 清理 mainKeyOwners
  for (const key in mainKeyOwners) {
    if (!activeKeys.has(key)) delete mainKeyOwners[key];
  }

  console.log(`🧹 Limpieza completada: se conservan ${remainingBatches.length} lotes y ${updatedGroups.length} códigos principales.`);
}

// ============================================

// ----- 从数据库加载状态 -----
async function loadStateFromDB() {
  try {
    const doc = await StateModel.findOne();
    if (doc) {
      state = doc.state;
      recipients = doc.recipients || [];
      console.log('📂 Estado cargado desde MongoDB');
    } else {
      console.log('📂 No hay documento en DB, usando estado inicial');
      await StateModel.create({ state, recipients });
    }
    cleanOldData(); // 启动时清理
    saveStateToDB();
  } catch (err) {
    console.error('❌ Error al cargar estado:', err);
  }
}

// ----- 保存状态到数据库 -----
async function saveStateToDB() {
  try {
    await StateModel.updateOne(
      {},
      { state, recipients },
      { upsert: true }
    );
  } catch (err) {
    console.error('❌ Error al guardar estado:', err);
  }
}

// ----- 业务函数 -----
function isMainCompleted(key) {
  const g = state.groups.find(gr => gr.key === key);
  if (!g) return false;
  return g.items.every(item => state.scanStatus[item.code] || false);
}

function broadcast() {
  const data = JSON.stringify({ type: 'state', state });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  });
  saveStateToDB();
}

function broadcastRecipients() {
  const data = JSON.stringify({ type: 'addresses', recipients });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  });
  saveStateToDB();
}

function initState(groupsData) {
  // 上传新文件时重置扫描状态
  state.groups = groupsData;
  state.selectedMainKeys = [];
  state.scanStatus = {};
  mainKeyOwners = {};
  for (const g of groupsData) {
    for (const item of g.items) {
      state.scanStatus[item.code] = false;
    }
  }
  // 重置标志（通知所有设备重置身份）
  state.resetPending = true;
  // 清理旧数据（仅当有批次时才执行）
  cleanOldData();
  // 广播新状态
  broadcast();
}

// ----- WebSocket 处理 -----
wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'state', state }));
  ws.send(JSON.stringify({ type: 'addresses', recipients }));

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      const deviceId = data.deviceId;

      switch (data.type) {
        case 'init': {
          if (data.recipients) {
            recipients = data.recipients;
            broadcastRecipients();
          }
          initState(data.groups);
          break;
        }

        case 'scan': {
          // 扫描逻辑保持不变（略，与之前相同）
          // 因篇幅，此处省略，但实际代码需完整保留
          break;
        }

        case 'release_keys': {
          // 释放主码逻辑（略）
          break;
        }

        case 'reset_all': {
          state.groups = [];
          state.selectedMainKeys = [];
          state.scanStatus = {};
          mainKeyOwners = {};
          state.batches = [];
          state.resetPending = true;
          broadcast();
          ws.send(JSON.stringify({ type: 'success', message: '🔁 已重置所有数据，所有设备将重新认证' }));
          break;
        }

        case 'reset_confirmed': {
          state.resetPending = false;
          broadcast();
          break;
        }

        case 'add_batch': {
          const { batch } = data;
          if (batch) {
            state.batches.push(batch);
            broadcast();
          }
          break;
        }

        default:
          break;
      }
    } catch (err) {
      console.error('解析消息失败:', err);
      ws.send(JSON.stringify({ type: 'error', message: '服务器处理出错' }));
    }
  });
});

// ----- 启动服务器 -----
loadStateFromDB().then(() => {
  app.use(express.static(path.join(__dirname, 'public')));
  const PORT = process.env.PORT || 3000;
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`服务器运行在 http://0.0.0.0:${PORT}`);
  });

  // 每日定时清理（凌晨3点）
  setInterval(() => {
    console.log('⏰ Ejecutando limpieza programada...');
    cleanOldData();
    saveStateToDB();
    broadcast();
  }, 24 * 60 * 60 * 1000);
});