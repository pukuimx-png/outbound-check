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

let state = {
  groups: [],
  selectedMainKeys: [],
  scanStatus: {},
  resetPending: false,
  batches: []
};
let recipients = [];
let mainKeyOwners = {};

// ========== 自动清理（保留最近30天） ==========
const KEEP_DAYS = 30;

function cleanOldData() {
  if (state.batches.length === 0) return;

  const now = new Date();
  const cutoff = new Date(now);
  cutoff.setDate(cutoff.getDate() - KEEP_DAYS);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  const remainingBatches = state.batches.filter(b => (b.createdAt || '') >= cutoffStr);
  const activeKeys = new Set();
  remainingBatches.forEach(b => b.keys.forEach(k => activeKeys.add(k)));

  const updatedGroups = state.groups.filter(g => activeKeys.has(g.key));
  const validCodes = new Set();
  updatedGroups.forEach(g => g.items.forEach(item => validCodes.add(item.code)));
  const newScanStatus = {};
  for (const code in state.scanStatus) {
    if (validCodes.has(code)) newScanStatus[code] = state.scanStatus[code];
  }

  state.batches = remainingBatches;
  state.groups = updatedGroups;
  state.scanStatus = newScanStatus;
  state.selectedMainKeys = state.selectedMainKeys.filter(k => activeKeys.has(k));
  for (const key in mainKeyOwners) {
    if (!activeKeys.has(key)) delete mainKeyOwners[key];
  }
  console.log(`🧹 Limpieza completada: ${remainingBatches.length} lotes, ${updatedGroups.length} códigos.`);
}
// =============================================

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
    cleanOldData();   // 启动时清理旧数据
    saveStateToDB();
  } catch (err) {
    console.error('❌ Error al cargar estado:', err);
  }
}

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

// ===== 关键修改：上传新文件时不调用 cleanOldData =====
function initState(groupsData) {
  state.groups = groupsData;
  state.selectedMainKeys = [];
  state.scanStatus = {};
  mainKeyOwners = {};
  for (const g of groupsData) {
    for (const item of g.items) {
      state.scanStatus[item.code] = false;
    }
  }
  state.resetPending = true;   // 强制所有设备重新认证
  // 移除 cleanOldData();   // 上传新文件时不清理，避免误删新groups
  broadcast();
}
// =====================================================

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
          const { code, mode } = data;
          if (mode === 'main') {
            const group = state.groups.find(g => g.key === code);
            if (!group) {
              ws.send(JSON.stringify({ type: 'error', message: `❌ 未找到主码: ${code}` }));
              return;
            }
            if (isMainCompleted(code)) {
              ws.send(JSON.stringify({ type: 'error', message: `⏳ 主码 ${code} 已完成，无法再次添加` }));
              return;
            }
            const owner = mainKeyOwners[code];
            if (owner && owner !== deviceId) {
              ws.send(JSON.stringify({ type: 'error', message: `❌ 主码 ${code} 已被其他设备锁定，不可重复扫描` }));
              return;
            }
            if (!owner) {
              mainKeyOwners[code] = deviceId;
              state.selectedMainKeys.push(code);
              broadcast();
              ws.send(JSON.stringify({ type: 'success', message: `✅ 已添加主码: ${code} (${group.items.length} 箱)` }));
            } else {
              const total = group.items.length;
              const scanned = group.items.filter(item => state.scanStatus[item.code]).length;
              const remaining = total - scanned;
              ws.send(JSON.stringify({ type: 'success', message: `✅ 可补扫主码: ${code} (剩余 ${remaining} 箱未扫)` }));
            }
          } else if (mode === 'sub') {
            let found = false;
            let matchedItem = null;
            for (const key of state.selectedMainKeys) {
              const g = state.groups.find(gr => gr.key === key);
              if (g) {
                const item = g.items.find(i => i.code === code);
                if (item) {
                  found = true;
                  matchedItem = item;
                  break;
                }
                const parts = code.split('/');
                if (parts.length === 2 && parts[0] === key) {
                  const idx = parseInt(parts[1]);
                  if (!isNaN(idx) && idx >= 1 && idx <= g.items.length) {
                    const realItem = g.items[idx - 1];
                    found = true;
                    matchedItem = realItem;
                    break;
                  }
                }
              }
            }
            if (!found) {
              ws.send(JSON.stringify({ type: 'error', message: `❌ 无效子码: ${code} (不在全局主码列表中)` }));
              return;
            }
            if (state.scanStatus[matchedItem.code]) {
              ws.send(JSON.stringify({ type: 'error', message: `⏳ 子码 ${code} 已扫描过` }));
              return;
            }
            state.scanStatus[matchedItem.code] = true;
            broadcast();
            const scannedCount = Object.values(state.scanStatus).filter(v => v).length;
            const total = state.selectedMainKeys.reduce((sum, k) => {
              const g = state.groups.find(gr => gr.key === k);
              return sum + (g ? g.items.length : 0);
            }, 0);
            ws.send(JSON.stringify({ type: 'success', message: `✅ 已扫描: ${code} (${scannedCount}/${total})` }));
          }
          break;
        }
        case 'release_keys': {
          const keysToRelease = data.keys || [];
          if (keysToRelease.length === 0) break;
          const filteredKeys = keysToRelease.filter(k => mainKeyOwners[k] === deviceId);
          state.selectedMainKeys = state.selectedMainKeys.filter(k => !filteredKeys.includes(k));
          for (const key of filteredKeys) {
            delete mainKeyOwners[key];
            const g = state.groups.find(gr => gr.key === key);
            if (g) {
              for (const item of g.items) {
                state.scanStatus[item.code] = false;
              }
            }
          }
          broadcast();
          ws.send(JSON.stringify({ type: 'success', message: `🔄 已释放主码: ${filteredKeys.join(', ')}` }));
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