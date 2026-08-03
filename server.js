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
    historyGroups: Array,
    historyScanStatus: Object,
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

// 默认状态（确保所有字段存在）
let state = {
  groups: [],
  historyGroups: [],
  historyScanStatus: {},
  selectedMainKeys: [],
  scanStatus: {},
  resetPending: false,
  batches: []
};
let recipients = [];
let mainKeyOwners = {};

// ========== 自动清理（保留最近30天，仅清理批次） ==========
const KEEP_DAYS = 30;

function cleanOldData() {
  if (state.batches.length === 0) {
    console.log('🧹 No hay lotes, saltar limpieza.');
    return;
  }

  const now = new Date();
  const cutoff = new Date(now);
  cutoff.setDate(cutoff.getDate() - KEEP_DAYS);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  const remainingBatches = state.batches.filter(b => (b.createdAt || '') >= cutoffStr);
  state.batches = remainingBatches;

  const validKeys = new Set(state.groups.map(g => g.key));
  for (const key in mainKeyOwners) {
    if (!validKeys.has(key)) {
      delete mainKeyOwners[key];
    }
  }

  console.log(`🧹 Limpieza completada: se conservan ${remainingBatches.length} lotes.`);
}
// ============================================================

// ----- 从数据库加载状态 -----
async function loadStateFromDB() {
  try {
    const doc = await StateModel.findOne();
    if (doc) {
      // 确保所有字段存在，防止旧文档缺少新字段
      state = {
        groups: doc.state.groups || [],
        historyGroups: doc.state.historyGroups || [],
        historyScanStatus: doc.state.historyScanStatus || {},
        selectedMainKeys: doc.state.selectedMainKeys || [],
        scanStatus: doc.state.scanStatus || {},
        resetPending: doc.state.resetPending || false,
        batches: doc.state.batches || []
      };
      recipients = doc.recipients || [];
      console.log('📂 Estado cargado desde MongoDB');
    } else {
      console.log('📂 No hay documento en DB, usando estado inicial');
      await StateModel.create({ state, recipients });
    }
    cleanOldData();
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

// ===== 核心修改：上传新文件时归档旧数据及其扫描状态 =====
function initState(groupsData) {
    try {
        // 确保 state.scanStatus 是对象
        if (!state.scanStatus) state.scanStatus = {};

        // ---- 归档旧主码及扫描状态 ----
        if (state.groups && state.groups.length > 0) {
            // 将当前 groups 追加到历史
            state.historyGroups = state.historyGroups.concat(state.groups);
            // 保存当前扫描状态到历史
            for (const g of state.groups) {
                for (const item of g.items) {
                    const code = item.code;
                    if (state.scanStatus[code]) {
                        state.historyScanStatus[code] = state.scanStatus[code];
                    }
                }
            }
        }

        // ---- 替换当前工作数据 ----
        state.groups = groupsData || [];
        state.selectedMainKeys = [];
        state.scanStatus = {};
        mainKeyOwners = {};
        for (const g of state.groups) {
            for (const item of g.items) {
                state.scanStatus[item.code] = false;
            }
        }

        // ---- 强制重新认证 ----
        state.resetPending = true;

        // 保留 state.batches 和 recipients
        broadcast();
    } catch (err) {
        console.error('❌ initState error:', err);
        // 即使出错也广播，避免客户端卡死
        broadcast();
    }
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
          // ... 扫描逻辑（保持不变） ...
          break;
        }

        case 'release_keys': {
          // ... 释放主码逻辑（保持不变） ...
          break;
        }

        case 'reset_all': {
          state.groups = [];
          state.historyGroups = [];
          state.historyScanStatus = {};
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
            const now = new Date();
            const createdAt = now.toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' });
            batch.createdAt = createdAt;
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