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

// 定义状态 Schema
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

// ========== 新增：按日期自动淘汰（保留最近 N 天） ==========
const KEEP_DAYS = 30; // 可在此调整保留天数

function cleanOldData() {
  const now = new Date();
  const cutoff = new Date(now);
  cutoff.setDate(cutoff.getDate() - KEEP_DAYS);
  const cutoffStr = cutoff.toISOString().slice(0, 10); // YYYY-MM-DD

  // 找出所有过期的批次（createdAt < cutoffStr）
  const expiredBatches = state.batches.filter(b => {
    const batchDate = b.createdAt || '';
    return batchDate < cutoffStr;
  });

  if (expiredBatches.length === 0) {
    console.log(`🧹 No hay datos antiguos (más de ${KEEP_DAYS} días) para limpiar.`);
    return;
  }

  // 收集过期批次涉及的主码
  const expiredKeys = new Set();
  expiredBatches.forEach(b => {
    b.keys.forEach(k => expiredKeys.add(k));
  });

  // 找出所有未过期的批次
  const remainingBatches = state.batches.filter(b => {
    const batchDate = b.createdAt || '';
    return batchDate >= cutoffStr;
  });

  // 计算每个主码在所有剩余批次中的最新日期
  const keyLatestDate = {};
  remainingBatches.forEach(b => {
    b.keys.forEach(k => {
      const date = b.createdAt || '';
      if (!keyLatestDate[k] || date > keyLatestDate[k]) {
        keyLatestDate[k] = date;
      }
    });
  });

  // 确定哪些主码在所有批次中都过期了（即不在 keyLatestDate 中）
  const keysToRemove = [];
  state.groups.forEach(g => {
    if (expiredKeys.has(g.key) && !keyLatestDate[g.key]) {
      keysToRemove.push(g.key);
    }
  });

  // 从 groups 中移除过期的组
  if (keysToRemove.length > 0) {
    state.groups = state.groups.filter(g => !keysToRemove.includes(g.key));
    // 从 selectedMainKeys 中移除
    state.selectedMainKeys = state.selectedMainKeys.filter(k => !keysToRemove.includes(k));
    // 从 scanStatus 中移除对应的 code
    keysToRemove.forEach(key => {
      const group = state.groups.find(g => g.key === key); // 已被过滤，不会再找到
      // 但 scanStatus 中的条目需要清除，但 items 已随 group 删除，所以只需删除对应的 code 状态
      // 由于 groups 已删除，我们无法获取 items，因此需要从原始数据中遍历
      // 或者我们保留 scanStatus 中的条目，但不会有影响，为了整洁，我们遍历所有 scanStatus 的 key，删除那些属于已删除主码的项
      // 因为 scanStatus 的 key 是 code（如 "FBA123/1"），无法直接关联主码，但我们可以通过 state.groups 检查哪些 code 不再存在。
      // 更简单：我们重构 scanStatus，仅保留当前 groups 中 items 的 code。
      const existingCodes = new Set();
      state.groups.forEach(g => {
        g.items.forEach(item => existingCodes.add(item.code));
      });
      // 重新构建 scanStatus
      const newScanStatus = {};
      for (const code in state.scanStatus) {
        if (existingCodes.has(code)) {
          newScanStatus[code] = state.scanStatus[code];
        }
      }
      state.scanStatus = newScanStatus;
    });
  }

  // 更新 batches 为未过期的批次
  state.batches = remainingBatches;

  // 从 mainKeyOwners 中移除已删除主码的所有者
  keysToRemove.forEach(key => {
    delete mainKeyOwners[key];
  });

  console.log(`🧹 Limpieza completada: se eliminaron ${expiredBatches.length} lotes antiguos y ${keysToRemove.length} códigos principales.`);
}

// ================================================================

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
    // 加载后执行清理
    cleanOldData();
    saveStateToDB(); // 保存清理后的状态
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

// 业务函数
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
  state.groups = groupsData;
  state.selectedMainKeys = [];
  state.scanStatus = {};
  mainKeyOwners = {};
  for (const g of groupsData) {
    for (const item of g.items) {
      state.scanStatus[item.code] = false;
    }
  }
  state.resetPending = true;
  // 上传新文件时，先清理旧数据（但保留最近30天）
  cleanOldData();
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
        // ... 其他 case 保持不变（scan, release_keys, reset_all, reset_confirmed, add_batch）...
        // 因篇幅，此处省略，但实际代码中要完整保留。
        default:
          break;
      }
    } catch (err) {
      console.error('解析消息失败:', err);
      ws.send(JSON.stringify({ type: 'error', message: '服务器处理出错' }));
    }
  });
});

// ----- 启动服务器（先加载数据库）-----
loadStateFromDB().then(() => {
  app.use(express.static(path.join(__dirname, 'public')));
  const PORT = process.env.PORT || 3000;
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`服务器运行在 http://0.0.0.0:${PORT}`);
  });
});

// ========== 可选：每日定时清理（如果服务器长期运行） ==========
// 每天凌晨3点执行清理
setInterval(() => {
  cleanOldData();
  saveStateToDB();
  broadcast(); // 通知所有客户端更新
}, 24 * 60 * 60 * 1000);