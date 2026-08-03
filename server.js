const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const mongoose = require('mongoose');

// ----- 连接 MongoDB（增强配置）-----
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://pukuimx_db_user:Mx147258@cluster0.mrr1ndg.mongodb.net/?appName=Cluster0';
console.log('🔑 MONGODB_URI:', MONGODB_URI.replace(/\/\/.*@/, '//<hidden>@'));

const mongooseOptions = {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  serverSelectionTimeoutMS: 10000,      // 10 秒选择服务器超时
  socketTimeoutMS: 45000,              // 45 秒套接字超时
  heartbeatFrequencyMS: 10000,         // 每 10 秒发送心跳
  maxPoolSize: 10,
  minPoolSize: 2,
  retryWrites: true,
  retryReads: true,
};

mongoose.connect(MONGODB_URI, mongooseOptions)
  .then(() => console.log('✅ MongoDB conectado'))
  .catch(err => console.error('❌ Error MongoDB inicial:', err));

mongoose.connection.on('error', (err) => {
  console.error('❌ MongoDB conexión error:', err);
});
mongoose.connection.on('disconnected', () => {
  console.warn('⚠️ MongoDB desconectado, intentando reconectar...');
});
process.on('SIGINT', () => {
  mongoose.connection.close(() => {
    console.log('🔌 MongoDB desconectado por apagado');
    process.exit(0);
  });
});

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

// 默认状态
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
    return false; // 没有需要清理的数据
  }

  const now = new Date();
  const cutoff = new Date(now);
  cutoff.setDate(cutoff.getDate() - KEEP_DAYS);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  const originalLength = state.batches.length;
  const remainingBatches = state.batches.filter(b => (b.createdAt || '') >= cutoffStr);
  state.batches = remainingBatches;

  // 清理无效的主码拥有者（如果主码已不在 groups 中）
  const validKeys = new Set(state.groups.map(g => g.key));
  for (const key in mainKeyOwners) {
    if (!validKeys.has(key)) {
      delete mainKeyOwners[key];
    }
  }

  const changed = (originalLength !== state.batches.length);
  if (changed) {
    console.log(`🧹 Limpieza completada: se conservan ${state.batches.length} lotes.`);
  }
  return changed;
}
// ============================================================

// ----- 保存状态（带重试机制）-----
async function saveStateToDB(retries = 3) {
  let attempt = 0;
  while (attempt < retries) {
    try {
      await StateModel.updateOne(
        {},
        { state, recipients },
        { upsert: true }
      );
      return; // 成功则退出
    } catch (err) {
      attempt++;
      console.error(`❌ Error al guardar estado (intento ${attempt}/${retries}):`, err.message);
      if (attempt >= retries) {
        console.error('❌ Falló guardar estado después de múltiples intentos');
        // 可选：写入本地紧急备份（如需可取消注释）
        // try { fs.writeFileSync('./state_backup.json', JSON.stringify({ state, recipients })); } catch(e) {}
        return;
      }
      // 延迟重试：1s, 2s, 3s
      await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
    }
  }
}

// ========== 广播节流（合并短时间内的多次广播） ==========
let broadcastPending = false;
function broadcast() {
  if (!broadcastPending) {
    broadcastPending = true;
    setImmediate(() => {
      broadcastPending = false;
      const data = JSON.stringify({ type: 'state', state });
      wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(data);
      });
      // 异步保存，捕获错误
      saveStateToDB().catch(err => console.error('❌ Broadcast save error:', err));
    });
  }
}

function broadcastRecipients() {
  const data = JSON.stringify({ type: 'addresses', recipients });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  });
  // 保存数据（但地址变更通常伴随 init，init 会触发 broadcast，所以这里可以不重复保存）
  // 但为保险起见，仍异步保存一次（可能会与 broadcast 重复，但可接受）
  saveStateToDB().catch(err => console.error('❌ BroadcastRecipients save error:', err));
}
// ======================================================

// ----- 从数据库加载状态 -----
async function loadStateFromDB() {
  try {
    const doc = await StateModel.findOne();
    if (doc) {
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
    // 清理旧数据并仅在变更时保存
    const changed = cleanOldData();
    if (changed) {
      await saveStateToDB();
    }
  } catch (err) {
    console.error('❌ Error al cargar estado:', err);
  }
}

// ----- 业务函数（保持不变） -----
function isMainCompleted(key) {
  const g = state.groups.find(gr => gr.key === key);
  if (!g) return false;
  return g.items.every(item => state.scanStatus[item.code] || false);
}

// ===== 核心：上传新文件时归档旧数据及其扫描状态 =====
function initState(groupsData) {
  try {
    if (!state.scanStatus) state.scanStatus = {};

    // ---- 归档旧主码及扫描状态 ----
    if (state.groups && state.groups.length > 0) {
      state.historyGroups = state.historyGroups.concat(state.groups);
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

    broadcast();
  } catch (err) {
    console.error('❌ initState error:', err);
    broadcast(); // 即使出错也广播
  }
}
// =====================================================

// ----- WebSocket 处理（完全保留原有逻辑）-----
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
            // 记录扫描时间（墨西哥城时区）
            const now = new Date();
            const timeStr = now.toLocaleTimeString('es-MX', { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'America/Mexico_City' });
            state.scanStatus[matchedItem.code] = timeStr;
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
          if (filteredKeys.length === 0) {
            ws.send(JSON.stringify({ type: 'error', message: '⚠️ 没有可释放的主码' }));
            break;
          }

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

// ----- 启动服务器（先启动，后加载数据）-----
app.use(express.static(path.join(__dirname, 'public')));
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ 服务器运行在 http://0.0.0.0:${PORT}`);
});

// 异步加载数据库，完成后广播初始状态
loadStateFromDB().then(() => {
  broadcast();
  broadcastRecipients();
}).catch(err => console.error('❌ 数据库加载失败:', err));

// 每日定时清理（凌晨3点）
setInterval(() => {
  console.log('⏰ Ejecutando limpieza programada...');
  const changed = cleanOldData();
  if (changed) {
    saveStateToDB().catch(err => console.error('❌ Limpieza save error:', err));
    broadcast();
  }
}, 24 * 60 * 60 * 1000);