const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const mongoose = require('mongoose');
const zlib = require('zlib');
const XLSX = require('xlsx');

// ----- 连接 MongoDB（增强配置）-----
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://pukuimx_db_user:Mx147258@cluster0.mrr1ndg.mongodb.net/?appName=Cluster0';
console.log('🔑 MONGODB_URI:', MONGODB_URI.replace(/\/\/.*@/, '//<hidden>@'));

const mongooseOptions = {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  serverSelectionTimeoutMS: 10000,
  socketTimeoutMS: 45000,
  heartbeatFrequencyMS: 10000,
  maxPoolSize: 10,
  minPoolSize: 2,
  retryWrites: true,
  retryReads: true,
};

mongoose.connect(MONGODB_URI, mongooseOptions)
  .then(() => console.log('✅ MongoDB conectado'))
  .catch(err => console.error('❌ Error MongoDB inicial:', err));

mongoose.connection.on('error', (err) => console.error('❌ MongoDB error:', err));
mongoose.connection.on('disconnected', () => console.warn('⚠️ MongoDB desconectado, reconectando...'));
process.on('SIGINT', () => { mongoose.connection.close(() => process.exit(0)); });

// ----- 状态 Schema -----
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

// ========== 自动清理（保留最近30天） ==========
const KEEP_DAYS = 30;
function cleanOldData() {
  if (state.batches.length === 0) return false;
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - KEEP_DAYS);
  const cutoffStr = cutoff.toISOString().slice(0, 10);
  const originalLength = state.batches.length;
  state.batches = state.batches.filter(b => (b.createdAt || '') >= cutoffStr);
  const validKeys = new Set(state.groups.map(g => g.key));
  for (const key in mainKeyOwners) {
    if (!validKeys.has(key)) delete mainKeyOwners[key];
  }
  const changed = (originalLength !== state.batches.length);
  if (changed) console.log(`🧹 Limpieza: ${state.batches.length} lotes.`);
  return changed;
}

// ========== 防抖保存（合并 2 秒内的写入） ==========
let saveTimeout = null;
function debouncedSave() {
  if (saveTimeout) clearTimeout(saveTimeout);
  saveTimeout = setTimeout(() => {
    saveTimeout = null;
    saveStateToDB().catch(console.error);
  }, 2000);
}

// ========== 增量广播 + 节流 ==========
let messageQueue = [];
let flushTimer = null;

function sendToAll(message) {
  if (message.type === 'full_state' || message.type === 'reset_all') {
    messageQueue = [];
    if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
    const data = JSON.stringify({ type: 'state', state });
    wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
    debouncedSave();
    return;
  }

  messageQueue.push(message);
  if (!flushTimer) {
    flushTimer = setTimeout(() => {
      flushTimer = null;
      const updates = messageQueue.slice();
      messageQueue = [];
      if (updates.length === 1) {
        const data = JSON.stringify(updates[0]);
        wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
      } else {
        const data = JSON.stringify({ type: 'batch_update', updates });
        wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
      }
    }, 100);
  }
}

function broadcastFullState() {
  sendToAll({ type: 'full_state' });
}

function broadcastRecipients() {
  const data = JSON.stringify({ type: 'addresses', recipients });
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(data); });
  debouncedSave();
}

// ----- 保存状态（重试）-----
async function saveStateToDB(retries = 3) {
  let attempt = 0;
  while (attempt < retries) {
    try {
      await StateModel.updateOne({}, { state, recipients }, { upsert: true });
      return;
    } catch (err) {
      attempt++;
      console.error(`❌ 保存失败 (${attempt}/${retries}):`, err.message);
      if (attempt >= retries) return;
      await new Promise(r => setTimeout(r, 1000 * attempt));
    }
  }
}

// ----- 加载状态 -----
async function loadStateFromDB() {
  try {
    const doc = await StateModel.findOne();
    if (doc) {
      state = { ...state, ...doc.state };
      recipients = doc.recipients || [];
      console.log('📂 Estado cargado desde MongoDB');
    } else {
      await StateModel.create({ state, recipients });
    }
    const changed = cleanOldData();
    if (changed) await saveStateToDB();
  } catch (err) {
    console.error('❌ Error al cargar estado:', err);
  }
}

// ----- 业务函数 -----
function isMainCompleted(key) {
  const g = state.groups.find(gr => gr.key === key);
  if (!g) return false;
  return g.items.every(item => state.scanStatus[item.code] || false);
}

function initState(groupsData) {
  try {
    if (state.groups && state.groups.length) {
      state.historyGroups = state.historyGroups.concat(state.groups);
      for (const g of state.groups) {
        for (const item of g.items) {
          if (state.scanStatus[item.code]) {
            state.historyScanStatus[item.code] = state.scanStatus[item.code];
          }
        }
      }
    }
    state.groups = groupsData || [];
    state.selectedMainKeys = [];
    state.scanStatus = {};
    mainKeyOwners = {};
    for (const g of state.groups) {
      for (const item of g.items) {
        state.scanStatus[item.code] = false;
      }
    }
    state.resetPending = true;
    broadcastFullState();
  } catch (err) {
    console.error('❌ initState error:', err);
    broadcastFullState();
  }
}

// ===== WebSocket =====
wss.on('connection', (ws) => {
  console.log('🔌 Nuevo cliente conectado');
  ws.send(JSON.stringify({ type: 'state', state }));
  ws.send(JSON.stringify({ type: 'addresses', recipients }));

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      const deviceId = data.deviceId;

      switch (data.type) {
        case 'init': {
          if (data.recipients) { recipients = data.recipients; broadcastRecipients(); }
          initState(data.groups);
          break;
        }
        case 'scan': {
          const { code, mode } = data;
          console.log(`📥 Recibido scan: code=${code}, mode=${mode}, deviceId=${deviceId}`);
          if (mode === 'main') {
            const group = state.groups.find(g => g.key === code);
            if (!group) {
              console.warn(`❌ 主码 no encontrado: ${code}`);
              ws.send(JSON.stringify({ type: 'error', message: `❌ 未找到主码: ${code}` }));
              return;
            }
            if (isMainCompleted(code)) {
              console.warn(`⏳ 主码 ${code} 已完成`);
              ws.send(JSON.stringify({ type: 'error', message: `⏳ 主码 ${code} 已完成，无法再次添加` }));
              return;
            }
            const owner = mainKeyOwners[code];
            if (owner && owner !== deviceId) {
              console.warn(`🔒 主码 ${code} 已被 ${owner} 锁定`);
              ws.send(JSON.stringify({ type: 'error', message: `❌ 主码 ${code} 已被其他设备锁定` }));
              return;
            }
            if (!owner) {
              mainKeyOwners[code] = deviceId;
              state.selectedMainKeys.push(code);
              sendToAll({ type: 'main_added', key: code, owner: deviceId });
              ws.send(JSON.stringify({ type: 'success', message: `✅ 已添加主码: ${code} (${group.items.length} 箱)` }));
              debouncedSave();
              console.log(`✅ 主码添加成功: ${code}`);
            } else {
              const total = group.items.length;
              const scanned = group.items.filter(item => state.scanStatus[item.code]).length;
              ws.send(JSON.stringify({ type: 'success', message: `✅ 可补扫主码: ${code} (剩余 ${total - scanned} 箱)` }));
            }
          } else if (mode === 'sub') {
  console.log(`🔍 Buscando subcódigo: ${code} en keys:`, state.selectedMainKeys);
  let found = false, matchedItem = null;
  for (const key of state.selectedMainKeys) {
    const g = state.groups.find(gr => gr.key === key);
    if (!g) continue;
    const item = g.items.find(i => i.code === code);
    if (item) { found = true; matchedItem = item; break; }
    const parts = code.split('/');
    if (parts.length === 2 && parts[0] === key) {
      const idx = parseInt(parts[1]);
      if (!isNaN(idx) && idx >= 1 && idx <= g.items.length) {
        found = true;
        matchedItem = g.items[idx - 1];
        break;
      }
    }
  }
  if (!found) {
    console.warn(`❌ Subcódigo no encontrado: ${code}`);
    ws.send(JSON.stringify({ type: 'error', message: `❌ 无效子码: ${code}` }));
    return;
  }
  if (state.scanStatus[matchedItem.code]) {
    console.warn(`⏳ Subcódigo ya escaneado: ${code}`);
    ws.send(JSON.stringify({ type: 'error', message: `⏳ 子码 ${code} 已扫描过` }));
    return;
  }
  const now = new Date();
  const timeStr = now.toLocaleTimeString('es-MX', { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'America/Mexico_City' });
  state.scanStatus[matchedItem.code] = timeStr;
  const scannedCount = Object.values(state.scanStatus).filter(v => v).length;
  const total = state.selectedMainKeys.reduce((sum, k) => {
    const g = state.groups.find(gr => gr.key === k);
    return sum + (g ? g.items.length : 0);
  }, 0);

  // 快速广播：直接发送，不经过节流队列
  const subMsg = JSON.stringify({ type: 'sub_scanned', code: matchedItem.code, time: timeStr, scannedCount, total });
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(subMsg); });
  ws.send(JSON.stringify({ type: 'success', message: `✅ 已扫描: ${code} (${scannedCount}/${total})` }));

  // 立即异步保存，不延迟（替换原来的 debouncedSave()）
  setImmediate(() => saveStateToDB().catch(console.error));

  console.log(`✅ Subcódigo escaneado: ${code}, tiempo: ${timeStr}`);
}
          break;
        }
        case 'release_keys': {
          const keysToRelease = data.keys || [];
          const filteredKeys = keysToRelease.filter(k => mainKeyOwners[k] === deviceId);
          if (filteredKeys.length === 0) {
            ws.send(JSON.stringify({ type: 'error', message: '⚠️ 没有可释放的主码' }));
            break;
          }
          state.selectedMainKeys = state.selectedMainKeys.filter(k => !filteredKeys.includes(k));
          for (const key of filteredKeys) {
            delete mainKeyOwners[key];
            const g = state.groups.find(gr => gr.key === key);
            if (g) for (const item of g.items) state.scanStatus[item.code] = false;
          }
          sendToAll({ type: 'keys_released', keys: filteredKeys });
          ws.send(JSON.stringify({ type: 'success', message: `🔄 已释放: ${filteredKeys.join(', ')}` }));
          debouncedSave();
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
          broadcastFullState();
          ws.send(JSON.stringify({ type: 'success', message: '🔁 已重置所有数据' }));
          break;
        }
        case 'reset_confirmed': {
          state.resetPending = false;
          sendToAll({ type: 'reset_confirmed' });
          break;
        }
        case 'add_batch': {
          const { batch } = data;
          if (batch) {
            const now = new Date();
            batch.createdAt = now.toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' });
            state.batches.push(batch);
            sendToAll({ type: 'batch_added', batch });
            debouncedSave();
          }
          break;
        }
        default: break;
      }
    } catch (err) {
      console.error('❌ 消息解析失败:', err);
      ws.send(JSON.stringify({ type: 'error', message: '服务器处理出错' }));
    }
  });
});

// ----- 静态资源缓存 -----
app.use(express.static(path.join(__dirname, 'public'), {
  maxAge: '1d',
  etag: true,
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) res.setHeader('Cache-Control', 'no-cache');
  }
}));

// ----- 导出辅助函数 -----
function buildTxtContent(filteredGroups, addresses) {
  const lines = ['Lista de verificación', 'Generado: ' + new Date().toLocaleString()];
  if (addresses.length) lines.push('Destinatarios: ' + addresses.join(', '));
  lines.push('─'.repeat(50));
  for (const g of filteredGroups) {
    lines.push('【' + (g.type === 'fba' ? 'FBA' : 'Custom') + ': ' + g.key + '】 ' + g.items.length + ' cajas');
    for (let j = 0; j < g.items.length; j++) {
      const item = g.items[j];
      const displayCode = g.type === 'fba' ? g.key + '/' + (j + 1) : item.code;
      const scanned = state.scanStatus[item.code] || state.historyScanStatus[item.code] || false;
      lines.push('  ' + String(j + 1).padStart(3, ' ') + '. ' + (scanned ? '✅' : '⬜') + ' ' + displayCode);
    }
    lines.push('');
  }
  return lines.join('\n');
}

// 构建 Excel（按操作员、车牌、地址、日期分组，每组分一个工作表）
function buildExcelBuffer(dateFilter, addressFilter, operatorDefault, vehicleDefault) {
  // 获取所有主码（当前+历史）
  let allGroups = state.groups.concat(state.historyGroups);

  // 日期筛选
  if (dateFilter && dateFilter !== 'all') {
    const validKeys = new Set();
    state.batches.forEach(b => { if (b.createdAt === dateFilter) b.keys.forEach(k => validKeys.add(k)); });
    allGroups = allGroups.filter(g => validKeys.has(g.key));
  }

  // 地址筛选
  if (addressFilter && addressFilter.length > 0) {
    allGroups = allGroups.filter(g => g.recipients && g.recipients.some(r => addressFilter.includes(r)));
  }

  if (allGroups.length === 0) return null;

  // 查找主码所属批次
  function findBatchForKey(key) {
    for (const b of state.batches) {
      if (b.keys.includes(key)) return b;
    }
    return null;
  }

  // 分组：按 operator|vehicle|addresses|date
  const groupsMap = {};
  for (const g of allGroups) {
    const batch = findBatchForKey(g.key);
    const op = batch ? (batch.operator || '') : (operatorDefault || '');
    const veh = batch ? (batch.vehicle || '') : (vehicleDefault || '');
    const addr = batch ? (batch.addresses || []) : (addressFilter || []);
    const addrKey = addr.slice().sort().join(',');
    const dateKey = batch ? (batch.createdAt || 'unknown') : 'unknown';
    const groupKey = op + '|' + veh + '|' + addrKey + '|' + dateKey;
    if (!groupsMap[groupKey]) {
      groupsMap[groupKey] = {
        operator: op,
        vehicle: veh,
        addresses: addrKey ? addrKey.split(',') : [],
        date: dateKey,
        groups: [],
        batch: batch // 用于统一填充卡板名（若同一组有多个批次，取第一个）
      };
    }
    groupsMap[groupKey].groups.push(g);
  }

  const wb = XLSX.utils.book_new();
  let sheetIndex = 1;
  for (const key in groupsMap) {
    const group = groupsMap[key];
    const rows = [];
    const now = new Date();
    const exportDate = now.toLocaleDateString('en-CA', { timeZone: 'America/Mexico_City' }) + ' ' +
                       now.toLocaleTimeString('es-MX', { hour: '2-digit', minute: '2-digit', timeZone: 'America/Mexico_City' });

    rows.push(['扫描员Escaneador:', group.operator || '']);
    rows.push(['车牌Placa:', group.vehicle || '']);
    rows.push(['导出日期Fecha:', exportDate]);
    rows.push(['地址Direccion de entrega:', group.addresses.join(', ')]);
    rows.push([]);
    rows.push(['#', '分组类型/Tipo', '分组Key/Guia', '子项序号/No.', '子项条码/Codigo', '已扫描/Escaneado', '箱数/Cajas', '卡板/Tarima', '时间/Hora']);
    
     const batchMap = {};
    for (const g of group.groups) {
        const batch = findBatchForKey(g.key);
        const batchName = batch ? (batch.name || 'Tarima') : 'Sin Tarima';
        if (!batchMap[batchName]) batchMap[batchName] = [];
        batchMap[batchName].push(g);
    }
    const sortedBatchNames = Object.keys(batchMap).sort((a, b) => {
        const numA = parseInt(a.replace(/[^0-9]/g, '')) || 0;
        const numB = parseInt(b.replace(/[^0-9]/g, '')) || 0;
        return numA - numB;
    });
    const sortedGroups = [];
    for (const batchName of sortedBatchNames) {
        sortedGroups.push(...batchMap[batchName]);
    }
    group.groups = sortedGroups;

    let idx = 0;
    for (const g of group.groups) {
      const typeLabel = g.type === 'fba' ? 'FBA货件' : '自定义';
      const totalBoxes = g.items.length;
      const totalText = '共' + totalBoxes + '箱/Total ' + totalBoxes + ' ' + (totalBoxes === 1 ? 'caja' : 'cajas');
      idx++;
      rows.push([idx, typeLabel, g.key, '', totalText, '', '']);

      // 子码行
      for (let j = 0; j < g.items.length; j++) {
        const item = g.items[j];
        const displayCode = g.type === 'fba' ? g.key + '/' + (j + 1) : item.code;
        const scanned = state.scanStatus[item.code] || state.historyScanStatus[item.code] || false;
        const scanTime = scanned && typeof scanned === 'string' ? scanned : (scanned ? '-' : '');
        // 获取该主码对应的批次名称
        const batch = findBatchForKey(g.key);
        const tarimaName = batch ? (batch.name || 'Tarima') : 'Sin Tarima';
        rows.push(['', '', '', j + 1, displayCode, scanned ? '是/Si' : '否/No', scanned ? 1 : 0, tarimaName, scanTime]);
      }
    }

    const ws = XLSX.utils.aoa_to_sheet(rows);
    ws['!cols'] = [{ wch: 5 }, { wch: 18 }, { wch: 15 }, { wch: 12 }, { wch: 20 }, { wch: 18 }, { wch: 12 }, { wch: 15 }, { wch: 15 }];
    const cleanVehicle = group.vehicle.replace(/[\\:*?/\[\]]/g, '');
    const sheetName = sheetIndex + '_' + (cleanVehicle || 'vehiculo') + '_' + (group.date || 'nodate');
    XLSX.utils.book_append_sheet(wb, ws, sheetName.slice(0, 31));
    sheetIndex++;
  }

  return XLSX.write(wb, { bookType: 'xlsx', type: 'buffer' });
}

// ----- 导出路由（支持 gzip 压缩）-----
app.use(express.json());

app.post('/export/:format', (req, res) => {
  const format = req.params.format; // 'txt' 或 'excel'
  const { date, addresses, operator, vehicle } = req.body;

  if (!['txt', 'excel'].includes(format)) {
    return res.status(400).json({ error: 'Formato no soportado' });
  }

  // 获取所有数据（但 buildExcelBuffer 内部会处理筛选）
  let fileBuffer, contentType, filename;

  try {
    if (format === 'txt') {
      // 简易 TXT 导出（复用之前逻辑，但为了简单，我们直接调用 buildExcelBuffer 的部分逻辑？不，TXT 我们简单处理）
      // 这里为了节省篇幅，略去 TXT 的详细实现（但用户主要要 Excel，所以重点关注 Excel）
      // 可以复用之前 txt 代码，或者暂不实现，但用户要求导出 TXT，我们一并提供。
      // 简便起见，我们调用 buildTxtContent 函数（需要补充该函数，稍后添加）
      // 先返回错误提示，但为了完整性，我加上一个简单的 TXT 生成
      const allGroups = state.groups.concat(state.historyGroups);
      // 筛选...
      let filtered = allGroups;
      if (date && date !== 'all') {
        const validKeys = new Set();
        state.batches.forEach(b => { if (b.createdAt === date) b.keys.forEach(k => validKeys.add(k)); });
        filtered = filtered.filter(g => validKeys.has(g.key));
      }
      if (addresses && addresses.length) {
        filtered = filtered.filter(g => g.recipients && g.recipients.some(r => addresses.includes(r)));
      }
      const text = buildTxtContent(filtered, addresses || []);
      fileBuffer = Buffer.from(text, 'utf-8');
      contentType = 'text/plain';
      filename = `Verificacion_${date || 'all'}.txt`;
    } else {
      // Excel
      const excelBuffer = buildExcelBuffer(date, addresses || [], operator || '', vehicle || '');
      if (!excelBuffer) {
        return res.status(404).json({ error: 'No hay datos para exportar' });
      }
      fileBuffer = excelBuffer;
      contentType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
      filename = `Verificacion_${date || 'all'}.xlsx`;
    }
  } catch (err) {
    console.error('❌ Error generando archivo:', err);
    return res.status(500).json({ error: 'Error al generar el archivo' });
  }

  // gzip 压缩
  zlib.gzip(fileBuffer, (err, compressed) => {
    if (err) {
      console.error('❌ Error comprimiendo:', err);
      return res.status(500).json({ error: 'Error al comprimir' });
    }
    res.setHeader('Content-Encoding', 'gzip');
    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Content-Length', compressed.length);
    res.send(compressed);
  });
});

// ----- 启动服务器 -----
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ 服务器运行在 http://0.0.0.0:${PORT}`);
});

loadStateFromDB().then(() => {
  broadcastFullState();
  broadcastRecipients();
}).catch(console.error);

setInterval(() => {
  const changed = cleanOldData();
  if (changed) { debouncedSave(); broadcastFullState(); }
}, 24 * 60 * 60 * 1000);
