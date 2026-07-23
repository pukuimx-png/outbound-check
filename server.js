const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

let state = {
    groups: [],
    selectedMainKeys: [],
    scanStatus: {}
};
let recipients = [];
// 记录每个主码的所有者设备ID
let mainKeyOwners = {};

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
}

function broadcastRecipients() {
    const data = JSON.stringify({ type: 'addresses', recipients });
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(data);
    });
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
    broadcast();
}

wss.on('connection', (ws) => {
    ws.send(JSON.stringify({ type: 'state', state }));
    ws.send(JSON.stringify({ type: 'addresses', recipients }));

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const deviceId = data.deviceId; // 客户端携带的设备ID

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
                        // 检查是否已完成
                        if (isMainCompleted(code)) {
                            ws.send(JSON.stringify({ type: 'error', message: `⏳ 主码 ${code} 已完成，无法再次添加` }));
                            return;
                        }
                        // 判断主码是否已被其他设备锁定
                        const owner = mainKeyOwners[code];
                        if (owner && owner !== deviceId) {
                            ws.send(JSON.stringify({ type: 'error', message: `❌ 主码 ${code} 已被其他设备锁定，不可重复扫描` }));
                            return;
                        }
                        if (!owner) {
                            // 首次添加：锁定
                            mainKeyOwners[code] = deviceId;
                            state.selectedMainKeys.push(code);
                            broadcast();
                            ws.send(JSON.stringify({ type: 'success', message: `✅ 已添加主码: ${code} (${group.items.length} 箱)` }));
                        } else {
                            // 同一设备重新添加（补扫）
                            // 计算剩余未扫箱数
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
                    // 只释放当前设备拥有的主码
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

                case 'reset_all':
                    state.selectedMainKeys = [];
                    mainKeyOwners = {};
                    for (const g of state.groups) {
                        for (const item of g.items) {
                            state.scanStatus[item.code] = false;
                        }
                    }
                    broadcast();
                    ws.send(JSON.stringify({ type: 'success', message: '🔁 已重置所有扫描记录' }));
                    break;

                default:
                    break;
            }
        } catch (err) {
            console.error('解析消息失败:', err);
            ws.send(JSON.stringify({ type: 'error', message: '服务器处理出错' }));
        }
    });
});

app.use(express.static(path.join(__dirname, 'public')));
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`服务器运行在 http://0.0.0.0:${PORT}`);
});