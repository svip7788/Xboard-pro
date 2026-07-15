<!doctype html>
<html lang="zh-CN">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>订阅诱饵分组控制台</title>
    <style>
        :root {
            color-scheme: light;
            --bg: #f4f7fb;
            --card: #fff;
            --text: #162033;
            --muted: #68758a;
            --line: #dfe5ee;
            --primary: #3157d5;
            --danger: #c9364f;
            --success: #138a5b;
            --warning: #b66a09;
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            background: var(--bg);
            color: var(--text);
            font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }
        button, input, select { font: inherit; }
        .page { width: min(1180px, calc(100% - 32px)); margin: 28px auto 56px; }
        .topbar { display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 20px; }
        h1 { margin: 0; font-size: 26px; }
        h2 { margin: 0 0 16px; font-size: 17px; }
        .subtitle, .hint { color: var(--muted); }
        .back { color: var(--primary); text-decoration: none; }
        .grid { display: grid; grid-template-columns: repeat(12, 1fr); gap: 16px; }
        .card {
            grid-column: span 6;
            padding: 20px;
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 14px;
            box-shadow: 0 5px 20px rgba(22, 32, 51, .05);
        }
        .wide { grid-column: 1 / -1; }
        .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
        .stat { padding: 14px; background: #f7f9fc; border-radius: 10px; }
        .stat strong { display: block; margin-top: 4px; font-size: 22px; }
        .field { margin-bottom: 16px; }
        label { display: block; margin-bottom: 7px; font-weight: 600; }
        input, select {
            width: 100%;
            height: 42px;
            padding: 0 12px;
            color: var(--text);
            background: #fff;
            border: 1px solid var(--line);
            border-radius: 9px;
            outline: none;
        }
        input:focus, select:focus { border-color: var(--primary); box-shadow: 0 0 0 3px rgba(49, 87, 213, .12); }
        .node-toolbar { display: flex; gap: 10px; margin-bottom: 10px; }
        .node-list {
            max-height: 310px;
            overflow: auto;
            border: 1px solid var(--line);
            border-radius: 10px;
        }
        .node {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 12px;
            border-bottom: 1px solid #edf0f5;
            cursor: pointer;
        }
        .node:last-child { border-bottom: 0; }
        .node input { width: 16px; height: 16px; }
        .node small { margin-left: auto; color: var(--muted); }
        .actions { display: flex; flex-wrap: wrap; gap: 10px; }
        button {
            padding: 9px 16px;
            color: #fff;
            background: var(--primary);
            border: 0;
            border-radius: 9px;
            cursor: pointer;
        }
        button.secondary { color: var(--text); background: #e9edf5; }
        button.danger { background: var(--danger); }
        button.warning { background: var(--warning); }
        button:disabled { opacity: .45; cursor: not-allowed; }
        .pill { padding: 5px 11px; border-radius: 999px; font-weight: 700; }
        .pill.on { color: var(--success); background: #dcf6eb; }
        .pill.off { color: var(--muted); background: #e9edf3; }
        .queue { min-height: 34px; padding: 8px 10px; background: #f7f9fc; border-radius: 8px; word-break: break-all; }
        .notice { display: none; margin-bottom: 16px; padding: 12px 14px; border-radius: 9px; }
        .notice.show { display: block; }
        .notice.error { color: #8b1f34; background: #fde8ed; }
        .notice.success { color: #096640; background: #dff7ec; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 9px; text-align: left; border-bottom: 1px solid var(--line); }
        #authWarning { display: none; }
        @media (max-width: 800px) {
            .card { grid-column: 1 / -1; }
            .stats { grid-template-columns: repeat(2, 1fr); }
            .topbar { align-items: flex-start; flex-direction: column; }
        }
    </style>
</head>
<body>
<main class="page">
    <div class="topbar">
        <div>
            <h1>订阅诱饵分组控制台</h1>
            <div class="subtitle">白银用户动态 A/B 分组，不修改节点记录</div>
        </div>
        <a class="back" href="{{ $adminUrl }}">返回管理后台</a>
    </div>

    <div id="notice" class="notice"></div>
    <div id="authWarning" class="notice error">
        未检测到管理后台登录令牌。请从已登录的管理后台当前标签页打开本页面。
    </div>

    <div class="grid">
        <section class="card wide">
            <div class="topbar">
                <h2>运行状态</h2>
                <span id="statusPill" class="pill off">读取中</span>
            </div>
            <div class="stats">
                <div class="stat"><span>有效用户</span><strong id="eligibleCount">-</strong></div>
                <div class="stat"><span>当前候选</span><strong id="candidateCount">-</strong></div>
                <div class="stat"><span>A 组 / 已拉取</span><strong id="countA">-</strong></div>
                <div class="stat"><span>B 组 / 已拉取</span><strong id="countB">-</strong></div>
            </div>
            <p class="hint">
                轮次：<span id="round">-</span>　
                当前分支：<span id="prefix">-</span>
            </p>
        </section>

        <section class="card">
            <h2>目标用户与节点</h2>
            <div class="field">
                <label for="groupSelect">用户主权限组</label>
                <select id="groupSelect"></select>
            </div>
            <div class="field">
                <label for="nodeSearch">替换节点</label>
                <div class="node-toolbar">
                    <input id="nodeSearch" placeholder="搜索节点名称或协议">
                    <button id="clearNodes" type="button" class="secondary">清空</button>
                </div>
                <div id="nodeList" class="node-list"></div>
            </div>
            <div class="actions">
                <button id="saveConfig" type="button">保存目标配置</button>
            </div>
        </section>

        <section class="card">
            <h2>启动新一轮</h2>
            <div class="field">
                <label for="hostA">A 组新域名或 IP</label>
                <input id="hostA" autocomplete="off" placeholder="a-new.example.com">
            </div>
            <div class="field">
                <label for="hostB">B 组新域名或 IP</label>
                <input id="hostB" autocomplete="off" placeholder="b-new.example.com">
            </div>
            <div class="actions">
                <button id="startRound" type="button">启动本轮</button>
                <button id="disableRound" type="button" class="danger">立即停止</button>
            </div>
            <p class="hint">启动前必须确认两个域名指向不同 IP，且都能转发现有节点。</p>
        </section>

        <section class="card wide">
            <h2>记录封锁结果</h2>
            <div class="actions">
                <button data-result="a" class="resultButton">只有 A 被墙</button>
                <button data-result="b" class="resultButton">只有 B 被墙</button>
                <button data-result="both" class="resultButton danger">A、B 都被墙</button>
                <button data-result="none" class="resultButton secondary">都未被墙</button>
            </div>
            <p class="hint">记录结果后会自动暂停域名替换，并选择下一待排查分支。</p>
        </section>

        <section class="card">
            <h2>分支队列</h2>
            <label>优先排查</label>
            <div id="positiveQueue" class="queue">无</div>
            <label style="margin-top:12px">延后复查</label>
            <div id="deferredQueue" class="queue">无</div>
        </section>

        <section class="card">
            <h2>单用户候选</h2>
            <table>
                <thead><tr><th>用户 ID</th><th>命中次数</th></tr></thead>
                <tbody id="findings"><tr><td colspan="2" class="hint">暂无</td></tr></tbody>
            </table>
        </section>
    </div>
</main>

<script>
    const API_BASE = @json($apiBase);
    const token = sessionStorage.getItem('access_token') || localStorage.getItem('access_token');
    let meta = { groups: [], servers: [], config: {} };
    let status = null;
    let selectedNodeIds = new Set();

    const $ = (id) => document.getElementById(id);

    function authHeader() {
        if (!token) return {};
        return { Authorization: token.startsWith('Bearer ') ? token : `Bearer ${token}` };
    }

    async function request(path, options = {}) {
        const response = await fetch(`${API_BASE}${path}`, {
            ...options,
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
                ...authHeader(),
                ...(options.headers || {})
            }
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || payload.status === 'fail') {
            const errors = payload.errors
                ? Object.values(payload.errors).flat().join('；')
                : '';
            throw new Error(errors || payload.message || `请求失败（${response.status}）`);
        }
        return payload.data;
    }

    function showNotice(message, type = 'success') {
        const element = $('notice');
        element.textContent = message;
        element.className = `notice show ${type}`;
        window.setTimeout(() => element.className = 'notice', 4500);
    }

    function selectedServerIds() {
        return [...selectedNodeIds];
    }

    function renderNodes(filter = '') {
        const keyword = filter.trim().toLowerCase();
        const list = $('nodeList');
        list.textContent = '';

        meta.servers
            .filter(server => `${server.name} ${server.type}`.toLowerCase().includes(keyword))
            .forEach(server => {
                const row = document.createElement('label');
                row.className = 'node';
                const input = document.createElement('input');
                input.type = 'checkbox';
                input.className = 'nodeCheck';
                input.value = server.id;
                input.checked = selectedNodeIds.has(Number(server.id));
                input.addEventListener('change', () => {
                    const id = Number(server.id);
                    input.checked ? selectedNodeIds.add(id) : selectedNodeIds.delete(id);
                });
                const name = document.createElement('span');
                name.textContent = `${server.name}（ID ${server.id}）`;
                const type = document.createElement('small');
                type.textContent = server.type;
                row.append(input, name, type);
                list.appendChild(row);
            });
    }

    function renderMeta() {
        selectedNodeIds = new Set((meta.config.target_server_ids || []).map(Number));
        const select = $('groupSelect');
        select.textContent = '';
        meta.groups.forEach(group => {
            const option = document.createElement('option');
            option.value = group.id;
            option.textContent = `${group.name}（${group.users_count} 人）`;
            option.selected = Number(meta.config.target_group_id) === Number(group.id);
            select.appendChild(option);
        });
        renderNodes($('nodeSearch').value);
    }

    function renderStatus(nextStatus) {
        status = nextStatus;
        $('statusPill').textContent = status.enabled ? '运行中' : '已暂停';
        $('statusPill').className = `pill ${status.enabled ? 'on' : 'off'}`;
        $('eligibleCount').textContent = status.eligible_count;
        $('candidateCount').textContent = status.candidate_count;
        $('countA').textContent = `${status.group_counts.A} / ${status.exposed_counts.A}`;
        $('countB').textContent = `${status.group_counts.B} / ${status.exposed_counts.B}`;
        $('round').textContent = status.round;
        $('prefix').textContent = status.active_prefix || '根分支';
        $('positiveQueue').textContent = status.positive_queue.join(', ') || '无';
        $('deferredQueue').textContent = status.deferred_queue.join(', ') || '无';
        $('hostA').value = status.host_a || '';
        $('hostB').value = status.host_b || '';
        $('disableRound').disabled = !status.enabled;
        document.querySelectorAll('.resultButton').forEach(button => {
            button.disabled = !status.enabled;
        });

        const findings = $('findings');
        findings.textContent = '';
        if (!status.findings.length) {
            const row = findings.insertRow();
            const cell = row.insertCell();
            cell.colSpan = 2;
            cell.textContent = '暂无';
            cell.className = 'hint';
        } else {
            status.findings.forEach(finding => {
                const row = findings.insertRow();
                row.insertCell().textContent = finding.user_id;
                row.insertCell().textContent = finding.confirmations;
            });
        }
    }

    async function refresh() {
        const [metaData, statusData] = await Promise.all([
            request('/meta'),
            request('/status')
        ]);
        meta = metaData;
        renderMeta();
        renderStatus(statusData);
    }

    $('nodeSearch').addEventListener('input', event => renderNodes(event.target.value));
    $('clearNodes').addEventListener('click', () => {
        selectedNodeIds.clear();
        document.querySelectorAll('.nodeCheck').forEach(input => input.checked = false);
    });

    $('saveConfig').addEventListener('click', async () => {
        try {
            const serverIds = selectedServerIds();
            if (!serverIds.length) throw new Error('至少选择一个替换节点');
            const data = await request('/config', {
                method: 'POST',
                body: JSON.stringify({
                    target_group_id: Number($('groupSelect').value),
                    target_server_ids: serverIds
                })
            });
            meta.config = data.config;
            renderStatus(data.status);
            showNotice('目标用户组和节点已保存');
        } catch (error) {
            showNotice(error.message, 'error');
        }
    });

    $('startRound').addEventListener('click', async () => {
        try {
            if (!confirm('确认两个域名使用不同 IP，并且转发节点已经测试可用？')) return;
            const data = await request('/start', {
                method: 'POST',
                body: JSON.stringify({
                    host_a: $('hostA').value.trim(),
                    host_b: $('hostB').value.trim()
                })
            });
            renderStatus(data);
            showNotice(`第 ${data.round} 轮已启动`);
        } catch (error) {
            showNotice(error.message, 'error');
        }
    });

    $('disableRound').addEventListener('click', async () => {
        try {
            if (!confirm('确定立即停止域名替换？')) return;
            renderStatus(await request('/disable', { method: 'POST', body: '{}' }));
            showNotice('诱饵分组已停止');
        } catch (error) {
            showNotice(error.message, 'error');
        }
    });

    document.querySelectorAll('.resultButton').forEach(button => {
        button.addEventListener('click', async () => {
            try {
                if (!confirm(`确认记录结果：${button.textContent}？`)) return;
                const data = await request('/result', {
                    method: 'POST',
                    body: JSON.stringify({ result: button.dataset.result })
                });
                renderStatus(data);
                showNotice('结果已记录，请准备下一轮新域名和 IP');
            } catch (error) {
                showNotice(error.message, 'error');
            }
        });
    });

    if (!token) {
        $('authWarning').style.display = 'block';
    } else {
        refresh().catch(error => showNotice(error.message, 'error'));
    }
</script>
</body>
</html>
