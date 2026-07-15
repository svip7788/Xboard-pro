<!doctype html>
<html lang="zh-CN">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>订阅诱饵分组控制台</title>
    <style>
        :root {
            --bg: #f4f7fb; --card: #fff; --text: #172033; --muted: #68758a;
            --line: #dfe5ee; --primary: #3157d5; --danger: #c9364f;
            --success: #138a5b; --warning: #b66a09;
        }
        * { box-sizing: border-box; }
        body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
        button,input,select,textarea { font: inherit; }
        .page { width: min(1200px,calc(100% - 32px)); margin: 28px auto 56px; }
        .topbar,.row { display: flex; align-items: center; gap: 12px; }
        .topbar { justify-content: space-between; margin-bottom: 20px; }
        h1 { margin: 0; font-size: 26px; } h2 { margin: 0 0 16px; font-size: 17px; }
        .muted,.hint { color: var(--muted); } .back { color: var(--primary); text-decoration: none; }
        .grid { display: grid; grid-template-columns: repeat(12,1fr); gap: 16px; }
        .card { grid-column: span 6; padding: 20px; background: var(--card); border: 1px solid var(--line); border-radius: 14px; box-shadow: 0 5px 20px rgba(22,32,51,.05); }
        .wide { grid-column: 1/-1; }
        .campaignbar { display: grid; grid-template-columns: 1fr auto auto; gap: 10px; }
        .stats { display: grid; grid-template-columns: repeat(4,1fr); gap: 12px; }
        .stat { padding: 14px; background: #f7f9fc; border-radius: 10px; }
        .stat strong { display: block; margin-top: 4px; font-size: 22px; }
        .field { margin-bottom: 16px; } label { display: block; margin-bottom: 7px; font-weight: 600; }
        input,select,textarea { width: 100%; padding: 10px 12px; color: var(--text); background: #fff; border: 1px solid var(--line); border-radius: 9px; outline: none; }
        input,select { height: 42px; } textarea { min-height: 132px; resize: vertical; }
        input:focus,select:focus,textarea:focus { border-color: var(--primary); box-shadow: 0 0 0 3px rgba(49,87,213,.12); }
        .node-toolbar { display: flex; gap: 10px; margin-bottom: 10px; }
        .node-list { max-height: 310px; overflow: auto; border: 1px solid var(--line); border-radius: 10px; }
        .node { display: flex; align-items: center; gap: 10px; padding: 10px 12px; border-bottom: 1px solid #edf0f5; cursor: pointer; }
        .node:last-child { border-bottom: 0; } .node input,.bucket input { width: 16px; height: 16px; }
        .node small { margin-left: auto; color: var(--muted); }
        .actions { display: flex; flex-wrap: wrap; gap: 10px; }
        button { padding: 9px 16px; color: #fff; background: var(--primary); border: 0; border-radius: 9px; cursor: pointer; }
        button.secondary { color: var(--text); background: #e9edf5; }
        button.danger { background: var(--danger); } button.warning { background: var(--warning); }
        button:disabled { opacity: .45; cursor: not-allowed; }
        .pill { padding: 5px 11px; border-radius: 999px; font-weight: 700; }
        .pill.on { color: var(--success); background: #dcf6eb; } .pill.off { color: var(--muted); background: #e9edf3; }
        .buckets { display: grid; grid-template-columns: repeat(auto-fit,minmax(180px,1fr)); gap: 10px; margin: 14px 0; }
        .bucket { display: flex; gap: 10px; padding: 13px; background: #f7f9fc; border: 1px solid var(--line); border-radius: 10px; }
        .bucket strong,.bucket span { display: block; } .bucket span { color: var(--muted); word-break: break-all; }
        .queue { min-height: 34px; padding: 8px 10px; background: #f7f9fc; border-radius: 8px; word-break: break-all; }
        .notice { display: none; margin-bottom: 16px; padding: 12px 14px; border-radius: 9px; }
        .notice.show { display: block; }
        .notice.error,.toast.error { color: #8b1f34; background: #fde8ed; }
        .notice.success,.toast.success { color: #096640; background: #dff7ec; }
        .toast {
            position: fixed; top: 20px; left: 50%; z-index: 9999;
            width: max-content; max-width: calc(100% - 32px); padding: 12px 18px;
            border-radius: 10px; box-shadow: 0 8px 28px rgba(22,32,51,.2);
            opacity: 0; visibility: hidden; transform: translate(-50%,-12px);
            transition: opacity .2s,transform .2s,visibility .2s; pointer-events: none;
        }
        .toast.show { opacity: 1; visibility: visible; transform: translate(-50%,0); }
        table { width: 100%; border-collapse: collapse; } th,td { padding: 9px; text-align: left; border-bottom: 1px solid var(--line); }
        @media (max-width:800px) {
            .card { grid-column: 1/-1; } .stats { grid-template-columns: repeat(2,1fr); }
            .topbar { align-items: flex-start; flex-direction: column; } .campaignbar { grid-template-columns: 1fr 1fr; }
            .campaignbar select { grid-column: 1/-1; }
        }
    </style>
</head>
<body>
<main class="page">
    <div class="topbar">
        <div>
            <h1>订阅诱饵分组控制台</h1>
            <div class="muted">多个用户组可独立运行，每个任务支持 A 至 J 多个域名</div>
        </div>
        <a class="back" href="{{ $adminUrl }}">返回管理后台</a>
    </div>

    <div id="notice" class="toast"></div>
    <div id="authWarning" class="notice error">未检测到管理后台登录令牌，请重新登录后台后打开本页。</div>

    <div class="grid">
        <section class="card wide">
            <h2>排查任务</h2>
            <div class="campaignbar">
                <select id="campaignSelect"></select>
                <button id="newCampaign" type="button">新建任务</button>
                <button id="deleteCampaign" type="button" class="danger">删除任务</button>
            </div>
        </section>

        <section class="card wide">
            <div class="topbar">
                <h2>运行状态</h2>
                <span id="statusPill" class="pill off">未创建</span>
            </div>
            <div class="stats">
                <div class="stat"><span>有效用户</span><strong id="eligibleCount">0</strong></div>
                <div class="stat"><span>当前候选</span><strong id="candidateCount">0</strong></div>
                <div class="stat"><span>分组数量</span><strong id="bucketCount">0</strong></div>
                <div class="stat"><span>当前轮次</span><strong id="round">0</strong></div>
            </div>
            <p class="hint">当前分支：<span id="activePath">根分支</span></p>
        </section>

        <section class="card">
            <h2>任务目标</h2>
            <div class="field">
                <label for="campaignName">任务名称</label>
                <input id="campaignName" placeholder="例如：客户端组排查">
            </div>
            <div class="field">
                <label for="groupSelect">用户主权限组</label>
                <select id="groupSelect"></select>
                <div id="groupHint" class="hint"></div>
            </div>
            <div class="field">
                <label for="nodeSearch">替换节点</label>
                <div class="node-toolbar">
                    <input id="nodeSearch" placeholder="搜索节点名称或协议">
                    <button id="clearNodes" type="button" class="secondary">清空</button>
                </div>
                <div id="nodeList" class="node-list"></div>
                <div class="hint">任务运行中也可增删节点，保存后立即生效。</div>
            </div>
            <button id="saveCampaign" type="button">保存任务</button>
        </section>

        <section class="card">
            <h2>本轮分组域名</h2>
            <div class="field">
                <label for="domains">每行一个域名或 IP</label>
                <textarea id="domains" placeholder="a.example.com&#10;b.example.com&#10;c.example.com"></textarea>
                <div id="domainHint" class="hint">最少 2 个，最多 10 个；首次启动后组数固定。</div>
            </div>
            <div class="actions">
                <button id="startRound" type="button">启动本轮</button>
                <button id="disableRound" type="button" class="danger">停止</button>
                <button id="resetCampaign" type="button" class="warning">重置任务</button>
            </div>
        </section>

        <section class="card wide">
            <h2>本轮分组与封锁结果 <span class="hint">（每 5 秒自动刷新）</span></h2>
            <div id="buckets" class="buckets"><span class="hint">启动任务后显示各组</span></div>
            <div class="actions">
                <button id="recordResult" type="button">记录勾选的被墙分组</button>
                <button id="recordNone" type="button" class="secondary">所有分组都未被墙</button>
            </div>
            <p class="hint">可以同时勾选 A、B、C 等多个分组，系统会分别加入待排查队列。</p>
        </section>

        <section class="card">
            <h2>分支队列</h2>
            <label>优先排查</label><div id="positiveQueue" class="queue">无</div>
            <label style="margin-top:12px">延后复查</label><div id="deferredQueue" class="queue">无</div>
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
    const token = readAdminToken();
    let meta = { groups: [], servers: [] };
    let campaigns = [];
    let current = null;
    let selectedNodeIds = new Set();
    let noticeTimer = null;
    let liveRefreshPending = false;

    const $ = id => document.getElementById(id);

    function readAdminToken() {
        for (const storage of [localStorage, sessionStorage]) {
            for (const key of ['XBOARD_ACCESS_TOKEN', 'Xboard_access_token', 'access_token']) {
                const raw = storage.getItem(key);
                if (!raw) continue;
                try {
                    const parsed = JSON.parse(raw);
                    if (typeof parsed === 'string') return parsed;
                    if (parsed && typeof parsed.value === 'string') return parsed.value;
                } catch { return raw; }
            }
        }
        return '';
    }

    async function request(path, options = {}) {
        const response = await fetch(`${API_BASE}${path}`, {
            ...options,
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
                Authorization: token.startsWith('Bearer ') ? token : `Bearer ${token}`,
                ...(options.headers || {})
            }
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || payload.status === 'fail') {
            const errors = payload.errors ? Object.values(payload.errors).flat().join('；') : '';
            throw new Error(errors || payload.message || `请求失败（${response.status}）`);
        }
        return payload.data;
    }

    function showNotice(message, type = 'success') {
        const element = $('notice');
        clearTimeout(noticeTimer);
        element.textContent = message;
        element.className = `toast show ${type}`;
        noticeTimer = setTimeout(() => element.className = 'toast', 4500);
    }

    function blankCampaign() {
        return {
            id: '', name: '', enabled: false, target_group_id: meta.groups[0]?.id || 0,
            target_server_ids: [], round: 0, bucket_count: 0, domains: [],
            active_path_label: '根分支', eligible_count: 0, candidate_count: 0,
            bucket_counts: [], exposed_counts: [], bucket_labels: [],
            positive_queue: [], deferred_queue: [], findings: []
        };
    }

    function upsertCampaign(campaign) {
        const index = campaigns.findIndex(item => item.id === campaign.id);
        if (index >= 0) campaigns[index] = campaign; else campaigns.push(campaign);
        current = campaign;
        renderCampaignSelect();
        renderCurrent();
    }

    function renderCampaignSelect() {
        const select = $('campaignSelect');
        select.textContent = '';
        if (current && !current.id) {
            const draft = document.createElement('option');
            draft.textContent = '新任务（未保存）';
            draft.value = '';
            draft.selected = true;
            select.appendChild(draft);
        }
        if (!campaigns.length) {
            if (!current?.id && !select.options.length) {
                const option = document.createElement('option');
                option.textContent = '尚未创建任务';
                option.value = '';
                select.appendChild(option);
            }
            return;
        }
        campaigns.forEach(campaign => {
            const option = document.createElement('option');
            option.value = campaign.id;
            option.textContent = `${campaign.name}${campaign.enabled ? '（运行中）' : ''}`;
            option.selected = current?.id === campaign.id;
            select.appendChild(option);
        });
    }

    function renderGroups() {
        const select = $('groupSelect');
        select.textContent = '';
        meta.groups.forEach(group => {
            const option = document.createElement('option');
            option.value = group.id;
            option.textContent = `${group.name}（${group.users_count} 人）`;
            option.selected = Number(current?.target_group_id) === Number(group.id);
            select.appendChild(option);
        });
    }

    function serversForGroup(groupId) {
        return meta.servers.filter(server =>
            (server.group_ids || []).map(Number).includes(Number(groupId))
        );
    }

    function renderNodes(filter = '') {
        const list = $('nodeList');
        const keyword = filter.trim().toLowerCase();
        const groupId = Number($('groupSelect').value || current?.target_group_id);
        const servers = serversForGroup(groupId).filter(server =>
            `${server.name} ${server.type}`.toLowerCase().includes(keyword)
        );
        list.textContent = '';
        if (!servers.length) {
            const hint = document.createElement('div');
            hint.className = 'hint';
            hint.style.padding = '12px';
            hint.textContent = keyword ? '没有匹配的节点' : '该用户组暂无可用节点';
            list.appendChild(hint);
            return;
        }
        servers.forEach(server => {
                const row = document.createElement('label');
                row.className = 'node';
                const input = document.createElement('input');
                input.type = 'checkbox';
                input.value = server.id;
                input.checked = selectedNodeIds.has(Number(server.id));
                input.addEventListener('change', () => {
                    input.checked ? selectedNodeIds.add(Number(server.id)) : selectedNodeIds.delete(Number(server.id));
                });
                const name = document.createElement('span');
                name.textContent = `${server.name}（ID ${server.id}）`;
                const type = document.createElement('small');
                type.textContent = server.type;
                row.append(input, name, type);
                list.appendChild(row);
            });
    }

    function renderBuckets() {
        const container = $('buckets');
        container.textContent = '';
        if (!current?.bucket_count) {
            const hint = document.createElement('span');
            hint.className = 'hint';
            hint.textContent = '启动任务后显示各组';
            container.appendChild(hint);
            return;
        }
        current.bucket_labels.forEach((label, index) => {
            const card = document.createElement('label');
            card.className = 'bucket';
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'positiveBucket';
            checkbox.value = index;
            checkbox.disabled = !current.enabled;
            const info = document.createElement('div');
            const title = document.createElement('strong');
            title.textContent = `${label} 组：${current.bucket_counts[index]} 人 / ${current.exposed_counts[index]} 已拉取`;
            const domain = document.createElement('span');
            domain.textContent = current.domains[index] || '等待下一轮域名';
            info.append(title, domain);
            card.append(checkbox, info);
            container.appendChild(card);
        });
    }

    function renderFindings() {
        const body = $('findings');
        body.textContent = '';
        if (!current?.findings.length) {
            const row = body.insertRow();
            const cell = row.insertCell();
            cell.colSpan = 2; cell.className = 'hint'; cell.textContent = '暂无';
            return;
        }
        current.findings.forEach(finding => {
            const row = body.insertRow();
            row.insertCell().textContent = finding.user_id;
            row.insertCell().textContent = finding.confirmations;
        });
    }

    function renderLiveStatus() {
        $('eligibleCount').textContent = current.eligible_count || 0;
        $('candidateCount').textContent = current.candidate_count || 0;
        $('bucketCount').textContent = current.bucket_count || 0;
        $('round').textContent = current.round || 0;
        $('activePath').textContent = current.active_path_label || '根分支';
        $('statusPill').textContent = current.enabled ? '运行中' : (current.id ? '已暂停' : '未创建');
        $('statusPill').className = `pill ${current.enabled ? 'on' : 'off'}`;
        $('groupSelect').disabled = current.round > 0;
        $('groupHint').textContent = current.round > 0
            ? '任务已有轮次；如需修改用户组，请先重置任务。'
            : '';
        $('positiveQueue').textContent = current.positive_queue.join(', ') || '无';
        $('deferredQueue').textContent = current.deferred_queue.join(', ') || '无';
        $('deleteCampaign').disabled = !current.id || current.enabled;
        $('saveCampaign').disabled = false;
        $('domains').disabled = current.enabled;
        $('startRound').disabled = !current.id || current.enabled;
        $('disableRound').disabled = !current.enabled;
        $('recordResult').disabled = !current.enabled;
        $('recordNone').disabled = !current.enabled;
        $('resetCampaign').disabled = !current.id || current.enabled;
        renderBuckets();
        renderFindings();
    }

    function renderCurrent() {
        current ||= blankCampaign();
        const allowedIds = new Set(
            serversForGroup(current.target_group_id).map(server => Number(server.id))
        );
        selectedNodeIds = new Set(
            (current.target_server_ids || []).map(Number).filter(id => allowedIds.has(id))
        );
        $('campaignName').value = current.name || '';
        $('domains').value = (current.domains || []).join('\n');
        $('domainHint').textContent = current.bucket_count
            ? `该任务固定为 ${current.bucket_count} 组；重置后才能修改组数。`
            : '最少 2 个，最多 10 个；首次启动后组数固定。';
        renderGroups();
        renderNodes($('nodeSearch').value);
        renderLiveStatus();
    }

    async function refresh() {
        const currentId = current?.id;
        const data = await request('/meta');
        meta = { groups: data.groups, servers: data.servers };
        campaigns = data.campaigns;
        current = campaigns.find(item => item.id === currentId) || campaigns[0] || blankCampaign();
        renderCampaignSelect();
        renderCurrent();
    }

    async function refreshLive() {
        if (liveRefreshPending || !current?.id) return;
        liveRefreshPending = true;
        try {
            const selectedId = current.id;
            campaigns = await request('/campaigns');
            const latest = campaigns.find(item => item.id === selectedId);
            if (!latest || current?.id !== selectedId) return;
            current = latest;
            renderCampaignSelect();
            renderLiveStatus();
        } finally {
            liveRefreshPending = false;
        }
    }

    $('campaignSelect').addEventListener('change', async event => {
        const selectedId = event.target.value;
        try {
            campaigns = await request('/campaigns');
            current = campaigns.find(item => item.id === selectedId) || blankCampaign();
            renderCampaignSelect();
            renderCurrent();
        } catch (error) {
            showNotice(error.message, 'error');
        }
    });
    $('newCampaign').addEventListener('click', () => {
        current = blankCampaign();
        renderCampaignSelect();
        renderCurrent();
    });
    $('nodeSearch').addEventListener('input', event => renderNodes(event.target.value));
    $('clearNodes').addEventListener('click', () => { selectedNodeIds.clear(); renderNodes($('nodeSearch').value); });
    $('groupSelect').addEventListener('change', () => {
        const groupId = Number($('groupSelect').value);
        const allowedIds = new Set(
            serversForGroup(groupId).map(server => Number(server.id))
        );
        selectedNodeIds = new Set([...selectedNodeIds].filter(id => allowedIds.has(id)));
        renderNodes($('nodeSearch').value);
    });

    $('saveCampaign').addEventListener('click', async () => {
        try {
            const campaign = await request('/campaigns', {
                method: 'POST',
                body: JSON.stringify({
                    campaign_id: current.id || null,
                    name: $('campaignName').value.trim(),
                    target_group_id: Number($('groupSelect').value),
                    target_server_ids: [...selectedNodeIds]
                })
            });
            upsertCampaign(campaign);
            showNotice('排查任务已保存');
        } catch (error) {
            await refresh().catch(() => {});
            showNotice(error.message, 'error');
        }
    });

    $('deleteCampaign').addEventListener('click', async () => {
        try {
            if (!confirm(`确认删除任务“${current.name}”？`)) return;
            campaigns = await request(`/campaigns/${encodeURIComponent(current.id)}`, { method: 'DELETE' });
            current = campaigns[0] || blankCampaign();
            renderCampaignSelect(); renderCurrent();
            showNotice('任务已删除');
        } catch (error) { showNotice(error.message, 'error'); }
    });

    $('startRound').addEventListener('click', async () => {
        try {
            if (!current.id) throw new Error('请先保存排查任务');
            const domains = $('domains').value.split(/\n+/).map(value => value.trim()).filter(Boolean);
            if (!confirm(`确认使用 ${domains.length} 个域名启动新一轮？`)) return;
            upsertCampaign(await request(`/campaigns/${encodeURIComponent(current.id)}/start`, {
                method: 'POST', body: JSON.stringify({ domains })
            }));
            showNotice(`第 ${current.round} 轮已启动`);
        } catch (error) { showNotice(error.message, 'error'); }
    });

    $('disableRound').addEventListener('click', async () => {
        try {
            if (!confirm('确认立即停止当前任务？')) return;
            upsertCampaign(await request(`/campaigns/${encodeURIComponent(current.id)}/disable`, { method: 'POST', body: '{}' }));
            showNotice('任务已停止');
        } catch (error) { showNotice(error.message, 'error'); }
    });

    $('resetCampaign').addEventListener('click', async () => {
        try {
            if (!confirm('重置会清空轮次、队列和命中记录，确认继续？')) return;
            upsertCampaign(await request(`/campaigns/${encodeURIComponent(current.id)}/reset`, { method: 'POST', body: '{}' }));
            showNotice('任务已重置');
        } catch (error) { showNotice(error.message, 'error'); }
    });

    async function submitResult(positiveBuckets) {
        upsertCampaign(await request(`/campaigns/${encodeURIComponent(current.id)}/result`, {
            method: 'POST', body: JSON.stringify({ positive_buckets: positiveBuckets })
        }));
        showNotice('结果已记录，请准备下一轮域名');
    }

    $('recordResult').addEventListener('click', async () => {
        try {
            const buckets = [...document.querySelectorAll('.positiveBucket:checked')].map(input => Number(input.value));
            if (!buckets.length) throw new Error('请勾选至少一个被墙分组');
            if (!confirm(`确认记录 ${buckets.length} 个被墙分组？`)) return;
            await submitResult(buckets);
        } catch (error) { showNotice(error.message, 'error'); }
    });
    $('recordNone').addEventListener('click', async () => {
        try {
            if (!confirm('确认所有分组都未被墙？')) return;
            await submitResult([]);
        } catch (error) { showNotice(error.message, 'error'); }
    });

    if (!token) $('authWarning').style.display = 'block';
    else {
        refresh().catch(error => showNotice(error.message, 'error'));
        setInterval(() => {
            if (!document.hidden) refreshLive().catch(() => {});
        }, 5000);
    }
</script>
</body>
</html>
