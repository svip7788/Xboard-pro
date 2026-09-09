<!doctype html>
<html lang="zh-CN">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>节点域名调度中心</title>
    <style>
        :root{--bg:#f8fafc;--card:#fff;--text:#1e293b;--muted:#64748b;--line:#e2e8f0;--primary:#2563eb;--danger:#dc2626;--success:#16a34a;--warning:#d97706}
        *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
        button,input,select,textarea{font:inherit}button{padding:8px 16px;font-weight:500;color:#fff;background:var(--primary);border:0;border-radius:6px;cursor:pointer}button:hover{opacity:.9}button:disabled{opacity:.5;cursor:not-allowed}
        button.secondary{color:var(--text);background:#fff;border:1px solid var(--line)}button.secondary:hover{background:var(--bg)}button.danger{background:var(--danger)}button.warning{background:var(--warning)}button.success{background:var(--success)}button.small{padding:5px 10px;font-size:12px}
        input,select,textarea{width:100%;padding:9px 12px;color:var(--text);background:#fff;border:1px solid var(--line);border-radius:6px;outline:none}input,select{height:40px}textarea{min-height:80px;resize:vertical}
        input:focus,select:focus,textarea:focus{border-color:var(--primary);box-shadow:0 0 0 2px rgba(37,99,235,.1)}
        .page{max-width:1280px;margin:0 auto;padding:24px 20px 60px}.topbar,.row,.actions{display:flex;align-items:center;gap:10px}.topbar{justify-content:space-between;margin-bottom:20px}.actions{flex-wrap:wrap}
        h1{margin:0;font-size:22px;font-weight:600}h2{margin:0 0 16px;font-size:15px;font-weight:600;color:var(--text)}h3{margin:0 0 8px;font-size:14px}.muted,.hint{color:var(--muted);font-size:13px}.back{color:var(--muted);text-decoration:none;font-size:13px}.back:hover{color:var(--primary)}
        .grid{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}.card{grid-column:span 6;padding:20px;background:var(--card);border:1px solid var(--line);border-radius:8px}.wide{grid-column:1/-1}.third{grid-column:span 4}
        .field{margin-bottom:14px}.field label{display:block;margin-bottom:6px;font-weight:500;font-size:13px}.campaignbar{display:grid;grid-template-columns:1fr auto auto;gap:9px}
        .group-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px}.group-option{display:flex!important;align-items:center;gap:8px;padding:8px 10px;margin:0!important;background:var(--bg);border:1px solid var(--line);border-radius:6px;cursor:pointer;font-size:13px}.group-option input{width:16px;height:16px;margin:0;flex:none}.group-list.cols-5{grid-template-columns:repeat(5,1fr)}.group-list.cols-5 .group-option{min-width:0}.group-list.cols-5 .group-option span{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .stats{display:grid;grid-template-columns:repeat(6,1fr);gap:12px}.stat{padding:14px;background:var(--bg);border-radius:6px;text-align:center}.stat span{font-size:12px;color:var(--muted)}.stat strong{display:block;margin-top:4px;font-size:22px;font-weight:600}
        .pill{display:inline-block;padding:3px 8px;border-radius:4px;font-size:12px;font-weight:500}.pill.on{color:var(--success);background:#dcfce7}.pill.off{color:var(--muted);background:#f1f5f9}.pill.warn{color:var(--warning);background:#fef3c7}.pill.bad{color:var(--danger);background:#fee2e2}
        .pool-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px}.pool{padding:16px;background:var(--bg);border:1px solid var(--line);border-radius:6px}.pool:hover{border-color:#cbd5e1}.pool-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}.pool-head strong{font-size:14px}.pool .host{font-family:ui-monospace,monospace;font-size:12px;color:var(--muted);padding:8px 10px;background:#fff;border-radius:4px;margin:10px 0;word-break:break-all}.pool .meta{font-size:12px;color:var(--muted)}.host-tools{display:flex;align-items:center;flex-wrap:wrap;gap:8px}.ping-result{font-size:12px;font-weight:600}.ping-result.ok{color:var(--success)}.ping-result.warn{color:var(--warning)}.ping-result.bad{color:var(--danger)}
        .node-list{max-height:260px;overflow:auto;border:1px solid var(--line);border-radius:6px}.node{display:flex;align-items:center;gap:9px;padding:8px 12px;border-bottom:1px solid var(--line);cursor:pointer;font-size:13px}.node:last-child{border:0}.node:hover{background:var(--bg)}.node input{width:16px;height:16px}.node small{margin-left:auto;color:var(--muted)}
        table{width:100%;border-collapse:collapse;font-size:13px}th,td{padding:10px 12px;text-align:left}th{font-weight:500;color:var(--muted);font-size:12px;border-bottom:1px solid var(--line)}td{border-bottom:1px solid var(--line)}tr:hover td{background:var(--bg)}.scroll{max-height:350px;overflow:auto}
        .empty{padding:20px;text-align:center;color:var(--muted);background:var(--bg);border-radius:6px;font-size:13px}.notice{display:none;padding:12px;border-radius:6px}.notice.error{color:#991b1b;background:#fee2e2}
        .toast{position:fixed;top:18px;left:50%;z-index:10020;max-width:calc(100% - 32px);padding:12px 18px;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,.15);opacity:0;visibility:hidden;transform:translate(-50%,-10px);transition:.2s;pointer-events:none;font-size:13px}.toast.show{opacity:1;visibility:visible;transform:translate(-50%,0)}.toast.success{color:#166534;background:#dcfce7}.toast.error{color:#991b1b;background:#fee2e2}
        .overlay,.modal{display:none;position:fixed;inset:0;z-index:10000;align-items:center;justify-content:center;padding:16px;background:rgba(0,0,0,.4)}.overlay.show,.modal.show{display:flex}.loading-box{display:flex;gap:10px;align-items:center;padding:14px 20px;background:#fff;border-radius:8px;font-size:13px}.spinner{width:18px;height:18px;border:2px solid var(--line);border-top-color:var(--primary);border-radius:50%;animation:spin .7s linear infinite}.modal{z-index:10010}.modal-card{width:min(1000px,100%);max-height:88vh;overflow:auto;padding:24px;background:#fff;border-radius:10px}.modal-head{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:16px}.modal-tools{display:grid;grid-template-columns:1fr auto;gap:9px;margin:12px 0}.pagination{display:flex;align-items:center;justify-content:flex-end;gap:10px;margin-top:12px}@keyframes spin{to{transform:rotate(360deg)}}
        .split{display:grid;grid-template-columns:1fr 1fr;gap:14px}.version{font-family:ui-monospace,monospace;font-size:12px}
        /* 树形排查 - 层次化样式 */
        .tree-list{display:flex;flex-direction:column;gap:0;position:relative}
        .tree-node{position:relative;padding:14px 16px;background:#fff;border:1px solid var(--line);border-radius:6px;margin-bottom:8px}
        .tree-node::before{content:'';position:absolute;left:-20px;top:24px;width:16px;height:2px;background:var(--line)}
        .tree-node::after{content:'';position:absolute;left:-20px;top:-8px;bottom:50%;width:2px;background:var(--line)}
        .tree-node:first-child::after{display:none}
        .tree-node[style*="margin-left: 0"]::before,.tree-node[style*="margin-left: 0"]::after{display:none}
        .tree-node[style*="margin-left: 24px"]{border-left:3px solid #93c5fd}
        .tree-node[style*="margin-left: 48px"]{border-left:3px solid #a5b4fc}
        .tree-node[style*="margin-left: 72px"]{border-left:3px solid #c4b5fd}
        .tree-node[style*="margin-left: 96px"]{border-left:3px solid #d8b4fe}
        .tree-node .pool-head{margin-bottom:8px}.tree-node .pool-head strong{font-size:14px}
        .tree-node .meta{color:var(--muted);font-size:12px;margin-top:8px}
        .tree-node .actions{margin-top:10px}
        .tree-depth-0{border-left:4px solid var(--primary);background:#f8fafc}
        .tree-depth-1{margin-left:28px!important;border-left:3px solid #60a5fa}
        .tree-depth-2{margin-left:56px!important;border-left:3px solid #818cf8}
        .tree-depth-3{margin-left:84px!important;border-left:3px solid #a78bfa}
        .tree-depth-4{margin-left:112px!important;border-left:3px solid #c084fc}
        .branch-fields{display:grid;gap:9px}.branch-row{display:grid;grid-template-columns:160px 1fr;gap:9px}
        /* 下拉菜单 */
        .dropdown{position:relative;display:inline-block}
        .dropdown-menu{display:none;position:absolute;right:0;top:100%;margin-top:6px;background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px;min-width:180px;box-shadow:0 4px 12px rgba(0,0,0,.1);z-index:100}
        .dropdown.open .dropdown-menu{display:block}
        /* 可折叠区域 */
        .collapsible .collapse-header{display:flex;justify-content:space-between;align-items:center;cursor:pointer;user-select:none;margin:-20px -20px 0;padding:16px 20px;border-radius:8px 8px 0 0;transition:background .15s}
        .collapsible .collapse-header:hover{background:var(--bg)}
        .collapsible .collapse-header h2{margin:0;display:flex;align-items:center;gap:8px}
        .collapse-icon{color:var(--muted);font-size:12px;transition:transform .2s}
        .collapsible.collapsed .collapse-icon{transform:rotate(-90deg)}
        .collapsible.collapsed .collapse-body{display:none}
        .collapsible .collapse-body{margin-top:12px}
        .badge{font-size:11px;padding:2px 8px;border-radius:10px;font-weight:500}
        .badge.on{background:#dcfce7;color:#166534}.badge.off{background:#fef3c7;color:#92400e}.badge.init{background:#e0e7ff;color:#3730a3}
        /* 树形排查区域 */
        .pool-section{grid-column:span 8}.edit-pool{grid-column:span 4}.tree-scroll{padding-right:0}
        .tree-scroll::-webkit-scrollbar{width:6px}.tree-scroll::-webkit-scrollbar-track{background:var(--bg);border-radius:3px}.tree-scroll::-webkit-scrollbar-thumb{background:var(--line);border-radius:3px}.tree-scroll::-webkit-scrollbar-thumb:hover{background:var(--muted)}
        /* 手机端适配 */
        @media(max-width:900px){
            .card,.third,.pool-section,.edit-pool{grid-column:1/-1}
            .stats{grid-template-columns:repeat(2,1fr)}
            .split{grid-template-columns:1fr}
            .campaignbar{grid-template-columns:1fr 1fr}.campaignbar select{grid-column:1/-1}
            .topbar{align-items:flex-start;flex-direction:column;gap:8px}
            .tree-node{margin-left:0!important;border-left-width:4px!important}
            .tree-scroll{max-height:none;padding-right:0}
            .pool-grid{grid-template-columns:1fr}
            .actions{gap:6px}.actions button{flex:1;min-width:0}
            h1{font-size:20px}h2{font-size:14px}
            .card{padding:16px}
            button{padding:10px 14px}button.small{padding:8px 12px}
            .stat{padding:12px}.stat strong{font-size:18px}
        }
        @media(max-width:480px){
            .page{padding:16px 12px 40px}
            .stats{grid-template-columns:repeat(3,1fr)}
            .tree-node .actions{flex-wrap:wrap}
            .modal-card{padding:16px}
        }
    </style>
</head>
<body>
<main class="page">
    <div class="topbar">
        <div><h1>节点域名调度中心</h1><div class="muted">全量接管节点域名、分批筛查、用户池与单用户规则</div></div>
        <a class="back" href="{{ $adminUrl }}">返回管理后台</a>
    </div>
    <div id="toast" class="toast"></div>
    <div id="authWarning" class="notice error">未检测到管理后台登录令牌，请重新登录后台。</div>
    <div id="loading" class="overlay"><div class="loading-box"><span class="spinner"></span><span id="loadingText">正在加载…</span></div></div>
    <div id="usersModal" class="modal"><div class="modal-card">
        <div class="modal-head"><h2 id="usersTitle">用户列表</h2><button id="closeUsers" class="secondary">关闭</button></div>
        <div id="poolUserTools" class="modal-tools" style="display:none"><input id="poolUserSearch" placeholder="搜索用户 ID 或邮箱"><button id="searchPoolUsers">搜索</button></div>
        <div class="scroll"><table><thead><tr><th>ID</th><th>邮箱</th><th id="usersStateHead">状态</th><th>拉取次数</th><th>最近拉取</th><th id="usersActionHead">操作</th></tr></thead><tbody id="usersBody"></tbody></table></div>
        <div id="poolUserPagination" class="pagination" style="display:none"><button id="prevPoolUsers" class="secondary">上一页</button><span id="poolUserPage"></span><button id="nextPoolUsers" class="secondary">下一页</button></div>
    </div></div>
    <div id="transferModal" class="modal"><div class="modal-card" style="width:min(520px,100%)">
        <div class="modal-head"><h2 id="transferTitle">转移用户</h2><button id="closeTransfer" class="secondary">关闭</button></div>
        <p id="transferHint" class="hint"></p>
        <div class="field"><label>转入用户池</label><select id="transferTarget"></select></div>
        <button id="confirmTransfer">确认一键转移</button>
    </div></div>
    <div id="splitTreeModal" class="modal"><div class="modal-card" style="width:min(680px,100%)">
        <div class="modal-head"><h2 id="splitTreeTitle">拆分排查节点</h2><button id="closeSplitTree" class="secondary">关闭</button></div>
        <div class="field"><label>拆分组数（2–10）</label><input id="splitTreeCount" type="number" min="2" max="10" value="2"></div>
        <div id="splitTreeBranches" class="branch-fields"></div>
        <button id="confirmSplitTree" style="margin-top:14px">确认创建独立下级池</button>
    </div></div>
    <div id="mergeTreeModal" class="modal"><div class="modal-card" style="width:min(680px,100%)">
        <div class="modal-head"><h2>合并旧排查树并重新打乱</h2><button id="closeMergeTree" class="secondary">关闭</button></div>
        <p id="mergeTreeHint" class="hint"></p>
        <div class="field"><label>新排查树名称</label><input id="mergeTreeName" placeholder="例如：第 2 轮合并排查"></div>
        <div class="field"><label>重新拆分组数（2–10）</label><input id="mergeTreeCount" type="number" min="2" max="10" value="2"></div>
        <div id="mergeTreeBranches" class="branch-fields"></div>
        <button id="confirmMergeTree" style="margin-top:14px">确认合并、打乱并重新分组</button>
    </div></div>
    <div id="treeHostModal" class="modal"><div class="modal-card" style="width:min(520px,100%)">
        <div class="modal-head"><h2 id="treeHostTitle">编辑分支域名/IP</h2><button id="closeTreeHost" class="secondary">关闭</button></div>
        <input id="treeHostNodeId" type="hidden">
        <div class="field"><label>域名或 IP</label><input id="treeHostValue" placeholder="domain.example.com 或 IP"></div>
        <div class="field"><label>自定义 target_id（可留空）</label><input id="treeWebhookId" placeholder="例如 aws-hk-01"></div>
        <p class="hint">保存后立即影响该分支用户下一次拉取的订阅。</p>
        <button id="confirmTreeHost">保存域名/IP</button>
    </div></div>

    <div class="grid">
        <section class="card wide">
            <h2>调度任务（一个任务可包含多个权限组）</h2>
            <div class="campaignbar">
                <select id="campaignSelect"></select>
                <button id="newCampaign">新建任务</button>
                <button id="deleteCampaign" class="danger">删除任务</button>
            </div>
        </section>

        <section class="card wide">
            <div class="topbar">
                <h2>系统状态</h2>
                <div class="actions">
                    <span id="routerStatus" class="pill off">未初始化</span>
                    <span id="configVersion" class="pill off version">v0</span>
                    <div class="dropdown" id="routerDropdown">
                        <button class="secondary small" onclick="toggleDropdown()">操作 ▾</button>
                        <div class="dropdown-menu">
                            <div id="routerMissing">
                                <p class="hint" style="margin:0 0 8px">初始化后再配置用户池</p>
                                <button id="initializeRouter" style="width:100%">初始化调度系统</button>
                            </div>
                            <div id="routerControls" style="display:none">
                                <button id="toggleRouter" class="success" style="width:100%">启用接管</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div class="stats">
                <div class="stat"><span>有效用户</span><strong id="eligibleCount">0</strong></div>
                <div class="stat"><span>已拉订阅用户</span><strong id="pulledUserCount">0</strong></div>
                <div class="stat"><span>已分组用户</span><strong id="groupedUserCount">0</strong></div>
                <div class="stat"><span>未拉未分组</span><strong id="unpulledUngroupedCount">0</strong></div>
                <div class="stat"><span>用户池</span><strong id="poolCount">0</strong></div>
                <div class="stat"><span>未测试</span><strong id="untestedCount">0</strong></div>
            </div>
        </section>

        <section class="card wide">
            <h2>任务目标</h2>
            <div class="field"><label>任务名称</label><input id="campaignName" placeholder="例如：客户端域名调度"></div>
            <div class="field">
                <label>campaign_id（对接 AWS 用）</label>
                <div class="host-tools">
                    <input id="campaignIdDisplay" readonly placeholder="保存任务后自动生成" style="flex:1;min-width:0">
                    <button id="copyCampaignId" type="button" class="secondary small" disabled>复制</button>
                </div>
            </div>
            <div class="field"><label>用户主权限组（可单选或多选）</label><div id="groupSelect" class="group-list"></div></div>
            <div class="field"><label>需要替换域名的节点（默认全选；取消勾选的节点保留原域名，适合不需要替换的协议节点）</label><div id="nodeSelect" class="node-list"></div></div>
            <button id="saveCampaign">保存任务</button>
        </section>

        <section class="card pool-section">
            <div class="topbar"><h2>用户池与域名</h2><button id="newPool">新增用户池</button></div>
            <div id="poolGrid" class="pool-grid"></div>
        </section>

        <section class="card edit-pool">
            <h2>编辑用户池</h2>
            <input id="poolId" type="hidden">
            <div class="split">
                <div class="field"><label>名称</label><input id="poolName" placeholder="安全组 1"></div>
                <div class="field"><label>类型</label><select id="poolType">
                    <option value="default">默认组</option><option value="probe">测试组</option><option value="observation">观察组</option>
                    <option value="safe">安全组</option><option value="emergency">应急组</option><option value="custom">自定义</option><option value="danger">危险组</option><option value="blacklist">封禁组（黑名单）</option>
                </select></div>
            </div>
            <div class="field"><label>统一域名/IP</label><input id="poolHost" placeholder="domain.example.com 或 IP"></div>
            <div class="field"><label>自定义 target_id（可留空）</label><input id="poolWebhookId" placeholder="例如 default-aws；留空使用系统标识"></div>
            <div class="split">
                <div class="field"><label>状态</label><select id="poolStatus"><option value="available">可用</option><option value="active">使用中</option><option value="standby">备用</option><option value="suspected">疑似被墙</option><option value="blocked">已被墙</option></select></div>
                <div class="field"><label>容量（0=不限）</label><input id="poolCapacity" type="number" min="0" value="0"></div>
            </div>
            <div class="field"><label>满员后自动转入</label><select id="poolOverflow"><option value="">不自动转入</option></select></div>
            <div class="field"><label><input id="poolEnabled" type="checkbox" style="width:auto;height:auto" checked> 启用该用户池</label></div>
            <div class="field"><label>备注</label><input id="poolNote"></div>
            <button id="savePool">保存用户池</button>
        </section>

        <section class="card wide">
            <div class="topbar"><h2>树形分支排查</h2><button id="openMergeTree" class="warning" disabled>合并旧排查树（0）</button></div>
            <div class="hint">勾选一个或多个最上层根组：已标记被墙的分支会连同未拉取用户全部打乱重组；成功后旧树和旧分组会直接删除。</div>
            <div id="investigationTree" class="tree-list tree-scroll" style="margin-top:12px"></div>
        </section>

        <section class="card">
            <h2>搜索与单用户规则</h2>
            <div class="row"><input id="userSearch" placeholder="用户 ID 或邮箱"><button id="searchUser">搜索</button></div>
            <div id="searchResults" style="margin-top:12px"></div>
        </section>

        <section class="card">
            <h2>编辑单用户规则</h2>
            <div class="field"><label>用户</label><input id="overrideUser" placeholder="先从左侧搜索选择"></div>
            <div class="field"><label>指定用户池</label><select id="overridePool"></select></div>
            <div class="field"><label>单用户统一域名/IP（可留空）</label><input id="overrideHost"></div>
            <div class="field"><label><input id="overrideLocked" type="checkbox" style="width:auto;height:auto" checked> 锁定，不允许自动分流修改</label></div>
            <div class="field"><label>失效时间（留空为永久）</label><input id="overrideExpires" type="datetime-local"></div>
            <div class="field"><label>备注</label><input id="overrideNote"></div>
            <button id="saveOverride">保存用户规则</button>
        </section>

        <section class="card wide">
            <div class="topbar">
                <h2>已生效的单用户规则 <span id="overrideCount" class="pill off">0</span></h2>
                <div class="actions">
                    <input type="text" id="overrideSearch" placeholder="搜索用户ID或邮箱" style="width:160px">
                    <button id="searchOverrides" class="secondary">搜索</button>
                    <button id="refreshOverrides" class="secondary">刷新</button>
                </div>
            </div>
            <div class="scroll" style="max-height:320px"><table><thead><tr><th>用户</th><th>用户池</th><th>指定域名/IP</th><th>锁定</th><th>备注</th><th>操作</th></tr></thead><tbody id="overrideRows"></tbody></table></div>
            <div class="actions" style="margin-top:8px;justify-content:space-between">
                <span id="overridePage" class="hint">第 1 / 1 页</span>
                <div><button id="prevOverrides" class="secondary small" disabled>上一页</button> <button id="nextOverrides" class="secondary small" disabled>下一页</button></div>
            </div>
        </section>

        <section class="card wide">
            <div class="topbar">
                <h2>换 IP 事件日志</h2>
                <div class="actions"><span id="wallPending" class="pill off">换IP队列 0</span><button id="refreshWall" class="secondary">刷新</button></div>
            </div>
            <div class="scroll" style="max-height:320px"><table><thead><tr><th>时间</th><th>类型</th><th>旧IP→新IP</th><th>受影响池</th><th>窗口内拉取</th><th>拿到过该地址</th></tr></thead><tbody id="wallEvents"></tbody></table></div>
        </section>
    </div>
</main>
<script>
const API_BASE=@json($apiBase);
let token='';
const tokenCandidates=readTokens();
function toggleCollapse(id){document.getElementById(id).classList.toggle('collapsed')}
function toggleDropdown(){$('routerDropdown').classList.toggle('open')}
document.addEventListener('click',e=>{if(!e.target.closest('#routerDropdown'))$('routerDropdown').classList.remove('open')})
let meta={groups:[],servers:[]},campaigns=[],current=null,noticeTimer=null,refreshing=false;
let poolModal={poolId:'',poolName:'',page:1,lastPage:1,total:0,q:''};
let transferSource=null;
let splitTreeNodeId='';
const mergeTreeNodeIds=new Set();
const pingStates=new Map();
const $=id=>document.getElementById(id);
function readTokens(){
    const candidates=[];
    const add=value=>{
        if(typeof value!=='string')return;
        value=value.trim();
        if(value.startsWith('Bearer '))value=value.slice(7).trim();
        if(value.length>=16&&!candidates.includes(value))candidates.push(value);
    };
    const scan=(value,hinted=false,depth=0)=>{
        if(depth>6||value==null)return;
        if(typeof value==='string'){
            if(hinted)add(value);
            try{scan(JSON.parse(value),hinted,depth+1)}catch{}
            return;
        }
        if(Array.isArray(value)){value.forEach(item=>scan(item,hinted,depth+1));return}
        if(typeof value==='object')Object.entries(value).forEach(([key,item])=>
            scan(item,hinted||/(?:access|auth|token)/i.test(key),depth+1)
        );
    };
    for(const storage of [localStorage,sessionStorage]){
        for(let index=0;index<storage.length;index++){
            const key=storage.key(index),raw=storage.getItem(key);
            scan(raw,/(?:access|auth|token)/i.test(key||''));
        }
    }
    document.cookie.split(';').forEach(part=>{
        const [key,...rest]=part.trim().split('=');
        if(/(?:access|auth|token)/i.test(key||''))add(decodeURIComponent(rest.join('=')));
    });
    return candidates;
}
async function request(path,options={}){const response=await fetch(`${API_BASE}${path}`,{...options,headers:{Accept:'application/json','Content-Type':'application/json',Authorization:token.startsWith('Bearer ')?token:`Bearer ${token}`,...(options.headers||{})}});const payload=await response.json().catch(()=>({}));if(!response.ok||payload.status==='fail'){const errors=payload.errors?Object.values(payload.errors).flat().join('；'):'';throw new Error(errors||payload.message||`请求失败（${response.status}）`)}return payload.data}
function toast(message,type='success'){clearTimeout(noticeTimer);$('toast').textContent=message;$('toast').className=`toast show ${type}`;noticeTimer=setTimeout(()=>$('toast').className='toast',4500)}
function loading(show,text='正在加载…'){$('loadingText').textContent=text;$('loading').classList.toggle('show',show)}
function api(path){if(!current?.id)throw new Error('请先保存调度任务');return `/campaigns/${encodeURIComponent(current.id)}${path}`}
function blankCampaign(){const first=Number(meta.groups[0]?.id||0);return{id:'',name:'',target_group_id:first||'',target_group_ids:first?[first]:[],target_server_ids:[],excluded_server_ids:[],eligible_count:0,router:null}}
function router(){return current?.router||null}
function pools(types=null){const order={default:0,probe:1,observation:2,safe:3,emergency:4,custom:5,danger:6,blacklist:7},list=[...(router()?.pools||[])].filter(pool=>!pool.tree_node_id).sort((left,right)=>(order[left.type]??5)-(order[right.type]??5));return types?list.filter(pool=>types.includes(pool.type)&&pool.enabled):list}
function poolTypeName(type){return {default:'默认组',probe:'排查组',observation:'观察组',safe:'安全组',emergency:'应急组',custom:'自定义组',danger:'危险组',blacklist:'黑名单'}[type]||type||'未知类型'}
function poolStatusName(status){return {available:'可用',active:'使用中',standby:'备用',suspected:'疑似被墙',blocked:'已被墙'}[status]||status||'未知状态'}
function formatTime(value){return value?new Date(Number(value)*1000).toLocaleString():'-'}
function option(select,value,label,selected=false){const item=document.createElement('option');item.value=value;item.textContent=label;item.selected=selected;select.appendChild(item)}
function fillSelect(id,items,value='',emptyLabel=''){const select=$(id);select.textContent='';if(emptyLabel)option(select,'',emptyLabel,value==='');items.forEach(item=>option(select,item.id,item.name,item.id===value))}
function groupNames(groupIds){const names=(groupIds||[]).map(id=>meta.groups.find(group=>Number(group.id)===Number(id))?.name).filter(Boolean);return names.join('、')}
function renderCampaigns(){const select=$('campaignSelect');select.textContent='';if(!campaigns.length&&!current?.id)option(select,'','尚未创建任务',true);if(current&&!current.id)option(select,'','新任务（未保存）',true);campaigns.forEach(item=>{const groups=groupNames(item.target_group_ids||[item.target_group_id]);option(select,item.id,`${item.name}${groups?`【${groups}】`:''}${item.router?.enabled?'（接管中）':''}`,item.id===current?.id)})}
function selectedGroupIds(){return [...document.querySelectorAll('.targetGroup:checked')].map(item=>Number(item.value)).filter(Boolean)}
function renderGroups(){const container=$('groupSelect');container.textContent='';if(!meta.groups?.length){container.innerHTML='<div class="empty">未读取到用户组，请刷新页面后重试</div>';return}const selected=new Set((current?.target_group_ids||[current?.target_group_id]).map(Number).filter(Boolean));meta.groups.forEach(group=>{const label=document.createElement('label');label.className='group-option';const input=document.createElement('input');input.type='checkbox';input.className='targetGroup';input.value=group.id;input.checked=selected.has(Number(group.id));input.onchange=renderNodes;const text=document.createElement('span');text.textContent=`${group.name}（${group.users_count} 人）`;label.append(input,text);container.appendChild(label)})}
function groupServers(){const ids=new Set(selectedGroupIds().map(String));return (meta.servers||[]).filter(server=>{let groups=server.group_ids;if(typeof groups==='string'){try{groups=JSON.parse(groups)}catch{groups=[]}}return (groups||[]).some(id=>ids.has(String(id)))})}
function renderNodes(){const box=$('nodeSelect'),list=groupServers();box.textContent='';if(!list.length){box.innerHTML='<div class="empty">请选择用户组后显示节点</div>';return}const excluded=new Set((current?.excluded_server_ids||[]).map(Number));list.forEach(server=>{const label=document.createElement('label');label.className='node';const input=document.createElement('input');input.type='checkbox';input.className='nodeReplace';input.value=server.id;input.checked=!excluded.has(Number(server.id));const name=document.createElement('span');name.textContent=server.name;const tag=document.createElement('small');tag.textContent=server.type;label.append(input,name,tag);box.appendChild(label)})}
function excludedServerIds(){return [...document.querySelectorAll('.nodeReplace')].filter(item=>!item.checked).map(item=>Number(item.value))}
function renderPingTarget(container,host,fallback='未配置域名'){
    container.className='host host-tools';
    const text=document.createElement('span');
    text.textContent=host||fallback;
    container.appendChild(text);
    if(!host)return;
    const button=document.createElement('button'),result=document.createElement('span');
    button.className='secondary small';
    button.dataset.pingHost=host;
    result.className='ping-result';
    result.dataset.pingHost=host;
    button.onclick=()=>pingTarget(host);
    container.append(button,result);
    applyPingState(host,button,result)
}
function applyPingState(host,button,result){
    const state=pingStates.get(host);
    button.disabled=state?.phase==='running';
    button.textContent=state?.buttonText||(state?.phase?'重新 Ping':'一键 Ping');
    result.textContent=state?.text||'';
    result.className=`ping-result ${state?.kind||''}`;
    result.title=state?.title||''
}
function refreshPingState(host){
    document.querySelectorAll('[data-ping-host]').forEach(element=>{
        if(element.dataset.pingHost!==host)return;
        const container=element.closest('.host-tools');
        if(container)applyPingState(
            host,
            container.querySelector('button[data-ping-host]'),
            container.querySelector('.ping-result[data-ping-host]')
        )
    })
}
async function pingTarget(host){
    pingStates.set(host,{phase:'running',buttonText:'创建检测…',text:'正在创建检测任务…',kind:''});
    refreshPingState(host);
    try{
        const task=await request('/ping',{method:'POST',body:JSON.stringify({host})});
        for(let count=0;count<24;count++){
            pingStates.set(host,{phase:'running',buttonText:'检测中…',text:`等待国内节点返回… ${count*4} 秒`,kind:''});
            refreshPingState(host);
            if(count>0)await new Promise(resolve=>setTimeout(resolve,4000));
            const data=await request(`/ping/${encodeURIComponent(task.id)}`);
            if(!data.done)continue;
            const latency=data.items.filter(item=>item.ok&&item.latency>0);
            const avg=latency.length?Math.round(latency.reduce((sum,item)=>sum+item.latency,0)/latency.length):0;
            const labels={reachable:'多数节点可达',partial:'部分节点可达',unreachable:'疑似不可达'};
            const text=`${labels[data.status]||'检测完成'} ${data.success_count}/${data.total_count}${avg?` · ${avg}ms`:''}`;
            pingStates.set(host,{phase:'done',buttonText:'重新 Ping',text,kind:data.status==='reachable'?'ok':data.status==='partial'?'warn':'bad',title:data.items.map(item=>`${item.node_name}${item.isp?` ${item.isp}`:''}：${item.ok?'可达':item.error||'失败'}${item.packet_loss>=0?`，丢包 ${item.packet_loss}%`:''}`).join('\n')});
            refreshPingState(host);
            toast(`${host}：${text}`,data.status==='unreachable'?'error':'success');
            return
        }
        throw new Error('检测超时，请稍后重试')
    }catch(error){pingStates.set(host,{phase:'error',buttonText:'重新 Ping',text:`检测失败：${error.message}`,kind:'bad',title:error.message});refreshPingState(host);toast(error.message,'error')}
}
function renderStatus(){const r=router();$('eligibleCount').textContent=current?.eligible_count||0;$('pulledUserCount').textContent=r?.pulled_user_count||0;$('groupedUserCount').textContent=r?.grouped_user_count||0;$('unpulledUngroupedCount').textContent=r?.unpulled_ungrouped_count||0;$('poolCount').textContent=r?.pools.length||0;$('untestedCount').textContent=r?.untested_count||0;$('configVersion').textContent=`v${r?.config_version||0}`;$('routerStatus').textContent=!r?'未初始化':r.enabled?'全量接管中':'未启用';$('routerStatus').className=`pill ${r?.enabled?'on':'off'}`;$('routerMissing').style.display=r?'none':'block';$('routerControls').style.display=r?'block':'none';if(r){$('toggleRouter').textContent=r.enabled?'停止接管':'启用接管';$('toggleRouter').className=r.enabled?'danger':'success'}}
function renderPools(){const grid=$('poolGrid');grid.textContent='';const list=pools();if(!list.length){grid.innerHTML='<div class="empty">初始化后配置用户池</div>';return}list.forEach(pool=>{const card=document.createElement('div');card.className='pool';const head=document.createElement('div');head.className='pool-head';const title=document.createElement('strong');title.textContent=pool.name;const state=document.createElement('span');state.className=`pill ${pool.status==='blocked'?'bad':pool.enabled?'on':'off'}`;state.textContent=poolStatusName(pool.status);head.append(title,state);const host=document.createElement('div');renderPingTarget(host,pool.host,`${Object.keys(pool.node_hosts||{}).length} 个节点独立地址`);const overflowName=pools().find(item=>item.id===pool.overflow_pool_id)?.name;const metaLine=document.createElement('div');metaLine.className='meta';metaLine.textContent=`${poolTypeName(pool.type)} · ${pool.member_count} 人 · ${pool.pulled_count} 已拉取 · 容量 ${pool.capacity||'不限'} · 接口标识 ${pool.webhook_id||pool.id}${overflowName?` · 满后→${overflowName}`:''}`;const actions=document.createElement('div');actions.className='actions';const actionItems=[['复制接口标识','copy-id'],['编辑','edit'],['用户','users']];if(pool.member_count>0&&!['danger','blacklist'].includes(pool.type))actionItems.push(['进入树形排查','tree']);if(!['danger','blacklist'].includes(pool.type)){actionItems.push(['转移已拉取','transfer']);actionItems.push(['转移未拉取','transfer-unpulled']);}actionItems.push(['删除','delete']);actionItems.forEach(([label,action])=>{const button=document.createElement('button');button.className='secondary small';button.textContent=label;button.onclick=()=>poolAction(action,pool);if(action==='transfer'&&pool.pulled_count<1)button.disabled=true;if(action==='transfer-unpulled'&&(pool.member_count-pool.pulled_count)<1)button.disabled=true;if(action==='delete'&&pool.id==='default')button.disabled=true;actions.appendChild(button)});card.append(head,host,metaLine,actions);grid.appendChild(card)})}
function renderPoolOverflowOptions(currentId='',value=''){const type=$('poolType').value;fillSelect('poolOverflow',pools().filter(item=>item.id!==currentId&&!['danger','blacklist'].includes(type)&&!['danger','blacklist'].includes(item.type)),value,'不自动转入')}
function editPool(pool=null){$('poolId').value=pool?.id||'';$('poolName').value=pool?.name||'';$('poolType').value=pool?.type||'safe';$('poolType').disabled=pool?.id==='default';$('poolHost').value=pool?.host||'';$('poolWebhookId').value=pool?.webhook_id||'';$('poolStatus').value=pool?.status||'available';$('poolCapacity').value=pool?.capacity||0;renderPoolOverflowOptions(pool?.id||'',pool?.overflow_pool_id||'');$('poolEnabled').checked=pool?.enabled??true;$('poolNote').value=pool?.note||'';window.scrollTo({top:$('poolName').getBoundingClientRect().top+window.scrollY-100,behavior:'smooth'})}
function openPoolTransfer(pool,movePulled=true){const targets=pools().filter(item=>item.id!==pool.id&&!['danger','blacklist'].includes(item.type)&&item.enabled&&item.status!=='blocked');if(!targets.length)return toast('暂无可用的转入用户池','error');const unpulledCount=Math.max(0,pool.member_count-pool.pulled_count);transferSource={...pool,mode:movePulled?'pool-pulled':'pool-unpulled'};fillSelect('transferTarget',targets,'');$('transferTitle').textContent=movePulled?'转移已拉取用户':'转移未拉取用户';$('transferHint').textContent=movePulled?`“${pool.name}”当前有 ${pool.pulled_count} 名已拉取用户（含锁定），转移后会继续锁定。`:`“${pool.name}”当前有 ${unpulledCount} 名未拉取用户（含锁定），转移后会继续锁定；已拉取的 ${pool.pulled_count} 人留在当前组。`;$('transferModal').classList.add('show')}
function openTreeTransfer(node,movePulled=true){const targets=pools(['default','probe','observation','safe','custom','emergency']).filter(item=>item.id!==node.pool_id&&item.status!=='blocked');if(!targets.length)return toast('暂无可用的普通用户池','error');const unpulledCount=Math.max(0,node.user_count-node.pulled_count);transferSource={...node,mode:movePulled?'tree-pulled':'tree-unpulled'};fillSelect('transferTarget',targets,'');$('transferTitle').textContent=movePulled?'转移已拉取用户':'转移未拉取用户';$('transferHint').textContent=movePulled?`“${node.name}”已有 ${node.pulled_count} 人拉取（含锁定）；只迁移这些已拉取用户并锁定，剩余 ${unpulledCount} 人继续留在当前分支观察。`:`“${node.name}”有 ${unpulledCount} 人尚未拉取（含锁定）；只迁移这些未拉取用户并锁定，已拉取的 ${node.pulled_count} 人继续留在当前分支。`;$('transferModal').classList.add('show')}
async function poolAction(action,pool){try{if(action==='copy-id'){const targetId=pool.webhook_id||pool.id;await navigator.clipboard.writeText(targetId);return toast(`接口标识已复制：${targetId}`)}if(action==='edit')return editPool(pool);if(action==='users')return showPoolUsers(pool);if(action==='transfer')return openPoolTransfer(pool,true);if(action==='transfer-unpulled')return openPoolTransfer(pool,false);if(action==='tree'){if(!confirm(`把“${pool.name}”的 ${pool.member_count} 名固定用户冻结为独立排查根组？\n创建后还需继续拆分并填写新域名。`))return;updateCurrent(await request(api(`/pools/${encodeURIComponent(pool.id)}/investigation`),{method:'POST',body:JSON.stringify({name:''})}));toast('已创建排查根组，请继续拆分');document.getElementById('investigationTree').scrollIntoView({behavior:'smooth'});return}if(action==='delete'){if(!confirm(`删除“${pool.name}”？`))return;updateCurrent(await request(api(`/pools/${encodeURIComponent(pool.id)}`),{method:'DELETE'}));toast('用户池已删除')}}catch(error){toast(error.message,'error')}}
function renderInvestigationTree(){
    const list=$('investigationTree'),nodes=router()?.investigation_nodes||[];
    list.textContent='';
    if(!nodes.length){mergeTreeNodeIds.clear();updateMergeTreeButton();list.innerHTML='<div class="empty">暂无树形排查</div>';return}
    const byId=new Map(nodes.map(node=>[node.id,node]));
    const mergeableIds=new Set(nodes.filter(node=>node.depth===0&&node.status!=='archived'&&treeMergeableCount(node.id)>0).map(node=>node.id));
    [...mergeTreeNodeIds].forEach(id=>{if(!mergeableIds.has(id))mergeTreeNodeIds.delete(id)});
    const childrenByParent=new Map(),ordered=[],visited=new Set();
    nodes.forEach(node=>{
        const parentId=node.parent_id&&byId.has(node.parent_id)?node.parent_id:'';
        if(!childrenByParent.has(parentId))childrenByParent.set(parentId,[]);
        childrenByParent.get(parentId).push(node)
    });
    childrenByParent.forEach(children=>children.sort((a,b)=>a.created_at-b.created_at));
    const appendBranch=node=>{
        if(visited.has(node.id))return;
        visited.add(node.id);
        ordered.push(node);
        (childrenByParent.get(node.id)||[]).forEach(appendBranch)
    };
    (childrenByParent.get('')||[]).forEach(appendBranch);
    nodes.forEach(appendBranch);
    ordered.forEach(node=>{
        const card=document.createElement('div');
        const depthClass=node.depth<=4?`tree-depth-${node.depth}`:'';
        card.className=`tree-node ${depthClass}`;
        card.style.marginLeft=node.depth>0?`${Math.min(node.depth,8)*28}px`:'';
        const head=document.createElement('div'),title=document.createElement('strong'),state=document.createElement('span');
        head.className='pool-head';
        const displayName=node.host&&!node.name.includes(node.host)?`${node.name} · ${node.host}`:node.name;
        title.textContent=`${node.depth===0?'根组':'L'+node.depth} · ${displayName}`;
        state.className=`pill ${node.status==='blocked'?'bad':node.status==='safe'?'on':'off'}`;
        state.textContent={active:'观察中',safe:'安全',blocked:'被墙',split:'已拆分',archived:'已归档'}[node.status]||node.status;
        head.append(title,state);
        const host=document.createElement('div'),metaLine=document.createElement('div'),actions=document.createElement('div');
        const hostLabel=document.createElement('div'),hostTarget=document.createElement('div');
        hostLabel.className='meta';hostLabel.textContent='域名/IP';
        renderPingTarget(hostTarget,node.children.length?'':node.host,node.children.length?'由下级分支独立配置':'未配置域名');
        host.append(hostLabel,hostTarget);
        metaLine.className='meta';
        metaLine.textContent=`${node.user_count} 人 · ${node.pulled_count} 已拉取 · 接口标识 ${node.webhook_id||node.id}${node.depth===0&&node.status!=='archived'?` · 整树 ${treeMergeableCount(node.id)} 人可合并`:''}${node.released_count?` · ${node.released_count} 已回流`:''}${node.source_node_ids?.length?` · 合并 ${node.source_node_ids.length} 棵旧树`:''}${node.parent_id&&byId.get(node.parent_id)?` · 上级：${byId.get(node.parent_id).name}`:''}`;
        actions.className='actions';
        const add=(label,fn,kind='secondary')=>{const button=document.createElement('button');button.className=`${kind} small`;button.textContent=label;button.onclick=fn;actions.appendChild(button)};
        add('复制接口标识',async()=>{const targetId=node.webhook_id||node.id;await navigator.clipboard.writeText(targetId);toast(`接口标识已复制：${targetId}`)});
        add('查看用户',()=>showTreeUsers(node));
        if(mergeableIds.has(node.id)){
            const label=document.createElement('label'),checkbox=document.createElement('input'),text=document.createElement('span');
            label.className='group-option';
            label.style.padding='5px 9px';
            checkbox.type='checkbox';checkbox.style.width='auto';checkbox.style.height='auto';
            checkbox.checked=mergeTreeNodeIds.has(node.id);
            checkbox.onchange=()=>{checkbox.checked?mergeTreeNodeIds.add(node.id):mergeTreeNodeIds.delete(node.id);updateMergeTreeButton()};
            text.textContent='选择整树合并';
            label.append(checkbox,text);actions.appendChild(label)
        }
        if(node.status!=='archived'&&!node.children.length){
            add('编辑域名',()=>openTreeHostEditor(node),'warning');
            if(['active','blocked'].includes(node.status))add('继续细分',()=>openSplitTree(node));
            if(node.pulled_count>0)add('迁移已拉取用户',()=>openTreeTransfer(node,true),'success');
            if(node.user_count>node.pulled_count)add('迁移未拉取用户',()=>openTreeTransfer(node,false),'warning');
        }
        if(node.depth===0&&node.status!=='archived')add('删除排查树',()=>deleteInvestigationTree(node),'danger');
        card.append(head,host,metaLine,actions);
        list.appendChild(card);
    });
    updateMergeTreeButton()
}
async function deleteInvestigationTree(node){if(!confirm(`删除“${node.name}”整棵排查树及全部下级分支？\n仍在树内的用户会解除固定分组，下次拉订阅时重新分配。手动锁定规则不受影响。`))return;try{const result=await request(api(`/investigations/${encodeURIComponent(node.id)}`),{method:'DELETE'});updateCurrent(result.campaign);toast(`排查树已删除，${result.released_count} 名用户等待重新分组`)}catch(error){toast(error.message,'error')}}
function openTreeHostEditor(node){$('treeHostNodeId').value=node.id;$('treeHostTitle').textContent=`编辑域名/IP：${node.name}`;$('treeHostValue').value=node.host||'';$('treeWebhookId').value=node.webhook_id||'';$('treeHostModal').classList.add('show');setTimeout(()=>$('treeHostValue').focus(),0)}
function openSplitTree(node){splitTreeNodeId=node.id;$('splitTreeTitle').textContent=`拆分：${node.name}（${node.user_count} 人）`;$('splitTreeCount').value=2;renderSplitTreeFields();$('splitTreeModal').classList.add('show')}
function renderBranchFields(countId,containerId){const count=Math.max(2,Math.min(10,Number($(countId).value)||2)),container=$(containerId),old=[...container.querySelectorAll('.branch-row')].map(row=>[row.children[0].value,row.children[1].value]);container.textContent='';for(let index=0;index<count;index++){const row=document.createElement('div');row.className='branch-row';const name=document.createElement('input');name.placeholder=`分支 ${String.fromCharCode(65+index)}`;name.value=old[index]?.[0]||`分支 ${String.fromCharCode(65+index)}`;const host=document.createElement('input');host.placeholder='全新域名或 IP';host.value=old[index]?.[1]||'';row.append(name,host);container.appendChild(row)}}
function branchValues(containerId){return [...$(containerId).querySelectorAll('.branch-row')].map(row=>({name:row.children[0].value.trim(),host:row.children[1].value.trim()}))}
function renderSplitTreeFields(){renderBranchFields('splitTreeCount','splitTreeBranches')}
function renderMergeTreeFields(){renderBranchFields('mergeTreeCount','mergeTreeBranches')}
function treeMergeableCount(rootId,nodes=router()?.investigation_nodes||[]){return nodes.filter(node=>node.root_id===rootId&&!node.children.length&&node.status!=='archived').reduce((sum,node)=>sum+node.mergeable_count,0)}
function updateMergeTreeButton(){const button=$('openMergeTree');button.textContent=`重组/合并旧树（${mergeTreeNodeIds.size}）`;button.disabled=mergeTreeNodeIds.size<1}
function openMergeTree(){const nodes=router()?.investigation_nodes||[],selected=nodes.filter(node=>mergeTreeNodeIds.has(node.id));if(!selected.length)return toast('请至少选择一个最上层根组','error');$('mergeTreeHint').textContent=`已选择 ${selected.length} 棵旧树，共 ${selected.reduce((sum,node)=>sum+treeMergeableCount(node.id,nodes),0)} 名用户。被墙分支全部用户都会打乱重组，手动锁定用户不移动，成功后旧树直接删除。`;$('mergeTreeName').value=selected.length>1?'合并排查树':'重组排查树';$('mergeTreeCount').value=2;renderMergeTreeFields();$('mergeTreeModal').classList.add('show')}
function renderOverridePoolOptions(){fillSelect('overridePool',pools(),$('overridePool').value,'仅使用单独域名')}
function resetTaskEditors(){$('poolId').value='';$('poolName').value='';$('poolHost').value='';$('poolWebhookId').value='';$('poolCapacity').value=0;$('poolNote').value='';$('userSearch').value='';$('searchResults').textContent='';$('overrideUser').value='';delete $('overrideUser').dataset.id;$('overrideHost').value='';$('overrideNote').value='';$('overrideExpires').value='';$('overrideSearch').value='';$('usersModal').classList.remove('show');$('transferModal').classList.remove('show');$('splitTreeModal').classList.remove('show');$('mergeTreeModal').classList.remove('show');$('treeHostModal').classList.remove('show');transferSource=null;splitTreeNodeId='';mergeTreeNodeIds.clear();updateMergeTreeButton();poolModal={poolId:'',poolName:'',page:1,lastPage:1,total:0,q:''};overrideModal={page:1,lastPage:1,total:0,q:''}}
function renderCurrent(){current||=blankCampaign();$('campaignName').value=current.name||'';$('campaignIdDisplay').value=current.id||'';$('copyCampaignId').disabled=!current.id;renderGroups();renderNodes();renderStatus();renderPools();renderInvestigationTree();renderOverridePoolOptions();$('deleteCampaign').disabled=!current.id||router()?.enabled}
function updateCurrent(campaign){const index=campaigns.findIndex(item=>item.id===campaign.id);if(index>=0)campaigns[index]=campaign;else campaigns.push(campaign);current=campaign;renderCampaigns();renderCurrent()}
async function refresh(full=true){const id=current?.id,isDraft=current&&current.id==='';if(full){const data=await request('/meta');meta={groups:data.groups,servers:data.servers};campaigns=data.campaigns}else campaigns=await request('/campaigns');if(isDraft&&!full){renderCampaigns();return}current=campaigns.find(item=>item.id===id)||campaigns[0]||blankCampaign();renderCampaigns();if(full)renderCurrent();else{renderStatus();renderPools();renderInvestigationTree()}}
async function showTreeUsers(node=null,page=null){if(node){poolModal={nodeId:node.id,poolName:node.name,page:1,lastPage:1,total:0,q:'',mode:'tree'};$('poolUserSearch').value=''}if(!poolModal.nodeId)return;if(page!==null)poolModal.page=page;const campaignId=current?.id;loading(true,'正在加载排查节点用户…');try{const result=await request(api(`/investigations/${encodeURIComponent(poolModal.nodeId)}/users?q=${encodeURIComponent(poolModal.q)}&page=${poolModal.page}&per_page=50`));if(current?.id!==campaignId)return;poolModal.page=result.pagination.page;poolModal.lastPage=result.pagination.last_page;poolModal.total=result.pagination.total;$('poolUserTools').style.display='grid';$('poolUserPagination').style.display='flex';$('usersTitle').textContent=`${poolModal.poolName}用户（${poolModal.total}）`;$('usersStateHead').textContent='拉取状态';$('usersActionHead').textContent='操作';$('usersBody').textContent='';result.items.forEach(user=>{const row=$('usersBody').insertRow();row.insertCell().textContent=user.id;row.insertCell().textContent=user.email;row.insertCell().textContent=user.exposed?'已拉取':'未拉取';row.insertCell().textContent=user.pull_count||0;row.insertCell().textContent=formatTime(user.last_pulled_at);row.insertCell().textContent='-'});$('poolUserPage').textContent=`第 ${poolModal.page} / ${poolModal.lastPage} 页`;$('prevPoolUsers').disabled=poolModal.page<=1;$('nextPoolUsers').disabled=poolModal.page>=poolModal.lastPage;$('usersModal').classList.add('show')}catch(error){toast(error.message,'error')}finally{loading(false)}}
function reloadPagedUsers(page){return poolModal.mode==='tree'?showTreeUsers(null,page):showPoolUsers(null,page)}
async function showPoolUsers(pool=null,page=null){if(pool){poolModal={poolId:pool.id,poolName:pool.name,page:1,lastPage:1,total:0,q:''};$('poolUserSearch').value=''}if(!poolModal.poolId)return;if(page!==null)poolModal.page=page;const campaignId=current?.id;loading(true,'正在加载用户…');try{const result=await request(api(`/pools/${encodeURIComponent(poolModal.poolId)}/users?q=${encodeURIComponent(poolModal.q)}&page=${poolModal.page}&per_page=50`));if(current?.id!==campaignId)return;poolModal.page=result.pagination.page;poolModal.lastPage=result.pagination.last_page;poolModal.total=result.pagination.total;$('poolUserTools').style.display='grid';$('poolUserPagination').style.display='flex';$('usersTitle').textContent=`${poolModal.poolName}用户（${poolModal.total}）`;$('usersStateHead').textContent='当前组';$('usersActionHead').textContent='移动到';$('usersBody').textContent='';result.items.forEach(user=>{const row=$('usersBody').insertRow();row.insertCell().textContent=user.id;row.insertCell().textContent=user.email;row.insertCell().textContent=user.pulled?'已拉取':'未拉取';row.insertCell().textContent=user.pull_count>0?user.pull_count:user.pulled?'历史已拉取':'0';row.insertCell().textContent=formatTime(user.last_pulled_at);const cell=row.insertCell(),wrap=document.createElement('div'),select=document.createElement('select'),button=document.createElement('button');wrap.className='actions';select.style.minWidth='130px';pools().filter(item=>item.enabled&&item.status!=='blocked').forEach(item=>option(select,item.id,item.name,item.id===poolModal.poolId));button.className='small';button.textContent='移动';button.onclick=async()=>{if(select.value===poolModal.poolId)return toast('用户已经在该组','error');if(!confirm(`把 ${user.email} 移动到“${select.options[select.selectedIndex].textContent}”？`))return;try{updateCurrent(await request(api(`/overrides/${user.id}`),{method:'POST',body:JSON.stringify({pool_id:select.value,host:'',node_hosts:{},server_name:'',transport_host:'',locked:true,note:`从${poolModal.poolName}手动移动`,expires_at:0})}));await showPoolUsers(null,poolModal.page);await loadOverrides();toast('用户已移动')}catch(error){toast(error.message,'error')}};wrap.append(select,button);cell.appendChild(wrap)});if(!result.items.length){const row=$('usersBody').insertRow(),cell=row.insertCell();cell.colSpan=6;cell.className='hint';cell.textContent='没有符合条件的用户'}$('poolUserPage').textContent=`第 ${poolModal.page} / ${poolModal.lastPage} 页，共 ${poolModal.total} 人`;$('prevPoolUsers').disabled=poolModal.page<=1;$('nextPoolUsers').disabled=poolModal.page>=poolModal.lastPage;$('usersModal').classList.add('show')}catch(error){toast(error.message,'error')}finally{loading(false)}}
let overrideModal={page:1,lastPage:1,total:0,q:''};
async function loadOverrides(page=null){const campaignId=current?.id,body=$('overrideRows');if(!body)return;if(!campaignId||!router()){body.textContent='';$('overrideCount').textContent='0';return}if(page!==null)overrideModal.page=page;const result=await request(api(`/overrides?q=${encodeURIComponent(overrideModal.q)}&page=${overrideModal.page}&per_page=50`));if(current?.id!==campaignId)return;overrideModal.page=result.pagination.page;overrideModal.lastPage=result.pagination.last_page;overrideModal.total=result.pagination.total;$('overrideCount').textContent=result.pagination.total;$('overridePage').textContent=`第 ${overrideModal.page} / ${overrideModal.lastPage} 页，共 ${overrideModal.total} 条`;$('prevOverrides').disabled=overrideModal.page<=1;$('nextOverrides').disabled=overrideModal.page>=overrideModal.lastPage;body.textContent='';result.items.forEach(user=>{const row=body.insertRow();row.insertCell().textContent=`${user.id} / ${user.email}`;row.insertCell().textContent=user.pool_id||'-';row.insertCell().textContent=user.override.host||Object.values(user.override.node_hosts||{}).join(', ')||'-';row.insertCell().textContent=user.override.locked?'是':'否';row.insertCell().textContent=user.override.note||'-';const cell=row.insertCell(),button=document.createElement('button');button.className='danger small';button.textContent='解除';button.onclick=async()=>{try{updateCurrent(await request(api(`/overrides/${user.id}`),{method:'DELETE'}));await loadOverrides(overrideModal.page);toast('规则已解除')}catch(error){toast(error.message,'error')}};cell.appendChild(button)});if(!result.items.length){const row=body.insertRow(),cell=row.insertCell();cell.colSpan=6;cell.className='hint';cell.textContent=overrideModal.q?'没有匹配的规则':'暂无手动规则'}}
let wallData=null;
function wallReasonLabel(reason){return {blocked:'被墙',machine:'机器挂壁'}[reason]||reason||'-'}
function renderWall(){const events=$('wallEvents');if(!events)return;const pendingPill=$('wallPending');if(!wallData){if(pendingPill){pendingPill.textContent='换IP队列 0';pendingPill.className='pill off'}events.textContent='';return}const pending=Number(wallData.pending_ip_rotates||0);if(pendingPill){pendingPill.textContent=`换IP队列 ${pending}`;pendingPill.className=`pill ${pending>0?'bad':'off'}`;pendingPill.title=pending>0?'有换 IP 事件排队等待写入，每分钟自动消化':'无积压换 IP 事件'}
events.textContent='';(wallData.events||[]).forEach(ev=>{const row=events.insertRow();const timeCell=row.insertCell();timeCell.textContent=formatTime(ev.at);if(ev.mode==='manual_fix')timeCell.innerHTML+=' <span class="pill warn">补</span>';if((ev.pools||[]).some(p=>p&&p.stale))timeCell.innerHTML+=' <span class="pill off" title="老 IP 首墙，曝光窗口不可信">跳过</span>';const reasonCell=row.insertCell();reasonCell.innerHTML=`<span class="pill ${ev.reason==='blocked'?'bad':'off'}">${wallReasonLabel(ev.reason)}</span>`;row.insertCell().textContent=`${ev.old_ip||'-'} → ${ev.new_ip||'-'}`;row.insertCell().textContent=(ev.pools||[]).map(p=>typeof p==='string'?p:p.pool_name).join('、')||'-';row.insertCell().textContent=ev.suspect_count||0;
const exact=(ev.pools||[]).reduce((sum,p)=>sum+Number(p&&p.exact_count||0),0);const exactCell=row.insertCell();exactCell.textContent=exact;exactCell.title='实际拿到过这个死地址的人数'});
if(!(wallData.events||[]).length){const row=events.insertRow();row.insertCell().colSpan=6;row.cells[0].className='empty';row.cells[0].textContent='暂无换 IP 事件记录'}}
async function loadWallLog(){const campaignId=current?.id;if(!campaignId||!router()){wallData=null;renderWall();return}const data=await request(api('/wall-log?limit=100'));if(current?.id!==campaignId)return;wallData=data;renderWall()}
$('refreshWall').onclick=()=>loadWallLog().catch(error=>toast(error.message,'error'));
$('refreshOverrides').onclick=()=>{overrideModal.q='';$('overrideSearch').value='';loadOverrides(1).catch(error=>toast(error.message,'error'))};
$('searchOverrides').onclick=()=>{overrideModal.q=$('overrideSearch').value.trim();loadOverrides(1).catch(error=>toast(error.message,'error'))};
$('overrideSearch').onkeydown=event=>{if(event.key==='Enter')$('searchOverrides').click()};
$('prevOverrides').onclick=()=>loadOverrides(overrideModal.page-1).catch(error=>toast(error.message,'error'));
$('nextOverrides').onclick=()=>loadOverrides(overrideModal.page+1).catch(error=>toast(error.message,'error'));
$('campaignSelect').onchange=async event=>{const selectedId=event.target.value;try{loading(true,'正在切换任务…');await refresh(false);current=campaigns.find(item=>item.id===selectedId)||blankCampaign();resetTaskEditors();renderCampaigns();renderCurrent();await loadOverrides();await loadWallLog().catch(()=>{})}catch(error){toast(error.message,'error')}finally{loading(false)}};
$('newCampaign').onclick=()=>{current=blankCampaign();resetTaskEditors();renderCampaigns();renderCurrent()};
$('copyCampaignId').onclick=async()=>{const id=current?.id||$('campaignIdDisplay').value;if(!id)return toast('请先保存任务','error');try{await navigator.clipboard.writeText(id);toast(`campaign_id 已复制：${id}`)}catch{toast('复制失败，请手动选中复制','error')}};
$('deleteCampaign').onclick=async()=>{try{if(!confirm(`删除“${current.name}”？`))return;campaigns=await request(api(''),{method:'DELETE'});current=campaigns[0]||blankCampaign();resetTaskEditors();renderCampaigns();renderCurrent();await loadOverrides();toast('任务已删除')}catch(error){toast(error.message,'error')}};
$('saveCampaign').onclick=async()=>{try{const name=$('campaignName').value.trim(),target_group_ids=selectedGroupIds();if(!name)throw new Error('请填写任务名称');if(!target_group_ids.length)throw new Error('请至少勾选一个用户主权限组');const result=await request('/campaigns',{method:'POST',body:JSON.stringify({campaign_id:current.id||null,name,target_group_ids,excluded_server_ids:excludedServerIds()})});updateCurrent(result);toast('任务已保存')}catch(error){toast(error.message,'error')}};
$('initializeRouter').onclick=async()=>{try{if(!current.id)throw new Error('请先保存任务');updateCurrent(await request(api('/router/initialize'),{method:'POST',body:'{}'}));toast('调度系统已初始化，请先配置用户池域名')}catch(error){toast(error.message,'error')}};
$('toggleRouter').onclick=async()=>{try{const enable=!router().enabled;if(!confirm(enable?'启用后目标组将完全使用插件域名，确认配置完整？':'警告：停止接管会立即向用户恢复系统原始节点域名，确认继续？'))return;updateCurrent(await request(api('/router/toggle'),{method:'POST',body:JSON.stringify({enabled:enable})}));toast(enable?'全量接管已启用':'已恢复系统原始域名')}catch(error){toast(error.message,'error')}};
$('poolType').onchange=()=>renderPoolOverflowOptions($('poolId').value,$('poolOverflow').value);$('newPool').onclick=()=>editPool();$('savePool').onclick=async()=>{try{const data={id:$('poolId').value||null,webhook_id:$('poolWebhookId').value.trim()||null,name:$('poolName').value.trim(),type:$('poolType').value,host:$('poolHost').value.trim(),node_hosts:{},server_name:'',transport_host:'',status:$('poolStatus').value,capacity:Number($('poolCapacity').value)||0,overflow_pool_id:$('poolOverflow').value,enabled:$('poolEnabled').checked,note:$('poolNote').value.trim()};updateCurrent(await request(api('/pools'),{method:'POST',body:JSON.stringify(data)}));editPool();toast('用户池已保存')}catch(error){toast(error.message,'error')}};
$('searchUser').onclick=async()=>{try{const rows=await request(api(`/users/search?q=${encodeURIComponent($('userSearch').value.trim())}`)),box=$('searchResults');box.textContent='';rows.forEach(user=>{const row=document.createElement('div'),head=document.createElement('div'),title=document.createElement('strong'),button=document.createElement('button'),poolLine=document.createElement('div'),hostLine=document.createElement('div');row.className='pool';head.className='pool-head';title.textContent=`${user.id} · ${user.email}`;button.className='secondary small';button.textContent='设置';button.onclick=()=>{$('overrideUser').value=`${user.id} / ${user.email}`;$('overrideUser').dataset.id=user.id;$('overridePool').value=user.override?.pool_id||user.pool_id||'';$('overrideHost').value=user.override?.host||'';$('overrideLocked').checked=user.override?.locked??true;$('overrideNote').value=user.override?.note||'';$('overrideExpires').value=user.override?.expires_at?new Date(user.override.expires_at*1000).toISOString().slice(0,16):''};head.append(title,button);poolLine.className='meta';poolLine.textContent=`用户池：${user.pool_name} · ${poolTypeName(user.pool_type)} · ${poolStatusName(user.pool_status)}`;hostLine.className='meta';hostLine.textContent=`域名/IP：${user.pool_hosts?.join('、')||'未配置'}`;row.append(head,poolLine,hostLine);box.appendChild(row)});if(!rows.length)box.innerHTML='<div class="empty">未找到用户</div>'}catch(error){toast(error.message,'error')}};
$('saveOverride').onclick=async()=>{try{const userId=Number($('overrideUser').dataset.id);if(!userId)throw new Error('请先搜索并选择用户');const expires=$('overrideExpires').value?Math.floor(new Date($('overrideExpires').value).getTime()/1000):0;updateCurrent(await request(api(`/overrides/${userId}`),{method:'POST',body:JSON.stringify({pool_id:$('overridePool').value||null,host:$('overrideHost').value.trim(),node_hosts:{},server_name:'',transport_host:'',locked:$('overrideLocked').checked,note:$('overrideNote').value.trim(),expires_at:expires})}));await loadOverrides();toast('用户规则已保存')}catch(error){toast(error.message,'error')}};
$('searchPoolUsers').onclick=()=>{poolModal.q=$('poolUserSearch').value.trim();reloadPagedUsers(1)};$('poolUserSearch').onkeydown=event=>{if(event.key==='Enter')$('searchPoolUsers').click()};$('prevPoolUsers').onclick=()=>reloadPagedUsers(poolModal.page-1);$('nextPoolUsers').onclick=()=>reloadPagedUsers(poolModal.page+1);
$('closeTransfer').onclick=()=>{$('transferModal').classList.remove('show');transferSource=null};$('transferModal').onclick=event=>{if(event.target===$('transferModal'))$('closeTransfer').click()};$('confirmTransfer').onclick=async()=>{if(!transferSource)return;const source=transferSource,targetId=$('transferTarget').value,targetName=$('transferTarget').options[$('transferTarget').selectedIndex]?.textContent,isTree=source.mode?.startsWith('tree-'),moveUnpulled=source.mode==='tree-unpulled'||source.mode==='pool-unpulled',userType=moveUnpulled?'未拉取':'已拉取';if(!confirm(`把“${source.name}”${userType}用户转入“${targetName}”？\n迁移用户将锁定到目标组。`))return;try{const path=isTree?(moveUnpulled?`/investigations/${encodeURIComponent(source.id)}/move-unpulled`:`/investigations/${encodeURIComponent(source.id)}/move`):(moveUnpulled?`/pools/${encodeURIComponent(source.id)}/move-unpulled`:`/pools/${encodeURIComponent(source.id)}/move-pulled`);const result=await request(api(path),{method:'POST',body:JSON.stringify({target_pool_id:targetId})});updateCurrent(result.campaign);$('closeTransfer').click();toast(isTree?`已移动并锁定 ${result.moved_count} 名${userType}用户，原分支剩余 ${result.remaining_count} 人`:`已移动并锁定 ${result.moved_count} 名${userType}用户`)}catch(error){toast(error.message,'error')}};
$('closeTreeHost').onclick=()=> $('treeHostModal').classList.remove('show');$('treeHostModal').onclick=event=>{if(event.target===$('treeHostModal'))$('closeTreeHost').click()};$('treeHostValue').onkeydown=event=>{if(event.key==='Enter')$('confirmTreeHost').click()};$('confirmTreeHost').onclick=async()=>{const nodeId=$('treeHostNodeId').value,host=$('treeHostValue').value.trim(),webhookId=$('treeWebhookId').value.trim();if(!nodeId||!host)return toast('请输入域名或 IP','error');try{updateCurrent(await request(api(`/investigations/${encodeURIComponent(nodeId)}/host`),{method:'POST',body:JSON.stringify({host,webhook_id:webhookId||null})}));$('closeTreeHost').click();toast('分支域名/IP和接口标识已更新')}catch(error){toast(error.message,'error')}};
$('splitTreeCount').oninput=renderSplitTreeFields;$('closeSplitTree').onclick=()=>{$('splitTreeModal').classList.remove('show');splitTreeNodeId=''};$('splitTreeModal').onclick=event=>{if(event.target===$('splitTreeModal'))$('closeSplitTree').click()};$('confirmSplitTree').onclick=async()=>{if(!splitTreeNodeId)return;const branches=branchValues('splitTreeBranches');if(branches.some(branch=>!branch.host))return toast('请填写每个分支的全新域名','error');if(!confirm(`确认把该节点固定均分为 ${branches.length} 个独立分支？`))return;try{updateCurrent(await request(api(`/investigations/${encodeURIComponent(splitTreeNodeId)}/split`),{method:'POST',body:JSON.stringify({branches})}));$('closeSplitTree').click();toast('下级分支已创建，用户分配已固定')}catch(error){toast(error.message,'error')}};
$('openMergeTree').onclick=openMergeTree;$('mergeTreeCount').oninput=renderMergeTreeFields;$('closeMergeTree').onclick=()=> $('mergeTreeModal').classList.remove('show');$('mergeTreeModal').onclick=event=>{if(event.target===$('mergeTreeModal'))$('closeMergeTree').click()};$('confirmMergeTree').onclick=async()=>{const nodeIds=[...mergeTreeNodeIds],branches=branchValues('mergeTreeBranches');if(!nodeIds.length)return toast('请至少选择一个最上层根组','error');if(branches.some(branch=>!branch.host))return toast('请填写每个新分支的全新域名','error');if(!confirm(`把 ${nodeIds.length} 棵旧树中的用户重新打乱并分为 ${branches.length} 组？\n被墙分支包含未拉取用户，手动锁定用户不移动，旧树和旧分组将直接删除。`))return;try{const result=await request(api('/investigations/merge'),{method:'POST',body:JSON.stringify({node_ids:nodeIds,name:$('mergeTreeName').value.trim(),branches})});mergeTreeNodeIds.clear();updateCurrent(result.campaign);$('closeMergeTree').click();toast(`旧树已删除；重分 ${result.merged_count} 人，回流 ${result.released_count} 人`)}catch(error){toast(error.message,'error')}};
$('closeUsers').onclick=()=> $('usersModal').classList.remove('show');$('usersModal').onclick=event=>{if(event.target===$('usersModal'))$('usersModal').classList.remove('show')};
async function boot(){
    loading(true,'正在验证管理后台登录状态…');
    let authenticated=false;
    for(const candidate of tokenCandidates){
        token=candidate;
        try{
            await refresh(true);
            authenticated=true;
            break;
        }catch{}
    }
    if(!authenticated){
        token='';
        $('authWarning').style.display='block';
        loading(false);
        return;
    }
    $('authWarning').style.display='none';
    await loadOverrides().catch(()=>{});
    await loadWallLog().catch(()=>{});
    loading(false);
    setInterval(()=>{
        if(!document.hidden&&!refreshing){
            refreshing=true;
            refresh(false).catch(()=>{}).finally(()=>refreshing=false);
        }
    },5000);
}
boot().catch(error=>{loading(false);toast(error.message,'error')});
</script>
</body>
</html>
