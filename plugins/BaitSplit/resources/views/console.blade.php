<!doctype html>
<html lang="zh-CN">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>节点域名调度中心</title>
    <style>
        :root{--bg:#f3f6fb;--card:#fff;--text:#172033;--muted:#68758a;--line:#dfe5ee;--primary:#3157d5;--danger:#c9364f;--success:#138a5b;--warning:#b66a09}
        *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
        button,input,select,textarea{font:inherit}button{padding:9px 15px;color:#fff;background:var(--primary);border:0;border-radius:9px;cursor:pointer}button:disabled{opacity:.45;cursor:not-allowed}
        button.secondary{color:var(--text);background:#e9edf5}button.danger{background:var(--danger)}button.warning{background:var(--warning)}button.success{background:var(--success)}button.small{padding:5px 9px;font-size:12px}
        input,select,textarea{width:100%;padding:10px 12px;color:var(--text);background:#fff;border:1px solid var(--line);border-radius:9px;outline:none}input,select{height:42px}textarea{min-height:90px;resize:vertical}
        input:focus,select:focus,textarea:focus{border-color:var(--primary);box-shadow:0 0 0 3px rgba(49,87,213,.12)}
        .page{width:min(1320px,calc(100% - 32px));margin:26px auto 60px}.topbar,.row,.actions{display:flex;align-items:center;gap:10px}.topbar{justify-content:space-between;margin-bottom:18px}.actions{flex-wrap:wrap}
        h1{margin:0;font-size:25px}h2{margin:0 0 14px;font-size:17px}h3{margin:0 0 8px;font-size:15px}.muted,.hint{color:var(--muted)}.back{color:var(--primary);text-decoration:none}
        .grid{display:grid;grid-template-columns:repeat(12,1fr);gap:15px}.card{grid-column:span 6;padding:19px;background:var(--card);border:1px solid var(--line);border-radius:14px;box-shadow:0 5px 20px rgba(22,32,51,.05)}.wide{grid-column:1/-1}.third{grid-column:span 4}
        .field{margin-bottom:14px}.field label{display:block;margin-bottom:6px;font-weight:600}.campaignbar{display:grid;grid-template-columns:1fr auto auto;gap:9px}
        .group-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px}.group-option{display:flex!important;align-items:center;gap:8px;padding:9px 11px;margin:0!important;background:#f7f9fc;border:1px solid var(--line);border-radius:9px;cursor:pointer}.group-option input{width:16px;height:16px;margin:0;flex:none}.group-list.cols-5{grid-template-columns:repeat(5,1fr)}.group-list.cols-5 .group-option{min-width:0}.group-list.cols-5 .group-option span{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .stats{display:grid;grid-template-columns:repeat(6,1fr);gap:10px}.stat{padding:12px;background:#f7f9fc;border-radius:10px}.stat strong{display:block;margin-top:3px;font-size:21px}
        .pill{display:inline-block;padding:4px 10px;border-radius:999px;font-weight:700}.pill.on{color:var(--success);background:#dcf6eb}.pill.off{color:var(--muted);background:#e9edf3}.pill.warn{color:#8b5910;background:#fff0cf}.pill.bad{color:#8b1f34;background:#fde8ed}
        .pool-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(245px,1fr));gap:11px}.pool{padding:14px;background:#f7f9fc;border:1px solid var(--line);border-radius:11px}.pool-head{display:flex;justify-content:space-between;gap:8px}.pool .host{margin:7px 0;word-break:break-all}.pool .meta{font-size:12px;color:var(--muted)}.host-tools{display:flex;align-items:center;flex-wrap:wrap;gap:8px}.ping-result{font-size:12px;font-weight:700}.ping-result.ok{color:var(--success)}.ping-result.warn{color:var(--warning)}.ping-result.bad{color:var(--danger)}
        .node-list{max-height:275px;overflow:auto;border:1px solid var(--line);border-radius:10px}.node{display:flex;align-items:center;gap:9px;padding:9px 11px;border-bottom:1px solid #edf0f5;cursor:pointer}.node:last-child{border:0}.node input{width:16px;height:16px}.node small{margin-left:auto;color:var(--muted)}
        table{width:100%;border-collapse:collapse}th,td{padding:8px;text-align:left;border-bottom:1px solid var(--line)}.scroll{max-height:350px;overflow:auto}
        .empty{padding:18px;text-align:center;color:var(--muted);background:#f7f9fc;border-radius:10px}.notice{display:none;padding:12px;border-radius:9px}.notice.error{color:#8b1f34;background:#fde8ed}
        .toast{position:fixed;top:18px;left:50%;z-index:10020;max-width:calc(100% - 32px);padding:12px 18px;border-radius:10px;box-shadow:0 8px 28px rgba(22,32,51,.2);opacity:0;visibility:hidden;transform:translate(-50%,-10px);transition:.2s;pointer-events:none}.toast.show{opacity:1;visibility:visible;transform:translate(-50%,0)}.toast.success{color:#096640;background:#dff7ec}.toast.error{color:#8b1f34;background:#fde8ed}
        .overlay,.modal{display:none;position:fixed;inset:0;z-index:10000;align-items:center;justify-content:center;padding:16px;background:rgba(23,32,51,.28)}.overlay.show,.modal.show{display:flex}.loading-box{display:flex;gap:10px;align-items:center;padding:13px 18px;background:#fff;border-radius:10px}.spinner{width:18px;height:18px;border:2px solid #dce3f5;border-top-color:var(--primary);border-radius:50%;animation:spin .7s linear infinite}.modal{z-index:10010}.modal-card{width:min(1000px,100%);max-height:88vh;overflow:auto;padding:20px;background:#fff;border-radius:14px}.modal-head{display:flex;justify-content:space-between;align-items:center;gap:10px}.modal-tools{display:grid;grid-template-columns:1fr auto;gap:9px;margin:12px 0}.pagination{display:flex;align-items:center;justify-content:flex-end;gap:10px;margin-top:12px}@keyframes spin{to{transform:rotate(360deg)}}
        .split{display:grid;grid-template-columns:1fr 1fr;gap:14px}.version{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
        .tree-list{display:grid;gap:10px}.tree-node{padding:13px;background:#f7f9fc;border:1px solid var(--line);border-left:4px solid var(--primary);border-radius:10px}.tree-node .meta{color:var(--muted);font-size:12px}.branch-fields{display:grid;gap:9px}.branch-row{display:grid;grid-template-columns:160px 1fr;gap:9px}
        @media(max-width:900px){.card,.third{grid-column:1/-1}.stats{grid-template-columns:repeat(2,1fr)}.split{grid-template-columns:1fr}.campaignbar{grid-template-columns:1fr 1fr}.campaignbar select{grid-column:1/-1}.topbar{align-items:flex-start;flex-direction:column}}
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
                <div class="actions"><span id="routerStatus" class="pill off">未初始化</span><span id="configVersion" class="pill off version">v0</span></div>
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

        <section class="card">
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

        <section class="card">
            <h2>全量接管</h2>
            <div id="routerMissing">
                <p class="hint">初始化不会立即切换线上域名。配置完默认、危险、测试和应急池后再启用。</p>
                <button id="initializeRouter">初始化调度系统</button>
            </div>
            <div id="routerControls" style="display:none">
                <p>目标组内所有受管节点都由本插件返回域名；配置异常时隐藏节点，不泄露系统原域名。</p>
                <div class="actions">
                    <button id="toggleRouter" class="success">启用接管</button>
                    <button id="syncUsers" class="secondary">同步用户</button>
                    <button id="rollbackConfig" class="warning">回滚配置</button>
                </div>
            </div>
        </section>

        <section class="card wide">
            <div class="topbar"><h2>用户池与域名</h2><button id="newPool">新增用户池</button></div>
            <div id="poolGrid" class="pool-grid"></div>
        </section>

        <section class="card">
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
            <div id="investigationTree" class="tree-list" style="margin-top:12px"></div>
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
                <h2>跟墙检测（被墙自动隔离）</h2>
                <div class="actions"><span id="wallTracked" class="pill off">累计跟踪 0 人</span><span id="wallPending" class="pill off">换IP队列 0</span><button id="refreshWall" class="secondary">刷新</button></div>
            </div>
            <div class="hint">AWS 换 IP 时 Webhook 带 <code>reason=blocked</code> 视为被墙：曾在死 IP 存活期内拉取过订阅的用户累计跟墙分，达阈值自动移入观察3；<code>reason=machine</code>（机器挂壁）不计分。</div>
            <div class="split" style="margin-top:12px">
                <div class="field"><label><input id="wallAutoIsolate" type="checkbox" style="width:auto;height:auto"> 达阈值自动隔离</label></div>
                <div class="field"><label>跟墙自动隔离目标池</label><select id="wallObservePool"></select></div>
            </div>
            <div class="split">
                <div class="field"><label>跟墙隔离阈值（命中次数）</label><input id="wallThreshold" type="number" min="1" max="50" value="2"></div>
                <div class="field"><label>被墙前回溯窗口（秒）</label><input id="wallLookback" type="number" min="60" max="86400" value="3600"></div>
            </div>
            <div class="split">
                <div class="field"><label>IP 新鲜阈值（秒，超过视为老 IP 首墙只换不记分）</label><input id="wallFresh" type="number" min="300" max="86400" value="7200"></div>
            </div>

            <h3 style="margin-top:20px">二分收敛诱捕（首墙全员可用）<span id="decoyState" class="pill off" style="margin-left:8px">未启用</span></h3>
            <div class="hint">首墙：新 IP 覆盖全组（普通人能用），不开测。二墙：拉过武装 IP 的人里抽一批继续测，其余嫌疑先锁进隔离中转池（观察3，白天不收回）；来源池只留相对干净的人用最新 IP。再被墙 → 对半拆续测，缩到下限进危险组。host 始终跟最新 IP，到点只收回金丝雀测试覆盖。</div>
            <div class="split" style="margin-top:12px">
                <div class="field"><label><input id="decoyEnabled" type="checkbox" style="width:auto;height:auto"> 启用夜间自动诱捕（每分钟调度检查）</label></div>
                <div class="field"><label>诱捕时段（本地时间）</label><div class="host-tools"><input id="decoyStart" placeholder="01:00" style="flex:1;min-width:0"><span>—</span><input id="decoyEnd" placeholder="08:00" style="flex:1;min-width:0"></div></div>
            </div>
            <div class="split">
                <div class="field"><label>初始每批人数</label><input id="decoyBatch" type="number" min="1" max="5000" value="60"></div>
                <div class="field"><label>收敛下限（人）</label><input id="decoyMinBatch" type="number" min="1" max="5000" value="8"></div>
            </div>
            <div class="split">
                <div class="field"><label>每批观察时长（分钟）</label><input id="decoyObserve" type="number" min="5" max="480" value="40"></div>
                <div class="field"><label>隔离中转池（被墙批先移入这里续测）</label><select id="decoyIsolate"></select></div>
            </div>
            <div class="field"><label>确认内鬼隔离池（缩到下限仍被墙 → 移入此池）</label><select id="decoyConfirm"></select></div>
            <div class="field"><label>嫌疑来源池（内鬼现在藏在哪些池，可多选，各自独立并行）</label><div id="decoySource" class="group-list cols-5"></div></div>

            <button id="saveWallSettings">保存跟墙 / 诱捕设置</button>

            <h3 style="margin-top:20px">各来源池诱捕进度</h3>
            <div id="decoyGroups" class="pool-grid"></div>

            <h3 style="margin-top:20px">嫌疑榜（跟墙分 Top 50）</h3>
            <div class="scroll" style="max-height:280px"><table><thead><tr><th>用户 ID</th><th>邮箱</th><th>跟墙分</th><th>最近命中</th><th>操作</th></tr></thead><tbody id="wallSuspects"></tbody></table></div>

            <h3 style="margin-top:20px">换 IP 事件日志（最近 100 条）</h3>
            <div class="scroll" style="max-height:320px"><table><thead><tr><th>时间</th><th>类型</th><th>旧IP→新IP</th><th>受影响池</th><th>受害/计分</th><th>本次隔离</th></tr></thead><tbody id="wallEvents"></tbody></table></div>
        </section>
    </div>
</main>
<script>
const API_BASE=@json($apiBase);
let token='';
const tokenCandidates=readTokens();
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
function renderStatus(){const r=router();$('eligibleCount').textContent=current?.eligible_count||0;$('pulledUserCount').textContent=r?.pulled_user_count||0;$('groupedUserCount').textContent=r?.grouped_user_count||0;$('unpulledUngroupedCount').textContent=r?.unpulled_ungrouped_count||0;$('poolCount').textContent=r?.pools.length||0;$('untestedCount').textContent=r?.untested_count||0;$('configVersion').textContent=`v${r?.config_version||0}`;$('routerStatus').textContent=!r?'未初始化':r.enabled?'全量接管中':'未启用';$('routerStatus').className=`pill ${r?.enabled?'on':'off'}`;$('routerMissing').style.display=r?'none':'block';$('routerControls').style.display=r?'block':'none';if(r){$('toggleRouter').textContent=r.enabled?'危险：恢复系统原域名':'启用全量接管';$('toggleRouter').className=r.enabled?'danger':'success'}}
function renderPools(){const grid=$('poolGrid');grid.textContent='';const list=pools();if(!list.length){grid.innerHTML='<div class="empty">初始化后配置用户池</div>';return}list.forEach(pool=>{const card=document.createElement('div');card.className='pool';const head=document.createElement('div');head.className='pool-head';const title=document.createElement('strong');title.textContent=pool.name;const state=document.createElement('span');state.className=`pill ${pool.status==='blocked'?'bad':pool.enabled?'on':'off'}`;state.textContent=poolStatusName(pool.status);head.append(title,state);const host=document.createElement('div');renderPingTarget(host,pool.host,`${Object.keys(pool.node_hosts||{}).length} 个节点独立地址`);const overflowName=pools().find(item=>item.id===pool.overflow_pool_id)?.name;const metaLine=document.createElement('div');metaLine.className='meta';metaLine.textContent=`${poolTypeName(pool.type)} · ${pool.member_count} 人 · ${pool.pulled_count} 已拉取 · 容量 ${pool.capacity||'不限'} · 接口标识 ${pool.webhook_id||pool.id}${overflowName?` · 满后→${overflowName}`:''}`;const actions=document.createElement('div');actions.className='actions';const actionItems=[['复制接口标识','copy-id'],['编辑','edit'],['用户','users']];if(pool.member_count>0&&pool.type!=='blacklist')actionItems.push(['进入树形排查','tree']);if(!['danger','blacklist'].includes(pool.type)){actionItems.push(['转移已拉取','transfer']);actionItems.push(['转移未拉取','transfer-unpulled']);}actionItems.push(['删除','delete']);actionItems.forEach(([label,action])=>{const button=document.createElement('button');button.className='secondary small';button.textContent=label;button.onclick=()=>poolAction(action,pool);if(action==='transfer'&&pool.pulled_count<1)button.disabled=true;if(action==='transfer-unpulled'&&(pool.member_count-pool.pulled_count)<1)button.disabled=true;if(action==='delete'&&['default','danger','probe','emergency','blacklist'].includes(pool.id))button.disabled=true;actions.appendChild(button)});card.append(head,host,metaLine,actions);grid.appendChild(card)})}
function renderPoolOverflowOptions(currentId='',value=''){const type=$('poolType').value;fillSelect('poolOverflow',pools().filter(item=>item.id!==currentId&&!['danger','blacklist'].includes(type)&&!['danger','blacklist'].includes(item.type)),value,'不自动转入')}
function editPool(pool=null){$('poolId').value=pool?.id||'';$('poolName').value=pool?.name||'';$('poolType').value=pool?.type||'safe';$('poolType').disabled=['default','danger','probe','emergency','blacklist'].includes(pool?.id||'');$('poolHost').value=pool?.host||'';$('poolWebhookId').value=pool?.webhook_id||'';$('poolStatus').value=pool?.status||'available';$('poolCapacity').value=pool?.capacity||0;renderPoolOverflowOptions(pool?.id||'',pool?.overflow_pool_id||'');$('poolEnabled').checked=pool?.enabled??true;$('poolNote').value=pool?.note||'';window.scrollTo({top:$('poolName').getBoundingClientRect().top+window.scrollY-100,behavior:'smooth'})}
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
        card.className='tree-node';
        card.style.marginLeft=`${Math.min(node.depth,8)*24}px`;
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
            if(node.status!=='safe')add('确认安全',()=>changeTreeStatus(node,'safe'),'success');
            if(node.status!=='blocked')add('标记被墙',()=>changeTreeStatus(node,'blocked'),'danger');
            if(['active','blocked'].includes(node.status))add('继续细分',()=>openSplitTree(node));
            if(['active','safe'].includes(node.status)&&node.pulled_count>0)add('迁移已拉取用户',()=>openTreeTransfer(node,true),'success');
            if(['active','safe'].includes(node.status)&&node.user_count>node.pulled_count)add('迁移未拉取用户',()=>openTreeTransfer(node,false),'warning');
        }
        if(node.depth===0&&node.status!=='archived')add('删除排查树',()=>deleteInvestigationTree(node),'danger');
        card.append(head,host,metaLine,actions);
        list.appendChild(card);
    });
    updateMergeTreeButton()
}
async function changeTreeStatus(node,status){const label=status==='safe'?'安全':'被墙';if(!confirm(`确认把“${node.name}”标记为${label}？${status==='blocked'?'\n只有已拉取该分支域名的用户会留下排查；未拉取用户下次订阅时重新分组。':''}`))return;try{const result=await request(api(`/investigations/${encodeURIComponent(node.id)}/status`),{method:'POST',body:JSON.stringify({status})});updateCurrent(result.campaign);toast(status==='blocked'?`已保留 ${result.suspect_count} 名嫌疑用户，${result.released_count} 名未拉取用户已回流`:`节点已标记为${label}`)}catch(error){toast(error.message,'error')}}
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
function resetTaskEditors(){$('poolId').value='';$('poolName').value='';$('poolHost').value='';$('poolWebhookId').value='';$('poolCapacity').value=0;$('poolNote').value='';$('userSearch').value='';$('searchResults').textContent='';$('overrideUser').value='';delete $('overrideUser').dataset.id;$('overrideHost').value='';$('overrideNote').value='';$('overrideExpires').value='';$('usersModal').classList.remove('show');$('transferModal').classList.remove('show');$('splitTreeModal').classList.remove('show');$('mergeTreeModal').classList.remove('show');$('treeHostModal').classList.remove('show');transferSource=null;splitTreeNodeId='';mergeTreeNodeIds.clear();updateMergeTreeButton();poolModal={poolId:'',poolName:'',page:1,lastPage:1,total:0,q:''}}
function renderCurrent(){current||=blankCampaign();$('campaignName').value=current.name||'';$('campaignIdDisplay').value=current.id||'';$('copyCampaignId').disabled=!current.id;renderGroups();renderNodes();renderStatus();renderPools();renderInvestigationTree();renderOverridePoolOptions();$('deleteCampaign').disabled=!current.id||router()?.enabled}
function updateCurrent(campaign){const index=campaigns.findIndex(item=>item.id===campaign.id);if(index>=0)campaigns[index]=campaign;else campaigns.push(campaign);current=campaign;renderCampaigns();renderCurrent()}
async function refresh(full=true){const id=current?.id,isDraft=current&&current.id==='';if(full){const data=await request('/meta');meta={groups:data.groups,servers:data.servers};campaigns=data.campaigns}else campaigns=await request('/campaigns');if(isDraft&&!full){renderCampaigns();return}current=campaigns.find(item=>item.id===id)||campaigns[0]||blankCampaign();renderCampaigns();if(full)renderCurrent();else{renderStatus();renderPools();renderInvestigationTree()}}
async function showTreeUsers(node=null,page=null){if(node){poolModal={nodeId:node.id,poolName:node.name,page:1,lastPage:1,total:0,q:'',mode:'tree'};$('poolUserSearch').value=''}if(!poolModal.nodeId)return;if(page!==null)poolModal.page=page;const campaignId=current?.id;loading(true,'正在加载排查节点用户…');try{const result=await request(api(`/investigations/${encodeURIComponent(poolModal.nodeId)}/users?q=${encodeURIComponent(poolModal.q)}&page=${poolModal.page}&per_page=50`));if(current?.id!==campaignId)return;poolModal.page=result.pagination.page;poolModal.lastPage=result.pagination.last_page;poolModal.total=result.pagination.total;$('poolUserTools').style.display='grid';$('poolUserPagination').style.display='flex';$('usersTitle').textContent=`${poolModal.poolName}用户（${poolModal.total}）`;$('usersStateHead').textContent='拉取状态';$('usersActionHead').textContent='操作';$('usersBody').textContent='';result.items.forEach(user=>{const row=$('usersBody').insertRow();row.insertCell().textContent=user.id;row.insertCell().textContent=user.email;row.insertCell().textContent=user.exposed?'已拉取':'未拉取';row.insertCell().textContent=user.pull_count||0;row.insertCell().textContent=formatTime(user.last_pulled_at);row.insertCell().textContent='-'});$('poolUserPage').textContent=`第 ${poolModal.page} / ${poolModal.lastPage} 页`;$('prevPoolUsers').disabled=poolModal.page<=1;$('nextPoolUsers').disabled=poolModal.page>=poolModal.lastPage;$('usersModal').classList.add('show')}catch(error){toast(error.message,'error')}finally{loading(false)}}
function reloadPagedUsers(page){return poolModal.mode==='tree'?showTreeUsers(null,page):showPoolUsers(null,page)}
async function showPoolUsers(pool=null,page=null){if(pool){poolModal={poolId:pool.id,poolName:pool.name,page:1,lastPage:1,total:0,q:''};$('poolUserSearch').value=''}if(!poolModal.poolId)return;if(page!==null)poolModal.page=page;const campaignId=current?.id;loading(true,'正在加载用户…');try{const result=await request(api(`/pools/${encodeURIComponent(poolModal.poolId)}/users?q=${encodeURIComponent(poolModal.q)}&page=${poolModal.page}&per_page=50`));if(current?.id!==campaignId)return;poolModal.page=result.pagination.page;poolModal.lastPage=result.pagination.last_page;poolModal.total=result.pagination.total;$('poolUserTools').style.display='grid';$('poolUserPagination').style.display='flex';$('usersTitle').textContent=`${poolModal.poolName}用户（${poolModal.total}）`;$('usersStateHead').textContent='当前组';$('usersActionHead').textContent='移动到';$('usersBody').textContent='';result.items.forEach(user=>{const row=$('usersBody').insertRow();row.insertCell().textContent=user.id;row.insertCell().textContent=user.email;row.insertCell().textContent=user.pulled?'已拉取':'未拉取';row.insertCell().textContent=user.pull_count>0?user.pull_count:user.pulled?'历史已拉取':'0';row.insertCell().textContent=formatTime(user.last_pulled_at);const cell=row.insertCell(),wrap=document.createElement('div'),select=document.createElement('select'),button=document.createElement('button');wrap.className='actions';select.style.minWidth='130px';pools().filter(item=>item.enabled&&item.status!=='blocked').forEach(item=>option(select,item.id,item.name,item.id===poolModal.poolId));button.className='small';button.textContent='移动';button.onclick=async()=>{if(select.value===poolModal.poolId)return toast('用户已经在该组','error');if(!confirm(`把 ${user.email} 移动到“${select.options[select.selectedIndex].textContent}”？`))return;try{updateCurrent(await request(api(`/overrides/${user.id}`),{method:'POST',body:JSON.stringify({pool_id:select.value,host:'',node_hosts:{},server_name:'',transport_host:'',locked:true,note:`从${poolModal.poolName}手动移动`,expires_at:0})}));await showPoolUsers(null,poolModal.page);await loadOverrides();toast('用户已移动')}catch(error){toast(error.message,'error')}};wrap.append(select,button);cell.appendChild(wrap)});if(!result.items.length){const row=$('usersBody').insertRow(),cell=row.insertCell();cell.colSpan=6;cell.className='hint';cell.textContent='没有符合条件的用户'}$('poolUserPage').textContent=`第 ${poolModal.page} / ${poolModal.lastPage} 页，共 ${poolModal.total} 人`;$('prevPoolUsers').disabled=poolModal.page<=1;$('nextPoolUsers').disabled=poolModal.page>=poolModal.lastPage;$('usersModal').classList.add('show')}catch(error){toast(error.message,'error')}finally{loading(false)}}
async function loadOverrides(){const campaignId=current?.id,body=$('overrideRows');if(!body)return;if(!campaignId||!router()){body.textContent='';return}const rows=await request(api('/overrides'));if(current?.id!==campaignId)return;body.textContent='';rows.forEach(user=>{const row=body.insertRow();row.insertCell().textContent=`${user.id} / ${user.email}`;row.insertCell().textContent=user.pool_id||'-';row.insertCell().textContent=user.override.host||Object.values(user.override.node_hosts||{}).join(', ')||'-';row.insertCell().textContent=user.override.locked?'是':'否';row.insertCell().textContent=user.override.note||'-';const cell=row.insertCell(),button=document.createElement('button');button.className='danger small';button.textContent='解除';button.onclick=async()=>{try{updateCurrent(await request(api(`/overrides/${user.id}`),{method:'DELETE'}));await loadOverrides();toast('规则已解除')}catch(error){toast(error.message,'error')}};cell.appendChild(button)});if(!rows.length){const row=body.insertRow(),cell=row.insertCell();cell.colSpan=6;cell.className='hint';cell.textContent='暂无手动规则'}}
let wallData=null;
function wallReasonLabel(reason){return {blocked:'被墙',machine:'机器挂壁'}[reason]||reason||'-'}
function renderWall(){const events=$('wallEvents'),suspects=$('wallSuspects');if(!wallData){$('wallTracked').textContent='累计跟踪 0 人';$('wallTracked').className='pill off';if($('wallPending')){$('wallPending').textContent='换IP队列 0';$('wallPending').className='pill off'}events.textContent='';suspects.textContent='';return}const settings=wallData.settings||{};$('wallAutoIsolate').checked=!!settings.auto_isolate;$('wallThreshold').value=settings.hit_threshold||2;$('wallLookback').value=settings.lookback_seconds||3600;if(document.activeElement!==$('wallFresh'))$('wallFresh').value=settings.fresh_max_seconds||7200;if(document.activeElement!==$('wallObservePool'))fillSelect('wallObservePool',pools(),settings.observe_pool_raw||'','自动（第一个观察组）');$('wallTracked').textContent=`累计跟踪 ${wallData.total_tracked||0} 人`;$('wallTracked').className=`pill ${wallData.total_tracked?'warn':'off'}`;const pending=Number(wallData.pending_ip_rotates||0);if($('wallPending')){$('wallPending').textContent=`换IP队列 ${pending}`;$('wallPending').className=`pill ${pending>0?'bad':'off'}`;$('wallPending').title=pending>0?'有换 IP 事件排队等待写入，每分钟自动消化':'无积压换 IP 事件'}
suspects.textContent='';(wallData.top_suspects||[]).forEach(item=>{const row=suspects.insertRow();row.insertCell().textContent=item.user_id;row.insertCell().textContent=item.email||'-';const hitCell=row.insertCell();hitCell.textContent=item.nights?`${item.hits} / ${item.nights}轮`:item.hits;hitCell.title=`跟墙命中 ${item.hits} 次，参与诱捕测试 ${item.nights||0} 轮（同一晚可能多轮）`;if(item.hits>=(settings.hit_threshold||2))hitCell.innerHTML=`<span class="pill bad">${hitCell.textContent}</span>`;row.insertCell().textContent=formatTime(item.last_at);const cell=row.insertCell(),button=document.createElement('button');button.className='danger small';button.textContent='隔离到观察3';button.onclick=()=>isolateSuspect(item.user_id);cell.appendChild(button)});if(!(wallData.top_suspects||[]).length){const row=suspects.insertRow();row.insertCell().colSpan=5;row.cells[0].className='empty';row.cells[0].textContent='暂无跟墙嫌疑用户'}
events.textContent='';(wallData.events||[]).forEach(ev=>{const row=events.insertRow();const timeCell=row.insertCell();timeCell.textContent=formatTime(ev.at);if(ev.mode==='decoy'){const ph=ev.phase||(ev.pools||[]).find(p=>p&&p.phase)?.phase||'';timeCell.innerHTML+=ph==='armed'?' <span class="pill warn">武装</span>':(ph==='testing'?' <span class="pill on">测</span>':' <span class="pill warn">诱</span>')}if(ev.mode==='manual_fix')timeCell.innerHTML+=' <span class="pill warn">补</span>';if((ev.pools||[]).some(p=>p&&p.stale))timeCell.innerHTML+=' <span class="pill off" title="老 IP 首墙，只换 IP 不记分">跳过</span>';const reasonCell=row.insertCell();reasonCell.innerHTML=`<span class="pill ${ev.reason==='blocked'?'bad':'off'}">${wallReasonLabel(ev.reason)}</span>`;row.insertCell().textContent=`${ev.old_ip||'-'} → ${ev.new_ip||'-'}`;row.insertCell().textContent=(ev.pools||[]).map(p=>typeof p==='string'?p:p.pool_name).join('、')||'-';row.insertCell().textContent=`${ev.suspect_count||0} / ${ev.scored_count||0}`;row.insertCell().textContent=ev.moved_count||0});if(!(wallData.events||[]).length){const row=events.insertRow();row.insertCell().colSpan=6;row.cells[0].className='empty';row.cells[0].textContent='暂无换 IP 事件记录'}
renderDecoy()}
function renderDecoy(){const d=wallData?.decoy;if(!d)return;const st=$('decoyState');if(d.enabled){if((d.testing_count||0)>0){st.textContent=`诱捕中·在测 ${d.testing_count} 人`;st.className='pill on'}else if(d.in_window){st.textContent='时段内·待被墙触发';st.className='pill warn'}else{st.textContent='已启用·白天';st.className='pill off'}}else{st.textContent='未启用';st.className='pill off'}
if(document.activeElement!==$('decoyEnabled'))$('decoyEnabled').checked=!!d.enabled;
if(document.activeElement!==$('decoyStart'))$('decoyStart').value=d.start||'01:00';
if(document.activeElement!==$('decoyEnd'))$('decoyEnd').value=d.end||'08:00';
if(document.activeElement!==$('decoyBatch'))$('decoyBatch').value=d.batch_size||60;
if(document.activeElement!==$('decoyMinBatch'))$('decoyMinBatch').value=d.min_batch||8;
if(document.activeElement!==$('decoyObserve'))$('decoyObserve').value=d.observe_minutes||40;
const all=pools();
fillSelect('decoyConfirm',all,d.raw?.confirm||'','自动（危险组）');
fillSelect('decoyIsolate',all,d.raw?.isolate||'','自动（第一个观察组）');
const srcBox=$('decoySource');srcBox.textContent='';const srcSel=new Set((d.source_pool_ids||[]).map(String));all.forEach(p=>{const label=document.createElement('label');label.className='group-option';const input=document.createElement('input');input.type='checkbox';input.className='decoySource';input.value=p.id;input.checked=srcSel.has(String(p.id));const span=document.createElement('span');span.textContent=`${p.name}（${p.host||'无IP'}）`;span.title=`${p.name}（${p.host||'无IP'}）`;label.append(input,span);srcBox.appendChild(label)});
const g=$('decoyGroups');g.textContent='';(d.domains||[]).forEach(dm=>{const card=document.createElement('div');card.className='pool';const head=document.createElement('div');head.className='pool-head';const t=document.createElement('strong');t.textContent=dm.pool_name;const s=document.createElement('span');const phase=dm.phase||'idle';const testing=(dm.current_batch||0)>0;let label='未触发',cls='off';if(phase==='armed'){label='已武装·待二墙';cls='warn'}else if(testing){label=`在测 ${dm.current_batch} 人`;cls='on'}else if(phase==='testing'){label=dm.cur_ip?'测试中·待抽批':'测试中';cls='warn'}s.className=`pill ${cls}`;s.textContent=label;head.append(t,s);const meta=document.createElement('div');meta.className='meta';meta.textContent=`阶段 ${phase} · 武装IP ${dm.armed_ip||'-'} · 当前IP ${dm.cur_ip||'-'} · 本批 ${dm.current_level||'-'} 人 · 待测 ${dm.queue_users||0} 人`;card.append(head,meta);g.appendChild(card)});if(!(d.domains||[]).length){g.innerHTML='<div class="empty">暂无来源池（请勾选嫌疑来源池并启用）</div>'}}
async function loadWallLog(){const campaignId=current?.id;if(!campaignId||!router()){wallData=null;renderWall();return}const data=await request(api('/wall-log?limit=100'));if(current?.id!==campaignId)return;wallData=data;renderWall()}
async function isolateSuspect(userId){const poolId=wallData?.settings?.observe_pool_id||pools().find(p=>p.type==='observation'&&/观察3/.test(p.name))?.id;if(!poolId)return toast('未找到观察3用户池，请先配置隔离目标池','error');if(!confirm(`把用户 ${userId} 锁定隔离到该观察池？`))return;try{updateCurrent(await request(api(`/overrides/${userId}`),{method:'POST',body:JSON.stringify({pool_id:poolId,host:'',node_hosts:{},server_name:'',transport_host:'',locked:true,note:'跟墙嫌疑手动隔离',expires_at:0})}));toast('已隔离该用户');await loadWallLog()}catch(error){toast(error.message,'error')}}
$('refreshWall').onclick=()=>loadWallLog().catch(error=>toast(error.message,'error'));
$('saveWallSettings').onclick=async()=>{try{const decoySources=[...document.querySelectorAll('.decoySource:checked')].map(i=>i.value);if($('decoyEnabled').checked&&!decoySources.length)throw new Error('启用诱捕需勾选至少 1 个嫌疑来源池');const body={wall_auto_isolate:$('wallAutoIsolate').checked,wall_hit_threshold:Number($('wallThreshold').value)||2,wall_lookback_seconds:Number($('wallLookback').value)||3600,wall_fresh_max_seconds:Number($('wallFresh').value)||7200,wall_observe_pool_id:$('wallObservePool').value,decoy_enabled:$('decoyEnabled').checked,decoy_source_pool_ids:decoySources.join(','),decoy_isolate_pool_id:$('decoyIsolate').value,decoy_confirm_pool_id:$('decoyConfirm').value,decoy_batch_size:Number($('decoyBatch').value)||60,decoy_min_batch:Number($('decoyMinBatch').value)||8,decoy_observe_minutes:Number($('decoyObserve').value)||40,decoy_start:$('decoyStart').value.trim()||'01:00',decoy_end:$('decoyEnd').value.trim()||'08:00'};await request('/wall-settings',{method:'POST',body:JSON.stringify(body)});toast('跟墙 / 诱捕设置已保存');await loadWallLog()}catch(error){toast(error.message,'error')}};
$('campaignSelect').onchange=async event=>{const selectedId=event.target.value;try{loading(true,'正在切换任务…');await refresh(false);current=campaigns.find(item=>item.id===selectedId)||blankCampaign();resetTaskEditors();renderCampaigns();renderCurrent();await loadOverrides();await loadWallLog().catch(()=>{})}catch(error){toast(error.message,'error')}finally{loading(false)}};
$('newCampaign').onclick=()=>{current=blankCampaign();resetTaskEditors();renderCampaigns();renderCurrent()};
$('copyCampaignId').onclick=async()=>{const id=current?.id||$('campaignIdDisplay').value;if(!id)return toast('请先保存任务','error');try{await navigator.clipboard.writeText(id);toast(`campaign_id 已复制：${id}`)}catch{toast('复制失败，请手动选中复制','error')}};
$('deleteCampaign').onclick=async()=>{try{if(!confirm(`删除“${current.name}”？`))return;campaigns=await request(api(''),{method:'DELETE'});current=campaigns[0]||blankCampaign();resetTaskEditors();renderCampaigns();renderCurrent();await loadOverrides();toast('任务已删除')}catch(error){toast(error.message,'error')}};
$('saveCampaign').onclick=async()=>{try{const name=$('campaignName').value.trim(),target_group_ids=selectedGroupIds();if(!name)throw new Error('请填写任务名称');if(!target_group_ids.length)throw new Error('请至少勾选一个用户主权限组');const result=await request('/campaigns',{method:'POST',body:JSON.stringify({campaign_id:current.id||null,name,target_group_ids,excluded_server_ids:excludedServerIds()})});updateCurrent(result);toast('任务已保存')}catch(error){toast(error.message,'error')}};
$('initializeRouter').onclick=async()=>{try{if(!current.id)throw new Error('请先保存任务');updateCurrent(await request(api('/router/initialize'),{method:'POST',body:'{}'}));toast('调度系统已初始化，请先配置用户池域名')}catch(error){toast(error.message,'error')}};
$('toggleRouter').onclick=async()=>{try{const enable=!router().enabled;if(!confirm(enable?'启用后目标组将完全使用插件域名，确认配置完整？':'警告：停止接管会立即向用户恢复系统原始节点域名，确认继续？'))return;updateCurrent(await request(api('/router/toggle'),{method:'POST',body:JSON.stringify({enabled:enable})}));toast(enable?'全量接管已启用':'已恢复系统原始域名')}catch(error){toast(error.message,'error')}};
$('syncUsers').onclick=async()=>{try{updateCurrent(await request(api('/router/sync-users'),{method:'POST',body:'{}'}));toast('用户已同步')}catch(error){toast(error.message,'error')}};
$('rollbackConfig').onclick=async()=>{try{if(!confirm('回滚到上一版用户池配置？'))return;updateCurrent(await request(api('/router/rollback'),{method:'POST',body:'{}'}));toast('配置已回滚')}catch(error){toast(error.message,'error')}};
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
