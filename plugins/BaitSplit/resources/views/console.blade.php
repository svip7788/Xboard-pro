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
        .group-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px}.group-option{display:flex!important;align-items:center;gap:8px;padding:9px 11px;margin:0!important;background:#f7f9fc;border:1px solid var(--line);border-radius:9px;cursor:pointer}.group-option input{width:16px;height:16px;margin:0}
        .stats{display:grid;grid-template-columns:repeat(6,1fr);gap:10px}.stat{padding:12px;background:#f7f9fc;border-radius:10px}.stat strong{display:block;margin-top:3px;font-size:21px}
        .pill{display:inline-block;padding:4px 10px;border-radius:999px;font-weight:700}.pill.on{color:var(--success);background:#dcf6eb}.pill.off{color:var(--muted);background:#e9edf3}.pill.warn{color:#8b5910;background:#fff0cf}.pill.bad{color:#8b1f34;background:#fde8ed}
        .pool-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(245px,1fr));gap:11px}.pool{padding:14px;background:#f7f9fc;border:1px solid var(--line);border-radius:11px}.pool-head{display:flex;justify-content:space-between;gap:8px}.pool .host{margin:7px 0;word-break:break-all}.pool .meta{font-size:12px;color:var(--muted)}
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
        <div class="modal-head"><h2>转移已拉取用户</h2><button id="closeTransfer" class="secondary">关闭</button></div>
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
            <div class="field"><label>用户主权限组（可单选或多选）</label><div id="groupSelect" class="group-list"></div></div>
            <p class="hint">启用后自动接管该权限组订阅返回的全部节点，无需选择节点。</p>
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
            <h2>树形分支排查</h2>
            <div class="hint">从上方用户池点击“进入树形排查”，再为每个下级分支配置全新域名。兄弟分支互不影响。</div>
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
            <div class="topbar"><h2>手动用户规则</h2><button id="refreshOverrides" class="secondary">刷新</button></div>
            <div class="scroll"><table><thead><tr><th>用户</th><th>用户池</th><th>域名覆盖</th><th>锁定</th><th>备注</th><th>操作</th></tr></thead><tbody id="overrideRows"></tbody></table></div>
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
function blankCampaign(){const first=meta.groups[0]?.id||0;return{id:'',name:'',target_group_id:first,target_group_ids:first?[first]:[],target_server_ids:[],eligible_count:0,router:null}}
function router(){return current?.router||null}
function pools(types=null){const order={default:0,probe:1,observation:2,safe:3,emergency:4,custom:5,danger:6,blacklist:7},list=[...(router()?.pools||[])].filter(pool=>!pool.tree_node_id).sort((left,right)=>(order[left.type]??5)-(order[right.type]??5));return types?list.filter(pool=>types.includes(pool.type)&&pool.enabled):list}
function formatTime(value){return value?new Date(Number(value)*1000).toLocaleString():'-'}
function option(select,value,label,selected=false){const item=document.createElement('option');item.value=value;item.textContent=label;item.selected=selected;select.appendChild(item)}
function fillSelect(id,items,value='',emptyLabel=''){const select=$(id);select.textContent='';if(emptyLabel)option(select,'',emptyLabel,value==='');items.forEach(item=>option(select,item.id,item.name,item.id===value))}
function groupNames(groupIds){const names=(groupIds||[]).map(id=>meta.groups.find(group=>Number(group.id)===Number(id))?.name).filter(Boolean);return names.join('、')}
function renderCampaigns(){const select=$('campaignSelect');select.textContent='';if(!campaigns.length&&!current?.id)option(select,'','尚未创建任务',true);if(current&&!current.id)option(select,'','新任务（未保存）',true);campaigns.forEach(item=>{const groups=groupNames(item.target_group_ids||[item.target_group_id]);option(select,item.id,`${item.name}${groups?`【${groups}】`:''}${item.router?.enabled?'（接管中）':''}`,item.id===current?.id)})}
function selectedGroupIds(){return [...document.querySelectorAll('.targetGroup:checked')].map(item=>Number(item.value)).filter(Boolean)}
function renderGroups(){const container=$('groupSelect');container.textContent='';const selected=new Set((current?.target_group_ids||[current?.target_group_id]).map(Number));meta.groups.forEach(group=>{const label=document.createElement('label');label.className='group-option';const input=document.createElement('input');input.type='checkbox';input.className='targetGroup';input.value=group.id;input.checked=selected.has(Number(group.id));const text=document.createElement('span');text.textContent=`${group.name}（${group.users_count} 人）`;label.append(input,text);container.appendChild(label)})}
function renderStatus(){const r=router();$('eligibleCount').textContent=current?.eligible_count||0;$('pulledUserCount').textContent=r?.pulled_user_count||0;$('groupedUserCount').textContent=r?.grouped_user_count||0;$('unpulledUngroupedCount').textContent=r?.unpulled_ungrouped_count||0;$('poolCount').textContent=r?.pools.length||0;$('untestedCount').textContent=r?.untested_count||0;$('configVersion').textContent=`v${r?.config_version||0}`;$('routerStatus').textContent=!r?'未初始化':r.enabled?'全量接管中':'未启用';$('routerStatus').className=`pill ${r?.enabled?'on':'off'}`;$('routerMissing').style.display=r?'none':'block';$('routerControls').style.display=r?'block':'none';if(r){$('toggleRouter').textContent=r.enabled?'危险：恢复系统原域名':'启用全量接管';$('toggleRouter').className=r.enabled?'danger':'success'}}
function renderPools(){const grid=$('poolGrid');grid.textContent='';const list=pools();if(!list.length){grid.innerHTML='<div class="empty">初始化后配置用户池</div>';return}list.forEach(pool=>{const card=document.createElement('div');card.className='pool';const head=document.createElement('div');head.className='pool-head';const title=document.createElement('strong');title.textContent=pool.name;const state=document.createElement('span');state.className=`pill ${pool.status==='blocked'?'bad':pool.enabled?'on':'off'}`;state.textContent=pool.status;head.append(title,state);const host=document.createElement('div');host.className='host';host.textContent=pool.host||`${Object.keys(pool.node_hosts||{}).length} 个节点独立地址`;const overflowName=pools().find(item=>item.id===pool.overflow_pool_id)?.name;const metaLine=document.createElement('div');metaLine.className='meta';metaLine.textContent=`${pool.type} · ${pool.member_count} 人 · ${pool.pulled_count} 已拉取 · 容量 ${pool.capacity||'不限'}${overflowName?` · 满后→${overflowName}`:''}`;const actions=document.createElement('div');actions.className='actions';const actionItems=[['编辑','edit'],['用户','users']];if(pool.member_count>0&&pool.type!=='blacklist')actionItems.push(['进入树形排查','tree']);if(!['danger','blacklist'].includes(pool.type))actionItems.push(['转移已拉取','transfer']);actionItems.push(['删除','delete']);actionItems.forEach(([label,action])=>{const button=document.createElement('button');button.className='secondary small';button.textContent=label;button.onclick=()=>poolAction(action,pool);if(action==='transfer'&&pool.pulled_count<1)button.disabled=true;if(action==='delete'&&['default','danger','probe','emergency','blacklist'].includes(pool.id))button.disabled=true;actions.appendChild(button)});card.append(head,host,metaLine,actions);grid.appendChild(card)})}
function renderPoolOverflowOptions(currentId='',value=''){const type=$('poolType').value;fillSelect('poolOverflow',pools().filter(item=>item.id!==currentId&&!['danger','blacklist'].includes(type)&&!['danger','blacklist'].includes(item.type)),value,'不自动转入')}
function editPool(pool=null){$('poolId').value=pool?.id||'';$('poolName').value=pool?.name||'';$('poolType').value=pool?.type||'safe';$('poolHost').value=pool?.host||'';$('poolStatus').value=pool?.status||'available';$('poolCapacity').value=pool?.capacity||0;renderPoolOverflowOptions(pool?.id||'',pool?.overflow_pool_id||'');$('poolEnabled').checked=pool?.enabled??true;$('poolNote').value=pool?.note||'';window.scrollTo({top:$('poolName').getBoundingClientRect().top+window.scrollY-100,behavior:'smooth'})}
function openPoolTransfer(pool){const targets=pools().filter(item=>item.id!==pool.id&&!['danger','blacklist'].includes(item.type)&&item.enabled&&item.status!=='blocked');if(!targets.length)return toast('暂无可用的转入用户池','error');transferSource=pool;fillSelect('transferTarget',targets,'');$('transferHint').textContent=`“${pool.name}”当前有 ${pool.pulled_count} 名已拉取用户；手动锁定用户不会移动。`;$('transferModal').classList.add('show')}
function openTreeTransfer(node){const targets=pools(['observation','safe','custom']).filter(item=>item.status!=='blocked');if(!targets.length)return toast('暂无可用的观察组或安全组','error');transferSource={...node,mode:'tree'};fillSelect('transferTarget',targets,'');$('transferHint').textContent=`“${node.name}”共有 ${node.user_count} 名固定用户；手动锁定用户不会移动。`;$('transferModal').classList.add('show')}
async function poolAction(action,pool){try{if(action==='edit')return editPool(pool);if(action==='users')return showPoolUsers(pool);if(action==='transfer')return openPoolTransfer(pool);if(action==='tree'){if(!confirm(`把“${pool.name}”的 ${pool.member_count} 名固定用户冻结为独立排查根节点？\n创建后还需继续拆分并填写新域名。`))return;updateCurrent(await request(api(`/pools/${encodeURIComponent(pool.id)}/investigation`),{method:'POST',body:JSON.stringify({name:`${pool.name} 排查树`})}));toast('已创建排查根节点，请继续拆分');document.getElementById('investigationTree').scrollIntoView({behavior:'smooth'});return}if(action==='delete'){if(!confirm(`删除“${pool.name}”？`))return;updateCurrent(await request(api(`/pools/${encodeURIComponent(pool.id)}`),{method:'DELETE'}));toast('用户池已删除')}}catch(error){toast(error.message,'error')}}
function renderInvestigationTree(){
    const list=$('investigationTree'),nodes=router()?.investigation_nodes||[];
    list.textContent='';
    if(!nodes.length){list.innerHTML='<div class="empty">暂无树形排查</div>';return}
    const byId=new Map(nodes.map(node=>[node.id,node]));
    [...nodes].sort((a,b)=>a.depth-b.depth||a.created_at-b.created_at).forEach(node=>{
        const card=document.createElement('div');
        card.className='tree-node';
        card.style.marginLeft=`${Math.min(node.depth,8)*24}px`;
        const head=document.createElement('div'),title=document.createElement('strong'),state=document.createElement('span');
        head.className='pool-head';
        title.textContent=`${node.depth===0?'根':'L'+node.depth} · ${node.name}`;
        state.className=`pill ${node.status==='blocked'?'bad':node.status==='safe'?'on':'off'}`;
        state.textContent={active:'观察中',safe:'安全',blocked:'被墙',split:'已拆分',archived:'已归档'}[node.status]||node.status;
        head.append(title,state);
        const host=document.createElement('div'),metaLine=document.createElement('div'),actions=document.createElement('div');
        host.className='host';
        host.textContent=node.host||'未配置域名';
        metaLine.className='meta';
        metaLine.textContent=`${node.user_count} 人 · ${node.pulled_count} 已拉取${node.parent_id&&byId.get(node.parent_id)?` · 上级：${byId.get(node.parent_id).name}`:''}`;
        actions.className='actions';
        const add=(label,fn,kind='secondary')=>{const button=document.createElement('button');button.className=`${kind} small`;button.textContent=label;button.onclick=fn;actions.appendChild(button)};
        add('查看用户',()=>showTreeUsers(node));
        if(!node.children.length){
            if(node.status!=='safe')add('确认安全',()=>changeTreeStatus(node,'safe'),'success');
            if(node.status!=='blocked')add('标记被墙',()=>changeTreeStatus(node,'blocked'),'danger');
            if(['active','blocked'].includes(node.status))add('继续细分',()=>openSplitTree(node));
            if(node.status==='safe')add('转入观察/安全组',()=>openTreeTransfer(node),'success');
        }
        card.append(head,host,metaLine,actions);
        list.appendChild(card);
    })
}
async function changeTreeStatus(node,status){const label=status==='safe'?'安全':'被墙';if(!confirm(`确认把“${node.name}”标记为${label}？`))return;try{updateCurrent(await request(api(`/investigations/${encodeURIComponent(node.id)}/status`),{method:'POST',body:JSON.stringify({status})}));toast(`节点已标记为${label}`)}catch(error){toast(error.message,'error')}}
function openSplitTree(node){splitTreeNodeId=node.id;$('splitTreeTitle').textContent=`拆分：${node.name}（${node.user_count} 人）`;$('splitTreeCount').value=2;renderSplitTreeFields();$('splitTreeModal').classList.add('show')}
function renderSplitTreeFields(){const count=Math.max(2,Math.min(10,Number($('splitTreeCount').value)||2)),container=$('splitTreeBranches'),old=[...container.querySelectorAll('.branch-row')].map(row=>[row.children[0].value,row.children[1].value]);container.textContent='';for(let index=0;index<count;index++){const row=document.createElement('div');row.className='branch-row';const name=document.createElement('input');name.placeholder=`分支 ${String.fromCharCode(65+index)}`;name.value=old[index]?.[0]||`分支 ${String.fromCharCode(65+index)}`;const host=document.createElement('input');host.placeholder='全新域名或 IP';host.value=old[index]?.[1]||'';row.append(name,host);container.appendChild(row)}}
function renderOverridePoolOptions(){fillSelect('overridePool',pools(),$('overridePool').value,'仅使用单独域名')}
function resetTaskEditors(){$('poolId').value='';$('poolName').value='';$('poolHost').value='';$('poolCapacity').value=0;$('poolNote').value='';$('userSearch').value='';$('searchResults').textContent='';$('overrideUser').value='';delete $('overrideUser').dataset.id;$('overrideHost').value='';$('overrideNote').value='';$('overrideExpires').value='';$('overrideRows').textContent='';$('usersModal').classList.remove('show');$('transferModal').classList.remove('show');$('splitTreeModal').classList.remove('show');transferSource=null;splitTreeNodeId='';poolModal={poolId:'',poolName:'',page:1,lastPage:1,total:0,q:''}}
function renderCurrent(){current||=blankCampaign();$('campaignName').value=current.name||'';renderGroups();renderStatus();renderPools();renderInvestigationTree();renderOverridePoolOptions();$('deleteCampaign').disabled=!current.id||router()?.enabled}
function updateCurrent(campaign){const index=campaigns.findIndex(item=>item.id===campaign.id);if(index>=0)campaigns[index]=campaign;else campaigns.push(campaign);current=campaign;renderCampaigns();renderCurrent()}
async function refresh(full=true){const id=current?.id,isDraft=current&&current.id==='';if(full){const data=await request('/meta');meta={groups:data.groups,servers:data.servers};campaigns=data.campaigns}else campaigns=await request('/campaigns');if(isDraft&&!full){renderCampaigns();return}current=campaigns.find(item=>item.id===id)||campaigns[0]||blankCampaign();renderCampaigns();if(full)renderCurrent();else{renderStatus();renderPools();renderInvestigationTree()}}
async function showTreeUsers(node=null,page=null){if(node){poolModal={nodeId:node.id,poolName:node.name,page:1,lastPage:1,total:0,q:'',mode:'tree'};$('poolUserSearch').value=''}if(!poolModal.nodeId)return;if(page!==null)poolModal.page=page;const campaignId=current?.id;loading(true,'正在加载排查节点用户…');try{const result=await request(api(`/investigations/${encodeURIComponent(poolModal.nodeId)}/users?q=${encodeURIComponent(poolModal.q)}&page=${poolModal.page}&per_page=50`));if(current?.id!==campaignId)return;poolModal.page=result.pagination.page;poolModal.lastPage=result.pagination.last_page;poolModal.total=result.pagination.total;$('poolUserTools').style.display='grid';$('poolUserPagination').style.display='flex';$('usersTitle').textContent=`${poolModal.poolName}用户（${poolModal.total}）`;$('usersStateHead').textContent='拉取状态';$('usersActionHead').textContent='操作';$('usersBody').textContent='';result.items.forEach(user=>{const row=$('usersBody').insertRow();row.insertCell().textContent=user.id;row.insertCell().textContent=user.email;row.insertCell().textContent=user.exposed?'已拉取':'未拉取';row.insertCell().textContent=user.pull_count||0;row.insertCell().textContent=formatTime(user.last_pulled_at);row.insertCell().textContent='-'});$('poolUserPage').textContent=`第 ${poolModal.page} / ${poolModal.lastPage} 页`;$('prevPoolUsers').disabled=poolModal.page<=1;$('nextPoolUsers').disabled=poolModal.page>=poolModal.lastPage;$('usersModal').classList.add('show')}catch(error){toast(error.message,'error')}finally{loading(false)}}
function reloadPagedUsers(page){return poolModal.mode==='tree'?showTreeUsers(null,page):showPoolUsers(null,page)}
async function showPoolUsers(pool=null,page=null){if(pool){poolModal={poolId:pool.id,poolName:pool.name,page:1,lastPage:1,total:0,q:''};$('poolUserSearch').value=''}if(!poolModal.poolId)return;if(page!==null)poolModal.page=page;const campaignId=current?.id;loading(true,'正在加载用户…');try{const result=await request(api(`/pools/${encodeURIComponent(poolModal.poolId)}/users?q=${encodeURIComponent(poolModal.q)}&page=${poolModal.page}&per_page=50`));if(current?.id!==campaignId)return;poolModal.page=result.pagination.page;poolModal.lastPage=result.pagination.last_page;poolModal.total=result.pagination.total;$('poolUserTools').style.display='grid';$('poolUserPagination').style.display='flex';$('usersTitle').textContent=`${poolModal.poolName}用户（${poolModal.total}）`;$('usersStateHead').textContent='当前组';$('usersActionHead').textContent='移动到';$('usersBody').textContent='';result.items.forEach(user=>{const row=$('usersBody').insertRow();row.insertCell().textContent=user.id;row.insertCell().textContent=user.email;row.insertCell().textContent=user.pulled?'已拉取':'未拉取';row.insertCell().textContent=user.pull_count>0?user.pull_count:user.pulled?'历史已拉取':'0';row.insertCell().textContent=formatTime(user.last_pulled_at);const cell=row.insertCell(),wrap=document.createElement('div'),select=document.createElement('select'),button=document.createElement('button');wrap.className='actions';select.style.minWidth='130px';pools().filter(item=>item.enabled&&item.status!=='blocked').forEach(item=>option(select,item.id,item.name,item.id===poolModal.poolId));button.className='small';button.textContent='移动';button.onclick=async()=>{if(select.value===poolModal.poolId)return toast('用户已经在该组','error');if(!confirm(`把 ${user.email} 移动到“${select.options[select.selectedIndex].textContent}”？`))return;try{updateCurrent(await request(api(`/overrides/${user.id}`),{method:'POST',body:JSON.stringify({pool_id:select.value,host:'',node_hosts:{},server_name:'',transport_host:'',locked:true,note:`从${poolModal.poolName}手动移动`,expires_at:0})}));await showPoolUsers(null,poolModal.page);await loadOverrides();toast('用户已移动')}catch(error){toast(error.message,'error')}};wrap.append(select,button);cell.appendChild(wrap)});if(!result.items.length){const row=$('usersBody').insertRow(),cell=row.insertCell();cell.colSpan=6;cell.className='hint';cell.textContent='没有符合条件的用户'}$('poolUserPage').textContent=`第 ${poolModal.page} / ${poolModal.lastPage} 页，共 ${poolModal.total} 人`;$('prevPoolUsers').disabled=poolModal.page<=1;$('nextPoolUsers').disabled=poolModal.page>=poolModal.lastPage;$('usersModal').classList.add('show')}catch(error){toast(error.message,'error')}finally{loading(false)}}
async function loadOverrides(){const campaignId=current?.id,body=$('overrideRows');if(!campaignId||!router()){body.textContent='';return}const rows=await request(api('/overrides'));if(current?.id!==campaignId)return;body.textContent='';rows.forEach(user=>{const row=body.insertRow();row.insertCell().textContent=`${user.id} / ${user.email}`;row.insertCell().textContent=user.pool_id||'-';row.insertCell().textContent=user.override.host||Object.values(user.override.node_hosts||{}).join(', ')||'-';row.insertCell().textContent=user.override.locked?'是':'否';row.insertCell().textContent=user.override.note||'-';const cell=row.insertCell(),button=document.createElement('button');button.className='danger small';button.textContent='解除';button.onclick=async()=>{try{updateCurrent(await request(api(`/overrides/${user.id}`),{method:'DELETE'}));await loadOverrides();toast('规则已解除')}catch(error){toast(error.message,'error')}};cell.appendChild(button)});if(!rows.length){const row=body.insertRow(),cell=row.insertCell();cell.colSpan=6;cell.className='hint';cell.textContent='暂无手动规则'}}
$('campaignSelect').onchange=async event=>{const selectedId=event.target.value;try{loading(true,'正在切换任务…');await refresh(false);current=campaigns.find(item=>item.id===selectedId)||blankCampaign();resetTaskEditors();renderCampaigns();renderCurrent();await loadOverrides()}catch(error){toast(error.message,'error')}finally{loading(false)}};
$('newCampaign').onclick=()=>{current=blankCampaign();resetTaskEditors();renderCampaigns();renderCurrent()};
$('deleteCampaign').onclick=async()=>{try{if(!confirm(`删除“${current.name}”？`))return;campaigns=await request(api(''),{method:'DELETE'});current=campaigns[0]||blankCampaign();resetTaskEditors();renderCampaigns();renderCurrent();await loadOverrides();toast('任务已删除')}catch(error){toast(error.message,'error')}};
$('saveCampaign').onclick=async()=>{try{const result=await request('/campaigns',{method:'POST',body:JSON.stringify({campaign_id:current.id||null,name:$('campaignName').value.trim(),target_group_ids:selectedGroupIds()})});updateCurrent(result);toast('任务已保存')}catch(error){toast(error.message,'error')}};
$('initializeRouter').onclick=async()=>{try{if(!current.id)throw new Error('请先保存任务');updateCurrent(await request(api('/router/initialize'),{method:'POST',body:'{}'}));toast('调度系统已初始化，请先配置用户池域名')}catch(error){toast(error.message,'error')}};
$('toggleRouter').onclick=async()=>{try{const enable=!router().enabled;if(!confirm(enable?'启用后目标组将完全使用插件域名，确认配置完整？':'警告：停止接管会立即向用户恢复系统原始节点域名，确认继续？'))return;updateCurrent(await request(api('/router/toggle'),{method:'POST',body:JSON.stringify({enabled:enable})}));toast(enable?'全量接管已启用':'已恢复系统原始域名')}catch(error){toast(error.message,'error')}};
$('syncUsers').onclick=async()=>{try{updateCurrent(await request(api('/router/sync-users'),{method:'POST',body:'{}'}));toast('用户已同步')}catch(error){toast(error.message,'error')}};
$('rollbackConfig').onclick=async()=>{try{if(!confirm('回滚到上一版用户池配置？'))return;updateCurrent(await request(api('/router/rollback'),{method:'POST',body:'{}'}));toast('配置已回滚')}catch(error){toast(error.message,'error')}};
$('poolType').onchange=()=>renderPoolOverflowOptions($('poolId').value,$('poolOverflow').value);$('newPool').onclick=()=>editPool();$('savePool').onclick=async()=>{try{const data={id:$('poolId').value||null,name:$('poolName').value.trim(),type:$('poolType').value,host:$('poolHost').value.trim(),node_hosts:{},server_name:'',transport_host:'',status:$('poolStatus').value,capacity:Number($('poolCapacity').value)||0,overflow_pool_id:$('poolOverflow').value,enabled:$('poolEnabled').checked,note:$('poolNote').value.trim()};updateCurrent(await request(api('/pools'),{method:'POST',body:JSON.stringify(data)}));editPool();toast('用户池已保存')}catch(error){toast(error.message,'error')}};
$('searchUser').onclick=async()=>{try{const rows=await request(api(`/users/search?q=${encodeURIComponent($('userSearch').value.trim())}`)),box=$('searchResults');box.textContent='';rows.forEach(user=>{const row=document.createElement('div');row.className='pool';row.textContent=`${user.id} · ${user.email} · ${user.pool_id}`;const button=document.createElement('button');button.className='secondary small';button.style.marginLeft='10px';button.textContent='设置';button.onclick=()=>{$('overrideUser').value=`${user.id} / ${user.email}`;$('overrideUser').dataset.id=user.id;$('overridePool').value=user.override?.pool_id||user.pool_id||'';$('overrideHost').value=user.override?.host||'';$('overrideLocked').checked=user.override?.locked??true;$('overrideNote').value=user.override?.note||'';$('overrideExpires').value=user.override?.expires_at?new Date(user.override.expires_at*1000).toISOString().slice(0,16):''};row.appendChild(button);box.appendChild(row)});if(!rows.length)box.innerHTML='<div class="empty">未找到用户</div>'}catch(error){toast(error.message,'error')}};
$('saveOverride').onclick=async()=>{try{const userId=Number($('overrideUser').dataset.id);if(!userId)throw new Error('请先搜索并选择用户');const expires=$('overrideExpires').value?Math.floor(new Date($('overrideExpires').value).getTime()/1000):0;updateCurrent(await request(api(`/overrides/${userId}`),{method:'POST',body:JSON.stringify({pool_id:$('overridePool').value||null,host:$('overrideHost').value.trim(),node_hosts:{},server_name:'',transport_host:'',locked:$('overrideLocked').checked,note:$('overrideNote').value.trim(),expires_at:expires})}));await loadOverrides();toast('用户规则已保存')}catch(error){toast(error.message,'error')}};
$('searchPoolUsers').onclick=()=>{poolModal.q=$('poolUserSearch').value.trim();reloadPagedUsers(1)};$('poolUserSearch').onkeydown=event=>{if(event.key==='Enter')$('searchPoolUsers').click()};$('prevPoolUsers').onclick=()=>reloadPagedUsers(poolModal.page-1);$('nextPoolUsers').onclick=()=>reloadPagedUsers(poolModal.page+1);
$('closeTransfer').onclick=()=>{$('transferModal').classList.remove('show');transferSource=null};$('transferModal').onclick=event=>{if(event.target===$('transferModal'))$('closeTransfer').click()};$('confirmTransfer').onclick=async()=>{if(!transferSource)return;const source=transferSource,targetId=$('transferTarget').value,targetName=$('transferTarget').options[$('transferTarget').selectedIndex]?.textContent,isTree=source.mode==='tree';if(!confirm(`把“${source.name}”${isTree?'全部固定':'已拉取'}用户转入“${targetName}”？`))return;try{const path=isTree?`/investigations/${encodeURIComponent(source.id)}/move`:`/pools/${encodeURIComponent(source.id)}/move-pulled`;const result=await request(api(path),{method:'POST',body:JSON.stringify({target_pool_id:targetId})});updateCurrent(result.campaign);$('closeTransfer').click();toast(`已移动 ${result.moved_count} 人`)}catch(error){toast(error.message,'error')}};
$('splitTreeCount').oninput=renderSplitTreeFields;$('closeSplitTree').onclick=()=>{$('splitTreeModal').classList.remove('show');splitTreeNodeId=''};$('splitTreeModal').onclick=event=>{if(event.target===$('splitTreeModal'))$('closeSplitTree').click()};$('confirmSplitTree').onclick=async()=>{if(!splitTreeNodeId)return;const branches=[...$('splitTreeBranches').querySelectorAll('.branch-row')].map(row=>({name:row.children[0].value.trim(),host:row.children[1].value.trim()}));if(branches.some(branch=>!branch.host))return toast('请填写每个分支的全新域名','error');if(!confirm(`确认把该节点固定均分为 ${branches.length} 个独立分支？`))return;try{updateCurrent(await request(api(`/investigations/${encodeURIComponent(splitTreeNodeId)}/split`),{method:'POST',body:JSON.stringify({branches})}));$('closeSplitTree').click();toast('下级分支已创建，用户分配已固定')}catch(error){toast(error.message,'error')}};
$('refreshOverrides').onclick=()=>loadOverrides().catch(error=>toast(error.message,'error'));$('closeUsers').onclick=()=> $('usersModal').classList.remove('show');$('usersModal').onclick=event=>{if(event.target===$('usersModal'))$('usersModal').classList.remove('show')};
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
