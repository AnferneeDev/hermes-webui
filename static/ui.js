// `todos` is the single source of truth for the Todos panel.  Any update
// goes through the `todo_state` SSE event (live) or session.todo_state
// (cold-load).  `todoStateMeta` doubles as a sentinel: while it is null
// no explicit signal has been seen, so loadTodos() falls back to the
// legacy reverse-scan over S.messages ‚Äî that keeps new clients working
// against old servers (Phase 1 may not yet be deployed everywhere).
// See api/todo_state.py for the wire contract.
const S={session:null,messages:[],entries:[],busy:false,pendingFiles:[],toolCalls:[],activeStreamId:null,currentDir:'.',activeProfile:'default',activeProfileIsDefault:true,showHiddenWorkspaceFiles:false,todos:[],todoStateMeta:null,_pendingSessionToolsets:null};

function assistantDisplayName(){
  if(S.activeProfile&&S.activeProfile!=='default') return S.activeProfile.charAt(0).toUpperCase()+S.activeProfile.slice(1);
  return window._botName||'Hermes';
}
const INFLIGHT={};  // keyed by session_id while request in-flight
const SESSION_QUEUES={};  // keyed by session_id for queued follow-up turns
const MAX_UPLOAD_BYTES=(window.__HERMES_CONFIG__&&window.__HERMES_CONFIG__.maxUploadBytes)||20*1024*1024;
const MAX_UPLOAD_MB=Math.round(MAX_UPLOAD_BYTES/1024/1024);
// Tracks which session's queue to drain in setBusy(false).
// Set to activeSid just before setBusy(false) in done/error handlers so the
// queue drains the session that *finished*, not the one currently viewed.
// Single-shot: setBusy() reads and clears this on every call. Concurrent
// back-to-back stream completions would overwrite it, but HTTPServer is
// single-threaded so only one done event fires at a time in practice.
let _queueDrainSid=null;
const $=id=>document.getElementById(id);
const OFFLINE_RECHECK_MS=2500;
const OFFLINE_HEALTH_TIMEOUT_MS=10000;
const OFFLINE_FETCH_FAILURES_BEFORE_BANNER=2;
let _offlineVisible=false;
let _offlineReason='browser';
let _offlineProbeTimer=null;
let _offlineChecking=false;
let _offlineProbePromise=null;
let _offlineHealthProbePromise=null;
let _offlineFetchProbeFailures=0;
let _offlineRawFetch=null;
let _offlineFetchPatched=false;
function _browserReportsOnline(){return !('onLine' in navigator)||navigator.onLine!==false;}
function _offlineHealthUrl(){const url=new URL('health',document.baseURI||location.href);url.searchParams.set('offline_probe',String(Date.now()));return url.href;}
function _setOfflineChecking(checking){
  _offlineChecking=!!checking;
  const btn=$('offlineCheckNow');
  if(btn){btn.disabled=_offlineChecking;btn.textContent=_offlineChecking?t('offline_checking'):t('offline_check_now');}
}
function _renderOfflineBanner(){
  const banner=$('offlineBanner');
  if(!banner)return;
  const detail=$('offlineDetails');
  if(detail)detail.textContent=t(_offlineReason==='browser'?'offline_browser_detail':'offline_network_detail');
  const title=$('offlineTitle');
  if(title)title.textContent=t('offline_title');
  const auto=$('offlineAutorefresh');
  if(auto)auto.textContent=t('offline_autorefresh');
  _setOfflineChecking(_offlineChecking);
  banner.hidden=false;
  banner.classList.add('visible');
}
function _startOfflineProbeTimer(){
  if(_offlineProbeTimer)return;
  _offlineProbeTimer=setInterval(()=>{checkOfflineRecoveryNow();},OFFLINE_RECHECK_MS);
}
function _stopOfflineProbeTimer(){
  if(_offlineProbeTimer){clearInterval(_offlineProbeTimer);_offlineProbeTimer=null;}
}
function showOfflineBanner(reason){
  _offlineVisible=true;
  _offlineReason=reason||(_browserReportsOnline()?'network':'browser');
  _renderOfflineBanner();
  _startOfflineProbeTimer();
}
function isOfflineBannerVisible(){return _offlineVisible;}
function _hideOfflineBanner(){
  _offlineVisible=false;
  _stopOfflineProbeTimer();
  _setOfflineChecking(false);
  const banner=$('offlineBanner');
  if(banner){banner.classList.remove('visible');banner.hidden=true;}
}
async function _probeOfflineRecovery(){
  if(_offlineHealthProbePromise)return _offlineHealthProbePromise;
  _offlineHealthProbePromise=(async()=>{
    const fetcher=_offlineRawFetch||window.fetch.bind(window);
    // Bound the probe so a black-hole network (connected, server hung, packets
    // dropped) can't delay the banner past a few seconds ‚Äî the probe now gates
    // the initial banner display on the offline-event/startup paths.
    let ctrl=null,timer=null;
    try{ctrl=(typeof AbortController!=='undefined')?new AbortController():null;}catch(_){ctrl=null;}
    if(ctrl)timer=setTimeout(()=>{try{ctrl.abort();}catch(_){}},OFFLINE_HEALTH_TIMEOUT_MS);
    try{
      const opts={cache:'no-store',credentials:'include'};
      if(ctrl)opts.signal=ctrl.signal;
      const res=await fetcher(_offlineHealthUrl(),opts);
      return !!(res&&res.ok);
    }catch(_){return false;}
    finally{if(timer)clearTimeout(timer);}
  })();
  try{return await _offlineHealthProbePromise;}
  finally{_offlineHealthProbePromise=null;}
}
async function _showOfflineBannerIfProbeFails(reason,opts){
  opts=opts||{};
  const visibleAtStart=_offlineVisible;
  const requireConsecutiveFailures=opts.requireConsecutiveFailures!==false;
  if(visibleAtStart)_setOfflineChecking(true);
  const ok=await _probeOfflineRecovery();
  if(visibleAtStart)_setOfflineChecking(false);
  if(ok){
    _offlineFetchProbeFailures=0;
    if(_offlineVisible){_stopOfflineProbeTimer();await _recoverFromOfflineSoftly();}
    return true;
  }
  if(!visibleAtStart&&requireConsecutiveFailures){
    _offlineFetchProbeFailures+=1;
    if(_offlineFetchProbeFailures<OFFLINE_FETCH_FAILURES_BEFORE_BANNER)return false;
  }
  showOfflineBanner(reason||(_browserReportsOnline()?'network':'browser'));
  return false;
}
async function checkOfflineRecoveryNow(){
  if(_offlineProbePromise)return _offlineProbePromise;
  _offlineProbePromise=(async()=>{
    if(!_offlineVisible)return false;
    _setOfflineChecking(true);
    const ok=await _probeOfflineRecovery();
    _setOfflineChecking(false);
    if(ok){_offlineFetchProbeFailures=0;if(!_offlineVisible)return true;_stopOfflineProbeTimer();await _recoverFromOfflineSoftly();return true;}
    showOfflineBanner(_browserReportsOnline()?'network':'browser');
    return false;
  })();
  try{return await _offlineProbePromise;}
  finally{_offlineProbePromise=null;}
}
// Recover from a transient "Connection lost" without a full page reload.
//
// The offline banner fires whenever a fetch/SSE errors ‚Äî which Android does
// aggressively every time the PWA is backgrounded, even for a second. The old
// behaviour here was `window.location.reload()`: a hard cold boot that re-runs
// the whole app and re-pulls /api/sessions + /api/session, producing the
// multi-second "reload to see the conversation I was just in" flash on every
// resume. The reload was also intermittent (only when a request actually
// errored that time), matching the reported "sometimes it reloads, sometimes
// it doesn't".
//
// The server keeps the agent running and buffers stream events while no
// subscriber is attached (#2307), so a hard reload is never required to
// recover ‚Äî we just need to reattach. This does the soft path: hide the
// banner, restart the gateway SSE (bfcache/background kills the connection),
// and re-fetch the active session so any messages that landed while we were
// away appear. A full reload is the fallback only if the soft path throws.
async function _recoverFromOfflineSoftly(){
  try{
    _hideOfflineBanner();
    if(typeof startGatewaySSE==='function') startGatewaySSE();
    if(S.session && typeof refreshSession==='function'){
      await refreshSession();
    }
    // After refreshSession() sets S.activeStreamId, reattach if a stream is live.
    // The server buffers events while no subscriber is attached (#2307/#3863).
    const sid=S.session&&S.session.session_id;
    const streamId=S.session&&S.session.active_stream_id;
    if(sid&&streamId&&typeof attachLiveStream==='function'){
      let status=null;
      try{
        status=await api(`/api/chat/stream/status?stream_id=${encodeURIComponent(streamId)}`);
      }catch(_){/* stream status check failed ‚Äî leave session refreshed but don't reattach */}
      // Outside the probe's catch so an attachLiveStream throw reaches the
      // outer fallback (hard reload) instead of being silently swallowed.
      if(status&&status.active) attachLiveStream(sid,streamId,S.session.pending_attachments||[],{reconnecting:true});
    }
    return true;
  }catch(_){
    // Soft reattach failed (server mid-restart, session gone, etc.) ‚Äî fall
    // back to the original hard reload so the user is never stuck offline.
    window.location.reload();
    return false;
  }
}
function _isAbortError(e){return !!(e&&(e.name==='AbortError'||e.code===20));}
function _patchOfflineFetch(){
  if(_offlineFetchPatched||typeof window.fetch!=='function')return;
  _offlineFetchPatched=true;
  _offlineRawFetch=window.fetch.bind(window);
  window.fetch=async function(...args){
    try{return await _offlineRawFetch(...args);}
    catch(e){
      if(!_isAbortError(e)&&(e instanceof TypeError||!_browserReportsOnline())){
        void _showOfflineBannerIfProbeFails(_browserReportsOnline()?'network':'browser');
      }
      throw e;
    }
  };
}
function initOfflineMonitor(){
  _patchOfflineFetch();
  window.addEventListener('offline',()=>{void _showOfflineBannerIfProbeFails('browser',{requireConsecutiveFailures:false});});
  window.addEventListener('online',()=>{if(_offlineVisible)checkOfflineRecoveryNow();});
  if(!_browserReportsOnline())void _showOfflineBannerIfProbeFails('browser',{requireConsecutiveFailures:false});
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',initOfflineMonitor,{once:true});
else initOfflineMonitor();
// Redirect to login when the server responds with 401 (auth session expired).
// Handles iOS PWA standalone mode and keeps subpath mounts like /hermes/ from
// escaping to the personal site root /login.
// #5578: on a login-shaped page, reload 'login' WITHOUT a next (avoid self-nesting).
function _redirectIfUnauth(res){if(res&&res.status===401){var _p=(window.location.pathname||'').replace(/\/+$/,'');if(/(?:^|\/)login$/.test(_p)){window.location.href='login';}else{window.location.href='login?next='+encodeURIComponent(window.location.pathname+window.location.search);}return true;}return false;}
function _getSessionQueue(sid, create=false){
  if(!sid) return [];
  if(!SESSION_QUEUES[sid]&&create) SESSION_QUEUES[sid]=[];
  return SESSION_QUEUES[sid]||[];
}
function _queueStorageKey(sid){
  return 'hermes-queue-'+sid;
}
function _clearPersistedSessionQueue(sid){
  if(!sid) return;
  const key=_queueStorageKey(sid);
  try{sessionStorage.removeItem(key);}catch(_){}
  try{localStorage.removeItem(key);}catch(_){}
}
function _persistSessionQueueStorage(sid, queue){
  if(!sid) return;
  const q=Array.isArray(queue)?queue:[];
  if(!q.length){_clearPersistedSessionQueue(sid);return;}
  const key=_queueStorageKey(sid);
  let payload='[]';
  try{payload=JSON.stringify(q);}catch(_){return;}
  try{sessionStorage.setItem(key,payload);}catch(_){}
  try{localStorage.setItem(key,payload);}catch(_){}
}
function _readPersistedSessionQueue(sid){
  if(!sid) return [];
  const key=_queueStorageKey(sid);
  const read=(store)=>{
    try{
      const raw=store&&store.getItem?store.getItem(key):null;
      if(!raw) return null;
      const parsed=JSON.parse(raw);
      return Array.isArray(parsed)?parsed:null;
    }catch(_){return null;}
  };
  const sessionValue=read(sessionStorage);
  if(sessionValue&&sessionValue.length) return sessionValue;
  const localValue=read(localStorage);
  if(localValue&&localValue.length){
    try{sessionStorage.setItem(key,JSON.stringify(localValue));}catch(_){}
    return localValue;
  }
  return [];
}
function queueSessionMessage(sid, payload){
  if(!sid||!payload) return 0;
  const q=_getSessionQueue(sid,true);
  // Stamp created_at so the restore path can detect stale entries (agent already responded)
  const entry={...payload, _queued_at: Date.now()};
  q.push(entry);
  _persistSessionQueueStorage(sid,q);
  return q.length;
}
function shiftQueuedSessionMessage(sid){
  const q=_getSessionQueue(sid,false);
  if(!q.length) return null;
  const next=q.shift();
  if(!q.length){
    delete SESSION_QUEUES[sid];
    _clearPersistedSessionQueue(sid);
  } else {
    _persistSessionQueueStorage(sid,q);
  }
  return next;
}
function getQueuedSessionCount(sid){
  return _getSessionQueue(sid,false).length;
}
function _compressionSessionLock(){
  return window._compressionLockSid||null;
}
function _setCompressionSessionLock(sid){
  window._compressionLockSid=sid||null;
}
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function jsArg(s){
  // Encode a value for safe interpolation inside an inline on* handler's JS
  // string literal. JSON.stringify quotes/escapes for the JS context; esc()
  // then makes the result safe inside the HTML attribute. Without this, a
  // value containing a quote breaks out of the handler (esc() alone is
  // HTML-escaping, which the browser decodes BEFORE executing the inline
  // handler). Promoted to a shared helper from the kanban dependency fix
  // (#3797). Use as onclick="fn(${jsArg(v)})" ‚Äî no manual quotes.
  return esc(JSON.stringify(String(s == null ? '' : s)));
}
function _matchBacktickFenceLine(line){
  const m=String(line||'').match(/^[ ]{0,3}(`{3,})([^`]*)$/);
  if(!m) return null;
  return {fence:m[1],len:m[1].length,info:(m[2]||'').trim()};
}
function _isBacktickFenceClose(line,minLen){
  const m=String(line||'').match(/^[ ]{0,3}(`{3,})[ \t]*$/);
  return !!(m&&m[1].length>=minLen);
}
/**
 * Render fenced code blocks inside user messages.
 * Extracts ```‚Ä¶``` fences, replaces them with placeholders,
 * escapes remaining text as plain HTML, then restores code blocks
 * with the same <pre><code> pipeline used by renderMd().
 * All non-fenced text stays escaped (no bold/italic/link interpretation).
 */

function _stripWorkspaceDisplayPrefix(text){
  // v1 sentinel format `[Workspace::v1: <escaped path>]\n` injected since #1918.
  // Legacy format `[Workspace: <path>]\n` may still be present in transcripts
  // saved before the v1 migration; fall through to the legacy regex when the
  // v1 strip didn't match. Mirrors the Python `include_legacy=True` branch in
  // api/streaming.py:_strip_workspace_prefix(). Per Opus advisor on stage-322.
  const value = String(text||'');
  const stripped = value.replace(/^\s*\[Workspace::v1:\s*(?:\\.|[^\]\\])+\]\s*/,'');
  if(stripped !== value) return stripped.trim();
  return value.replace(/^\s*\[Workspace:[^\]]+\]\s*/,'').trim();
}
function _renderUserFencedBlocks(text){
  const stash=[];
  const contextStash=[];
  const mathStash=[];
  const stashMath=(type,src)=>{mathStash.push({type,src});return '\x00UM'+(mathStash.length-1)+'\x00';};
  const sentContextHtml=(label,quoteText)=>{
    const safeLabel=String(label||'').trim()||'Context';
    const safeQuote=String(quoteText||'').replace(/\s+$/,'');
    return `<figure class="sent-selection-context" data-selected-context="1"><figcaption class="sent-selection-context-label">${esc(safeLabel)}</figcaption><blockquote class="sent-selection-context-quote">${esc(safeQuote)}</blockquote></figure>`;
  };
  const stashContext=(label,quote)=>{contextStash.push(sentContextHtml(label,quote));return '\x00UC'+(contextStash.length-1)+'\x00';};
  const stashSelectedContextBlocks=(value)=>{
    const lines=String(value||'').split('\n');
    const marker='<!-- hermes-selected-context -->';
    const out=[];
    for(let i=0;i<lines.length;i++){
      const labelMatch=lines[i].match(/^\*\*([^\n]{1,200}):\*\*\s*$/);
      if(!labelMatch){out.push(lines[i]);continue;}
      const quoteLines=[];
      let j=i+1;
      if(lines[j]!==marker){out.push(lines[i]);continue;}
      j++;
      while(j<lines.length&&/^>/.test(lines[j])){
        quoteLines.push(lines[j].replace(/^>[ \t]?/,''));
        j++;
      }
      if(!quoteLines.length){out.push(lines[i]);continue;}
      out.push(stashContext(labelMatch[1], quoteLines.join('\n')));
      i=j-1;
    }
    return out.join('\n');
  };
  const restoreMath=html=>String(html||'').replace(/\x00UM(\d+)\x00/g,(_,i)=>{
    const item=mathStash[+i];
    if(!item) return '';
    if(item.type==='display') return `<div class="katex-block" data-katex="display">${esc(item.src)}</div>`;
    return `<span class="katex-inline" data-katex="inline">${esc(item.src)}</span>`;
  });
  let s=String(text||'');
  // Extract fenced code blocks FIRST so math regexes never run inside fenced
  // content. If math were stashed first, a user-typed code block containing
  // \[..\] / \(..\) / $$..$$ would be rendered as a KaTeX block inside
  // <pre><code> instead of as literal source. Mirrors renderMd()'s ordering.
  // CommonMark ¬ß4.5 line-anchored fence: the closing run must use at least
  // as many backticks as the opener, so inner triple-backtick fences remain content.
  s=s.replace(/(^|\n)[ ]{0,3}(`{3,})([^\n`]*)\n(?:([\s\S]*?)\n)?[ ]{0,3}\2`*[ \t]*(?=\n|$)/g,(_,lead,_fence,info,code)=>{
    const langInfo=(info||'').trim();
    const langMatch=langInfo.match(/^(\w[\w+-]*)$/);
    let lang=langMatch?(langMatch[1]||'').trim().toLowerCase():'';
    code=code||'';
    // Remove one trailing newline if present (the fence consumes its own)
    if(code.endsWith('\n')) code=code.slice(0,-1);
    const h=lang?`<div class="pre-header">${esc(lang)}</div>`:'';
    const langAttr=lang?` class="language-${esc(lang)}"`:'';
    const preClass=/^(md|markdown|mdx)$/.test(lang)?' class="md-source-block"':'';
    if(lang==='diff'||lang==='patch'){
      const colored=esc(code).split('\n').map(line=>{
        if(line.startsWith('@@')) return `<span class="diff-line diff-hunk">${line}</span>`;
        if(line.startsWith('+')) return `<span class="diff-line diff-plus">${line}</span>`;
        if(line.startsWith('-')) return `<span class="diff-line diff-minus">${line}</span>`;
        return `<span class="diff-line">${line}</span>`;
      }).join('\n');
      stash.push(`${h}<pre class="diff-block"><code${langAttr}>${colored}</code></pre>`);
    } else {
      stash.push(`${h}<pre${preClass}><code${langAttr}>${esc(code)}</code></pre>`);
    }
    return lead+'\x00UF'+(stash.length-1)+'\x00';
  });
  // Now stash math from the OUTSIDE-of-fence text. Display delimiters must
  // run before inline so $$..$$ isn't mis-parsed as $..$..$..$.
  s=s.replace(/\$\$([\s\S]+?)\$\$/g,(_,m)=>stashMath('display',m));
  s=s.replace(/\\\[([\s\S]+?)\\\]/g,(_,m)=>stashMath('display',m));
  s=s.replace(/\$([^\s$\n][^$\n]*?[^\s$\n]|\S)\$/g,(_,m)=>stashMath('inline',m));
  s=s.replace(/\\\((.+?)\\\)/g,(_,m)=>stashMath('inline',m));
  // Render selected-context payloads produced by Reply with selection as calm
  // quote cards in the sent user bubble. Keep ordinary user Markdown escaped;
  // only blocks carrying the internal marker get custom treatment.
  s=stashSelectedContextBlocks(s);
  // Escape remaining plain text and convert newlines to <br>
  s=esc(s).replace(/\n/g,'<br>');
  // Restore stashed code/context blocks, then math placeholders as KaTeX targets.
  s=s.replace(/\x00UF(\d+)\x00/g,(_,i)=>stash[+i]);
  s=s.replace(/\x00UC(\d+)\x00/g,(_,i)=>contextStash[+i]||'');
  s=restoreMath(s);
  return s;
}
function _statusCardHtml(card){
  card=card||{};
  const rows=Array.isArray(card.rows)?card.rows:[];
  const sessionId=String(card.sessionId||'');
  const shortSessionId=sessionId.length>22?`${sessionId.slice(0,10)}‚Ä¶${sessionId.slice(-8)}`:sessionId;
  const copyIcon=(typeof li==='function')?li('copy',13):'Copy';
  const copyBtn=sessionId
    ? `<button class="status-card-session-copy" type="button" data-copy-status-session="${esc(card.sessionId||'')}" title="${esc(t('copy'))}" onclick="copyStatusSessionId(this);event.stopPropagation()"><span>${esc(shortSessionId)}</span>${copyIcon}</button>`
    : '';
  const rowHtml=rows.map(row=>`
    <div class="status-card-row">
      <span class="status-card-label">${esc(row.label||'')}</span>
      <span class="status-card-value">${esc(row.value||'')}</span>
    </div>`).join('');
  return `<div class="status-card" data-status-card="1">
    <div class="status-card-head">
      <div class="status-card-title-wrap">
        <div class="status-card-title">${esc(card.title||t('status_heading'))}</div>
        <div class="status-card-subtitle">${esc(card.subtitle||'')}</div>
      </div>
      ${copyBtn}
    </div>
    <div class="status-card-grid">${rowHtml}</div>
  </div>`;
}

function _compressionRecoveryHtml(recovery, sessionId){
  if(!recovery||typeof recovery!=='object') return '';
  if(String(recovery.terminal_state||'')!=='compression_exhausted') return '';
  const action=String(recovery.recommended_action||'');
  if(action!=='start_focused_continuation') return '';
  const sid=String(recovery.source_session_id||sessionId||'');
  const title=String(recovery.title||'Context compression exhausted');
  const summary=String(recovery.summary||'Start a focused continuation, then describe the next narrow task.');
  const actionLabel=String(recovery.action_label||'Start focused continuation');
  const icon=(typeof li==='function')?li('git-branch',14):'';
  return `<div class="compression-recovery-card" data-compression-recovery-card="1">
    <div class="compression-recovery-copy">
      <div class="compression-recovery-title">${esc(title)}</div>
      <div class="compression-recovery-summary">${esc(summary)}</div>
    </div>
    <button class="compression-recovery-action" type="button" data-recovery-session-id="${esc(sid)}" onclick="startCompressionRecovery(this);event.stopPropagation()">${icon}<span>${esc(actionLabel)}</span></button>
  </div>`;
}

function _activeCompressionRecoveryPayload(){
  if(!S||!S.session) return null;
  const recovery=S.session.compression_recovery;
  if(recovery&&typeof recovery==='object'&&String(recovery.terminal_state||'')==='compression_exhausted') return recovery;
  // A cleared session-level recovery payload is authoritative. Only scan
  // message metadata for older sessions that never exposed this field.
  if(Object.prototype.hasOwnProperty.call(S.session,'compression_recovery')) return null;
  const messages=Array.isArray(S.messages)?S.messages:[];
  for(let i=messages.length-1;i>=0;i--){
    const msg=messages[i];
    const msgRecovery=msg&&msg._compressionRecovery;
    if(msgRecovery&&typeof msgRecovery==='object'&&String(msgRecovery.terminal_state||'')==='compression_exhausted') return msgRecovery;
  }
  return null;
}

function isGenericCompressionContinuationIntent(text){
  const raw=String(text||'').trim().toLowerCase();
  if(!raw) return false;
  const normalized=raw.replace(/[^\p{L}\p{N}]+/gu,' ').trim();
  const generic=new Set(['continue','continue please','go on','keep going','resume','proceed','carry on','ÁªßÁª≠','ÁªßÁª≠Âêß','Êé•ÁùÄ','Êé•ÁùÄÂÅö','ÁªßÁª≠ÂÅö','ÁªßÁª≠ÊâßË°å']);
  if(generic.has(normalized)) return true;
  const parts=normalized.split(/\s+/).filter(Boolean);
  return !!parts.length&&parts.length<=2&&parts.every(part=>generic.has(part));
}

function shouldInterceptCompressionRecoveryContinuation(text, files){
  const hasFiles=Array.isArray(files)&&files.length>0;
  if(hasFiles||!isGenericCompressionContinuationIntent(text)) return false;
  const recovery=_activeCompressionRecoveryPayload();
  return !!(recovery&&String(recovery.recommended_action||'')==='start_focused_continuation');
}

function showCompressionRecoveryContinuationHint(){
  const card=document.querySelector('[data-compression-recovery-card="1"]');
  if(card&&typeof card.scrollIntoView==='function'){
    try{card.scrollIntoView({block:'center',behavior:'smooth'});}catch(_){card.scrollIntoView();}
    const btn=card.querySelector('.compression-recovery-action');
    if(btn&&typeof btn.focus==='function') setTimeout(()=>btn.focus(),120);
  }
  if(typeof showToast==='function') showToast('This session exhausted context compression. Start a focused continuation, then describe the next narrow task.',4500,'warning');
}

async function startCompressionRecovery(btn){
  const sourceSid=String((btn&&btn.dataset&&btn.dataset.recoverySessionId)||(S.session&&S.session.session_id)||'').trim();
  if(!sourceSid) return;
  let retiredRecoveryCard=false;
  if(btn){btn.disabled=true;btn.classList.add('loading');}
  try{
    const data=await api('/api/session/compression-recovery/start',{method:'POST',body:JSON.stringify({session_id:sourceSid})});
    const sid=data&&data.session&&data.session.session_id;
    if(!sid) throw new Error('Compression recovery did not return a session.');
    try{localStorage.setItem('hermes-webui-session',sid);}catch(_){}
    if(typeof loadSession==='function') await loadSession(sid,{preserveActiveInput:false});
    else if(data.session){S.session=data.session;if(typeof _adoptRegenerationRevision==='function')_adoptRegenerationRevision(data.session);S.messages=data.session.messages||[];syncTopbar();renderMessages();}
    if(typeof renderSessionList==='function') await renderSessionList();
    if(typeof _setActiveSessionUrl==='function') _setActiveSessionUrl(sid);
    if(typeof showToast==='function') showToast((data&&data.message)||'Started focused continuation.',3000,'success');
    const composer=$('msg');
    if(composer&&typeof composer.focus==='function') composer.focus();
  }catch(e){
    // #7710: a cross-profile refusal now also arrives as 409
    // (``session_profile_mismatch``). That is NOT a stale recovery action ‚Äî
    // the card is still valid, the request was simply refused because the
    // session belongs to another profile. Retiring it would hide a live card
    // and show a false "conversation already moved on" note.
    if(e&&e.status===409&&typeof _sessionProfileMismatchFromError==='function'
       &&_sessionProfileMismatchFromError(e)){
      if(typeof setStatus==='function') setStatus('Session belongs to a different profile');
      return;
    }
    // A 409 means this session no longer has an active recovery action (the
    // session already moved on ‚Äî e.g. a substantive prompt cleared it). The
    // persisted card in the transcript is stale, so retire it and show a neutral
    // note instead of a raw error. The server is authoritative on availability.
    if(e&&e.status===409){
      const staleCard=(btn&&btn.closest&&btn.closest('.compression-recovery-card'))
        ||document.querySelector('[data-compression-recovery-card="1"]');
      if(staleCard){
        staleCard.setAttribute('data-compression-recovery-consumed','1');
        const staleBtn=staleCard.querySelector('.compression-recovery-action');
        if(staleBtn){staleBtn.disabled=true;staleBtn.classList.remove('loading');}
        retiredRecoveryCard=true;
      }
      if(typeof showToast==='function') showToast('This conversation already moved on ‚Äî the focused-continuation action is no longer available.',4000,'info');
      return;
    }
    if(typeof showToast==='function') showToast('Compression recovery failed: '+(e&&e.message||e),5000,'error');
  }finally{
    // Do NOT re-enable a button we deliberately retired in the 409 branch.
    if(btn){if(!retiredRecoveryCard) btn.disabled=false;btn.classList.remove('loading');}
  }
}

const MESSAGE_RENDER_WINDOW_DEFAULT=50;
const MESSAGE_VIRTUAL_THRESHOLD_ROWS=80;
const MESSAGE_VIRTUAL_BUFFER_PX=900;
const MESSAGE_VIRTUAL_DEFAULT_ROW_HEIGHTS={
  user:120,
  process_wakeup:96,
  assistant:160,
  tool_call:400,
  default:140,
};
function _messageVirtualDefaultHeightForRole(role){
  return MESSAGE_VIRTUAL_DEFAULT_ROW_HEIGHTS[
    role&&Object.prototype.hasOwnProperty.call(MESSAGE_VIRTUAL_DEFAULT_ROW_HEIGHTS,role)?role:'default'
  ];
}
// Cycle-aware measurement burst tracking (#6654/#6717): instead of a flat
// render cap, the burst remembers every window cycle key it has already seen.
// An UNSEEN key means the window is still converging forward (A->B->C->settled
// ‚Äî content reflow, late fonts/images, dynamic height) and may proceed; a key
// that REPEATS means the browser is oscillating (A->B->A->B) and the burst
// terminates. Keys repeat only when geometry flaps, so legitimate convergence
// is never capped while oscillation is always bounded.
// Absolute per-burst ceiling (#6717 re-gate): the seen-key rule alone only
// ends the burst on a REPEATED key, so a browser emitting a monotonically
// changing geometry (A->B->C->D->... never repeating) would still loop without
// limit ‚Äî the #6654 CPU-runaway class under a different trigger ‚Äî and the
// seen-key collection would grow without bound (memory). A distinct-key
// convergence realistically settles in a handful of frames, so this
// conservative cap (the historical MESSAGE_VIRTUAL_MEASUREMENT_MAX_RERENDERS
// was 2) ends the burst regardless of key novelty AND bounds the seen-key
// memory to the cap. Reset through _resetMessageVirtualMeasurementBurst().
const MESSAGE_VIRTUAL_MEASUREMENT_MAX_RERENDERS=12;
let _messageVirtualMeasurementSeenKeys=[];
let _messageRenderWindowSid=null;
let _messageRenderWindowSize=MESSAGE_RENDER_WINDOW_DEFAULT;
let _messageVirtualHeightCache=[];
let _messageVirtualHeightCacheEntries=[];
let _messageVirtualHeightCacheLen=0;
let _messageVirtualHeightCacheSrc=null;
let _messageVirtualEstimatedRowHeight=_messageVirtualDefaultHeightForRole('default');
let _messageVirtualScrollRaf=0;
let _messageVirtualWindowKey='';
let _messageVirtualMeasurementCycleKey='';
let _messageVirtualMeasurementBurstActive=false;
// Provenance of the QUEUED virtualized render: 'internal' while the pending
// rAF was requested by the internal measurement chain, 'external' for any
// other trigger (user scroll / scroll-settle / message append / session
// load). The origin lives on the QUEUED render itself, NOT in a global
// consumable flag: when an internal and an external request coalesce into one
// rAF, _scheduleMessageVirtualizedRender lets EXTERNAL win, so a render that
// fires after any external trigger is never mis-attributed as internal
// (#6717 re-gate). renderMessages() resets the burst on every UNMARKED
// (externally initiated) render trigger, so an overlapping external render
// always starts a fresh cycle even when an internal measurement callback is
// still pending.
let _messageVirtualRenderQueuedOrigin=null;
let _messageVirtualScrollActive=false;
let _messageVirtualScrollSettleTimer=0;
let _messageVirtualDeferredMeasurement=null;
let _msgNodeRecycleEnabled=false;
const _recycleStash=new Map();
const _recycleResetAttrs=[
  'data-transparent-turn-collapsed',
  'data-transparent-turn-toggle-bound',
  'data-anchor-scene-live-owner',
  'data-anchor-stream-id',
  'data-latest-assistant-response',
  'role',
  'aria-label',
  // Defensive reset for legacy/restored shells that may still carry the fallback live-turn marker.
  'data-live-assistant-turn',
];
let _scrollbarDragActive=false;
function _markMessageVirtualScrollActive(){
  _messageVirtualScrollActive=true;
  clearTimeout(_messageVirtualScrollSettleTimer);
  _messageVirtualScrollSettleTimer=setTimeout(()=>{
    _messageVirtualScrollActive=false;
    if(_messageVirtualDeferredMeasurement){
      const deferred=_messageVirtualDeferredMeasurement;
      _messageVirtualDeferredMeasurement=null;
      _scheduleMessageVirtualMeasurementRefresh(deferred);
    }
  },150);
}
// Cached visWithIdx array ‚Äî invalidated when S.messages.length changes.
let _visWithIdxCache=null;
let _visWithIdxCacheLen=0;
let _visWithIdxCacheSrc=null;  // S.messages reference ‚Äî detects wholesale replacement with same length
function clearVisibleMessageRowCache(){
  _visWithIdxCache=null;
  _visWithIdxCacheLen=0;
  _visWithIdxCacheSrc=null;
}
function _clearMessageVirtualHeightCache(){
  _messageVirtualHeightCache=[];
  _messageVirtualHeightCacheEntries=[];
  _messageVirtualHeightCacheLen=0;
  _messageVirtualHeightCacheSrc=null;
  _messageVirtualEstimatedRowHeight=_messageVirtualDefaultHeightForRole('default');
  _messageVirtualWindowKey='';
  _messageVirtualMeasurementCycleKey='';
  _resetMessageVirtualMeasurementBurst();
  _messageVirtualScrollActive=false;
  clearTimeout(_messageVirtualScrollSettleTimer);
  _messageVirtualScrollSettleTimer=0;
  _messageVirtualDeferredMeasurement=null;
  if(typeof _clearUserRowIntrinsicHeightCache==='function') _clearUserRowIntrinsicHeightCache();
}
function _resetMessageRenderWindow(sid){
  _messageRenderWindowSid=sid||null;
  _messageRenderWindowSize=MESSAGE_RENDER_WINDOW_DEFAULT;
  _cancelMessageVirtualizedRender();
  _clearRenderCache();
  clearVisibleMessageRowCache();
  _clearMessageVirtualHeightCache();
}
function _restoreMessageRenderWindowAfterSettledRender(){
  _messageRenderWindowSize=MESSAGE_RENDER_WINDOW_DEFAULT;
}
function _cancelMessageVirtualizedRender(){
  if(_messageVirtualScrollRaf){
    cancelAnimationFrame(_messageVirtualScrollRaf);
    _messageVirtualScrollRaf=0;
  }
}
function _messageIsRenderable(m){
  if(!m||!m.role||m.role==='tool') return false;
  if(m._source === 'process_wakeup') return !!(msgContent(m)||m.attachments?.length);
  if(_isContextCompactionMessage(m)||_isPreservedCompressionTaskListMessage(m)) return false;
  if(_isRecoveryControlMessage(m)) return false;
  const hasTc=Array.isArray(m.tool_calls)&&m.tool_calls.length>0;
  const hasTu=Array.isArray(m.content)&&m.content.some(p=>p&&p.type==='tool_use');
  const hasPartialTc=Array.isArray(m._partial_tool_calls)&&m._partial_tool_calls.length>0;
  const hasReasoningAnchor=hasTc||hasTu||_messageHasReasoningPayload(m);
  const hasAssistantVisibleAnchor=hasTc||hasTu||hasPartialTc||_messageHasReasoningPayload(m)||_assistantMessageHasVisibleContent(m);
  return !!(msgContent(m)||m._statusCard||m.attachments?.length||(m.role==='assistant'&&(hasReasoningAnchor||hasAssistantVisibleAnchor)));
}
function _getVisibleMessagesWithIdx(){
  if(!_visWithIdxCache || _visWithIdxCacheLen !== S.messages.length || _visWithIdxCacheSrc !== S.messages){
    const rebuilt=[];
    let rawIdx=0;
    for(const m of (S.messages||[])){
      if(_messageIsRenderable(m)) rebuilt.push({m,rawIdx});
      rawIdx++;
    }
    _visWithIdxCache=rebuilt;
    _visWithIdxCacheLen=S.messages.length;
    _visWithIdxCacheSrc=S.messages;
  }
  return _visWithIdxCache;
}
function _messageVirtualWindow(opts){
  const total=Math.max(0, Number(opts&&opts.total)||0);
  const threshold=Math.max(1, Number(opts&&opts.threshold)||MESSAGE_VIRTUAL_THRESHOLD_ROWS);
  const defaultHeight=Math.max(1, Number(opts&&opts.defaultHeight)||_messageVirtualDefaultHeightForRole('default'));
  const bufferPx=Math.max(0, Number(opts&&opts.bufferPx)||MESSAGE_VIRTUAL_BUFFER_PX);
  const viewportHeight=Math.max(defaultHeight, Number(opts&&opts.viewportHeight)||defaultHeight*6);
  const keepTailCount=Math.max(0, Number(opts&&opts.keepTailCount)||0);
  const tailStart=Math.max(0, total-keepTailCount);
  const heights=Array.isArray(opts&&opts.heights)?opts.heights:[];
  const roleForIdx=typeof (opts&&opts.roleForIdx)==='function'?opts.roleForIdx:null;
  const rowHeightFor=(idx)=>{
    const cached=Number(heights[idx]);
    if(Number.isFinite(cached)&&cached>0) return cached;
    return roleForIdx?Math.max(1,_messageVirtualDefaultHeightForRole(roleForIdx(idx))):defaultHeight;
  };
  if(total<=Math.max(threshold, keepTailCount)){
    return {virtualized:false,start:0,end:total,topPad:0,bottomPad:0,total,tailStart};
  }
  const scrollTop=Math.max(0, Number(opts&&opts.scrollTop)||0);
  const targetTop=Math.max(0, scrollTop-bufferPx);
  const targetBottom=scrollTop+viewportHeight+bufferPx;
  let start=0;
  let offset=0;
  while(start<tailStart&&offset+rowHeightFor(start)<=targetTop){
    offset+=rowHeightFor(start);
    start++;
  }
  if(start>=tailStart){
    return {virtualized:true,start:tailStart,end:tailStart,topPad:offset,bottomPad:0,total,tailStart};
  }
  let end=start;
  let cursor=offset;
  while(end<tailStart&&cursor<targetBottom){
    cursor+=rowHeightFor(end);
    end++;
  }
  if(end<=start) end=Math.min(total, start+1);
  let bottomPad=0;
  for(let i=end;i<tailStart;i++) bottomPad+=rowHeightFor(i);
  return {
    virtualized:true,
    start,
    end,
    topPad:offset,
    bottomPad,
    total,
    tailStart,
  };
}
function _messageVirtualSpacer(height, where){
  const spacer=document.createElement('div');
  spacer.className='message-virtual-spacer';
  spacer.dataset.virtualSpacer=where||'gap';
  spacer.setAttribute('aria-hidden','true');
  spacer.style.height=Math.max(0,Math.round(height||0))+'px';
  spacer.style.flex='0 0 auto';
  return spacer;
}
function _messageVirtualWindowKeyFor(windowMetrics){
  if(!windowMetrics) return '';
  return [
    windowMetrics.virtualized?1:0,
    windowMetrics.start,
    windowMetrics.end,
    Math.round(windowMetrics.topPad||0),
    Math.round(windowMetrics.bottomPad||0),
    windowMetrics.tailStart||0,
  ].join(':');
}
function _messageVirtualMeasurementCycleKeyFor(windowMetrics){
  if(!windowMetrics) return '';
  return [
    windowMetrics.virtualized?1:0,
    windowMetrics.start,
    windowMetrics.end,
    windowMetrics.tailStart||0,
  ].join(':');
}
function _resetMessageVirtualMeasurementBurst(){
  _messageVirtualMeasurementSeenKeys=[];
  _messageVirtualMeasurementBurstActive=false;
  // Strip any pending internal provenance: an external reset must never let
  // an internal marker survive onto a render that fires later (#6717 re-gate).
  _messageVirtualRenderQueuedOrigin=null;
}
function _scheduleMessageVirtualMeasurementRefresh(windowMetrics){
  if(_messageVirtualScrollActive){
    _messageVirtualDeferredMeasurement=windowMetrics;
    return;
  }
  const cycleKey=_messageVirtualMeasurementCycleKeyFor(windowMetrics);
  if(_messageVirtualMeasurementCycleKey!==cycleKey){
    _messageVirtualMeasurementCycleKey=cycleKey;
  }
  // Cycle-aware burst tracking (#6717 re-gate): the burst follows ONLY the
  // internally measurement-scheduled chain (requestAnimationFrame ->
  // _scheduleMessageVirtualizedRender -> renderMessages -> re-measure -> here).
  // Within one burst we remember every cycle key already seen. An UNSEEN key
  // (genuine forward convergence A->B->C->settled ‚Äî content reflow, late
  // fonts/images, dynamic height) may proceed, so convergence is never capped
  // at a flat render count; a key that REPEATS (WebKit's A->B->A->B window
  // oscillation) ends the burst, so the rAF/measure loop can never run forever
  // (#6654). The burst starts fresh on every EXTERNALLY initiated render
  // (session load, message append, real content change ‚Äî see renderMessages)
  // and on measurement settlement ‚Äî never on a cycle-key change alone.
  if(!_messageVirtualMeasurementBurstActive){
    _messageVirtualMeasurementSeenKeys=[];
    _messageVirtualMeasurementBurstActive=true;
  }
  // Absolute per-burst cap (#6717 re-gate): even an ALL-DISTINCT monotonic
  // key sequence (A->B->C->D->... never repeating) must terminate ‚Äî a repeated
  // key is not the only way the burst can end. This also bounds the seen-key
  // collection to the cap (memory). Distinct-key convergence settles in a
  // handful of frames, so the cap is far above any legitimate multi-pass
  // reflow while still closing the #6654 CPU-runaway class.
  if(_messageVirtualMeasurementSeenKeys.length>=MESSAGE_VIRTUAL_MEASUREMENT_MAX_RERENDERS){
    _resetMessageVirtualMeasurementBurst();
    return;
  }
  const lastKey = _messageVirtualMeasurementSeenKeys.length ? _messageVirtualMeasurementSeenKeys[_messageVirtualMeasurementSeenKeys.length - 1] : null;
  if(cycleKey !== lastKey && _messageVirtualMeasurementSeenKeys.includes(cycleKey)){
    // Non-consecutive repeated key (A->B->A): the window is oscillating, not
    // converging. End the burst (no further internal re-render is scheduled),
    // so the next externally initiated cycle starts fresh instead of being
    // starved of retries. A consecutive same-key pass (A->A) proceeds because
    // heights changed while the window bounds did not (e.g. row shrink across
    // two passes), still bounded by the absolute per-burst cap (#6717 re-gate).
    _resetMessageVirtualMeasurementBurst();
    return;
  }
  _messageVirtualMeasurementSeenKeys.push(cycleKey);
  // The internal-measurement origin travels WITH the scheduled render request
  // (threaded through BOTH rAF layers into renderMessages), so coalescing can
  // never consume a stale global marker onto an unrelated render. When an
  // external request coalesces into the queued rAF, EXTERNAL wins and the
  // render that fires degrades to an unmarked (burst-resetting) render
  // (#6717 re-gate).
  requestAnimationFrame(()=>{ _scheduleMessageVirtualizedRender(true,{origin:'internal'}); });
}
function _markMessageVirtualMeasurementsSettled(windowMetrics){
  _messageVirtualMeasurementCycleKey=_messageVirtualMeasurementCycleKeyFor(windowMetrics);
  _resetMessageVirtualMeasurementBurst();
}
function _messageVirtualHeightEntryMatches(previousEntry, nextEntry){
  return !!(
    previousEntry&&nextEntry&&
    previousEntry.m===nextEntry.m
  );
}
function _messageVirtualHeightPrefixEntryMatches(previousEntry, nextEntry){
  return !!(
    previousEntry&&nextEntry&&
    previousEntry.rawIdx===nextEntry.rawIdx&&
    _messageVirtualHeightEntryMatches(previousEntry, nextEntry)
  );
}
function _syncMessageVirtualHeightCache(visWithIdx){
  const nextEntries=Array.isArray(visWithIdx)
    ? visWithIdx.map(entry=>entry?{rawIdx:entry.rawIdx,m:entry.m}:entry)
    : [];
  if(
    _messageVirtualHeightCacheLen===S.messages.length &&
    _messageVirtualHeightCacheSrc===S.messages &&
    _messageVirtualHeightCacheEntries.length===nextEntries.length
  ) return;
  const previousEntries=Array.isArray(_messageVirtualHeightCacheEntries)?_messageVirtualHeightCacheEntries:[];
  const previousHeights=Array.isArray(_messageVirtualHeightCache)?_messageVirtualHeightCache.slice():[];
  let nextHeights=null;
  if(!previousEntries.length){
    nextHeights=new Array(nextEntries.length);
  }else if(!nextEntries.length){
    _clearMessageVirtualHeightCache();
    _messageVirtualHeightCacheLen=S.messages.length;
    _messageVirtualHeightCacheSrc=S.messages;
    return;
  }else{
    const sharedPrefix=Math.min(previousEntries.length,nextEntries.length);
    let prefixMatches=true;
    for(let i=0;i<sharedPrefix;i++){
      if(!_messageVirtualHeightPrefixEntryMatches(previousEntries[i], nextEntries[i])){
        prefixMatches=false;
        break;
      }
    }
    if(prefixMatches){
      nextHeights=previousHeights.slice(0, sharedPrefix);
      nextHeights.length=nextEntries.length;
    }else if(nextEntries.length>=previousEntries.length){
      const prependedCount=nextEntries.length-previousEntries.length;
      let suffixMatches=true;
      for(let i=0;i<previousEntries.length;i++){
        if(!_messageVirtualHeightEntryMatches(previousEntries[i], nextEntries[i+prependedCount])){
          suffixMatches=false;
          break;
        }
      }
      if(suffixMatches){
        nextHeights=new Array(nextEntries.length);
        for(let i=0;i<previousEntries.length;i++){
          nextHeights[prependedCount+i]=previousHeights[i];
        }
      }
    }
  }
  if(nextHeights===null){
    _clearMessageVirtualHeightCache();
    _messageVirtualHeightCache=new Array(nextEntries.length);
  }else{
    _messageVirtualHeightCache=nextHeights;
    _messageVirtualWindowKey='';
  }
  _messageVirtualHeightCacheEntries=nextEntries;
  _messageVirtualHeightCacheLen=S.messages.length;
  _messageVirtualHeightCacheSrc=S.messages;
}
function _messageVirtualRoleForEntry(entry){
  const m=entry&&entry.m;
  if(!m) return 'default';
  if(m._source === 'process_wakeup') return 'process_wakeup';
  if(m.role==='user') return 'user';
  if(m.role==='assistant'){
    if((Array.isArray(m.tool_calls)&&m.tool_calls.length>0)||
       (Array.isArray(m.content)&&m.content.some(p=>p&&p.type==='tool_use'))||
       (Array.isArray(m._partial_tool_calls)&&m._partial_tool_calls.length>0))
      return 'tool_call';
    return 'assistant';
  }
  return 'default';
}
function _currentMessageVirtualWindow(visWithIdx, keepTailCount){
  _syncMessageVirtualHeightCache(visWithIdx);
  const container=$('messages');
  // #4325 opt-out: when the user disables transcript virtualization, always
  // render the full transcript (no windowing). Mirrors the <=threshold path so
  // every downstream consumer (render, anchor, prepend-delta) treats it as a
  // plain non-virtualized list.
  if(typeof window!=='undefined' && window._virtualizeTranscript===false){
    const total=visWithIdx.length;
    const tailStart=Math.max(0, total-Math.max(0, Number(keepTailCount)||0));
    return {virtualized:false,start:0,end:total,topPad:0,bottomPad:0,total,tailStart};
  }
  return _messageVirtualWindow({
    total:visWithIdx.length,
    scrollTop:container?container.scrollTop:0,
    viewportHeight:container?container.clientHeight:(_messageVirtualEstimatedRowHeight*6),
    heights:_messageVirtualHeightCache,
    defaultHeight:_messageVirtualEstimatedRowHeight,
    roleForIdx:idx=>_messageVirtualRoleForEntry(visWithIdx[idx]),
    keepTailCount,
  });
}
function _messageVirtualPrependedHeightDelta(prependedRenderableCount){
  const count=Math.max(0, Number(prependedRenderableCount)||0);
  if(count<=0) return null;
  const visWithIdx=_getVisibleMessagesWithIdx();
  const virtualWindow=_currentMessageVirtualWindow(visWithIdx,_messageVirtualKeepTailCount());
  if(!virtualWindow||!virtualWindow.virtualized) return null;
  const limit=Math.min(count,_messageVirtualHeightCache.length);
  let total=0;
  for(let i=0;i<limit;i++){
    const cached=Number(_messageVirtualHeightCache[i]);
    total+=(Number.isFinite(cached)&&cached>0)?cached:_messageVirtualDefaultHeightForRole(_messageVirtualRoleForEntry(visWithIdx[i]));
  }
  return Math.max(0,Math.round(total));
}
function _messageVisibleIndexForRawIdx(rawIdx, visWithIdx){
  const list=Array.isArray(visWithIdx)?visWithIdx:_getVisibleMessagesWithIdx();
  for(let i=0;i<list.length;i++){
    if(list[i]&&list[i].rawIdx===rawIdx) return i;
  }
  return -1;
}
function _safeEncodeURIComponent(v){
  try{return encodeURIComponent(String(v));}
  catch(e){
    // encodeURIComponent threw URIError -> one or more lone UTF-16 surrogates.
    // Walk the string as UTF-16 code units: keep valid high(D800-DBFF) +
    // low(DC00-DFFF) pairs intact (so emoji survive) and drop lone surrogates.
    // No regex lookbehind/lookahead so this parses on every browser engine
    // (some older WebViews / Safari <16.4 don't support lookbehind in regex
    // literals, which would otherwise brick ui.js at parse time).
    const s=String(v);
    let cleaned='';
    for(let i=0;i<s.length;i++){
      const c=s.charCodeAt(i);
      if(c>=0xD800&&c<=0xDBFF){
        const n=(i+1<s.length)?s.charCodeAt(i+1):0;
        if(n>=0xDC00&&n<=0xDFFF){cleaned+=s[i]+s[i+1];i++;}
      }else if(c<0xDC00||c>0xDFFF){
        cleaned+=s[i];
      }
    }
    return encodeURIComponent(cleaned);
  }
}

function _messageViewportAnchorKeyForMessage(m){
  if(typeof _compressionMessageAnchorKey!=='function') return '';
  const key=_compressionMessageAnchorKey(m);
  if(!key) return '';
  return [key.role||'',key.ts??'',key.attachments??0,key.text||''].map(v=>_safeEncodeURIComponent(v)).join('|');
}
function _messageVisibleIndexForAnchorKey(anchorKey, visWithIdx){
  const key=String(anchorKey||'');
  if(!key) return -1;
  const list=Array.isArray(visWithIdx)?visWithIdx:_getVisibleMessagesWithIdx();
  for(let i=0;i<list.length;i++){
    if(list[i]&&_messageViewportAnchorKeyForMessage(list[i].m)===key) return i;
  }
  return -1;
}
function _messageSessionIndexBase(){
  const n=Number(typeof _oldestIdx!=='undefined'?_oldestIdx:0);
  return Number.isFinite(n)?Math.max(0,n):0;
}
function _messageSessionIndexForRawIdx(rawIdx){
  const n=Number(rawIdx);
  if(!Number.isFinite(n)) return null;
  return _messageSessionIndexBase()+n;
}
function _messageRawIdxForSessionIndex(sessionIdx){
  const n=Number(sessionIdx);
  if(!Number.isFinite(n)) return null;
  return n-_messageSessionIndexBase();
}
function _messageVirtualScrollTopForVisibleIdx(visWithIdx, visibleIdx, container){
  const idx=Math.max(0,Number(visibleIdx)||0);
  _syncMessageVirtualHeightCache(visWithIdx);
  const limit=Math.min(idx,_messageVirtualHeightCache.length);
  let offset=0;
  for(let i=0;i<limit;i++){
    const cached=Number(_messageVirtualHeightCache[i]);
    offset+=(Number.isFinite(cached)&&cached>0)?cached:_messageVirtualDefaultHeightForRole(_messageVirtualRoleForEntry(visWithIdx[i]));
  }
  const viewport=container?Math.max(0,Number(container.clientHeight)||0):0;
  return Math.max(0,Math.round(offset-(viewport*0.35)));
}
function _messageVirtualKeepTailCount(){
  return Math.min(_currentMessageRenderWindowSize(), MESSAGE_RENDER_WINDOW_DEFAULT);
}
function _captureMessageViewportAnchor(){
  const container=$('messages');
  if(!container) return null;
  const containerRect=container.getBoundingClientRect();
  const rows=Array.from(container.querySelectorAll('[data-msg-idx]'));
  for(const row of rows){
    const rawIdx=Number(row&&row.dataset&&row.dataset.msgIdx);
    if(!Number.isFinite(rawIdx)) continue;
    const rect=row.getBoundingClientRect();
    if(rect.bottom>containerRect.top+1){
      const sessionIdx=Number(row&&row.dataset&&row.dataset.sessionMsgIdx);
      // Record the current top-spacer (virtual topPad) height so the compensation
      // path can fall back to a topPad-delta shift when the anchor row itself is
      // recycled out of the render window after a measurement-driven re-render.
      const spacer=container.querySelector('[data-virtual-spacer="before"]');
      const topPadBefore=spacer?parseFloat(spacer.style.height||'0')||0:0;
      return {
        rawIdx,
        sessionIdx:Number.isFinite(sessionIdx)?sessionIdx:_messageSessionIndexForRawIdx(rawIdx),
        key:row&&row.dataset?String(row.dataset.messageAnchorKey||''):'',
        topOffset:rect.top-containerRect.top,
        topPadBefore,
        // Snapshot the scroll height at capture so a later realign can detect that
        // content grew between capture and restore ‚Äî the streaming case where the
        // anchor's topOffset is stale and realigning to it would yank a still reader
        // backward (issue #5637).
        scrollHeightAtCapture:container.scrollHeight,
        inputGeneration:typeof _messageScrollInputGeneration==='number' ? _messageScrollInputGeneration : 0,
      };
    }
  }
  return null;
}
// Temporarily suppress the browser's native overflow-anchor on a scroll
// container so a JS scrollTop write is not double-compensated by the browser's
// own scroll-anchoring in the same frame. Returns a release fn that restores the
// prior inline value on the NEXT frame (after layout settles). No-op on desktop,
// where the resting computed value is already `none` (CSS hover/fine-pointer
// media query) ‚Äî suppressing `none` changes nothing and the release restores the
// same empty inline value. Only mobile (resting `auto`) is actually affected,
// which is exactly where the double-compensation jump-back happens.
//
// Both this helper and _fixMobileScrollJank() gate on the SAME question ‚Äî "is
// the browser's native scroll-anchor layer currently active on this element?" ‚Äî
// routed through this one predicate so the two guards can't drift apart if the
// CSS media query ever changes (maintainer review on #5338). The computed-value
// test is more robust than a matchMedia('(hover:hover) and (pointer:fine)')
// check because it reflects the real resting value, including any inline
// override, not just the viewport media state.
function _browserOverflowAnchorActive(el){
  if(!el) return false;
  try{ return getComputedStyle(el).overflowAnchor==='auto'; }catch(_){ return false; }
}
// iOS/iPadOS WebKit detection for the issue #5637 stale-anchor hold gate. CSS
// overflow-anchor is INERT on iOS WebKit (see static/style.css ‚Äî the mobile
// content-visibility block deliberately does NOT set overflow-anchor:none because
// it is a no-op on iOS and, on Android, re-opens the #4856/#5338 jump-to-top
// regression). So `overflow-anchor:auto` computes on `.messages` on iOS but the
// engine never actually holds the viewport there. The stale-anchor refusal relies
// on that engine to hold the reader, so it is only safe on Android (working
// overflow-anchor), NOT iOS ‚Äî refusing on iOS leaves a scrolled-up reader unheld,
// the same class as the desktop regression, one platform over.
// Detection covers classic iPhone/iPod/iPad UAs AND iPadOS 13+, which reports a
// desktop 'MacIntel' platform but is distinguishable by touch support (a real Mac
// has maxTouchPoints 0). Excludes MSStream (old IE on Windows Phone false-matched
// 'like iPhone').
function _isIOSWebKit(){
  try{
    const nav=(typeof navigator!=='undefined')?navigator:null;
    if(!nav) return false;
    if(nav.MSStream) return false;
    const ua=String(nav.userAgent||'');
    if(/iP(ad|hone|od)/.test(ua)) return true;
    // iPadOS 13+ masquerades as macOS; a Mac has no touch, an iPad does.
    if(nav.platform==='MacIntel' && Number(nav.maxTouchPoints)>1) return true;
  }catch(_){}
  return false;
}
// Stable "native overflow-anchor holds this viewport" predicate for the issue
// #5637 stale-anchor hold gate. The two stale-anchor refusals below assume the
// browser's native overflow-anchor layer will hold the viewport once the JS
// restore is refused. That is only true where the engine ACTUALLY compensates:
//   - desktop (hover+fine-pointer): CSS keeps `.messages` at overflow-anchor:none
//     -> engine off -> refusing leaves nothing to hold the reader. Excluded via
//     matchMedia('(pointer:coarse)') being false.
//   - iOS WebKit: overflow-anchor is INERT (see _isIOSWebKit) even though it
//     computes to `auto` -> engine never holds -> refusing strands a scrolled-up
//     reader. Excluded via _isIOSWebKit().
//   - Android touch: overflow-anchor:auto AND the engine works -> refusing is safe,
//     native anchoring holds. This is the ONLY platform the refusal targets.
// We must NOT decide this with `_browserOverflowAnchorActive(#messages)` alone,
// because `_restoreMessageViewportAnchor` temporarily writes an inline
// `overflowAnchor:'none'` on #messages for its own scroll write and only restores
// it on the next frame; when the realign fires every live tick that inline 'none'
// persists across ticks, so a computed-value probe would read 'none' mid-realign
// and wrongly classify a touch device as "desktop", letting the stale realign
// through. A matchMedia('(pointer:coarse)') test reflects the input device and
// cannot be mutated by that inline override, so it stays steady mid-realign;
// desktop (fine pointer) stays false. Fall back to the computed-anchor probe when
// matchMedia is unavailable.
function _isTouchLikeMessageViewport(el){
  // iOS WebKit is touch (pointer:coarse) but overflow-anchor is inert there, so the
  // refusal's premise fails ‚Äî treat it like desktop (keep the semantic realign).
  if(_isIOSWebKit()) return false;
  try{
    if(typeof matchMedia==='function' && matchMedia('(pointer:coarse)').matches) return true;
  }catch(_){}
  // Best-effort fallback for the (today essentially non-existent) no-matchMedia
  // environment: the computed-anchor probe can transiently read 'none' during a
  // realign burst (see comment above), so on such a touch device this could
  // re-admit the original yank. matchMedia('(pointer:coarse)') is universally
  // supported in every browser this UI targets, so the primary path is what runs.
  return _browserOverflowAnchorActive(el);
}
function _suppressBrowserOverflowAnchor(container){
  if(!container||!container.style) return null;
  // Only engage when the browser layer is actually active (auto). On desktop
  // (none) there is nothing to suppress.
  if(!_browserOverflowAnchorActive(container)) return null;
  const prevInline=container.style.overflowAnchor||'';
  container.style.overflowAnchor='none';
  let released=false;
  return function _release(){
    if(released) return;
    released=true;
    const restore=()=>{
      // Only restore if we still own the suppression (another render may have
      // re-set it); compare against the value we wrote.
      if(container.style.overflowAnchor==='none') container.style.overflowAnchor=prevInline;
    };
    if(typeof requestAnimationFrame==='function') requestAnimationFrame(restore);
    else restore();
  };
}
function _restoreMessageViewportAnchor(anchor, rawIdxDelta){
  const container=$('messages');
  if(!container||!anchor) return false;
  const anchorKey=String(anchor.key||'');
  const sessionIdx=Number(anchor.sessionIdx);
  const hasSessionIdx=Number.isFinite(sessionIdx);
  let row=anchorKey?Array.from(container.querySelectorAll('[data-message-anchor-key]')).find(el=>el&&el.dataset&&el.dataset.messageAnchorKey===anchorKey):null;
  if(row&&row.getClientRects&&row.getClientRects().length===0) row=null;
  // The anchor key is content-derived (role|ts|attachments|first-160-chars, built by
  // _messageViewportAnchorKeyForMessage) so it goes STALE while a live assistant
  // message is still streaming: every chunk that changes the first 160 chars
  // recomputes that row's data-message-anchor-key, so a snapshot captured mid-stream
  // no longer matches by key. We used to concede the moment the keyed lookup missed
  // (`if(!row&&anchorKey) return false`), and the caller then fell back to an ABSOLUTE
  // scrollTop=snapshot.top that does NOT compensate the above-viewport height growth
  // from that same streaming chunk ‚Äî the residual DESKTOP scroll jump-back. (Desktop
  // rests at overflow-anchor:none, so #5392's mobile overflow-anchor guard is a no-op
  // here; this is a distinct code path.) The anchored row is still in the DOM under
  // its STABLE session-relative index, so recover it via sessionIdx before conceding.
  // A genuinely removed anchor (message compressed/deleted away) misses key AND
  // sessionIdx and still returns false. A missing sessionIdx is NOT degraded to the
  // window-relative rawIdx (which could resolve to a different message), preserving
  // the original per-tier guard.
  if(!row&&hasSessionIdx) row=container.querySelector(`[data-session-msg-idx="${sessionIdx}"]`);
  if(!row&&(anchorKey||hasSessionIdx)) return false;
  const targetIdx=Number(anchor.rawIdx)+Number(rawIdxDelta||0);
  if(!row&&Number.isFinite(targetIdx)) row=container.querySelector(`[data-msg-idx="${targetIdx}"]`);
  if(!row) return false;
  const containerRect=container.getBoundingClientRect();
  const rect=row.getBoundingClientRect();
  const targetTop=Number(anchor.topOffset)||0;
  // Streaming stale-anchor guard (issue #5637). During a live stream, content grows
  // ABOVE the viewport between anchor capture and this restore, so the anchor's
  // captured topOffset is stale and the realign delta becomes a spurious few-hundred-px
  // value that yanks a still reader backward. Detect it by content growth + absence of
  // real input intent ‚Äî NOT by a scrollTop diff, because on an overflow-anchor:auto
  // container the browser itself moves scrollTop to compensate the growth (so a still
  // reader's scrollTop is not stationary). _recentMessage*ScrollIntent reflects genuine
  // touch/wheel/key input, which the browser's anchor layer never writes. If content
  // grew since capture AND there is no recent input intent AND the realign would move
  // scrollTop non-trivially, refuse it and let the browser overflow-anchor hold. An
  // actively scrolling reader (recent intent) keeps the legitimate realign; legacy
  // snapshots without the captured geometry keep prior behavior.
  //
  // Desktop guard (issue #5637 gate cert): the refusal is only safe where the
  // browser's native overflow-anchor layer can actually hold the viewport, i.e.
  // touch viewports where `.messages` computes to `overflow-anchor:auto`. On
  // hover+fine-pointer desktops `.messages` is `overflow-anchor:none`, so refusing
  // the realign would leave NOTHING to hold the reader after above-viewport growth
  // ‚Äî the very yank this fixes on mobile, reintroduced on desktop. Gate the refusal
  // on `_isTouchLikeMessageViewport` so desktop keeps its semantic scrollTop realign.
  const _realignDelta=(rect.top-containerRect.top)-targetTop;
  const _shAtCap=Number(anchor.scrollHeightAtCapture);
  if(Number.isFinite(_shAtCap)){
    const _grewSinceCapture=(container.scrollHeight-_shAtCap)>4;
    const _activeIntent=(typeof _recentMessageScrollIntent==='function' && _recentMessageScrollIntent())
      || (typeof _recentMessageTouchScrollIntent==='function' && _recentMessageTouchScrollIntent());
    const _touchHold=(typeof _isTouchLikeMessageViewport==='function' && _isTouchLikeMessageViewport(container));
    if(_touchHold&&_grewSinceCapture&&!_activeIntent&&Math.abs(_realignDelta)>8){
      return false;
    }
  }
  _programmaticScroll=true;_programmaticScrollSetAt=performance.now();
  // Mobile-only jump fix: the resting overflow-anchor on .messages is `auto` on
  // touch devices (CSS media query keeps it `none` only for hover+fine-pointer
  // desktops). When we write scrollTop here to realign the anchor row, a mobile
  // browser's OWN overflow-anchor machinery ALSO shifts scrollTop in the same
  // frame if content height above the viewport changed ‚Äî the two compensations
  // stack and yank the reader to an unrelated turn (the mobile jump-back). This is why the
  // bug is mobile-only and never reproduces on a desktop (none) browser. Suppress
  // the browser layer for this write; _releaseAnchorSuppression restores it next
  // frame. Desktop is already `none`, so this is a no-op there.
  const _releaseAnchorSuppression=(typeof _suppressBrowserOverflowAnchor==='function')
    ? _suppressBrowserOverflowAnchor(container) : null;
  container.scrollTop+=(rect.top-containerRect.top)-targetTop;
  if(_releaseAnchorSuppression) _releaseAnchorSuppression();
  if(typeof _deferClearProgrammaticScroll==='function') _deferClearProgrammaticScroll();
  else requestAnimationFrame(()=>{ setTimeout(()=>{ _programmaticScroll=false; },0); });
  return true;
}
let _messageViewportAnchorRemounting=false;
function _remountMessageViewportAnchor(anchor){
  const container=$('messages');
  if(!container||!anchor||_messageViewportAnchorRemounting) return false;
  const anchorKey=String(anchor.key||'');
  const visibleKeyNode=anchorKey
    ? Array.from(container.querySelectorAll('[data-message-anchor-key]')).find(node=>node&&node.dataset&&node.dataset.messageAnchorKey===anchorKey&&(!node.getClientRects||node.getClientRects().length>0))
    : null;
  if(visibleKeyNode) return true;
  const sessionIdx=Number(anchor.sessionIdx);
  const hasSessionIdx=Number.isFinite(sessionIdx);
  if(!anchorKey&&hasSessionIdx&&container.querySelector(`[data-session-msg-idx="${sessionIdx}"]`)) return true;
  const targetIdx=Number(anchor.rawIdx);
  if(!anchorKey&&!hasSessionIdx&&Number.isFinite(targetIdx)&&container.querySelector(`[data-msg-idx="${targetIdx}"]`)) return true;
  if(typeof _getVisibleMessagesWithIdx!=='function'||
     typeof _messageVisibleIndexForRawIdx!=='function'||
     typeof _messageVirtualScrollTopForVisibleIdx!=='function'||
     typeof renderMessages!=='function') return false;
  const visWithIdx=_getVisibleMessagesWithIdx();
  let visIdx=anchorKey?_messageVisibleIndexForAnchorKey(anchorKey,visWithIdx):-1;
  if(visIdx<0&&hasSessionIdx){
    const rawFromSession=_messageRawIdxForSessionIndex(sessionIdx);
    if(Number.isFinite(rawFromSession)) visIdx=_messageVisibleIndexForRawIdx(rawFromSession,visWithIdx);
  }
  if(visIdx<0&&Number.isFinite(targetIdx)) visIdx=_messageVisibleIndexForRawIdx(targetIdx,visWithIdx);
  if(visIdx<0) return false;
  // A virtualized anchor may be outside the current DOM. Scroll to its virtual
  // row and render once so the semantic restore below has a real target.
  _programmaticScroll=true;_programmaticScrollSetAt=performance.now();
  container.scrollTop=_messageVirtualScrollTopForVisibleIdx(visWithIdx,visIdx,container);
  _messageVirtualWindowKey='';
  _messageViewportAnchorRemounting=true;
  try{
    renderMessages({preserveScroll:true});
  }finally{
    _messageViewportAnchorRemounting=false;
    requestAnimationFrame(()=>{ setTimeout(()=>{ _programmaticScroll=false; },0); });
  }
  if(anchorKey){
    return !!Array.from(container.querySelectorAll('[data-message-anchor-key]')).find(node=>node&&node.dataset&&node.dataset.messageAnchorKey===anchorKey&&(!node.getClientRects||node.getClientRects().length>0));
  }
  if(hasSessionIdx) return !!container.querySelector(`[data-session-msg-idx="${sessionIdx}"]`);
  return Number.isFinite(targetIdx)&&!!container.querySelector(`[data-msg-idx="${targetIdx}"]`);
}
function _compensateScrollForMeasurementDelta(renderFn){
  const container=$('messages');
  if(!container) return renderFn();
  const anchorBefore=_captureMessageViewportAnchor();
  const scrollTopBefore=container.scrollTop;
  container.classList.add('vscroll-measuring');
  try{ renderFn(); }finally{ container.classList.remove('vscroll-measuring'); }
  if(!anchorBefore) return;
  if(scrollTopBefore<1){
    const spacer=container.querySelector('[data-virtual-spacer="before"]');
    if(!spacer||parseFloat(spacer.style.height||'0')<=0) return;
  }
  // Re-find the anchor row after the measurement-driven re-render. The primary
  // lookup is by rawIdx (the DOM index), but on a big virtualized session a large
  // scroll delta can RECYCLE the old anchor row out of the render window entirely
  // (verified via real-device telemetry: DOM collapsed to 1 row, scrollHeight
  // lurched by tens of thousands of px). The old code did `if(!row) return` here,
  // abandoning compensation ‚Üí the full estimated‚Üîmeasured height lurch hit
  // scrollTop uncompensated and threw the viewport to the top (the recurring
  // mobile scroll jump-back). Fall back to the stable sessionIdx anchor (captured in
  // _captureMessageViewportAnchor) before giving up, mirroring the "recover via
  // sessionIdx when the primary anchor key is gone" approach used elsewhere but for
  // the virtualization-measurement compensation path.
  let row=container.querySelector(`[data-msg-idx="${anchorBefore.rawIdx}"]`);
  if(!row&&Number.isFinite(Number(anchorBefore.sessionIdx))){
    row=container.querySelector(`[data-session-msg-idx="${anchorBefore.sessionIdx}"]`);
  }
  if(!row){
    // Anchor row is no longer rendered (recycled out of the virtual window). We
    // cannot measure its live offset, but we CAN keep the viewport visually
    // stable by compensating for the top-spacer (topPad) height change: the
    // whole reason scrollHeight lurched is that the estimated topPad was replaced
    // by a measured one. Shift scrollTop by that same delta so content under the
    // viewport does not appear to jump. Without this the browser lands at an
    // uncompensated absolute scrollTop against a wildly different scrollHeight.
    const spacerAfter=container.querySelector('[data-virtual-spacer="before"]');
    const topPadAfter=spacerAfter?parseFloat(spacerAfter.style.height||'0')||0:0;
    const topPadBefore=Number(anchorBefore.topPadBefore);
    if(Number.isFinite(topPadBefore)){
      const padDelta=topPadAfter-topPadBefore;
      if(Math.abs(padDelta)>=2){
        _programmaticScroll=true;_programmaticScrollSetAt=performance.now();
        container.scrollTop=Math.max(0,scrollTopBefore+padDelta);
        _lastScrollTop=container.scrollTop;
        _deferClearProgrammaticScroll();
      }
    }
    return;
  }
  const containerRect=container.getBoundingClientRect();
  const rowRect=row.getBoundingClientRect();
  const actualOffset=rowRect.top-containerRect.top;
  const delta=actualOffset-anchorBefore.topOffset;
  if(Math.abs(delta)<2) return;
  _programmaticScroll=true;_programmaticScrollSetAt=performance.now();
  container.scrollTop=scrollTopBefore+delta;
  _lastScrollTop=container.scrollTop;
  _deferClearProgrammaticScroll();
}
function _messageViewportIntersectsRenderedRow(){
  const container=$('messages');
  if(!container) return true;
  const containerRect=container.getBoundingClientRect();
  const rows=Array.from(container.querySelectorAll('[data-msg-idx]'));
  for(const row of rows){
    const rect=row.getBoundingClientRect();
    if(rect.bottom>containerRect.top+1&&rect.top<containerRect.bottom-1) return true;
  }
  return false;
}
// #5637/#5638 follow-up ‚Äî kill the content-visibility scrollHeight collapse at its
// source. A virtualization wipe-and-rebuild recreates user rows as FRESH elements, which
// discards content-visibility:auto's last-remembered size, so an off-screen user row
// falls back to the flat `contain-intrinsic-size: auto 96px` estimate in the stylesheet.
// A tall user row (e.g. a long paste) then collapses scrollHeight by (realHeight-96px)
// the instant it's rebuilt off-screen, and the browser either force-clamps scrollTop
// (dTop‚âàdH layer-1 jump) or re-anchors to a far row (dTop‚â´dH browser re-anchor jump) ‚Äî
// both mobile jump-back classes trace to this one collapse. Remember each user row's
// height keyed by its STABLE session-relative index so a rebuild reserves the real
// height, not 96px. Measured height (exact) wins; before a row is ever measured, a
// content-length estimate reserves the bulk so the fresh-element frame doesn't collapse
// either. Refreshed every measure pass, so edits self-heal. Desktop rests at
// content-visibility:visible (intrinsic-size ignored) ‚Üí inert there, zero behavior change.
const _userRowIntrinsicHeightBySessionIdx=Object.create(null);
// Cleared on session switch alongside _messageVirtualHeightCache (both are
// per-session measured-height caches keyed by session-relative index). Without this,
// keys collide across sessions ‚Äî _messageSessionIndexForRawIdx = _messageSessionIndexBase()
// + rawIdx and the base is 0 for the common non-offset session ‚Äî so a new session's
// off-screen user rows would inherit the previous session's remembered heights and
// inflate scrollHeight until each is re-measured. Delete keys in place to keep the
// const binding stable for any closure that captured it.
function _clearUserRowIntrinsicHeightCache(){
  for(const k in _userRowIntrinsicHeightBySessionIdx) delete _userRowIntrinsicHeightBySessionIdx[k];
}
function _rememberUserRowIntrinsicHeight(sessionMsgIdx, height){
  const key=Number(sessionMsgIdx);
  if(!Number.isFinite(key)||!(height>0)) return;
  _userRowIntrinsicHeightBySessionIdx[key]=Math.round(height);
}
function _estimateUserRowIntrinsicHeight(rawText){
  const t=String(rawText||'');
  if(!t) return 96;
  // ~48 half-width chars/line at the mobile user-bubble width (‚âà90% of a phone viewport),
  // ~22px per line + ~24px row chrome; floored at the stylesheet's 96px so a short row never
  // reserves LESS than today (estimate can only add reserved height for tall rows, never
  // regress). CJK / full-width characters occupy ~2 columns each, so a Chinese/Japanese/
  // Korean paste wraps at ~24 chars/line ‚Äî counting them as 1 badly UNDER-estimates the
  // height (a 3k-char CJK paste is ~2x taller than the naive length/48 guess). Weight wide
  // characters as 2 columns so the fresh-row reserve is close to reality even for a row the
  // reader has never scrolled into view (content-visibility:auto reports only the reserve
  // for a never-painted row, so a good estimate is the only backstop there). Uses a Unicode
  // range test (no \p{} ‚Äî keep the RegExp engine-portable across the supported browsers).
  const explicitLines=(t.match(/\n/g)||[]).length+1;
  let columns=0;
  for(let i=0;i<t.length;i++){
    const c=t.charCodeAt(i);
    // CJK Unified + Ext-A, Hiragana/Katakana, Hangul, CJK symbols/punctuation, full-width forms.
    const wide=(c>=0x1100&&c<=0x115F)||(c>=0x2E80&&c<=0xA4CF)||(c>=0xAC00&&c<=0xD7A3)||
               (c>=0xF900&&c<=0xFAFF)||(c>=0xFE30&&c<=0xFE4F)||(c>=0xFF00&&c<=0xFF60)||(c>=0xFFE0&&c<=0xFFE6);
    columns+=wide?2:1;
  }
  const wrapLines=Math.ceil(columns/48);
  const lines=Math.max(explicitLines, wrapLines);
  return Math.max(96, Math.round(lines*22+24));
}
function _applyUserRowIntrinsicHeight(row, rawText){
  if(!row||!row.style||!row.dataset) return;
  const key=Number(row.dataset.sessionMsgIdx);
  const remembered=Number.isFinite(key)?Number(_userRowIntrinsicHeightBySessionIdx[key])||0:0;
  const estimate=_estimateUserRowIntrinsicHeight(rawText!=null?rawText:row.dataset.rawText);
  // Reserve the LARGER of the remembered measurement and the content estimate. A remembered
  // height can be a PARTIAL paint: a user row taller than the viewport that only ever had its
  // top slice scrolled through content-visibility:auto reports just the painted portion, not
  // its full height ‚Äî persisting that would under-reserve and let scrollHeight collapse on the
  // next rebuild (the jump-back). Taking the max means a good estimate floors the reserve even
  // when the measurement under-read, while a full measurement (row shorter than the viewport,
  // fully painted) still wins when it exceeds the estimate.
  const h=Math.max(remembered, estimate);
  if(h>0) row.style.containIntrinsicSize='auto '+Math.round(h)+'px';
}
function _measureMessageVirtualRow(inner, entry){
  if(!inner||!entry) return 0;
  const primary=inner.querySelector(`[data-msg-idx="${entry.rawIdx}"]`);
  if(!primary) return 0;
  let totalHeight=Math.max(0, primary.getBoundingClientRect().height||0);
  if(primary.classList.contains('assistant-segment')){
    let sibling=primary.nextElementSibling;
    while(sibling){
      if(sibling.hasAttribute('data-msg-idx')) break;
      if(!(sibling.matches&&sibling.matches('.tool-call-group,.tool-card-row,.agent-activity-thinking,.thinking-card-row'))) break;
      totalHeight+=Math.max(0, sibling.getBoundingClientRect().height||0);
      sibling=sibling.nextElementSibling;
    }
  }
  // Persist the measured height so a later wipe-and-rebuild of this user row reserves its
  // real off-screen height instead of collapsing to the 96px estimate (the collapse that
  // clamps/re-anchors the viewport ‚Äî #5637/#5638 mobile jump-back, both classes). The
  // typeof guard keeps _measureMessageVirtualRow runnable in the node test harnesses that
  // extract it without this helper (they stub every collaborator by name).
  if(totalHeight>0 && primary.dataset && primary.dataset.role==='user'
     && typeof _rememberUserRowIntrinsicHeight==='function'){
    _rememberUserRowIntrinsicHeight(primary.dataset.sessionMsgIdx, totalHeight);
    primary.style.containIntrinsicSize='auto '+Math.round(totalHeight)+'px';
  }
  return totalHeight;
}
function _updateMessageVirtualMeasurements(renderVisWithIdx, renderVisibleIdxs, virtualWindow){
  const inner=$('msgInner');
  if(!inner||!virtualWindow||!virtualWindow.virtualized||!renderVisWithIdx.length) return;
  let changed=false;
  let measuredCount=0;
  let measuredTotal=0;
  for(let vi=0;vi<renderVisWithIdx.length;vi++){
    const entry=renderVisWithIdx[vi];
    if(!entry) continue;
    const totalHeight=_measureMessageVirtualRow(inner, entry);
    if(totalHeight<=0) continue;
    const visibleIdx=Number(renderVisibleIdxs&&renderVisibleIdxs[vi]);
    if(!Number.isFinite(visibleIdx)) continue;
    if(Math.abs((Number(_messageVirtualHeightCache[visibleIdx])||0)-totalHeight)>1){
      _messageVirtualHeightCache[visibleIdx]=totalHeight;
      changed=true;
    }
    measuredTotal+=totalHeight;
    measuredCount++;
  }
  if(measuredCount>0){
    _messageVirtualEstimatedRowHeight=Math.max(60, Math.round(measuredTotal/measuredCount));
  }
  if(changed){
    _scheduleMessageVirtualMeasurementRefresh(virtualWindow);
  }else{
    _markMessageVirtualMeasurementsSettled(virtualWindow);
  }
}
// #5638 follow-up ‚Äî the non-virtualized transcript path (the #4325 opt-out, where
// _virtualizeTranscript===false renders every row with no windowing) never runs the
// virtualized measure pass above, so a user row's real height is never remembered.
// content-visibility:auto on user rows then collapses a freshly-rebuilt off-screen tall
// user row to its flat contain-intrinsic-size estimate on every renderMessages() rebuild
// (each streaming frame does inner.innerHTML='' then rebuilds all rows as FRESH elements
// that have never painted at full size). scrollHeight shrinks by (realHeight-estimate),
// the browser force-clamps scrollTop, and the viewport jumps backward ‚Äî the desktop/mobile
// jump-back, with JS=none because the clamp is the browser's own.
//
// The reliable moment to read a user row's REAL height is JUST BEFORE the wipe: the old
// rows are still in the DOM, laid out at full height (content-visibility:auto reports the
// true rect height once an element has painted, at any scroll position ‚Äî verified: a tall
// off-screen user row still measures its real height pre-wipe). A POST-render read is
// unreliable because a freshly-rebuilt off-screen row reports its collapsed reserve, not
// its real size, so it would persist the wrong (small) value. Capture pre-wipe, keyed by
// the stable session-relative index, so the rebuild's _applyUserRowIntrinsicHeight reserves
// the real off-screen height and scrollHeight stays stable across the rebuild.
// Desktop rests at content-visibility:visible (intrinsic-size ignored) ‚Üí inert there.
function _rememberRenderedUserRowIntrinsicHeights(){
  const container=$('messages');
  const inner=$('msgInner');
  if(!container||!inner) return;
  const rows=inner.querySelectorAll('.msg-row[data-role="user"][data-msg-idx]');
  if(!rows.length) return;
  const cRect=container.getBoundingClientRect();
  // Only trust a row that is currently WITHIN (or straddling) the viewport: such a row has
  // been painted at full size, so getBoundingClientRect().height is its REAL height. A row
  // that content-visibility:auto is skipping (fully off-screen and never painted this
  // session) reports only its contain-intrinsic-size reserve ‚Äî persisting THAT would poison
  // the remembered height with the collapsed value and defeat the estimate backstop for a
  // never-seen row. The viewport intersection test is the reliable "has this row painted?"
  // signal (an off-screen row that WAS painted earlier keeps its real height too, but we
  // don't need it here ‚Äî it either was captured on a prior in-view pass or the estimate
  // covers it). Small margin so a row just above/below the fold still counts as painted.
  const margin=Math.max(0, cRect.height||0);
  for(let i=0;i<rows.length;i++){
    const row=rows[i];
    if(!row||!row.dataset||!row.style) continue;
    const r=row.getBoundingClientRect();
    const measured=Math.max(0, r.height||0);
    if(!(measured>0)) continue;
    // In-viewport (with a one-screen margin) ‚áí painted ‚áí height is trustworthy ‚Äî but only
    // for a row that FITS the viewport. A row taller than the viewport only ever paints the
    // intersecting slice under content-visibility:auto, so its measured height is a PARTIAL
    // value, not the full row. Floor every persisted height at the content estimate so a
    // partial paint can never lower the reserve below a reasonable full-row guess; a full
    // paint (short row) still wins when it exceeds the estimate.
    const inView=(r.bottom>=cRect.top-margin)&&(r.top<=cRect.bottom+margin);
    if(!inView) continue;
    const estimate=(typeof _estimateUserRowIntrinsicHeight==='function')
      ? _estimateUserRowIntrinsicHeight(row.dataset.rawText) : 0;
    const h=Math.max(measured, estimate);
    if(!(h>0)) continue;
    const key=Number(row.dataset.sessionMsgIdx);
    const remembered=Number.isFinite(key)?Number(_userRowIntrinsicHeightBySessionIdx[key])||0:0;
    // Keep the tallest reserve seen ‚Äî a row mid-collapse (rebuild transient) can report a
    // shrunken size; never let that overwrite a good taller remembered value.
    if(h>=remembered && typeof _rememberUserRowIntrinsicHeight==='function'){
      _rememberUserRowIntrinsicHeight(row.dataset.sessionMsgIdx, h);
      row.style.containIntrinsicSize='auto '+Math.round(h)+'px';
    }
  }
}
function _scheduleMessageVirtualizedRender(force, request){
  const container=$('messages');
  const inner=$('msgInner');
  if(!container||!inner) return;
  const visWithIdx=_getVisibleMessagesWithIdx();
  const virtualWindow=_currentMessageVirtualWindow(visWithIdx,_messageVirtualKeepTailCount());
  const nextKey=_messageVirtualWindowKeyFor(virtualWindow);
  if(!force&&nextKey===_messageVirtualWindowKey) return;
  if(!virtualWindow.virtualized){
    _messageVirtualWindowKey=nextKey;
    return;
  }
  // The request carries its own origin: the internal measurement chain passes
  // {origin:'internal'} (see _scheduleMessageVirtualMeasurementRefresh);
  // every other caller is external by default.
  const requestOrigin=(request&&request.origin==='internal')?'internal':'external';
  if(_messageVirtualScrollRaf){
    // Coalescing into an already-queued rAF: the queued render is shared, so
    // merge the provenance. EXTERNAL always wins ‚Äî the render that actually
    // fires must reset the burst, and an internal marker must never survive
    // onto an external render (#6717 re-gate). An internal request joining a
    // queued render never downgrades an already-external one.
    if(requestOrigin==='external') _messageVirtualRenderQueuedOrigin='external';
    return;
  }
  _messageVirtualRenderQueuedOrigin=requestOrigin;
  _messageVirtualScrollRaf=requestAnimationFrame(()=>{
    _messageVirtualScrollRaf=0;
    // The provenance belongs to THIS queued render: it was set when the render
    // was scheduled (and possibly flipped to 'external' by a coalesced
    // external request). Consume it here ‚Äî no global consumable flag that an
    // unrelated render could steal (#6717 re-gate).
    const internalMeasurement=_messageVirtualRenderQueuedOrigin==='internal';
    _messageVirtualRenderQueuedOrigin=null;
    const liveVisWithIdx=_getVisibleMessagesWithIdx();
    const liveWindow=_currentMessageVirtualWindow(liveVisWithIdx,_messageVirtualKeepTailCount());
    const liveKey=_messageVirtualWindowKeyFor(liveWindow);
    if(!force&&liveKey===_messageVirtualWindowKey) return;
    if(_scrollbarDragActive){
      _programmaticScroll=true;
      _programmaticScrollSetAt=performance.now();
      _compensateScrollForMeasurementDelta(()=>{ renderMessages({ preserveScroll:true, _internalMeasurement: internalMeasurement }); });
      _deferClearProgrammaticScroll();
      _messageVirtualWindowKey=liveKey;
      return;
    }
    _msgNodeRecycleEnabled=true;
    try{
      _compensateScrollForMeasurementDelta(()=>{ renderMessages({ preserveScroll:true, _internalMeasurement: internalMeasurement }); });
    }
    finally{ _msgNodeRecycleEnabled=false; }
  });
}

// ‚îÄ‚îÄ renderMd / _renderUserFencedBlocks cache ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
// Long sessions re-render the same messages on every renderMessages() call.
// Cache the rendered HTML so unchanged messages skip the expensive regex
// pipeline entirely.  ~95% of messages are identical between renders.
const _renderCache = new Map();
const _renderCacheMax = 300;
function _clearRenderCache(){ _renderCache.clear(); }
function _renderCacheKey(text, isUser){
  // Fold render_user_markdown state into user-message keys so toggling the
  // setting invalidates cached plain-text renders (#3870).
  const p = isUser ? (window._renderUserMarkdown ? 'um' : 'u') : 'a';
  // Short content: use the full string as key (cheap Map lookup).
  // Long content: length + prefix + suffix is good enough ‚Äî collisions on
  // 20-char prefix+suffix are vanishingly rare for chat messages.
  if(text.length <= 500) return p + ':' + text;
  return p + ':' + text.length + ':' + text.slice(0,20) + ':' + text.slice(-20);
}
function _getCachedRender(text, isUser){
  const key = _renderCacheKey(text, isUser);
  const hit = _renderCache.get(key);
  if(hit !== undefined) return hit;
  const rendered = isUser
    ? (window._renderUserMarkdown ? renderMd(text) : _renderUserFencedBlocks(text))
    : renderMd(_stripXmlToolCallsDisplay(String(text)));
  if(_renderCache.size > _renderCacheMax) _renderCache.clear();
  _renderCache.set(key, rendered);
  return rendered;
}
// ‚îÄ‚îÄ Message-level media snapshot stamping ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
// /api/media serves a file's CURRENT bytes. Since ETag revalidation (#6922),
// an in-place overwrite (same filename) also rewrites every historical chat
// preview that referenced it ‚Äî the old/new comparison is lost. At settle time
// the backend freezes the bytes of each local-file MEDIA: reference into a
// content-addressed store and stamps the message with
// `_media_snapshots: {path: digest}`. This helper rewrites the rendered HTML
// of ONE message to append `&snap=<digest>` to the matching /api/media URLs
// (and `data-snap` on lazy-preview placeholders), so old previews keep
// showing the file as it was when the message was emitted.
// Runs AFTER the text-keyed render cache: the cache stays pure-text, and each
// message stamps its own digests ‚Äî two messages with identical text but
// different snapshots (exactly the old/new case) resolve independently.
function _stampMediaSnapshots(html, snaps){
  if(!html || !snaps || typeof snaps !== 'object') return html;
  let out = String(html);
  // Direct media URLs: rewrite each COMPLETE `path=` query value atomically.
  // Value-level parsing (decode the whole value, exact map lookup) instead of
  // substring split/join: when one path is a PREFIX of another
  // (/tmp/a.png vs /tmp/a.png.backup) the naive rewrite corrupts the longer
  // URL and drops its own digest. Matching is boundary-aware ‚Äî the value runs
  // to the next `&` separator or an attribute quote/whitespace ‚Äî so nothing
  // outside the value ever moves.
  out = out.replace(/api\/media[?&]path=([^&"'\s<>]+)/g, (match, encodedPath)=>{
    let decoded;
    try{ decoded = decodeURIComponent(encodedPath); }
    catch(e){ return match; }
    const digest = snaps[decoded];
    if(typeof digest === 'string' && /^[0-9a-f]{64}$/.test(digest)){
      return match + '&snap=' + digest;
    }
    return match;
  });
  // Lazy-preview placeholders: data-path="<html-escaped raw path>" ‚Äî match the
  // complete attribute value, unescape HTML entities, then exact lookup (same
  // prefix-safety: a longer path's attribute can never be partially matched).
  const _unescapeHtml=(s)=>String(s||'').replace(/&quot;/g,'"').replace(/&#39;/g,"'").replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&');
  out = out.replace(/data-path="([^"]*)"/g, (match, rawValue)=>{
    const decoded = _unescapeHtml(rawValue);
    const digest = snaps[decoded];
    if(typeof digest === 'string' && /^[0-9a-f]{64}$/.test(digest)){
      return match + ' data-snap="' + digest + '"';
    }
    return match;
  });
  return out;
}
function _currentMessageRenderWindowSize(){
  return Math.max(
    MESSAGE_RENDER_WINDOW_DEFAULT,
    Number(_messageRenderWindowSize)||MESSAGE_RENDER_WINDOW_DEFAULT
  );
}
function _messageRenderableMessageCount(){
  return _getVisibleMessagesWithIdx().length;
}
function _messageHiddenBeforeCount(){
  return Math.max(0,_messageRenderableMessageCount()-_currentMessageRenderWindowSize());
}
function _isSessionEndlessScrollEnabled(){
  return window._sessionEndlessScrollEnabled===true;
}
function _wireMessageWindowLoadEarlierButton(){
  const indicator=$('loadOlderIndicator');
  if(!indicator) return;
  indicator.onclick=()=>{
    if(typeof _loadOlderMessages==='function') _loadOlderMessages();
  };
}
function _isSessionJumpButtonsEnabled(){
  return window._sessionJumpButtonsEnabled===true;
}
function _applySessionNavigationPrefs(){
  const container=$('messages');
  if(container) container.classList.toggle('session-nav-enabled',_isSessionJumpButtonsEnabled());
  _updateSessionStartJumpButton();
}
function _updateSessionStartJumpButton(){
  const btn=$('jumpToSessionStartBtn');
  const container=$('messages');
  if(!btn||!container) return;
  if(!_isSessionJumpButtonsEnabled()){
    btn.style.display='none';
    return;
  }
  const hasSession=!!(S&&S.session&&S.messages&&S.messages.length);
  const awayFromStart=container.scrollTop>Math.max(240,container.clientHeight*0.35);
  const hasScrollableHistory=container.scrollHeight>container.clientHeight+Math.max(240,container.clientHeight*0.35);
  const canRevealStart=hasScrollableHistory||_messageHiddenBeforeCount()>0||!!(typeof _messagesTruncated!=='undefined'&&_messagesTruncated);
  btn.style.display=(hasSession&&canRevealStart&&awayFromStart)?'flex':'none';
}
async function jumpToSessionStart(){
  const container=$('messages');
  if(!container||!S.session) return;
  _scrollPinned=false;
  _messageUserUnpinned=true;
  _programmaticScroll=true;_programmaticScrollSetAt=performance.now();
  try{
    // During active streaming, skip full message load ‚Äî API response won't
    // include live messages from the current turn, and replacing S.messages
    // would lose user/assistant/tool messages.
    if(!(S.busy||S.activeStreamId)){
      if(typeof _ensureAllMessagesLoaded==='function') await _ensureAllMessagesLoaded();
    }
    _messageRenderWindowSize=Math.max(_currentMessageRenderWindowSize(),_messageRenderableMessageCount());
    container.scrollTop=0;
    _messageVirtualWindowKey='';
    // During streaming, skip renderMessages ‚Äî it rebuilds the DOM but tool card
    // insertion is blocked by !S.busy, losing Activity until "done" fires.
    if(!(S.busy||S.activeStreamId)){
      renderMessages({ preserveScroll:true });
    }else if(typeof _scheduleMessageVirtualizedRender==='function'){
      // ...but on a virtualized transcript SOMETHING still has to mount the new
      // render window. The scroll listener used to do it; it now correctly skips
      // while a programmatic scroll is in flight (the idle re-render-loop fix),
      // and this path deliberately does not call renderMessages() ‚Äî so without an
      // explicit schedule the jump lands on an all-spacer, zero-row transcript.
      // Force the window update here, after invalidating _messageVirtualWindowKey.
      _scheduleMessageVirtualizedRender(true);
    }
    requestAnimationFrame(()=>{
      container.scrollTop=0;
      _updateSessionStartJumpButton();
      _deferClearProgrammaticScroll();
    });
  }catch(e){
    console.warn('jumpToSessionStart failed:',e);
    _programmaticScroll=false;
  }
}

function _userMessageDomId(rawIdx){
  return `msg-user-${rawIdx}`;
}

function _questionJumpButtonHtml(questionRawIdx, assistantRawIdx){
  if(typeof questionRawIdx!=='number'||questionRawIdx<0) return '';
  const label=t('jump_to_question')||'Response';
  const title=t('jump_to_question_label')||'Jump to the start of this response';
  const aIdx=(typeof assistantRawIdx==='number'&&assistantRawIdx>=0)?assistantRawIdx:-1;
  return `<button class="msg-question-jump-btn session-jump-btn session-jump-btn--inline" type="button" title="${esc(title)}" aria-label="${esc(title)}" onclick="jumpToTurnQuestion(${questionRawIdx},${aIdx})"><span aria-hidden="true">‚Üë</span><span>${esc(label)}</span></button>`;
}

function _highlightQuestionRow(row){
  if(!row) return;
  row.classList.remove('msg-question-highlight');
  void row.offsetWidth;
  row.classList.add('msg-question-highlight');
  window.setTimeout(()=>row.classList.remove('msg-question-highlight'),1800);
}

async function jumpToTurnQuestion(questionRawIdx, assistantRawIdx){
  const container=$('messages');
  if(!container||typeof questionRawIdx!=='number'||questionRawIdx<0) return;
  const clampTargetScrollTop=(scrollTop)=>{
    const maxTop=Math.max(0,container.scrollHeight-container.clientHeight);
    const n=Number(scrollTop);
    return Math.max(0,Math.min(Number.isFinite(n)?n:container.scrollTop,maxTop));
  };
  const scrollToTarget=()=>{
    const hasAssistant=typeof assistantRawIdx==='number'&&assistantRawIdx>=0;
    if(hasAssistant){
      // A single assistant rawIdx can render multiple segment nodes ‚Äî some hidden
      // (assistant-segment-worklog-source / assistant-segment-anchor are display:none).
      // scrollIntoView() on a hidden node silently no-ops, so only treat a VISIBLE
      // segment (getClientRects().length>0) as a successful target; otherwise fall
      // through to the question-row fallback rather than suppressing it. (#3934)
      const segs=container.querySelectorAll('[data-msg-idx="'+assistantRawIdx+'"]');
      for(const seg of segs){
        if(seg.getClientRects().length>0){
          seg.scrollIntoView({block:'start',behavior:'smooth'});
          return true;
        }
      }
    }
    const row=document.getElementById(_userMessageDomId(questionRawIdx));
    if(!row) return false;
    row.scrollIntoView({block:'center',behavior:'smooth'});
    _highlightQuestionRow(row);
    return true;
  };
  // Cancel load-time bottom settling before any visible or virtualized target
  // path can return. The jump owner keeps every native smooth-scroll frame out
  // of the manual-reader listener, then reconciles against the final geometry.
  _cancelBottomSettle();
  _beginMessageJumpScroll(container);
  if(scrollToTarget()) return;
  const visWithIdx=_getVisibleMessagesWithIdx();
  const visibleIdx=_messageVisibleIndexForRawIdx(questionRawIdx, visWithIdx);
  if(visibleIdx>=0){
    _programmaticScroll=true;_programmaticScrollSetAt=performance.now();
    const virtualTarget=clampTargetScrollTop(_messageVirtualScrollTopForVisibleIdx(visWithIdx, visibleIdx, container));
    container.scrollTop=virtualTarget;
    _messageVirtualWindowKey='';
    renderMessages({ preserveScroll:true });
    requestAnimationFrame(()=>{
      if(!scrollToTarget()&&_messageHiddenBeforeCount()>0){
        _messageRenderWindowSize=Math.max(_currentMessageRenderWindowSize(),_messageRenderableMessageCount());
        _messageVirtualWindowKey='';
        renderMessages({ preserveScroll:true });
        requestAnimationFrame(scrollToTarget);
      }
      _deferClearProgrammaticScroll();
    });
    return;
  }
  if(_messageHiddenBeforeCount()>0){
    _messageRenderWindowSize=Math.max(_currentMessageRenderWindowSize(),_messageRenderableMessageCount());
    _messageVirtualWindowKey='';
    renderMessages({ preserveScroll:true });
    requestAnimationFrame(scrollToTarget);
  }
}

const DASHBOARD_STATUS_TTL_MS=60000;
let _dashboardStatusCache=null;
let _dashboardStatusFetchedAt=0;
let _dashboardLastNonNeverMode='auto'; // Server-scoped dashboard config keeps this restore target session-global on purpose.
let _dashboardSettingsLoadSeq=0;
let _dashboardSettingsWriteSeq=0;

function _dashboardHostIsLoopback(host){
  // Canonical loopback classifier shared by the browser origin and the
  // resolved dashboard target. Normalizes brackets, case, zone ids, and a
  // terminal hostname dot; classifies IPv4 127/8, IPv6 ::1, IPv4-mapped IPv6
  // whose embedded IPv4 is 127/8, and localhost/.localhost names (RFC 6761).
  if(!host) return false;
  let h=String(host).replace(/^\[|\]$/g,'').toLowerCase();
  if(h.endsWith('.')) h=h.slice(0,-1);
  if(h==='localhost'||h.endsWith('.localhost')) return true;
  const ipv4=/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(h);
  if(ipv4){
    const octets=ipv4.slice(1).map(Number);
    return octets.every(o=>o>=0&&o<=255)&&octets[0]===127;
  }
  if(h.includes(':')){
    const zone=h.indexOf('%');
    if(zone!==-1) h=h.slice(0,zone);
    if(h==='::1'||h==='0:0:0:0:0:0:0:1') return true;
    const mapped=/^(?:::ffff:|0:0:0:0:0:ffff:)(.+)$/.exec(h);
    if(mapped){
      const tail=mapped[1];
      const dotted=/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(tail);
      if(dotted){
        const octets=dotted.slice(1).map(Number);
        return octets.every(o=>o>=0&&o<=255)&&octets[0]===127;
      }
      const hex=/^([0-9a-f]{1,4}):([0-9a-f]{1,4})$/.exec(tail);
      if(hex) return (parseInt(hex[1],16)>>>8)===127;
      return false;
    }
    return false;
  }
  return false;
}

function _dashboardIsBrowserLoopback(){
  return _dashboardHostIsLoopback(window.location.hostname||'');
}

function _dashboardUrlIsLoopback(url){
  if(!url) return false;
  try{
    return _dashboardHostIsLoopback(new URL(url).hostname);
  }catch(_){return false;}
}

function _normalizeDashboardEnabledMode(mode){
  return mode==='auto'||mode==='always'||mode==='never'?mode:'auto';
}

function _setDashboardModeForChip(mode){
  mode=_normalizeDashboardEnabledMode(mode);
  if(mode==='auto'||mode==='always') _dashboardLastNonNeverMode=mode;
}

function _getDashboardChipRestoreMode(){
  return _dashboardLastNonNeverMode||'auto';
}

function _dashboardBrowserUrl(status){
  if(!status||!status.running) return '';
  if(status.browser_url||status.url){
    try{return new URL(status.browser_url||status.url).toString().replace(/\/$/,'');}
    catch(_){}
  }
  if(!status.port) return '';
  let source;
  try{source=new URL('http://127.0.0.1:'+status.port);}
  catch(_){return '';}
  const browserHost=window.location.hostname||source.hostname;
  const displayHost=browserHost.includes(':')&&!browserHost.startsWith('[')?'['+browserHost+']':browserHost;
  return source.protocol+'//'+displayHost+':'+status.port;
}
function _stripInlineEventHandlers(node){
  if(!node)return;
  const strip=el=>{
    Array.from(el.attributes||[]).forEach(attr=>{
      if(attr.name&&attr.name.toLowerCase().startsWith('on'))el.removeAttribute(attr.name);
    });
    if('onclick' in el)el.onclick=null;
    Array.from(el.children||[]).forEach(strip);
  };
  strip(node);
}
function _syncNavActionMirrors(){
  const rail=document.querySelector('.rail');
  const sidebar=document.querySelector('.sidebar-nav');
  if(!rail||!sidebar)return;
  const anchor=sidebar.querySelector('.dashboard-link,[data-dashboard-link]')||sidebar.querySelector('[data-panel="logs"]');
  const sources=Array.from(rail.querySelectorAll('.nav-tab:not([data-panel]):not([data-dashboard-link])')).filter(source=>source.id);
  const mirrors=Array.from(sidebar.querySelectorAll('[data-nav-action-mirror]'));
  const sourceIds=new Set(sources.map(source=>source.id));
  mirrors.forEach(mirror=>{
    if(!sourceIds.has(mirror.getAttribute('data-nav-action-mirror')))mirror.remove();
  });
  let next=anchor||null;
  sources.slice().reverse().forEach(source=>{
    const sourceVisible=(()=>{
      if(source.hidden||source.getAttribute('aria-hidden')==='true')return false;
      if(source.classList.contains('nav-tab-hidden'))return false;
      if(source.style&&(source.style.display==='none'||source.style.visibility==='hidden'))return false;
      if(typeof window!=='undefined'&&typeof window.getComputedStyle==='function'){
        const computed=window.getComputedStyle(source);
        if(computed&&(computed.display==='none'||computed.visibility==='hidden'))return false;
      }
      return true;
    })();
    let mirror=mirrors.find(el=>el.getAttribute('data-nav-action-mirror')===source.id);
    if(!mirror){
      mirror=source.cloneNode(true);
      _stripInlineEventHandlers(mirror);
      mirror.id=source.id+'Mobile';
      mirror.classList.remove('rail-btn');
      mirror.classList.add('has-tooltip--bottom');
      mirror.setAttribute('data-nav-action-mirror',source.id);
      mirror.addEventListener('click',e=>{
        e.preventDefault();
        if(mirror._navActionSource)mirror._navActionSource.click();
        if(typeof closeMobileSidebar==='function')closeMobileSidebar();
      });
    }else{
      mirror.innerHTML=source.innerHTML;
      _stripInlineEventHandlers(mirror);
    }
    if(mirror.parentNode!==sidebar||mirror.nextElementSibling!==next)sidebar.insertBefore(mirror,next);
    next=mirror;
    mirror._navActionSource=source;
    mirror.classList.toggle('nav-action-visible',sourceVisible);
    const label=source.getAttribute('data-tooltip')||source.getAttribute('aria-label')||'';
    if(label)mirror.setAttribute('data-label',label);
  });
}
function _initNavActionMirrors(){
  _syncNavActionMirrors();
  const rail=document.querySelector('.rail');
  if(rail&&window.MutationObserver)new MutationObserver(_syncNavActionMirrors).observe(rail,{
    childList:true,
    subtree:true,
    attributes:true,
    attributeFilter:['class','style','hidden','aria-hidden','data-tooltip','aria-label'],
  });
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',_initNavActionMirrors,{once:true});
else _initNavActionMirrors();
function _applyDashboardStatus(status){
  const running=!!(status&&status.running);
  const url=running?_dashboardBrowserUrl(status):'';
  const warning=running&&!_dashboardIsBrowserLoopback()&&_dashboardUrlIsLoopback(url)?t('dashboard_loopback_warning'):'';
  document.querySelectorAll('[data-dashboard-link]').forEach(btn=>{
    btn.classList.toggle('dashboard-link-visible',running);
    btn.classList.toggle('nav-action-visible',running);
    btn.style.display=running?'':'none';
    btn.dataset.dashboardUrl=url;
    const tipText=warning||t('tab_dashboard');
    if(btn.hasAttribute('data-tooltip')){
      // Sync the custom CSS tooltip and explicitly clear the native title so
      // the slow ~1.5s native browser tooltip does not co-fire alongside the
      // fast custom tooltip (#1775).
      btn.setAttribute('data-tooltip',tipText);
      if(btn.hasAttribute('title')) btn.removeAttribute('title');
    } else {
      btn.title=tipText;
    }
    btn.setAttribute('aria-label',tipText);
  });
}
async function refreshDashboardStatus(force=false){
  const now=Date.now();
  // Skip the interval-driven poll while the tab is hidden: the 60s interval
  // equals the cache TTL, so every background tick was a real /api/dashboard/status
  // fetch that never hit the cache ‚Äî a needless wakeup on a tab nobody is
  // looking at (battery/CPU, #2476). Forced calls (settings save, init, the
  // visibilitychange catch-up) still run. A visible tab keeps its live status.
  if(!force&&typeof document!=='undefined'&&document.hidden){
    return _dashboardStatusCache;
  }
  if(!force&&_dashboardStatusCache&&(now-_dashboardStatusFetchedAt)<DASHBOARD_STATUS_TTL_MS){
    _applyDashboardStatus(_dashboardStatusCache);
    return _dashboardStatusCache;
  }
  try{
    const status=await api('/api/dashboard/status',{timeoutToast:false});
    _dashboardStatusCache=status||{running:false};
  }catch(_){
    _dashboardStatusCache={running:false};
  }
  _dashboardStatusFetchedAt=Date.now();
  _applyDashboardStatus(_dashboardStatusCache);
  return _dashboardStatusCache;
}
async function loadDashboardSettings(){
  const modeEl=$('settingsDashboardMode');
  const urlEl=$('settingsDashboardUrl');
  if(!modeEl&&!urlEl) return;
  const loadSeq=++_dashboardSettingsLoadSeq;
  const writeSeq=_dashboardSettingsWriteSeq;
  try{
    const cfg=await api('/api/dashboard/config');
    if(loadSeq!==_dashboardSettingsLoadSeq||writeSeq!==_dashboardSettingsWriteSeq) return;
    const mode=_normalizeDashboardEnabledMode(cfg&&cfg.enabled);
    if(modeEl) modeEl.value=mode;
    _setDashboardModeForChip(mode);
    if(urlEl) urlEl.value=cfg.url||'';
    if(typeof _renderTabVisibilityChips==='function') _renderTabVisibilityChips();
  }catch(_){/* leave defaults visible */}
}
async function saveDashboardSettings(opts){
  opts=opts||{};
  const modeEl=$('settingsDashboardMode');
  const urlEl=$('settingsDashboardUrl');
  const statusEl=$('settingsDashboardStatus');
  const payload={enabled:(modeEl&&modeEl.value)||'auto',url:(urlEl&&urlEl.value||'').trim()};
  _dashboardSettingsWriteSeq+=1;
  try{
    const saved=await api('/api/dashboard/config',{method:'POST',body:JSON.stringify(payload)});
    const mode=_normalizeDashboardEnabledMode(saved&&saved.enabled);
    if(modeEl) modeEl.value=mode;
    _setDashboardModeForChip(mode);
    if(urlEl) urlEl.value=saved.url||'';
    if(statusEl) statusEl.textContent='Dashboard link settings saved.';
    await refreshDashboardStatus(true);
    if(typeof _renderTabVisibilityChips==='function') _renderTabVisibilityChips();
  }catch(err){
    if(statusEl) statusEl.textContent='Dashboard link settings failed to save.';
    else if(typeof showToast==='function') showToast('Dashboard link settings failed to save.');
    try{await loadDashboardSettings();}catch(_){}
    if(opts.raiseOnError) throw err;
  }
}
function openHermesDashboard(event){
  if(event){event.preventDefault();event.stopPropagation();}
  const btn=event&&event.currentTarget?event.currentTarget:document.querySelector('[data-dashboard-link]');
  const url=(btn&&btn.dataset&&btn.dataset.dashboardUrl)||_dashboardBrowserUrl(_dashboardStatusCache);
  if(!url) return false;
  window.open(url,'_blank','noopener,noreferrer');
  return false;
}
function _initDashboardLinkProbe(){
  loadDashboardSettings();
  refreshDashboardStatus(true);
  setInterval(refreshDashboardStatus,DASHBOARD_STATUS_TTL_MS);
  // Catch up once when the tab becomes visible again, since the interval poll
  // was skipped while hidden and its cache is now stale.
  if(typeof document!=='undefined'&&typeof document.addEventListener==='function'){
    document.addEventListener('visibilitychange',()=>{
      if(!document.hidden) refreshDashboardStatus(true);
    });
  }
}
if(document.readyState==='complete'){
  _initDashboardLinkProbe();
}else{
  document.addEventListener('DOMContentLoaded',_initDashboardLinkProbe,{once:true});
}

/* ‚îÄ‚îÄ Image lightbox ‚Äî click any .msg-media-img to enlarge ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ */
function _openImgLightbox(imgEl) {
  if(!imgEl || !imgEl.src) return;
  const src=imgEl.src, alt=imgEl.alt||'';
  // Find sibling images in the same message for prev/next navigation.
  // Walk up from the clicked image to find the message container, then
  // collect all .msg-media-img within it.
  // Composer attach-tray chips bypass sibling detection ‚Äî each chip click
  // opens a single-image lightbox (no navigation between staged uploads).
  let allImages = [];
  let startIndex = 0;
  if(!imgEl.closest('.attach-tray')){
    let container = imgEl.closest('.msg-row, .assistant-turn-blocks, .assistant-turn, .user-turn');
    if(!container) container = imgEl.parentElement;
    if(container){
      const siblings = container.querySelectorAll('.msg-media-img');
      if(siblings.length>1){
        allImages = Array.from(siblings);
        startIndex = allImages.indexOf(imgEl);
        if(startIndex===-1) startIndex=0;
      }
    }
  }
  _openImgLightboxWithNav(src, alt, allImages, startIndex);
}

const _MERMAID_VIEWER_MIN_SCALE = 0.25;
const _MERMAID_VIEWER_MAX_SCALE = 8;
const _MERMAID_VIEWER_ZOOM_STEP = 1.2;
const _MERMAID_VIEWER_INLINE_MIN_HEIGHT = 220;

function _mermaidViewerIcon(kind) {
  const icons = {
    zoomIn: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="10" cy="10" r="6"></circle><path d="M10 7v6M7 10h6"></path><path d="M15 15l4 4"></path></svg>',
    zoomOut: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="10" cy="10" r="6"></circle><path d="M7 10h6"></path><path d="M15 15l4 4"></path></svg>',
    reset: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 9V4H1"></path><path d="M1 4l4 4"></path><path d="M10 4a8 8 0 1 1-5.66 13.66"></path></svg>',
    fit: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"></path></svg>',
    fullscreen: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"></path><path d="M4 4l5 5M20 4l-5 5M4 20l5-5M20 20l-5-5"></path></svg>',
  };
  return icons[kind] || '';
}

function _createMermaidViewerButton(label, iconKind, onClick) {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'mermaid-viewer-btn';
  btn.setAttribute('aria-label', label);
  btn.setAttribute('title', label);
  btn.innerHTML = _mermaidViewerIcon(iconKind);
  btn.onclick = e => {
    e.preventDefault();
    e.stopPropagation();
    onClick(e);
  };
  return btn;
}

function _mermaidSvgBox(svgEl) {
  const box = {x: 0, y: 0, width: 0, height: 0};
  if(!svgEl) return box;
  const viewBox = svgEl.viewBox && svgEl.viewBox.baseVal;
  if(viewBox && viewBox.width && viewBox.height){
    box.x = Number(viewBox.x) || 0;
    box.y = Number(viewBox.y) || 0;
    box.width = Number(viewBox.width) || 0;
    box.height = Number(viewBox.height) || 0;
    return box;
  }
  const rawViewBox = svgEl.getAttribute && svgEl.getAttribute('viewBox');
  if(rawViewBox){
    const parts = rawViewBox.trim().split(/[,\s]+/).map(Number);
    if(parts.length >= 4 && parts.every(n => Number.isFinite(n))){
      box.x = parts[0] || 0;
      box.y = parts[1] || 0;
      box.width = parts[2] || 0;
      box.height = parts[3] || 0;
      return box;
    }
  }
  const width = Number.parseFloat(svgEl.getAttribute && svgEl.getAttribute('width')) || (svgEl.getBoundingClientRect ? svgEl.getBoundingClientRect().width : 0) || 0;
  const height = Number.parseFloat(svgEl.getAttribute && svgEl.getAttribute('height')) || (svgEl.getBoundingClientRect ? svgEl.getBoundingClientRect().height : 0) || 0;
  box.width = width || 800;
  box.height = height || 450;
  return box;
}

function _mountMermaidViewer(svgEl, options = {}) {
  if(!svgEl) return null;
  const mode = options.mode === 'lightbox' ? 'lightbox' : 'inline';
  const openLightbox = typeof options.openLightbox === 'function' ? options.openLightbox : () => _openMermaidLightbox(svgEl);
  const box = _mermaidSvgBox(svgEl);
  const host = svgEl.parentNode;
  const root = document.createElement('div');
  root.className = 'mermaid-viewer mermaid-viewer--' + mode;
  const toolbar = document.createElement('div');
  toolbar.className = 'mermaid-viewer-toolbar';
  const viewport = document.createElement('div');
  viewport.className = 'mermaid-viewer-viewport';
  const canvas = document.createElement('div');
  canvas.className = 'mermaid-viewer-canvas';
  canvas.style.width = Math.max(1, Math.round(box.width)) + 'px';
  canvas.style.height = Math.max(1, Math.round(box.height)) + 'px';
  svgEl.classList.add('mermaid-viewer-svg');
  if(mode === 'lightbox') svgEl.classList.add('mermaid-lightbox-svg');
  svgEl.style.width = '100%';
  svgEl.style.height = '100%';
  svgEl.style.display = 'block';
  viewport.appendChild(canvas);
  root.appendChild(toolbar);
  root.appendChild(viewport);
  if(host) host.replaceChild(root, svgEl);
  canvas.appendChild(svgEl);

  const state = {
    box,
    canvas,
    dragging: false,
    dragOriginX: 0,
    dragOriginY: 0,
    dragPointerId: null,
    dragStartX: 0,
    dragStartY: 0,
    dragged: false,
    mode,
    root,
    toolbar,
    scale: 1,
    svg: svgEl,
    viewport,
    x: 0,
    y: 0,
    pinching: false,
    pinchStartDist: 0,
    pinchStartScale: 1,
    pinchStartCX: 0,
    pinchStartCY: 0,
    pinchStartX: 0,
    pinchStartY: 0,
  };
  root._mermaidViewer = state;

  function _lightboxViewportEnvelope() {
    const width = Math.round((window.innerWidth || box.width) * 0.9);
    const height = Math.round((window.innerHeight || box.height) * 0.9);
    return {
      width: Math.max(1, Number.isFinite(width) ? width : 1),
      height: Math.max(1, Number.isFinite(height) ? height : 1),
    };
  }

  function _viewportFallbackSize(){
    if(mode === 'lightbox') return _lightboxViewportEnvelope();
    const width = Math.round(window.innerWidth || box.width);
    const height = Math.round((window.innerHeight || box.height) * 0.7);
    return {
      width: Math.max(1, Number.isFinite(width) ? width : 1),
      height: Math.max(1, Number.isFinite(height) ? height : 1),
    };
  }

  function _viewportSize(){
    const rect = viewport.getBoundingClientRect ? viewport.getBoundingClientRect() : null;
    const fallback = _viewportFallbackSize();
    const width = mode === 'lightbox'
      ? fallback.width
      : (viewport.clientWidth || (rect && rect.width) || fallback.width);
    const height = mode === 'lightbox'
      ? fallback.height
      : (viewport.clientHeight || (rect && rect.height) || fallback.height);
    return {
      width: Math.max(1, Number(width) || box.width || 1),
      height: Math.max(1, Number(height) || box.height || 1),
    };
  }

  function _rawFitScale(size){
    return Math.min(size.width / Math.max(1, box.width), size.height / Math.max(1, box.height));
  }

  function _minScale(){
    // Inline stays bounded by readable-height minimum to preserve usability.
    // Lightbox allows fit-to-screen to shrink below the old 0.25 floor when
    // the diagram envelope is narrower than 25%.
    if(mode === 'lightbox') return Math.min(_MERMAID_VIEWER_MIN_SCALE, _rawFitScale(_viewportSize()));
    return Math.min(_MERMAID_VIEWER_MIN_SCALE, _inlineViewportHeight() / Math.max(1, box.height));
  }

  function _inlineViewportHeight(){
    const size = _viewportSize();
    const widthFitScale = size.width / Math.max(1, box.width);
    const widthBasedHeight = Math.max(1, Math.round(box.height * widthFitScale));
    const fallback = _viewportFallbackSize();
    return Math.min(fallback.height, Math.max(_MERMAID_VIEWER_INLINE_MIN_HEIGHT, widthBasedHeight));
  }

  function _applyTransform(){
    canvas.style.transform = `translate(${Math.round(state.x)}px, ${Math.round(state.y)}px) scale(${state.scale})`;
    canvas.style.transformOrigin = '0 0';
  }

  function _centerForScale(nextScale){
    const size = _viewportSize();
    const scaledWidth = box.width * nextScale;
    const scaledHeight = box.height * nextScale;
    state.x = scaledWidth < size.width ? Math.round((size.width - scaledWidth) / 2) : 0;
    state.y = scaledHeight < size.height ? Math.round((size.height - scaledHeight) / 2) : 0;
  }

  function _fitScale(){
    const size = _viewportSize();
    return Math.max(_minScale(), Math.min(_MERMAID_VIEWER_MAX_SCALE, _rawFitScale(size)));
  }

  function _setScale(nextScale, anchorX, anchorY){
    const bounded = Math.max(_minScale(), Math.min(_MERMAID_VIEWER_MAX_SCALE, nextScale));
    if(!Number.isFinite(bounded) || !box.width || !box.height) return;
    const focusX = Number.isFinite(anchorX) ? anchorX : _viewportSize().width / 2;
    const focusY = Number.isFinite(anchorY) ? anchorY : _viewportSize().height / 2;
    if(state.scale){
      const ratio = bounded / state.scale;
      state.x = focusX - (focusX - state.x) * ratio;
      state.y = focusY - (focusY - state.y) * ratio;
    }
    state.scale = bounded;
    _applyTransform();
  }

  function _fitViewer(){
    const nextScale = _fitScale();
    state.fitScale = nextScale;
    state.scale = nextScale;
    _centerForScale(nextScale);
    _applyTransform();
  }

  function _resizeToEnvelope(){
    if(mode !== 'lightbox') return;
    const hadFitScale = Number.isFinite(state.fitScale);
    const previousFitScale = hadFitScale ? state.fitScale : _fitScale();
    const wasAtFit = !hadFitScale || Math.abs(state.scale - previousFitScale) < 1e-9;
    const envelope = _lightboxViewportEnvelope();
    viewport.style.width = Math.max(1, Math.round(envelope.width)) + 'px';
    viewport.style.height = Math.max(1, Math.round(envelope.height)) + 'px';
    const nextFitScale = _fitScale();
    state.fitScale = nextFitScale;
    if(wasAtFit){
      state.scale = nextFitScale;
      _centerForScale(state.scale);
    } else {
      state.scale = Math.max(_minScale(), Math.min(_MERMAID_VIEWER_MAX_SCALE, state.scale));
    }
    _applyTransform();
  }

  function _resetViewer(){
    state.scale = 1;
    _centerForScale(1);
    _applyTransform();
  }

  function _zoomIn(){
    const size = _viewportSize();
    _setScale(state.scale * _MERMAID_VIEWER_ZOOM_STEP, size.width / 2, size.height / 2);
  }

  function _zoomOut(){
    const size = _viewportSize();
    _setScale(state.scale / _MERMAID_VIEWER_ZOOM_STEP, size.width / 2, size.height / 2);
  }

  function _zoomFromWheel(e){
    if(e.preventDefault) e.preventDefault();
    const rect = viewport.getBoundingClientRect ? viewport.getBoundingClientRect() : {left: 0, top: 0};
    const anchorX = Number.isFinite(e.clientX) ? e.clientX - rect.left : undefined;
    const anchorY = Number.isFinite(e.clientY) ? e.clientY - rect.top : undefined;
    const deltaMode = Number(e.deltaMode) || 0;
    const lineScale = deltaMode === 1 ? 30 : deltaMode === 2 ? 600 : 1;
    const factor = Math.exp((-(Number(e.deltaY) || 0)) * lineScale * 0.0015);
    _setScale(state.scale * factor, anchorX, anchorY);
  }

  function _onPointerDown(e){
    if(state.pinching) return;
    if(e.button != null && e.button !== 0) return;
    state.dragging = true;
    state.dragged = false;
    state.dragOriginX = Number(e.clientX) || 0;
    state.dragOriginY = Number(e.clientY) || 0;
    state.dragPointerId = e.pointerId != null ? e.pointerId : null;
    state.dragStartX = state.x;
    state.dragStartY = state.y;
    viewport.classList.add('is-panning');
    if(state.dragPointerId != null && viewport.setPointerCapture) viewport.setPointerCapture(state.dragPointerId);
    if(e.preventDefault) e.preventDefault();
  }

  function _onPointerMove(e){
    if(state.pinching) return;
    if(!state.dragging) return;
    const dx = (Number(e.clientX) || 0) - state.dragOriginX;
    const dy = (Number(e.clientY) || 0) - state.dragOriginY;
    if(Math.abs(dx) + Math.abs(dy) > 3) state.dragged = true;
    state.x = state.dragStartX + dx;
    state.y = state.dragStartY + dy;
    _applyTransform();
  }

  function _endPointerDrag(){
    if(!state.dragging) return;
    state.dragging = false;
    if(state.dragPointerId != null && viewport.releasePointerCapture){
      try{ viewport.releasePointerCapture(state.dragPointerId); }catch(_){}
    }
    state.dragPointerId = null;
    viewport.classList.remove('is-panning');
  }

  function _openViewerOnClick(e){
    if(state.pinching) return;
    if(mode !== 'inline') return;
    if(state.dragged){
      state.dragged = false;
      return;
    }
    if(e.preventDefault) e.preventDefault();
    if(e.stopPropagation) e.stopPropagation();
    openLightbox();
  }

  function _touchDist(touches){
    if(!touches || touches.length < 2) return 0;
    const dx = touches[0].clientX - touches[1].clientX;
    const dy = touches[0].clientY - touches[1].clientY;
    return Math.sqrt(dx * dx + dy * dy);
  }

  function _onTouchStart(e){
    if(e.touches.length === 2){
      state.pinching = true;
      state.pinchStartDist = _touchDist(e.touches);
      state.pinchStartScale = state.scale;
      state.pinchStartX = state.x;
      state.pinchStartY = state.y;
      const rect = viewport.getBoundingClientRect();
      state.pinchStartCX = (e.touches[0].clientX + e.touches[1].clientX) / 2 - (rect.left || 0);
      state.pinchStartCY = (e.touches[0].clientY + e.touches[1].clientY) / 2 - (rect.top || 0);
      _endPointerDrag();
      if(e.preventDefault) e.preventDefault();
    }
  }

  function _onTouchMove(e){
    if(!state.pinching || e.touches.length < 2) return;
    const rect = viewport.getBoundingClientRect();
    const cx = (e.touches[0].clientX + e.touches[1].clientX) / 2 - (rect.left || 0);
    const cy = (e.touches[0].clientY + e.touches[1].clientY) / 2 - (rect.top || 0);
    const currDist = _touchDist(e.touches);
    if(state.pinchStartDist > 0 && state.pinchStartScale > 0){
      const rawScale = state.pinchStartScale * (currDist / state.pinchStartDist);
      const boundedScale = Math.max(_minScale(), Math.min(_MERMAID_VIEWER_MAX_SCALE, rawScale));
      const ratio = boundedScale / state.pinchStartScale;
      state.scale = boundedScale;
      state.x = cx - (state.pinchStartCX - state.pinchStartX) * ratio;
      state.y = cy - (state.pinchStartCY - state.pinchStartY) * ratio;
      _applyTransform();
    }
    if(e.preventDefault) e.preventDefault();
  }

  function _onTouchEnd(e){
    if(e.touches.length < 2 && state.pinching){
      state.pinching = false;
      state.dragged = true;
    }
  }

  viewport.onpointerdown = _onPointerDown;
  viewport.onpointermove = _onPointerMove;
  viewport.onpointerup = _endPointerDrag;
  viewport.onpointercancel = _endPointerDrag;
  viewport.onpointerleave = _endPointerDrag;
  viewport.onwheel = _zoomFromWheel;
  viewport.onclick = _openViewerOnClick;
  viewport.addEventListener('touchstart', _onTouchStart, {passive: false});
  viewport.addEventListener('touchmove', _onTouchMove, {passive: false});
  viewport.addEventListener('touchend', _onTouchEnd);
  viewport.addEventListener('touchcancel', function _onTouchCancel(){ state.pinching = false; });
  root.onclick = e => e.stopPropagation();
  state.fit = _fitViewer;
  state.reset = _resetViewer;
  state.zoomIn = _zoomIn;
  state.zoomOut = _zoomOut;
  state.zoomAt = _setScale;
  state.applyTransform = _applyTransform;
  state.resizeToEnvelope = _resizeToEnvelope;
  state.openLightbox = openLightbox;

  toolbar.appendChild(_createMermaidViewerButton('Zoom in', 'zoomIn', _zoomIn));
  toolbar.appendChild(_createMermaidViewerButton('Zoom out', 'zoomOut', _zoomOut));
  toolbar.appendChild(_createMermaidViewerButton('Reset view', 'reset', _resetViewer));
  toolbar.appendChild(_createMermaidViewerButton('Fit to screen', 'fit', _fitViewer));
  if(mode === 'inline'){
    toolbar.appendChild(_createMermaidViewerButton('Fullscreen', 'fullscreen', openLightbox));
  }

  if(mode === 'lightbox'){
    state.resizeToEnvelope();
  } else {
    const initialHeight = _inlineViewportHeight();
    const readableScale = initialHeight / Math.max(1, box.height);
    state.scale = Math.max(_minScale(), Math.min(_MERMAID_VIEWER_MAX_SCALE, readableScale));
    viewport.style.width = '100%';
    viewport.style.height = Math.max(1, Math.round(initialHeight)) + 'px';
    _centerForScale(state.scale);
    _applyTransform();
  }

  return root;
}

function _openMermaidLightbox(svgEl) {
  if(!svgEl) return;
  const lb = document.createElement('div');
  lb.className = 'img-lightbox';
  lb.setAttribute('role', 'dialog');
  lb.setAttribute('aria-modal', 'true');
  lb.setAttribute('aria-label', 'Mermaid diagram');
  const clone = svgEl.cloneNode(true);
  const idMap = new Map();
  const idPrefix = 'mermaid-lightbox-'+Math.random().toString(36).slice(2,10)+'-';
  const idNodes = [clone, ...clone.querySelectorAll('[id]')].filter(el => el.id);
  idNodes.forEach(el => {
    const nextId = idPrefix + el.id;
    idMap.set(el.id, nextId);
    el.id = nextId;
  });
  if(idMap.size){
    const refAttrs = ['href','xlink:href','fill','stroke','filter','clip-path','mask','marker-start','marker-mid','marker-end','aria-labelledby','aria-describedby'];
    [clone, ...clone.querySelectorAll('*')].forEach(el => {
      refAttrs.forEach(attr => {
        const value = el.getAttribute(attr);
        if(!value) return;
        let nextValue = value.replace(/url\(#([^)]+)\)/g, (match, refId) => idMap.has(refId) ? `url(#${idMap.get(refId)})` : match);
        if(nextValue.startsWith('#') && idMap.has(nextValue.slice(1))){
          nextValue = '#'+idMap.get(nextValue.slice(1));
        }
        if(nextValue !== value){
          el.setAttribute(attr, nextValue);
        }
      });
    });
    clone.querySelectorAll('style').forEach(styleEl => {
      let styleText = styleEl.textContent || '';
      idMap.forEach((nextId, originalId) => {
        const escapedId = originalId.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        styleText = styleText.replace(new RegExp(`url\\(#${escapedId}\\)`, 'g'), `url(#${nextId})`);
        styleText = styleText.replace(new RegExp(`(^|[^\\w-])#${escapedId}(?=$|[^\\w-])`, 'g'), (match, prefix) => `${prefix}#${nextId}`);
      });
      styleEl.textContent = styleText;
    });
  }
  clone.removeAttribute('width');
  clone.removeAttribute('height');
  const viewer = _mountMermaidViewer(clone, {mode:'lightbox'});
  if(viewer && viewer._mermaidViewer && typeof viewer._mermaidViewer.resizeToEnvelope === 'function'){
    lb._mermaidResizeHandler = () => {
      if(lb._mermaidResizeTimer && typeof clearTimeout === 'function') clearTimeout(lb._mermaidResizeTimer);
      lb._mermaidResizeTimer = setTimeout(() => {
        lb._mermaidResizeTimer = null;
        viewer._mermaidViewer.resizeToEnvelope();
      }, 120);
    };
    if(window && typeof window.addEventListener === 'function'){
      window.addEventListener('resize', lb._mermaidResizeHandler);
    }
  }
  const cls = document.createElement('button');
  cls.className = 'img-lightbox-close';
  cls.setAttribute('aria-label', 'Close');
  cls.textContent = '√ó';
  cls.onclick = () => _closeImgLightbox(lb);
  lb.appendChild(viewer);
  lb.appendChild(cls);
  lb.onclick = () => _closeImgLightbox(lb);
  lb._keyHandler = e => {
    if(e.key==='Escape') _closeImgLightbox(lb);
  };
  document.body.appendChild(lb);
  document.addEventListener('keydown', lb._keyHandler);
  return lb;
}
function _openImgLightboxWithNav(src, alt, images, index) {
  const lb = document.createElement('div');
  lb.className = 'img-lightbox';
  lb.setAttribute('role', 'dialog');
  lb.setAttribute('aria-modal', 'true');
  lb.setAttribute('aria-label', alt || 'Image');
  const img = document.createElement('img');
  img.src = src;
  img.alt = alt || '';
  img.onclick = e => e.stopPropagation();
  const cls = document.createElement('button');
  cls.className = 'img-lightbox-close';
  cls.setAttribute('aria-label', 'Close');
  cls.textContent = '√ó';
  cls.onclick = () => _closeImgLightbox(lb);
  lb.appendChild(img);
  lb.appendChild(cls);
  // Prev/Next navigation ‚Äî store index and images on lb so a single set of
  // handlers reads live values without closure churn on every nav.
  lb._navIndex = index;
  lb._navImages = (images && images.length>1) ? images : null;
  if(lb._navImages){
    const prevBtn = document.createElement('button');
    prevBtn.className = 'img-lightbox-nav img-lightbox-nav-prev';
    prevBtn.setAttribute('aria-label', 'Previous image');
    prevBtn.innerHTML = '‚Äπ';
    prevBtn.onclick = e => { e.stopPropagation(); _navigateLightbox(lb, -1); };
    lb.appendChild(prevBtn);
    const nextBtn = document.createElement('button');
    nextBtn.className = 'img-lightbox-nav img-lightbox-nav-next';
    nextBtn.setAttribute('aria-label', 'Next image');
    nextBtn.innerHTML = '‚Ä∫';
    nextBtn.onclick = e => { e.stopPropagation(); _navigateLightbox(lb, 1); };
    lb.appendChild(nextBtn);
    lb._counterEl = document.createElement('div');
    lb._counterEl.className = 'img-lightbox-counter';
    lb.appendChild(lb._counterEl);
    lb._counterEl.textContent = (index+1) + ' / ' + images.length;
  }
  lb.onclick = () => _closeImgLightbox(lb);
  document.body.appendChild(lb);
  // Single keyboard handler ‚Äî reads lb._navX live, no remove/add churn.
  lb._keyHandler = e => {
    if(e.key==='Escape'){ _closeImgLightbox(lb); return; }
    if(lb._navImages){
      if(e.key==='ArrowLeft'){ e.preventDefault(); _navigateLightbox(lb, -1); }
      if(e.key==='ArrowRight'){ e.preventDefault(); _navigateLightbox(lb, 1); }
    }
  };
  document.addEventListener('keydown', lb._keyHandler);
}
function _navigateLightbox(lb, direction) {
  const images = lb._navImages;
  if(!images) return;
  const newIndex = lb._navIndex + direction;
  if(newIndex<0 || newIndex>=images.length) return;
  lb._navIndex = newIndex;
  const nextImg = images[newIndex];
  const lbImg = lb.querySelector('img');
  if(!lbImg) return;
  lbImg.src = nextImg.src;
  lbImg.alt = nextImg.alt || '';
  lb.setAttribute('aria-label', nextImg.alt || 'Image');
  // Update counter via stored reference ‚Äî no DOM query.
  if(lb._counterEl) lb._counterEl.textContent = (newIndex+1) + ' / ' + images.length;
}
function _closeImgLightbox(lb) {
  if(!lb || !lb.parentNode) return;
  document.removeEventListener('keydown', lb._keyHandler);
  if(lb._mermaidResizeHandler && window && typeof window.removeEventListener === 'function'){
    window.removeEventListener('resize', lb._mermaidResizeHandler);
  }
  if(lb._mermaidResizeTimer && typeof clearTimeout === 'function'){
    clearTimeout(lb._mermaidResizeTimer);
    lb._mermaidResizeTimer = null;
  }
  lb.style.animation = 'lb-in .12s ease reverse';
  setTimeout(() => lb.parentNode && lb.parentNode.removeChild(lb), 120);
}

document.addEventListener('click', e => {
  if(!e.target || !e.target.closest) return;
  const sessionLink=e.target.closest('a.session-link[href]');
  if(sessionLink){
    const href=sessionLink.getAttribute('href')||'';
    const m=href.match(/(?:^|\/)session\/([^?#]+)/i);
    if(m&&typeof loadSession==='function'){
      e.preventDefault();
      try{loadSession(decodeURIComponent(m[1]));}catch(_){loadSession(m[1]);}
    }
    return;
  }
  const workspaceLink=e.target.closest('a[href^="#workspace="]');
  if(workspaceLink){
    e.preventDefault();
    const href=workspaceLink.getAttribute('href')||'';
    try{
      const rel=decodeURIComponent(href.slice('#workspace='.length));
      if(rel && typeof openArtifactPath==='function') openArtifactPath(rel);
    }catch(_){}
    return;
  }
  // Message-attached images (already wired since v0.50.x).
  let img = e.target.closest('.msg-media-img');
  if(img){ _openImgLightbox(img); return; }
  const mermaidSvg = e.target.closest('.mermaid-rendered svg');
  if(mermaidSvg){ _openMermaidLightbox(mermaidSvg); return; }
  // Composer attach-tray image thumbnails ‚Äî click any pasted/dropped image
  // chip to lightbox-zoom it before sending. Excludes audio/video chips,
  // which keep their inline media controls. SVG thumbnails (.attach-thumb--svg)
  // are still images visually, so they qualify.
  img = e.target.closest('.attach-thumb');
  if(img && img.tagName === 'IMG'){
    _openImgLightbox(img);
    return;
  }
});

const _IMAGE_EXTS=/\.(png|jpg|jpeg|gif|webp|bmp|ico|avif)$/i;
const _PDF_EXTS=/\.pdf$/i;
const _HTML_EXTS=/\.(html?|htm)$/i;
const _ARCHIVE_EXTS=/\.(zip|tar|tar\.gz|tgz|tar\.bz2|tbz2|tar\.xz|txz)$/i;
const _SVG_EXTS=/\.svg$/i;
const _AUDIO_EXTS=/\.(mp3|ogg|wav|m4a|aac|flac|wma|opus|webm|oga)$/i;
const _VIDEO_EXTS=/\.(mp4|webm|mkv|mov|avi|ogv|m4v)$/i;
const _CSV_EXTS=/\.csv$/i;
const _EXCALIDRAW_EXTS=/\.excalidraw$/i;
// ‚îÄ‚îÄ Media playback speed controls ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
const MEDIA_PLAYBACK_RATES=[0.5,0.75,1,1.25,1.5,2];
const MEDIA_PLAYBACK_STORAGE_KEY='hermes-media-playback-rate';
function _getStoredMediaPlaybackRate(){
  try{
    const raw=localStorage.getItem(MEDIA_PLAYBACK_STORAGE_KEY);
    const rate=Number(raw);
    return MEDIA_PLAYBACK_RATES.includes(rate)?rate:1;
  }catch(_){return 1;}
}
function _setStoredMediaPlaybackRate(rate){
  if(!MEDIA_PLAYBACK_RATES.includes(rate)) return;
  try{localStorage.setItem(MEDIA_PLAYBACK_STORAGE_KEY,String(rate));}catch(_){}
}
function _syncMediaSpeedButtons(editor, rate){
  if(!editor) return;
  editor.querySelectorAll('.media-speed-btn').forEach(b=>{
    const active=Number(b.dataset.rate)===rate;
    b.classList.toggle('active',active);
    b.setAttribute('aria-pressed',active?'true':'false');
  });
}
function _applyMediaPlaybackRate(media, rate=_getStoredMediaPlaybackRate()){
  if(!media) return;
  media.playbackRate=rate;
  _syncMediaSpeedButtons(media.closest('.msg-media-editor,.preview-media-wrap'),rate);
}
let _mediaVisibilityObserver=null;
function _promoteVisibleVideoPreload(video){
  if(!video||!video.matches||!video.matches('.msg-media-video')) return;
  if(video.isConnected===false) return;
  if(video.dataset&&video.dataset.visiblePreload==='1') return;
  if(video.dataset) video.dataset.visiblePreload='1';
  video.preload='auto';
  // Off-screen history stays metadata-only. Once a card approaches the
  // viewport, restart just that resource so Chromium fills a playable buffer.
  if(video.paused&&video.readyState<4&&typeof video.load==='function') video.load();
}
function _observeVideoPreload(video){
  if(!video||!video.matches||!video.matches('.msg-media-video')) return;
  if(video.dataset&&video.dataset.visiblePreload==='1') return;
  if(_mediaVisibilityObserver) _mediaVisibilityObserver.observe(video);
}
function _unobserveVideoPreload(video){
  if(!video||!_mediaVisibilityObserver) return;
  _mediaVisibilityObserver.unobserve(video);
}
function _initMediaVisibilityObserver(){
  if(_mediaVisibilityObserver||typeof IntersectionObserver==='undefined') return;
  _mediaVisibilityObserver=new IntersectionObserver(entries=>{
    for(const entry of entries){
      if(!entry.isIntersecting) continue;
      const video=entry.target;
      _unobserveVideoPreload(video);
      _promoteVisibleVideoPreload(video);
    }
  },{root:null,rootMargin:'300px 0px',threshold:0.01});
}
function _mediaKindForName(name=''){
  const clean=String(name||'').split('?')[0].toLowerCase();
  if(_VIDEO_EXTS.test(clean)) return 'video';
  if(_AUDIO_EXTS.test(clean)) return 'audio';
  if(_IMAGE_EXTS.test(clean)) return 'image';
  return '';
}
function _mediaSpeedControlsHtml(kind, label){
  const safeLabel=esc(label||kind||'media');
  const current=_getStoredMediaPlaybackRate();
  return `<div class="media-speed-controls" role="group" aria-label="Playback speed for ${safeLabel}">${MEDIA_PLAYBACK_RATES.map(rate=>`<button type="button" class="media-speed-btn${rate===current?' active':''}" data-rate="${rate}" aria-pressed="${rate===current?'true':'false'}">${rate}√ó</button>`).join('')}</div>`;
}
function _mediaPlayerHtml(kind, src, name, extra=''){
  const safeName=esc(name||'media');
  const safeSrc=esc(src);
  const tag=kind==='video'
    ? `<video class="msg-media-player msg-media-video" src="${safeSrc}" controls preload="metadata" playsinline title="${safeName}"></video>`
    : `<audio class="msg-media-player msg-media-audio" src="${safeSrc}" controls preload="metadata" title="${safeName}"></audio>`;
  return `<div class="msg-media-editor msg-media-editor--${kind}" data-media-kind="${kind}">${tag}<div class="msg-media-meta"><span class="msg-media-name">${safeName}</span>${extra}</div>${_mediaSpeedControlsHtml(kind,safeName)}</div>`;
}
// Shared MEDIA: token renderer used by both the full-pipeline renderMd() and
// the streaming smd path in messages.js. Centralised so the live + settled
// representations of the same MEDIA token stay byte-identical, otherwise the
// streamed prose loses its image when the answer settles (#MEDIA-in-stream).
// `sessionId` is forwarded into /api/media so the same allow-list check applies
// to streamed references too; falls back to whatever the current session is.
// data:image/* URIs the renderer may embed directly as <img src>. Only raster
// formats plus base64 SVG (scripts do not execute inside <img>), only safe payload
// chars, and bounded size ‚Äî everything else (data:text/html etc.) must
// keep rendering as inert text so a model-emitted data: URI can never become an
// executable document.
const _DATA_IMAGE_RE=/^data:image\/(?:png|jpe?g|gif|webp|avif)(?:;base64)?,[a-z0-9+/=%._~:@!$&'()*+,;-]*$/i;
const _DATA_IMAGE_SVG_RE=/^data:image\/svg\+xml;base64,[a-z0-9+/=]+$/i;
const _DATA_IMAGE_MAX_LEN=2*1024*1024;

// The streaming renderer calls this ui-owned predicate too. Keep the dangerous
// SVG form base64-only: URL-encoded XML is a document-shaped payload, not a
// normal inline image transport.
function _isSafeDataImageUri(ref){
  const value=String(ref||'');
  return value.length<=_DATA_IMAGE_MAX_LEN
    && (_DATA_IMAGE_RE.test(value)||_DATA_IMAGE_SVG_RE.test(value));
}

function _dataImageHtml(ref, altText){
  if(!_isSafeDataImageUri(ref)) return null;
  return `<img class="msg-media-img" src="${esc(ref)}" alt="${esc(altText||'image')}" loading="lazy">`;
}

// Markdown image syntax ![alt](url) ‚Üí HTML. https:// keeps the historical direct
// <img>; file:// and bare data:image/ URIs route through the same helpers the
// MEDIA: pipeline uses, so ![x](file:///p.png) renders the artifact card instead
// of the broken "!<a>" anchor it used to produce, and ![x](data:image/...) stops
// dumping raw base64 text into the chat.
function _mdImageHtml(alt, url){
  if(/^data:/i.test(url)){
    const img=_dataImageHtml(url, alt);
    if(img) return img;
    return esc(`![${alt}](${String(url).slice(0,64)}‚Ä¶)`);
  }
  if(/^file:\/\//i.test(url)) return _inlineMediaHtmlForRef(url,undefined,alt);
  return `<img src="${url.replace(/"/g,'%22')}" alt="${esc(alt)}" class="msg-media-img" loading="lazy">`;
}

function _mediaTokenParts(source, matchOffset, rawRef){
  let ref=String(rawRef||'');
  let suffix='';
  const before=String(source||'').slice(0,Number(matchOffset)||0);
  // Quotes are valid path/URL bytes, so detach one only when the prose has the
  // same opener immediately before MEDIA:. The entity forms are what the real
  // streaming parser passes after escaping text nodes.
  for(const family of [
    {value:'"', forms:['"','&quot;']},
    {value:"'", forms:["'",'&#39;']},
  ]){
    if(!family.forms.some(form=>before.endsWith(form))) continue;
    let quote='', closeAt=-1;
    for(const form of family.forms){
      const index=ref.lastIndexOf(form);
      if(index>closeAt){ quote=form; closeAt=index; }
    }
    if(closeAt<=0) continue;
    const afterQuote=ref.slice(closeAt+quote.length);
    if(!/^[.,;:!?]*$/.test(afterQuote)) continue;
    ref=ref.slice(0,closeAt);
    suffix=family.value+afterQuote;
    break;
  }
  let punctuationStart=ref.length;
  while(punctuationStart>0&&'.,;:!?'.includes(ref.charAt(punctuationStart-1))){
    punctuationStart-=1;
  }
  const trailingPunctuation=ref.slice(punctuationStart);
  for(const delimiter of ['***','___','**','__','*','_','`']){
    if(!before.endsWith(delimiter)) continue;
    const openerStart=before.length-delimiter.length;
    if(openerStart>0&&before.charAt(openerStart-1)===delimiter.charAt(0)) continue;
    let candidate=ref;
    let afterDelimiter='';
    if(trailingPunctuation&&candidate.slice(0,-trailingPunctuation.length).endsWith(delimiter)){
      candidate=candidate.slice(0,-trailingPunctuation.length);
      afterDelimiter=trailingPunctuation;
    }
    if(candidate===delimiter) return null;
    if(candidate.endsWith(delimiter)&&candidate.length>delimiter.length){
      const closerStart=candidate.length-delimiter.length;
      if(candidate.charAt(closerStart-1)===delimiter.charAt(0)) continue;
      ref=candidate.slice(0,-delimiter.length);
      // The matching closer proves only its own bytes are outside the
      // reference. Punctuation immediately before it may be a legal
      // filename or URL byte and must remain bound to the ref.
      suffix=delimiter+afterDelimiter;
      break;
    }
  }
  // A bare trailing punctuation byte is ambiguous: it may be prose, but it
  // may also be part of a real local filename or remote URL. Only the quote
  // and delimiter branches above have evidence from a matching opener that a
  // closer is outside the MEDIA ref, so preserve every other byte verbatim.
  if(!ref) return null;
  return [ref,suffix];
}

function _inlineMediaHtmlForRef(ref, sessionId, altText){
  if(ref==null) return '';
  // data:image/* ‚Üí inline <img>; any other data: scheme renders as inert
  // truncated text (never routed to api/media, never embedded).
  if(/^data:/i.test(ref)){
    const img=_dataImageHtml(ref,altText===undefined?'image':altText);
    if(img) return img;
    return `<code>${esc(String(ref).slice(0,64))}‚Ä¶</code>`;
  }
  // Keep this logic self-contained: some tests extract renderMd() alone and
  // execute it in node, without the top-level helper functions from ui.js.
  // Tests look for `new URL(ref)` / `u.pathname` / `api/media?path=` patterns,
  // so the variable name is the original `ref` (not `r`) and the file://
  // unwrap keeps the matched identifier visible.
  if(/^file:\/\//i.test(ref)){
    try{
      const u=new URL(ref);
      ref=decodeURIComponent(u.pathname||ref.replace(/^file:\/\//i,''));
    }catch(_){
      try{ref=decodeURIComponent(ref.replace(/^file:\/\//i,''));}
      catch(__){ref=ref.replace(/^file:\/\//i,'');}
    }
  }
  if(/^https?:\/\//i.test(ref)){
    let src=ref;
    if(/^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?/i.test(src)){
      const base=(typeof document!=='undefined'&&document.baseURI||'').replace(/\/$/,'');
      src=src.replace(/^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?/i,base);
    }
    const urlPath=src.split('?')[0];
    // SVG URLs ‚Üí render inline as image (must precede the https:// <img>
    // catch-all below so extensionless CDN SVG paths still match)
    if(_SVG_EXTS.test(urlPath)){
      return `<img class="msg-media-svg" src="${esc(src)}" alt="${esc(typeof t==='function'?t('media_svg_label'):'svg')}" loading="lazy">`;
    }
    const mediaKind=_mediaKindForName(urlPath);
    if(mediaKind==='audio'||mediaKind==='video') return _mediaPlayerHtml(mediaKind,src,urlPath.split('/').pop()||mediaKind);
    // Render all https:// URLs as <img> ‚Äî extensionless CDN paths like fal.media still work (#853)
    if(_IMAGE_EXTS.test(urlPath) || /^https?:\/\//i.test(src)){
      return `<img class="msg-media-img" src="${esc(src)}" alt="image" loading="lazy">`;
    }
    return `<a href="${esc(src)}" target="_blank" rel="noopener">${esc(src)}</a>`;
  }
  // Local file path ‚Äî route through /api/media so the session allow-list check
  // (api/routes.py _resolve_media_path) gates the access the same way it does
  // for the full-pipeline renderer.
  const sid=sessionId
    || (typeof S!=='undefined'&&S&&S.session&&S.session.session_id?String(S.session.session_id):'')
    || '';
  const apiUrl='api/media?path='+encodeURIComponent(ref)+(sid?'&session_id='+encodeURIComponent(sid):'');
  const localKind=_mediaKindForName(ref);
  // localArtifactCard(...)
  if(localKind==='image'){
    const safeName=esc(altText===undefined?(ref.split('/').pop()||'image'):altText);
    const tt=(typeof t==='function')?t:(key=>({media_download:'Download'}[key]||key));
    const dlLabel=esc(tt('media_download'));
    const dlSvg='<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg>';
    return `<span class="msg-artifact-image"><img class="msg-media-img" src="${esc(apiUrl)}" alt="${safeName}" loading="lazy"><a class="msg-artifact-download" href="${esc(apiUrl)}" download="${safeName}" title="${dlLabel}" aria-label="${dlLabel}" onclick="event.stopPropagation()">${dlSvg}</a></span>`;
  }
  if(_SVG_EXTS.test(ref)) return `<img class="msg-media-svg" src="${esc(apiUrl)}" alt="${esc(altText===undefined?(typeof t==='function'?t('media_svg_label'):'svg'):altText)}" loading="lazy">`;
  if(localKind==='audio'||localKind==='video'){
    return _mediaPlayerHtml(localKind,apiUrl+'&inline=1',ref.split('/').pop()||ref);
  }
  if(_PDF_EXTS.test(ref)){
    const fname=esc(ref.split('/').pop()||ref);
    return `<div class="pdf-preview-load" data-path="${esc(ref)}"><span class="pdf-preview-spinner">‚è≥</span> ${esc(typeof t==='function'?t('pdf_loading'):'Loading')} ${fname}...</div>`;
  }
  if(_HTML_EXTS.test(ref)){
    return `<div class="html-preview-load" data-path="${esc(ref)}"><span class="html-preview-spinner">‚è≥</span> ${esc(typeof t==='function'?t('html_loading'):'Loading')}...</div>`;
  }
  const fname=esc(ref.split('/').pop()||ref);
  if(/\.(patch|diff)$/i.test(ref)) return `<div class="diff-inline-load" data-path="${esc(ref)}">${esc(typeof t==='function'?t('diff_loading'):'Loading diff')} ${fname}...</div>`;
  if(_CSV_EXTS.test(ref)) return `<div class="csv-inline-load" data-path="${esc(ref)}">${esc(typeof t==='function'?t('csv_loading'):'Loading')} ${fname}...</div>`;
  if(_EXCALIDRAW_EXTS.test(ref)) return `<div class="excalidraw-inline-load" data-path="${esc(ref)}">${esc(typeof t==='function'?t('excalidraw_loading'):'Loading')} ${fname}...</div>`;
  return `<a class="msg-media-link" href="${esc(apiUrl+'&download=1')}" download="${fname}">üìé ${fname}</a>`;
}
function _renderAttachmentHtml(fname, url){
  const kind=_mediaKindForName(fname);
  if(kind==='image') return `<img class="msg-media-img" src="${esc(url)}" alt="${esc(fname)}" loading="lazy">`;
  if(kind==='audio'||kind==='video') return _mediaPlayerHtml(kind,url,fname);
  if(_HTML_EXTS.test(fname)){
    const inlineUrl=url+(String(url).includes('?')?'&':'?')+'inline=1';
    return `<a class="msg-file-badge msg-file-badge--html" href="${esc(inlineUrl)}" target="_blank" rel="noopener">${li('file-code',12)} ${esc(fname)}</a>`;
  }
  return `<div class="msg-file-badge">${li('paperclip',12)} ${esc(fname)}</div>`;
}
document.addEventListener('click', e => {
  const btn=e.target&&e.target.closest?e.target.closest('.media-speed-btn'):null;
  if(!btn) return;
  const editor=btn.closest('.msg-media-editor,.preview-media-wrap');
  if(!editor) return;
  const media=editor.querySelector('audio,video');
  if(!media) return;
  const rate=Number(btn.dataset.rate)||1;
  _setStoredMediaPlaybackRate(rate);
  _applyMediaPlaybackRate(media,rate);
});
document.addEventListener("loadedmetadata", e=>{
  if(e.target&&e.target.matches&&e.target.matches('.msg-media-player,audio,video')){
    _applyMediaPlaybackRate(e.target);
  }
},true);
document.addEventListener('play',e=>{
  if(e.target&&e.target.matches&&e.target.matches('.msg-media-video')){
    _promoteVisibleVideoPreload(e.target);
  }
},true);
function _initMediaPlaybackObserver(){
  if(!document.body||window._mediaPlaybackObserver) return;
  _initMediaVisibilityObserver();
  window._mediaPlaybackObserver=new MutationObserver(records=>{
    for(const rec of records){
      for(const node of rec.removedNodes||[]){
        if(!node||node.nodeType!==1) continue;
        const videos=[];
        if(node.matches&&node.matches('.msg-media-video')) videos.push(node);
        if(node.querySelectorAll) videos.push(...node.querySelectorAll('.msg-media-video'));
        videos.forEach(_unobserveVideoPreload);
      }
      for(const node of rec.addedNodes||[]){
        if(!node||node.nodeType!==1) continue;
        const media=[];
        if(node.matches&&node.matches('audio,video')) media.push(node);
        if(node.querySelectorAll) media.push(...node.querySelectorAll('audio,video'));
        media.forEach(m=>{
          _applyMediaPlaybackRate(m);
          _observeVideoPreload(m);
        });
      }
    }
  });
  window._mediaPlaybackObserver.observe(document.body,{childList:true,subtree:true});
  document.querySelectorAll('audio,video').forEach(m=>{
    _applyMediaPlaybackRate(m);
    _observeVideoPreload(m);
  });
}
if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',_initMediaPlaybackObserver);
else _initMediaPlaybackObserver();
setTimeout(_initMediaPlaybackObserver,0);

// ‚îÄ‚îÄ Ambient provider quota indicator (#1766) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
let _providerQuotaRefreshInFlight=false;

function _formatQuotaMoneyShort(value){
  const n=Number(value);
  if(!Number.isFinite(n)) return '';
  if(Math.abs(n)>=100) return '$'+n.toFixed(0);
  if(Math.abs(n)>=10) return '$'+n.toFixed(1);
  return '$'+n.toFixed(2);
}
function _formatQuotaPercentShort(value){
  const n=Number(value);
  if(!Number.isFinite(n)) return '';
  return Math.max(0,Math.min(100,n)).toFixed(0)+'%';
}
function _providerQuotaIndicatorText(status){
  if(!status||status.status!=='available') return null;
  const provider=status.display_name||status.provider||'Provider';
  const accountLimits=status.account_limits||null;
  if(accountLimits&&Array.isArray(accountLimits.windows)&&accountLimits.windows.length){
    const w=accountLimits.windows.find(x=>x&&Number.isFinite(Number(x.remaining_percent)))||accountLimits.windows[0];
    const remaining=_formatQuotaPercentShort(w&&w.remaining_percent);
    if(remaining) return {label:remaining, title:provider+' ‚Äî '+(status.message||'Provider usage loaded')+' ‚Äî '+remaining+' remaining'};
  }
  const quota=status.quota||null;
  if(quota){
    const remaining=_formatQuotaMoneyShort(quota.limit_remaining);
    const used=_formatQuotaMoneyShort(quota.usage);
    const limit=_formatQuotaMoneyShort(quota.limit);
    if(remaining){
      const parts=[];
      if(used) parts.push('used '+used);
      if(limit) parts.push('limit '+limit);
      return {label:remaining, title:provider+' ‚Äî '+(status.message||'Provider quota loaded')+(parts.length?' ‚Äî '+parts.join(' ¬∑ '):'')};
    }
  }
  return null;
}
function renderProviderQuotaIndicator(status){
  const chip=$('providerQuotaChip');
  const label=$('providerQuotaChipLabel');
  const mobileAction=$('composerMobileQuotaAction');
  const mobileLabel=$('composerMobileQuotaLabel');
  if(!chip||!label) return;
  // Hide entirely when the user has disabled the ambient quota chip in Settings.
  // Boot defaults this on; an explicit false preference suppresses it.
  if(window._showQuotaChip!==true){
    chip.hidden=true;
    label.textContent='';
    chip.removeAttribute('title');
    if(mobileAction){mobileAction.style.display='none';mobileAction.removeAttribute('title');}
    if(mobileLabel) mobileLabel.textContent='';
    return;
  }
  const text=_providerQuotaIndicatorText(status);
  if(!text||status.status!=='available'||(!status.quota&&!status.account_limits)){
    chip.hidden=true;
    label.textContent='';
    chip.removeAttribute('title');
    if(mobileAction){mobileAction.style.display='none';mobileAction.removeAttribute('title');}
    if(mobileLabel) mobileLabel.textContent='';
    return;
  }
  label.textContent=text.label;
  chip.title=text.title;
  chip.hidden=false;
  if(mobileAction){mobileAction.style.display='';mobileAction.title=text.title;}
  if(mobileLabel) mobileLabel.textContent=text.label;
}
async function refreshProviderQuotaIndicator(){
  // Short-circuit before the fetch when the chip is disabled ‚Äî no point asking
  // the server for quota data the UI will throw away.
  if(window._showQuotaChip!==true){
    const chip=$('providerQuotaChip');
    if(chip){chip.hidden=true;chip.removeAttribute('title');}
    const mobileAction=$('composerMobileQuotaAction');
    if(mobileAction){mobileAction.style.display='none';mobileAction.removeAttribute('title');}
    const mobileLabel=$('composerMobileQuotaLabel');
    if(mobileLabel) mobileLabel.textContent='';
    return;
  }
  if(_providerQuotaRefreshInFlight) return;
  _providerQuotaRefreshInFlight=true;
  try{
    const status=await api('/api/provider/quota');
    renderProviderQuotaIndicator(status);
  }catch(_e){
    renderProviderQuotaIndicator(null);
  }finally{
    _providerQuotaRefreshInFlight=false;
  }
}
window.addEventListener('visibilitychange',()=>{
  if(document.visibilityState==='visible'&&typeof refreshProviderQuotaIndicator==='function') refreshProviderQuotaIndicator();
});

// Dynamic model labels -- populated by populateModelDropdown(), fallback to static map
let _dynamicModelLabels={};
window._configuredModelBadges=window._configuredModelBadges||{};
const MODEL_STATE_KEY='hermes-webui-model-state';
const PENDING_SESSION_MODEL_PREFIX='hermes-webui-pending-session-model:';
const PENDING_SESSION_MODEL_MAX_AGE_MS=10*60*1000;

// ‚îÄ‚îÄ Smart model resolver ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
// Finds the best matching option value in a <select> for a given model ID.
// Handles mismatches like 'claude-sonnet-4-6' vs 'anthropic/claude-sonnet-4.6'.
// When a preferred provider is supplied, duplicate normalized IDs prefer that
// provider's option so Settings/profile rehydration doesn't snap back to the
// first colliding entry.
function _getOptionProviderId(opt){
  if(!opt) return '';
  if(opt.dataset && opt.dataset.provider) return opt.dataset.provider;
  const group=opt.parentElement;
  if(group && group.tagName==='OPTGROUP' && group.dataset && group.dataset.provider){
    return group.dataset.provider;
  }
  const value=String(opt.value||'');
  if(value.startsWith('@') && value.includes(':')){
    // Non-greedy parse for @custom:<slug>:<model> ‚Äî provider is the slug only.
    // Preserves endpoint-style host:port custom slugs (e.g. custom:localhost:11434)
    // while keeping colon-bearing model ids (e.g. @custom:backup:model-a:free -> custom:backup).
    if(value.startsWith('@custom:')){
      const afterCustom=value.substring('@custom:'.length);
      const parts=afterCustom.split(':');
      if(parts.length>=3 && /^\d+$/.test(parts[1])){
        const port=parseInt(parts[1], 10);
        const host=parts[0];
        const hl=host.toLowerCase();
        if(port>=1 && port<=65535 && (hl==='localhost' || host.includes('.'))){
          return 'custom:'+host+':'+parts[1];
        }
      }
      const firstColon=afterCustom.indexOf(':');
      if(firstColon>=0) return 'custom:'+afterCustom.substring(0,firstColon);
      return 'custom:'+afterCustom;
    }
    // Other @provider:model ‚Äî provider is up to first colon
    return value.slice(1,value.indexOf(':'));
  }
  return '';
}
function _providerFromModelValue(modelId){
  const value=String(modelId||'').trim();
  if(value.startsWith('@')&&value.includes(':')){
    // Non-greedy parse for @custom:<slug>:<model> ‚Äî provider is the slug only.
    // Preserves endpoint-style host:port custom slugs (e.g. custom:localhost:11434)
    // while keeping colon-bearing model ids (e.g. @custom:backup:model-a:free -> custom:backup).
    if(value.startsWith('@custom:')){
      const afterCustom=value.substring('@custom:'.length);
      const parts=afterCustom.split(':');
      if(parts.length>=3 && /^\d+$/.test(parts[1])){
        const port=parseInt(parts[1], 10);
        const host=parts[0];
        const hl=host.toLowerCase();
        if(port>=1 && port<=65535 && (hl==='localhost' || host.includes('.'))){
          return 'custom:'+host+':'+parts[1];
        }
      }
      const firstColon=afterCustom.indexOf(':');
      if(firstColon>=0) return 'custom:'+afterCustom.substring(0,firstColon);
      return 'custom:'+afterCustom;
    }
    // Other @provider:model ‚Äî provider is up to first colon
    return value.slice(1,value.indexOf(':'));
  }
  return '';
}
function _modelPickerOptionIdentity(modelId, providerId){
  let value=String(modelId||'');
  const provider=String(providerId||'').trim();
  if(value.startsWith('@')&&value.includes(':')){
    const exactPrefix=provider ? `@${provider}:` : '';
    if(exactPrefix && value.toLowerCase().startsWith(exactPrefix.toLowerCase())){
      value=value.substring(exactPrefix.length);
    }else if(value.startsWith('@custom:')){
      const afterCustom=value.substring('@custom:'.length);
      const parts=afterCustom.split(':');
      let splitAt=-1;
      if(parts.length>=3 && /^\d+$/.test(parts[1])){
        const port=parseInt(parts[1], 10);
        const host=parts[0];
        const hl=host.toLowerCase();
        if(port>=1 && port<=65535 && (hl==='localhost' || host.includes('.'))){
          splitAt=parts[0].length + 1 + parts[1].length;
        }
      }
      if(splitAt<0) splitAt=afterCustom.indexOf(':');
      value=splitAt>=0 ? afterCustom.substring(splitAt+1) : afterCustom;
    }else{
      value=value.substring(value.indexOf(':')+1);
    }
  }
  return value.replace(/-/g,'.').toLowerCase();
}
function _deduplicateModelPickerOptions(sel,selectedValue){
  if(!sel||!sel.querySelectorAll) return 0;
  let removed=0;
  for(const group of sel.querySelectorAll('optgroup')){
    const options=Array.from(group.children||[]).filter(opt=>opt&&opt.tagName==='OPTION');
    const byIdentity=new Map();
    for(const opt of options){
      const identity=_modelPickerOptionIdentity(opt.value,_getOptionProviderId(opt));
      if(!identity) continue;
      if(!byIdentity.has(identity)) byIdentity.set(identity,[]);
      byIdentity.get(identity).push(opt);
    }
    for(const candidates of byIdentity.values()){
      if(candidates.length<2) continue;
      const selected=candidates.find(opt=>opt.value===selectedValue);
      const routable=candidates.find(opt=>String(opt.value||'').startsWith('@'));
      const survivor=selected||routable||candidates[0];
      for(const opt of candidates){
        if(opt===survivor) continue;
        group.removeChild(opt);
        removed++;
      }
    }
  }
  return removed;
}
function _providerSkipsModelMismatchWarning(providerId){
  const p=String(providerId||'').toLowerCase();
  return !p||p==='custom'||p.startsWith('custom:')||p==='openrouter';
}
function _providerDefersMissingModelFallback(providerId){
  const p=String(providerId||'').toLowerCase();
  // Named custom providers and OpenRouter can legitimately route vendor-prefixed
  // model IDs that are not present in the current static catalog. Do not
  // silently rewrite those sessions to the default just because the option has
  // not been hydrated yet (#2405).
  return p.startsWith('custom:')||p==='openrouter';
}
function _modelStateForSelect(sel, modelId){
  const value=String(modelId||'').trim();
  if(!value) return {model:'',model_provider:null};
  const explicitProvider=_providerFromModelValue(value);
  if(explicitProvider){
    const selected=sel&&sel.options
      ?Array.from(sel.options).find(o=>String(o.value||'')===value)
      :null;
    const routedModel=selected&&selected.dataset&&selected.dataset.model;
    // Read the provider from the matched option's authoritative data-provider
    // rather than re-parsing the value at its LAST colon: a colon-bearing model
    // id (e.g. model-a:free) synthesized as @custom:backup:model-a:free would
    // otherwise mis-parse to provider "custom:backup:model-a" (#6221 re-gate).
    const routedProvider=selected?String(_getOptionProviderId(selected)||'').trim():'';
    // Normally-rendered catalog options only carry the qualified
    // @custom:<slug>:<model> value ‚Äî data-model is set solely by the fallback
    // injection path (_ensureModelOptionInDropdown). When it is missing, strip
    // the @custom:<slug>: prefix instead of sending the raw dropdown value as
    // the model id (#6884). The prefix must come from the option metadata's
    // authoritative provider (routedProvider), NOT from explicitProvider: the
    // latter re-parses the value at its LAST colon, so a colon-bearing model
    // id like @custom:backup:model-a:free would otherwise strip to just
    // "free" (re-gate on the #6221 family). Only custom providers are
    // stripped: a non-custom qualified id like @safe:gpt-4o-mini is a real
    // provider namespace and must be preserved (#1771).
    const effectiveProvider=routedProvider||explicitProvider;
    const effectiveProviderLc=effectiveProvider.toLowerCase();
    const isCustomProvider=effectiveProviderLc==='custom'||effectiveProviderLc.startsWith('custom:');
    const explicitPrefix=`@${effectiveProvider}:`;
    const strippedModel=isCustomProvider&&value.toLowerCase().startsWith(explicitPrefix.toLowerCase())
      ?value.slice(explicitPrefix.length)
      :value;
    return {model:routedModel||strippedModel||value,model_provider:effectiveProvider};
  }
  // Resolve the provider from the option whose VALUE matches the requested
  // model ‚Äî never blindly from sel.selectedOptions[0] (#5567). During a profile
  // /tab switch or a model-list rebuild the dropdown transiently still has the
  // PREVIOUS profile's default option selected (e.g. an ollama model), so reading
  // selectedOptions[0] would stamp that foreign provider onto a model it doesn't
  // own ‚Äî which is then persisted into the session's model_provider and re-sent
  // on every turn, bricking it with a "Provider 'X'‚Ä¶no API key" error for a
  // provider the session never used.
  let opt=null;
  const selected=sel&&sel.selectedOptions&&sel.selectedOptions[0];
  // Prefer the currently-selected option ONLY when it actually is the requested
  // model ‚Äî this preserves the user's exact pick in the same-value/different-
  // provider collision case (two providers offering the same model id).
  if(selected&&String(selected.value||'')===value){
    opt=selected;
  }else if(sel&&sel.options){
    opt=Array.from(sel.options).find(o=>String(o.value||'')===value)||null;
  }
  const provider=String(_getOptionProviderId(opt)||'').trim();
  return {model:value,model_provider:(provider&&provider!=='default')?provider:null};
}
function _captureModelDropdownSelection(sel){
  if(!sel||!sel.value) return null;
  try{
    const state=_modelStateForSelect(sel,sel.value);
    if(state&&state.model) return state;
  }catch(_){}
  return {model:String(sel.value||''),model_provider:null};
}
function _modelProviderForSend(modelId){
  const sessionProvider=(S&&S.session&&S.session.model_provider)||null;
  if(sessionProvider) return sessionProvider;
  const model=String(modelId||'').trim();
  if(!model) return null;
  const explicitProvider=typeof _providerFromModelValue==='function'
    ? _providerFromModelValue(model)
    : '';
  if(explicitProvider) return explicitProvider;
  const sel=typeof $==='function' ? $('modelSelect') : null;
  if(sel&&String(sel.value||'').trim()===model&&typeof _modelStateForSelect==='function'){
    try{
      const dropdownState=_modelStateForSelect(sel,sel.value);
      if(dropdownState&&String(dropdownState.model||'').trim()===model){
        return dropdownState.model_provider||null;
      }
    }catch(_){}
  }
  if(typeof _readPersistedModelState==='function'){
    try{
      const persisted=_readPersistedModelState();
      if(persisted&&String(persisted.model||'').trim()===model){
        return persisted.model_provider||null;
      }
    }catch(_){}
  }
  return null;
}
function _reconcileModelDropdownSelection(sel,data,previousState,opts){
  if(!sel) return null;
  const activeSession=(typeof S!=='undefined'&&S&&S.session)?S.session:null;
  // Fresh boot is the only path where the profile/server default intentionally
  // beats a browser-persisted or static fallback value. Every other model-list
  // rebuild should preserve the loaded session model or the user's current
  // in-page selection when it still exists in the refreshed catalog.
  const shouldApplyBootDefault=!!(opts&&opts.preferProfileDefaultOnFreshBoot);

  // Helper: apply the requested model, but if it is missing from the current
  // catalog (cross-provider selection after a partial/timed-out rebuild), inject
  // it as a custom option instead of returning null and letting the browser
  // silently snap to the first <option>. _ensureModelOptionInDropdown already
  // tries _applyModelToDropdown first, so delegate to it (single scan) and keep
  // the plain-apply fallback for the unlikely case it is unavailable.
  const _applyOrEnsure = function(modelId, providerId) {
    if (typeof _ensureModelOptionInDropdown === 'function') {
      return _ensureModelOptionInDropdown(modelId, sel, providerId);
    }
    return _applyModelToDropdown(modelId, sel, providerId);
  };

  if(shouldApplyBootDefault && data&&data.default_model && !(activeSession&&activeSession.model)){
    return _applyOrEnsure(data.default_model, data.active_provider||null);
  }
  if(activeSession&&activeSession.model){
    return _applyOrEnsure(activeSession.model, activeSession.model_provider||null);
  }
  if(previousState&&previousState.model){
    return _applyOrEnsure(previousState.model, previousState.model_provider||null);
  }
  return null;
}
function _providerQualifiedModelValueForSelect(sel, modelId){
  return _modelStateForSelect(sel,modelId).model;
}
function _readPersistedModelState(){
  try{
    const raw=localStorage.getItem(MODEL_STATE_KEY);
    if(raw){
      const parsed=JSON.parse(raw);
      if(parsed&&parsed.model){
        return {
          model:String(parsed.model||''),
          model_provider:parsed.model_provider?String(parsed.model_provider):(_providerFromModelValue(parsed.model)||null),
        };
      }
    }
  }catch(_){}
  const legacy=localStorage.getItem('hermes-webui-model');
  if(!legacy) return null;
  return {model:legacy,model_provider:_providerFromModelValue(legacy)||null};
}
function _writePersistedModelState(model, modelProvider){
  const value=String(model||'').trim();
  const provider=modelProvider?String(modelProvider).trim():(_providerFromModelValue(value)||null);
  if(!value){
    localStorage.removeItem('hermes-webui-model');
    localStorage.removeItem(MODEL_STATE_KEY);
    return;
  }
  localStorage.setItem('hermes-webui-model', value);
  try{
    localStorage.setItem(MODEL_STATE_KEY, JSON.stringify({model:value,model_provider:provider||null}));
  }catch(_){}
}
function _clearPersistedModelState(){
  localStorage.removeItem('hermes-webui-model');
  localStorage.removeItem(MODEL_STATE_KEY);
}
function _pendingSessionModelKey(sessionId){
  return PENDING_SESSION_MODEL_PREFIX+String(sessionId||'');
}
function _rememberPendingSessionModel(sessionId, model, modelProvider){
  const sid=String(sessionId||'').trim();
  const value=String(model||'').trim();
  if(!sid||!value) return;
  const provider=modelProvider?String(modelProvider).trim():(_providerFromModelValue(value)||null);
  try{
    sessionStorage.setItem(_pendingSessionModelKey(sid), JSON.stringify({
      model:value,
      model_provider:provider||null,
      saved_at:Date.now(),
    }));
  }catch(_){}
}
function _readPendingSessionModel(sessionId){
  const sid=String(sessionId||'').trim();
  if(!sid) return null;
  try{
    const raw=sessionStorage.getItem(_pendingSessionModelKey(sid));
    if(!raw) return null;
    const parsed=JSON.parse(raw);
    const model=String(parsed&&parsed.model||'').trim();
    if(!model){
      sessionStorage.removeItem(_pendingSessionModelKey(sid));
      return null;
    }
    const savedAt=Number(parsed.saved_at||0);
    if(savedAt&&Date.now()-savedAt>PENDING_SESSION_MODEL_MAX_AGE_MS){
      sessionStorage.removeItem(_pendingSessionModelKey(sid));
      return null;
    }
    return {
      model,
      model_provider:parsed&&parsed.model_provider?String(parsed.model_provider):(_providerFromModelValue(model)||null),
    };
  }catch(_){
    try{sessionStorage.removeItem(_pendingSessionModelKey(sid));}catch(__){}
    return null;
  }
}
function _clearPendingSessionModel(sessionId){
  const sid=String(sessionId||'').trim();
  if(!sid) return;
  try{sessionStorage.removeItem(_pendingSessionModelKey(sid));}catch(_){}
}
// #5924: the recovery-send deliberate-pick signal. Returns {model, model_provider}
// ONLY when the active session's own model is a genuine non-default pick vs the
// profile default ‚Äî the same signal send()'s persistent-pick path (_isCrossProviderPick)
// uses, generalized to same-provider non-default picks too. Used by the recovery
// paths (cmdRetry / submitEdit) to decide whether to re-arm the single-shot
// explicit-pick marker: the marker is consumed by the failed send before we reach
// recovery, so we can't read it back, and comparing _chatPayloadModel() to itself
// either false-negatives (an already-applied pick looks unchanged) or false-positives
// (provider inference manufactures a "change"). A non-default session model is the
// durable, inference-free evidence of a real pick. Returns null (no re-arm ‚Üí the
// server's compatible-model resolution runs) when the session is on the default.
function _deliberateSessionModelPick(sessionId){
  if(!S.session||S.session.session_id!==sessionId) return null;
  const model=String(S.session.model||'').trim();
  if(!model) return null;
  // Require SESSION-OWNED provider evidence ‚Äî a stored model_provider on the
  // session itself. Do NOT infer a provider from the model string: an
  // unreachable/renamed model like "@removed:mistral-large" with no stored
  // provider must NOT count as a deliberate pick (round-2/3 false-positive).
  const provider=S.session.model_provider?String(S.session.model_provider).trim():'';
  if(!provider) return null;
  // Require a KNOWN profile default to compare against. If we don't know the
  // default (empty window._defaultModel), we can't prove this is a non-default
  // pick, so fail closed ‚Üí no re-arm (server compatible-model resolution runs).
  const defaultModel=(typeof window!=='undefined'&&window._defaultModel)?String(window._defaultModel):'';
  const activeProvider=(typeof window!=='undefined'&&window._activeProvider)?String(window._activeProvider):'';
  if(!defaultModel||!activeProvider) return null;
  // Non-default = a different model OR a different provider than the profile
  // default. A session sitting exactly on the profile default is NOT a pick.
  const isDefault=(model===defaultModel)&&(provider===activeProvider);
  if(isDefault) return null;
  return {model, model_provider:provider};
}
// #5924: re-arm the single-shot explicit-pick marker from a recovery pick, but
// ONLY if it's still safe at fire time. Guards the SILENT same-session race where
// the user changes the model DURING the recovery's awaits: (1) the session must
// still be the captured one; (2) the session's CURRENT model/provider must still
// equal the captured pick (a mid-flight change means the pick is stale ‚Äî skip);
// (3) never clobber a NEWER pending marker (an onchange during the await already
// wrote the authoritative one). Returns true if it re-armed.
function _reArmRecoveryPick(sessionId, pick){
  if(!pick||!pick.model) return false;
  if(!S.session||S.session.session_id!==sessionId) return false;
  // Current session state must still match the captured pick (no mid-flight change).
  if(String(S.session.model||'')!==String(pick.model||'')
     ||String(S.session.model_provider||'')!==String(pick.model_provider||'')) return false;
  // Do not overwrite a newer marker written by an onchange during the await.
  if(typeof _readPendingSessionModel==='function'){
    const existing=_readPendingSessionModel(sessionId);
    if(existing&&existing.model
       &&(String(existing.model)!==String(pick.model)
          ||String(existing.model_provider||'')!==String(pick.model_provider||''))) return false;
  }
  if(typeof _rememberPendingSessionModel==='function'){
    _rememberPendingSessionModel(sessionId, pick.model, pick.model_provider);
    return true;
  }
  return false;
}
function _applyPendingSessionModelForSession(sessionId){
  if(!S.session||S.session.session_id!==sessionId) return false;
  const pending=_readPendingSessionModel(sessionId);
  if(!pending) return false;
  const sameModel=String(S.session.model||'')===pending.model;
  const sameProvider=String(S.session.model_provider||'')===String(pending.model_provider||'');
  if(sameModel&&sameProvider){
    _clearPendingSessionModel(sessionId);
    return false;
  }
  S.session.model=pending.model;
  S.session.model_provider=pending.model_provider||null;
  const retry=_persistSessionModelCorrection(pending.model,pending.model_provider||null,{propagateErrors:true});
  if(retry&&typeof retry.then==='function'){
    retry.then(()=>_clearPendingSessionModel(sessionId)).catch(()=>{});
  }
  return true;
}
function _findModelInDropdown(modelId, sel, preferredProviderId){
  if(!modelId||!sel) return null;
  const options=Array.from(sel.options);
  const opts=options.map(o=>o.value);
  // 0. Exact match ‚Äî highest priority when it doesn't conflict with a
  // cross-provider preference (#3360, guarded for #1228/#1313).
  // When all models share the same provider (e.g. a custom proxy),
  // normalization can collapse distinct multi-slash IDs to the same key
  // and options.find() returns whichever appears first in the DOM instead
  // of the exact value.  But when the exact option belongs to a *different*
  // provider than the preferred one, we must fall through to the provider-
  // aware match so rehydration doesn't snap to the wrong provider row.
  if(opts.includes(modelId)){
    const exactOpt=options.find(o=>o.value===modelId);
    const exactProv=exactOpt?_getOptionProviderId(exactOpt).toLowerCase():'';
    const pref=String(preferredProviderId||'').toLowerCase();
    if(!pref || !exactProv || exactProv===pref) return modelId;
  }
  // 1. Restore lookup keeps the older hierarchy-preserving matcher instead of
  // the picker-dedup identity, so missing qualified models do not substitute a
  // different suffix-sharing sibling.
  const norm=s=>String(s||'')
    .toLowerCase()
    .replace(/^@([^:]+:)+/,'')
    .replace(/^[^/]+\//,'')
    .replace(/-/g,'.');
  const target=norm(modelId);
  let explicitProvider='';
  const rawModel=String(modelId||'');
  if(rawModel.startsWith('@')&&rawModel.includes(':')){
    explicitProvider=rawModel.slice(1,rawModel.lastIndexOf(':'));
  }
  const preferred=String(preferredProviderId||explicitProvider||'').toLowerCase();
  if(preferred){
    if(preferred==='custom'||preferred.startsWith('custom:')){
      // A slash is part of a custom endpoint's upstream model ID, not a
      // provider namespace. Match the exact routed ID (allowing only the
      // WebUI's @provider: wrapper and dash/dot spelling compatibility).
      const routeNorm=value=>{
        let routed=String(value||'');
        const prefix=`@${preferred}:`;
        if(routed.toLowerCase().startsWith(prefix)) routed=routed.slice(prefix.length);
        return routed.toLowerCase().replace(/-/g,'.');
      };
      const providerOptions=options.filter(o=>_getOptionProviderId(o).toLowerCase()===preferred);
      const providerMatch=providerOptions.find(o=>routeNorm(o.value)===routeNorm(rawModel));
      if(providerMatch) return providerMatch.value;
      // Legacy sessions may store only the bare suffix of a routed custom
      // option. Preserve #6195's provider-hinted repair, but only for an
      // explicit @provider: row; an unwrapped slash ID belongs to the active
      // endpoint and must not substitute for a distinct bare model.
      if(!rawModel.includes('/')&&!rawModel.startsWith('@')){
        const prefix=`@${preferred}:`;
        const suffixMatches=providerOptions.filter(o=>
          String(o.value||'').toLowerCase().startsWith(prefix)
          &&norm(o.value)===target
        );
        if(suffixMatches.length===1) return suffixMatches[0].value;
      }
      return null;
    }
    const providerMatch=options.find(o=>norm(o.value)===target&&_getOptionProviderId(o).toLowerCase()===preferred);
    if(providerMatch) return providerMatch.value;
  }
  // 2. Normalized match ‚Äî but ONLY when unambiguous. If the bare id
  // matches across multiple provider groups AND no provider hint is
  // available, return null instead of snapping to the first group's
  // option. This prevents a deliberate non-default pick from reverting
  // to the default provider on re-render (#6195).
  const exact=opts.find(o=>norm(o)===target);
  if(exact){
    const normMatches=options.filter(o=>norm(o.value)===target);
    if(normMatches.length>1 && !preferred && !explicitProvider && !rawModel.includes('/')){
      return null;  // ambiguous bare id ‚Äî caller must inject the correct option
    }
    return exact;
  }
  // If the request is provider-qualified (either explicit @provider:model or
  // a slash-qualified vendor/model id), do NOT fuzzy-match a sibling model
  // once exact/provider-aware lookup failed. Returning null lets the caller
  // preserve the raw typed value instead of snapping to the closest catalog
  // entry. This keeps uncatalogued models routable instead of silently turning
  // them into a nearby curated sibling.
  if(rawModel.startsWith('@')||rawModel.includes('/')) return null;
  // 3. Prefix/substring: require the candidate to start with the FULL normalized target
  // (not a truncated base). This avoids false matches like gpt.5.5 ‚Üí gpt.5.4.mini (#1188).
  // Only fall back to the shorter base form if target itself is very short (a bare root
  // like "gpt" or "claude") where stripping would be a no-op anyway.
  const base=target.replace(/\.\d+$/,'');  // strip trailing version number
  const useBase=base.length<=4||base===target; // bare root ‚Äî stripping changed nothing meaningful
  const prefixTarget=useBase?base:target;
  // When the typed target is a COMPLETE versioned name (ends in a digit, e.g.
  // "mimo-v2.5" ‚Üí norm "mimo.v2.5"), a prefix hit on a longer option is only
  // legitimate if the extra text continues the VERSION ("." + digit, e.g.
  // mimo.v2 ‚Üí mimo.v2.5...). If the extra text is a variant/tier suffix
  // ("." + non-digit, e.g. mimo.v2.5.pro from "mimo-v2.5-pro"), the user asked
  // for the base model that simply isn't in the catalog ‚Äî do NOT silently snap
  // them to the -pro/-flash tier (and a different price tier). Let resolution
  // fall through to null so the caller reports no-match instead. (#3368)
  const targetEndsInVersion=/\d$/.test(target);
  const partial=opts.find(o=>{
    const no=norm(o);
    if(!no.startsWith(prefixTarget)) return false;
    if(targetEndsInVersion && no!==target){
      const rest=no.slice(target.length);
      // reject "." + non-digit (variant/tier suffix); allow "" or "." + digit (version continuation)
      if(rest && !/^\.\d/.test(rest)) return false;
    }
    return true;
  });
  return partial||null;
}

// Set the model picker to the best match for modelId.
// Returns the resolved value that was actually set, or null if nothing matched.
function _refreshOpenModelDropdown(){
  const dd=$('composerModelDropdown');
  if(dd&&dd.classList&&dd.classList.contains('open')&&typeof renderModelDropdown==='function'){
    renderModelDropdown();
    if(typeof _positionModelDropdown==='function') _positionModelDropdown();
  }
  const sdd=$('settingsModelDropdown');
  if(sdd&&sdd.classList&&sdd.classList.contains('open')&&typeof renderModelDropdown==='function'){
    // Re-rendering the OPEN settings picker (e.g. when a late live-model fetch
    // resolves) must not re-grab search focus on touch ‚Äî same coarse-pointer rule
    // as openSettingsModelDropdown, or the mobile keyboard pops after opening.
    const _coarsePointer=(typeof window.matchMedia==='function')&&window.matchMedia('(pointer: coarse)').matches;
    renderModelDropdown({
      dropdownId:'settingsModelDropdown',
      selectId:'settingsModel',
      forceOpenKey:'settingsModel',
      closeDropdown:closeSettingsModelDropdown,
      selectModel:selectSettingsModelFromDropdown,
      scopeNoteText:t('settings_desc_model')||'Used for new conversations. Existing conversations keep their selected model.',
      autoFocusSearch:!_coarsePointer,
    });
  }
}
function _applyModelToDropdown(modelId, sel, preferredProviderId, opts){
  if(!modelId||!sel) return null;
  const isRichPickerSelect=sel.id==='modelSelect'||sel.id==='settingsModel';
  const currentState=(isRichPickerSelect&&typeof _modelStateForSelect==='function')
    ? _modelStateForSelect(sel, sel.value)
    : null;
  const resolved=_findModelInDropdown(modelId,sel,preferredProviderId);
  if(resolved){
    sel.value=resolved;
    const preferredProvider=String(preferredProviderId||'').trim().toLowerCase();
    if(preferredProvider&&sel.options){
      // Assigning select.value picks the first duplicate value. Restore the
      // provider-specific option that the caller matched (#6131).
      const preferredOption=Array.from(sel.options).find(o=>
        String(o.value||'')===String(resolved)
        && String(_getOptionProviderId(o)||'').trim().toLowerCase()===preferredProvider
      );
      if(preferredOption) preferredOption.selected=true;
    }
    if(isRichPickerSelect){
      const resolvedState=typeof _modelStateForSelect==='function'
        ? _modelStateForSelect(sel, resolved)
        : {model:resolved,model_provider:preferredProviderId||null};
      const pickerChanged= !!(opts&&opts.forceRefresh) || !currentState
        || String(currentState.model||'')!==String(resolvedState.model||'')
        || String(currentState.model_provider||'')!==String(resolvedState.model_provider||'');
      if(sel.id==='modelSelect'&&typeof syncModelChip==='function') syncModelChip();
      if(sel.id==='settingsModel'&&typeof syncSettingsModelChip==='function') syncSettingsModelChip();
      if(pickerChanged) _refreshOpenModelDropdown();
    }
    return resolved;
  }
  return null;
}
function _ensureModelOptionInDropdown(modelId, sel, preferredProviderId){
  if(!modelId||!sel) return null;
  if(typeof _deduplicateModelPickerOptions==='function') _deduplicateModelPickerOptions(sel,sel.value);
  const requestedProvider=String(preferredProviderId||_providerFromModelValue(modelId)||'').trim();
  const applied=_applyModelToDropdown(modelId,sel,requestedProvider||null);
  if(applied){
    const appliedState=typeof _modelStateForSelect==='function'
      ?_modelStateForSelect(sel,applied)
      :{model:applied,model_provider:null};
    if(!requestedProvider||String(appliedState&&appliedState.model_provider||'').toLowerCase()===requestedProvider.toLowerCase()) return applied;
  }
  const explicitPrefix=requestedProvider?`@${requestedProvider}:`:'';
  const rawModel=String(modelId||'');
  const bareModel=explicitPrefix&&rawModel.toLowerCase().startsWith(explicitPrefix.toLowerCase())
    ?rawModel.slice(explicitPrefix.length)
    :rawModel;
  const value=requestedProvider?`${explicitPrefix}${bareModel}`:rawModel;
  const opt=document.createElement('option');
  opt.value=value;
  opt.textContent=typeof getModelLabel==='function'?getModelLabel(modelId):modelId;
  opt.dataset.custom='1';
  const badge=(window._configuredModelBadges||{})[value];
  const rawBadge=(window._configuredModelBadges||{})[rawModel];
  if(badge&&badge.provider) opt.dataset.provider=badge.provider;
  if(rawBadge&&rawBadge.provider) opt.dataset.provider=rawBadge.provider;
  if(requestedProvider) opt.dataset.model=bareModel;
  const provider=requestedProvider||(badge&&badge.provider)||(rawBadge&&rawBadge.provider)||_providerFromModelValue(value)||'';
  if(provider) opt.dataset.provider=provider;
  sel.appendChild(opt);
  sel.value=value;
  if(sel.id==='modelSelect'){
    if(typeof syncModelChip==='function') syncModelChip();
    _refreshOpenModelDropdown();
  }
  if(sel.id==='settingsModel'){
    if(typeof syncSettingsModelChip==='function') syncSettingsModelChip();
    _refreshOpenModelDropdown();
  }
  return value;
}
function _modelStateFromAppliedDropdown(sel, modelValue){
  const state=(typeof _modelStateForSelect==='function')
    ? _modelStateForSelect(sel,modelValue)
    : {model:modelValue,model_provider:null};
  return {model:state.model||modelValue,model_provider:state.model_provider||null};
}
function _persistSessionModelCorrection(model, provider, opts){
  if(!S.session) return;
  const request=fetch(new URL('api/session/update',document.baseURI||location.href).href,{
    method:'POST',credentials:'include',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({session_id:S.session.id||S.session.session_id,model:model,model_provider:provider||null})
  });
  return opts&&opts.propagateErrors ? request : request.catch(()=>{});
}
let _modelDropdownRequestSeq=0;
let _modelCatalogFallbackRetried=false;

function _applySessionModelFallback(sel){
  if(!sel) return null;
  const configuredDefault=String(window._defaultModel||'').trim();
  if(configuredDefault){
    const appliedDefault=_applyModelToDropdown(configuredDefault,sel,window._activeProvider||null);
    if(appliedDefault) return _modelStateFromAppliedDropdown(sel,appliedDefault);
  }
  const first=sel.querySelector('optgroup > option, option');
  if(first){
    sel.value=first.value;
    if(sel.id==='modelSelect'){
      if(typeof syncModelChip==='function') syncModelChip();
      _refreshOpenModelDropdown();
    }
    return _modelStateFromAppliedDropdown(sel,first.value);
  }
  return null;
}

async function populateModelDropdown(opts={}){
  const sel=$('modelSelect');
  if(!sel) return;
  // `_activeProvider` is refreshed from the /api/models response below.
  if(typeof _modelDropdownRequestSeq!=='number') _modelDropdownRequestSeq=0;
  if(typeof _modelCatalogFallbackRetried!=='boolean') _modelCatalogFallbackRetried=false;
  const requestSeq=++_modelDropdownRequestSeq;
  try{
    const modelsUrl=new URL('api/models',document.baseURI||location.href);
    const requestedFreshness=opts&&opts.freshness?String(opts.freshness):'';
    if(opts&&opts.freshness) modelsUrl.searchParams.set('freshness',opts.freshness);
    const _modelsRes=await fetch(modelsUrl.href,{credentials:'include'});
    if(requestSeq!==_modelDropdownRequestSeq) return;
    const customRedirectIfUnauth=opts&&typeof opts.redirectIfUnauth==='function'?opts.redirectIfUnauth:null;
    if(customRedirectIfUnauth){
      if(customRedirectIfUnauth(_modelsRes)) return;
    }else if(_redirectIfUnauth(_modelsRes)) return;
    // `_activeProvider` is populated from the /api/models payload below.
    const data=await _modelsRes.json();
    if(requestSeq!==_modelDropdownRequestSeq) return;
    window._activeProvider=data.active_provider||null;
    window._defaultModel=data.default_model||null;
    window._configuredModelBadges=data.configured_model_badges||{};
    window._modelEndpointErrors={};
    // Keep g.extra_models label hydration in this function for /model and tail selections.

    const _synthGroupsFromConfigured=()=>{
      const badgeMap=window._configuredModelBadges||{};
      const grouped=new Map();
      const addModel=(providerId,modelId)=>{
        const pid=String(providerId||'configured').trim()||'configured';
        const mid=String(modelId||'').trim();
        if(!mid) return;
        if(!grouped.has(pid)) grouped.set(pid,[]);
        const arr=grouped.get(pid);
        if(arr.some(m=>m.id===mid)) return;
        arr.push({id:mid,label:getModelLabel(mid)});
      };

      for(const [modelId,badge] of Object.entries(badgeMap)){
        const mid=String(modelId||'').trim();
        // Prefer canonical IDs only; skip derived aliases such as
        // @provider:model and provider/model to avoid noisy duplicates.
        if(!mid||mid.startsWith('@')||mid.includes('/')) continue;
        const provider=(badge&&badge.provider)||'configured';
        addModel(provider,mid);
      }

      if(grouped.size===0&&data&&data.default_model){
        addModel(data.active_provider||'configured',data.default_model);
      }

      const groups=[];
      for(const [providerId,models] of grouped.entries()){
        const display=(String(providerId).startsWith('custom:')
          ? String(providerId).slice('custom:'.length)
          : String(providerId))||'Configured';
        groups.push({provider:display,provider_id:providerId,models});
      }
      return groups;
    };

    const usedConfiguredFallback=!(Array.isArray(data.groups)&&data.groups.length);
    const groups=usedConfiguredFallback
      ? _synthGroupsFromConfigured()
      : data.groups;
    const willRetry=usedConfiguredFallback && requestedFreshness!=='session_visit' && !_modelCatalogFallbackRetried;

    if(!groups.length){
      if(willRetry){
        _modelCatalogFallbackRetried=true;
        populateModelDropdown({...opts,freshness:'session_visit'}).catch(()=>{});
      }
      return; // no server groups and no configured fallback
    }
    const previousSelection=_captureModelDropdownSelection(sel);
    // Clear existing options
    sel.innerHTML='';
    _dynamicModelLabels={};
    for(const g of groups){
      const og=document.createElement('optgroup');
      og.label=g.provider;
      if(g.provider_id) og.dataset.provider=g.provider_id;
      if(g.models_endpoint_error){
        const errorKey=g.provider_id||g.provider||'';
        og.dataset.modelsEndpointError=JSON.stringify(g.models_endpoint_error);
        if(errorKey) window._modelEndpointErrors[errorKey]=g.models_endpoint_error;
      }
      for(const m of (Array.isArray(g.models)?g.models:[])){
        const opt=document.createElement('option');
        opt.value=m.id;
        opt.textContent=m.label;
        if(m && (m.supports_fast_tier === true || String(m.supports_fast_tier).toLowerCase()==='true')){
          opt.dataset.fast='1';
        }else if(m && (m.supports_fast_tier === false || String(m.supports_fast_tier).toLowerCase()==='false')){
          opt.dataset.fast='0';
        }
        og.appendChild(opt);
        _dynamicModelLabels[m.id]=m.label||m.id;
      }
      // Hydrate the label map from extra_models too (the catalog tail that
      // doesn't render as <option> entries when the picker is capped ‚Äî see
      // _build_nous_featured_set in api/config.py for the rationale). This
      // keeps a model selected from the slash-command autocomplete or a
      // persisted-localStorage value renderable with its proper label
      // instead of falling back to the bare ID. #1567.
      if(Array.isArray(g.extra_models)){
        try{ og.dataset.extraModels=JSON.stringify(g.extra_models); }catch(_e){ og.dataset.extraModels='[]'; }
        for(const m of g.extra_models){
          if(m && m.id) _dynamicModelLabels[m.id]=m.label||m.id;
        }
      }
      sel.appendChild(og);
    }
    if(typeof _deduplicateModelPickerOptions==='function'){
      _deduplicateModelPickerOptions(sel,previousSelection&&previousSelection.model||'');
    }
    _reconcileModelDropdownSelection(sel,data,previousSelection,opts);
    if(typeof syncModelChip==='function') syncModelChip();
    const dd=$('composerModelDropdown');
    if(dd&&dd.classList.contains('open')&&typeof renderModelDropdown==='function'){
      renderModelDropdown();
      _positionModelDropdown();
    }
    // Kick off a background live-model fetch for the active provider.
    // This runs after the static list is already shown (no blocking flicker).
    if(data.active_provider && !willRetry) _fetchLiveModels(data.active_provider, sel, requestSeq);
    if(willRetry){
      _modelCatalogFallbackRetried=true;
      populateModelDropdown({...opts,freshness:'session_visit'}).catch(()=>{});
    }
  }catch(e){
    if(requestSeq!==_modelDropdownRequestSeq) return;
    // API unavailable -- keep the hardcoded HTML options as fallback
    console.warn('Failed to load models from server:',e.message);
    if(typeof syncModelChip==='function') syncModelChip();
  }
}

// Cache so we don't re-fetch on every page load
const _liveModelCache={};
// Tracks providers for which a live-model fetch is in flight.
// Used by syncTopbar() to defer model corrections until the fetch completes,
// preventing premature fallback to the first static model (#1169).
const _liveModelFetchPending=new Set();

function _addLiveModelsToSelect(provider, models, sel){
  if(!provider||!models||!models.length||!sel) return 0;
  const currentVal=sel.value;
  let providerGroup=null;
  for(const og of sel.querySelectorAll('optgroup')){
    if(og.dataset.provider&&og.dataset.provider===provider){
      providerGroup=og; break;
    }
    if(og.label&&og.label.toLowerCase().includes(provider.toLowerCase())){
      providerGroup=og; break;
    }
  }
  if(!providerGroup){
    providerGroup=document.createElement('optgroup');
    providerGroup.label=provider.charAt(0).toUpperCase()+provider.slice(1)+' (live)';
    providerGroup.dataset.provider=provider;
    sel.appendChild(providerGroup);
  }else if(!providerGroup.dataset.provider){
    providerGroup.dataset.provider=provider;
  }
  const existingIds=new Set([...sel.options].map(o=>o.value));
  const _ap=(window._activeProvider||'').toLowerCase();
  const _providerLower=String(provider||'').toLowerCase();
  const _isNamedCustomActiveProvider=_ap.startsWith('custom:');
  const _isPortalFetch=_ap && _ap!=='openrouter' && _ap!=='custom' && _ap!=='openai-codex' && (_providerLower===_ap||_isNamedCustomActiveProvider&&_providerLower===_ap);
  // Keep existingNorm.has( within the #907 source slice.
  const optionIdentity=typeof _modelPickerOptionIdentity==='function'
    ? (modelId,providerId)=>_modelPickerOptionIdentity(modelId,providerId)
    : (modelId,providerId)=>{
        let value=String(modelId||'');
        const provider=String(providerId||'').trim();
      if(value.startsWith('@')&&value.includes(':')){
        const exactPrefix=provider ? `@${provider}:` : '';
        if(exactPrefix && value.toLowerCase().startsWith(exactPrefix.toLowerCase())){
          value=value.substring(exactPrefix.length);
        }else if(value.startsWith('@custom:')){
          const namedProvider=value.substring('@custom:'.length);
          const splitAt=namedProvider.indexOf(':');
          value=splitAt>=0 ? namedProvider.substring(splitAt+1) : namedProvider;
        }else{
          value=value.substring(value.indexOf(':')+1);
        }
      }
        return value.split('/').pop().replace(/-/g,'.').toLowerCase();
      };
  const existingNorm=new Set([...sel.options].map(o=>optionIdentity(o.value,_getOptionProviderId(o))));
  let added=0;
  for(const m of models){
    let mid=m.id;
    if(_isPortalFetch && !mid.startsWith('@')){
      mid=`@${provider}:${mid}`;
    }
    if(existingIds.has(mid)) continue;
    const identity=optionIdentity(mid,provider);
    if(existingNorm.has(identity)){
      const sameGroup=Array.from(providerGroup.children||[]).find(o=>optionIdentity(o.value,_getOptionProviderId(o))===identity);
      if(sameGroup){
        const incomingRoutable=String(mid).startsWith('@');
        const existingRoutable=String(sameGroup.value||'').startsWith('@');
        if(!(!existingRoutable&&incomingRoutable)) continue; // let proxy replace catalog twin
      }
    }
    const opt=document.createElement('option');
    opt.value=mid;
    opt.textContent=m.label||m.id;
    opt.title='Live model ‚Äî fetched from provider';
    opt.dataset.provider=provider;
    if(m && (m.supports_fast_tier === true || String(m.supports_fast_tier).toLowerCase()==='true')){
      opt.dataset.fast='1';
    }else if(m && (m.supports_fast_tier === false || String(m.supports_fast_tier).toLowerCase()==='false')){
      opt.dataset.fast='0';
    }
    providerGroup.appendChild(opt);
    existingIds.add(mid);
    existingNorm.add(identity);
    _dynamicModelLabels[mid]=m.label||m.id;
    added++;
  }
  if(typeof _deduplicateModelPickerOptions==='function') _deduplicateModelPickerOptions(sel,currentVal);
  const currentState=(currentVal&&typeof _modelStateForSelect==='function')
    ? _modelStateForSelect(sel, currentVal)
    : {model:currentVal||'', model_provider:(S.session&&S.session.model_provider)||null};
  const currentProvider=currentState&&currentState.model_provider||null;
  if(added>0 && currentVal) _applyModelToDropdown(currentVal, sel, currentProvider, {forceRefresh:true});
  // After live models are added, re-apply the session's model in case it was
  // absent from the static list and syncTopbar() fired before the live fetch
  // completed (#1169). This ensures the session model wins over any premature
  // fallback that may have set sel.value to the first available option.
  if(S.session && S.session.model && sel.id==='modelSelect'){
    const sessionProvider=S.session.model_provider||null;
    const sessionAlreadyRefreshed=added>0 && currentVal
      && String((currentState&&currentState.model)||'')===String(S.session.model||'')
      && String((currentState&&currentState.model_provider)||'')===String(sessionProvider||'');
    const reapplied=_applyModelToDropdown(S.session.model, sel, sessionProvider, {forceRefresh:added>0&&!sessionAlreadyRefreshed});
    if(reapplied && typeof syncModelChip==='function') syncModelChip();
  }
  return added;
}

async function _fetchLiveModels(provider, sel, requestSeq=null){
  if(!provider||!sel) return;
  if(requestSeq!==null&&requestSeq!==_modelDropdownRequestSeq) return;
  // Already fetched ‚Äî apply cached models to this select element (#872)
  if(_liveModelCache[provider]){
    if(requestSeq!==null&&requestSeq!==_modelDropdownRequestSeq) return;
    const added=_addLiveModelsToSelect(provider,_liveModelCache[provider],sel);
    if(added>0 && typeof syncModelChip==='function') syncModelChip();
    return;
  }
  _liveModelFetchPending.add(provider);
  try{
    const url=new URL('api/models/live',document.baseURI||location.href);
    url.searchParams.set('provider',provider);
    const _liveRes=await fetch(url.href,{credentials:'include'});
    if(requestSeq!==null&&requestSeq!==_modelDropdownRequestSeq) return;
    if(_redirectIfUnauth(_liveRes)) return;
    const data=await _liveRes.json();
    if(requestSeq!==null&&requestSeq!==_modelDropdownRequestSeq) return;
    if(!data.models||!data.models.length) return;
    _liveModelCache[provider]=data.models;
    if(requestSeq!==null&&requestSeq!==_modelDropdownRequestSeq) return;
    const added=_addLiveModelsToSelect(provider,data.models,sel);
    if(added>0){
      if(typeof syncModelChip==='function') syncModelChip();
      console.debug('[hermes] Live models loaded for',provider+':',added,'new models added');
    }
  }catch(e){
    console.debug('[hermes] Live model fetch failed for',provider,e.message);
  }finally{
    _liveModelFetchPending.delete(provider);
  }
}

/**
 * Check if the given model ID belongs to a different provider than the one
 * currently configured in Hermes. Returns a warning string if mismatched,
 * or null if the selection looks compatible.
 *
 * Provider detection is intentionally loose ‚Äî we compare the model's slash
 * prefix (e.g. "openai/" from "openai/gpt-4o") against the active provider
 * name. Custom/local endpoints report active_provider='custom', a named
 * custom provider such as 'custom:zenmux', or the base_url hostname; skip the
 * check for those values to avoid false positives.
 */
function _checkProviderMismatch(modelId){
  const ap=(window._activeProvider||'').toLowerCase();
  if(_providerSkipsModelMismatchWarning(ap)) return null; // can't reliably check
  // @provider: prefixed IDs came from that provider's live model list ‚Äî no mismatch possible
  if(modelId.startsWith('@')) return null;
  const slash=modelId.indexOf('/');
  if(slash<0) return null; // bare model name, no provider prefix
  const modelProvider=modelId.substring(0,slash).toLowerCase();
  // Normalise common aliases
  const aliases={'claude':'anthropic','gpt':'openai','gemini':'google'};
  const norm=p=>aliases[p]||p;
  if(norm(modelProvider)!==norm(ap)){
    return (window.t?window.t('provider_mismatch_warning',modelId,ap):
      `"${modelId}" may not work with your configured provider (${ap}). Send anyway or run \`hermes model\` to switch.`);
  }
  return null;
}

function _selectedModelOption(){
  const sel=$('modelSelect');
  if(!sel) return null;
  return sel.options[sel.selectedIndex]||null;
}

function _normalizeConfiguredModelKey(modelId){
  let s=String(modelId||'').trim().toLowerCase();
  let strippedAtProvider=false;
  // Strip @provider: prefix (e.g., @custom:jingdong:GLM-5 -> jingdong:GLM-5).
  // Defensive: trailing-colon / trailing-slash falls back to the original key
  // so malformed configs don't collapse distinct ids to '' (matches backend _norm_model_id).
  if(s.startsWith('@')&&s.includes(':')){const ci=s.indexOf(':',1);const cand=s.slice(ci+1);strippedAtProvider=!!cand;s=cand||s;}
  // Skip slash-based stripping for URI-scheme IDs (e.g. gpt://folder/model)
  // whose slashes are path separators, not provider delimiters (#3429).
  const _hasScheme=/^[a-z][a-z0-9+.-]*:\/\//i.test(s);
  if(!_hasScheme){
    // Strip provider-qualified prefixes that contain colons before the first
    // slash (e.g. 'custom:llm-proxy/model' ‚Üí 'model').  Without this, badge-
    // key variants like 'custom:llm-proxy/opencode_go/deepseek-v4-pro' and the
    // bare 'opencode_go/deepseek-v4-pro' produce different normalized keys and
    // aren't deduped in the configured section (#3360).
    if(!strippedAtProvider&&s.includes('/')&&s.indexOf(':')!==-1&&s.indexOf(':')<s.indexOf('/')){
      s=s.slice(s.indexOf('/')+1)||s;
    }
    // Strip only the first slash-segment (provider prefix), preserving any
    // remaining vendor hierarchy. Using split('/').pop() here previously
    // discarded ALL segments except the last, collapsing distinct multi-slash
    // IDs like 'vendor_a/deepseek-v4-pro' and 'vendor_b/deepseek/deepseek-v4-pro'
    // to the same key, causing badge misattribution and configured-entry
    // suppression (#3360).
    if(s.includes('/')) s=s.replace(/^[^/]+\//, '')||s;
  }
  return s.replace(/-/g,'.');
}

function _isEquivalentConfiguredModelEntry(modelId,badge,entries){
  const normalized=_normalizeConfiguredModelKey(modelId);
  const provider=String(badge&&badge.provider||'').toLowerCase();
  const matchingEntries=(entries||[]).filter(existing=>
    _normalizeConfiguredModelKey(existing.value)===normalized
  );
  if(matchingEntries.some(existing=>{
    const entryProvider=String(existing.providerId||'').toLowerCase();
    return !provider||!entryProvider||entryProvider===provider;
  })) return true;
  // @provider:model is an equivalent routing spelling only when an existing
  // picker row belongs to that same provider. This supports named custom
  // providers (@custom:name:model) without collapsing matching model IDs from
  // different providers.
  const rawId=String(modelId||'');
  // `<provider>/<model>` is another routing spelling of `<model>`, so its badge
  // key must not become a second picker row. configured_model_badges holds every
  // spelling of a configured model and renderModelDropdown() synthesises a row
  // for each key this predicate does not recognise. The provider-qualified
  // spelling was missed because _normalizeConfiguredModelKey() strips only one
  // leading slash segment (#3360 keeps `vendor_a/x` and `vendor_b/y/x` distinct),
  // so `acme/example-model` and `custom/acme/example-model` normalise to
  // different keys and the picker lists one model twice.
  // Match it the way the `@provider:` rule below does: the badge declares a
  // provider, the key starts with that provider's `<provider>/` prefix, and an
  // existing row from the same provider normalises equal to the remainder. Two
  // different models never satisfy the last clause, so this can only drop a
  // duplicate of a row the catalog already produced.
  const slashPrefix=provider?`${provider}/`:'';
  if(slashPrefix&&rawId.toLowerCase().startsWith(slashPrefix)){
    const slashRoutedId=rawId.slice(slashPrefix.length);
    if(slashRoutedId&&(entries||[]).some(entry=>
      String(entry.providerId||'').toLowerCase()===provider
      &&_normalizeConfiguredModelKey(entry.value)===_normalizeConfiguredModelKey(slashRoutedId)
    )) return true;
  }
  const prefix=provider?`@${provider}:`:'';
  if(!prefix||!rawId.toLowerCase().startsWith(prefix)) return false;
  const routedId=rawId.slice(prefix.length);
  return (entries||[]).some(entry=>
    String(entry.providerId||'').toLowerCase()===provider
    &&_normalizeConfiguredModelKey(entry.value)===_normalizeConfiguredModelKey(routedId)
  );
}

function _getConfiguredModelBadge(modelId,badgeMap,providerId){
  const map=badgeMap||window._configuredModelBadges||{};
  if(!modelId||!map) return null;
  const provider=String(providerId||'').toLowerCase();
  const exact=map[modelId];
  if(exact && (!provider || !exact.provider || String(exact.provider).toLowerCase()===provider)) return exact;
  const targetNorm=_normalizeConfiguredModelKey(modelId);
  const matches=[];
  for(const [candidate,badge] of Object.entries(map)){
    if(_normalizeConfiguredModelKey(candidate)===targetNorm) matches.push(badge);
  }
  if(!matches.length) return null;
  if(provider){
    const providerMatch=matches.find(badge=>String(badge&&badge.provider||'').toLowerCase()===provider);
    if(providerMatch) return providerMatch;
    return matches.length===1 ? matches[0] : null;
  }
  return matches[0];
}

function _compactComposerModelChipLabel(modelId,labelText){
  const id=String(modelId||'').trim();
  const raw=String(labelText||'').trim();
  if(!raw) return getModelLabel(id);
  const idLower=id.toLowerCase();
  const rawLower=raw.toLowerCase();
  const slash=id.indexOf('/');
  if(slash>0){
    const provider=id.slice(0,slash).toLowerCase();
    if(rawLower.startsWith(provider+'/')){
      return raw.slice(provider.length+1).trim();
    }
  }
  if(id&&rawLower===idLower&&raw.includes('/')){
    return raw.slice(raw.indexOf('/')+1).trim();
  }
  if(raw.includes('/') && !/^[a-z][a-z0-9+.-]*:\/\//i.test(raw)){
    const parts=raw.split('/').map(s=>s.trim()).filter(Boolean);
    if(parts.length>=2){
      const tail=parts[parts.length-1];
      const tailLower=tail.toLowerCase();
      if(idLower && (tailLower===idLower || idLower.endsWith('/'+tailLower))) return tail;
      if(parts.length===2){
        const leadLower=parts[0].toLowerCase();
        if(tailLower.startsWith(leadLower+'-')) return tail;
      }
    }
  }
  return raw;
}

function syncModelChip(){
  const sel=$('modelSelect');
  const chip=$('composerModelChip');
  const label=$('composerModelLabel');
  const mobileLabel=$('composerMobileModelLabel');
  const mobileAction=$('composerMobileModelAction');
  const dd=$('composerModelDropdown');
  if(!sel||!chip||!label) return;
  // Don't show a model label until boot has finished loading to prevent flash of wrong default
  if(!S._bootReady){
    label.textContent='';
    if(mobileLabel) mobileLabel.textContent='';
    chip.title='Conversation model';
    return;
  }
  const opt=_selectedModelOption();
  const text=opt?opt.textContent:getModelLabel(sel.value||'');
  const compactText=_compactComposerModelChipLabel(sel.value||'', text);
  const gatewayRouting=_latestGatewayRoutingForSession(S.session);
  const displayText=_formatGatewayModelLabel(sel.value||'',compactText,gatewayRouting)||compactText;
  label.textContent=displayText;
  if(mobileLabel) mobileLabel.textContent=displayText;
  chip.title=gatewayRouting?`${sel.value||'Conversation model'} ${_gatewayRoutingLabel(gatewayRouting)}`:(sel.value||'Conversation model');
  chip.classList.toggle('active',!!(dd&&dd.classList.contains('open')));
  if(mobileAction) mobileAction.classList.toggle('active',!!(dd&&dd.classList.contains('open')));
}

// Remembers where #composerModelDropdown lives in the composer-footer so the
// phone path can move it to <body> and put it back exactly. Captured lazily on
// the first reparent (see _positionModelDropdown phone branch).
let _modelDropdownHome=null;

// Return the model dropdown into its original .composer-footer slot and clear
// every inline style the phone path wrote, so the desktop CSS (position:absolute
// anchored on the relatively-positioned .composer-footer) fully governs again.
// Safe to call when the element never moved ‚Äî it just no-ops the reinsert.
function _restoreModelDropdownHome(){
  const dd=document.getElementById('composerModelDropdown');
  if(!dd) return;
  dd.classList.remove('model-dropdown--floating');
  dd.style.left='';
  dd.style.top='';
  dd.style.bottom='';
  dd.style.width='';
  dd.style.maxWidth='';
  dd.style.maxHeight='';
  if(_modelDropdownHome&&_modelDropdownHome.parent&&dd.parentNode!==_modelDropdownHome.parent){
    const ref=_modelDropdownHome.nextSibling;
    if(ref&&ref.parentNode===_modelDropdownHome.parent){
      _modelDropdownHome.parent.insertBefore(dd,ref);
    }else{
      _modelDropdownHome.parent.appendChild(dd);
    }
  }
}

function _positionModelDropdown(){
  const dd=$('composerModelDropdown');
  const chip=$('composerModelChip');
  const mobileAction=$('composerMobileModelAction');
  const footer=document.querySelector('.composer-footer');
  if(!dd||!footer) return;
  const panel=$('composerMobileConfigPanel');
  const anchor=(panel&&panel.classList.contains('open')&&mobileAction)?mobileAction:(chip&&chip.offsetParent?chip:mobileAction);
  if(!anchor) return;
  const isPhone=typeof window.matchMedia==='function'&&window.matchMedia('(max-width:640px)').matches;
  if(isPhone){
    // #6080: .composer-footer sets container-type:inline-size (and a
    // backdrop-filter under the Geist Contrast skin) ‚Äî both establish a fixed
    // containing block, so a position:fixed dropdown left inside the footer
    // resolves against the FOOTER (bottom of screen) instead of the viewport
    // and lands below the fold. Reparent to <body> ‚Äî exactly the working
    // #profileDropdown idiom ‚Äî so position:fixed is viewport-relative on ALL
    // skins, then compute coordinates against the visual viewport.
    if(!_modelDropdownHome){
      _modelDropdownHome={parent:dd.parentNode,nextSibling:dd.nextSibling};
    }
    if(dd.parentNode!==document.body) document.body.appendChild(dd);
    dd.classList.add('model-dropdown--floating');
    const anchorRect=anchor.getBoundingClientRect();
    const visualViewport=window.visualViewport;
    const viewportWidth=Math.max(1,Number(visualViewport&&visualViewport.width)||window.innerWidth||1);
    const viewportHeight=Math.max(1,Number(visualViewport&&visualViewport.height)||window.innerHeight||1);
    const viewportTop=Math.max(0,Number(visualViewport&&visualViewport.offsetTop)||0);
    const viewportBottom=viewportTop+viewportHeight;
    const margin=8;
    const gap=6;
    const viewportLeft=Math.max(0,Number(visualViewport&&visualViewport.offsetLeft)||0);
    const viewportRight=viewportLeft+viewportWidth;
    const titlebar=document.querySelector('.app-titlebar');
    const titlebarBottom=titlebar&&typeof titlebar.getBoundingClientRect==='function'
      ? Number(titlebar.getBoundingClientRect().bottom)||0
      : 0;
    const contentTop=Math.max(viewportTop+margin,titlebarBottom+margin);
    const menuWidth=Math.max(1,viewportWidth-margin*2);
    const left=Math.max(viewportLeft+margin,Math.min(anchorRect.left,viewportRight-menuWidth-margin));
    dd.style.left=`${left}px`;
    dd.style.width=`${menuWidth}px`;
    dd.style.maxWidth=`${menuWidth}px`;
    dd.style.bottom='auto';
    const menuHeight=Math.max(dd.scrollHeight,dd.offsetHeight);
    const aboveSpace=Math.max(0,anchorRect.top-contentTop-gap-margin);
    const belowSpace=Math.max(0,viewportBottom-anchorRect.bottom-gap-margin);
    const openAbove=aboveSpace>=Math.min(menuHeight,belowSpace)||aboveSpace>=belowSpace;
    const availableHeight=Math.max(1,openAbove?aboveSpace:belowSpace);
    dd.style.maxHeight=`${availableHeight}px`;
    const visibleHeight=Math.min(menuHeight||availableHeight,availableHeight);
    const top=openAbove
      ? anchorRect.top-gap-visibleHeight
      : anchorRect.bottom+gap;
    dd.style.top=`${Math.max(contentTop,Math.min(top,viewportBottom-margin-visibleHeight))}px`;
    return;
  }
  // Desktop (>640px): keep the current master behaviour ‚Äî an absolutely
  // positioned .composer-footer child. Restore the element into the footer (in
  // case a prior phone open moved it to <body>) and clear the phone inline
  // styles so the desktop CSS anchor is byte-for-byte identical to master.
  _restoreModelDropdownHome();
  const anchorRect=anchor.getBoundingClientRect();
  const footerRect=footer.getBoundingClientRect();
  let left=anchorRect.left-footerRect.left;
  const maxLeft=Math.max(0, footer.clientWidth-dd.offsetWidth);
  left=Math.max(0, Math.min(left, maxLeft));
  dd.style.left=`${left}px`;
}

function _readModelOverflowData(group){
  if(!group||!group.dataset||!group.dataset.extraModels) return [];
  try{
    const parsed=JSON.parse(group.dataset.extraModels);
    return Array.isArray(parsed)?parsed.filter(m=>m&&m.id):[];
  }catch(_e){
    return [];
  }
}

function _appendOverflowOptionsToGroup(group, extraModels){
  if(!group||!Array.isArray(extraModels)||!extraModels.length) return 0;
  // The selected model may already have been injected into the <select> (e.g. a
  // hidden overflow model picked from search via _ensureModelOptionInDropdown).
  // Appending it again here would create a duplicate row once the group expands,
  // so reuse/move any existing option with the same value instead of re-creating it. (#3691)
  const parentSelect=(group.parentNode&&group.parentNode.tagName==='SELECT')?group.parentNode:null;
  const existingByValue=new Map();
  if(parentSelect){
    for(const opt of Array.from(parentSelect.querySelectorAll('option'))){
      if(opt&&typeof opt.value==='string') existingByValue.set(opt.value,opt);
    }
  }
  let appended=0;
  for(const m of extraModels){
    if(!m||!m.id) continue;
    const existing=existingByValue.get(m.id);
    if(existing){
      // Move the already-present option into this group rather than duplicating it.
      if(existing.parentNode!==group) group.appendChild(existing);
      continue;
    }
    const opt=document.createElement('option');
    opt.value=m.id;
    opt.textContent=m.label||m.id;
    group.appendChild(opt);
    appended++;
  }
  if(group.dataset){
    group.dataset.extraModels='[]';
    group.dataset.overflowExpanded='1';
  }
  return appended;
}

function _mountSearchableModelSelect(opts={}){
  const root=opts.root;
  if(!root) return null;
  const choices=Array.isArray(opts.choices)
    ? opts.choices
      .map(choice=>choice&&choice.id?{id:String(choice.id),label:String(choice.label||choice.id)}:null)
      .filter(Boolean)
    : [];
  const selectedValue=String(opts.selectedValue||'');
  const onModelChange=typeof opts.onModelChange==='function' ? opts.onModelChange : ()=>{};
  const selectId=opts.selectId||'';
  const customInputId=opts.customInputId||'';
  const listedChoiceIds=new Set(choices.map(choice=>choice.id));
  const listedSelection=listedChoiceIds.has(selectedValue) ? selectedValue : '';
  const customSelection=listedSelection ? '' : selectedValue;
  let lastListedValue=listedSelection||(choices[0]?choices[0].id:'');
  root.innerHTML=
    `<div class="model-search-row">`+
      `<input class="model-search-input" type="text" placeholder="${esc(t('model_search_placeholder')||'Search models‚Ä¶')}" spellcheck="false" autocomplete="off">`+
      `<button class="model-search-clear" title="Clear search">${li('x',10)}</button>`+
    `</div>`+
    `<select ${selectId?`id="${esc(selectId)}"`:''}></select>`+
    `<div class="model-group model-custom-sep">${esc(t('model_custom_label')||'Custom model ID')}</div>`+
    `<div class="model-custom-row">`+
      `<input ${customInputId?`id="${esc(customInputId)}"`:''} class="model-custom-input" type="text" placeholder="${esc(t('model_custom_placeholder')||'e.g. openai/gpt-5.4')}" spellcheck="false" autocomplete="off">`+
      `<button class="model-custom-btn" title="Use this model">${li('plus',12)}</button>`+
    `</div>`;
  const searchInput=root.querySelector('.model-search-input');
  const clearButton=root.querySelector('.model-search-clear');
  const selectEl=selectId ? root.querySelector(`#${selectId}`) : root.querySelector('select');
  const customInput=customInputId ? root.querySelector(`#${customInputId}`) : root.querySelector('.model-custom-input');
  const customButton=root.querySelector('.model-custom-btn');
  if(!searchInput||!clearButton||!selectEl||!customInput||!customButton) return null;

  const noMatchesOption=document.createElement('option');
  noMatchesOption.value='';
  noMatchesOption.textContent='No matching models';
  noMatchesOption.disabled=true;
  noMatchesOption.hidden=true;
  selectEl.appendChild(noMatchesOption);

  for(const choice of choices){
    const option=document.createElement('option');
    option.value=choice.id;
    option.textContent=choice.label;
    selectEl.appendChild(option);
  }
  if(listedSelection){
    selectEl.value=listedSelection;
  }else if(customSelection){
    selectEl.selectedIndex=-1;
  }else if(choices.length){
    selectEl.value=choices[0].id;
    onModelChange(lastListedValue);
  }
  customInput.value=customSelection;

  const applyFilter=()=>{
    const needle=(searchInput.value||'').trim().toLowerCase();
    let visibleCount=0;
    for(const option of Array.from(selectEl.options)){
      if(option===noMatchesOption) continue;
      const haystack=`${option.textContent||''} ${option.value||''}`.toLowerCase();
      const visible=!needle||haystack.includes(needle);
      option.hidden=!visible;
      if(visible) visibleCount++;
    }
    noMatchesOption.hidden=visibleCount!==0;
  };

  const applyCustomSelection=()=>{
    onModelChange((customInput.value||'').trim());
  };

  searchInput.addEventListener('input', applyFilter);
  clearButton.addEventListener('click', ()=>{
    searchInput.value='';
    applyFilter();
    searchInput.focus();
  });
  selectEl.addEventListener('change', ()=>{
    customInput.value='';
    lastListedValue=selectEl.value||lastListedValue;
    onModelChange(lastListedValue);
  });
  customInput.addEventListener('input', ()=>{
    const value=(customInput.value||'').trim();
    if(value){
      selectEl.selectedIndex=-1;
      onModelChange(value);
      return;
    }
    customInput.value='';
    if(lastListedValue){
      selectEl.value=lastListedValue;
      onModelChange(lastListedValue);
      return;
    }
    onModelChange('');
  });
  customInput.addEventListener('keydown', (event)=>{
    if(event.key!=='Enter') return;
    event.preventDefault();
    applyCustomSelection();
  });
  customButton.addEventListener('click', (event)=>{
    event.preventDefault();
    applyCustomSelection();
  });
  applyFilter();
  return {searchInput,selectEl,customInput,customButton};
}

function renderModelDropdown(){
  const opts=arguments[0]||{};
  const dd=$(opts.dropdownId||'composerModelDropdown');
  const sel=$(opts.selectId||'modelSelect');
  if(!dd||!sel) return;
  if(typeof _deduplicateModelPickerOptions==='function') _deduplicateModelPickerOptions(sel,sel.value);
  // Whether the search input should auto-grab focus on (re-)render. Default true
  // preserves the composer picker's behavior exactly; the settings picker passes
  // false on coarse-pointer devices so opening it doesn't pop the mobile keyboard.
  const _autoFocusSearch=opts.autoFocusSearch!==false;
  const selectFromDropdown=typeof opts.selectModel==='function'
    ? opts.selectModel
    : (value,provider)=>selectModelFromDropdown(value,provider);
  const closeDropdown=typeof opts.closeDropdown==='function'
    ? opts.closeDropdown
    : closeModelDropdown;
  // Group(s) that must render OPEN even though they aren't the selected group ‚Äî
  // set when the user expands a group's overflow via "Show more" so a later full
  // re-render doesn't re-collapse it (_groupOpenState is rebuilt per render, so
  // this cross-render intent persists on a global). Resolved as a function-local
  // so renderModelDropdown stays self-contained when eval'd in isolation (the
  // #3691 node test driver evals the function body without module scope).
  const _forceOpenGroups=(()=>{
    const _g=(typeof window!=='undefined')?window:(typeof globalThis!=='undefined'?globalThis:{});
    const key=opts.forceOpenKey||'composer';
    if(!_g.__modelGroupForceOpenByPicker) _g.__modelGroupForceOpenByPicker={};
    if(!_g.__modelGroupForceOpenByPicker[key]) _g.__modelGroupForceOpenByPicker[key]=new Set();
    return _g.__modelGroupForceOpenByPicker[key];
  })();
  const _modelData=[];
  const _groupMeta=new Map();
  const _groupOrder=[];
  const _badgeMap=window._configuredModelBadges||{};
  const _ensureGroupMeta=(groupKey,groupLabel,providerId,optgroup)=>{
    if(!_groupMeta.has(groupKey)){
      _groupMeta.set(groupKey,{
        key:groupKey,
        label:groupLabel||'',
        providerId:providerId||'',
        optgroup:optgroup||null,
        modelsEndpointError:null,
        modelCount:0,
        hiddenCount:0,
        endpointErrorOnly:false,
      });
      _groupOrder.push(groupKey);
    }
    return _groupMeta.get(groupKey);
  };
  const _vendorPrefix=(rawId)=>{
    const stripped=String(rawId||'').replace(/^@([^:]+:)+/,'');
    const slash=stripped.indexOf('/');
    return slash>0?stripped.slice(0,slash):'';
  };
  const SUB_GROUP_PROVIDERS=new Set(['openrouter','nous']);
  const SUB_GROUP_MIN_MODELS=8;
  for(const child of Array.from(sel.children)){
    if(child.tagName==='OPTGROUP'){
      const providerId=child.dataset&&child.dataset.provider?child.dataset.provider:'';
      const groupKey=providerId||child.label||`group-${_groupOrder.length}`;
      const groupMeta=_ensureGroupMeta(groupKey,child.label||'',providerId,child);
      let modelsEndpointError=null;
      if(child.dataset&&child.dataset.modelsEndpointError){
        try{ modelsEndpointError=JSON.parse(child.dataset.modelsEndpointError); }catch(_e){ modelsEndpointError=null; }
      }
      groupMeta.modelsEndpointError=modelsEndpointError;
      for(const opt of Array.from(child.children)){
        const rawValue=String(opt.value||'');
        const displayName=rawValue.startsWith('@custom:')
          ? getModelLabel(rawValue)
          : (opt.textContent||getModelLabel(rawValue));
        const entry={value:opt.value,name:esc(displayName),id:esc(opt.value),group:child.label||'',groupKey,providerId,modelsEndpointError,badge:_getConfiguredModelBadge(opt.value,_badgeMap,providerId),hiddenByDefault:false};
        _modelData.push(entry);
        groupMeta.modelCount++;
      }
      for(const overflowModel of _readModelOverflowData(child)){
        const displayName=overflowModel.id.startsWith('@custom:')
          ? getModelLabel(overflowModel.id)
          : (overflowModel.label||getModelLabel(overflowModel.id));
        _modelData.push({
          value:overflowModel.id,
          name:esc(displayName),
          id:esc(overflowModel.id),
          group:child.label||'',
          groupKey,
          providerId,
          modelsEndpointError,
          badge:_getConfiguredModelBadge(overflowModel.id,_badgeMap,providerId),
          hiddenByDefault:true,
        });
        groupMeta.modelCount++;
        groupMeta.hiddenCount++;
      }
      if(modelsEndpointError && !child.children.length && !groupMeta.hiddenCount){
        groupMeta.endpointErrorOnly=true;
        _modelData.push({value:`__models_endpoint_error__:${providerId||child.label||''}`,name:'',id:'',group:child.label||'',groupKey,providerId,modelsEndpointError,endpointErrorOnly:true});
      }
    }
    if(child.tagName==='OPTION'){
      const groupKey='__ungrouped__';
      _ensureGroupMeta(groupKey,'','',null);
      const rawValue=String(child.value||'');
      const displayName=rawValue.startsWith('@custom:')
        ? getModelLabel(rawValue)
        : (child.textContent||getModelLabel(rawValue));
      _modelData.push({value:child.value,name:esc(displayName),id:esc(child.value),group:'',groupKey,providerId:'',badge:_getConfiguredModelBadge(child.value,_badgeMap),hiddenByDefault:false});
      _groupMeta.get(groupKey).modelCount++;
    }
  }
  for(const [modelId,badge] of Object.entries(_badgeMap)){
    if(_isEquivalentConfiguredModelEntry(modelId,badge,_modelData)) continue;
    _modelData.push({
      value:modelId,
      name:esc(getModelLabel(modelId)),
      id:esc(modelId),
      group:'',
      badge,
    });
  }
  // Create search input FIRST before filterModels definition
  const _scopeNote=document.createElement('div');
  _scopeNote.className='model-scope-note';
  _scopeNote.textContent=opts.scopeNoteText||(t('model_scope_advisory')||'Applies to this conversation from your next message.');
  const _searchRow=document.createElement('div');
  _searchRow.className='model-search-row';
  _searchRow.innerHTML=`<input class="model-search-input" type="text" placeholder="${esc(t('model_search_placeholder')||'Search models‚Ä¶')}" spellcheck="false" autocomplete="off"><button class="model-search-clear" title="Clear search">${li('x',10)}</button>`;
  const _si=_searchRow.querySelector('.model-search-input');
  const _sc=_searchRow.querySelector('.model-search-clear');
  // Create custom model section elements
  const _custSep=document.createElement('div');
  _custSep.className='model-group model-custom-sep';
  _custSep.textContent=t('model_custom_label')||'Custom model ID';
  const _custRow=document.createElement('div');
  _custRow.className='model-custom-row';
  _custRow.innerHTML=`<input class="model-custom-input" type="text" placeholder="${esc(t('model_custom_placeholder')||'e.g. openai/gpt-5.4')}" spellcheck="false" autocomplete="off"><button class="model-custom-btn" title="Use this model">${li('plus',12)}</button>`;
  const _ci=_custRow.querySelector('.model-custom-input');
  const _cb=_custRow.querySelector('.model-custom-btn');
  const _configuredRank=(badge)=>{
    if(!badge) return Number.POSITIVE_INFINITY;
    if(badge.role==='primary') return 0;
    if(badge.role==='fallback'){
      const m=String(badge.label||'').match(/fallback\s+(\d+)/i);
      return m?Number(m[1]):999;
    }
    return 500;
  };
  const _selectedModelState=(typeof _modelStateForSelect==='function')?_modelStateForSelect(sel,sel.value):{model:sel&&sel.value||'',model_provider:null};
  const _modelProviderForSelectedBadge=(m)=>{
    const _provider=String((m&&m.providerId)||(m&&m.badge&&m.badge.provider)||((typeof _providerFromModelValue==='function')?_providerFromModelValue(m&&m.value):'')||'').trim();
    return (_provider&&_provider!=='default')?_provider:null;
  };
  const _isSelectedModelRow=(m)=>{
    const _rowModel=String((m&&m.value)||'');
    const _rowProvider=String(_modelProviderForSelectedBadge(m)||'');
    const _stateModel=String((_selectedModelState&&_selectedModelState.model)||(sel&&sel.value)||'');
    const _stateProvider=String((_selectedModelState&&_selectedModelState.model_provider)||'');
    // Normalize both sides to the same model/provider identity. Catalog rows
    // carry the qualified @custom:<slug>:<model> value while the outgoing
    // state model is bare (#6884) ‚Äî a raw string comparison would leave no
    // row marked active/"Selected" after a restore. _modelPickerOptionIdentity
    // is the same identity used for picker dedup, so the row that survives is
    // exactly the one the send path resolves.
    const _norm=(model,provider)=>typeof _modelPickerOptionIdentity==='function'
      ?_modelPickerOptionIdentity(model,provider)
      :String(model||'');
    return _norm(_rowModel,_rowProvider)===_norm(_stateModel,_stateProvider)
      &&_rowProvider===_stateProvider;
  };
  const _selectedModelBadge=(m)=>_isSelectedModelRow(m)
    ?`<span class="model-opt-badge model-opt-badge--selected">${esc(t('model_badge_selected')||'Selected')}</span>`
    :'';
  const _renderProviderEndpointHint=(entry,parent)=>{
    if(!entry||!entry.label||!entry.modelsEndpointError) return;
    const hint=document.createElement('div');
    hint.className='model-provider-hint';
    hint.textContent=entry.modelsEndpointError.message||'Models endpoint could not be reached for this provider.';
    (parent||dd).appendChild(hint);
  };
  // Build a single model-option row (mirrors the main render loop's row markup),
  // used both by the main render and by the in-place overflow reveal below.
  const _buildModelRow=(m,withProviderChip)=>{
    const row=document.createElement('div');
    row.className='model-opt'+(_isSelectedModelRow(m)?' active':'');
    const badgeHtml=m.badge?`<span class="model-opt-badge model-opt-badge--${esc(m.badge.role||'configured')}">${esc(m.badge.label||'Configured')}</span>`:'';
    const _plainGroup=m.group?String(m.group).replace(/\s*\(\d+\s+of\s+\d+\)\s*$/,''):'';
    const providerChip=(_plainGroup&&withProviderChip)?`<span class="model-opt-provider">${esc(_plainGroup)}</span>`:'';
    row.innerHTML=`<div class="model-opt-top"><span class="model-opt-name">${esc(m.name)}</span>${badgeHtml}${_selectedModelBadge(m)}${providerChip}</div><span class="model-opt-id">${esc(m.id)}</span>`;
    row.onclick=()=>selectFromDropdown(m.value,m.providerId||(m.badge&&m.badge.provider)||null);
    return row;
  };
  const _expandOverflowGroup=(groupMetaEntry)=>{
    if(!groupMetaEntry||!groupMetaEntry.optgroup) return;
    const og=groupMetaEntry.optgroup;
    const groupKey=groupMetaEntry.key;
    const extraModels=_readModelOverflowData(og);
    // Nothing to reveal ‚Äî no overflow tail advertised.
    if(!extraModels.length) return;
    // Append the overflow models to the source <select> so the dropdown's state
    // stays the source of truth (search, re-render, selection all see them).
    // NOTE: guard on extraModels.length (above), NOT on the append return value ‚Äî
    // _appendOverflowOptionsToGroup returns the count of NEWLY-created <option>s
    // and returns 0 (while still clearing dataset.extraModels) when every overflow
    // model already existed as an option. Bailing on a 0 return would leave those
    // already-present-but-hidden rows unrevealed and the expander dead (#bug3).
    _appendOverflowOptionsToGroup(og,extraModels);
    // Full re-render fallback ‚Äî the proven path. Used when the in-place reveal
    // can't run (minimal/headless DOM without CSS.escape/rAF/insertBefore, or any
    // unexpected failure). Produces the same end state: overflow appended,
    // expander gone, search term reapplied.
    const _fullReRender=()=>{
      const _term=(_si&&_si.value)||'';
      renderModelDropdown(opts);
      const ns=dd.querySelector('.model-search-input');
      if(ns){ ns.value=_term; (ns._listeners&&ns._listeners.input)?ns._listeners.input():ns.dispatchEvent(new Event('input')); }
    };
    // IN-PLACE reveal: build the newly-revealed rows and insert them directly into
    // the existing group wrapper (before the "Show more" expander), then remove
    // the expander. No full re-render ‚Äî so the group stays open, every other
    // group keeps its collapsed/open state, and the scroll position is preserved.
    // The user lands on the first new row. Falls back to a full re-render if the
    // runtime lacks the DOM APIs this needs.
    const _canInPlace = typeof CSS!=='undefined' && CSS && typeof CSS.escape==='function'
      && typeof dd.querySelector==='function';
    if(!_canInPlace){ _fullReRender(); return; }
    let wrap, moreEl;
    try{
      wrap=dd.querySelector(`.model-group-body[data-group="${CSS.escape(groupKey)}"]`);
      moreEl=wrap?wrap.querySelector('.model-opt-more'):null;
    }catch(_){ _fullReRender(); return; }
    if(!wrap||!moreEl||typeof wrap.insertBefore!=='function'){
      _fullReRender();
      return;
    }
    try{
      const _plainLabel=String(groupMetaEntry.label||'').replace(/\s*\(\d+\s+of\s+\d+\)\s*$/,'');
      const _alreadyShown=new Set(Array.from(wrap.querySelectorAll('.model-opt .model-opt-id')).map(el=>el.textContent));
      let firstNewRow=null;
      for(const m of extraModels){
        if(!m||!m.id) continue;
        if(_alreadyShown.has(esc(m.id))) continue;
        const row=_buildModelRow({value:m.id,name:m.label||m.id,id:m.id,group:_plainLabel,groupKey,providerId:(og.dataset&&og.dataset.provider)||''},false);
        wrap.insertBefore(row,moreEl);
        if(!firstNewRow) firstNewRow=row;
      }
      // Sync the in-memory model data so a later _filterModels() re-render (e.g.
      // after a search is typed and cleared) keeps the group fully expanded
      // instead of snapping back to the capped view + a fresh "Show more". The
      // overflow rows were just appended to the live <select>, so flip their
      // _modelData entries to no-longer-hidden and zero the group's hidden count.
      for(const _md of _modelData){
        if(_md && _md.groupKey===groupKey && _md.hiddenByDefault){
          _md.hiddenByDefault=false;
        }
      }
      if(groupMetaEntry && typeof groupMetaEntry.hiddenCount==='number'){
        groupMetaEntry.hiddenCount=0;
      }
      // The group is now fully expanded ‚Äî drop the "Show more" expander, and bump
      // the heading count to the full total. Also force the group OPEN (the user
      // just asked to see more of it) regardless of any prior collapsed state.
      moreEl.remove();
      wrap.style.display='';
      _forceOpenGroups.add(groupKey);
      const heading=wrap.previousElementSibling;
      if(heading&&heading.classList&&heading.classList.contains('model-group')){
        const _total=wrap.querySelectorAll('.model-opt').length;
        heading.textContent=_total>1?`${_plainLabel} (${_total})`:_plainLabel;
        heading.classList.add('collapsible','open');
      }
      // Scroll so the first newly-revealed row sits near the top of the dropdown
      // viewport ‚Äî the user asked to "land on the new models" after Show more,
      // not be reset to the top of the list and not have it jump unpredictably.
      if(firstNewRow && typeof firstNewRow.offsetTop==='number' && typeof requestAnimationFrame==='function'){
        const _targetTop=Math.max(0,firstNewRow.offsetTop-48);
        const _doScroll=()=>{ try{ dd.scrollTop=_targetTop; }catch(_){} };
        _doScroll();                                   // immediate
        requestAnimationFrame(()=>{ _doScroll(); requestAnimationFrame(_doScroll); });
        if(typeof setTimeout==='function') setTimeout(_doScroll,80); // after any refocus settles
      }
    }catch(_err){
      // Any unexpected DOM failure ‚Äî fall back to the proven full re-render so
      // the overflow still gets revealed.
      _fullReRender();
    }
  };
  // Collapsible group state ‚Äî persists across _filterModels calls
  const _groupOpenState={};
  let _prevHasSearch=false;  // tracks search->empty transition to reset open-state
  let _groupWrappers={};
  // The group that owns the currently-selected model. Groups start COLLAPSED by
  // default (#4279); the selected provider's group is the one exception so the
  // user always sees their active model without expanding anything. (#4279 + UX)
  const _selectedGroupKey=(()=>{
    const _selVal=String((sel&&sel.value)||'');
    if(!_selVal) return null;
    const _hit=_modelData.find(m=>m&&!m.endpointErrorOnly&&_isSelectedModelRow(m)) || _modelData.find(m=>m&&!m.endpointErrorOnly&&String(m.value||'')===_selVal);
    return _hit?_hit.groupKey:null;
  })();
  const _makeModelRow=(m,shouldRenderHeading)=>{
    const row=document.createElement('div');
    row.className='model-opt'+(_isSelectedModelRow(m)?' active':'');
    const badgeHtml=m.badge?`<span class="model-opt-badge model-opt-badge--${esc(m.badge.role||'configured')}">${esc(m.badge.label||'Configured')}</span>`:'';
    const _plainGroup=m.group?String(m.group).replace(/\s*\(\d+\s+of\s+\d+\)\s*$/,''):'';
    const _underOwnHeading=shouldRenderHeading&&!!(m.groupKey&&_groupWrappers[m.groupKey]);
    const providerChip=(_plainGroup&&!_underOwnHeading)?`<span class="model-opt-provider">${esc(_plainGroup)}</span>`:'';
    row.innerHTML=`<div class="model-opt-top"><span class="model-opt-name">${esc(m.name)}</span>${badgeHtml}${_selectedModelBadge(m)}${providerChip}</div><span class="model-opt-id">${esc(m.id)}</span>`;
    row.onclick=()=>selectFromDropdown(m.value,m.providerId||(m.badge&&m.badge.provider)||null);
    return row;
  };
  const _filterModels=(term)=>{
    // Preserve focus across the re-render if the search input already had it ‚Äî so a
    // touch user typing a query (where autoFocusSearch is suppressed to avoid the
    // initial keyboard pop) doesn't lose focus mid-word on each keystroke re-render.
    const _hadFocus=(typeof document!=='undefined')&&document.activeElement===_si;
    term=term.trim().toLowerCase();
    const hasSearch=!!term;
    // On a fresh search, expand all groups so every match is visible (#collapse).
    if(hasSearch) for(const k in _groupOpenState) _groupOpenState[k]=true;
    // When a search is CLEARED (search -> empty), reset the per-group open state
    // so the collapsed-except-selected default re-applies ‚Äî otherwise every group
    // the search auto-expanded would stay open, defeating the collapse UX. Groups
    // the user explicitly expanded via "Show more" (_forceOpenGroups) and the
    // selected group remain open through the defaulting logic below.
    else if(_prevHasSearch){ for(const k in _groupOpenState) delete _groupOpenState[k]; }
    _prevHasSearch=hasSearch;
    const found=new Set();
    // Fold whitespace/hyphens/dots on both sides so "ox alpha", "ox-alpha" and
    // "ox.alpha" all match the same model (OpenRouter display names use spaces,
    // ids use slashes/hyphens) (#7228).
    const _foldModelSearch=(s)=>String(s||'').toLowerCase().replace(/[\s._-]+/g,'');
    const foldTerm=_foldModelSearch(term);
    for(const m of _modelData){
      const name=m.name.toLowerCase();
      const id=m.id.toLowerCase();
      if(name.includes(term)||id.includes(term)
         ||(foldTerm&&(_foldModelSearch(name).includes(foldTerm)||_foldModelSearch(id).includes(foldTerm)))){
        found.add(m.value);
      }
    }
    const matches=(m)=>!term||found.has(m.value);
    const configuredCandidates=_modelData
      .filter(m=>m.badge&&matches(m));
    const configuredBySemanticKey=new Map();
    const _configuredProviderKey=(m)=>String((m&&m.badge&&m.badge.provider)||_providerFromModelValue(m&&m.value)||'').toLowerCase();
    const _configuredModelKey=(m)=>_normalizeConfiguredModelKey(m&&m.value||'');
    const _configuredDisplayPriority=(m)=>{
      // Prefer plain IDs over provider-qualified aliases for readability.
      const v=String((m&&m.value)||'');
      if(v.startsWith('@')) return 0;
      if(v.includes('/')) return 1;
      return 2;
    };
    for(const candidate of configuredCandidates){
      const semanticKey=`${_configuredProviderKey(candidate)}::${_configuredModelKey(candidate)}`;
      const existing=configuredBySemanticKey.get(semanticKey);
      if(!existing){
        configuredBySemanticKey.set(semanticKey,candidate);
        continue;
      }
      const candidatePriority=_configuredDisplayPriority(candidate);
      const existingPriority=_configuredDisplayPriority(existing);
      if(candidatePriority>existingPriority){
        configuredBySemanticKey.set(semanticKey,candidate);
      }
    }
    const configuredModels=[...configuredBySemanticKey.values()]
      .sort((a,b)=>{
        const configuredRankA=_configuredRank(a.badge);
        const configuredRankB=_configuredRank(b.badge);
        if(configuredRankA!==configuredRankB) return configuredRankA-configuredRankB;
        return a.name.localeCompare(b.name);
      });
    const configuredIds=new Set(configuredModels.map(m=>m.value));
    const configuredSemanticKeys=new Set(configuredModels.map(m=>`${_configuredProviderKey(m)}::${_configuredModelKey(m)}`));
    const _effectiveHiddenCount=(groupKey)=>_modelData.filter(m=>
      m.groupKey===groupKey
      && m.hiddenByDefault
      && !configuredSemanticKeys.has(`${_configuredProviderKey(m)}::${_configuredModelKey(m)}`)
    ).length;
    dd.innerHTML='';
    dd.appendChild(_scopeNote);
    dd.appendChild(_searchRow);
    dd.appendChild(_custSep);
    dd.appendChild(_custRow);
    if(configuredModels.length){
      const configuredHeading=document.createElement('div');
      configuredHeading.className='model-group';
      configuredHeading.textContent=t('model_group_configured')||'Configured';
      dd.appendChild(configuredHeading);
      // ‰∏∫‰∫ÜÊòæÁ§∫ÂéüÂßãIDÔºåÂª∫Á´ã badgeKeyMap: badgeÂØπË±°->ÂéüÂßãkey
      const badgeKeyMap = new Map();
      for(const [k, v] of Object.entries(_badgeMap)){
        badgeKeyMap.set(v, k);
      }
      for(const m of configuredModels){
        const row=document.createElement('div');
        row.className='model-opt'+(_isSelectedModelRow(m)?' active':'');
        let badgeLabel = '';
        if (m.badge) {
          // Áõ¥Êé•Áî®badgeÁöÑÂéüÂßãkeyÔºàÂç≥config.yamlÈáåÁöÑIDÔºâ
          const rawId = badgeKeyMap.get(m.badge) || m.value || m.badge.label || 'Configured';
          badgeLabel = rawId;
          if(m.badge.provider){
            const providerName=m.badge.provider.replace(/^custom:/,'').split('/')[0];
            badgeLabel += ` (${providerName})`;
          }
        }
        const badgeHtml=m.badge?`<span class="model-opt-badge model-opt-badge--${esc(m.badge.role||'configured')}">${esc(badgeLabel)}</span>`:'';
        row.innerHTML=`<div class="model-opt-top"><span class="model-opt-name">${m.name}</span>${badgeHtml}${_selectedModelBadge(m)}</div><span class="model-opt-id">${esc(m.id)}</span>`;
        row.onclick=()=>selectFromDropdown(m.value,(m.badge&&m.badge.provider)||m.providerId||null);
        dd.appendChild(row);
      }
    }
    for(const groupKey of _groupOrder){
      const meta=_groupMeta.get(groupKey);
      if(!meta) continue;
      const hiddenCount=_effectiveHiddenCount(groupKey);
      const groupRows=_modelData.filter(m=>
        m.groupKey===groupKey
        && !configuredIds.has(m.value)
        && !m.endpointErrorOnly
        && matches(m)
        && (!m.hiddenByDefault || !!term)
      );
      const shouldRenderHeading=!!meta.label&&(groupRows.length||meta.endpointErrorOnly||(!term&&hiddenCount));
      if(shouldRenderHeading){
        const heading=document.createElement('div');
        heading.className='model-group';
        // When COLLAPSED (hiddenCount>0) keep the backend-decorated label verbatim
        // ("Nous (2 of 4)") so the overflow count shows. When EXPANDED, strip that
        // decoration and append the rendered-row count, otherwise the heading reads
        // "Nous (2 of 4) (4)" (double count). Count rendered rows, not modelCount,
        // so hoisted-configured models aren't double-counted. (#3691)
        const count=hiddenCount?0:groupRows.length;
        const _plainLabel=String(meta.label||'').replace(/\s*\(\d+\s+of\s+\d+\)\s*$/,'');
        heading.textContent=count>1?`${_plainLabel} (${count})`:meta.label;
        dd.appendChild(heading);
        const wrapper=document.createElement('div');
        wrapper.className='model-group-body';
        wrapper.dataset.group=groupKey;
        // A group carrying a provider endpoint-error hint must stay visible by
        // default ‚Äî otherwise the "models endpoint unreachable" warning is hidden
        // inside a collapsed body and the user never sees it. (#2540 surface)
        const _hasEndpointError=!!(meta&&(meta.modelsEndpointError||meta.endpointErrorOnly));
        if(hasSearch) _groupOpenState[groupKey]=true;
        else if(_forceOpenGroups.has(groupKey)) _groupOpenState[groupKey]=true;
        else if(_hasEndpointError) _groupOpenState[groupKey]=true;
        else if(!(groupKey in _groupOpenState)) _groupOpenState[groupKey]=(groupKey===_selectedGroupKey);
        if(!_groupOpenState[groupKey]) wrapper.style.display='none';
        else heading.classList.add('open');
        heading.classList.add('collapsible');
        dd.appendChild(wrapper);
        _groupWrappers[groupKey]=wrapper;
        // Render the provider endpoint-error hint inside the collapsible group
        // so it collapses/expands with it (the group is force-opened above when
        // an error is present, so the hint stays visible by default).
        _renderProviderEndpointHint(meta,wrapper);
        heading.addEventListener('click',(e)=>{
          e.stopPropagation();
          const w=dd.querySelector(`.model-group-body[data-group="${CSS.escape(groupKey)}"]`);
          if(!w) return;
          const closed=w.style.display==='none';
          w.style.display=closed?'':'none';
          _groupOpenState[groupKey]=closed;
          // Keep the cross-render force-open intent in sync with manual toggles:
          // collapsing a previously overflow-expanded group should let it
          // re-collapse on the next render too.
          if(closed) _forceOpenGroups.add(groupKey); else _forceOpenGroups.delete(groupKey);
          heading.classList.toggle('open',closed);
        });
        const useSubGroups=(
          SUB_GROUP_PROVIDERS.has(meta.providerId) &&
          groupRows.length>=SUB_GROUP_MIN_MODELS
        );
        if(useSubGroups){
          const byPrefix=new Map();
          for(const m of groupRows){
            const pfx=_vendorPrefix(m.value)||'other';
            if(!byPrefix.has(pfx)) byPrefix.set(pfx,[]);
            byPrefix.get(pfx).push(m);
          }
          const sorted=[...byPrefix.entries()].sort((a,b)=>{
            if(a[0]==='other') return 1;
            if(b[0]==='other') return -1;
            return b[1].length-a[1].length;
          });
          for(const [pfx,pfxRows] of sorted){
            if(pfxRows.length>=2){
              const subKey=`${groupKey}::${pfx}`;
              if(!(subKey in _groupOpenState)) _groupOpenState[subKey]=true;
              if(hasSearch) _groupOpenState[subKey]=true;
              const subHeading=document.createElement('div');
              subHeading.className='model-group sub collapsible';
              subHeading.dataset.group=subKey;
              if(_groupOpenState[subKey]) subHeading.classList.add('open');
              subHeading.textContent=pfx;
              const subWrapper=document.createElement('div');
              subWrapper.className='model-group-body sub';
              subWrapper.dataset.group=subKey;
              if(!_groupOpenState[subKey]) subWrapper.style.display='none';
              subHeading.addEventListener('click',(e)=>{
                e.stopPropagation();
                const closed=subWrapper.style.display==='none';
                subWrapper.style.display=closed?'':'none';
                _groupOpenState[subKey]=closed;
                subHeading.classList.toggle('open',closed);
              });
              wrapper.appendChild(subHeading);
              wrapper.appendChild(subWrapper);
              for(const m of pfxRows) subWrapper.appendChild(_makeModelRow(m,shouldRenderHeading));
            } else {
              for(const m of pfxRows) wrapper.appendChild(_makeModelRow(m,shouldRenderHeading));
            }
          }
        } else {
          for(const m of groupRows) wrapper.appendChild(_makeModelRow(m,shouldRenderHeading));
        }
      } else {
        for(const m of groupRows) dd.appendChild(_makeModelRow(m,shouldRenderHeading));
      }
      if(!term&&hiddenCount){
        const showAll=document.createElement('div');
        showAll.className='model-opt-more';
        showAll.tabIndex=0;
        showAll.setAttribute('role','button');
        const _moreLabel=esc(t('model_show_all_models',hiddenCount)||`Show ${hiddenCount} more`);
        showAll.innerHTML=`<span class="model-opt-more-chevron" aria-hidden="true"></span><span class="model-opt-more-label">${_moreLabel}</span>`;
        const _doExpand=()=>{
          // The reveal itself (in-place row insert + open + scroll-to-new) is
          // handled by _expandOverflowGroup; just trigger it.
          _expandOverflowGroup(meta);
        };
        showAll.onclick=(e)=>{
          if(e&&typeof e.stopPropagation==='function') e.stopPropagation();
          _doExpand();
        };
        showAll.addEventListener('keydown',e=>{
          if(e.key==='Enter'||e.key===' '){
            e.preventDefault();
            _doExpand();
          }
        });
        // Keep the expander inside the collapsible group so it hides/shows with it.
        if(_groupWrappers[groupKey]) _groupWrappers[groupKey].appendChild(showAll);
        else dd.appendChild(showAll);
      }
    }
    if(term&&found.size===0){
      const noResult=document.createElement('div');
      noResult.className='model-search-no-results';
      noResult.textContent=t('model_search_no_results')||'No models found';
      noResult.style.padding='12px 14px';
      noResult.style.color='var(--muted)';
      noResult.style.textAlign='center';
      dd.appendChild(noResult);
    }
    if(_autoFocusSearch||_hadFocus) _si.focus();
  };
  _si.addEventListener('input',()=>_filterModels(_si.value));
  // Keyboard navigation through filtered model rows (#2791).
  const _visibleModelRows=()=>Array.from(dd.querySelectorAll('.model-opt,.model-opt-more')).filter(el=>{
    let node=el.parentElement;
    while(node&&node!==dd){
      if(node.classList.contains('model-group-body')&&node.style.display==='none') return false;
      node=node.parentElement;
    }
    return true;
  });
  const _activeRowIndex=(rows)=>rows.findIndex(r=>r.classList.contains('is-highlighted'));
  const _highlightRow=(rows,idx)=>{
    for(const r of rows) r.classList.remove('is-highlighted');
    if(idx<0||idx>=rows.length) return;
    const row=rows[idx];
    row.classList.add('is-highlighted');
    if(typeof row.scrollIntoView==='function') row.scrollIntoView({block:'nearest'});
  };
  _si.addEventListener('keydown',e=>{
    if(e.key==='Escape'){closeDropdown();return;}
    if(e.key==='ArrowDown'||e.key==='ArrowUp'||e.key==='Enter'){
      const rows=_visibleModelRows();
      if(!rows.length){if(e.key==='Enter') e.preventDefault();return;}
      const cur=_activeRowIndex(rows);
      if(e.key==='ArrowDown'){e.preventDefault();_highlightRow(rows,cur<0?0:Math.min(rows.length-1,cur+1));return;}
      if(e.key==='ArrowUp'){e.preventDefault();_highlightRow(rows,cur<=0?rows.length-1:cur-1);return;}
      if(e.key==='Enter'){
        e.preventDefault();
        const pick=cur>=0?rows[cur]:rows[0];
        if(pick) pick.click();
      }
    }
  });
  _si.addEventListener('click',e=>e.stopPropagation());
  _sc.onclick=()=>{ _si.value=''; _filterModels(''); _si.focus(); };
  _sc.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){ _si.value=''; _filterModels(''); _si.focus(); e.preventDefault(); }});
  const _applyCustom=()=>{const v=_ci.value.trim();if(!v)return;selectFromDropdown(v,null);_ci.value='';};
  _cb.onclick=_applyCustom;
  _ci.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();_applyCustom();}if(e.key==='Escape'){closeDropdown();}});
  _ci.addEventListener('click',e=>e.stopPropagation());
  dd.appendChild(_scopeNote);
  dd.appendChild(_searchRow);
  dd.appendChild(_custSep);
  dd.appendChild(_custRow);
  _filterModels('');
}

async function selectModelFromDropdown(value){
  const preferredProviderId=arguments[1];
  const sel=$('modelSelect');
  if(!sel) { closeModelDropdown(); return; }
  const provider=String(preferredProviderId||'').trim()||null;
  const currentState=(typeof _modelStateForSelect==='function')
    ? _modelStateForSelect(sel, sel.value)
    : {model:sel.value,model_provider:null};
  const sameModel=String(currentState.model||'')===String(value||'');
  const sameProvider=String(currentState.model_provider||'')===String(provider||'');
  if(sameModel&&sameProvider){ closeModelDropdown(); return; }
  // Resolve the provider-specific option so duplicate bare IDs (e.g. gpt-5.5
  // under OpenAI Codex vs OpenRouter) update session model_provider correctly.
  if(typeof _ensureModelOptionInDropdown==='function'){
    _ensureModelOptionInDropdown(value, sel, provider);
  }else{
    sel.value=value;
  }
  syncModelChip();
  closeModelDropdown();
  if(typeof sel.onchange==='function') await sel.onchange();
}

async function toggleModelDropdown(){
  const dd=$('composerModelDropdown');
  const chip=$('composerModelChip');
  const sel=$('modelSelect');
  if(!dd||!chip||!sel) return;
  const open=dd.classList.contains('open');
  if(open){closeModelDropdown(); return;}
  if(typeof closeProfileDropdown==='function') closeProfileDropdown();
  if(typeof closeWsDropdown==='function') closeWsDropdown();
  if(typeof closeReasoningDropdown==='function') closeReasoningDropdown();
  if(typeof closeToolsetsDropdown==='function') closeToolsetsDropdown();
  if(typeof window._ensureModelDropdownReady==='function'){
    const ready=window._ensureModelDropdownReady();
    if(ready&&typeof ready.catch==='function') ready.catch(()=>{});
  }
  if(dd.classList.contains('open')) return;
  renderModelDropdown();
  dd.classList.add('open');
  _positionModelDropdown();
  const activeRow=dd.querySelector('.model-opt.active');
  if(activeRow&&typeof activeRow.scrollIntoView==='function') activeRow.scrollIntoView({block:'nearest'});
  chip.classList.add('active');
  const mobileAction=$('composerMobileModelAction');
  if(mobileAction) mobileAction.classList.add('active');
}

function closeModelDropdown(){
  const dd=$('composerModelDropdown');
  const chip=$('composerModelChip');
  const mobileAction=$('composerMobileModelAction');
  if(dd) dd.classList.remove('open');
  if(chip) chip.classList.remove('active');
  if(mobileAction) mobileAction.classList.remove('active');
  // If the phone path reparented the menu onto <body>, put it back in the
  // footer and clear the fixed-position inline styles so the DOM returns to its
  // baseline shape and the next desktop open anchors correctly (#6080).
  if(typeof _restoreModelDropdownHome==='function') _restoreModelDropdownHome();
}

function closeSettingsModelDropdown(){
  const dd=$('settingsModelDropdown');
  const chip=$('settingsModelChip');
  if(dd) dd.classList.remove('open');
  if(chip){
    chip.classList.remove('active');
    chip.setAttribute('aria-expanded','false');
  }
}

function syncSettingsModelChip(){
  const sel=$('settingsModel');
  const chip=$('settingsModelChip');
  if(!sel||!chip) return;
  const opt=sel.selectedOptions&&sel.selectedOptions[0];
  const text=(opt&&opt.textContent)||getModelLabel(sel.value||'')||t('settings_label_model')||'Default Model';
  chip.textContent=text;
  chip.title=sel.value||text;
}

function selectSettingsModelFromDropdown(value,preferredProviderId){
  const sel=$('settingsModel');
  if(!sel){closeSettingsModelDropdown();return;}
  const provider=String(preferredProviderId||'').trim()||null;
  if(typeof _ensureModelOptionInDropdown==='function'){
    _ensureModelOptionInDropdown(value,sel,provider);
  }else{
    sel.value=value;
    if(typeof syncSettingsModelChip==='function') syncSettingsModelChip();
  }
  closeSettingsModelDropdown();
  try{
    if(typeof Event==='function') sel.dispatchEvent(new Event('change',{bubbles:true}));
    else if(typeof sel.onchange==='function') sel.onchange();
  }catch(_){}
}

function openSettingsModelDropdown(){
  const dd=$('settingsModelDropdown');
  const sel=$('settingsModel');
  const chip=$('settingsModelChip');
  if(!dd||!sel) return;
  // Auto-focus the search on desktop only. On touch (coarse pointer) grabbing focus
  // pops the on-screen keyboard the instant the chip is tapped ‚Äî the composer picker
  // doesn't do it either, so match that behavior on touch. Computed before render so
  // renderModelDropdown's own initial focus is suppressed too (not just the outer one).
  const _coarsePointer=(typeof window.matchMedia==='function')&&window.matchMedia('(pointer: coarse)').matches;
  renderModelDropdown({
    dropdownId:'settingsModelDropdown',
    selectId:'settingsModel',
    forceOpenKey:'settingsModel',
    closeDropdown:closeSettingsModelDropdown,
    selectModel:selectSettingsModelFromDropdown,
    scopeNoteText:t('settings_desc_model')||'Used for new conversations. Existing conversations keep their selected model.',
    autoFocusSearch:!_coarsePointer,
  });
  dd.classList.add('open');
  if(chip){
    chip.classList.add('active');
    chip.setAttribute('aria-expanded','true');
  }
  if(!_coarsePointer){
    setTimeout(()=>{
      const input=dd.querySelector('.model-search-input');
      if(input) input.focus();
    },0);
  }
}

function toggleSettingsModelDropdown(){
  const dd=$('settingsModelDropdown');
  if(dd&&dd.classList.contains('open')){closeSettingsModelDropdown();return;}
  openSettingsModelDropdown();
}

function mountSettingsModelPicker(){
  const chip=$('settingsModelChip');
  const sel=$('settingsModel');
  if(!chip||!sel) return;
  syncSettingsModelChip();
  if(!chip._settingsModelPickerBound){
    chip._settingsModelPickerBound=true;
    chip.addEventListener('click',e=>{
      e.preventDefault();
      e.stopPropagation();
      toggleSettingsModelDropdown();
    });
    chip.addEventListener('keydown',e=>{
      if(e.key==='Enter'||e.key===' '||e.key==='ArrowDown'){
        e.preventDefault();
        toggleSettingsModelDropdown();
      }
    });
  }
}

document.addEventListener('click',e=>{
  if(
    !e.target.closest('#composerModelChip') &&
    !e.target.closest('#composerMobileModelAction') &&
    !e.target.closest('#composerModelDropdown')
  ) closeModelDropdown();
  if(
    !e.target.closest('#settingsModelChip') &&
    !e.target.closest('#settingsModel') &&
    !e.target.closest('#settingsModelDropdown')
  ) closeSettingsModelDropdown();
});
window.addEventListener('resize',()=>{
  const dd=$('composerModelDropdown');
  if(dd&&dd.classList.contains('open')) _positionModelDropdown();
  // Keep the reasoning dropdown aligned under its chip when the window
  // resizes while open ‚Äî same pattern as the model dropdown above.
  const rdd=$('composerReasoningDropdown');
  if(rdd&&rdd.classList.contains('open')&&typeof _positionReasoningDropdown==='function'){
    _positionReasoningDropdown();
  }
});

// visualViewport resize/scroll fire on mobile when the on-screen keyboard opens
// or the URL bar collapses/expands ‚Äî the phone dropdown is fixed to the visual
// viewport, so it must be re-measured against the new offsets. Coalesce with rAF
// so a burst of scroll/resize events triggers at most one reposition per frame.
let _modelDropdownRepositionScheduled=false;
function _repositionOpenModelDropdown(){
  const dd=$('composerModelDropdown');
  if(!(dd&&dd.classList.contains('open'))||_modelDropdownRepositionScheduled) return;
  _modelDropdownRepositionScheduled=true;
  requestAnimationFrame(()=>{
    _modelDropdownRepositionScheduled=false;
    const openDd=$('composerModelDropdown');
    if(openDd&&openDd.classList.contains('open')) _positionModelDropdown();
  });
}
if(window.visualViewport){
  window.visualViewport.addEventListener('resize',_repositionOpenModelDropdown);
  window.visualViewport.addEventListener('scroll',_repositionOpenModelDropdown);
}

// ‚îÄ‚îÄ Fit-based composer footer collapse ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
// Stage classes on .composer-footer:
//   (none) full labels ¬∑ .cf-icons icon chips ¬∑ .cf-icons.cf-burger hamburger.
let _composerFitScheduled=false;
let _composerFitResizeObserver=null;
let _composerFitMutationObserver=null;
let _composerFitObservedFooter=null;
let _composerFitResizeListenerBound=false;

function _fitComposerFooter(){
  const footer=document.querySelector('.composer-footer');
  if(!footer) return;
  const left=footer.querySelector('.composer-left');
  if(!left) return;
  if(!left.clientWidth) return;
  const overflows=function(){return left.scrollWidth>left.clientWidth+1;};
  // Measure without ever PAINTING the expanded state. Stripping the stage
  // classes makes the footer briefly full-width, which grows the composer and
  // shrinks #messages by a few px; restoring them a moment later shrinks it
  // back. A pinned reader sees that as a vertical up/down jitter on every
  // fit pass (and fit passes run on each context-indicator update during SSE).
  // `visibility:hidden` + a fixed height freeze the layout box during the
  // measurement, so the scroll container's clientHeight never changes.
  const prevVisibility=footer.style.visibility;
  const prevHeight=footer.style.height;
  const frozenHeight=footer.getBoundingClientRect().height;
  if(frozenHeight>0){
    footer.style.height=frozenHeight+'px';
    footer.style.visibility='hidden';
  }
  let next='';
  try{
    footer.classList.remove('cf-icons','cf-burger');
    if(overflows()){
      footer.classList.add('cf-icons');
      next='cf-icons';
      if(overflows()){
        footer.classList.add('cf-burger');
        next='cf-icons cf-burger';
      }
    }
  }finally{
    // Restore the measured stage, then release the frozen box in the same
    // task so no intermediate geometry is ever committed to the screen.
    footer.classList.toggle('cf-icons',next.includes('cf-icons'));
    footer.classList.toggle('cf-burger',next.includes('cf-burger'));
    if(frozenHeight>0){
      footer.style.height=prevHeight;
      footer.style.visibility=prevVisibility;
    }
  }
}
window._fitComposerFooter=_fitComposerFooter;

function _scheduleComposerFit(){
  if(_composerFitScheduled) return;
  _composerFitScheduled=true;
  requestAnimationFrame(function(){
    _composerFitScheduled=false;
    try{_fitComposerFooter();}catch(_){ }
  });
}
window._scheduleComposerFit=_scheduleComposerFit;

function _initComposerFooterFit(){
  const footer=document.querySelector('.composer-footer');
  const left=footer&&footer.querySelector('.composer-left');
  if(!footer||!left) return;
  _scheduleComposerFit();
  if(_composerFitObservedFooter===footer) return;
  if(_composerFitResizeObserver){try{_composerFitResizeObserver.disconnect();}catch(_){ }}
  if(_composerFitMutationObserver){try{_composerFitMutationObserver.disconnect();}catch(_){ }}
  _composerFitResizeObserver=null;
  _composerFitMutationObserver=null;
  _composerFitObservedFooter=footer;
  if(window.ResizeObserver){
    try{
      _composerFitResizeObserver=new ResizeObserver(_scheduleComposerFit);
      _composerFitResizeObserver.observe(footer);
      // Also observe the left control group directly: the footer's outer width
      // may not change when right-side controls (status/context chips) appear or
      // resize, but that shrinks .composer-left's available room and must
      // retrigger a refit. (Codex gate #4657.)
      if(left && left!==footer){try{_composerFitResizeObserver.observe(left);}catch(_){ }}
    }catch(_){ }
  }
  if(window.MutationObserver){
    try{
      _composerFitMutationObserver=new MutationObserver(_scheduleComposerFit);
      _composerFitMutationObserver.observe(left,{
        childList:true,subtree:true,characterData:true,
        attributes:true,attributeFilter:['class','style','hidden']
      });
    }catch(_){ }
  }
  if(!_composerFitResizeListenerBound){
    window.addEventListener('resize',_scheduleComposerFit);
    _composerFitResizeListenerBound=true;
  }
}
window._initComposerFooterFit=_initComposerFooterFit;

if(document.readyState==='loading'){
  document.addEventListener('DOMContentLoaded',_initComposerFooterFit);
}else{
  _initComposerFooterFit();
}

// ‚îÄ‚îÄ Reasoning effort chip ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
let _currentReasoningEffort=null;
let _currentReasoningEffortsSupported=null;
// Whether the model accepts the thinking on/off toggle when supported_efforts
// is empty (GLM-4.5‚Äì5.1 on native zai). Undefined = unknown, treated as true
// so the chip stays visible by default (prior behavior).
let _currentReasoningToggleSupported=undefined;
let _profileTransitionReasoningContext=null;

function _normalizeReasoningEffort(eff){
  return String(eff||'').trim().toLowerCase();
}

function _formatReasoningEffortLabel(effort){
  if(effort==='none') return 'None';
  if(!effort) return 'Default';
  if(effort==='minimal') return 'Minimal';
  if(effort==='low') return 'Low';
  if(effort==='medium') return 'Medium';
  if(effort==='high') return 'High';
  if(effort==='xhigh') return 'XHigh';
  if(effort==='max') return 'Max';
  return effort.charAt(0).toUpperCase()+effort.slice(1);
}

function _reasoningEffortContext(){
  const transition=_profileTransitionReasoningContext;
  const session=S&&S.session;
  if(transition&&(!session||session.profile!==transition.profile)){
    const ctx={};
    if(transition.model) ctx.model=transition.model;
    if(transition.provider) ctx.provider=transition.provider;
    return ctx;
  }
  const sel=$('modelSelect');
  const model=(S&&S.session&&S.session.model)||(sel&&sel.value)||'';
  let provider=(S&&S.session&&S.session.model_provider)||'';
  if(!provider&&sel&&model&&typeof _modelStateForSelect==='function'){
    provider=_modelStateForSelect(sel, model).model_provider||'';
  }
  const ctx={};
  if(model) ctx.model=model;
  if(provider) ctx.provider=provider;
  return ctx;
}

function _reasoningEffortQuery(){
  const params=new URLSearchParams(_reasoningEffortContext());
  const qs=params.toString();
  return qs?('?'+qs):'';
}

function _applyReasoningOptions(supportedEfforts){
  const dd=$('composerReasoningDropdown');
  if(!dd) return;
  const supported=new Set(Array.isArray(supportedEfforts)?supportedEfforts:[]);
  dd.querySelectorAll('.reasoning-option').forEach(function(opt){
    const effort=opt.dataset.effort;
    // 'none' (turn thinking off) and '' (Default = clear override, provider
    // default = thinking on) are meta-options outside the effort ladder. They
    // are always shown so a thinking-toggle-only model (GLM-4.5‚Äì5.1 on native
    // zai, where the ladder is empty) still has an operable two-state control:
    // Default (on) + None (off). Without the Default option the toggle is
    // one-way off-only ‚Äî the user can disable thinking but cannot re-enable it.
    // (#6219 round-3)
    if(effort==='none'||effort===''){
      opt.style.display='';
      return;
    }
    if(!supported.size){
      opt.style.display='none';
      return;
    }
    opt.style.display=supported.has(effort)?'':'none';
  });
}

function _applyReasoningChip(eff){
  const meta=arguments[1]||null;
  const effort=_normalizeReasoningEffort(eff);
  _currentReasoningEffort=effort;
  if(meta&&Array.isArray(meta.supported_efforts)){
    _currentReasoningEffortsSupported=meta.supported_efforts;
  }
  // supports_thinking_toggle: the model accepts the thinking on/off toggle even
  // when the effort ladder is empty (GLM-4.5‚Äì5.1 on native zai accept
  // `thinking: {"type": ...}` but NOT `reasoning_effort`). Without honoring this
  // flag, returning an empty supported_efforts hides the entire chip and
  // silently regresses the working thinking on/off control for those models.
  // Default true preserves prior behavior when the field is absent.
  if(meta&&typeof meta.supports_thinking_toggle==='boolean'){
    _currentReasoningToggleSupported=meta.supports_thinking_toggle;
  }
  const wrap=$('composerReasoningWrap');
  const label=$('composerReasoningLabel');
  const chip=$('composerReasoningChip');
  const mobileLabel=$('composerMobileReasoningLabel');
  const mobileAction=$('composerMobileReasoningAction');
  if(!wrap||!label) return;
  const supportedEfforts=(typeof _currentReasoningEffortsSupported==='undefined')
    ?null
    :_currentReasoningEffortsSupported;
  const toggleSupported=(typeof _currentReasoningToggleSupported==='undefined')
    ?true
    :_currentReasoningToggleSupported;
  const hasEffortLadder=Array.isArray(supportedEfforts)
    ?supportedEfforts.length>0
    :true;
  // Show the chip if there is an effort ladder OR a thinking toggle is still
  // available. Only hide when the model supports neither.
  const supports=hasEffortLadder||toggleSupported;
  if(!supports){
    wrap.style.display='none';
    if(mobileAction) mobileAction.style.display='none';
    return;
  }
  wrap.style.display='';
  if(mobileAction) mobileAction.style.display='';
  if(typeof _applyReasoningOptions==='function') _applyReasoningOptions(supportedEfforts);
  const text=_formatReasoningEffortLabel(effort);
  label.textContent=text;
  if(mobileLabel) mobileLabel.textContent=text;
  if(chip){
    const inactive=!effort||effort==='none';
    chip.classList.toggle('inactive',inactive);
    const labelText='Reasoning effort: '+text;
    chip.title=labelText;
    chip.setAttribute('aria-label',labelText);
  }
  if(mobileAction) mobileAction.classList.toggle('inactive',!effort||effort==='none');
  _highlightReasoningOption(effort);
}

// Tracks the model/provider identity of the last reasoning fetch so routine
// topbar syncs can serve the cached chip state instead of re-hitting the
// network. null = never fetched.
let _lastReasoningFetchKey=null;
// Monotonic dispatch counter. Each fetchReasoningChip() increments it and the
// async handlers capture their own value; a response (success OR failure) only
// applies if it is still the most recent dispatch. This defeats out-of-order
// resolution even when two fetches share the same model/provider key (e.g. a
// profile switch that resets the cache and refetches the same default model but
// a different agent.reasoning_effort) ‚Äî #4650 review.
let _reasoningFetchSeq=0;

function fetchReasoningChip(keyOverride){
  // Set the cache key OPTIMISTICALLY before the request so rapid routine syncs
  // while this GET is in flight short-circuit instead of re-dispatching (that
  // in-flight window is exactly where the #4650 storm lived).
  const key=keyOverride===undefined?_reasoningEffortQuery():keyOverride;
  const seq=++_reasoningFetchSeq;
  _lastReasoningFetchKey=key;
  api('/api/reasoning'+key).then(function(st){
    // Ignore a stale/superseded response: only the most recent dispatch may
    // apply, so an older in-flight GET can't poison the current chip (#4650).
    if(seq!==_reasoningFetchSeq) return;
    _applyReasoningChip((st&&st.reasoning_effort)||'', st||{});
  }).catch(function(){
    // Same staleness guard on failure: a stale error must neither hide the chip
    // nor clear a newer fetch's key. Only the latest dispatch clears the key so
    // routine syncs retry after a genuine transient failure.
    if(seq!==_reasoningFetchSeq) return;
    _lastReasoningFetchKey=null;
    _applyReasoningChip('', {supported_efforts:[], supports_thinking_toggle:false});
  });
}

function refreshProfileTransitionReasoningChip(model, provider){
  _profileTransitionReasoningContext={profile:(S&&S.activeProfile)||'default',model,provider};
  _currentReasoningEffort=null;
  _currentReasoningEffortsSupported=null;
  _currentReasoningToggleSupported=undefined;
  _lastReasoningFetchKey=null;
  ++_reasoningFetchSeq;
  _applyReasoningChip('', {supported_efforts:[], supports_thinking_toggle:false});
  const params=new URLSearchParams();
  if(model) params.set('model',model);
  if(provider) params.set('provider',provider);
  fetchReasoningChip(params.size?'?'+params.toString():undefined);
}

function clearProfileTransitionReasoningContext(){
  _profileTransitionReasoningContext=null;
}

function syncReasoningChip(){
  // #4650: syncTopbar() calls this on every routine UI refresh, and during
  // streaming those fire at high frequency. Before a9ce2889 this served the
  // cached _currentReasoningEffort after the first load; that commit made it
  // refetch unconditionally to refresh supported-efforts after a model switch,
  // which turned ordinary syncs into a GET /api/reasoning storm (one per token).
  // Restore the cache short-circuit but keep a9ce2889's intent: only hit the
  // network when nothing is cached yet OR the model/provider identity changed
  // since the last fetch (the only inputs that change /api/reasoning's answer).
  // The user-pick and model-switch paths still update the cache directly.
  const key=_reasoningEffortQuery();
  // Short-circuit on the KEY alone: if a fetch for this exact model/provider has
  // already been dispatched (in-flight) or completed, do not dispatch another ‚Äî
  // this is what stops the #4650 storm, including the COLD-cache window where
  // _currentReasoningEffort is still null between the first dispatch and its
  // response (10 syncs before the first GET resolves must produce ONE request,
  // not ten). Apply the cached chip only once we actually have an effort value.
  if(_lastReasoningFetchKey===key){
    if(_currentReasoningEffort!==null) _applyReasoningChip(_currentReasoningEffort);
    return;
  }
  fetchReasoningChip();
}

function _highlightReasoningOption(effort){
  const dd=$('composerReasoningDropdown');
  if(!dd) return;
  dd.querySelectorAll('.reasoning-option').forEach(function(opt){
    opt.classList.toggle('selected',opt.dataset.effort===effort);
  });
}

function toggleReasoningDropdown(){
  const dd=$('composerReasoningDropdown');
  const chip=$('composerReasoningChip');
  if(!dd||!chip) return;
  const open=dd.classList.contains('open');
  if(open){closeReasoningDropdown();return;}
  if(typeof closeProfileDropdown==='function') closeProfileDropdown();
  if(typeof closeWsDropdown==='function') closeWsDropdown();
  closeModelDropdown();
  if(typeof closeToolsetsDropdown==='function') closeToolsetsDropdown();
  _highlightReasoningOption(_currentReasoningEffort);
  dd.classList.add('open');
  _positionReasoningDropdown();
  chip.classList.add('active');
  const mobileAction=$('composerMobileReasoningAction');
  if(mobileAction) mobileAction.classList.add('active');
}

function _positionReasoningDropdown(){
  const dd=$('composerReasoningDropdown');
  const chip=$('composerReasoningChip');
  const mobileAction=$('composerMobileReasoningAction');
  const footer=document.querySelector('.composer-footer');
  if(!dd||!chip||!footer) return;
  const panel=$('composerMobileConfigPanel');
  const anchor=(panel&&panel.classList.contains('open')&&mobileAction)?mobileAction:chip;
  const chipRect=anchor.getBoundingClientRect();
  const footerRect=footer.getBoundingClientRect();
  let left=chipRect.left-footerRect.left;
  const maxLeft=Math.max(0,footer.clientWidth-dd.offsetWidth);
  left=Math.max(0,Math.min(left,maxLeft));
  dd.style.left=`${left}px`;
}

function closeReasoningDropdown(){
  const dd=$('composerReasoningDropdown');
  const chip=$('composerReasoningChip');
  const mobileAction=$('composerMobileReasoningAction');
  if(dd) dd.classList.remove('open');
  if(chip) chip.classList.remove('active');
  if(mobileAction) mobileAction.classList.remove('active');
}

document.addEventListener('click',function(e){
  if(
    !e.target.closest('#composerReasoningChip') &&
    !e.target.closest('#composerMobileReasoningAction') &&
    !e.target.closest('#composerReasoningDropdown')
  ) closeReasoningDropdown();
  if(e.target.closest('.reasoning-option')){
    const opt=e.target.closest('.reasoning-option');
    const effort=opt&&opt.dataset.effort;
    // NOTE: effort may be the empty string for the "Default" option (clears
    // the override). Check option presence, not truthiness ‚Äî `if(effort)` would
    // silently ignore the Default click and leave the toggle one-way off-only.
    // (#6219 round-3)
    if(opt){
      const payload=Object.assign({effort:effort},_reasoningEffortContext());
      api('/api/reasoning',{method:'POST',body:JSON.stringify(payload)})
        .then(function(st){
          // For Default (effort=''), the returned reasoning_effort is '' (clear)
          // ‚Äî display 'Default' rather than an empty toast.
          const display=(st&&st.reasoning_effort)||effort||'Default';
          _applyReasoningChip((st&&st.reasoning_effort)||effort, st||{});
          showToast('üß† Reasoning effort set to '+display);
        })
        .catch(function(){showToast('üß† Failed to set effort');});
      closeReasoningDropdown();
    }
  }
});

// ‚îÄ‚îÄ Session toolsets chip (#493) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
let _currentSessionToolsets = null; // null = active profile defaults, array = custom list
let _toolsetsCatalog = null;

function _applyToolsetsChip(toolsets) {
  _currentSessionToolsets = toolsets;
  const wrap = $('composerToolsetsWrap');
  const label = $('composerToolsetsLabel');
  const chip = $('composerToolsetsChip');
  if (!wrap || !label) return;
  // Visibility is controlled entirely by responsive CSS ‚Äî the chip shows only
  // at wide composer-footer widths (>= 1100px container query). At narrower
  // widths the layout is too cramped (model + reasoning + profile + workspace
  // + context-ring + send) to add another chip. Cleared inline style so the
  // CSS @container query is the single source of truth. State is still
  // tracked so /api/session/toolsets continues to work for cron/scripted
  // callers regardless of UI visibility. (#1431)
  wrap.style.display = '';
  const hasCustom = Array.isArray(toolsets) && toolsets.length > 0;
  const isStaged = hasCustom
    && typeof S !== 'undefined'
    && S
    && !S.session
    && Array.isArray(S._pendingSessionToolsets);
  if (hasCustom) {
    const stagedSuffix = isStaged ? ' (staged)' : '';
    label.textContent = toolsets.join(', ') + stagedSuffix;
    chip.classList.add('has-custom');
    chip.title = t('session_toolsets') + ': ' + toolsets.join(', ') + stagedSuffix;
  } else {
    label.textContent = t('session_toolsets_profile_defaults');
    chip.classList.remove('has-custom');
    chip.title = t('session_toolsets') + ': ' + t('session_toolsets_profile_defaults');
  }
}

function _syncToolsetsChip() {
  if (typeof S === 'undefined' || !S || !S.session) {
    const stagedToolsets = (typeof S !== 'undefined' && S && Array.isArray(S._pendingSessionToolsets))
      ? S._pendingSessionToolsets
      : null;
    _applyToolsetsChip(stagedToolsets);
    return;
  }
  _applyToolsetsChip(S.session.enabled_toolsets || null);
}

function syncToolsetsChip() {
  _syncToolsetsChip();
}

function _normalizeToolsetsCatalog(payload) {
  const servers = payload && Array.isArray(payload.servers) ? payload.servers : [];
  const seen = new Set();
  const names = [];
  servers.forEach(function(server) {
    const name = String((server && server.name) || '').trim();
    if (!name || seen.has(name)) return;
    seen.add(name);
    names.push(name);
  });
  return names;
}

function _loadToolsetsCatalog() {
  if (Array.isArray(_toolsetsCatalog)) return Promise.resolve(_toolsetsCatalog);
  return api('/api/mcp/servers')
    .then(function(payload) {
      _toolsetsCatalog = _normalizeToolsetsCatalog(payload);
      return _toolsetsCatalog;
    })
    .catch(function() {
      _toolsetsCatalog = false;
      return [];
    });
}

function invalidateToolsetsCatalog(payload) {
  _toolsetsCatalog = payload && Array.isArray(payload.servers) ? _normalizeToolsetsCatalog(payload) : null;
}
if (typeof window !== 'undefined') window.invalidateToolsetsCatalog = invalidateToolsetsCatalog;

function _toolsetsInputList(input) {
  if (!input) return [];
  return input.value.split(',').map(s => s.trim()).filter(Boolean);
}

function _ensureToolsetsPresetSection() {
  const dd = $('composerToolsetsDropdown');
  if (!dd) return null;
  let section = $('toolsetsPresetSections');
  if (section) return section;
  section = document.createElement('div');
  section.id = 'toolsetsPresetSections';
  section.className = 'toolsets-preset-sections';
  const inputRow = dd.querySelector('.toolsets-dropdown-input-row');
  if (inputRow) dd.insertBefore(section, inputRow);
  else dd.appendChild(section);
  return section;
}

function _appendToolsetsLabel(section, text) {
  const label = document.createElement('div');
  label.className = 'toolsets-dropdown-desc';
  label.textContent = text;
  section.appendChild(label);
}

function _renderToolsetsPresetSections(opts) {
  const state = opts && opts.state;
  const input = opts && opts.input;
  const section = _ensureToolsetsPresetSection();
  if (!section || !state || !input) return;
  const selected = _toolsetsInputList(input);
  const selectedSet = new Set(selected);
  const hasCustom = selected.length > 0;
  state.textContent = hasCustom
    ? 'üîß ' + selected.join(', ')
    : 'üë§ ' + t('session_toolsets_profile_defaults');

  section.innerHTML = '';
  const defaultsBtn = document.createElement('button');
  defaultsBtn.type = 'button';
  defaultsBtn.id = 'toolsetsProfileDefaultsBtn';
  defaultsBtn.className = 'toolsets-action-btn toolsets-clear-btn';
  defaultsBtn.textContent = t('session_toolsets_use_profile_defaults');
  section.appendChild(defaultsBtn);

  _appendToolsetsLabel(section, t('session_toolsets_configured_servers'));
  if (_toolsetsCatalog === null) {
    _appendToolsetsLabel(section, t('session_toolsets_loading_servers'));
    return;
  }
  if (_toolsetsCatalog === false) {
    _appendToolsetsLabel(section, t('mcp_load_failed'));
    return;
  }
  if (!Array.isArray(_toolsetsCatalog) || !_toolsetsCatalog.length) {
    _appendToolsetsLabel(section, t('session_toolsets_no_configured_servers'));
    return;
  }
  _toolsetsCatalog.forEach(function(name) {
    const row = document.createElement('label');
    row.className = 'toolsets-server-option';
    row.style.display = 'flex';
    row.style.alignItems = 'center';
    row.style.gap = '6px';
    row.style.margin = '4px 0';
    row.style.fontSize = '12px';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.className = 'toolsets-server-checkbox';
    checkbox.value = name;
    checkbox.checked = selectedSet.has(name);
    row.appendChild(checkbox);
    row.appendChild(document.createTextNode(name));
    section.appendChild(row);
  });
}

function _populateToolsetsDropdown() {
  const desc = $('toolsetsDropdownDesc');
  const state = $('toolsetsDropdownState');
  const input = $('toolsetsInput');
  const applyBtn = $('toolsetsApplyBtn');
  const clearBtn = $('toolsetsClearBtn');
  if (!desc || !state || !input) return;
  desc.textContent = t('session_toolsets_desc');
  if (applyBtn) applyBtn.textContent = t('session_toolsets_apply');
  if (clearBtn) clearBtn.textContent = t('session_toolsets_clear');
  input.placeholder = t('session_toolsets_placeholder');
  // Escape key handler for toolsets input
  input.onkeydown = function(e) { if(e.key === 'Escape') closeToolsetsDropdown(); };
  input.oninput = function() { _renderToolsetsPresetSections({ state, input }); };
  const hasCustom = Array.isArray(_currentSessionToolsets) && _currentSessionToolsets.length > 0;
  if (hasCustom) {
    input.value = _currentSessionToolsets.join(', ');
  } else {
    input.value = '';
  }
  _renderToolsetsPresetSections({ state, input });
}

function _positionToolsetsDropdown() {
  const dd = $('composerToolsetsDropdown');
  const chip = $('composerToolsetsChip');
  const footer = document.querySelector('.composer-footer');
  if (!dd || !chip || !footer) return;
  // Defense: if the chip has been hidden by responsive CSS (e.g. resize across
  // 1100px container threshold while dropdown was open), don't try to anchor
  // to a zero-rect element ‚Äî close the dropdown instead. (#1431)
  if (chip.offsetParent === null) { closeToolsetsDropdown(); return; }
  const chipRect = chip.getBoundingClientRect();
  const footerRect = footer.getBoundingClientRect();
  let left = chipRect.left - footerRect.left;
  const maxLeft = Math.max(0, footer.clientWidth - dd.offsetWidth);
  left = Math.max(0, Math.min(left, maxLeft));
  dd.style.left = left + 'px';
}

function toggleToolsetsDropdown() {
  const dd = $('composerToolsetsDropdown');
  const chip = $('composerToolsetsChip');
  if (!dd || !chip) return;
  // Don't open when the chip itself is hidden by responsive CSS (#1431).
  // offsetParent === null catches display:none on the element or any ancestor.
  if (chip.offsetParent === null) return;
  const open = dd.classList.contains('open');
  if (open) { closeToolsetsDropdown(); return; }
  if (typeof closeProfileDropdown === 'function') closeProfileDropdown();
  if (typeof closeWsDropdown === 'function') closeWsDropdown();
  closeModelDropdown();
  if (typeof closeReasoningDropdown === 'function') closeReasoningDropdown();
  _syncToolsetsChip();
  _populateToolsetsDropdown();
  _loadToolsetsCatalog().then(function() {
    const stillOpen = dd && dd.classList.contains('open');
    if (stillOpen) {
      const state = $('toolsetsDropdownState');
      const input = $('toolsetsInput');
      _renderToolsetsPresetSections({ state, input });
    }
  });
  dd.classList.add('open');
  _positionToolsetsDropdown();
  chip.classList.add('active');
  // Focus the input after a tick so the layout has settled
  setTimeout(() => { const inp = $('toolsetsInput'); if (inp) inp.focus(); }, 50);
}

function closeToolsetsDropdown() {
  const dd = $('composerToolsetsDropdown');
  const chip = $('composerToolsetsChip');
  if (dd) dd.classList.remove('open');
  if (chip) chip.classList.remove('active');
}

function _applySessionToolsets(toolsets) {
  if (typeof S === 'undefined' || !S) return;
  if (!S.session) {
    S._pendingSessionToolsets = toolsets;
    _applyToolsetsChip(toolsets);
    if (Array.isArray(toolsets) && toolsets.length) {
      showToast('üîß ' + t('session_toolsets_applied') + ': ' + toolsets.join(', '));
    } else {
      showToast('üåç ' + t('session_toolsets_cleared'));
    }
    return;
  }
  const sid = S.session.session_id;
  api('/api/session/toolsets', {
    method: 'POST',
    body: JSON.stringify({ session_id: sid, toolsets: toolsets })
  })
    .then(function(r) {
      if (r && r.ok) {
        S.session.enabled_toolsets = r.enabled_toolsets || null;
        _applyToolsetsChip(r.enabled_toolsets || null);
        if (r.enabled_toolsets && r.enabled_toolsets.length) {
          showToast('üîß ' + t('session_toolsets_applied') + ': ' + r.enabled_toolsets.join(', '));
        } else {
          showToast('üåç ' + t('session_toolsets_cleared'));
        }
      } else {
        showToast(t('session_toolsets_failed') + (r && r.error ? r.error : 'Unknown error'), 3000, 'error');
      }
    })
    .catch(function(err) {
      showToast(t('session_toolsets_failed') + (err.message || err), 3000, 'error');
    });
}

// Click-outside handler for toolsets dropdown
document.addEventListener('click', function(e) {
  if (
    !e.target.closest('#composerToolsetsChip') &&
    !e.target.closest('#composerToolsetsDropdown')
  ) closeToolsetsDropdown();
  // Active profile defaults button
  if (e.target.closest('#toolsetsProfileDefaultsBtn')) {
    _applySessionToolsets(null);
    closeToolsetsDropdown();
    return;
  }
  // Apply button
  if (e.target.closest('#toolsetsApplyBtn')) {
    const input = $('toolsetsInput');
    if (!input) return;
    const raw = input.value.trim();
    if (!raw) {
      showToast(t('session_toolsets_desc'), 2000);
      return;
    }
    const toolsets = raw.split(',').map(s => s.trim()).filter(Boolean);
    if (toolsets.length === 0) {
      showToast(t('session_toolsets_desc'), 2000);
      return;
    }
    _applySessionToolsets(toolsets);
    closeToolsetsDropdown();
  }
  // Clear button
  if (e.target.closest('#toolsetsClearBtn')) {
    _applySessionToolsets(null);
    closeToolsetsDropdown();
  }
});

document.addEventListener('change', function(e) {
  if (!e.target.closest('#toolsetsPresetSections')) return;
  if (!e.target.classList.contains('toolsets-server-checkbox')) return;
  const input = $('toolsetsInput');
  const state = $('toolsetsDropdownState');
  if (!input) return;
  const checked = Array.from(document.querySelectorAll('#toolsetsPresetSections .toolsets-server-checkbox:checked'))
    .map(el => String(el.value || '').trim())
    .filter(Boolean);
  const catalogSet = new Set(Array.isArray(_toolsetsCatalog) ? _toolsetsCatalog : []);
  const manual = _toolsetsInputList(input).filter(name => !catalogSet.has(name));
  input.value = checked.concat(manual).join(', ');
  _renderToolsetsPresetSections({ state, input });
});

// Position toolsets dropdown on resize, OR close it if the chip is no longer
// visible (e.g. resize crossed the 1100px container threshold while dropdown
// was open ‚Äî the wrap is hidden by CSS but the dropdown sibling stays open
// without an anchor). (#1431)
window.addEventListener('resize', () => {
  const dd = $('composerToolsetsDropdown');
  if (!dd || !dd.classList.contains('open')) return;
  const chip = $('composerToolsetsChip');
  if (!chip || chip.offsetParent === null) { closeToolsetsDropdown(); return; }
  _positionToolsetsDropdown();
});

function _syncMobileComposerConfigButton(open){
  const btn=$('composerMobileConfigBtn');
  if(!btn) return;
  btn.classList.toggle('active',!!open);
  btn.setAttribute('aria-expanded',open?'true':'false');
}

function closeMobileComposerConfig(){
  const panel=$('composerMobileConfigPanel');
  if(panel) panel.classList.remove('open');
  _syncMobileComposerConfigButton(false);
  if(typeof closeWsDropdown==='function') closeWsDropdown();
}

function openMobileComposerConfig(){
  const panel=$('composerMobileConfigPanel');
  if(!panel) return;
  if(typeof closeProfileDropdown==='function') closeProfileDropdown();
  if(typeof closeWsDropdown==='function') closeWsDropdown();
  closeModelDropdown();
  closeReasoningDropdown();
  if(typeof closeToolsetsDropdown==='function') closeToolsetsDropdown();
  panel.classList.add('open');
  _syncMobileComposerConfigButton(true);
}

function toggleMobileComposerConfig(){
  const panel=$('composerMobileConfigPanel');
  if(!panel) return;
  const open=panel.classList.contains('open');
  if(open){
    closeMobileComposerConfig();
    closeModelDropdown();
    closeReasoningDropdown();
    if(typeof closeToolsetsDropdown==='function') closeToolsetsDropdown();
    return;
  }
  openMobileComposerConfig();
}

function openComposerContextMenu(e){
  if(e){
    e.preventDefault();
    e.stopPropagation();
  }
  const tooltip=$('ctxTooltip');
  if(tooltip){
    tooltip.classList.remove('ctx-tooltip-active');
    tooltip.setAttribute('aria-hidden','true');
  }
  openMobileComposerConfig();
}
window.openComposerContextMenu=openComposerContextMenu;

document.addEventListener('click',function(e){
  if(
    e.target.closest('#composerMobileConfigBtn') ||
    e.target.closest('#composerMobileConfigPanel') ||
    e.target.closest('#composerWsDropdown') ||
    e.target.closest('#composerModelDropdown') ||
    e.target.closest('#composerReasoningDropdown')
  ) return;
  closeMobileComposerConfig();
});

document.addEventListener('keydown',function(e){
  if(e.key!=='Escape') return;
  const panel=$('composerMobileConfigPanel');
  if(!panel||!panel.classList.contains('open')) return;
  e.preventDefault();
  closeMobileComposerConfig();
  if(typeof closeWsDropdown==='function') closeWsDropdown();
  closeModelDropdown();
  closeReasoningDropdown();
});

window.addEventListener('resize',function(){
  if(window.matchMedia && !window.matchMedia('(max-width: 640px)').matches){
    closeMobileComposerConfig();
    closeModelDropdown();
    closeReasoningDropdown();
    if(typeof closeWsDropdown==='function') closeWsDropdown();
  }
});

// ‚îÄ‚îÄ Scroll pinning ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
// When streaming, auto-scroll only while the user is following the live tail.
// Any manual scroll up sets a sticky unpinned flag until the user scrolls back
// to the bottom (near-bottom hysteresis on downward motion) or clicks ‚Üì.
// Programmatic scrolls are ignored via _programmaticScroll. Fixes #1469 / #1360 / #1731.
// #6606 ownership: ui.js/messages.js load before boot.js, and boot.js only
// assigns window._autoScrollFollow after its awaited settings request. Every
// consumer in this file (scroll listener, settle paths, scrollIfPinned,
// DOM-replace gate) can run before that assignment ‚Äî a bare read would throw
// ReferenceError. Establish the default synchronously here (single owner);
// boot.js overwrites it with the saved setting after hydration. The typeof
// guard preserves an explicit saved `false` if boot.js already ran.
if(typeof window._autoScrollFollow==='undefined'){ window._autoScrollFollow=true; }
let _scrollPinned=true;
let _programmaticScroll=false;
let _programmaticScrollSetAt=0;
let _programmaticScrollResetTimer=0;
const PROGRAMMATIC_SCROLL_VALID_MS=150;
function _freshProgrammaticScrollActive(){
  if(!_programmaticScroll) return false;
  const age=performance.now()-_programmaticScrollSetAt;
  if(!Number.isFinite(age)||age<0||age>PROGRAMMATIC_SCROLL_VALID_MS){
    _programmaticScroll=false;
    return false;
  }
  return true;
}
function _deferClearProgrammaticScroll(ms){clearTimeout(_programmaticScrollResetTimer);_programmaticScrollResetTimer=setTimeout(()=>{_programmaticScroll=false;},ms||80);}
let _messageJumpScrollGeneration=0;
let _messageJumpScrollOwner=null;
let _messageJumpScrollSettleTimer=0;
function _messageJumpSessionId(){
  if(typeof S!=='undefined'&&S.session&&S.session.session_id) return String(S.session.session_id);
  return '';
}
function _scheduleMessageJumpScrollReconcile(generation,ms){
  if(!_messageJumpScrollOwner||_messageJumpScrollOwner.generation!==generation) return;
  clearTimeout(_messageJumpScrollSettleTimer);
  _messageJumpScrollSettleTimer=setTimeout(()=>_finishMessageJumpScroll(generation),ms||220);
}
function _beginMessageJumpScroll(container){
  const previous=_messageJumpScrollOwner;
  const preserved=previous?previous.preserved:{
    scrollPinned:_scrollPinned,
    messageUserUnpinned:_messageUserUnpinned,
    nearBottomCount:_nearBottomCount,
  };
  clearTimeout(_messageJumpScrollSettleTimer);
  const generation=++_messageJumpScrollGeneration;
  _messageJumpScrollOwner={generation,container,sessionId:_messageJumpSessionId(),preserved};
  // While the jump owner is active, temporarily release the reader pin so a
  // live token arriving between smooth-scroll frames cannot let scrollIfPinned()
  // reclaim the bottom and snap the reader off the jump target (#6621). The
  // preserved snapshot above is what _finishMessageJumpScroll() reconciles
  // against once the jump settles.
  _scrollPinned=false;
  _messageUserUnpinned=true;
  _nearBottomCount=0;
  _programmaticScroll=true;
  _programmaticScrollSetAt=performance.now();
  _scheduleMessageJumpScrollReconcile(generation,300);
  return generation;
}
function _finishMessageJumpScroll(generation){
  const owner=_messageJumpScrollOwner;
  if(!owner||owner.generation!==generation) return;
  if(owner.sessionId!==_messageJumpSessionId()){
    _cancelMessageJumpScroll();
    return;
  }
  clearTimeout(_messageJumpScrollSettleTimer);
  _messageJumpScrollSettleTimer=0;
  const container=owner.container;
  const maxTop=Math.max(0,container.scrollHeight-container.clientHeight);
  const top=Math.max(0,Math.min(Number(container.scrollTop)||0,maxTop));
  const bottomDistance=maxTop-top;
  if(bottomDistance>80){
    _scrollPinned=false;
    _messageUserUnpinned=true;
    _nearBottomCount=0;
  }else{
    _scrollPinned=owner.preserved.scrollPinned;
    _messageUserUnpinned=owner.preserved.messageUserUnpinned;
    _nearBottomCount=owner.preserved.nearBottomCount;
  }
  _lastScrollTop=container.scrollTop;
  _lastMessageClientHeight=container.clientHeight;
  _messageJumpScrollOwner=null;
  _programmaticScroll=false;
  if(typeof _syncScrollToBottomCue==='function'){
    _syncScrollToBottomCue(!_scrollPinned&&bottomDistance>80,{newMessage:_newMessageCueVisible});
  }
  if(typeof _updateSessionStartJumpButton==='function') _updateSessionStartJumpButton();
  // An external-session refresh deferred while the reader was temporarily
  // unpinned for the jump window (#6621) would otherwise stay stranded once the
  // near-tail reconciliation restores follow mode. Flush it when the terminal
  // state is genuinely pinned to the tail.
  if(_scrollPinned&&!_messageUserUnpinned&&typeof _flushDeferredActiveSessionExternalRefresh==='function'){
    _flushDeferredActiveSessionExternalRefresh();
  }
}
function _cancelMessageJumpScroll(){
  ++_messageJumpScrollGeneration;
  clearTimeout(_messageJumpScrollSettleTimer);
  _messageJumpScrollSettleTimer=0;
  // _beginMessageJumpScroll temporarily unpins the reader for the ownership
  // window (#6621); a cancel that isn't a reconcile must put the pre-jump pin
  // state back, or the transient unpinned state would leak. Callers that want a
  // different terminal pin state (e.g. scrollToBottom) set it right after this.
  const owner=_messageJumpScrollOwner;
  if(owner&&owner.preserved){
    _scrollPinned=owner.preserved.scrollPinned;
    _messageUserUnpinned=owner.preserved.messageUserUnpinned;
    _nearBottomCount=owner.preserved.nearBottomCount;
  }
  _messageJumpScrollOwner=null;
  _programmaticScroll=false;
}
let _nearBottomCount=0;
let _lastScrollTop=null;
let _lastMessageClientHeight=null;   // #4702: track scroller height to ignore iOS portrait toolbar-settle reflows (a clientHeight increase fires a scroll event with decreased scrollTop that is NOT a user scroll)
// Sticky-unpin model (#3343 supersedes #3330's proximity re-pin): once the user
// scrolls up, streaming stops auto-following until they return to the bottom or
// click ‚Üì. The upward-intent TIMEOUT mechanism (_lastMessageUpwardIntentMs /
// MESSAGE_UPWARD_INTENT_MS) is removed ‚Äî sticky-unpin makes it unnecessary.
// Keep the non-message intent timestamp at -Infinity so load-time isn't read as
// intent (the #3330 follow-up fix); 0 would mark the first NON_MESSAGE_SCROLL_INTENT
// window after load as suppressed.
let _lastNonMessageScrollIntentMs=-Infinity;
let _messageUserUnpinned=false;
// A monotonic ownership token lets delayed restores distinguish reader input
// that happened after a snapshot from input that merely happened recently.
let _messageScrollInputGeneration=0;
let _bottomSettleToken=0;
let _settleRAF=0;
let _settleRO=null;
let _settleTimer=0;
let _settleFinalTimer=0;
const NON_MESSAGE_SCROLL_INTENT_SUPPRESS_MS=350;
let _touchStartY=null;
let _messageTouchScrollActive=false;
let _lastMessageTouchScrollIntentMs=-Infinity;
let _deferredOlderMessagesTimer=0;
const MESSAGE_TOUCH_SCROLL_SUPPRESS_MS=1200;
// #4970 review: track recent LOW-DELTA upward message-pane wheel intent separately from
// the decisive deltaY<-30 sticky-unpin threshold. A gentle trackpad wheel
// (deltaY:-5) is real user intent but never crosses -30, so without this the
// post-render artifact suppression would swallow it for the whole window.
const MESSAGE_WHEEL_INTENT_SUPPRESS_MS=1200;
let _lastMessageWheelIntentMs=-Infinity;
let _lastMessageScrollIntentMs=-Infinity;
// #4970 review (greptile P1): keyboard scrolling of the message pane (PageUp/Down,
// arrows, Space, Home/End) fires a native `scroll` event with NO wheel/touch/
// scrollbar/non-message intent. Without recording it, a keyboard scroll-up inside
// the post-render artifact window is swallowed and live-follow snaps the reader
// back to the bottom. Stamp a generic scroll-key intent so the suppression skips it.
const MESSAGE_KEY_SCROLL_INTENT_SUPPRESS_MS=1200;
let _lastMessageKeyScrollIntentMs=-Infinity;
let _newMessageCueVisible=false;
let _lastMessageRenderAt=-Infinity;
function _recentMessageRenderArtifactWindow(ms){
  return performance.now()-_lastMessageRenderAt<(ms||1400);
}
function _cancelBottomSettle(){ _cancelMessageJumpScroll(); _bottomSettleToken++; if(_settleRO){ _settleRO.disconnect(); _settleRO=null; } clearTimeout(_settleTimer); clearTimeout(_settleFinalTimer); cancelAnimationFrame(_settleRAF); }
function _markMessageTouchScrollIntent(active=true){
  _messageTouchScrollActive=!!active;
  _lastMessageTouchScrollIntentMs=performance.now();
}
function _recentMessageTouchScrollIntent(){
  return _messageTouchScrollActive || performance.now()-_lastMessageTouchScrollIntentMs<MESSAGE_TOUCH_SCROLL_SUPPRESS_MS;
}
// #4970: true when the reader recently made ANY upward message-pane wheel
// motion, including gentle low-delta trackpad wheels below the -30 sticky-unpin
// threshold. The post-render artifact suppression must NOT fire when this is
// true, otherwise a real gentle scroll-up right after a render gets swallowed.
function _recentMessageWheelIntent(){
  return performance.now()-_lastMessageWheelIntentMs<MESSAGE_WHEEL_INTENT_SUPPRESS_MS;
}
function _recentMessageScrollIntent(){
  // This manual-reader snapshot signal intentionally excludes the raw
  // touch/key recency helpers: those also record near-tail events for render
  // artifact suppression. Only this timestamp is guarded by bottom distance.
  return performance.now()-_lastMessageScrollIntentMs<MESSAGE_WHEEL_INTENT_SUPPRESS_MS
    || (typeof _scrollbarDragActive!=='undefined'&&!!_scrollbarDragActive);
}
// #4970 review (greptile P1): true when the reader recently used the keyboard to
// scroll the message pane. Keyboard scrolls fire a native scroll event with no
// wheel/touch intent, so the post-render artifact suppression must skip them.
function _recentMessageKeyScrollIntent(){
  return performance.now()-_lastMessageKeyScrollIntentMs<MESSAGE_KEY_SCROLL_INTENT_SUPPRESS_MS;
}
function _isMessageReaderUnpinned(){
  return !!_messageUserUnpinned;
}
function _olderMessagesPrefetchReady(){
  const el=document.getElementById('messages');
  if(!el) return false;
  const olderPrefetchPx=Math.max(600,el.clientHeight*1.5);
  return _isSessionEndlessScrollEnabled()&&el.scrollTop<olderPrefetchPx && typeof _messagesTruncated!=='undefined' && _messagesTruncated && typeof _loadOlderMessages==='function';
}
function _scheduleDeferredOlderMessagesLoad(){
  clearTimeout(_deferredOlderMessagesTimer);
  _deferredOlderMessagesTimer=setTimeout(()=>{
    _deferredOlderMessagesTimer=0;
    if(_recentMessageTouchScrollIntent()){
      _scheduleDeferredOlderMessagesLoad();
      return;
    }
    if(_olderMessagesPrefetchReady()) _loadOlderMessages();
  },MESSAGE_TOUCH_SCROLL_SUPPRESS_MS+50);
}
function _recordNonMessageScrollIntent(e){
  const el=document.getElementById('messages');
  const target=e&&e.target;
  if(!el||!target) return;
  if(!el.contains(target)){ _lastNonMessageScrollIntentMs=performance.now(); return; }
  // Capture the guards before cancelling the active owner: cancellation clears
  // the programmatic flag and jump owner, but a low-delta upward wheel must still
  // count as reader takeover when it interrupted an owned scroll.
  const wheelUp=typeof e.deltaY==='number'&&e.deltaY<0;
  const guardedWheelUp=wheelUp&&_freshProgrammaticScrollActive();
  const jumpScrollOwned=typeof _messageJumpScrollOwner!=='undefined'&&!!_messageJumpScrollOwner;
  if(e.type==='touchmove'||(typeof e.deltaY==='number'&&e.deltaY!==0)){
    if(typeof _messageScrollInputGeneration==='number') _messageScrollInputGeneration++;
    if(jumpScrollOwned||e.type==='touchmove'||(typeof e.deltaY==='number'&&e.deltaY< -30)||guardedWheelUp){
      if(typeof _cancelBottomSettle==='function') _cancelBottomSettle();
    }
  }
  // Any message-pane scroll input that interrupts an active jump owner is a
  // reader takeover, regardless of direction or the programmatic-latch age
  // (#6621): _cancelBottomSettle above restores the pre-jump snapshot, so
  // without this a gentle wheel-up OR wheel-down (or a touch scroll) after the
  // latch expires would leave the reader pinned and let the next token snap to
  // the bottom. Establish the unpinned reader-owned state explicitly; a reader
  // who wants the bottom re-pins by reaching it (<=80px) or pressing End.
  if(jumpScrollOwned&&(wheelUp||e.type==='touchmove'||(typeof e.deltaY==='number'&&e.deltaY!==0))){
    _messageUserUnpinned=true;
    _scrollPinned=false;
    _nearBottomCount=0;
  }
  if(typeof e.deltaY==='number'&&e.deltaY<0) _lastMessageWheelIntentMs=performance.now();
  // Keep e.deltaY< -30 as the ordinary direct sticky-unpin threshold.
  if(e.type==='touchmove'||(typeof e.deltaY==='number'&&e.deltaY< -30)||guardedWheelUp){
    if(e.type==='touchmove') _markMessageTouchScrollIntent(true);
    if((typeof e.deltaY==='number'&&e.deltaY< -30)||guardedWheelUp){
      _messageUserUnpinned=true;
      _nearBottomCount=0;
      _scrollPinned=false;
    } else if(e.type==='touchmove'&&_touchStartY!==null&&e.touches&&e.touches[0]){
      // Detect upward-scroll intent on touch: dragging the finger DOWN the
      // screen scrolls the content up into earlier history (scrollTop
      // decreases) ‚Äî the same "user scrolled away" signal the wheel deltaY<0
      // branch and the scroll listener's movedUp branch use. dy>0 = finger
      // moved down = reveal earlier content = unpin.
      const dy=e.touches[0].clientY-_touchStartY;
      if(dy>8){
        _messageUserUnpinned=true;
        _nearBottomCount=0;
        _scrollPinned=false;
      }
    }
  }
  // #4970: record ANY upward message-pane wheel motion as recent wheel intent,
  // including gentle low-delta trackpad wheels (e.g. deltaY:-5) that never reach
  // the decisive -30 sticky-unpin threshold below. The post-render artifact
  // suppression consults _recentMessageWheelIntent() so it cannot swallow a real
  // gentle scroll-up. Ordinarily this does NOT unpin on its own: the <-30 branch
  // and the scroll listener's movedUp branch remain the stable threshold. The
  // exception is an active programmatic-scroll guard. That guard returns before
  // its listener can see the native scroll event, so even a small capture-phase
  // upward wheel input must immediately stop live-tail follow (#6414).
  if(e.type==='touchmove'||(typeof e.deltaY==='number'&&e.deltaY!==0)){
    const bottomDistance=el.scrollHeight-el.scrollTop-el.clientHeight;
    if(bottomDistance>120) _lastMessageScrollIntentMs=performance.now();
  }
}
function _recentNonMessageScrollIntent(){
  return performance.now()-_lastNonMessageScrollIntentMs<NON_MESSAGE_SCROLL_INTENT_SUPPRESS_MS;
}
function _setScrollToBottomCueText(btn, textKey, labelKey){
  if(!btn) return;
  const label=btn.querySelector('.session-jump-btn__text');
  if(label){
    label.setAttribute('data-i18n',textKey);
    label.textContent=(typeof t==='function')?t(textKey):label.textContent;
  }
  btn.setAttribute('data-i18n-aria-label',labelKey);
  btn.setAttribute('data-i18n-title',labelKey);
  const accessible=(typeof t==='function')?t(labelKey):btn.getAttribute('aria-label')||'';
  if(accessible){
    btn.setAttribute('aria-label',accessible);
    btn.setAttribute('title',accessible);
  }
}
function _syncScrollToBottomCue(show, opts){
  const btn=$('scrollToBottomBtn');
  if(!btn) return;
  const newMessage=!!(opts&&opts.newMessage);
  btn.classList.toggle('scroll-to-bottom-btn--new-message',newMessage);
  if(newMessage) _setScrollToBottomCueText(btn,'session_new_message','session_new_message_label');
  else _setScrollToBottomCueText(btn,'session_jump_end','session_jump_end_label');
  btn.style.display=show?'flex':'none';
}
function _showNewMessageScrollCue(){
  _newMessageCueVisible=true;
  _syncScrollToBottomCue(true,{newMessage:true});
}
function _clearNewMessageScrollCue(){
  _newMessageCueVisible=false;
  _syncScrollToBottomCue(false,{newMessage:false});
}
function _maybeShowNewMessageScrollCue(scrollSnapshot){
  const el=document.getElementById('messages');
  if(!el||!scrollSnapshot) return;
  const previousHeight=Number(scrollSnapshot.scrollHeight)||0;
  const distance=el.scrollHeight-el.scrollTop-el.clientHeight;
  if(el.scrollHeight>previousHeight+24 && distance>80) _showNewMessageScrollCue();
  else _syncScrollToBottomCue(distance>80,{newMessage:_newMessageCueVisible});
}
if(typeof document!=='undefined'){
  document.addEventListener('wheel',_recordNonMessageScrollIntent,{capture:true,passive:true});
  document.addEventListener('touchmove',_recordNonMessageScrollIntent,{capture:true,passive:true});
  document.addEventListener('touchstart',function(e){
    const el=document.getElementById('messages');
    if(e.touches&&e.touches[0]) _touchStartY=e.touches[0].clientY;
    if(el&&e.target&&el.contains(e.target)) _markMessageTouchScrollIntent(true);
  },{capture:true,passive:true});
  document.addEventListener('touchend',function(){ _touchStartY=null; if(_messageTouchScrollActive) _markMessageTouchScrollIntent(false); },{capture:true,passive:true});
  document.addEventListener('touchcancel',function(){ _touchStartY=null; if(_messageTouchScrollActive) _markMessageTouchScrollIntent(false); },{capture:true,passive:true});
}
// Reset hook for session-switch ‚Äî called from sessions.js loadSession() to
// prevent the new chat's first scroll comparing against the previous chat's
// scrollTop (Opus stage-302 SHOULD-FIX, #1731 follow-up).
function _resetScrollDirectionTracker(){
  _cancelMessageJumpScroll();
  _clearNewMessageScrollCue();
  _lastScrollTop=null;
  _lastMessageClientHeight=null;
  _messageUserUnpinned=false;
  _scrollPinned=true;
  _nearBottomCount=0;
  _touchStartY=null;
  _messageTouchScrollActive=false;
  _lastMessageTouchScrollIntentMs=-Infinity;
  // #4970 review: also clear low-delta wheel intent on session switch, else a
  // gentle wheel in the previous chat leaves _recentMessageWheelIntent() true
  // into the new chat's first post-render window ‚Äî the artifact then isn't
  // suppressed, falls into movedUp, and falsely unpins live-follow.
  _lastMessageWheelIntentMs=-Infinity;
  _lastMessageScrollIntentMs=-Infinity;
  // #4970 review (greptile P1): same hygiene for keyboard scroll intent.
  _lastMessageKeyScrollIntentMs=-Infinity;
  clearTimeout(_deferredOlderMessagesTimer);
  _deferredOlderMessagesTimer=0;
}
function _resetStreamScrollFollow(){
  // Cancel any in-flight jump owner FIRST: a new stream is a definitive
  // pin-to-follow, and _cancelMessageJumpScroll() restores the pre-jump snapshot
  // (#6621), so it must run BEFORE the pinned-state assignments below or it would
  // undo them and silently disable auto-follow for the new stream.
  _cancelBottomSettle();
  _clearNewMessageScrollCue();
  _messageUserUnpinned=false;
  _scrollPinned=true;
  _nearBottomCount=0;
  _lastScrollTop=null;
  // #4970 review: clear low-delta wheel intent on fresh stream start too, else a
  // gentle upward wheel within the prior 1200ms can under-suppress a genuine
  // no-intent render artifact and silently disable live follow for the new stream.
  _lastMessageWheelIntentMs=-Infinity;
  _lastMessageScrollIntentMs=-Infinity;
  // #4970 review (greptile P1): same hygiene for keyboard scroll intent.
  _lastMessageKeyScrollIntentMs=-Infinity;
}
if(typeof window!=='undefined'){
  window._resetScrollDirectionTracker=_resetScrollDirectionTracker;
  window._resetStreamScrollFollow=_resetStreamScrollFollow;
}
/* ‚îÄ‚îÄ Pull-to-refresh for PWA standalone (Android) ‚îÄ‚îÄ */
(function(){
  if(typeof document==='undefined') return;
  const isStandalone=window.navigator?.standalone||matchMedia('(display-mode:standalone),(display-mode:fullscreen)').matches;
  if(!isStandalone) return;
  const el=document.getElementById('messages');
  if(!el) return;
  let _ptrState=0; // 0=idle, 1=pulling, 2=ready
  let _ptrStartY=0;
  let _ptrCurrentY=0;
  const THRESHOLD=80;
  let _indicator=null;
  function _ptrCreateIndicator(){
    if(_indicator) return;
    _indicator=document.createElement('div');
    _indicator.className='pull-to-refresh-indicator';
    _indicator.innerHTML='<span class="ptr-icon">‚Üì</span> <span class="ptr-text">Pull to refresh</span>';
    el.parentNode.insertBefore(_indicator,el);
  }
  function _ptrUpdate(progress){
    _ptrCreateIndicator();
    const pulling=progress<1;
    _indicator.classList.toggle('active',progress>0);
    const icon=_indicator.querySelector('.ptr-icon');
    const text=_indicator.querySelector('.ptr-text');
    if(icon) icon.classList.toggle('ready',!pulling);
    if(text) text.textContent=pulling?'Pull to refresh':'Release to refresh';
  }
  function _ptrReset(){
    _ptrState=0;
    _ptrStartY=0;
    _ptrCurrentY=0;
    if(_indicator) _indicator.classList.remove('active');
  }
  el.addEventListener('touchstart',function(e){
    if(el.scrollTop>0||_ptrState!==0) return;
    _ptrStartY=e.touches[0].clientY;
    _ptrState=1;
  },{passive:true});
  el.addEventListener('touchmove',function(e){
    if(_ptrState!==1) return;
    _ptrCurrentY=e.touches[0].clientY;
    const pull=_ptrCurrentY-_ptrStartY;
    if(pull<0){ _ptrReset(); return; }
    /* If not at the top, smooth-scroll to top first.
       Next pull gesture will trigger the refresh. */
    if(el.scrollTop>0){
      el.scrollTo({top:0,behavior:'smooth'});
      _ptrReset();
      return;
    }
    const progress=Math.min(pull/THRESHOLD,1);
    _ptrUpdate(progress);
    _ptrState=progress>=1?2:1;
    if(progress>0.3) e.preventDefault();
  },{passive:false});
  el.addEventListener('touchend',function(){
    if(_ptrState===2){
      if(typeof window.refreshSessionList==='function'){
        Promise.resolve(window.refreshSessionList('pull', {force:true, refreshActive:true})).catch(()=>{}).finally(_ptrReset);
      }else{
        window.location.reload();
      }
      return;
    }
    _ptrReset();
  },{passive:true});
  el.addEventListener('touchcancel',_ptrReset,{passive:true});
})();
(function(){
  const el=document.getElementById('messages');
  if(!el) return;
  el.addEventListener('pointerdown',(e)=>{
    if(e.target===el&&e.offsetX>=el.clientWidth){
      if(typeof _cancelBottomSettle==='function') _cancelBottomSettle();
      _scrollbarDragActive=true;
      if(typeof _messageScrollInputGeneration==='number') _messageScrollInputGeneration++;
    }
  },{passive:true});
  window.addEventListener('pointerup',()=>{
    if(!_scrollbarDragActive) return;
    _scrollbarDragActive=false;
    _scheduleMessageVirtualizedRender(true);
  },{passive:true});
  window.addEventListener('pointercancel',()=>{
    if(!_scrollbarDragActive) return;
    _scrollbarDragActive=false;
    _scheduleMessageVirtualizedRender(true);
  },{passive:true});
  window.addEventListener('blur',()=>{ _scrollbarDragActive=false; },{passive:true});
  document.addEventListener('visibilitychange',()=>{
    if(document.visibilityState==='hidden') _scrollbarDragActive=false;
  },{passive:true});
  // #4970 review (greptile P1): record keyboard-driven message-pane scrolling as
  // user intent. PageUp/PageDown, Arrow keys, Space/Shift+Space, Home/End scroll
  // the pane and fire a native scroll event with no wheel/touch intent ‚Äî without
  // this stamp a keyboard scroll-up inside the post-render artifact window is
  // swallowed and live-follow snaps the reader back to the bottom. Only count it
  // when the scroll container (or a descendant) is the active/scrolling target,
  // not when typing in the composer or activating an in-transcript control.
  const _MESSAGE_SCROLL_KEYS=new Set([
    'PageUp','PageDown','ArrowUp','ArrowDown','Home','End','Spacebar',' ',
  ]);
  const _isMessageInteractiveKeyTarget=(node)=>{
    if(!node||!el.contains(node)) return false;
    if(node.tagName==='INPUT'||node.tagName==='TEXTAREA'||node.isContentEditable) return true;
    return !!(node.closest&&node.closest('button,a[href],select,summary,[role="button"],[role="tab"],[role="menuitem"],[contenteditable="true"]'));
  };
  document.addEventListener('keydown',(e)=>{
    if(!e||!_MESSAGE_SCROLL_KEYS.has(e.key)) return;
    const a=document.activeElement;
    const t=e.target;
    // Ignore keys aimed at editable fields (composer, inputs, contenteditable).
    if(a&&(a.tagName==='INPUT'||a.tagName==='TEXTAREA'||a.isContentEditable)) return;
    // Space/Spacebar activates focused transcript controls (buttons, role=button,
    // links, tabs) rather than scrolling. The listener is capture-phase, so target
    // handlers have not yet preventDefault()/stopPropagation()'d; inspect the
    // active/target control path directly.
    if((e.key===' '||e.key==='Spacebar')&&(_isMessageInteractiveKeyTarget(t)||_isMessageInteractiveKeyTarget(a))) return;
    // Count only when the message pane itself is the scroll target: it is focused,
    // contains the focus, or the pointer is over it (keyboard scroll w/o focus).
    if(a===el||el.contains(a)||el.matches(':hover')){
      if(typeof _cancelBottomSettle==='function') _cancelBottomSettle();
      const now=performance.now();
      if(typeof _messageScrollInputGeneration==='number') _messageScrollInputGeneration++;
      _lastMessageKeyScrollIntentMs=now;
      const bottomDistance=el.scrollHeight-el.scrollTop-el.clientHeight;
      if(bottomDistance>120) _lastMessageScrollIntentMs=now;
    }
  },{capture:true,passive:true});
  let _scrollRaf=0;
  el.addEventListener('scroll',()=>{
    if(_messageJumpScrollOwner){
      _scheduleMessageJumpScrollReconcile(_messageJumpScrollOwner.generation);
      return;
    }
    if(_freshProgrammaticScrollActive()) return;
    _scheduleMessageVirtualizedRender();
    _markMessageVirtualScrollActive();
    cancelAnimationFrame(_scrollRaf);
    _scrollRaf=requestAnimationFrame(()=>{
      const top=el.scrollTop;
      const bottomDistance=el.scrollHeight-top-el.clientHeight;
      const nearBottom=bottomDistance<250;
      // #4702: iOS Safari (esp. portrait) resolves its dynamic toolbar height
      // AFTER first paint. When the toolbar collapses the scroller GROWS
      // (clientHeight increases), which fires a scroll event with a DECREASED
      // scrollTop even though the user never scrolled. Without this guard that
      // reflow is misread as an upward scroll and falsely unpins a freshly-opened
      // session, stranding portrait readers at the top (sibling: #4701). On
      // desktop/landscape the scroller height is stable, so `grew` is always
      // false and behavior is byte-identical.
      const grew=_lastMessageClientHeight!==null&&el.clientHeight>_lastMessageClientHeight+1;
      _lastMessageClientHeight=el.clientHeight;
      const movedUp=!grew&&_lastScrollTop!==null&&top<_lastScrollTop-2;
      const movedDown=_lastScrollTop!==null&&top>_lastScrollTop+2;
      // Suppress the post-render scroll artifact: right after renderMessages()
      // rebuilds #msgInner, the browser can emit a non-user upward scroll event.
      // The typeof guards keep this branch inert in unit harnesses that inject
      // the listener body without these helpers (and short-circuit before any
      // call), while production evaluates the real intent/recency helpers.
      // #4970: also require no recent low-delta message-pane wheel intent, so a
      // gentle trackpad scroll-up (deltaY>-30) right after a render still unpins
      // instead of being swallowed for the artifact window.
      // #4970 review: and never suppress while a scrollbar drag is active ‚Äî a
      // manual scrollbar-drag upward scroll inside the window is real intent.
      // typeof guard keeps the #4295 node harness (no _scrollbarDragActive
      // injected) inert via short-circuit.
      // #4970 review (greptile P1): likewise skip suppression when the reader
      // recently scrolled the pane with the keyboard ‚Äî a keyboard scroll-up is
      // real intent that produces a native scroll event with no wheel/touch.
      if(movedUp
        && typeof _recentMessageRenderArtifactWindow==='function'
        && typeof _recentMessageTouchScrollIntent==='function'
        && typeof _recentNonMessageScrollIntent==='function'
        && typeof _recentMessageWheelIntent==='function'
        && typeof _recentMessageKeyScrollIntent==='function'
        && (typeof _scrollbarDragActive==='undefined' || !_scrollbarDragActive)
        && _recentMessageRenderArtifactWindow(1400)
        && !_recentMessageTouchScrollIntent()
        && !_recentNonMessageScrollIntent()
        && !_recentMessageWheelIntent()
        && !_recentMessageKeyScrollIntent()){
        _lastScrollTop=top;
        return;
      }
      _lastScrollTop=top;
      if(movedUp&&bottomDistance>1){
        // Only a real scroll-away unpins. A collapse ABOVE the tail (worklog
        // "Done" fold, thinking/tool card collapse, interim-note collapse) shrinks
        // scrollHeight while the reader is still flush at the tail, so the browser
        // clamps scrollTop DOWN by the collapsed height and fires a scroll event:
        // movedUp is true while bottomDistance stays ~0. Reading that as user
        // intent killed live-follow mid-stream on a reader who never scrolled.
        // The render-artifact suppression below cannot cover it: it needs a
        // renderMessages() within the last 1400ms, and the collapse paths above run
        // from the streaming handlers, which update the DOM incrementally and never
        // stamp _lastMessageRenderAt. A genuine upward scroll always leaves the true
        // bottom first, so it still has bottomDistance>1 here.
        _cancelBottomSettle();
        _nearBottomCount=0;
        _scrollPinned=false;
        _messageUserUnpinned=true;
      }else if(movedDown&&nearBottom){
        _nearBottomCount=_nearBottomCount+1;
        if(_nearBottomCount>=2){
          // Only re-pin when the reader has genuinely reached the true bottom
          // tail (<=80px). nearBottom spans a ~250px band, so proximity alone
          // must NOT clear the sticky unpin flag (#4295) ‚Äî a reader scanning the
          // last lines mid-stream would otherwise get yanked back to the bottom.
          if(!_messageUserUnpinned||bottomDistance<=80){
            _messageUserUnpinned=false;
            _scrollPinned=true;
          }
          _nearBottomCount=0;
        }
      }else if(!_messageUserUnpinned){
        if(nearBottom){
          _nearBottomCount=_nearBottomCount+1;
          if(_nearBottomCount>=2){_scrollPinned=true;_nearBottomCount=0;}
        }else if(!movedUp && window._autoScrollFollow && _scrollPinned){
          // Content-grew-beneath-a-pinned-viewport case (NOT a user scroll-away).
          // During streaming on a tall transcript (esp. mobile, where chunks land
          // fast), new content increases scrollHeight under a stationary viewport,
          // so bottomDistance crosses the nearBottom threshold even though the
          // reader never scrolled (top did NOT move up, _messageUserUnpinned is
          // false). Previously this fell through to `_scrollPinned=false`, killing
          // auto-follow mid-stream: the follow writer and this listener then fought
          // frame-by-frame, the viewport stalled while content kept growing, and it
          // was progressively stranded mid-transcript (the "jump back" report).
          // Keep the pin and re-snap to the true bottom instead of unpinning.
          _nearBottomCount=0;
          if(typeof _setMessageScrollToBottom==='function') _setMessageScrollToBottom();
        }else{
          _nearBottomCount=0;
          _scrollPinned=false;
        }
      }else if(!nearBottom){
        _nearBottomCount=0;
        _scrollPinned=false;
      }
      if(nearBottom) _clearNewMessageScrollCue();
      const showBottomButton=!_scrollPinned && el.scrollHeight-top-el.clientHeight>80;
      _syncScrollToBottomCue(showBottomButton,{newMessage:_newMessageCueVisible});
      if(typeof _updateSessionStartJumpButton==='function') _updateSessionStartJumpButton();
      // Prefetch older messages before the reader hits the hard top. Prepending
      // then preserving scrollTop is seamless only if there is runway left for
      // the user's continued upward wheel/touch movement.
      const olderPrefetchPx=Math.max(600,el.clientHeight*1.5);
      if(_isSessionEndlessScrollEnabled()&&el.scrollTop<olderPrefetchPx && typeof _messagesTruncated!=='undefined' && _messagesTruncated && typeof _loadOlderMessages==='function'){
        if(_recentMessageTouchScrollIntent()) _scheduleDeferredOlderMessagesLoad();
        else _loadOlderMessages();
      }
    });
  });
})();
function _fmtTokens(n){if(!n||n<0)return'0';if(n>=1e6)return(n/1e6).toFixed(1)+'M';if(n>=1e3)return(n/1e3).toFixed(1)+'k';return String(n);}
function _formatTurnDuration(seconds){
  const n=Number(seconds);
  if(!Number.isFinite(n)||n<0)return'';
  const total=Math.max(0,Math.round(n));
  if(total<60)return`${total}s`;
  const h=Math.floor(total/3600);
  const m=Math.floor((total%3600)/60);
  const s=total%60;
  if(h)return`${h}h ${m}m`;
  return`${m}m ${s}s`;
}
function _formatFirstToken(ms){
  const n=Number(ms);
  if(!Number.isFinite(n)||n<0)return'';
  if(n<1000)return`${Math.round(n)}ms`;
  return`${(n/1000).toFixed(2)}s`;
}
function _formatActiveElapsedTimer(seconds){
  const n=Number(seconds);
  if(!Number.isFinite(n)||n<0)return'';
  const total=Math.max(0,Math.floor(n));
  const m=Math.floor(total/60);
  const s=total%60;
  return`${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
}
function _processedElapsedLabel(seconds){
  const text=_formatTurnDuration(seconds);
  return text?t('processed_elapsed',text):'';
}
const _COMPRESSION_ELAPSED_MAX_SECONDS=5*60;
let _compressionElapsedTimer=null;
function _compressionElapsedStartedAt(state){const n=Number(state&&state.startedAt);return Number.isFinite(n)&&n>0?n:null;}
function _compressionElapsedLabel(state){
  const started=_compressionElapsedStartedAt(state);
  if(!started)return'';
  const elapsed=Math.max(0,(Date.now()/1000)-started);
  if(elapsed>=_COMPRESSION_ELAPSED_MAX_SECONDS)return '5+ min';
  return _formatActiveElapsedTimer(elapsed);
}
function _compressionElapsedExpired(state){const started=_compressionElapsedStartedAt(state);return !!(started&&((Date.now()/1000)-started)>=_COMPRESSION_ELAPSED_MAX_SECONDS);}
function _compressionLiveCardNode(){return document.querySelector('[data-live-compression-card="1"][data-compression-started-at]');}
function _compressionLiveCardState(){
  const node=_compressionLiveCardNode();
  const started=Number(node&&node.getAttribute('data-compression-started-at'));
  if(!node||!S.session||!Number.isFinite(started)||started<=0)return null;
  return {sessionId:S.session.session_id,phase:'running',automatic:true,message:node.getAttribute('data-compression-message')||'Auto-compressing context...',startedAt:started};
}
function _updateCompressionElapsedCards(state){
  if(!state)return false;
  return false;
}
function _updateCompressionElapsedTimer(){
  const state=_compressionStateForCurrentSession()||_compressionLiveCardState();
  if(state&&state.automatic&&state.phase==='running'){
    _updateCompressionElapsedCards(state);
    if(_compressionElapsedExpired(state)) _clearCompressionElapsedTimer();
  }else _clearCompressionElapsedTimer();
}
function _startCompressionElapsedTimer(){if(!_compressionElapsedTimer)_compressionElapsedTimer=setInterval(_updateCompressionElapsedTimer,1000);}
function _clearCompressionElapsedTimer(){if(_compressionElapsedTimer){clearInterval(_compressionElapsedTimer);_compressionElapsedTimer=null;}}
let _activityElapsedTimer=null;
let _activityElapsedTimerGroup=null;
function _activityNowSeconds(){return Date.now()/1000;}
function _isActivityTimerGroup(group){
  return !!(group&&group.getAttribute('data-run-activity-group')==='1');
}
function _activityElapsedStartedAt(group){
  if(!group)return null;
  const raw=(group.dataset&&group.dataset.turnStartedAt!==undefined&&group.dataset.turnStartedAt!=='')
    ?group.dataset.turnStartedAt
    :(S.session&&S.session.pending_started_at);
  const started=Number(raw);
  return Number.isFinite(started)&&started>0?started:null;
}
function _activityElapsedLabel(group){
  const started=_activityElapsedStartedAt(group);
  if(!started)return'';
  return _formatActiveElapsedTimer(_activityNowSeconds()-started);
}
function _activityProcessedElapsedLabel(group){
  const started=_activityElapsedStartedAt(group);
  if(!started)return'';
  return _processedElapsedLabel(_activityNowSeconds()-started);
}
function _activitySettledProcessedLabel(group){
  let durationText=_formatTurnDuration(group&&group.dataset&&group.dataset.turnDuration);
  if(!durationText&&group){
    const durationEl=group.querySelector&&group.querySelector('.tool-call-group-duration');
    const legacy=String(durationEl&&durationEl.textContent||'').replace(/^\s*Done in\s+/i,'').trim();
    if(legacy) durationText=legacy;
  }
  return durationText?t('processed_elapsed',durationText):'';
}
function _activityMarkObserved(group, ts){
  if(!group||group.getAttribute('data-live-tool-call-group')!=='1')return;
  const stamp=Number(ts||_activityNowSeconds());
  if(Number.isFinite(stamp)&&stamp>0) group.setAttribute('data-last-activity-at',String(stamp));
}
function _activityLastObservedAge(group){
  const stamp=Number(group&&group.getAttribute('data-last-activity-at'));
  if(!Number.isFinite(stamp)||stamp<=0)return null;
  return Math.max(0,_activityNowSeconds()-stamp);
}
function _activityClockLabel(ts){
  const stamp=Number(ts||_activityNowSeconds());
  if(!Number.isFinite(stamp)||stamp<=0)return'';
  try{return new Date(stamp*1000).toLocaleTimeString([], {hour:'numeric',minute:'2-digit'});}catch(_){return'';}
}
// Full date+time label for the worklog event-time tooltip (title attr). Guards the
// same valid-Date range as _timestampSeconds so a bad epoch never yields "Invalid
// Date" in the tooltip. (#5739)
function _activityFullClockLabel(ts){
  const stamp=Number(ts);
  if(!Number.isFinite(stamp)||stamp<=0||stamp>8.64e12)return'';
  try{
    const d=new Date(stamp*1000);
    if(isNaN(d.getTime()))return'';
    return d.toLocaleString([], {year:'numeric',month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});
  }catch(_){return'';}
}
function _timestampSeconds(value){
  if(value===undefined||value===null||value==='') return null;
  if(value instanceof Date){
    const stamp=value.getTime()/1000;
    return (Number.isFinite(stamp)&&stamp>0&&Math.abs(stamp)<=8.64e12)?stamp:null;
  }
  const numeric=Number(value);
  if(Number.isFinite(numeric)&&numeric>0){
    const stamp=numeric>1e12?numeric/1000:numeric;
    // Reject epochs outside JavaScript's valid Date range (¬±8.64e15 ms = ¬±8.64e12 s);
    // otherwise new Date(stamp*1000) yields "Invalid Date" and renders literally
    // (e.g. a garbage numeric timestamp like 1e20 passes finite/>0). (#5739 gate.)
    return (Number.isFinite(stamp)&&stamp>0&&stamp<=8.64e12)?stamp:null;
  }
  if(typeof value==='string'){
    const text=value.trim();
    if(!text||/^[+-]?(?:\d+\.?\d*|\.\d+)$/.test(text)) return null;
    const parsed=Date.parse(text);
    if(Number.isFinite(parsed)&&parsed>0){
      const stamp=parsed/1000;
      return stamp<=8.64e12?stamp:null;
    }
  }
  return null;
}
function _firstValidTimestampSeconds(...values){
  for(const value of values){
    const stamp=_timestampSeconds(value);
    if(stamp) return stamp;
  }
  return null;
}
function _transparentEventTimestampSeconds(row, opts){
  opts=opts||{};
  for(const key of ['ts','timestamp','created_at']){
    const stamp=_timestampSeconds(opts[key]);
    if(stamp) return stamp;
  }
  const toolCall=opts.toolCall||row&&row._tcData||null;
  if(toolCall&&typeof toolCall==='object'){
    for(const key of ['ts','timestamp','created_at','started_at','completed_at']){
      const stamp=_timestampSeconds(toolCall[key]);
      if(stamp) return stamp;
    }
  }
  if(row&&typeof row.getAttribute==='function'){
    for(const key of ['data-event-at','data-activity-at']){
      const stamp=_timestampSeconds(row.getAttribute(key));
      if(stamp) return stamp;
    }
  }
  if(opts.live===true) return _activityNowSeconds();
  return null;
}
function _syncTransparentEventTimestamp(row, header, opts){
  if(!row||!header) return null;
  opts=opts||{};
  const showEventTimestamp=!(typeof window!=='undefined'&&window._transparentEventTimestamps===false);
  const live=opts.live===true||row.getAttribute&&(
    row.getAttribute('data-live-tid')==='1'||
    row.getAttribute('data-live-thinking')==='1'||
    row.getAttribute('data-live-assistant')==='1'||
    row.getAttribute('data-live-stream-owned')==='1'
  );
  const explicitTs=_firstValidTimestampSeconds(opts.ts, opts.timestamp, opts.created_at);
  const toolCall=opts.toolCall||row&&row._tcData||null;
  const toolTs=toolCall&&typeof toolCall==='object'
    ? _firstValidTimestampSeconds(
      toolCall.ts,
      toolCall.timestamp,
      toolCall.created_at,
      toolCall.started_at,
      toolCall.completed_at
    )
    : null;
  const attrTs=row&&typeof row.getAttribute==='function'
    ? _firstValidTimestampSeconds(
      row.getAttribute('data-event-at'),
      row.getAttribute('data-activity-at')
    )
    : null;
  const ts=explicitTs||toolTs||attrTs||(live?_activityNowSeconds():null);
  const label=ts?_activityClockLabel(ts):'';
  let timeEl=header.querySelector('.transparent-event-time');
  if(!label){
    if(timeEl) timeEl.remove();
    row.removeAttribute('data-event-at');
    row.removeAttribute('data-event-at-source');
    return null;
  }
  const source=explicitTs||toolTs||attrTs?'event':'live';
  row.setAttribute('data-event-at',String(ts));
  row.setAttribute('data-event-at-source',source);
  if(!showEventTimestamp){
    if(timeEl) timeEl.remove();
    return null;
  }
  if(!timeEl){
    timeEl=document.createElement('span');
    timeEl.className='transparent-event-time';
  }
  timeEl.textContent=label;
  // Full date+time tooltip: the bare clock label is date-ambiguous when a settled
  // session is reviewed days later (or a run crosses midnight), and timing is the
  // whole point of this label. (#5739 Fable UX fix.)
  const fullLabel=_activityFullClockLabel(ts);
  if(fullLabel) timeEl.setAttribute('title',fullLabel); else timeEl.removeAttribute('title');
  timeEl.setAttribute('data-event-at',String(ts));
  timeEl.setAttribute('data-event-at-source',source);
  const anchor=header.querySelector('.transparent-event-status,.thinking-card-btn-row,.tool-card-toggle,.thinking-card-toggle');
  if(timeEl.parentNode!==header){
    if(anchor&&anchor.parentNode===header) header.insertBefore(timeEl,anchor);
    else header.appendChild(timeEl);
  }else if(anchor&&timeEl.nextSibling!==anchor){
    header.insertBefore(timeEl,anchor);
  }
  return timeEl;
}
function _activityStatusNode({kind='info',label='',detail='',status='done',ts=null,id=''}){
  const row=document.createElement('div');
  row.className=`agent-activity-status agent-activity-status-${kind} agent-activity-status-${status}`;
  if(id) row.setAttribute('data-activity-event-id',id);
  if(ts) row.setAttribute('data-activity-at',String(ts));
  const iconMap={run:li('play',13),model:li('bot',13),waiting:'<span class="tool-card-running-dot"></span>',thinking:li('lightbulb',13),tool:li('wrench',13),done:li('check',13),warning:li('alert-triangle',13)};
  row.innerHTML=`<span class="agent-activity-status-icon">${iconMap[kind]||li('clock',13)}</span><span class="agent-activity-status-copy"><span class="agent-activity-status-label">${esc(label)}</span>${detail?`<span class="agent-activity-status-detail">${esc(detail)}</span>`:''}</span><span class="agent-activity-status-time">${esc(_activityClockLabel(ts))}</span>`;
  return row;
}
function _appendActivityEvent(group, event){
  if(!group)return null;
  const body=group.querySelector('.tool-call-group-body');
  if(!body)return null;
  const eventId=event&&event.id;
  let row=eventId?body.querySelector(`.agent-activity-status[data-activity-event-id="${CSS.escape(eventId)}"]`):null;
  const next=_activityStatusNode(event||{});
  if(row){row.replaceWith(next);row=next;}
  else{body.appendChild(next);row=next;}
  _activityMarkObserved(group,event&&event.ts);
  return row;
}
function _ensureLiveActivityBaseline(group){
  if(!group||group.getAttribute('data-live-tool-call-group')!=='1')return;
  const started=_activityElapsedStartedAt(group)||_activityNowSeconds();
  if(!group.getAttribute('data-turn-started-at')) group.setAttribute('data-turn-started-at',String(started));
  if(!group.getAttribute('data-last-activity-at')) group.setAttribute('data-last-activity-at',String(started));
  _appendActivityEvent(group,{id:'run-started',kind:'run',label:'Run started',detail:'Observable activity will appear here as the agent works.',status:'done',ts:started});
  const modelLabel=(S.session&&S.session.model)?getModelLabel(S.session.model):'';
  if(modelLabel)_appendActivityEvent(group,{id:'run-model',kind:'model',label:`Model: ${modelLabel}`,detail:S.activeProfile&&S.activeProfile!=='default'?`Profile: ${S.activeProfile}`:'',status:'done',ts:started});
}
function _setActivityElapsedStartedAt(group){
  if(!group||group.getAttribute('data-live-tool-call-group')!=='1')return;
  const started=_activityElapsedStartedAt(group);
  if(started)group.setAttribute('data-turn-started-at',String(started));
}
function _updateActiveActivityElapsedTimer(){
  const group=_activityElapsedTimerGroup;
  if(!group||!group.isConnected||group.getAttribute('data-live-tool-call-group')!=='1'||group.getAttribute('data-live-activity-current')!=='1'){
    _clearActivityElapsedTimer();
    return;
  }
  const durationEl=group.querySelector('.tool-call-group-duration');
  const label=_activityElapsedLabel(group);
  const processedLabel=_activityProcessedElapsedLabel(group);
  if(label){
    group.setAttribute('data-active-turn-elapsed',label);
  }else{
    group.removeAttribute('data-active-turn-elapsed');
  }
  const labelEl=group.querySelector('.tool-worklog-label') || group.querySelector('.tool-call-group-label');
  if(labelEl&&processedLabel){
    labelEl.textContent=processedLabel;
    labelEl.setAttribute('data-sweep-label', processedLabel);
  }
  if(durationEl){
    durationEl.textContent='';
    durationEl.style.display='none';
  }
}
function _startActivityElapsedTimer(group){
  if(!group||group.getAttribute('data-live-tool-call-group')!=='1')return;
  _setActivityElapsedStartedAt(group);
  // Last-resort fallback for recovered live renders that arrive before session metadata.
  if(!group.getAttribute('data-turn-started-at')) group.setAttribute('data-turn-started-at',String(_activityNowSeconds()));
  if(_activityElapsedTimerGroup&&_activityElapsedTimerGroup!==group)_clearActivityElapsedTimer();
  _activityElapsedTimerGroup=group;
  _updateActiveActivityElapsedTimer();
  if(!_activityElapsedTimer)_activityElapsedTimer=setInterval(_updateActiveActivityElapsedTimer,1000);
}
function _clearActivityElapsedTimer(){
  if(_activityElapsedTimer){
    clearInterval(_activityElapsedTimer);
    _activityElapsedTimer=null;
  }
  if(_activityElapsedTimerGroup&&_activityElapsedTimerGroup.isConnected){
    _activityElapsedTimerGroup.removeAttribute('data-active-turn-elapsed');
    const durationEl=_activityElapsedTimerGroup.querySelector('.tool-call-group-duration');
    if(durationEl){durationEl.textContent='';durationEl.style.display='none';}
  }
  _activityElapsedTimerGroup=null;
}

const _MOBILE_CONFIG_BASE_LABEL='Workspace, model, quota, reasoning, and context settings';

function _setCtxCompressButton(btn,text){
  if(!btn)return;
  if(text){
    btn.style.display='';
    btn.textContent=text;
    btn.onclick=function(e){
      if(e)e.stopPropagation();
      const ta=$('msg');
      if(ta){ta.value='/compress ';ta.focus();autoResize();}
    };
  }else{
    btn.style.display='none';
    btn.textContent='';
    btn.onclick=null;
  }
}

function _syncMobileCtxDisplay(state){
  const mobileConfigBtn=$('composerMobileConfigBtn');
  const row=$('composerMobileContextAction');
  const usageLine=$('composerMobileContextUsage');
  const tokensLine=$('composerMobileContextTokens');
  const thresholdLine=$('composerMobileContextThreshold');
  const costLine=$('composerMobileContextCost');
  const compressBtn=$('composerMobileCtxCompressBtn');
  if(!state||!state.visible){
    if(row)row.style.display='none';
    if(mobileConfigBtn){
      mobileConfigBtn.setAttribute('aria-label',_MOBILE_CONFIG_BASE_LABEL);
      mobileConfigBtn.setAttribute('title',_MOBILE_CONFIG_BASE_LABEL);
    }
    _setCtxCompressButton(compressBtn,'');
    // Reset context ring to 0% to clear any stale values from previous sessions
    var arc = document.getElementById('ctx-arc');
    var num = document.getElementById('ctx-num');
    if (arc && num) {
      var circumference = 87.96;
      arc.setAttribute('stroke-dashoffset', circumference);
      num.textContent = '0';
      arc.setAttribute('stroke', '#22c55e');
    }
    return;
  }
  (function updateCtxRing(pct) {
    var arc = document.getElementById('ctx-arc');
    var num = document.getElementById('ctx-num');
    if (!arc || !num) return;
    var offset = 87.96 * (1 - Math.min(pct, 100) / 100);
    arc.setAttribute('stroke-dashoffset', offset);
    num.textContent = Math.round(pct);
    arc.setAttribute('stroke',
      pct <= 50 ? '#22c55e' : pct <= 85 ? '#f97316' : '#ef4444'
    );
  })(state.pct);
  if(mobileConfigBtn){
    mobileConfigBtn.setAttribute('aria-label',`${_MOBILE_CONFIG_BASE_LABEL}; ${state.label}`);
    mobileConfigBtn.setAttribute('title',`${_MOBILE_CONFIG_BASE_LABEL} \u00b7 ${state.label}`);
  }
  if(row){
    row.style.display='';
    row.setAttribute('aria-label',state.label);
    row.classList.toggle('ctx-mid',state.pct>50&&state.pct<=75);
    row.classList.toggle('ctx-high',state.pct>75);
  }
  if(usageLine)usageLine.textContent=state.usageText||'';
  if(tokensLine)tokensLine.textContent=state.tokensText||'';
  if(thresholdLine){
    if(state.thresholdText){
      thresholdLine.style.display='';
      thresholdLine.textContent=state.thresholdText;
    }else{
      thresholdLine.style.display='none';
      thresholdLine.textContent='';
    }
  }
  if(costLine){
    if(state.costText){
      costLine.style.display='';
      costLine.textContent=state.costText;
    }else{
      costLine.style.display='none';
      costLine.textContent='';
    }
  }
  _setCtxCompressButton(compressBtn,state.compressText||'');
}

function _mergeUsageForCtxIndicator(latest, fallback){
  const latestObj=(latest&&typeof latest==='object')?latest:{};
  const fallbackObj=(fallback&&typeof fallback==='object')?fallback:{};
  const merged={...latestObj};
  for(const field of [
    'input_tokens','output_tokens','estimated_cost',
    'cache_read_tokens','cache_write_tokens','cache_hit_percent',
    'turn_cache_hit_percent','duration_seconds','tps','gateway_routing',
  ]){
    if(merged[field]==null&&fallbackObj[field]!=null){
      merged[field]=fallbackObj[field];
    }
  }
  if(!(Number(latestObj.context_length)>0)&&Number(fallbackObj.context_length)>0){
    merged.context_length=fallbackObj.context_length;
  }
  for(const field of ['threshold_tokens','last_prompt_tokens']){
    if(latestObj[field]==null&&fallbackObj[field]!=null){
      merged[field]=fallbackObj[field];
    }
  }
  if(!Object.hasOwn(latestObj,'post_compression_context_tokens_estimate')&&fallbackObj.post_compression_context_tokens_estimate!=null){
    merged.post_compression_context_tokens_estimate=fallbackObj.post_compression_context_tokens_estimate;
  }
  return merged;
}

// Context usage indicator in composer footer
function _syncCtxIndicator(usage){
  const wrap=$('ctxIndicatorWrap');
  const el=$('ctxIndicator');
  if(!el)return;
  const ctxHidden=!!(window._composerControlVisibility&&window._composerControlVisibility.hide_composer_context);
  if(ctxHidden){
    if(wrap) wrap.style.display='none';
    _syncMobileCtxDisplay({visible:false});
    return;
  }
  // #1436: Use last_prompt_tokens only ‚Äî NEVER fall back to cumulative
  // input_tokens for the "context window % used" calculation.  input_tokens
  // is summed across all turns, so dividing it by the context window gives a
  // nonsense percentage (often >100%) on long sessions.  When we have no
  // last-prompt data we render "¬∑" + "tokens used" via the !hasPromptTok
  // branch below ‚Äî honest "no data" instead of misleading "890% used".
  const postCompressionEstimate=Number(usage.post_compression_context_tokens_estimate)||0;
  const hasPostCompressionEstimate=postCompressionEstimate>0;
  const promptTok=usage.last_prompt_tokens||0;
  const contextPromptTok=hasPostCompressionEstimate?postCompressionEstimate:promptTok;
  const totalTok=(usage.input_tokens||0)+(usage.output_tokens||0);
  const cacheReadTok=usage.cache_read_tokens||0;
  const cacheWriteTok=usage.cache_write_tokens||0;
  // Default context window to 128K when not provided by backend
  const DEFAULT_CTX=128*1024;
  const ctxWindow=usage.context_length||DEFAULT_CTX;
  const cost=usage.estimated_cost;
  // Show indicator whenever we have any usage data (tokens or cost)
  if(!promptTok&&!totalTok&&!cost&&!cacheReadTok&&!cacheWriteTok){
    if(wrap) wrap.style.display='none';
    _syncMobileCtxDisplay({visible:false});
    return;
  }
  if(wrap){
    // Defensive reset: keep dynamic context display from being stuck hidden.
    wrap.classList.remove('composer-control-hidden');
    wrap.removeAttribute('aria-hidden');
    wrap.style.display='';
  }
  let hasPromptTok=!!promptTok;
  if(hasPostCompressionEstimate) hasPromptTok=true;
  const rawPct=hasPromptTok?Math.round((contextPromptTok/ctxWindow)*100):0;
  const pct=Math.min(100,rawPct);
  const overflowed=rawPct>100;
  const ring=$('ctxRingValue');
  const center=$('ctxPercent');
  const usageLine=$('ctxTooltipUsage');
  const tokensLine=$('ctxTooltipTokens');
  const thresholdLine=$('ctxTooltipThreshold');
  const costLine=$('ctxTooltipCost');
  if(ring){
    const circumference=61.261056745;
    ring.style.strokeDasharray=String(circumference);
    ring.style.strokeDashoffset=String(circumference*(1-pct/100));
  }
  if(center) center.textContent=hasPromptTok?String(pct):'\u00b7';
  const hasExplicitCtx=!!usage.context_length;
  el.classList.toggle('ctx-mid',pct>50&&pct<=75);
  el.classList.toggle('ctx-high',pct>75);
  // ‚îÄ‚îÄ Compress affordance (#524) ‚îÄ‚îÄ
  // Show a hint in the tooltip when context usage is high so users
  // discover /compress without having to know the slash command.
  const compressWrap=$('ctxTooltipCompress');
  const compressBtn=$('ctxCompressBtn');
  const compressText=pct>=75?t('ctx_compress_action'):(pct>=50?t('ctx_compress_hint'):'');
  if(compressWrap) compressWrap.style.display=compressText?'':'none';
  _setCtxCompressButton(compressBtn,compressText);
  const cacheHitPct=usage.cache_hit_percent;
  const cacheText=cacheHitPct!=null?t('usage_cache_hit_detail',cacheHitPct,_fmtTokens(cacheReadTok),_fmtTokens(cacheWriteTok)):'';
  const contextLabel=hasPostCompressionEstimate?'Estimated next model context':'Context window';
  let label=hasPromptTok?`${contextLabel} ${pct}% used`:`${_fmtTokens(totalTok)} tokens used`;
  if(!hasExplicitCtx&&hasPromptTok) label+=' (est. 128K)';
  if(cost) label+=` \u00b7 $${cost<0.01?cost.toFixed(4):cost.toFixed(2)}`;
  if(cacheText) label+=` \u00b7 ${cacheText}`;
  el.setAttribute('aria-label',label);
  const usageText=hasPromptTok?(overflowed?`${contextLabel}: ${rawPct}% used (context exceeded)`:`${contextLabel}: ${pct}% used (${100-pct}% left)`):`${_fmtTokens(totalTok)} tokens used`;
  const tokensText=hasPromptTok?`${contextLabel}: ${_fmtTokens(contextPromptTok)} / ${_fmtTokens(ctxWindow)} tokens used`:`In: ${_fmtTokens(usage.input_tokens||0)} \u00b7 Out: ${_fmtTokens(usage.output_tokens||0)}`;
  if(usageLine) usageLine.textContent=usageText;
  if(tokensLine) tokensLine.textContent=tokensText;
  const threshold=usage.threshold_tokens||0;
  let thresholdText='';
  if(thresholdLine){
    if(threshold&&ctxWindow){
      thresholdText=`Auto-compress at ${_fmtTokens(threshold)} (${Math.round(threshold/ctxWindow*100)}%)`;
      thresholdLine.style.display='';
      thresholdLine.textContent=thresholdText;
    }else{
      thresholdLine.style.display='none';
      thresholdLine.textContent='';
    }
  }
  let costText='';
  if(costLine){
    if(cost){
      costText=`Estimated cost: $${cost<0.01?cost.toFixed(4):cost.toFixed(2)}`;
      if(cacheText) costText+=` \u00b7 ${cacheText}`;
      costLine.style.display='';
      costLine.textContent=costText;
    }else if(cacheText){
      costText=cacheText;
      costLine.style.display='';
      costLine.textContent=costText;
    }else{
      costLine.style.display='none';
      costLine.textContent='';
    }
  }
  _syncMobileCtxDisplay({
    visible:true,
    hasPromptTok,
    pct,
    label,
    usageText,
    tokensText,
    thresholdText,
    costText,
    compressText
  });
}

// ‚îÄ‚îÄ Touch support: toggle context tooltip on tap (#524) ‚îÄ‚îÄ
// Hover/focus still exposes the compact tooltip, but a click/tap now opens the
// shared composer config menu used by the phone footer so the richer context
// details and compress action have one interaction path.
document.addEventListener('DOMContentLoaded',function(){
  const wrap=document.getElementById('ctxIndicatorWrap');
  const tooltip=document.getElementById('ctxTooltip');
  if(!wrap||!tooltip)return;
  const btn=document.getElementById('ctxIndicator');
  if(!btn)return;
  btn.addEventListener('click',openComposerContextMenu);
  // Close on outside tap
  document.addEventListener('click',function(){
    tooltip.classList.remove('ctx-tooltip-active');
    tooltip.setAttribute('aria-hidden','true');
  },{passive:true});
  // Prevent tooltip click from closing itself
  tooltip.addEventListener('click',function(e){e.stopPropagation();});
});

function _setMessageScrollToBottom(){
  const el=$('messages');
  if(!el) return;
  _programmaticScroll=true;_programmaticScrollSetAt=performance.now();
  el.scrollTop=el.scrollHeight;
  _lastScrollTop=el.scrollTop;_lastMessageClientHeight=el.clientHeight;
  _nearBottomCount=2;
  _scrollPinned=true;
  requestAnimationFrame(()=>{
    // Retry the bottom write on the next layout frame so a DOM rebuild that
    // grows the transcript after the first write doesn't strand a pinned
    // conversation mid-scroll (#3319). But by this frame the user may have
    // scrolled up ‚Äî under the sticky-unpin model (#3343) _messageUserUnpinned
    // is the authoritative "user scrolled away" signal, so DON'T snap them back
    // or re-pin if so; only release the programmatic-scroll latch.
    if(_messageUserUnpinned || !_scrollPinned || _recentNonMessageScrollIntent()){
      _deferClearProgrammaticScroll();
      return;
    }
    el.scrollTop=el.scrollHeight;
    _lastScrollTop=el.scrollTop;_lastMessageClientHeight=el.clientHeight;
    _nearBottomCount=2;
    _scrollPinned=true;
    _deferClearProgrammaticScroll();
  });
}
function _isMessagePaneNearBottom(threshold=250){
  const el=$('messages');
  if(!el) return false;
  return el.scrollHeight-el.scrollTop-el.clientHeight<=threshold;
}
function _messageBottomDistance(){
  const el=$('messages');
  if(!el) return 0;
  return el.scrollHeight-el.scrollTop-el.clientHeight;
}
// #5514/#5515: when the composer grows (typing multiple rows, Shift+Enter, a
// multi-line paste / WisprFlow), the flex:1 `.messages` viewport shrinks by the
// same delta. A reader pinned to the bottom is then stranded Œîpx above it ‚Äî the
// transcript appears to "scroll up" one row per composer row, and (the #5515
// half) it reads as a random upward jump during normal use. autoResize() only
// resized the textarea; nothing re-pinned the transcript. Re-pin the bottom, but
// ONLY when the reader is genuinely still pinned (sticky-unpin model: honor
// _messageUserUnpinned so we never yank a reader who scrolled away, and never
// fight a stream that already unpinned). Cheap no-op when not pinned.
function _repinMessagesAfterComposerResize(){
  if(_messageUserUnpinned || !_scrollPinned) return;
  const el=$('messages');
  if(!el) return;
  // Already at/very near the bottom? nothing to do (avoids needless writes while
  // idle-reading a short conversation that isn't scrollable).
  if(_messageBottomDistance()<=1) return;
  if(typeof _setMessageScrollToBottom==='function') _setMessageScrollToBottom();
  else { el.scrollTop=el.scrollHeight; }
}
if(typeof window!=='undefined') window._repinMessagesAfterComposerResize=_repinMessagesAfterComposerResize;
function _shouldFollowMessagesOnDomReplace(){
  // Final stream settlement replaces the live DOM with persisted messages. Keep
  // following only for users who are still pinned or effectively at the tail.
  // A broad near-bottom window causes long answers/mobile readers who scroll up
  // a little to read mid-stream to get snapped back to the bottom on completion.
  return window._autoScrollFollow && !_messageUserUnpinned && (_scrollPinned || _isMessagePaneNearBottom(120));
}
function _followMessagesAfterDomReplace(){
  if(_shouldFollowMessagesOnDomReplace()){
    scrollToBottom();
    return true;
  }
  return false;
}
function _settleMessageScrollToBottom(force, explicit){
  // `explicit` = a user-invoked scroll-to-bottom (End button / scrollToBottom()).
  // When explicit, late-layout settling runs even if Auto-follow is OFF ‚Äî the
  // setting only suppresses AUTOMATIC streaming follow, not a deliberate jump
  // to the bottom. (Codex #4006 r3.)
  // can grow the transcript after the first scroll write. Re-apply the bottom
  // position when content settles so late layout does not leave the viewport
  // above the real end. User scroll increments _bottomSettleToken and cancels.
  //
  // Firefox paints each scrollTop write as a visible reflow step. The old
  // rAF-polling approach read scrollHeight across frames ‚Äî the read itself
  // forced a reflow in Firefox, causing visible jitter.
  //
  // ResizeObserver approach: the browser notifies us when the container
  // resizes (no scrollHeight polling needed). On each notification we write
  // scrollTop once via rAF (batches multiple resize callbacks per frame into
  // a single write). After 300ms of no resize events, the observer disconnects.
  const token=++_bottomSettleToken;
  cancelAnimationFrame(_settleRAF);
  if(_settleRO){ _settleRO.disconnect(); _settleRO=null; }
  clearTimeout(_settleTimer);
  clearTimeout(_settleFinalTimer);

  // Sync write anchors the viewport immediately.
  _setMessageScrollToBottom();

  if(force) return;

  const el=document.getElementById('messages');
  if(!el) return;
  // Observe the GROWING content node, not the scroll container. #messages is the
  // scroller but its box is fixed by the flex layout, so it never resizes ‚Äî the
  // transcript grows inside #msgInner (.messages-inner). Observing #messages
  // would mean the callback never fires. (Codex review #2.)
  const observed=document.getElementById('msgInner')||el;

  // Instance-owned cleanup: close over THIS observer so a stale callback (from a
  // superseded settle) only ever disconnects its own observer, never the newer
  // active one that may now be in the global _settleRO. (Codex review #3.)
  const ro=new ResizeObserver(()=>{
    if(token!==_bottomSettleToken){ ro.disconnect(); if(_settleRO===ro) _settleRO=null; return; }
    if((!window._autoScrollFollow&&!explicit)||!_scrollPinned||_messageUserUnpinned||_recentNonMessageScrollIntent()){
      ro.disconnect(); if(_settleRO===ro) _settleRO=null;
      _programmaticScroll=false;
      return;
    }
    // Write scrollTop once per frame ‚Äî ResizeObserver batches multiple
    // notifications per frame, so this is at most one write per frame.
    cancelAnimationFrame(_settleRAF);
    _settleRAF=requestAnimationFrame(()=>{
      if(token!==_bottomSettleToken) return;
      _setMessageScrollToBottom();
    });
    // After 300ms of quiet, disconnect ‚Äî layout is stable.
    clearTimeout(_settleTimer);
    _settleTimer=setTimeout(()=>{
      if(token!==_bottomSettleToken) return;
      ro.disconnect(); if(_settleRO===ro) _settleRO=null;
      _setMessageScrollToBottom();
    },300);
  });
  _settleRO=ro;
  ro.observe(observed);
  // #4702: for an explicit (user/open) settle, also observe the SCROLLER itself.
  // On iOS the transcript content (#msgInner) may not resize, but the scroller
  // grows when the portrait toolbar collapses after first paint ‚Äî observing both
  // re-anchors the bottom after that late viewport settle. Desktop never resizes
  // here, so this is a no-op off-mobile.
  if(explicit&&observed!==el){ try{ ro.observe(el); }catch(_){ } }

  // Static-content safety net: a fully-static response (no Prism/KaTeX/Mermaid/
  // late images) never resizes after the initial sync write, so the
  // ResizeObserver callback above never fires and its 300ms quiet-timer is never
  // armed. Arm a single 2s top-level fallback so a late settle still runs for
  // that case. The token check inside _settleFinalScroll makes this a no-op if a
  // newer settle started, and it self-skips if the user unpinned. (Review #2/#3.)
  clearTimeout(_settleFinalTimer);
  _settleFinalTimer=setTimeout(()=>{
    if(token!==_bottomSettleToken) return;
    ro.disconnect(); if(_settleRO===ro) _settleRO=null;
    if((!window._autoScrollFollow&&!explicit)||!_scrollPinned||_messageUserUnpinned||_recentNonMessageScrollIntent()){ _programmaticScroll=false; return; }
    _settleFinalScroll(token);
  },2000);
}

function _settleFinalScroll(token){
  if(token!==_bottomSettleToken) return;
  const el=document.getElementById('messages');
  if(!el){ _programmaticScroll=false; return; }
  if(_messageUserUnpinned||!_scrollPinned||_recentNonMessageScrollIntent()||_recentMessageTouchScrollIntent()){
    _programmaticScroll=false;
    return;
  }
  _programmaticScroll=true;_programmaticScrollSetAt=performance.now();
  el.scrollTop=el.scrollHeight;
  _lastScrollTop=el.scrollTop;_lastMessageClientHeight=el.clientHeight;
  _nearBottomCount=2;
  _scrollPinned=true;
  _deferClearProgrammaticScroll();
}
function scrollIfPinned(){
  if(!window._autoScrollFollow) return;
  // A jump-to-question owner is mid-flight: it deliberately holds the reader at
  // the jump target across smooth-scroll frames, so never let a live token
  // reclaim the bottom while it is active (#6621). _finishMessageJumpScroll()
  // reconciles the pin state once the jump settles.
  if(typeof _messageJumpScrollOwner!=='undefined'&&_messageJumpScrollOwner) return;
  if(_messageUserUnpinned){
    // Only scrollToBottom() cleared this flag, so one scroll-up permanently
    // killed auto-follow. Re-pin ONLY when the reader has genuinely returned to
    // the true bottom tail (<=80px), NOT on mere near-bottom proximity ‚Äî the
    // #4295 invariant is that proximity alone (inside the ~250px band) must not
    // re-pin, or a reader scanning the last few lines gets yanked to the bottom
    // mid-stream. Also bail on ANY recent message-pane scroll intent (wheel,
    // key, touch) and non-message intent, so an active scroll-up near the tail
    // is never overridden. Uses the same _nearBottomCount debounce as the
    // scroll listener (~4859-4866).
    if(_recentNonMessageScrollIntent()||_recentMessageScrollIntent()||_recentMessageTouchScrollIntent()||_recentMessageWheelIntent()||_recentMessageKeyScrollIntent()){ _nearBottomCount=0; return; }
    if(_messageBottomDistance()>80){ _nearBottomCount=0; return; }
    _nearBottomCount=_nearBottomCount+1;
    if(_nearBottomCount<2) return;
    _nearBottomCount=0;
    _messageUserUnpinned=false;
    _scrollPinned=true;
  }
  if(!_scrollPinned) return;
  if(_recentNonMessageScrollIntent()) return;
  if(_messageBottomDistance()>500) _setMessageScrollToBottom();
  _settleMessageScrollToBottom(false);
}
function scrollToBottom(){
  // An explicit scroll-to-bottom (End button, or any definitive pin-to-bottom)
  // supersedes a pending jump-to-question reconciliation: cancel the active jump
  // owner first so its deferred _finishMessageJumpScroll() can't restore the
  // pre-jump unpinned snapshot and silently undo this pin (#6621). The jump path
  // itself never calls scrollToBottom(), and while a jump owner is active the
  // reader is unpinned so the internal auto-follow callers don't reach here.
  if(typeof _messageJumpScrollOwner!=='undefined'&&_messageJumpScrollOwner&&typeof _cancelMessageJumpScroll==='function') _cancelMessageJumpScroll();
  _clearNewMessageScrollCue();
  _scrollPinned=true;
  _messageUserUnpinned=false;
  // Write scrollTop once synchronously to anchor the viewport, then let
  // ResizeObserver settle handle any late layout growth (Prism, KaTeX,
  // Mermaid, images).  Using force=false so the observer runs ‚Äî force=true
  // was skipping the observer and causing Firefox paint jumps when
  // renderMessages({preserveScroll:true}) + scrollToBottom() fired back-to-back.
  _setMessageScrollToBottom();
  _settleMessageScrollToBottom(false, true);
  _syncScrollToBottomCue(false,{newMessage:false});
  if(typeof _updateSessionStartJumpButton==='function') _updateSessionStartJumpButton();
  if(typeof _flushDeferredActiveSessionExternalRefresh==='function') _flushDeferredActiveSessionExternalRefresh();
}

function _fmtOllamaLabel(mid){
  const [namePart, ...variantParts] = mid.split(':');
  const variant = variantParts.join(':');
  const _fmt = (s) => {
    const tokens = s.replace(/[-_]/g, ' ').split(' ');
    return tokens.map(t => {
      const alphaOnly = t.replace(/\./g, '');
      if (t.length <= 3 && /^[a-zA-Z.]+$/.test(t)) return t.toUpperCase();
      if (/^\d/.test(alphaOnly)) return t.toUpperCase();
      return t.charAt(0).toUpperCase() + t.slice(1);
    }).join(' ');
  };
  let label = _fmt(namePart);
  if (variant) label += ' (' + _fmt(variant) + ')';
  return label;
}

// Bedrock cross-region inference routing heads. `global` belongs here too: the
// catalog in api/config.py ships six `global.anthropic.claude-*` IDs and the
// first-party routing notes treat that as the canonical Bedrock shape. Keep this
// set byte-identical to _regions in api/config.py ‚Äî backend catalog labels and
// runtime picker fallback labels diverge otherwise.
const _BEDROCK_REGION_PREFIXES = new Set(['us', 'eu', 'apac', 'global', 'us-gov']);
// Vendor namespaces Bedrock/Vertex put in front of the real model id.
const _DOTTED_VENDOR_PREFIXES = new Set([
  'anthropic', 'amazon', 'meta', 'mistral', 'cohere', 'ai21',
  'stability', 'writer', 'deepseek', 'qwen', 'openai', 'google',
  // Bedrock foundation-model vendors added after the first pass. Without these,
  // real IDs rendered with the namespace intact ("Us.luma.ray 2",
  // "Twelvelabs.marengo Embed 2 7", "Ibm.granite 3 8B Instruct").
  'luma', 'twelvelabs', 'ibm', 'nvidia', 'snowflake',
]);
/** Drop a Bedrock/Vertex dotted routing+vendor prefix from a model id.
 *
 *  Only the documented shapes are stripped ‚Äî `<region>.<vendor>.<model>` and
 *  `<vendor>.<model>` ‚Äî plus a trailing `:<n>` provisioned-revision suffix.
 *  Anything else is returned unchanged, so `deepseek.v3`, `foo.bar.baz` and the
 *  version dot in `gpt-4.1` are never rewritten.
 *
 *  Mirrors _strip_dotted_provider_prefix() in api/config.py. */
function _stripDottedModelPrefix(bare){
  const value = String(bare || '');
  if (!value || !value.includes('.') || value.includes('://') || value.startsWith('@')) return value;
  const segs = value.split('.');
  let i = 0;
  if (segs.length - i >= 3 && _BEDROCK_REGION_PREFIXES.has((segs[i] || '').toLowerCase())
      && _DOTTED_VENDOR_PREFIXES.has((segs[i + 1] || '').toLowerCase())) i++;
  if (segs.length - i >= 2 && _DOTTED_VENDOR_PREFIXES.has((segs[i] || '').toLowerCase())) {
    // Dropping the vendor is only safe when what remains still names the model.
    // A bare version remainder (`deepseek.v3`) means the vendor WAS the name.
    const remainder = segs.slice(i + 1).join('.');
    if (!/^v?\d+(?:[.\-]\d+)*$/i.test(remainder)) i++;
  }
  if (i === 0) return value;
  return segs.slice(i).join('.').replace(/:\d+$/, '');
}
function getModelLabel(modelId){
  if(!modelId) return 'Unknown';
  const rawId=String(modelId||'');
  // The catalog is the authority on model identity: the backend knows the real
  // provider/model split and ships an exact `m.label` per routing id, so a
  // catalogued id ‚Äî including a plain-lane `@custom:` id whose model contains
  // colons (`@custom:ollamacloud/qwen3.5:397b`) ‚Äî renders verbatim. The
  // string parsing below is only a legacy fallback for ids the catalog has
  // never seen (pre-hydration, stale sessions, removed providers), where a
  // first-colon split alone cannot tell a `@custom:<slug>:<model>` from a
  // plain-lane `@custom:<model-with-colon>` (#7240).
  if(_dynamicModelLabels[modelId]) return _dynamicModelLabels[modelId];
  // Preserve custom gateway model IDs exactly as configured. A custom id is
  // `@custom:<model>` in the plain custom lane or `@custom:<slug>:<model>` for
  // a named custom provider; the provider slug may itself be an endpoint
  // authority (e.g. `custom:10.8.71.41:8080`). The model portion may contain
  // colons (tag/variant suffixes like `:free`, `:31b`, `:397b`) and vendor
  // slashes, so only the leading provider segment is peeled (#7240).
  // Examples:
  //   @custom:ai_gateway:Qwen3.6-35B-A3B            -> Qwen3.6-35B-A3B
  //   @custom:omni:kg/stepfun/step-3.7-flash:free    -> kg/stepfun/step-3.7-flash:free
  //   @custom:qwen397b-64k                           -> qwen397b-64k
  if(rawId.startsWith('@custom:')){
    const rest=rawId.slice('@custom:'.length);
    const sep=rest.indexOf(':');
    if(sep<0) return rest||rawId;
    // A provider slug is a config key or a host:port authority ‚Äî it never
    // contains a `/`. A slash-bearing first segment is therefore the model
    // itself in the plain custom lane (`@custom:ollamacloud/qwen3.5:397b`
    // must render the whole remainder, not just `397b`), mirroring the
    // `/`-means-routable rule api/config.py applies when building ids.
    if(rest.slice(0,sep).includes('/')) return rest||rawId;
    let model=rest.slice(sep+1);
    // Endpoint-style slug (`custom:10.8.71.41:8080:model`): the `:port` belongs
    // to the provider segment, mirroring the host:port slug check in
    // api/config.py, so it is consumed before the model label starts.
    const portMatch=/^(\d{1,5}):/.exec(model);
    if(portMatch){
      const port=Number(portMatch[1]);
      if(port>=1&&port<=65535){
        const host=rest.slice(0,sep).toLowerCase();
        if(host==='localhost'||host.includes('.')||/^\d{1,3}(\.\d{1,3}){3}$/.test(host)){
          model=model.slice(portMatch[0].length);
        }
      }
    }
    return model||rawId;
  }
  // Static fallback for common models
  const STATIC_LABELS={'openai/gpt-5.4-mini':'GPT-5.4 Mini','openai/gpt-4o':'GPT-4o','openai/o3':'o3','openai/o4-mini':'o4-mini','anthropic/claude-sonnet-4.6':'Sonnet 4.6','anthropic/claude-sonnet-4-5':'Sonnet 4.5','anthropic/claude-haiku-3-5':'Haiku 3.5','google/gemini-3.1-pro-preview':'Gemini 3.1 Pro','google/gemini-3-flash-preview':'Gemini 3 Flash','google/gemini-3.1-flash-lite-preview':'Gemini 3.1 Flash Lite','google/gemini-2.5-pro':'Gemini 2.5 Pro','google/gemini-2.5-flash':'Gemini 2.5 Flash','deepseek/deepseek-v4-flash':'DeepSeek V4 Flash','deepseek/deepseek-v4-pro':'DeepSeek V4 Pro','deepseek/deepseek-chat-v3-0324':'DeepSeek V3 (legacy)','meta-llama/llama-4-scout':'Llama 4 Scout'};
  if(STATIC_LABELS[modelId]) return STATIC_LABELS[modelId];
  // Safe Ollama-tag fallback: strip only the first slash-segment (provider
  // prefix) so multi-slash IDs preserve their vendor hierarchy (#3360).
  // URI-scheme ids (e.g. `gpt://${FOLDER}/deepseek-v4-flash/latest`, provider
  // `yandex:gpt`) must NOT be first-segment-stripped ‚Äî `indexOf('/')` would
  // land inside the `://` and leave `/${FOLDER}/...` path junk (#3429). For a
  // `scheme://authority/path...` id, drop the scheme AND the authority, then
  // pick the model name from the PATH segments only. A version/channel tail
  // (`latest`/`stable`/numeric) is skipped only when a real model segment
  // precedes it ‚Äî never promoting the authority or a container folder (#3429).
  let _last;
  const _uriMatch = /^[a-z][a-z0-9+.-]*:\/\/(.+)$/i.exec(modelId);
  if (_uriMatch) {
    const _all = _uriMatch[1].split('/').filter(Boolean);
    // _all[0] is the authority (folder/host); the model lives in the path tail.
    const _path = _all.slice(1);
    // A pure version/channel tail: named channels, or a bare version number
    // (`v4`, `1.2`, `20231231`) ‚Äî NOT a mixed model name that merely starts
    // with a digit (`2026-model`, `4o-mini`), which must be kept as the label.
    const _isVersionTail = (s) => /^(latest|stable|current|default|v\d[\d.]*|\d[\d.]*)$/i.test(s);
    const _isPlaceholder = (s) => /\$\{[^}]*\}/.test(s);
    // Walk path segments right-to-left; the model name is the LAST segment that
    // is neither a version/channel tail (`latest`, `v4`, `1.2`) nor a `${...}`
    // env-var placeholder. Fall back to the last non-placeholder segment, then
    // the literal last segment. Never returns the authority (`_all[0]`).
    let _pick = '';
    let _lastUsable = '';
    for (let _i = _path.length - 1; _i >= 0; _i--) {
      const _seg = _path[_i];
      if (_isPlaceholder(_seg)) continue;
      if (!_lastUsable) _lastUsable = _seg;
      if (!_isVersionTail(_seg)) { _pick = _seg; break; }
    }
    // Fallbacks: the chosen non-version segment, else the last non-placeholder
    // path segment. NEVER the authority and NEVER a `${...}` placeholder ‚Äî for
    // a degenerate id (`gpt://folder123`, `gpt://folder123/${MODEL}`) fall all
    // the way back to the raw id rather than leak the folder/host or env var.
    const _lastPath = _path[_path.length - 1] || '';
    _last = _pick || _lastUsable || (_lastPath && !_isPlaceholder(_lastPath) ? _lastPath : '') || modelId;
  } else {
    _last = modelId.includes('/') ? (modelId.slice(modelId.indexOf('/')+1) || modelId) : modelId;
  }
  // Strip @provider: prefix if present (e.g. @ollama-cloud:kimi-k2.6)
  if (_last.startsWith('@') && _last.includes(':')) _last = _last.split(':').slice(1).join(':');
  // Bedrock/Vertex ids carry a dotted region + vendor prefix and sometimes a
  // trailing `:<n>` version ‚Äî `us.anthropic.claude-opus-5`,
  // `us.anthropic.claude-sonnet-4-5-20250929-v1:0`. Left intact, the dotted head
  // survives into the label as raw plumbing ("Us.anthropic.claude Opus 5" in the
  // turn footer).
  //
  // Only the documented `<region>.<vendor>.<model>` / `<vendor>.<model>` shapes
  // are stripped, via a CLOSED allow-list. A generic "drop leading letters-only
  // dot segments" loop rewrites any uncatalogued id ‚Äî `deepseek.v3` became "V3"
  // and `foo.bar.baz` became "BAZ". Kept in lockstep with
  // _strip_dotted_provider_prefix() in api/config.py; the paired test
  // tests/test_dotted_model_label.py asserts both agree.
  const _stripped = _stripDottedModelPrefix(_last);
  if (_stripped !== _last) {
    _last = _stripped;
    // The normalized id is what the label tables are keyed on, so retry them ‚Äî
    // `us.anthropic.claude-sonnet-4-5` should land on the same "Sonnet 4.5" as
    // `anthropic/claude-sonnet-4-5` rather than falling through to the raw id.
    if (_dynamicModelLabels[_last]) return _dynamicModelLabels[_last];
    if (STATIC_LABELS[_last]) return STATIC_LABELS[_last];
    if (STATIC_LABELS['anthropic/' + _last]) return STATIC_LABELS['anthropic/' + _last];
    // No table entry: prettify the Claude family the way the tables do ‚Äî drop
    // the `claude-` vendor word, the `-YYYYMMDD` date-pin and `-v1` revision
    // (snapshot noise, not a name), then title-case. Bedrock is the only
    // dotted-prefix source here, so this stays scoped to that path.
    if (/^claude-/i.test(_last)) {
      _last = _last
        .replace(/^claude-/i, '')
        .replace(/-v\d+$/i, '')
        .replace(/-\d{8}$/, '')
        .replace(/-/g, ' ')
        .replace(/\b\w/g, c => c.toUpperCase())
        .trim();
    }
  }
  const looksLikeOllamaTag = /^[a-z0-9][\w.-]*:[\w.-]+$/i.test(_last);
  const atProvider=(rawId.startsWith('@')&&rawId.includes(':'))
    ? rawId.slice(1,rawId.indexOf(':')).toLowerCase()
    : '';
  const allowOllamaFormat=!atProvider||atProvider.startsWith('ollama');
  // Narrow: only apply Ollama formatter to IDs with explicit @ollama prefix or colon-tag format.
  // Avoids reformatting bare provider model IDs like claude-sonnet-4-6 or gpt-4o.
  const looksLikeBareOllamaId = modelId.startsWith('@ollama') || looksLikeOllamaTag;
  const ollamaLabel = _fmtOllamaLabel(_last);
  if (allowOllamaFormat && (modelId.startsWith('ollama/') || modelId.startsWith('@ollama') || looksLikeOllamaTag || looksLikeBareOllamaId) && ollamaLabel !== _last) {
    return ollamaLabel;
  }
  return _last || 'Unknown';
}

function _gatewayProviderName(provider){
  const text=String(provider||'').trim();
  if(!text)return'';
  return text.replace(/^custom:/,'').replace(/[-_]/g,' ').replace(/\b\w/g,c=>c.toUpperCase());
}
function _gatewayRoutingLabel(routing){
  if(!routing)return'';
  const provider=_gatewayProviderName(routing.used_provider||routing.provider);
  return provider?`via ${provider}`:'';
}
function _formatGatewayModelLabel(modelId,labelText,routing){
  if(!routing)return'';
  const usedModel=String(routing.used_model||'').trim();
  const base=usedModel
    ?_compactComposerModelChipLabel(usedModel,getModelLabel(usedModel))
    :_compactComposerModelChipLabel(modelId,labelText||getModelLabel(modelId));
  const via=_gatewayRoutingLabel(routing);
  return via?`${base} ${via}`:base;
}
function _usedModelTurnChipLabel(msg){
  if(!msg)return'';
  // Gateway turns own their model label via _formatGatewayModelLabel (which
  // falls back to msg._usedModel when routing omits used_model), so suppress
  // the additive chip whenever routing metadata is present ‚Äî not only when
  // routing.used_model is set ‚Äî to guarantee one model label per turn.
  if(msg._gatewayRouting)return'';
  const usedModel=String(msg._usedModel||'').trim();
  if(!usedModel)return'';
  return _compactComposerModelChipLabel(usedModel,getModelLabel(usedModel));
}
function _gatewayRoutingFailoverText(routing){
  if(!routing||!routing.has_failover)return'';
  const attempts=Array.isArray(routing.routing)?routing.routing:[];
  const providers=attempts.map(a=>_gatewayProviderName(a&&a.provider)).filter(Boolean);
  const unique=[];providers.forEach(p=>{if(!unique.includes(p))unique.push(p);});
  if(unique.length>=2)return`Failover: ${unique[0]} ‚Üí ${unique[unique.length-1]}`;
  const from=_gatewayProviderName(routing.requested_provider);
  const to=_gatewayProviderName(routing.used_provider);
  if(from&&to&&from!==to)return`Failover: ${from} ‚Üí ${to}`;
  return'Gateway failover detected';
}
function _gatewayModelWarningText(routing){
  if(!routing||!routing.model_changed)return'';
  const requested=getModelLabel(routing.requested_model||'requested model');
  const used=getModelLabel(routing.used_model||'served model');
  return`Model switched: ${requested} ‚Üí ${used}`;
}
function _latestGatewayRoutingForSession(session){
  if(!session)return null;
  if(session.gateway_routing)return session.gateway_routing;
  const history=Array.isArray(session.gateway_routing_history)?session.gateway_routing_history:[];
  return history.length?history[history.length-1]:null;
}

function _stripXmlToolCallsDisplay(s){
  // Strip <function_calls>...</function_calls> blocks emitted by DeepSeek and
  // similar models in their raw response text.  These are processed separately
  // as tool calls; leaving them in the content causes them to render visibly
  // in the settled chat bubble.  (#702)
  // Also handles DSML-prefixed variants from DeepSeek/Bedrock, including
  // spacing variants like "<ÔΩúDSML |function_calls" and truncated prefixes.
  if(!s) return s;
  const lo=String(s).toLowerCase();
  if(lo.indexOf('function_calls')===-1 && lo.indexOf('dsml')===-1) return s;
  // Support both plain <function_calls> and DSML-prefixed variants.
  s=s.replace(/<(?:\s*ÔΩú\s*DSML\s*[ÔΩú|]\s*)?function_calls>[\s\S]*?<\/(?:\s*ÔΩú\s*DSML\s*[ÔΩú|]\s*)?function_calls>/gi,'');
  // Also remove truncated opening tags (missing closing ">" at stream tail).
  s=s.replace(/<(?:\s*ÔΩú\s*DSML\s*[ÔΩú|]\s*)?function_calls(?:>|$)[\s\S]*$/i,'');
  // Remove malformed DSML tag fragments like "<ÔΩúDSML |" that can leak in tokens.
  s=s.replace(/<\s*ÔΩú\s*DSML\s*[ÔΩú|]\s*/gi,'');
  return s.replace(/^\s+/, '');
}

function _sanitizeThinkingDisplayText(text){
  const stripped=_stripXmlToolCallsDisplay(String(text||''));
  return stripped.trim();
}

function _normalizeThinkingEchoCompare(text){
  return String(text||'').replace(/\s+/g,' ').trim();
}

function _stripVisibleAssistantEchoFromThinking(thinkingText, ...visibleTexts){
  const clean=_sanitizeThinkingDisplayText(thinkingText);
  const thinkingNorm=_normalizeThinkingEchoCompare(clean);
  if(!thinkingNorm) return '';
  for(const visibleText of visibleTexts){
    const visibleNorm=_normalizeThinkingEchoCompare(visibleText);
    if(visibleNorm&&visibleNorm===thinkingNorm) return '';
  }
  return clean;
}

function renderMd(raw){
  let s=(raw||'').replace(/\r\n/g,'\n').replace(/\r/g,'\n');
  // ‚îÄ‚îÄ Entity decode: must run FIRST so &gt; lines become > for the blockquote
  // pre-pass below. LLMs sometimes emit HTML-entity-encoded output; without this
  // a blockquote sent as "&gt; text" would never be recognised as a blockquote.
  s=s.replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&').replace(/&quot;/g,'"').replace(/&#39;/g,"'");
  // ‚îÄ‚îÄ Blockquote pre-pass (must run BEFORE every other markdown pass) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
  // Group consecutive >-prefixed lines, strip the > prefix from each line,
  // recursively render the stripped content with the full pipeline, and
  // replace the group with a stash token. This is the only way fenced code,
  // headings, hr, and ordered lists inside a blockquote can render correctly:
  // the per-line passes downstream don't know about > prefixes, and by the
  // time the blockquote handler used to run those passes had already mangled
  // the >-prefixed lines.
  //
  // Walks lines (instead of using a single regex) so >-prefixed lines that
  // sit inside a non-blockquote fenced block (e.g. a shell prompt in a
  // ```bash``` example) are not miscaptured as a blockquote.
  const _bq_stash=[];
  s=(function _applyBlockquotes(input){
    const lines=input.split('\n');
    const out=[];
    let inFence=false;     // inside a non-blockquote backtick fence
    let fenceLen=0;
    let bqStart=-1;
    const flush=(end)=>{
      if(bqStart<0) return;
      // Strip "> " prefix (and bare ">" ‚Üí empty) from each line
      const stripped=lines.slice(bqStart,end).map(l=>l.replace(/^> ?/,'')).join('\n');
      // Recursive call: full pipeline on stripped content. Handles fenced
      // code, headings, hr, ordered/unordered lists, nested blockquotes
      // (>>) ‚Äî anything that renderMd handles at the top level.
      const rendered=renderMd(stripped);
      _bq_stash.push('<blockquote>'+rendered+'</blockquote>');
      // Surround the token with blank lines so the paragraph splitter
      // isolates it as its own chunk (otherwise the token gets wrapped
      // in <p>...<br> with adjacent text, producing invalid HTML).
      out.push('');
      out.push('\x00Q'+(_bq_stash.length-1)+'\x00');
      out.push('');
      bqStart=-1;
    };
    for(let i=0;i<lines.length;i++){
      const line=lines[i];
      if(inFence){
        out.push(line);
        if(_isBacktickFenceClose(line,fenceLen)){inFence=false;fenceLen=0;}
        continue;
      }
      const fenceOpen=_matchBacktickFenceLine(line);
      if(fenceOpen){
        flush(i);
        out.push(line);
        inFence=true;
        fenceLen=fenceOpen.len;
        continue;
      }
      if(/^>/.test(line)){
        if(bqStart<0) bqStart=i;
      } else {
        flush(i);
        out.push(line);
      }
    }
    flush(lines.length);
    return out.join('\n');
  })(s);
  // ‚îÄ‚îÄ MEDIA: token stash (must run first, before any other processing) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
  // Detect MEDIA:<path-or-url> tokens emitted by the agent (e.g. screenshots,
  // generated images) and replace them with inline <img> or download links.
  // Stashed so the path/URL is never processed as markdown.
  const media_stash=[];
  s=s.replace(/MEDIA:([^\s\)\]]+)/g,(token,raw_ref,offset)=>{
    const parts=_mediaTokenParts(s,offset,raw_ref);
    if(!parts) return token;
    media_stash.push(parts[0]);
    return '\x00D'+(media_stash.length-1)+'\x00'+parts[1];
  });
  // ‚îÄ‚îÄ End MEDIA stash ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
  // Pre-pass: decode HTML entities first so markdown processing works correctly.
  // This prevents double-escaping when LLM outputs entities like &lt; &gt; &amp;
  const decode=s=>s.replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&amp;/g,'&').replace(/&quot;/g,'"').replace(/&#39;/g,"'");
  s=decode(s);
  // Pre-pass: convert safe inline HTML tags the model may emit into their
  // markdown equivalents so the pipeline can render them correctly.
  // Only runs OUTSIDE fenced code blocks and backtick spans (stash + restore).
  // Unsafe tags (anything not in the allowlist) are left as-is and will be
  // HTML-escaped by esc() when they reach an innerHTML assignment -- no XSS risk.
  // Fence stash: protect code blocks and backtick spans from all further processing.
  // Must run BEFORE math_stash so $..$ inside code spans is not extracted as math.
  // Split into fenced blocks (\x00P ‚Äî kept stashed until after all markdown passes)
  // and inline backtick spans (\x00F ‚Äî restored before bold/italic so **`code`** works).
  // Fenced blocks are converted to <pre><code> here so their content is HTML-escaped
  // and never exposed to list/heading/table regexes that could corrupt the layout.
  // Fixes #1154: diff/patch lines inside fenced blocks (e.g. + added, - removed)
  // were matching the unordered-list regex and injecting <ul>/<li> inside <pre>,
  // breaking </pre> closure and corrupting all subsequent message rendering.
  const _preBlock_stash=[];
  const fence_stash=[];
  // CommonMark ¬ß4.5: opening fence must start a line (with up to 3 spaces of indent)
  // and closing fence must start a line with the same backtick char and at least
  // as many backticks as the opener. Without line/fence-length anchoring, a literal
  // ``` inside a code block (e.g. a nested markdown example) terminates the outer
  // block at the wrong place, leaking content into the markdown stream where
  // bold/italic/inline-code passes corrupt it. Fixes #1438 and #1696.
  s=s.replace(/(^|\n)[ ]{0,3}(`{3,})([^\n`]*)\n(?:([\s\S]*?)\n)?[ ]{0,3}\2`*[ \t]*(?=\n|$)/g,(_,lead,_fence,info,code)=>{
    const langInfo=(info||'').trim();
    const langMatch=langInfo.match(/^(\w[\w+-]*)$/);
    const lang=langMatch?(langMatch[1]||'').trim().toLowerCase():'';
    code=code||'';
    const codeLines=code.split('\n');
    const firstCodeLine=codeLines.find(line=>line.trim())||'';
    const firstMermaidLine=codeLines.map(line=>line.trim()).find(line=>line&&!line.startsWith('%%'))||'';
    const looksLikeLineNumberedToolOutput=/^\s*\d+\|/.test(firstCodeLine);
    const looksLikeMermaidStart=firstMermaidLine==='---'||/^(graph|flowchart|sequenceDiagram|classDiagram|classDiagram-v2|stateDiagram|stateDiagram-v2|erDiagram|journey|gantt|pie|gitGraph|mindmap|timeline|quadrantChart|requirementDiagram|C4Context|C4Container|C4Component|C4Dynamic|c4Context|c4Container|c4Component|c4Dynamic|sankey-beta|block-beta|packet-beta|xychart-beta|kanban|architecture-beta)\b/.test(firstMermaidLine);
    if(lang==='mermaid'&&!looksLikeLineNumberedToolOutput&&looksLikeMermaidStart){
      const id='mermaid-'+Math.random().toString(36).slice(2,10);
      _preBlock_stash.push(`<div class="mermaid-block" data-mermaid-id="${id}">${esc(code.trim())}</div>`);
    } else {
      const h=lang?`<div class="pre-header">${esc(lang)}</div>`:'';
      const langAttr=lang?` class="language-${esc(lang)}"`:'';
      const preClass=/^(md|markdown|mdx)$/.test(lang)?' class="md-source-block"':'';
      // For diff/patch blocks, wrap each line in a colored span
      if(lang==='diff'||lang==='patch'){
        const colored=esc(code.replace(/\n$/,'')).split('\n').map(line=>{
          if(line.startsWith('@@')) return `<span class="diff-line diff-hunk">${line}</span>`;
          if(line.startsWith('+')) return `<span class="diff-line diff-plus">${line}</span>`;
          if(line.startsWith('-')) return `<span class="diff-line diff-minus">${line}</span>`;
          return `<span class="diff-line">${line}</span>`;
        }).join('\n');
        _preBlock_stash.push(`${h}<pre class="diff-block"><code${langAttr}>${colored}</code></pre>`);
      // For JSON/YAML blocks, add tree-view placeholder with raw data
      } else if(lang==='json'||lang==='yaml'){
        const rawCode=esc(code.replace(/\n$/,''));
        // Encode newlines as &#10; to prevent HTML attribute normalization
        // (browsers collapse \n to spaces inside attribute values).
        const rawAttr=rawCode.replace(/"/g,'&quot;').replace(/\n/g,'&#10;');
        const blockId='tree-'+Math.random().toString(36).slice(2,10);
        _preBlock_stash.push(`<div class="code-tree-wrap" data-raw="${rawAttr}" data-lang="${lang}" id="${blockId}">${h}<pre class="tree-raw-view"><code${langAttr}>${rawCode}</code></pre></div>`);
      // CSV blocks ‚Üí render as styled table
      } else if(lang==='csv'){
        const rows=code.replace(/\n$/,'').split('\n').filter(r=>r.trim());
        if(rows.length>=2){
          const headers=rows[0].split(',').map(c=>c.trim());
          const body=rows.slice(1).map(r=>'<tr>'+r.split(',').map(c=>`<td>${esc(c.trim())}</td>`).join('')+'</tr>').join('');
          _preBlock_stash.push(`${h}<div class="csv-table-wrap"><table class="csv-table"><thead><tr>${headers.map(h=>`<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${body}</tbody></table></div>`);
        } else {
          _preBlock_stash.push(`${h}<pre${preClass}><code${langAttr}>${esc(code.replace(/\n$/,''))}</code></pre>`);
        }
      } else {
        _preBlock_stash.push(`${h}<pre${preClass}><code${langAttr}>${esc(code.replace(/\n$/,''))}</code></pre>`);
      }
    }
    return lead+'\x00P'+(_preBlock_stash.length-1)+'\x00';
  });
  s=s.replace(/`([^`\n]+)`/g,(_,c)=>{fence_stash.push('<code>'+esc(c)+'</code>');return '\x00F'+(fence_stash.length-1)+'\x00';});
  // Math stash: protect $$..$$ and $..$ from markdown processing
  // Runs AFTER fence_stash so backtick code spans protect their dollar-sign contents
  const math_stash=[];
  // Display math: $$...$$ and \[...\] (must come before inline to avoid mis-parsing)
  s=s.replace(/\$\$([\s\S]+?)\$\$/g,(_,m)=>{math_stash.push({type:'display',src:m});return '\x00M'+(math_stash.length-1)+'\x00';});
  // Match a single literal backslash before the display delimiter (the common LLM form).
  s=s.replace(/\\\[([\s\S]+?)\\\]/g,(_,m)=>{math_stash.push({type:'display',src:m});return '\x00M'+(math_stash.length-1)+'\x00';});
  // Inline math: $...$ ‚Äî require non-space/non-digit at opening boundary to avoid
  // false positives on currency like "$1,000 xu·ªëng ~$95" or "costs $5 and $10".
  // Aligns with smd's se() guard which also rejects $ followed by digits.
  s=s.replace(/\$([^\s$\d\n][^$\n]*?[^\s$\n]|[^\s\d])\$/g,(_,m)=>{if(m.includes(' | '))return '\$'+m+'\$';math_stash.push({type:'inline',src:m});return '\x00M'+(math_stash.length-1)+'\x00';});
  // Also stash \(...\) LaTeX delimiters.
  // Match a single literal backslash before the delimiter (the common LLM form).
  s=s.replace(/\\\((.+?)\\\)/g,(_,m)=>{math_stash.push({type:'inline',src:m});return '\x00M'+(math_stash.length-1)+'\x00';});
  // Safe tag ‚Üí markdown equivalent (these produce the same output as **text** etc.)
  // Stash raw <pre> blocks so the inline <code> rewrite below does not run
  // inside them. Running that rewrite in <pre> content can introduce stray
  // backticks for multiline code and break subsequent code-box rendering.
  const rawPreStash=[];
  s=s.replace(/(<pre\b[^>]*>[\s\S]*?<\/pre>)/gi,m=>{rawPreStash.push(m);return `\x00R${rawPreStash.length-1}\x00`;});
  // Bare file:// artifact links ‚Üí media. Some gateway/tool surfaces emit bare
  // file:// links for local artifacts instead of MEDIA: tokens; browser clients
  // cannot open the server filesystem directly, so route them through /api/media.
  // Runs AFTER fenced-block (\x00P), inline-code (\x00F), AND raw-<pre> (\x00R)
  // stashing so a file:// inside any code/preformatted region stays literal text
  // (#3219/#3234). Only bare URLs (line-start or whitespace-delimited) match, so
  // normal [label](file://...) markdown anchors keep the link path below.
  s=s.replace(/(^|\s)(file:\/\/[^\s<>"')\]]+)/g,(_,lead,raw_ref)=>{
    media_stash.push(raw_ref);
    return lead+'\x00D'+(media_stash.length-1)+'\x00';
  });
  s=s.replace(/<strong>([\s\S]*?)<\/strong>/gi,(_,t)=>'**'+t+'**');
  s=s.replace(/<b>([\s\S]*?)<\/b>/gi,(_,t)=>'**'+t+'**');
  // Keep boundary whitespace OUTSIDE the generated *...* delimiters: the inline
  // emphasis regex below deliberately rejects a leading/trailing space (so
  // `a * b * c` is not italicised), and `<em> x </em>` would otherwise degrade
  // into literal asterisks (or a bullet list at line start).
  const _emphasis=(t)=>{
    const m=String(t).match(/^(\s*)([\s\S]*?)(\s*)$/);
    return (m && m[2]) ? m[1]+'*'+m[2]+'*'+m[3] : t;
  };
  s=s.replace(/<em>([\s\S]*?)<\/em>/gi,(_,t)=>_emphasis(t));
  s=s.replace(/<i>([\s\S]*?)<\/i>/gi,(_,t)=>_emphasis(t));
  s=s.replace(/<code>([^<]*?)<\/code>/gi,(_,t)=>'`'+t+'`');
  // Convert <br> to a newline, EXCEPT inside genuine markdown table rows ‚Äî there a
  // newline would split the row and destroy the table. No sentinel token is used on
  // purpose: any fixed placeholder is attacker-suppliable in message text and would be
  // rewritten on the way out.
  // The "is this a table row" test must match the DOWNSTREAM table parser's grammar
  // exactly (a run of pipe-lines whose SECOND line is a separator). A looser per-line
  // test would treat pipe-wrapped prose like `| note<br># heading |` as a table row and
  // silently strip its heading/list rendering.
  {
    const _rowRe=/^ {0,3}\|.+\|[ \t]*$/;
    const _sepRe=/^\|[\s|:-]+\|$/;
    const _lines=s.split('\n');
    const _isTableRow=new Array(_lines.length).fill(false);
    for(let i=0;i<_lines.length;){
      if(!_rowRe.test(_lines[i])){ i++; continue; }
      let j=i;
      while(j<_lines.length && _rowRe.test(_lines[j])) j++;
      // A block qualifies only if the parser would accept it: >=2 rows and a separator
      // in the second position.
      if(j-i>=2 && _sepRe.test(_lines[i+1].trim())){
        for(let k=i;k<j;k++) _isTableRow[k]=true;
      }
      i=j;
    }
    s=_lines.map((line,i)=>_isTableRow[i]?line:line.replace(/<br\s*\/?>/gi,'\n')).join('\n');
  }
  // ‚îÄ‚îÄ Glued-bold-heading lift (issue #1446) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
  // LLMs in thinking/reasoning mode frequently emit a "section header" glued
  // to the end of the previous paragraph with no whitespace, like:
  //
  //   Para 1 text.**Heading to Para 2**
  //
  //   Para 2 text.**Heading to Para 3**
  //
  // CommonMark renders that correctly as paragraph-end inline bold, but the
  // visual effect is a run-on label rather than a section break. Lift the
  // glued bold into its own paragraph when it follows a sentence terminator
  // and is followed by a blank line.
  //
  // Constraints (avoid false positives):
  //   - Trigger only on a sentence terminator (.!?) IMMEDIATELY before `**`
  //     (no space) ‚Äî that pattern is almost always a glued heading, not
  //     intentional emphasis.
  //   - Inner text length ‚â§ 80 chars ‚Äî long bold runs are usually emphasis
  //     prose, not headings.
  //   - Trailing `\n\n` required ‚Äî preserves mid-paragraph emphasis like
  //     "this is **important**." untouched.
  //   - Inner text must not contain newlines or `*` (single-line bold only).
  //   - Runs after fenced code, math, and raw <pre> are stashed, so code
  //     content is protected (see pipeline notes).
  s=s.replace(/([.!?])\*\*([^*\n]{1,80})\*\*\n\n/g,'$1\n\n**$2**\n\n');
  // Inline backtick spans: restore <code> tags produced in the stash callback above.
  // Must happen BEFORE bold/italic so **`code`** ‚Üí <strong><code>code</code></strong>.
  s=s.replace(/\x00F(\d+)\x00/g,(_,i)=>fence_stash[+i]);
  function _isCjkAutolinkChar(ch){
    return /[\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af]/.test(ch||'');
  }
  // Return one URL's exclusive end inside a maximal whitespace-free URL run.
  // nextCjk and nextQuery are suffix tables shared by every URL in that run.
  function _bareAutolinkEnd(run,start,nextCjk,nextQuery){
    const schemeEnd=run.indexOf('://',start)+3;
    let authorityEnd=schemeEnd;
    while(authorityEnd<run.length&&!/[/?#]/.test(run[authorityEnd])) authorityEnd++;
    const pathStart=run[authorityEnd]==='/'?authorityEnd:-1;
    const queryFragmentStart=nextQuery[schemeEnd];
    const firstCjkPath=pathStart<0?-1:nextCjk[pathStart];
    const firstCjkQuery=queryFragmentStart<0?-1:nextCjk[queryFragmentStart+1];
    const boundaryMarks='Ôºå„ÄÇÔºéÔΩ°ÔºõÔºöÔºÅÔºü„ÄÅÔºâ„Äë„Äç„Äã„Äï';
    for(let i=schemeEnd;i<run.length;i++){
      const mark=run[i];
      if(!boundaryMarks.includes(mark)) continue;
      if(run.startsWith('http://',i+1)||run.startsWith('https://',i+1)) return i;
      // UTS #46 maps these three authority characters to an ASCII dot. They
      // are label separators before an ASCII label. Also retain a CJK label
      // when the host prefix already contains raw CJK; this covers real IDNs
      // such as ‰æãÂ≠ê„ÄÇ‰∏≠ÂõΩ without mistaking example.com„ÄÇÂèÇËßÅdocs/ for one.
      if((mark==='„ÄÇ'||mark==='Ôºé'||mark==='ÔΩ°')&&i<authorityEnd
         &&i+1<authorityEnd){
        if(/[A-Za-z0-9_\-]/.test(run[i+1])) continue;
        let cjkLabel=nextCjk[„mwÛFÚµÎ(ö+my◊&‚w&˜FS∞ß–¢ÚÚ3SìcbGVÊ&∆W2‚66Ü˜6V‚6ÚÊ˜&÷¬◊V«Fí◊Fˆˆ¬GW&‚ÜÜÊFgV¬FÚ6˜W∆P¢ÚÚF˜¶V‚&˜w2íó2‰UdU"6VB(	BˆÊ«ívVÁVñÊV«í∆ˆÊr&V6ˆÊñÊr'VÁ2&R‚6∆6∞¢ÚÚ&WfVÁG2%6Ü˜r2V&∆ñW"7FW2"7GV#¢ˆÊ«í6vÜV‚FÜRˆ÷óGFVB&VfóÇó0¢ÚÚv˜'FÇóG2˜v‚&˜r‡¶6ˆÁ7BıE$Â5$TÂEı4UEDƒTEı$ıuÙ4”3∞¶6ˆÁ7BıE$Â5$TÂEı4UEDƒTEı$ıuÙ4ı4ƒ4≥”∞¢ÚÚBÇí&WGW&Á2FÜR∂WíÊ÷RóG6V∆bf˜"‚VÊ∂Ê˜v‚∂Wí¬6ÚBÜ≤ó«∆∆óFW&∆FˆW6‚w@¢ÚÚf∆¬&6≤‚FÜó2&W6ˆ«fW2fñBÇíˆÊ«ívÜV‚FÜR∂Wíó2vVÁVñÊV«íFVfñÊVB¿¢ÚÚ˜FÜW'vó6RW6W2FÜRVÊv∆ó6Ç∆óFW&¬(	B∂VWñÊrFÜR∆&V¬6˜'&V7B&Vf˜&RFÜP¢ÚÚ∆ˆ6∆R∂Wó2&R&W6VÁBñ‚WfW'í'VÊF∆R‚Ñf&∆RUÇìÜ‚f7B÷fˆ∆∆˜r‚ê¶gVÊ7Fñˆ‚˜D˜$FVfV«BÜ∂Wí¬∆óFW&¬¬‚‚Ê&w2ó∞¢G'ó∞¢ñbáGóVˆbC””“vgVÊ7Fñˆ‚ró∞¢6ˆÁ7Bc◊BÜ∂Wí¬‚‚Ê&w2ì∞¢ñbábbbb”÷∂Wíí&WGW&‚c∞¢–¢÷6F6ÇÖÚó≤–¢&WGW&‚∆óFW&√∞ß–¢ÚÚ6∆V‚¬ñ‚÷f∆˜rff˜&FÊ6R7Gñ∆VBˆ‚FÜRWÜó7FñÊr$∆ˆBV&∆ñW"÷W76vW2 ¢ÚÚñ∆¬á6÷Rfó7V¬∆ÊwVvR¬6ÚóB&VG22ÊFófRí‚6Ü˜w2FÜRWÜ7BÜñFFV‡¢ÚÚ6˜VÁC≤FÜRñ∆¬ó2FÜR6∆ñ6≤F&vWBvóFÇ∆VFñÊrW÷6ÜWg&ˆ‚‡¶gVÊ7Fñˆ‚ˆ'Vñ∆EG&Á7&VÁDV&∆ñW%7FW4ff˜&FÊ6RÜÜñFFV‰6˜VÁBó∞¢6ˆÁ7BV√÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢V¬Ê6∆74Ê÷S“wG&Á7&VÁB÷V&∆ñW"◊7FW2s∞¢V¬Á6WDGG&ñ'WFRÇvFF÷Ê6Ü˜"÷V&∆ñW"◊7FW2r¬srì∞¢V¬Á6WDGG&ñ'WFRÇvFF÷Ê6Ü˜"◊66VÊR◊&˜rr¬srì∞¢V¬Á6WDGG&ñ'WFRÇvFF÷Ê6Ü˜"◊6WGF∆VB◊66VÊR◊&˜rr¬srì∞¢V¬Á6WDGG&ñ'WFRÇw&ˆ∆Rr¬v'WGFˆ‚rì∞¢V¬Á6WDGG&ñ'WFRÇwF&ñÊFWÇr¬srì∞¢V¬Á6WDGG&ñ'WFRÇvFF÷V&∆ñW"÷6˜VÁBr≈7G&ñÊrÜÜñFFV‰6˜VÁBíì∞¢ÚÚìÜ‚vóFÇVÊv∆ó6Çf∆∆&6≤¬÷F6ÜñÊrFÜR6ñ&∆ñÊr$WáÊB∆¬"Ú$6ˆ∆∆6R∆¬ ¢ÚÚ6ˆÁG&ˆ«2rBÇíGFW&‚‚∂Wó2∆ófRñ‚FÜRV‚∆ˆ6∆RÜìÜ‚Êß2ì≤BÇíf∆«2&6≤F¢ÚÚV‚f˜"˜FÜW"∆ˆ6∆W2ÊBFÚFÜR∂WíÊ÷Rñb'6VÁB(	B6ÚwV&BvóFÇ∆óFW&¬‡¢6ˆÁ7B∆&V√÷ÜñFFV‰6˜VÁC”””¢Ú˜D˜$FVfV«BÇw6Ü˜uˆV&∆ñW%˜7FWˆˆÊRr¬u6Ü˜rV&∆ñW"7FWrê¢¢˜D˜$FVfV«BÇw6Ü˜uˆV&∆ñW%˜7FW2r¬u6Ü˜rr∂ÜñFFV‰6˜VÁB≤rV&∆ñW"7FW2r∆ÜñFFV‰6˜VÁBì∞¢V¬Á6WDGG&ñ'WFRÇv&ñ÷∆&V¬r∆∆&V¬ì∞¢V¬ÊñÊÊW$ÖD‘√÷«7‚6∆73“'G&Á7&VÁB÷V&∆ñW"◊7FW2÷6ÜWg&ˆ‚#‚G∂∆íÇv6ÜWg&ˆ‚◊Wr√2ó”¬˜7„„«7‚6∆73“'G&Á7&VÁB÷V&∆ñW"◊7FW2÷∆&V¬#‚G∂W62Ü∆&V¬ó”¬˜7„Ê∞¢V¬ÊFDWfVÁD∆ó7FVÊW"Çv∂WñF˜v‚r¬ÜWbì”Á∞¢ñbÜWbÊ∂Wì””“tVÁFW"w«∆WbÊ∂Wì””“rró≤WbÁ&WfVÁDFVfV«BÇì≤V¬Ê6∆ñ6≤Çì≤–¢“ì∞¢&WGW&‚V√∞ß–¢ÚÚ÷FW&ñ∆ó¶RFÜRˆ÷óGFVB&VfóÇ&˜w2f˜"6VB6WGF∆VBG&Á7&VÁBGW&‚¿¢ÚÚ&W6W'fñÊrFÜR&VFW"w2fñWw˜'B˜6óFñˆ‚á&˜w2&RñÁ6W'FVB$ıdRFÜR6∆ñ6∂V@¢ÚÚff˜&FÊ6R¬6ÚvóFÜ˜WB6ˆ◊VÁ6Fñˆ‚FÜR6ˆÁFVÁB&V∆˜rv˜V∆BßV◊F˜v‚í‡¶gVÊ7Fñˆ‚˜&WfV≈G&Á7&VÁDV&∆ñW%7FW2Ü÷W76vR¬6Vv÷VÁB¬&tñGÇ¬ff˜&FÊ6TV¬ó∞¢6ˆÁ7BGW&‰V√◊6Vv÷VÁBÊ6∆˜6W7BÇrÊ76ó7FÁB◊GW&‚rì∞¢ÚÚ3SìcbÑ6ˆFWÇc2ì¢&V6˜&BFÜR&WfV¬ñ‚FÜRU%4ï5DTÂB6WBá7W'fófW2&V'Vñ∆B¢ÚÚ7vóF6Ç÷víÚ66ÜR&˜VÊB◊G&óíÊBñÁf∆ñFFRFÜó26W76ñˆ‚w266ÜVBÖD‘¬6¢ÚÚFÜR7F˜&VB÷&∑Wó6‚wB&R◊6W'fVB7F∆R÷6VB‡¢6ˆÁ7B&WfVƒ∂Wì’˜G&Á7&VÁE&WfVƒ∂WíÖ2Á6W76ñˆ‚be2Á6W76ñˆ‚Á6W76ñˆÂˆñB¬&tñGÇì∞¢˜G&Á7&VÁE&WfV∆VEGW&Á2ÊFBá&WfVƒ∂Wíì∞¢G'ó∞¢6ˆÁ7B6ñC’2Á6W76ñˆ‚be2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢ñbá6ñBbe˜6W76ñˆ‰áF÷ƒ66ÜRbgGóVˆb˜6W76ñˆ‰áF÷ƒ66ÜRÊFV∆WFS””“vgVÊ7Fñˆ‚rí˜6W76ñˆ‰áF÷ƒ66ÜRÊFV∆WFRá6ñBì∞¢÷6F6ÇÖÚó≤–¢ñbáGW&‰V¬ó∞¢GW&‰V¬Á6WDGG&ñ'WFRÇvFF◊G&Á7&VÁB÷V&∆ñW"◊&WfV∆VBr¬srì∞¢ÚÚgV∆¬'V‚Ê˜r÷˜VÁFVB(i"G&˜FÜR6VB÷6˜VÁB7F6Ç6ÚFÜRG&6R∆&V¿¢ÚÚ&V6ˆ◊WFW2g&ˆ“FÜRÜÊ˜r6ˆ◊∆WFRíDÙ“‡¢GW&‰V¬Á&V÷˜fTGG&ñ'WFRÇvFF◊G&Á7&VÁB◊F˜F¬◊Fˆˆ¬÷6˜VÁBrì∞¢–¢6ˆÁ7B◊6w4V√“BÇv÷W76vW2rì∞¢6ˆÁ7B&We67&ˆ∆≈F˜÷◊6w4V√ˆ◊6w4V¬Á67&ˆ∆≈F˜£∞¢6ˆÁ7B&We67&ˆ∆ƒÜVñváC÷◊6w4V√ˆ◊6w4V¬Á67&ˆ∆ƒÜVñváC£∞¢6ˆÁ7B66VÊS÷÷W76vRbf÷W76vRÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊS∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‰V¬ì∞¢ñbÇ66VÊW«¬&∆ˆ6∑2ó≤ñbÜff˜&FÊ6TV¬íff˜&FÊ6TV¬Á&V÷˜fRÇì≤&WGW&„≤–¢6ˆÁ7B&˜w3’ˆÊ6Ü˜%66VÊU&˜w4f˜%&VÊFW&ñÊrá66VÊR«∑6WGF∆VCßG'VW“ó«≈µ”∞¢6ˆÁ7B∆7DÊˆÂFW&÷ñÊ≈v˜&µ&˜tñÊFWÉ’ˆÊ6Ü˜%66VÊT∆7DÊˆÂFW&÷ñÊ≈v˜&µ&˜tñÊFWÇá&˜w2ì∞¢6ˆÁ7BfñÊƒÁ7vW#’7G&ñÊrÄ¢á66VÊRbgGóVˆb66VÊRÊfñÊ≈ˆÁ7vW#””“w7G&ñÊrrbg66VÊRÊfñÊ≈ˆÁ7vW"ê¢«¬ˆ76ó7FÁDÊ6Ü˜%66VÊTfñÊƒÁ7vW%FWáBÜ÷W76vRê¢«¬áGóVˆb◊6t6ˆÁFVÁC””“vgVÊ7Fñˆ‚sˆ◊6t6ˆÁFVÁBÜ÷W76vRì¢rrê¢«¬rp¢ì∞¢ÚÚFÜRff˜&FÊ6Rw2FF÷6˜VÁBFV∆«2W2Ü˜r÷Áí&VfóÇ&˜w2FÚ'Vñ∆BáFÜR&˜w0¢ÚÚ&VÊFW&VBˆ‚FÜRñÊóFñ¬72&RFÜRFñ¬gFW"FÜBñÊFWÇí‡¢6ˆÁ7BÜñFFV„‘ÁV÷&W"Üff˜&FÊ6TV¬bfff˜&FÊ6TV¬ÊvWDGG&ñ'WFRÇvFF÷V&∆ñW"÷6˜VÁBríó«√∞¢6ˆÁ7B7F˜ñGÉ÷ÜñFFV„„ˆÜñFFV„•ˆ6ˆ◊WFUG&Á7&VÁDÜñFFVÂ&VfóÑ6˜VÁBá&˜w2ì∞¢6ˆÁ7Bg&s÷Fˆ7V÷VÁBÊ7&VFTFˆ7V÷VÁDg&v÷VÁBÇì∞¢f˜"Ü∆WBñGÉ”∂ñGÉ«7F˜ñGÉ∂ñGÇ≥”ó∞¢6ˆÁ7BÊˆFS’ˆÊ6Ü˜%66VÊUG&Á7&VÁDÊˆFTf˜%&˜rá&˜w5∂ñGÖ“«∑6WGF∆VCßG'VR∆fñÊƒÁ7vW"∆∆ófUFˆ∂V‰fñÊ≈&VfóÑV∆ñvñ&∆S¶ñGÉÊ∆7DÊˆÂFW&÷ñÊ≈v˜&µ&˜tñÊFWá“ì∞¢ñbÜÊˆFRó≤ÊˆFRÁ6WDGG&ñ'WFRÇvFF÷V&∆ñW"◊&WfV∆VBr¬srì≤g&rÊVÊD6Üñ∆BÜÊˆFRì≤–¢–¢ÚÚñÁ6W'BFÜR&VfóÇvÜW&RFÜRff˜&FÊ6R6óG2¬FÜV‚G&˜FÜRff˜&FÊ6R‡¢ñbÜff˜&FÊ6TV¬bfff˜&FÊ6TV¬Á&VÁDV∆V÷VÁC””÷&∆ˆ6∑2ó∞¢&∆ˆ6∑2ÊñÁ6W'D&Vf˜&RÜg&r∆ff˜&FÊ6TV¬ì∞¢ff˜&FÊ6TV¬Á&V÷˜fRÇì∞¢÷V«6W∞¢&∆ˆ6∑2ÊVÊD6Üñ∆BÜg&rì∞¢–¢ñbáGW&‰V¬í˜7ñÊ5G&Á7&VÁDWfVÁD6ˆÁG&ˆ«2áGW&‰V¬ì∞¢ÚÚÜˆ∆BFÜR&VFW"w2˜6óFñˆ„¢&˜w2∆ÊFVB&˜fRFÜRˆ∆Bff˜&FÊ6RˆñÁB¬6¢ÚÚFBFÜRÜVñváBFV«FFÚ67&ˆ∆≈F˜áFÜRw2˜v‚∆ˆB÷V&∆ñW"ñFñˆ“í‡¢ñbÜ◊6w4V¬ó∞¢6ˆÁ7BFV«F÷◊6w4V¬Á67&ˆ∆ƒÜVñváB◊&We67&ˆ∆ƒÜVñváC∞¢◊6w4V¬Á67&ˆ∆≈F˜◊&We67&ˆ∆≈F˜∂FV«F∞¢–ß–¢ÚÚFÜRñÊóFñ¬6VB&VÊFW"ˆ÷óG2&˜w5≥‚‚&˜w2Ê∆VÊwFÇ÷6””≤&V6ˆ◊WFRFÜ@¢ÚÚ&VfóÇ∆VÊwFÇg&ˆ“FÜR7W'&VÁB66VÊR6ÚFÜR&WfV¬ó2WÜ7BWfV‚ñbFÜR6˜VÁ@¢ÚÚGG&ñ'WFRó2÷ó76ñÊrÜ66ÜR&˜VÊB◊G&óí‡¶gVÊ7Fñˆ‚ˆ6ˆ◊WFUG&Á7&VÁDÜñFFVÂ&VfóÑ6˜VÁBá&˜w2ó∞¢6ˆÁ7B6’ıE$Â5$TÂEı4UEDƒTEı$ıuÙ4∞¢6ˆÁ7B6∆6≥’ıE$Â5$TÂEı4UEDƒTEı$ıuÙ4ı4ƒ4≥∞¢&WGW&‚á&˜w2Ê∆VÊwFÉÊ6∑6∆6≤ìÚá&˜w2Ê∆VÊwFÇ÷6ì£∞ß–¢ÚÚˆÊR◊6Ü˜BFˆ∂V„¢FÜR7G&V“ñBˆbFÜRGW&‚FÜB•U5B6WGF∆VBB5E$T’ÙDÙ‰R‡¢ÚÚFÜR∂VW÷˜V‚WÜ6WFñˆ‚∆ñW2FÚÙ‰≈íFÜó2ˆÊRGW&‚w26WGF∆VB&VÊFW"¬FÜV‡¢ÚÚó26∆V&VB6ÚWfW'í˜FÜW"ÜÜó7F˜&ñ6¬í6WGF∆VBv˜&∂∆ˆr&VÊFW'26ˆ◊7BWfV‡¢ÚÚvÜñ∆RFÜR&VFW"ó2ñÊÊVB‚6WB&ñváB&Vf˜&RFÜR5E$T’ÙDÙ‰P¢ÚÚ&VÊFW$÷W76vW2á∑&W6W'fU67&ˆ∆√ßG'VW“í6∆¬ÊB6∆V&VBgFW"FÜR6WGF∆VB◊66VÊP¢ÚÚ&VÊFW"73≤ÁV∆¬B∆¬˜FÜW"Fñ÷W2‡¶∆WBˆ∂VW6WGF∆VEv˜&∂∆ˆt˜V‰f˜%7G&V‘ñC÷ÁV∆√∞¶gVÊ7Fñˆ‚˜6Ü˜V∆D∂VW6WGF∆VEv˜&∂∆ˆt˜V‰f˜%7G&V’6WGF∆Rá7G&V‘ñBó∞¢ÚÚ&˜VÊBb67&ˆ∆¬÷ßV◊wV&C¢6ˆ∆∆6ñÊrFÜR•U5B◊6WGF∆VB∆ófRv˜&∂∆ˆrñÁFÚ¢ÚÚ6ˆ◊7B7V÷÷'íB5E$T’ÙDÙ‰R6á&ñÊ∑2FÜRG&Á67&óB'íáVÊG&VG2ˆbÇ‚FÜP¢ÚÚ&W7V«FñÊr&6∑v&BßV◊ÜóG2&VFW'2ñ‚EtÚ˜6óFñˆÁ2¬6Ú∂VWFÜBˆÊP¢ÚÚv˜&∂∆ˆr˜V‚f˜"$ıDÇˆ‚FÜR6WGF∆R&VÊFW"(	BFÜR∆ófR”Á6WGF∆VBDÙ“7vó0¢ÚÚFÜV‚ÜVñváB◊7F&∆RÊBFÜW&Ró2ÊÚ6á&ñÊ≤f˜"Áí67&ˆ∆¬FÇFÚ÷ó6ÜÊF∆S†¢Ú¢ÚÚ‚î‰‰TBfˆ∆∆˜vW"BFÜR∆ófRFñ√¢FÜR6á&ñÊ≤∆˜vW'267&ˆ∆ƒÜVñváB¬FÜP¢ÚÚ'&˜w6W"6∆◊267&ˆ∆≈F˜FÚFÜRÊWr÷Ç¬ÊBFÜRfñWw˜'B6Ê2Wv&@¢ÚÚWfV‚FÜ˜VvÇñ‚7FFRó26˜'&V7B‡¢ÚÚ"‚TÂî‰‰TB&VFW"vÜÚ67&ˆ∆∆VBUFÚ&VBñÁ6ñFRFÜRßW7B◊6WGF∆VBGW&‡¢ÚÚáFÜR÷ˆ&ñ∆R.[ËYπÓZJ~ã{2"&W˜'B¬4‘Ù$îƒU45$Ùƒ¬fˆ∆∆˜r◊Wì¢FÜRv˜&∂∆ˆr6óG0¢ÚÚ$ıdRFÜVó"fñWw˜'B¬6Ú6ˆ∆∆6ñÊróBV∆«2FÜVó"6ˆÁFVÁBWFÚFÜRF˜ ¢ÚÚˆbFÜRGW&‚‚ˆ‚FW6∑F˜˜fW&f∆˜r÷Ê6Ü˜#¶ÊˆÊR≤FÜR•26Ê6Ü˜B&W7F˜&P¢ÚÚ∂VWFÜV“WB¬'WBˆ‚÷ˆ&ñ∆RFÜR552&W7FñÊrf«VRó2˜fW&f∆˜r÷Ê6Ü˜#†¢ÚÚWFÚ‰BˆfóÑ÷ˆ&ñ∆U67&ˆ∆ƒ¶Ê≤Çíf∆ó2‚ñÊ∆ñÊR˜fW&f∆˜r÷Ê6Ü˜#¶ÊˆÊR˜fW ¢ÚÚFÜR6WGF∆R&VÊFW"(	BvÜñ6Çó2WÜ7F«íFÜRw&ˆÊr7FFS¢ÊFófRÊ6Ü˜&ñÊró0¢ÚÚ7W&W76VBGW&ñÊrFÜRˆÊRg&÷RFÜRVÁñÊÊVB&VFW"ÊVVG2óBFÚ'6˜& ¢ÚÚFÜR&˜fR◊fñWw˜'B6á&ñÊ≤¬6ÚFÜR6ˆÁFVÁB∆V2FÚFÜRGW&‚w2F˜‚∂VWñÊp¢ÚÚFÜRv˜&∂∆ˆr˜V‚&V÷˜fW2FÜR6á&ñÊ≤VÁFó&V«í¬vÜñ6ÇfóÜW2óBf˜"WfW'ê¢ÚÚFWfñ6RˆÊ6Ü˜"÷÷ˆFR6ˆ÷&ñÊFñˆ‚ñÁ7FVBˆbfñváFñÊrFÜRÊ6Ü˜"VÊvñÊR‡¢Ú¢ÚÚ44ıî‰s¢FÜRWÜ6WFñˆ‚ó2vFVBˆ‚FÜRˆÊR◊6Ü˜BFˆ∂V‚÷F6ÜñÊrFÜó2GW&‚w0¢ÚÚ7G&V“ñB¬6ÚóB∆ñW2Ù‰≈íFÚFÜRGW&‚FÜBßW7B6WGF∆VB(	BÊ˜BFÚWfW'ê¢ÚÚÜó7F˜&ñ6¬6WGF∆VBv˜&∂∆ˆrˆ‚WfW'í&R◊&VÊFW"ávÜñ6Çv˜V∆BFVfVBFÜP¢ÚÚ6ˆ◊7B◊v˜&∂∆ˆrFVfV«Bf˜"7BGW&Á2í‡¢&WGW&‚á7G&V‘ñBbeˆ∂VW6WGF∆VEv˜&∂∆ˆt˜V‰f˜%7G&V‘ñC””◊7G&V‘ñBì∞ß–¢ÚÚˆÊR◊6Ü˜BFˆ∂V‚6WBˆ6∆V"íW6VB'íFÜR5E$T’ÙDÙ‰RÜÊF∆W"Ü÷W76vW2Êß2ì†¢ÚÚ&“FÜR∂VW÷˜V‚WÜ6WFñˆ‚f˜"WÜ7F«íFÜRGW&‚FÜBßW7B6WGF∆VB¬&VÊFW"¿¢ÚÚFÜV‚Fó6&“6Ú7V'6WVVÁB&R◊&VÊFW'26ˆ∆∆6RÜó7F˜&ñ6¬v˜&∂∆ˆw22Ê˜&÷¬‡¶gVÊ7Fñˆ‚ˆ&‘∂VW6WGF∆VEv˜&∂∆ˆt˜V‚á7G&V‘ñBó∞¢ˆ∂VW6WGF∆VEv˜&∂∆ˆt˜V‰f˜%7G&V‘ñC◊7G&V‘ñCı7G&ñÊrá7G&V‘ñBì¶ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆFó6&‘∂VW6WGF∆VEv˜&∂∆ˆt˜V‚Çó∞¢ˆ∂VW6WGF∆VEv˜&∂∆ˆt˜V‰f˜%7G&V‘ñC÷ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆ76ó7FÁEGW&‰Ü5fó6ñ&∆U&VÊFW&VE6Vv÷VÁBáGW&‚ó∞¢ñbÇGW&Á««GóVˆbGW&‚ÁVW'ï6V∆V7F˜$∆¬”“vgVÊ7Fñˆ‚rí&WGW&‚ÁV∆√∞¢f˜"Ü6ˆÁ7B6VrˆbGW&‚ÁVW'ï6V∆V7F˜$∆¬ÇrÊ76ó7FÁB◊6Vv÷VÁBríó∞¢ñbá6VrÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6Rríí6ˆÁFñÁVS∞¢ñbá6VrÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv76ó7FÁB◊6Vv÷VÁB÷Ê6Ü˜"ríí6ˆÁFñÁVS∞¢ñbÇá6VrÁFWáD6ˆÁFVÁG«¬rríÁG&ñ“Çíí&WGW&‚G'VS∞¢–¢&WGW&‚f«6S∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ∆∆6TßW7E6WGF∆VEv˜&∂∆ˆtñÂ∆6Rá7G&V‘ñBó∞¢ÚÚ3cCC¢5E$T’ÙDÙ‰RW6VBFÚ&VÊFW"FÜR6WGF∆VBGW&‚ˆÊ6RvóFÇóG2v˜&∂∆ˆp¢ÚÚf˜&6VB˜V‚¬FÜV‚ñ÷÷VFñFV«í'V‚6V6ˆÊBgV∆¬&VÊFW"ˆÊ«íFÚ6ˆ∆∆6RóB‡¢ÚÚFÜB6V6ˆÊBñÊÊW$ÖD‘√“rv&V'Vñ∆B&V÷˜fW2FÜRv˜&∂∆ˆrFÜRW6W"ßW7B6rÊ@¢ÚÚ6‚Wá˜6R&W6WBg&÷R‚∂VWFÜR6ÊˆÊñ6¬fó'7B&VÊFW"¬FÜV‚6ˆ∆∆6RóG0¢ÚÚˆÊR6WGF∆VBw&˜Wñ‚∆6R‚FÜR&˜w2&R&V∆V6VBgFW"FÜRw&˜Wó2ÜñFFV„∞¢ÚÚ‚WáÊB7Fñ∆¬&V'Vñ∆G2FÜV“g&ˆ“FÜR6WGF∆VBG&Á67&óBfñFÜRÊ˜&÷¿¢ÚÚ3SÉ3íFVfW'&VB◊&˜rFÇ‡¢6ˆÁ7BñÊÊW#“BÇv◊6tñÊÊW"rì∞¢ñbÇñÊÊW'«¬7G&V‘ñBí&WGW&‚f«6S∞¢6ˆÁ7Bw&˜W‘'&íÊg&ˆ“ÜñÊÊW"ÁVW'ï6V∆V7F˜$∆¬Çu∂FF÷Ê6Ü˜"◊6WGF∆VB◊66VÊR÷˜vÊW#“#%“ríê¢Êfñ«FW"Ü6ÊFñFFS”Ê6ÊFñFFRÊvWDGG&ñ'WFRÇvFF÷Ê6Ü˜"◊7G&V“÷ñBrì””’7G&ñÊrá7G&V‘ñBíê¢Á˜Çì∞¢ñbÇw&˜Wí&WGW&‚f«6S∞¢6ˆÁ7B˜vÊW%GW&„◊GóVˆbw&˜WÊ6∆˜6W7C””“vgVÊ7Fñˆ‚sˆw&˜WÊ6∆˜6W7BÇrÊ76ó7FÁB◊GW&‚rì¶ÁV∆√∞¢6ˆÁ7B˜vÊW$Ü5fó6ñ&∆U6Vv÷VÁC’ˆ76ó7FÁEGW&‰Ü5fó6ñ&∆U&VÊFW&VE6Vv÷VÁBÜ˜vÊW%GW&‚ì∞¢ñbÜ˜vÊW$Ü5fó6ñ&∆U6Vv÷VÁC””÷ÁV∆¬í&WGW&‚f«6S∞¢6ˆÁ7BFó66∆˜7W&T∂Wì÷w&˜WÊvWDGG&ñ'WFRÇvFF÷7FófóGí÷Fó66∆˜7W&R÷∂Wíró«¬rs∞¢6ˆÁ7B6fVDFó66∆˜7W&S’˜&VD7FófóGîFó66∆˜7W&U7FFRÜFó66∆˜7W&T∂Wíì∞¢6ˆÁ7B&˜w3’ˆFVfW'&VEv˜&∂∆ˆu&˜w4g&ˆ‘w&˜WÜw&˜Wì∞¢ñbÇ&˜w7«¬&˜w2Ê∆VÊwFÇí&WGW&‚f«6S∞¢6ˆÁ7B÷F6É“ıÊÊ6Ü˜"◊66VÊS¢Ö∆B≤íBÚÊWÜV2ÜFó66∆˜7W&T∂Wíì∞¢6ˆÁ7B÷W76vS÷÷F6Çbe2Ê÷W76vW2be2Ê÷W76vW5¥ÁV÷&W"Ü÷F6Ö≥“ï”∞¢6ˆÁ7BW'&˜&VC“Ü÷W76vRbf÷W76vRÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊRb`¢ˆÊ6Ü˜%66VÊTÜ4W'&˜&VEFW&÷ñÊ≈7FFRÜ÷W76vRÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊRíì∞¢6ˆÁ7B∂VW˜V„“˜vÊW$Ü5fó6ñ&∆U6Vv÷VÁ@¢«¬6fVDFó66∆˜7W&S””“v˜V‚p¢«¬ÜW'&˜&VBbg6fVDFó66∆˜7W&R”“v6∆˜6VBrê¢«¬Ö˜v˜&∂∆ˆtFWFñ«4WáÊFVDFVfV«BÇíbg6fVDFó66∆˜7W&R”“v6∆˜6VBrì∞¢ñbÇ∂VW˜V‚ó∞¢6ˆÁ7BFWFñƒFó66∆˜7W&S◊GóVˆbˆ6GW&Uv˜&∂∆ˆtFWFñƒFó66∆˜7W&U7FFS””“vgVÊ7Fñˆ‚p¢Úˆ6GW&Uv˜&∂∆ˆtFWFñƒFó66∆˜7W&U7FFRÜw&˜Wê¢¢ÁV∆√∞¢w&˜WÂˆFVfW'&VEv˜&∂∆ˆu&˜w3◊&˜w3∞¢w&˜WÂˆFVfW'&VEv˜&∂∆ˆtFó66∆˜7W&S÷FWFñƒFó66∆˜7W&RbfFWFñƒFó66∆˜7W&RÁ6ó¶P¢ÚFWFñƒFó66∆˜7W&P¢¢ÁV∆√∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF◊v˜&∂∆ˆr◊&˜w2÷FVfW'&VBr¬srì∞¢w&˜WÊ6∆74∆ó7BÊFBÇwFˆˆ¬÷6∆¬÷w&˜W÷6ˆ∆∆6VBrì∞¢w&˜WÊ6∆74∆ó7BÁ&V÷˜fRÇv˜V‚rì∞¢6ˆÁ7B7V÷÷'ì÷w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬◊v˜&∂∆ˆr◊7V÷÷'í¬ÁFˆˆ¬÷6∆¬÷w&˜W◊7V÷÷'írì∞¢ñbá7V÷÷'íí7V÷÷'íÁ6WDGG&ñ'WFRÇv&ñ÷WáÊFVBr¬vf«6Rrì∞¢˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íÜw&˜Wì∞¢&WVW7DÊñ÷Fñˆ‰g&÷RÇÇì”Á∞¢ñbÇw&˜WÊó46ˆÊÊV7FVG«¬w&˜WÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwFˆˆ¬÷6∆¬÷w&˜W÷6ˆ∆∆6VBríí&WGW&„∞¢ñbÜw&˜WÊvWDGG&ñ'WFRÇvFF◊v˜&∂∆ˆr◊&˜w2÷FVfW'&VBrí”“srí&WGW&„∞¢6ˆÁ7B∆ó7C’˜Fˆˆ≈v˜&∂∆ˆt∆ó7DV¬Üw&˜Wì∞¢ñbÜ∆ó7Bí∆ó7BÁ&W∆6T6Üñ∆G&V‚Çì∞¢“ì∞¢–¢&WGW&‚G'VS∞ß–¢ÚÚG'VRvÜñ∆RßW7B◊6WGF∆VBv˜&∂∆ˆró2&VñÊrf˜&6R◊&VÊFW&VB˜V‚Ü&WGvVV‡¢ÚÚˆ&‘∂VW6WGF∆VEv˜&∂∆ˆt˜V‚ÊBˆFó6&‘∂VW6WGF∆VEv˜&∂∆ˆt˜V‚í‚&VÊFW$÷W76vW2Çê¢ÚÚ6ˆÁ7V«G2FÜó26ÚóBFˆW2‰ıBw&óFRFÜRf˜&6VB÷˜V‚DÙ“ñÁFÚ˜6W76ñˆ‰áF÷ƒ66ÜS†¢ÚÚFÜR∂VW÷˜V‚ó2G&Á6ñVÁB6WGF∆R÷g&÷RFWfñ6R¬ÊB66ÜñÊróBv˜V∆BW'6ó7@¢ÚÚFÜRf˜&6VB÷˜V‚v˜&∂∆ˆr7&˜726W76ñˆ‚7vóF6ÜW2Ú&W7F˜&W2¬6ñ∆VÁF«í˜fW'&ñFñÊp¢ÚÚW6W"÷6ˆ∆∆6VBv˜&∂∆ˆr‚Ç3S#cvFR÷6W'C¢∂VW÷˜V‚◊W7BÊ˜B∆V≤ñÁFÚ66ÜR‚ê¶gVÊ7Fñˆ‚ˆó4∂VW6WGF∆VEv˜&∂∆ˆt˜V‰&÷VBÇó∞¢&WGW&‚ˆ∂VW6WGF∆VEv˜&∂∆ˆt˜V‰f˜%7G&V‘ñB”÷ÁV∆√∞ß–¶ñbáGóVˆbvñÊF˜r”“wVÊFVfñÊVBró∞¢vñÊF˜rÂˆ&‘∂VW6WGF∆VEv˜&∂∆ˆt˜V„’ˆ&‘∂VW6WGF∆VEv˜&∂∆ˆt˜V„∞¢vñÊF˜rÂˆFó6&‘∂VW6WGF∆VEv˜&∂∆ˆt˜V„’ˆFó6&‘∂VW6WGF∆VEv˜&∂∆ˆt˜V„∞ß–¶gVÊ7Fñˆ‚˜&VÊFW%6WGF∆VDÊ6Ü˜%66VÊTf˜$÷W76vRÜ÷W76vR¬6Vv÷VÁB¬&tñGÇó∞¢ñbÇ÷W76vW«¬÷W76vRÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊW«¬6Vv÷VÁBí&WGW&‚f«6S∞¢ñbÇˆÊ6Ü˜%66VÊU66VÊTÜ5v˜&∂∆ˆuv˜'Fáï&˜w2Ü÷W76vRÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊRíí&WGW&‚f«6S∞¢ñbáGóVˆbó5G&Á7&VÁE7G&V”””“vgVÊ7Fñˆ‚rbfó5G&Á7&VÁE7G&V“Çíó∞¢&WGW&‚˜&VÊFW%6WGF∆VDÊ6Ü˜%66VÊUG&Á7&VÁDf˜$÷W76vRÜ÷W76vR«6Vv÷VÁB«&tñGÇì∞¢–¢ñbáGóVˆbó46ˆ◊7Ev˜&∂∆ˆt÷ˆFS””“vgVÊ7Fñˆ‚rbbó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇíí&WGW&‚f«6S∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2á6Vv÷VÁBÊ6∆˜6W7BÇrÊ76ó7FÁB◊GW&‚ríì∞¢ñbÇ&∆ˆ6∑2í&WGW&‚f«6S∞¢6ˆÁ7B66VÊS÷÷W76vRÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊS∞¢6ˆÁ7B&˜w3’ˆÊ6Ü˜%66VÊU&˜w4f˜%&VÊFW&ñÊrá66VÊR«∑6WGF∆VCßG'VW“ì∞¢ñbÇ&˜w2Ê∆VÊwFÇí&WGW&‚f«6S∞¢&∆ˆ6∑2ÁVW'ï6V∆V7F˜$∆¬ÇrÊ76ó7FÁB◊6Vv÷VÁE∂FF÷◊6r÷ñGÖ“ríÊf˜$V6ÇÜÊˆFS”Á∞¢6ˆÁ7BñGÉ‘ÁV÷&W"ÜÊˆFRÊvWDGG&ñ'WFRÇvFF÷◊6r÷ñGÇríì∞¢ñbÑÁV÷&W"Êó4fñÊóFRÜñGÇíbfñGÉ«&tñGÇó∞¢ÊˆFRÊ6∆74∆ó7BÊFBÇv76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6Rrì∞¢ÊˆFRÁ6WDGG&ñ'WFRÇv&ñ÷ÜñFFV‚r¬wG'VRrì∞¢ÊˆFRÊÜñFFV„◊G'VS∞¢–¢“ì∞¢&∆ˆ6∑2ÁVW'ï6V∆V7F˜$∆¬ÇrÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W¶Ê˜BÖ∂FF÷Ê6Ü˜"◊66VÊR÷˜vÊW#“#%“í¬ÁFˆˆ¬÷6∆¬÷w&˜W¶Ê˜BÖ∂FF÷Ê6Ü˜"◊66VÊR÷˜vÊW#“#%“í¬ÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊs¶Ê˜BÖ∂FF÷Ê6Ü˜"◊66VÊR◊&˜s“#%“í¬Áv¬◊&V6ˆ‚ríÊf˜$V6ÇÜV√”ÊV¬Á&V÷˜fRÇíì∞¢6ˆÁ7B7G&V‘ñC’7G&ñÊrÜ÷W76vRÂˆÊ6Ü˜%˜7G&V’ˆñG««66VÊRÁ7G&V’ˆñG««66VÊRÊñFVÁFóGíbg66VÊRÊñFVÁFóGíÁ7G&V’ˆñG«¬rrì∞¢6ˆÁ7B∂VW6WGF∆VEv˜&∂∆ˆt˜V„’˜6Ü˜V∆D∂VW6WGF∆VEv˜&∂∆ˆt˜V‰f˜%7G&V’6WGF∆Rá7G&V‘ñBì∞¢6ˆÁ7B7FófóGî∂Wì÷Ê6Ü˜"◊66VÊS¢G∑&tñGá÷∞¢ñbá7G&V‘ñBbb˜&VD7FófóGîFó66∆˜7W&U7FFRÜ7FófóGî∂Wííó∞¢ˆ6˜î7FófóGîFó66∆˜7W&U7FFRÜ∆ófS¢G∑7G&V‘ñG÷¬7FófóGî∂Wíì∞¢–¢ÚÚ3SìC¢‚W'&˜&VBGW&‚FÜB&ˆGV6VB76ó7FÁB6ˆÁFVÁBáFˆˆ¬6∆«2¢ÚÚ&V6ˆÊñÊrí◊W7BÊ˜BÜñFRFÜB6ˆÁFVÁB&VÜñÊB6ˆ∆∆6VBÜVFW"(	BFÜRW6W ¢ÚÚ&VG2∆ˆÊRW'&˜"6&B2&Ê˜FÜñÊr6÷R&6≤"‚vÜV‚FÜR6WGF∆VB66VÊRw0¢ÚÚFW&÷ñÊ≈˜7FFRó2‚W'&˜"ˆfñ«W&RÑ‰ıBÊ˜&÷¬6ˆ◊∆WFñˆ‚í∂VWFÜP¢ÚÚv˜&∂∆ˆrUÖ‰DTB'íFVfV«B6ÚFÜR&ˆGV6VB&W7ˆÁ6R7Fó2fó6ñ&∆R‚FÜó0¢ÚÚFÇó2ˆÊ«í&V6ÜVBf˜"v˜&∂∆ˆr◊v˜'Fáí66VÊW2áFÜRwV&BBFÜRF˜ ¢ÚÚ&WVó&W2„”Fˆˆ¬˜FÜñÊ∂ñÊrˆ6ˆ◊&W76ñˆ‚&˜rí¬6ÚvVÁVñÊV«í÷V◊GíW'&˜&V@¢ÚÚGW&‚(	B&V¬Êı˜&W7ˆÁ6RvóFÇ¶W&Ú&ˆGV6VB6ˆÁFVÁB(	BÊWfW"vWG2ÜW&RÊ@¢ÚÚ7Fñ∆¬6Ü˜w2ˆÊ«íóG2W'&˜"6&B¬ÊÚÜÁFˆ“V◊Gí&ˆGí‚W6W"vÜÚÜ0¢ÚÚWá∆ñ6óF«í6ˆ∆∆6VBDÑï2GW&‚w2v˜&∂∆ˆrá6fVBv6∆˜6VBrFó66∆˜7W&R7FFRê¢ÚÚó27Fñ∆¬&W7V7FVB¬6ÚFÜRFVfV«B÷˜V‚ÊWfW"fñváG2‚ñÁFVÁFñˆÊ¬6ˆ∆∆6R‡¢6ˆÁ7BW'&˜&VEv˜&∂∆ˆt∂VW˜V„’ˆÊ6Ü˜%66VÊTÜ4W'&˜&VEFW&÷ñÊ≈7FFRá66VÊRê¢bb˜&VD7FófóGîFó66∆˜7W&U7FFRÜ7FófóGî∂Wíí”“v6∆˜6VBs∞¢ÚÚ∂VW6WGF∆VEv˜&∂∆ˆt˜V‚f˜&6W26ˆ∆∆6VC¶f«6Rf˜"FÜRÙ‰RÜVñváB◊7F&∆R6WGF∆P¢ÚÚ&VÊFW"ˆbFÜRßW7B◊6WGF∆VBGW&‚ÜÊÚ5E$T’ÙDÙ‰R6á&ñÊ≤ßV◊íf˜"&˜FÇñÊÊV@¢ÚÚfˆ∆∆˜vW'2‰BVÁñÊÊVB÷ñB◊GW&‚&VFW'2‚FÜR∂VW÷˜V‚ó2÷FRvVÁVñÊV«ê¢ÚÚG&Á6ñVÁB'íFÜR5E$T’ÙDÙ‰RÜÊF∆W"Ü÷W76vW2Êß2ì¢&ñváBgFW"FÜó2&VÊFW"ó@¢ÚÚFó6&◊2FÜRFˆ∂V‚ÊB'VÁ267&ˆ∆¬’$U4U%dî‰r6ˆ∆∆6R72¬6Úv˜&∂∆ˆrFÜP¢ÚÚ&VFW"ÜB÷ÁV∆«í6ˆ∆∆6VB&WGW&Á2FÚóG26˜ñVBFó66∆˜7W&R7FFP¢ÚÚÖˆ6˜î7FófóGîFó66∆˜7W&U7FFR&˜fRívóFÜ˜WBFÜRßV◊‚vÜñ∆RFÜRFˆ∂V‚ó0¢ÚÚ&÷VBFÜó2f˜&6VB÷˜V‚DÙ“ó2«6Ú∂WBıUBˆb˜6W76ñˆ‰áF÷ƒ66ÜP¢ÚÚÖˆó4∂VW6WGF∆VEv˜&∂∆ˆt˜V‰&÷VBí¬6ÚóBÊWfW"W'6ó7G27&˜72&W7F˜&W2‡¢6ˆÁ7Bw&˜W’ˆÊ6Ü˜%66VÊUv˜&∂∆ˆtw&˜WÜ&∆ˆ6∑2«∞¢∆ófS¶f«6R¿¢6ˆ∆∆6VC¢Ü∂VW6WGF∆VEv˜&∂∆ˆt˜VÁ«∆W'&˜&VEv˜&∂∆ˆt∂VW˜V‚í¿¢&Vf˜&TÊ6Ü˜#ßG'VR¿¢Ê6Ü˜#ß6Vv÷VÁB¿¢7FófóGî∂Wí¿¢7G&V‘ñB¿¢GW&‰GW&Fñˆ„¶÷W76vRÂ˜GW&‰GW&Fñˆ‚”◊VÊFVfñÊVBbf÷W76vRÂ˜GW&‰GW&Fñˆ‚”÷ÁV∆√ˆ÷W76vRÂ˜GW&‰GW&Fñˆ„ß66VÊRÁGW&ÂˆGW&Fñˆ‚¿¢“ì∞¢ñbÇw&˜Wí&WGW&‚f«6S∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF÷Ê6Ü˜"◊6WGF∆VB◊66VÊR÷˜vÊW"r¬srì∞¢ÚÚ3SÉ3ì¢f˜"4Ùƒƒ4TB6WGF∆VBv˜&∂∆ˆr¬FVfW"'Vñ∆FñÊrFÜR&˜rDÙ“VÁFñ¬FÜP¢ÚÚW6W"fó'7BWáÊG2óB‚&V6ˆÊñÊr÷ÜVgíGW&‚6‚6''íÉ≤7FófóGí&˜w3∞¢ÚÚVvW&«í÷FW&ñ∆ó¶ñÊrFÜV“f˜"WfW'íÜó7F˜&ñ6¬GW&‚&∆∆ˆˆÁ2FÜRDÙ“ÊB¢ÚÚ∆FW"7ñÊ6á&ˆÊ˜W2∆ñ˜WBÜRÊr‚˜VÊñÊrG&˜F˜v‚íFó2FÜRF"ñÁFÚ¢ÚÚ◊V«Fí‘t"g&VW¶R‚FÜR7V÷÷'í6Üó&VÊFW'2g&ˆ“FF◊GW&‚÷GW&Fñˆ‚¬Ê˜BFÜP¢ÚÚ&˜w2¬6ÚFVfW'&VBv˜&∂∆ˆr7Fñ∆¬6Ü˜w2óG2%&ˆ6W76VBñ‚á2"∆&V¬‚ˆ‡¢ÚÚWáÊB¬˜Fˆvv∆T7FófóGîw&˜W÷FW&ñ∆ó¶W2FÜR7F6ÜVB&˜w2WÜ7F«íˆÊ6R‡¢6ˆÁ7B6ˆ∆∆6VC÷w&˜WÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwFˆˆ¬÷6∆¬÷w&˜W÷6ˆ∆∆6VBrì∞¢ñbÜ6ˆ∆∆6VBó∞¢w&˜WÂˆFVfW'&VEv˜&∂∆ˆu&˜w3◊&˜w3∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF◊v˜&∂∆ˆr◊&˜w2÷FVfW'&VBr¬srì∞¢6ˆÁ7B∆ó7C’˜Fˆˆ≈v˜&∂∆ˆt∆ó7DV¬Üw&˜Wì∞¢ñbÜ∆ó7Bí∆ó7BÊñÊÊW$ÖD‘√“rs∞¢˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íÜw&˜Wì∞¢&WGW&‚G'VS∞¢–¢w&˜WÂˆFVfW'&VEv˜&∂∆ˆu&˜w3÷ÁV∆√∞¢w&˜WÁ&V÷˜fTGG&ñ'WFRÇvFF◊v˜&∂∆ˆr◊&˜w2÷FVfW'&VBrì∞¢&WGW&‚˜&VÊFW$Ê6Ü˜%66VÊU&˜w4ñÁFıv˜&∂∆ˆrÜw&˜W«&˜w2«∑6WGF∆VCßG'VW“ì∞ß–¶gVÊ7Fñˆ‚˜7ñÊ4∆ófUv˜&∂∆ˆu&V6ˆÁ4f˜$Ê6Ü˜"ÜÊ6Ü˜"¬Fó7∆ïFWáD˜fW'&ñFRó∞¢ñbÖ2Ê7FófU7G&V‘ñBbfó4∆ófTÊ6Ü˜$7FófóGï66VÊT˜vÊW"Ö2Ê7FófU7G&V‘ñBíí&WGW&„∞¢ÚÚv˜&∂∆ˆr&V6ˆ‚÷÷ó'&˜&ñÊrÜfˆ∆FñÊrñÁFW&÷VFñFR&˜6RñÁFÚF˜v˜&∂∆ˆr&ñ¿¢ÚÚÊBÜñFñÊrFÜRñÊ∆ñÊR76ó7FÁB◊6Vv÷VÁFfñ76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6V ¢ÚÚ(i"Fó7∆ì¶ÊˆÊRíó2FÜR6ˆ◊7Bv˜&∂∆ˆr&W6VÁFFñˆ‚Ç33Cí‚ñ‚G&Á7&VÁ@¢ÚÚ7G&V“÷ˆFR&˜6R◊W7B7Fí2fó6ñ&∆R¬6á&ˆÊˆ∆ˆvñ6∆«í◊∆6VBñÊ∆ñÊR6Vv÷VÁG0¢ÚÚñÁFW&∆VfVBvóFÇFˆˆ¬&˜w2(	B6ÚFÚ‰ıB'Vñ∆BFÜRv˜&∂∆ˆr&ñ¬˜"ÜñFRFÜP¢ÚÚñÊ∆ñÊR6Vv÷VÁBÜW&R‚vóFÜ˜WBFÜó2vFRWfW'í&˜VÊBw2&˜6R÷ó'&˜"ñ∆W2ñÁF¢ÚÚFÜR6ñÊv∆RF˜&ñ¬vÜñ∆RFˆˆ¬&˜w2VÊBBFÜR&˜GFˆ“¬6Ú∆¬&˜6R'VÊ6ÜW0¢ÚÚ&˜fR∆¬Fˆˆ«2GW&ñÊr∆ófR◊V«Fí◊&˜VÊBGW&‚Ç3Cìbì≤óBˆÊ«í6V∆b÷ÜV«2vÜV‡¢ÚÚFÜRGW&‚6WGF∆W2ÊB&VÊFW$÷W76vW2Çí&V'Vñ∆G2vóFÇFÜR6ˆ◊7B÷ˆÊ«ê¢ÚÚ÷W76vT&V∆ˆÊw4ñÂv˜&∂∆ˆvvFRávÜñ6Çó2«&VGíó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇí÷ˆÊ«íí‡¢ñbáGóVˆbó46ˆ◊7Ev˜&∂∆ˆt÷ˆFS””“vgVÊ7Fñˆ‚rbbó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇíí&WGW&„∞¢ñbÇÊ6Ü˜'«¬Ê6Ü˜"Ê÷F6ÜW7«¬Ê6Ü˜"Ê÷F6ÜW2Çu∂FF÷∆ófR÷76ó7FÁC“#%“ríí&WGW&„∞¢6ˆÁ7B&∆ˆ6∑3÷Ê6Ü˜"Á&VÁDV∆V÷VÁC∞¢ñbÇ&∆ˆ6∑2í&WGW&„∞¢6ˆÁ7Bw&˜W÷VÁ7W&T∆ófUv˜&∂∆ˆt6ˆÁFñÊW"Ü&∆ˆ6∑2«∞¢7FófóGî∂Wì•ˆ7FófóGî∂Wîf˜$∆ófUGW&‚Çí¿¢Ê6Ü˜"¿¢“ì∞¢ñbÜw&˜Wí˜7ñÊ5v˜&∂∆ˆu&V6ˆ‰g&ˆ‘Ê6Ü˜"Üw&˜W¬Ê6Ü˜"¬Fó7∆ïFWáD˜fW'&ñFRì∞ß–¶gVÊ7Fñˆ‚ˆ6∆V$∆ófT7FófóGïW6W$ñÁFVÁBÇó∞¢ˆ∆ófT7FófóGïW6W$WáÊFVB“VÊFVfñÊVC∞ß–¶gVÊ7Fñˆ‚VÁ7W&T7FófóGîw&˜WÜñÊÊW"¬˜G2ó∞¢˜G3÷˜G7««∑”∞¢ñbÇñÊÊW"í&WGW&‚ÁV∆√∞¢6ˆÁ7B∆ófS“˜G2Ê∆ófS∞¢6ˆÁ7B7FófóGî∂Wì÷˜G2Ê7FófóGî∂Wó«¬Ü∆ófSıˆ7FófóGî∂Wîf˜$∆ófUGW&‚Çì¶ÁV∆¬ì∞¢6ˆÁ7B'W'7DñC÷˜G2Ê'W'7DñB”◊VÊFVfñÊVBbf˜G2Ê'W'7DñB”÷ÁV∆√ı7G&ñÊrÜ˜G2Ê'W'7DñBì¢rs∞¢6ˆÁ7B6Vv÷VÁE6W÷˜G2Á6Vv÷VÁE6W”◊VÊFVfñÊVBbf˜G2Á6Vv÷VÁE6W”÷ÁV∆√ı7G&ñÊrÜ˜G2Á6Vv÷VÁE6Wì¢rs∞¢6ˆÁ7B∆ófU6V∆V7F˜'3◊6Vv÷VÁE6W¢Ú∞¢ÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF÷∆ófR◊6Vv÷VÁB◊6W“"G¥552ÊW66Rá6Vv÷VÁE6Wó“%÷¿¢ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF÷∆ófR◊6Vv÷VÁB◊6W“"G¥552ÊW66Rá6Vv÷VÁE6Wó“%÷¿¢ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%’∂FF÷∆ófR◊6Vv÷VÁB◊6W“"G¥552ÊW66Rá6Vv÷VÁE6Wó“%÷¿¢–¢¢'W'7Dñ@¢Ú∞¢ÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF÷7FófóGí÷'W'7B÷ñC“"G¥552ÊW66RÜ'W'7DñBó“%÷¿¢ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF÷7FófóGí÷'W'7B÷ñC“"G¥552ÊW66RÜ'W'7DñBó“%÷¿¢ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%’∂FF÷7FófóGí÷'W'7B÷ñC“"G¥552ÊW66RÜ'W'7DñBó“%÷¿¢–¢¢∞¢rÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF÷∆ófR÷7FófóGí÷7W'&VÁC“#%“r¿¢rÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF÷∆ófR÷7FófóGí÷7W'&VÁC“#%“r¿¢rÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%’∂FF÷∆ófR÷7FófóGí÷7W'&VÁC“#%“r¿¢”∞¢∆WBw&˜W∞¢ñbÜ∆ófRó∞¢ñbÜ7FófóGî∂Wíó∞¢w&˜W÷ñÊÊW"ÁVW'ï6V∆V7F˜"ÜÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷∂Wì“"G¥552ÊW66RÜ7FófóGî∂Wíó“%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷∂Wì“"G¥552ÊW66RÜ7FófóGî∂Wíó“%÷ì∞¢–¢ñbÇw&˜Wó∞¢f˜"Ü6ˆÁ7B6V¬ˆb∆ófU6V∆V7F˜'2ó∞¢w&˜W÷ñÊÊW"ÁVW'ï6V∆V7F˜"á6V¬ì∞¢ñbÜw&˜Wí'&V≥∞¢–¢–¢÷V«6W∞¢ñbÜ7FófóGî∂Wíó∞¢w&˜W÷ñÊÊW"ÁVW'ï6V∆V7F˜"ÜÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%’∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷∂Wì“"G¥552ÊW66RÜ7FófóGî∂Wíó“%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%’∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷∂Wì“"G¥552ÊW66RÜ7FófóGî∂Wíó“%÷ì∞¢–¢ñbÇw&˜Wbg6Vv÷VÁE6Wó∞¢w&˜W÷ñÊÊW"ÁVW'ï6V∆V7F˜"ÜÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%’∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF÷∆ófR◊6Vv÷VÁB◊6W“"G¥552ÊW66Rá6Vv÷VÁE6Wó“%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%’∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF÷∆ófR◊6Vv÷VÁB◊6W“"G¥552ÊW66Rá6Vv÷VÁE6Wó“%÷ì∞¢–¢ñbÇw&˜Wbf'W'7DñBó∞¢w&˜W÷ñÊÊW"ÁVW'ï6V∆V7F˜"ÜÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%’∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF÷7FófóGí÷'W'7B÷ñC“"G¥552ÊW66RÜ'W'7DñBó“%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%’∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%’∂FF÷7FófóGí÷'W'7B÷ñC“"G¥552ÊW66RÜ'W'7DñBó“%÷ì∞¢–¢ñbÇw&˜Wbf7FófóGî∂Wíó∞¢w&˜W÷ñÊÊW"ÁVW'ï6V∆V7F˜"ÜÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷∂Wì“"G¥552ÊW66RÜ7FófóGî∂Wíó“%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷∂Wì“"G¥552ÊW66RÜ7FófóGî∂Wíó“%÷ì∞¢–¢ñbÇw&˜Wbb7FófóGî∂Wíó∞¢w&˜W÷ñÊÊW"ÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%’∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%’∂FF◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%”¶Ê˜BÖ∂FF◊'V‚÷7FófóGí÷w&˜W“#%“írì∞¢–¢–¢ñbÇw&˜Wbb7FófóGî∂Wíbb6Vv÷VÁE6W””“""bb'W'7DñBó∞¢6ˆÁ7B6ÊFñFFW3÷∆ófP¢Ú'&íÊg&ˆ“ÜñÊÊW"ÁVW'ï6V∆V7F˜$∆¬ÇrÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%“ríê¢¢'&íÊg&ˆ“ÜñÊÊW"ÁVW'ï6V∆V7F˜$∆¬ÇrÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%”¶Ê˜BÖ∂FF◊'V‚÷7FófóGí÷w&˜W“#%“íríì∞¢w&˜W÷6ÊFñFFW2Êfñ«FW"ÜV√”ÊV¬Êó46ˆÊÊV7FVB”÷f«6RíÁ˜Çí«¬ÁV∆√∞¢–¢ñbÇw&˜Wó∞¢w&˜W÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢∆WB6ˆ∆∆6VC÷˜G2Ê6ˆ∆∆6VB”÷f«6S∞¢ñbávñÊF˜rÂ˜v˜&∂∆ˆtFWFñ«4WáÊFVD'îFVfV«C””◊G'VRí6ˆ∆∆6VC÷f«6S∞¢6ˆÁ7B6fVE7FFS’˜&VD7FófóGîFó66∆˜7W&U7FFRÜ7FófóGî∂Wíì∞¢ÚÚ&W7F˜&RFÜRW6W"w2Wá∆ñ6óBWáÊBñÁFVÁBvÜV‚&V7&VFñÊrFÜR∆ófP¢ÚÚ7FófóGíw&˜WvóFÜñ‚FÜR6÷RGW&‚Ç3#ìÇí¬FÜV‚∆WBW'6ó7FVB6ÜB˜GW&‡¢ÚÚ7FFRvñ‚7&˜726W76ñˆ‚7vóF6ÜW2ÊB&V∆ˆG2‚6fVB6∆˜6VB◊7FFR6Ü˜V∆@¢ÚÚ˜fW'&ñFRFÜRFVfV«B÷WáÊFVB&VfW&VÊ6Rf˜"6WGF∆VBw&˜W2FÜRW6W"Ü0¢ÚÚWá∆ñ6óF«í6ˆ∆∆6VB‡¢ñbÜ∆ófRbbˆ∆ófT7FófóGïW6W$WáÊFVB””“G'VRí6ˆ∆∆6VC÷f«6S∞¢V«6RñbÜ∆ófRbbˆ∆ófT7FófóGïW6W$WáÊFVB””“f«6Rí6ˆ∆∆6VC◊G'VS∞¢ñbÜ∆ófRbb6fVE7FFS””“v˜V‚rí6ˆ∆∆6VC÷f«6S∞¢V«6RñbÜ∆ófRbb6fVE7FFS””“v6∆˜6VBrí6ˆ∆∆6VC◊G'VS∞¢w&˜WÊ6∆74Ê÷S“vvVÁB÷7FófóGí÷w&˜WFˆˆ¬◊v˜&∂∆ˆr÷w&˜W7FófóGír≤Ü6ˆ∆∆6VCÚrFˆˆ¬÷6∆¬÷w&˜W÷6ˆ∆∆6VBs¢rrì∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF◊Fˆˆ¬÷6∆¬÷w&˜Wr¬srì∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF÷vVÁB÷7FófóGí÷w&˜Wr¬srì∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜Wr¬srì∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF◊Fˆˆ¬◊v˜&∂∆ˆr÷∂Wír∆7FófóGî∂Wó«¬rrì∞¢ñbÜ7FófóGî∂Wííw&˜WÁ6WDGG&ñ'WFRÇvFF÷7FófóGí÷Fó66∆˜7W&R÷∂Wír∆7FófóGî∂Wíì∞¢ñbÜ∆ófRó∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜Wr¬srì∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜Wr¬srì∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF÷∆ófR÷7FófóGí÷7W'&VÁBr¬srì∞¢–¢ñbÜ'W'7DñBíw&˜WÁ6WDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBr∆'W'7DñBì∞¢ñbá6Vv÷VÁE6Wíw&˜WÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wr«6Vv÷VÁE6Wì∞¢w&˜WÊ6∆74∆ó7BÁFˆvv∆RÇv˜V‚r¬6ˆ∆∆6VBì∞¢w&˜WÊñÊÊW$ÖD‘√÷∆'WGFˆ‚GóS“&'WGFˆ‚"6∆73“'Fˆˆ¬÷6∆¬÷w&˜W◊7V÷÷'íFˆˆ¬◊v˜&∂∆ˆr◊7V÷÷'í7FófóGí◊7V÷÷'í"&ñ÷WáÊFVC“"G∂6ˆ∆∆6VCÚvf«6Rs¢wG'VRw“"ˆÊ6∆ñ6≥“%˜Fˆvv∆T7FófóGîw&˜WáFÜó2í#„«7‚6∆73“&2÷F˜B#„¬˜7„„«7‚6∆73“'Fˆˆ¬÷6∆¬÷w&˜W÷∆&V¬Fˆˆ¬◊v˜&∂∆ˆr÷∆&V¬2◊FWáB#Â'VÊÊñÊs¬˜7„„«7‚6∆73“'Fˆˆ¬÷6∆¬÷w&˜W÷GW&Fñˆ‚#„¬˜7„„«7‚6∆73“'Fˆˆ¬÷6∆¬÷w&˜W÷6ÜWg&ˆ‚2÷6&WB#‚G∂∆íÇv6ÜWg&ˆ‚◊&ñváBr√"ó”¬˜7„„¬ˆ'WGFˆ„„∆Fób6∆73“'Fˆˆ¬÷6∆¬÷w&˜W÷&ˆGíFˆˆ¬◊v˜&∂∆ˆr÷&ˆGí7FófóGí÷&ˆGí#„∆Fób6∆73“'v˜&∂∆ˆr#„∆Fób6∆73“'Fˆˆ¬◊v˜&∂∆ˆr÷∆ó7B#„¬ˆFóc„¬ˆFóc„¬ˆFócÊ∞¢6ˆÁ7BÊ6Ü˜#÷˜G2ÊÊ6Ü˜'«∆ÁV∆√∞¢ñbÜÊ6Ü˜"bfÊ6Ü˜"Á&VÁDV∆V÷VÁC””÷ñÊÊW"ó∞¢ñbÜ˜G2Ê&Vf˜&TÊ6Ü˜"íñÊÊW"ÊñÁ6W'D&Vf˜&RÜw&˜W¬Ê6Ü˜"ì∞¢V«6RÊ6Ü˜"ÊñÁ6W'DF¶6VÁDV∆V÷VÁBÇvgFW&VÊBr¬w&˜Wì∞¢–¢V«6RñÊÊW"ÊVÊD6Üñ∆BÜw&˜Wì∞¢÷V«6RñbÜ7FófóGî∂Wíbbw&˜WÊvWDGG&ñ'WFRÇvFF÷7FófóGí÷Fó66∆˜7W&R÷∂Wíríó∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF÷7FófóGí÷Fó66∆˜7W&R÷∂Wír∆7FófóGî∂Wíì∞¢–¢ñbÜ'W'7DñBbbw&˜WÊvWDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBrííw&˜WÁ6WDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBr∆'W'7DñBì∞¢ñbá6Vv÷VÁE6Wbbw&˜WÊvWDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wrííw&˜WÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wr«6Vv÷VÁE6Wì∞¢ñbÇw&˜WÊvWDGG&ñ'WFRÇvFF◊Fˆˆ¬◊v˜&∂∆ˆr÷∂Wíríbf7FófóGî∂Wííw&˜WÁ6WDGG&ñ'WFRÇvFF◊Fˆˆ¬◊v˜&∂∆ˆr÷∂Wír∆7FófóGî∂Wíì∞¢ñbÜ˜G2ÁGW&‰GW&Fñˆ‚”◊VÊFVfñÊVBbf˜G2ÁGW&‰GW&Fñˆ‚”÷ÁV∆¬íw&˜WÁ6WDGG&ñ'WFRÇvFF◊GW&‚÷GW&Fñˆ‚r≈7G&ñÊrÜ˜G2ÁGW&‰GW&Fñˆ‚íì∞¢ñbÜ˜G2ÁGW&Â7F'FVDB”◊VÊFVfñÊVBbf˜G2ÁGW&Â7F'FVDB”÷ÁV∆¬íw&˜WÁ6WDGG&ñ'WFRÇvFF◊GW&‚◊7F'FVB÷Br≈7G&ñÊrÜ˜G2ÁGW&Â7F'FVDBíì∞¢6ˆÁ7B7V÷÷'ì÷w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬◊v˜&∂∆ˆr◊7V÷÷'í¬ÁFˆˆ¬÷6∆¬÷w&˜W◊7V÷÷'írì∞¢ñbá7V÷÷'íó∞¢7V÷÷'íÁ&V÷˜fTGG&ñ'WFRÇvFF÷∆ófR◊7V÷÷'í◊7FFñ2rì∞¢7V÷÷'íÁ&V÷˜fTGG&ñ'WFRÇv&ñ÷Fó6&∆VBrì∞¢7V÷÷'íÊFó6&∆VC÷f«6S∞¢–¢6ˆÁ7BÊ6Ü˜#÷˜G2ÊÊ6Ü˜'«∆ÁV∆√∞¢ñbÜÊ6Ü˜"bfÊ6Ü˜"Á&VÁDV∆V÷VÁC””÷ñÊÊW"bfw&˜WÁ&VÁDV∆V÷VÁC””÷ñÊÊW"ó∞¢ñbÜ˜G2Ê&Vf˜&TÊ6Ü˜"ó∞¢ñbÜw&˜WÊÊWáDV∆V÷VÁE6ñ&∆ñÊr”÷Ê6Ü˜"íñÊÊW"ÊñÁ6W'D&Vf˜&RÜw&˜W∆Ê6Ü˜"ì∞¢÷V«6RñbÜw&˜WÁ&Wfñ˜W4V∆V÷VÁE6ñ&∆ñÊr”÷Ê6Ü˜"ó∞¢Ê6Ü˜"ÊñÁ6W'DF¶6VÁDV∆V÷VÁBÇvgFW&VÊBr∆w&˜Wì∞¢–¢–¢ñbÜÊ6Ü˜"bf˜G2Á7ñÊ4Ê6Ü˜%&V6ˆ‚”÷f«6Rí˜7ñÊ5v˜&∂∆ˆu&V6ˆ‰g&ˆ‘Ê6Ü˜"Üw&˜W¬Ê6Ü˜"ì∞¢˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íÜw&˜Wì∞¢&WGW&‚w&˜W∞ß–¶gVÊ7Fñˆ‚Ê˜&÷∆ó¶T∆ófT7FófóGîw&˜W∆6V÷VÁBáGW&‚ó∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÇ&∆ˆ6∑2í&WGW&„∞¢ÚÚ6ˆ◊7Bv˜&∂∆ˆrˆÊ«ì¢FÜó2&V˜&FW'2ÁFˆˆ¬÷6∆¬÷w&˜WˆÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W ¢ÚÚ6ˆÁFñÊW'2¬vÜñ6ÇWÜó7B6ˆ∆V«íˆ‚FÜR6ˆ◊7Bv˜&∂∆ˆr∆ófRFÇ‚G&Á7&VÁ@¢ÚÚ7G&V“&VÊFW'2Fˆˆ¬&˜w22f∆BÁG&Á7&VÁB÷WfVÁB◊&˜v2ÊBÊWfW"'Vñ∆G0¢ÚÚFÜW6Rw&˜W6ˆÁFñÊW'2á6VRVÊD∆ófUFˆˆƒ6&Bw2G&Á7&VÁB'&Ê6Çí¬ÊBFÜP¢ÚÚv˜&∂∆ˆr&˜6R◊&ñ¬ó2vFVBˆfbñ‚G&Á7&VÁB÷ˆFRÇ3Cìbí¬6ÚFÜR6V∆V7F˜ ¢ÚÚ&V∆˜r÷F6ÜW2Ê˜FÜñÊrÊBFÜó2ó2ÊÚ÷˜FÜW&R‚∂WBñ◊∆ñ6óBÜV◊Gí÷F6Çê¢ÚÚ&FÜW"FÜ‚‚V&«í&WGW&‚6Ú&V6ˆÊÊV7B˜&W7F˜&R&VÜfñ˜"ó2VÊ6ÜÊvVB‡¢6ˆÁ7Bw&˜W3‘'&íÊg&ˆ“Ä¢&∆ˆ6∑2ÁVW'ï6V∆V7F˜$∆¬ÇrÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜W“#%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%“rê¢ì∞¢w&˜W2Á6˜'BÇÜ∆"ì”Á∞¢6ˆÁ7B3‘ÁV÷&W"ÜÊvWDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wríì∞¢6ˆÁ7B'3‘ÁV÷&W"Ü"ÊvWDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wríì∞¢ñbÑÁV÷&W"Êó4fñÊóFRÜ2íbdÁV÷&W"Êó4fñÊóFRÜ'2íbf2”÷'2í&WGW&‚2÷'3∞¢6ˆÁ7Bc‘ÁV÷&W"ÜÊvWDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBríì∞¢6ˆÁ7B'c‘ÁV÷&W"Ü"ÊvWDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBríì∞¢ñbÑÁV÷&W"Êó4fñÊóFRÜbíbdÁV÷&W"Êó4fñÊóFRÜ'bíbfb”÷'bí&WGW&‚b÷'c∞¢&WGW&‚∞¢“ì∞¢f˜"Ü6ˆÁ7Bw&˜Wˆbw&˜W2ó∞¢6ˆÁ7B'W'7DñC÷w&˜WÊvWDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBró«¬rs∞¢6ˆÁ7B6Vv÷VÁE6W÷w&˜WÊvWDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wró«¬rs∞¢6ˆÁ7BÊ6Ü˜#◊6Vv÷VÁE6W¢ÚˆfñÊD∆ófT76ó7FÁDÊ6Ü˜$f˜%6Vv÷VÁBÜ&∆ˆ6∑2¬6Vv÷VÁE6Wê¢¢'W'7Dñ@¢ÚˆfñÊD∆FW7Efó6ñ&∆T∆ófT76ó7FÁD'î'W'7BÜ&∆ˆ6∑2¬'W'7DñBê¢¢ˆfñÊD∆FW7Efó6ñ&∆T∆ófT76ó7FÁBÜ&∆ˆ6∑2ì∞¢ñbÇÊ6Ü˜"í6ˆÁFñÁVS∞¢ñbÜÊ6Ü˜"bfw&˜WÁ&Wfñ˜W4V∆V÷VÁE6ñ&∆ñÊr”÷Ê6Ü˜"íÊ6Ü˜"ÊñÁ6W'DF¶6VÁDV∆V÷VÁBÇvgFW&VÊBr∆w&˜Wì∞¢˜7ñÊ5v˜&∂∆ˆu&V6ˆ‰g&ˆ‘Ê6Ü˜"Üw&˜W¬Ê6Ü˜"ì∞¢–ß–¶gVÊ7Fñˆ‚VÁ7W&U'V‰7FófóGîw&˜WÜñÊÊW"¬˜G2ó∞¢˜G3÷˜G7««∑”∞¢ñbÇñÊÊW"í&WGW&‚ÁV∆√∞¢∆WBw&˜W÷ñÊÊW"ÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6∆¬÷w&˜W∂FF◊'V‚÷7FófóGí÷w&˜W“#%“rì∞¢ñbÇw&˜Wó∞¢w&˜W÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢6ˆÁ7B6ˆ∆∆6VC÷˜G2Ê6ˆ∆∆6VB”÷f«6S∞¢w&˜WÊ6∆74Ê÷S“wFˆˆ¬÷6∆¬÷w&˜WvVÁB÷7FófóGí÷w&˜W'V‚÷7FófóGí÷w&˜Wr≤Ü6ˆ∆∆6VCÚrFˆˆ¬÷6∆¬÷w&˜W÷6ˆ∆∆6VBs¢r˜V‚rì∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF◊Fˆˆ¬÷6∆¬÷w&˜Wr¬srì∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF÷vVÁB÷7FófóGí÷w&˜Wr¬srì∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF◊'V‚÷7FófóGí÷w&˜Wr¬srì∞¢w&˜WÊñÊÊW$ÖD‘√÷∆'WGFˆ‚GóS“&'WGFˆ‚"6∆73“'Fˆˆ¬÷6∆¬÷w&˜W◊7V÷÷'í"&ñ÷WáÊFVC“"G∂6ˆ∆∆6VCÚvf«6Rs¢wG'VRw“"ˆÊ6∆ñ6≥“%˜Fˆvv∆T7FófóGîw&˜WáFÜó2í#„«7‚6∆73“'Fˆˆ¬÷6∆¬÷w&˜W÷6ÜWg&ˆ‚#‚G∂∆íÇv6ÜWg&ˆ‚◊&ñváBr√"ó”¬˜7„„«7‚6∆73“'Fˆˆ¬÷6∆¬÷w&˜W÷∆&V¬#Â'VÊÊñÊs¬˜7„„«7‚6∆73“'Fˆˆ¬÷6∆¬÷w&˜W÷GW&Fñˆ‚#„¬˜7„„¬ˆ'WGFˆ„„∆Fób6∆73“'Fˆˆ¬÷6∆¬÷w&˜W÷&ˆGí#„¬ˆFócÊ∞¢ñbÜñÊÊW"Êfó'7D6Üñ∆BíñÊÊW"ÊñÁ6W'D&Vf˜&RÜw&˜W¬ñÊÊW"Êfó'7D6Üñ∆Bì∞¢V«6RñÊÊW"ÊVÊD6Üñ∆BÜw&˜Wì∞¢–¢ñbÜ˜G2ÁGW&‰GW&Fñˆ‚”◊VÊFVfñÊVBbf˜G2ÁGW&‰GW&Fñˆ‚”÷ÁV∆¬íw&˜WÁ6WDGG&ñ'WFRÇvFF◊GW&‚÷GW&Fñˆ‚r≈7G&ñÊrÜ˜G2ÁGW&‰GW&Fñˆ‚íì∞¢ñbÜ˜G2ÁGW&Â7F'FVDB”◊VÊFVfñÊVBbf˜G2ÁGW&Â7F'FVDB”÷ÁV∆¬íw&˜WÁ6WDGG&ñ'WFRÇvFF◊GW&‚◊7F'FVB÷Br≈7G&ñÊrÜ˜G2ÁGW&Â7F'FVDBíì∞¢˜6WD7FófóGîV∆6VE7F'FVDBÜw&˜Wì∞¢ˆVÁ7W&T∆ófT7FófóGî&6V∆ñÊRÜw&˜Wì∞¢˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íÜw&˜Wì∞¢ñbÜ˜G2Ê∆ófR”÷f«6Rí˜7F'D7FófóGîV∆6VEFñ÷W"Üw&˜Wì∞¢&WGW&‚w&˜W∞ß–¢ÚÚ)H)H∆ófTfˆ˜FW"Fñ÷W"Ü÷ˆGV∆R÷∆WfV¬6ñÊv∆WFˆ‚í)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H ¶6ˆÁ7Bˆ∆ófU'VÂ7FGW5Fñ÷W'3◊∑”≤ÚÚ∂WñVB'í6W76ñˆ‰ñB¬÷Ç7FófP¶∆WBˆ∆ófU'VÂ7FGW5Fˆ∂VÁ3÷ÁV∆√∞¶∆WBˆ∆ófU'VÂ7FGW56W76ñˆ‰ñC÷ÁV∆√∞¶gVÊ7Fñˆ‚ˆf˜&÷E'V‰V∆6VBá6V6ˆÊG2ó∞¢6ˆÁ7B„‘ÁV÷&W"á6V6ˆÊG2ì∞¢ñbÇÁV÷&W"Êó4fñÊóFRÜ‚ó«∆„√ó&WGW&‚s£s∞¢6ˆÁ7BF˜F√‘÷FÇÊ÷ÇÉƒ÷FÇÊf∆ˆ˜"Ü‚íì∞¢ñbáF˜F√„”3có∞¢6ˆÁ7BÉ‘÷FÇÊf∆ˆ˜"áF˜F¬Û3cì∞¢6ˆÁ7B”‘÷FÇÊf∆ˆ˜"ÇáF˜F¬S3cíÛcì∞¢&WGW&‚Ç≤vÇrµ7G&ñÊrÜ“íÁE7F'BÉ"¬srí≤v“s∞¢–¢6ˆÁ7B”‘÷FÇÊf∆ˆ˜"áF˜F¬Ûcì∞¢6ˆÁ7B3◊F˜F¬Sc∞¢&WGW&‚7G&ñÊrÜ“íÁE7F'BÉ"¬srí≤s¢rµ7G&ñÊrá2íÁE7F'BÉ"¬srì∞ß–¶gVÊ7Fñˆ‚ˆ÷˜fT∆ófU'VÂ7FGW5FıGW&‰VÊBÜV¬ó∞¢V√÷V««¬BÇv∆ófU'VÂ7FGW2rì∞¢ñbÇV¬í&WGW&‚ÁV∆√∞¢6ˆÁ7BGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÜ&∆ˆ6∑2bfV¬Á&VÁDV∆V÷VÁC””÷&∆ˆ6∑2bf&∆ˆ6∑2Ê∆7DV∆V÷VÁD6Üñ∆B”÷V¬í&∆ˆ6∑2ÊVÊD6Üñ∆BÜV¬ì∞¢&WGW&‚V√∞ß–¶gVÊ7Fñˆ‚∆6T∆ófU'VÂ7FGW4Ü˜7BÇó∞¢∆WBV√“BÇv∆ófU'VÂ7FGW2rì∞¢ñbÇV¬ó∞¢V√÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢V¬ÊñC“v∆ófU'VÂ7FGW2s∞¢V¬ÊÜñFFV„◊G'VS∞¢–¢∆WBGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢ñbÇGW&‚ó∞¢GW&„’ˆ7&VFT76ó7FÁEGW&‚Çì∞¢GW&‚ÊñC“v∆ófT76ó7FÁEGW&‚s∞¢ñbÖ2Á6W76ñˆ‚íGW&‚ÊFF6WBÁ6W76ñˆ‰ñC’2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢6ˆÁ7BñÊÊW#“BÇv◊6tñÊÊW"rì∞¢ñbÜñÊÊW"íñÊÊW"ÊVÊD6Üñ∆BáGW&‚ì∞¢–¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÜ&∆ˆ6∑2bfV¬Á&VÁDV∆V÷VÁB”÷&∆ˆ6∑2í&∆ˆ6∑2ÊVÊD6Üñ∆BÜV¬ì∞¢V¬Ê6∆74Ê÷S“v∆ófR◊'V‚◊7FGW2∆ófR÷fˆ˜FW"s∞¢&WGW&‚ˆ÷˜fT∆ófU'VÂ7FGW5FıGW&‰VÊBÜV¬ì∞ß–¶gVÊ7Fñˆ‚6Ü˜t∆ófU'VÂ7FGW2á6ñB∆˜G2ó∞¢ñbáGóVˆbó46ˆ◊7Ev˜&∂∆ˆt÷ˆFS””“vgVÊ7Fñˆ‚rbfó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇíó∞¢ˆ∆ófU'VÂ7FGW56W76ñˆ‰ñC◊6ñC∞¢ˆ∆ófU'VÂ7FGW5Fˆ∂VÁ3÷˜G2bf˜G2ÁFˆ∂VÁ7«∆ÁV∆√∞¢6ˆÁ7BV√“BÇv∆ófU'VÂ7FGW2rì∞¢ñbÜV¬ó∂V¬ÊÜñFFV„◊G'VS∂V¬ÊñÊÊW$ÖD‘√“rs∑–¢&WGW&„∞¢–¢6ˆÁ7BV√◊∆6T∆ófU'VÂ7FGW4Ü˜7BÇì∞¢ñbÇV¬ó&WGW&„∞¢ˆ∆ófU'VÂ7FGW56W76ñˆ‰ñC◊6ñC∞¢6ˆÁ7B7F'FVDC÷˜G2bf˜G2Á7F'FVDG«∆ÁV∆√∞¢ˆ∆ófU'VÂ7FGW5Fˆ∂VÁ3÷˜G2bf˜G2ÁFˆ∂VÁ7«∆ÁV∆√∞¢V¬ÊÜñFFV„÷f«6S∞¢˜&VÊFW$∆ófU'VÂ7FGW46ˆÁFVÁBÜV¬«7F'FVDBì∞¢˜7F'D∆ófU'VÂ7FGW5Fñ÷W"á6ñB«7F'FVDBì∞ß–¶gVÊ7Fñˆ‚˜&VÊFW$∆ófU'VÂ7FGW46ˆÁFVÁBÜV¬«7F'FVDBó∞¢ñbÇV¬ó&WGW&„∞¢6ˆÁ7BÊ˜s‘FFRÊÊ˜rÇíÛ∞¢6ˆÁ7BV∆6VC◊7F'FVDCÙ÷FÇÊ÷ÇÉ∆Ê˜r◊7F'FVDBì£∞¢6ˆÁ7BFñ÷U7G#’ˆf˜&÷E'V‰V∆6VBÜV∆6VBì∞¢6ˆÁ7BFˆ∂VÁ3’ˆ∆ófU'VÂ7FGW5Fˆ∂VÁ3∞¢V¬ÊñÊÊW$ÖD‘√÷«7‚6∆73“&∆ófR◊'V‚◊7FGW2÷F˜BFˆˆ¬÷6&B◊'VÊÊñÊr÷F˜B#„¬˜7„„«7‚6∆73“&∆ófR◊'V‚◊7FGW2◊FWáB∆b◊Fñ÷R#‚G∑Fñ÷U7G'”¬˜7„‚G∑Fˆ∂VÁ3ˆ«7‚6∆73“&∆b◊6W#Ï+s¬˜7„„«7‚6∆73“&∆b◊Fˆ∂VÁ2#‚Gµˆf◊EFˆ∂VÁ2áFˆ∂VÁ2ó“Fˆ∂VÁ3¬˜7„Ê¢rw”«7‚6∆73“&∆b◊6W#Ï+s¬˜7„„«7‚6∆73“&∆b◊7FGW2#Â'VÊÊñÊs¬˜7„Ê∞ß–¶gVÊ7Fñˆ‚WFFT∆ófU'VÂ7FGW2Ü˜G2ó∞¢ñbÜ˜G2bf˜G2Á6W76ñˆ‰ñBbeˆ∆ófU'VÂ7FGW56W76ñˆ‰ñBbf˜G2Á6W76ñˆ‰ñB”’ˆ∆ófU'VÂ7FGW56W76ñˆ‰ñBí&WGW&„∞¢ñbÜ˜G2bf˜G2ÁFˆ∂VÁ2”◊VÊFVfñÊVBïˆ∆ófU'VÂ7FGW5Fˆ∂VÁ3÷˜G2ÁFˆ∂VÁ3∞¢6ˆÁ7BV√“BÇv∆ófU'VÂ7FGW2rì∞¢ñbÜV¬bbV¬ÊÜñFFV‚ó∞¢ˆ÷˜fT∆ófU'VÂ7FGW5FıGW&‰VÊBÜV¬ì∞¢6ˆÁ7BFñ÷W#’ˆ∆ófU'VÂ7FGW5Fñ÷W'5µˆ∆ófU'VÂ7FGW56W76ñˆ‰ñE”∞¢6ˆÁ7B7F'FVDC◊Fñ÷W"bgFñ÷W"Á7F'FVDG«∆ÁV∆√∞¢˜&VÊFW$∆ófU'VÂ7FGW46ˆÁFVÁBÜV¬«7F'FVDBì∞¢–ß–¶gVÊ7Fñˆ‚˜7ñÊ4∆ófU'VÂ7FGW4gFW%&VÊFW"Çó∞¢6ˆÁ7B6ñC’2Á6W76ñˆ‚be2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢ñbÇ6ñG«¬2Ê7FófU7G&V‘ñG«¬2Ê'W7íí&WGW&„∞¢6ˆÁ7BFñ÷W#’ˆ∆ófU'VÂ7FGW5Fñ÷W'5∑6ñE”∞¢6ˆÁ7B7F'FVDC“áFñ÷W"bgFñ÷W"Á7F'FVDBó«¬ÇÖ2Á6W76ñˆ‚be2Á6W76ñˆ‚ÁVÊFñÊu˜7F'FVEˆBó«ƒFFRÊÊ˜rÇíÛì∞¢ñbáGóVˆbó46ˆ◊7Ev˜&∂∆ˆt÷ˆFS””“vgVÊ7Fñˆ‚rbfó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇíó∞¢6ˆÁ7BV√“BÇv∆ófU'VÂ7FGW2rì∞¢ñbÜV¬ó∂V¬ÊÜñFFV„◊G'VS∂V¬ÊñÊÊW$ÖD‘√“rs∑–¢&WGW&„∞¢–¢6ˆÁ7BV√“BÇv∆ófU'VÂ7FGW2rì∞¢ñbÜV¬bfV¬Êó46ˆÊÊV7FVBbbV¬ÊÜñFFV‚ó∞¢ˆ÷˜fT∆ófU'VÂ7FGW5FıGW&‰VÊBÜV¬ì∞¢˜&VÊFW$∆ófU'VÂ7FGW46ˆÁFVÁBÜV¬«7F'FVDBì∞¢&WGW&„∞¢–¢6Ü˜t∆ófU'VÂ7FGW2á6ñB«∑7F'FVDB«Fˆ∂VÁ3•ˆ∆ófU'VÂ7FGW5Fˆ∂VÁ7“ì∞ß–¶gVÊ7Fñˆ‚ÜñFT∆ófU'VÂ7FGW2á6ñBó∞¢ñbá6ñBbeˆ∆ófU'VÂ7FGW56W76ñˆ‰ñBbg6ñB”’ˆ∆ófU'VÂ7FGW56W76ñˆ‰ñBí&WGW&„∞¢6ˆÁ7BV√“BÇv∆ófU'VÂ7FGW2rì∞¢ñbÜV¬ó∂V¬ÊÜñFFV„◊G'VS∂V¬ÊñÊÊW$ÖD‘√“rs∑–¢ˆ6∆V$∆ófU'VÂ7FGW5Fñ÷W"á6ñG«≈ˆ∆ófU'VÂ7FGW56W76ñˆ‰ñBì∞¢ˆ∆ófU'VÂ7FGW5Fˆ∂VÁ3÷ÁV∆√∞¢ˆ∆ófU'VÂ7FGW56W76ñˆ‰ñC÷ÁV∆√∞ß–¶gVÊ7Fñˆ‚˜7F'D∆ófU'VÂ7FGW5Fñ÷W"á6ñB«7F'FVDBó∞¢ñbÇ6ñBó&WGW&„∞¢ˆ6∆V$∆ófU'VÂ7FGW5Fñ÷W"á6ñBì∞¢ˆ∆ófU'VÂ7FGW5Fñ÷W'5∑6ñE”◊∑7F'FVDB∆ñÁFW'f√ß6WDñÁFW'f¬ÇÇì”Á∞¢6ˆÁ7BV√“BÇv∆ófU'VÂ7FGW2rì∞¢ñbÇV««∆V¬ÊÜñFFV‚óµˆ6∆V$∆ófU'VÂ7FGW5Fñ÷W"á6ñBì∑&WGW&„∑–¢ñbÖˆ∆ófU'VÂ7FGW56W76ñˆ‰ñB”◊6ñBó&WGW&„∞¢˜&VÊFW$∆ófU'VÂ7FGW46ˆÁFVÁBÜV¬«7F'FVDBì∞¢“√ó”∞ß–¶gVÊ7Fñˆ‚ˆ6∆V$∆ófU'VÂ7FGW5Fñ÷W"á6ñBó∞¢6ˆÁ7BC’ˆ∆ófU'VÂ7FGW5Fñ÷W'5∑6ñE”∞¢ñbáBó∂6∆V$ñÁFW'f¬áBÊñÁFW'f¬ì∂FV∆WFRˆ∆ófU'VÂ7FGW5Fñ÷W'5∑6ñE”∑–ß–¶gVÊ7Fñˆ‚VÁ7W&U'V‰7FófóGîf˜$7W'&VÁEGW&‚Çó∞¢ÚÚÜ6R3¢Fó6&∆VB(	BF˜∆ófR'V‚7FófóGí6&B&V÷˜fV@¢&WGW&‚ÁV∆√∞ß–¶gVÊ7Fñˆ‚6∆˜6T7W'&VÁD∆ófT7FófóGîw&˜WÇó∞¢6ˆÁ7BGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢ñbÇGW&‚í&WGW&„∞¢GW&‚ÁVW'ï6V∆V7F˜$∆¬ÇrÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%’∂FF÷∆ófR÷7FófóGí÷7W'&VÁC“#%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%’∂FF÷∆ófR÷7FófóGí÷7W'&VÁC“#%“ríÊf˜$V6ÇÜw&˜W”Á∞¢w&˜WÁ&V÷˜fTGG&ñ'WFRÇvFF÷∆ófR÷7FófóGí÷7W'&VÁBrì∞¢ˆfñÊ∆ó¶T∆ófT7FófóGîFó66∆˜7W&Tw&˜WÜw&˜Wì∞¢“ì∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊&W76ñˆÂ7FFTf˜$7W'&VÁE6W76ñˆ‚Çó∞¢6ˆÁ7B7FFS◊vñÊF˜rÂˆ6ˆ◊&W76ñˆÂVì∞¢ñbÇ7FFW«¬2Á6W76ñˆÁ««7FFRÁ6W76ñˆ‰ñB”’2Á6W76ñˆ‚Á6W76ñˆÂˆñBí&WGW&‚ÁV∆√∞¢&WGW&‚7FFS∞ß–¶gVÊ7Fñˆ‚ó46ˆ◊&W76ñˆÂVï'VÊÊñÊrÇó∞¢6ˆÁ7B7FFS’ˆ6ˆ◊&W76ñˆÂ7FFTf˜$7W'&VÁE6W76ñˆ‚Çì∞¢6ˆÁ7B∆ˆ6≥’ˆ6ˆ◊&W76ñˆÂ6W76ñˆ‰∆ˆ6≤Çì∞¢&WGW&‚Çá7FFRbg7FFRÁÜ6S””“w'VÊÊñÊrrí«¬Ü∆ˆ6≤bb2Á6W76ñˆ‚bb∆ˆ6≥””’2Á6W76ñˆ‚Á6W76ñˆÂˆñBíì∞ß–¢ÚÚ&W7F˜&RFÜR6ˆ◊˜6W"∆6VÜˆ∆FW"6fVBvÜV‚WFÚ÷6ˆ◊7Fñˆ‚7F'FVB‚6fRF¢ÚÚ6∆¬vÜVÊWfW"6ˆ◊&W76ñˆ‚∆VfW2FÜR'VÊÊñÊr7FFR¬g&ˆ“ÁíFÇÜ6∆V"¿¢ÚÚÊˆ‚◊'VÊÊñÊr6WD6ˆ◊&W76ñˆÂVí¬˜"Fó&V7BvñÊF˜rÂˆ6ˆ◊&W76ñˆÂVì÷ÁV∆¬ñ‚FÜP¢ÚÚ54RÜÊF∆W"í(	BóBÊÚ÷˜2vÜV‚Ê˜FÜñÊrv26fVB‚Ç33S"ê¶gVÊ7Fñˆ‚˜&W7F˜&T6ˆ◊&W76ñˆÂ∆6VÜˆ∆FW"Çó∞¢6ˆÁ7BˆñÁWC“BÇv◊6rrì∞¢ñbÖˆñÁWBbgGóVˆbˆ6ˆ◊&W76ñˆÂ∆6VÜˆ∆FW%6fVC””“w7G&ñÊrró∞¢ˆñÁWBÁ∆6VÜˆ∆FW#’ˆ6ˆ◊&W76ñˆÂ∆6VÜˆ∆FW%6fVC∞¢–¢ˆ6ˆ◊&W76ñˆÂ∆6VÜˆ∆FW%6fVC÷ÁV∆√∞ß–¶gVÊ7Fñˆ‚6∆V$6ˆ◊&W76ñˆÂVíÇó∞¢vñÊF˜rÂˆ6ˆ◊&W76ñˆÂVì÷ÁV∆√∞¢ˆ6∆V$6ˆ◊&W76ñˆ‰V∆6VEFñ÷W"Çì∞¢˜6WD6ˆ◊&W76ñˆÂ6W76ñˆ‰∆ˆ6≤ÜÁV∆¬ì∞¢˜&W7F˜&T6ˆ◊&W76ñˆÂ∆6VÜˆ∆FW"Çì∞¢&VÊFW$6ˆ◊&W76ñˆÂVíÇì∞ß–¶gVÊ7Fñˆ‚6WD6ˆ◊&W76ñˆÂVíá7FFRó∞¢ñbÇ7FFRó∞¢6∆V$6ˆ◊&W76ñˆÂVíÇì∞¢&WGW&„∞¢–¢6ˆÁ7BÊWáE7FFS◊≤‚‚Á7FFW”∞¢ñbÜÊWáE7FFRÊWFˆ÷Fñ2bfÊWáE7FFRÁÜ6S””“w'VÊÊñÊrrbbˆ6ˆ◊&W76ñˆ‰V∆6VE7F'FVDBÜÊWáE7FFRíó∞¢ÊWáE7FFRÁ7F'FVDC‘FFRÊÊ˜rÇíÛ∞¢–¢vñÊF˜rÂˆ6ˆ◊&W76ñˆÂVì÷ÊWáE7FFS∞¢ñbÜÊWáE7FFRÁ6W76ñˆ‰ñBí˜6WD6ˆ◊&W76ñˆÂ6W76ñˆ‰∆ˆ6≤ÜÊWáE7FFRÁ6W76ñˆ‰ñBì∞¢ñbÜÊWáE7FFRÊWFˆ÷Fñ2bfÊWáE7FFRÁÜ6S””“w'VÊÊñÊrró∞¢˜7F'D6ˆ◊&W76ñˆ‰V∆6VEFñ÷W"Çì∞¢6ˆÁ7BˆñÁWC“BÇv◊6rrì∞¢ñbÖˆñÁWBbeˆ6ˆ◊&W76ñˆÂ∆6VÜˆ∆FW%6fVC””÷ÁV∆¬ó∞¢ˆ6ˆ◊&W76ñˆÂ∆6VÜˆ∆FW%6fVC’ˆñÁWBÁ∆6VÜˆ∆FW#∞¢ˆñÁWBÁ∆6VÜˆ∆FW#◊GóVˆbC””“vgVÊ7Fñˆ‚s˜BÇv6ˆ◊˜6W%ˆ6ˆ◊&W76ñˆÂ˜vñ∆≈˜VWVRró«¬uGóR÷W76vR(	BóBvñ∆¬VWVRÊB6VÊBgFW"6ˆ◊&W76ñˆ‚s¢uGóR÷W76vR(	BóBvñ∆¬VWVRÊB6VÊBgFW"6ˆ◊&W76ñˆ‚s∞¢–¢“V«6R∞¢ˆ6∆V$6ˆ◊&W76ñˆ‰V∆6VEFñ÷W"Çì∞¢ÚÚ∆VfñÊrFÜR'VÊÊñÊr7FFRÜRÊr‚6WD6ˆ◊&W76ñˆÂVíÜFˆÊRíí◊W7B&W7F˜&RFÜP¢ÚÚ∆6VÜˆ∆FW"FˆÚ(	BÊ˜BˆÊ«í6∆V$6ˆ◊&W76ñˆÂVíÇí‚Ç33S"∆V≤fóÇê¢˜&W7F˜&T6ˆ◊&W76ñˆÂ∆6VÜˆ∆FW"Çì∞¢–¢&VÊFW$6ˆ◊&W76ñˆÂVíÇì∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊&W76ñˆ‰6&G4áF÷¬á7FFRó∞¢ñbÇ7FFRí&WGW&‚rs∞¢ñbá7FFRÊWFˆ÷Fñ2í&WGW&‚ˆWFÙ6ˆ◊&W76ñˆ‰6&G4áF÷¬á7FFRì∞¢6ˆÁ7B6÷EFWáC◊7FFRÊ6ˆ÷÷ÊEFWáG«¬rˆ6ˆ◊&W72s∞¢6ˆÁ7Bfˆ7W5FWáC◊7FFRÊfˆ7W5F˜ñ3ˆG∑BÇvfˆ7W5ˆ∆&V¬ró”¢G∑7FFRÊfˆ7W5F˜ñ7÷¢rs∞¢6ˆÁ7BÜVFW%FWáC◊7FFRÁÜ6S””“vFˆÊRp¢Úá7FFRÁ7V÷÷'ìÚÊÜVF∆ñÊW««BÇv6ˆ◊&W75ˆ6ˆ◊∆WFUˆ∆&V¬ríê¢¢7FFRÁÜ6S””“vW'&˜"p¢Úá7FFRÊW'&˜%FWáG««BÇv6ˆ◊&W75ˆfñ∆VEˆ∆&V¬ríê¢¢áGóVˆb7FFRÊ&Vf˜&T6˜VÁC””“vÁV÷&W"rÚBÇvÂˆ÷W76vW2r¬7FFRÊ&Vf˜&T6˜VÁBí¢rrì∞¢6ˆÁ7B7FGW4&ˆGì◊7FFRÁÜ6S””“vW'&˜"p¢Ú∑7FFRÊW'&˜%FWáG««BÇv6ˆ◊&W75ˆfñ∆VEˆ∆&V¬rí¬fˆ7W5FWáE“Êfñ«FW"Ñ&ˆˆ∆V‚íÊ¶ˆñ‚Çu∆‚rê¢¢∑BÇv6ˆ◊&W76ñÊrrí¬fˆ7W5FWáE“Êfñ«FW"Ñ&ˆˆ∆V‚íÊ¶ˆñ‚Çu∆‚rì∞¢6ˆÁ7B7FGW4∆&V√◊7FFRÁÜ6S””“vFˆÊRp¢ÚBÇv6ˆ◊&W75ˆ6ˆ◊∆WFUˆ∆&V¬rê¢¢7FFRÁÜ6S””“vW'&˜"p¢ÚBÇv6ˆ◊&W75ˆfñ∆VEˆ∆&V¬rê¢¢BÇv6ˆ◊&W75˜'VÊÊñÊuˆ∆&V¬rì∞¢6ˆÁ7B7FGW4ñ6ˆ„◊7FFRÁÜ6S””“vFˆÊRp¢Ú∆íÇv6ÜV6≤r√2ê¢¢7FFRÁÜ6S””“vW'&˜"p¢Ú∆íÇwÇr√2ê¢¢«7‚6∆73“'Fˆˆ¬÷6&B◊'VÊÊñÊr÷F˜B#„¬˜7„Ê∞¢6ˆÁ7BFˆÊT6&DáF÷√◊7FFRÁÜ6S””“vFˆÊRp¢Úˆ6ˆ◊&W76ñˆÂ7FGW46&DáF÷¬á∞¢7FGW4∆&V¬¿¢&WfñWuFWáC¢ÜVFW%FWáB¿¢FWFñ√¢∑7FFRÁ7V÷÷'ìÚÁFˆ∂VÂˆ∆ñÊR¬7FFRÁ7V÷÷'ìÚÊÊ˜FR¬fˆ7W5FWáE“Êfñ«FW"Ñ&ˆˆ∆V‚íÊ¶ˆñ‚Çu∆‚rí¿¢ñ6ˆ„¢7FGW4ñ6ˆ‚¿¢˜V„¢G'VR¿¢f&ñÁD6∆73¢wFˆˆ¬÷6&B÷6ˆ◊&W72÷6ˆ◊∆WFRr¿¢“ê¢¢rs∞¢6ˆÁ7B&VfW&VÊ6TáF÷√“á7FFRÁÜ6S””“vFˆÊRrbg7FFRÁ&VfW&VÊ6UFWáBê¢Úˆ6ˆ◊&W76ñˆÂ&VfW&VÊ6T6&DáF÷¬á7FFRÁ&VfW&VÊ6UFWáB¬f«6Rê¢¢rs∞¢&WGW&‚ ¢∆Fób6∆73“'Fˆˆ¬÷6&B◊&˜r6ˆ◊&W76ñˆ‚÷6&B◊&˜r"FF÷6ˆ◊&W76ñˆ‚÷6&C“##‡¢∆Fób6∆73“'Fˆˆ¬÷6&BFˆˆ¬÷6&B÷6ˆ◊&W72÷6ˆ÷÷ÊB#‡¢∆Fób6∆73“'Fˆˆ¬÷6&B÷ÜVFW""ˆÊ6∆ñ6≥“'FÜó2Ê6∆˜6W7BÇrÁFˆˆ¬÷6&BríÊ6∆74∆ó7BÁFˆvv∆RÇv˜V‚rí#‡¢«7‚6∆73“'Fˆˆ¬÷6&B÷ñ6ˆ‚#‚G∂∆íÇw6WGFñÊw2r√2ó”¬˜7„‡¢«7‚6∆73“'Fˆˆ¬÷6&B÷Ê÷R#‚G∂W62áBÇv6ˆ÷÷ÊEˆ∆&V¬ríó”¬˜7„‡¢«7‚6∆73“'Fˆˆ¬÷6&B◊&WfñWr#‚G∂W62Ü6÷EFWáBó”¬˜7„‡¢¬ˆFóc‡¢¬ˆFóc‡¢¬ˆFóc‡¢∆Fób6∆73“'Fˆˆ¬÷6&B◊&˜r6ˆ◊&W76ñˆ‚÷6&B◊&˜r"FF÷6ˆ◊&W76ñˆ‚÷6&C“##‡¢G∑7FFRÁÜ6S””“vFˆÊRp¢ÚFˆÊT6&DáF÷¿¢¢ˆ6ˆ◊&W76ñˆÂ7FGW46&DáF÷¬á∞¢7FGW4∆&V¬¿¢&WfñWuFWáC¢ÜVFW%FWáB¿¢FWFñ√¢7FGW4&ˆGí¿¢ñ6ˆ„¢7FGW4ñ6ˆ‚¿¢˜V„¢f«6R¿¢f&ñÁD6∆73¢7FFRÁÜ6S””“vW'&˜"p¢ÚwFˆˆ¬÷6&B÷6ˆ◊&W72÷W'&˜"p¢¢wFˆˆ¬÷6&B÷6ˆ◊&W72◊'VÊÊñÊrr¿¢“ê¢–¢¬ˆFóc‡¢G∑&VfW&VÊ6TáF÷«÷∞ß–¶gVÊ7Fñˆ‚ˆWFÙ6ˆ◊&W76ñˆ‰&6TFWFñ¬á7FFRó∞¢6ˆÁ7B'VÊÊñÊs◊7FFRbg7FFRÁÜ6S””“w'VÊÊñÊrs∞¢ñbá'VÊÊñÊró&WGW&‚t6ˆ◊&W76ñÊr6ˆÁFWáBs∞¢ñbá7FFRbg7FFRÁÜ6S””“vFˆÊRró&WGW&‚t6ˆÁFWáBWFÚ÷6ˆ◊&W76VBs∞¢&WGW&‚rs∞ß–¶gVÊ7Fñˆ‚ˆWFÙ6ˆ◊&W76ñˆÂ&WfñWuFWáBá7FFRó∞¢6ˆÁ7B'VÊÊñÊs◊7FFRbg7FFRÁÜ6S””“w'VÊÊñÊrs∞¢ñbá'VÊÊñÊró&WGW&‚t6ˆ◊&W76ñÊr6ˆÁFWáBs∞¢ñbá7FFRbg7FFRÁÜ6S””“vFˆÊRró&WGW&‚t6ˆÁFWáBWFÚ÷6ˆ◊&W76VBs∞¢&WGW&‚rs∞ß–¶gVÊ7Fñˆ‚ˆWFÙ6ˆ◊&W76ñˆ‰FWFñ≈FWáBá7FFRó∞¢6ˆÁ7B'VÊÊñÊs◊7FFRbg7FFRÁÜ6S””“w'VÊÊñÊrs∞¢ñbá'VÊÊñÊró&WGW&‚rs∞¢&WGW&‚rs∞ß–¶gVÊ7Fñˆ‚ˆWFÙ6ˆ◊&W76ñˆ‰6&G4áF÷¬á7FFRó∞¢6ˆÁ7B&WfñWs’ˆWFÙ6ˆ◊&W76ñˆÂ&WfñWuFWáBá7FFRì∞¢6ˆÁ7BFˆÊS◊7FFRbg7FFRÁÜ6S””“vFˆÊRs∞¢&WGW&‚ ¢∆Fób6∆73“'Fˆˆ¬÷6&B◊&˜r6ˆ◊&W76ñˆ‚÷6&B◊&˜rWFÚ÷6ˆ◊&W76ñˆ‚÷FófñFW"◊&˜rWFÚ÷6ˆ◊&W76ñˆ‚÷ñÊ∆ñÊR◊&˜r"FF÷6ˆ◊&W76ñˆ‚÷6&C“##‡¢∆Fób6∆73“&WFÚ÷6ˆ◊&W76ñˆ‚÷FófñFW"WFÚ÷6ˆ◊&W76ñˆ‚÷ñÊ∆ñÊRG∂FˆÊSÚrWFÚ÷6ˆ◊&W76ñˆ‚÷FófñFW"÷FˆÊRs¢rw“"&ñ÷∆&V√“"G∂W62á&WfñWró“#‡¢«7‚6∆73“&WFÚ÷6ˆ◊&W76ñˆ‚÷FófñFW"÷∆&V¬#‚G∂FˆÊSˆ∆íÇvfñ∆R◊FWáBr√2ì¶∆íÇv∆ˆFW"r√2ó“G∂W62á&WfñWró”¬˜7„‡¢¬ˆFóc‡¢¬ˆFócÊ∞ß–¶gVÊ7Fñˆ‚ˆWFÙ6ˆ◊&W76ñˆÂv˜&∂∆ˆtÊˆFRá7FFRó∞¢6ˆÁ7B&˜s÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢&˜rÊ6∆74Ê÷S“wFˆˆ¬÷6&B◊&˜r6ˆ◊&W76ñˆ‚÷6&B◊&˜rWFÚ÷6ˆ◊&W76ñˆ‚÷FófñFW"◊&˜rWFÚ÷6ˆ◊&W76ñˆ‚÷ñÊ∆ñÊR◊&˜rs∞¢&˜rÁ6WDGG&ñ'WFRÇvFF÷6ˆ◊&W76ñˆ‚÷6&Br¬srì∞¢6ˆÁ7B∆&V√’ˆWFÙ6ˆ◊&W76ñˆÂ&WfñWuFWáBá7FFRì∞¢6ˆÁ7BFˆÊS◊7FFRbg7FFRÁÜ6S””“vFˆÊRs∞¢&˜rÊñÊÊW$ÖD‘√÷ ¢∆Fób6∆73“&WFÚ÷6ˆ◊&W76ñˆ‚÷FófñFW"WFÚ÷6ˆ◊&W76ñˆ‚÷ñÊ∆ñÊRG∂FˆÊSÚrWFÚ÷6ˆ◊&W76ñˆ‚÷FófñFW"÷FˆÊRs¢rw“"&ñ÷∆&V√“"G∂W62Ü∆&V¬ó“#‡¢«7‚6∆73“&WFÚ÷6ˆ◊&W76ñˆ‚÷FófñFW"÷∆&V¬#‚G∂FˆÊSˆ∆íÇvfñ∆R◊FWáBr√2ì¶∆íÇv∆ˆFW"r√2ó“G∂W62Ü∆&V¬ó”¬˜7„‡¢¬ˆFócÊ∞¢&WGW&‚&˜s∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊&W76ñˆ‰6&G4ÊˆFRá7FFRó∞¢6ˆÁ7Bw&÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢w&Ê6∆74Ê÷S“v6ˆ◊&W76ñˆ‚◊GW&‚s∞¢w&ÊñÊÊW$ÖD‘√÷∆Fób6∆73“&6ˆ◊&W76ñˆ‚◊GW&‚÷&∆ˆ6∑2#‚Gµˆ6ˆ◊&W76ñˆ‰6&G4áF÷¬á7FFRó”¬ˆFócÊ∞¢&WGW&‚w&∞ß–¶gVÊ7Fñˆ‚VÊD∆ófT6ˆ◊&W76ñˆ‰6&Bá7FFRó∞¢ñbÇ2Á6W76ñˆÁ«¬2Ê7FófU7G&V‘ñG«¬7FFRí&WGW&‚f«6S∞¢ñbÜó4∆ófTÊ6Ü˜$7FófóGï66VÊT˜vÊW"Ö2Ê7FófU7G&V‘ñBíó∞¢&WGW&‚˜&VÊFW$∆ófTÊ6Ü˜$7FófóGï66VÊTf˜%7G&V“Ö2Ê7FófU7G&V‘ñB¬2Á6W76ñˆ‚Á6W76ñˆÂˆñBì∞¢–¢6ˆÁ7B67&ˆ∆≈6Ê6Ü˜C’ˆ6GW&T÷W76vU67&ˆ∆≈6Ê6Ü˜BÇì∞¢∆WBGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢ñbÇGW&‚ó∞¢GW&„’ˆ7&VFT76ó7FÁEGW&‚Çì∞¢GW&‚ÊñC“v∆ófT76ó7FÁEGW&‚s∞¢ñbÖ2Á6W76ñˆ‚íGW&‚ÊFF6WBÁ6W76ñˆ‰ñC’2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢BÇv◊6tñÊÊW"ríÊVÊD6Üñ∆BáGW&‚ì∞¢–¢6ˆÁ7BñÊÊW#’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÇñÊÊW"í&WGW&‚f«6S∞¢6∆˜6T7W'&VÁD∆ófT7FófóGîw&˜WÇì∞¢ñbá7FFRÊWFˆ÷Fñ2ó∞¢6ˆÁ7Bw&˜W÷VÁ7W&T∆ófUv˜&∂∆ˆt6ˆÁFñÊW"ÜñÊÊW"«∂7FófóGî∂Wì•ˆ7FófóGî∂Wîf˜$∆ófUGW&‚Çó“ì∞¢6ˆÁ7B∆ó7C’˜Fˆˆ≈v˜&∂∆ˆt∆ó7DV¬Üw&˜Wì∞¢ñbÇw&˜W«¬∆ó7Bí&WGW&‚f«6S∞¢6ˆÁ7BÊˆFS’ˆWFÙ6ˆ◊&W76ñˆÂv˜&∂∆ˆtÊˆFRá7FFRì∞¢ÊˆFRÁ6WDGG&ñ'WFRÇvFF÷∆ófR÷6ˆ◊&W76ñˆ‚÷6&Br¬srì∞¢ÊˆFRÁ6WDGG&ñ'WFRÇvFF÷6ˆ◊&W76ñˆ‚◊Ü6Rr≈7G&ñÊrá7FFRÁÜ6W«¬rríì∞¢ñbá7FFRÁÜ6S””“w'VÊÊñÊrró∞¢6ˆÁ7B7F'FVC’ˆ6ˆ◊&W76ñˆ‰V∆6VE7F'FVDBá7FFRó«ƒFFRÊÊ˜rÇíÛ∞¢ÊˆFRÁ6WDGG&ñ'WFRÇvFF÷6ˆ◊&W76ñˆ‚◊7F'FVB÷Br≈7G&ñÊrá7F'FVBíì∞¢ÊˆFRÁ6WDGG&ñ'WFRÇvFF÷6ˆ◊&W76ñˆ‚÷÷W76vRr≈7G&ñÊrá7FFRÊ÷W76vW«¬t6ˆ◊&W76ñÊr6ˆÁFWáBríì∞¢˜7F'D6ˆ◊&W76ñˆ‰V∆6VEFñ÷W"Çì∞¢“V«6R∞¢ÊˆFRÁ&V÷˜fTGG&ñ'WFRÇvFF÷6ˆ◊&W76ñˆ‚◊7F'FVB÷Brì∞¢ÊˆFRÁ&V÷˜fTGG&ñ'WFRÇvFF÷6ˆ◊&W76ñˆ‚÷÷W76vRrì∞¢6ˆÁ7Bˆ7FófT6ˆ◊7FFR“ˆ6ˆ◊&W76ñˆÂ7FFTf˜$7W'&VÁE6W76ñˆ‚Çì∞¢ñbÇˆ7FófT6ˆ◊7FFR«¬ˆ7FófT6ˆ◊7FFRÊWFˆ÷Fñ2«¬ˆ7FófT6ˆ◊7FFRÁÜ6R”“w'VÊÊñÊrrí∞¢ˆ6∆V$6ˆ◊&W76ñˆ‰V∆6VEFñ÷W"Çì∞¢–¢–¢6ˆÁ7BWÜó7FñÊu'VÊÊñÊs÷w&˜WÁVW'ï6V∆V7F˜"Çu∂FF÷∆ófR÷6ˆ◊&W76ñˆ‚÷6&C“#%’∂FF÷6ˆ◊&W76ñˆ‚◊7F'FVB÷E“rì∞¢6ˆÁ7BWÜó7FñÊtFˆÊS‘'&íÊg&ˆ“Üw&˜WÁVW'ï6V∆V7F˜$∆¬Çu∂FF÷∆ófR÷6ˆ◊&W76ñˆ‚÷6&C“#%’∂FF÷6ˆ◊&W76ñˆ‚◊Ü6S“&FˆÊR%“rííÁ˜Çì∞¢6ˆÁ7BWÜó7FñÊs◊7FFRÁÜ6S””“w'VÊÊñÊrsˆWÜó7FñÊu'VÊÊñÊs¢ÜWÜó7FñÊu'VÊÊñÊw«∆WÜó7FñÊtFˆÊRì∞¢ñbÜWÜó7FñÊríWÜó7FñÊrÁ&W∆6UvóFÇÜÊˆFRì∞¢V«6R∆ó7BÊVÊD6Üñ∆BÜÊˆFRì∞¢˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íÜw&˜Wì∞¢ˆ÷˜fT∆ófU'VÂ7FGW5FıGW&‰VÊBÇì∞¢˜&W7F˜&T÷W76vU67&ˆ∆≈6Ê6Ü˜E6÷Tg&÷Rá67&ˆ∆≈6Ê6Ü˜Bì∞¢ñbáGóVˆb67&ˆ∆ƒñeñÊÊVC””“vgVÊ7Fñˆ‚rí67&ˆ∆ƒñeñÊÊVBÇì∞¢&WGW&‚G'VS∞¢–¢6ˆÁ7BÊˆFS’ˆ6ˆ◊&W76ñˆ‰6&G4ÊˆFRá7FFRì∞¢ñbÇÊˆFRí&WGW&‚f«6S∞¢ÊˆFRÁ6WDGG&ñ'WFRÇvFF÷∆ófR÷6ˆ◊&W76ñˆ‚÷6&Br¬srì∞¢ñbá7FFRÊWFˆ÷Fñ2bg7FFRÁÜ6S””“w'VÊÊñÊrró∞¢6ˆÁ7B7F'FVC’ˆ6ˆ◊&W76ñˆ‰V∆6VE7F'FVDBá7FFRó«ƒFFRÊÊ˜rÇíÛ∞¢ÊˆFRÁ6WDGG&ñ'WFRÇvFF÷6ˆ◊&W76ñˆ‚◊7F'FVB÷Br≈7G&ñÊrá7F'FVBíì∞¢ÊˆFRÁ6WDGG&ñ'WFRÇvFF÷6ˆ◊&W76ñˆ‚÷÷W76vRr≈7G&ñÊrá7FFRÊ÷W76vW«¬tWFÚ÷6ˆ◊&W76ñÊr6ˆÁFWáB‚‚‚ríì∞¢˜7F'D6ˆ◊&W76ñˆ‰V∆6VEFñ÷W"Çì∞¢“V«6R∞¢ÚÚ6ˆ◊∆WFñˆ‚˜"W'&˜#¢6∆V"FÜRV∆6VB◊Fñ÷W"GG&ñ'WFW26ÚFÜP¢ÚÚñÁFW'f¬&VFW"Öˆ6ˆ◊&W76ñˆ‰∆ófT6&E7FFRíFˆW6‚wB∂VWG&VFñÊp¢ÚÚFÜR&W∆6VB6&B2'VÊÊñÊr6ˆ◊&W76ñˆ‚Ç3#ìs2í‡¢ÊˆFRÁ&V÷˜fTGG&ñ'WFRÇvFF÷6ˆ◊&W76ñˆ‚◊7F'FVB÷Brì∞¢ÊˆFRÁ&V÷˜fTGG&ñ'WFRÇvFF÷6ˆ◊&W76ñˆ‚÷÷W76vRrì∞¢ÚÚˆÊ«í6∆V"FÜRv∆ˆ&¬Fñ÷W"vÜV‚FÜR¶7FófR¢6W76ñˆ‚Ü2ÊÚ'VÊÊñÊp¢ÚÚ6ˆ◊&W76ñˆ‚‚‚54R6ˆ◊∆WFñˆ‚f˜"&6∂w&˜VÊB6W76ñˆ‚◊W7BÊ˜@¢ÚÚ∂ñ∆¬FÜRFñ÷W"FÜBw2G&ófñÊrFÜR7W'&VÁB6W76ñˆ‚w2Fó7∆í‡¢6ˆÁ7Bˆ7FófT6ˆ◊7FFR“ˆ6ˆ◊&W76ñˆÂ7FFTf˜$7W'&VÁE6W76ñˆ‚Çì∞¢ñbÇˆ7FófT6ˆ◊7FFR«¬ˆ7FófT6ˆ◊7FFRÊWFˆ÷Fñ2«¬ˆ7FófT6ˆ◊7FFRÁÜ6R”“w'VÊÊñÊrrí∞¢ˆ6∆V$6ˆ◊&W76ñˆ‰V∆6VEFñ÷W"Çì∞¢–¢–¢6ˆÁ7BWÜó7FñÊs÷ñÊÊW"ÁVW'ï6V∆V7F˜"Çu∂FF÷∆ófR÷6ˆ◊&W76ñˆ‚÷6&C“#%“rì∞¢ñbÜWÜó7FñÊríWÜó7FñÊrÁ&W∆6UvóFÇÜÊˆFRì∞¢V«6RñÊÊW"ÊVÊD6Üñ∆BÜÊˆFRì∞¢˜&W7F˜&T÷W76vU67&ˆ∆≈6Ê6Ü˜E6÷Tg&÷Rá67&ˆ∆≈6Ê6Ü˜Bì∞¢ñbáGóVˆb67&ˆ∆ƒñeñÊÊVC””“vgVÊ7Fñˆ‚rí67&ˆ∆ƒñeñÊÊVBÇì∞¢&WGW&‚G'VS∞ß–¶gVÊ7Fñˆ‚ˆó4ÜÊFˆfe7V÷÷'ïFˆˆ≈ñ∆ˆBáf«VRó∞¢ñbÇf«VW««GóVˆbf«VR”“vˆ&¶V7Bw«ƒ'&íÊó4'&íáf«VRíí&WGW&‚f«6S∞¢&WGW&‚f«VRÂˆÜÊFˆfe˜7V÷÷'ïˆ6&B””“G'VS∞ß–¶gVÊ7Fñˆ‚˜'6TÜÊFˆfe7V÷÷'ïñ∆ˆBÜ6ˆÁFVÁBó∞¢ñbÇ6ˆÁFVÁBí&WGW&‚ÁV∆√∞¢ñbáGóVˆb6ˆÁFVÁC””“vˆ&¶V7Brbb'&íÊó4'&íÜ6ˆÁFVÁBíí&WGW&‚ˆó4ÜÊFˆfe7V÷÷'ïFˆˆ≈ñ∆ˆBÜ6ˆÁFVÁBìˆ6ˆÁFVÁC¶ÁV∆√∞¢ñbáGóVˆb6ˆÁFVÁB”“w7G&ñÊrrí&WGW&‚ÁV∆√∞¢G'í∞¢6ˆÁ7B'6VC‘•4Ù‚Á'6RÜ6ˆÁFVÁBì∞¢&WGW&‚ˆó4ÜÊFˆfe7V÷÷'ïFˆˆ≈ñ∆ˆBá'6VBì˜'6VC¶ÁV∆√∞¢“6F6ÇÜRí∞¢&WGW&‚ÁV∆√∞¢–ß–¶gVÊ7Fñˆ‚ˆÜÊFˆfe7V÷÷'ï7FFTg&ˆ‘÷W76vRÜ“ó∞¢ñbÇ◊«∆“Á&ˆ∆R”“wFˆˆ¬rí&WGW&‚ÁV∆√∞¢6ˆÁ7Bñ∆ˆB“˜'6TÜÊFˆfe7V÷÷'ïñ∆ˆBÜ“Ê6ˆÁFVÁBì∞¢ñbÇñ∆ˆBí&WGW&‚ÁV∆√∞¢ñbÖ7G&ñÊráñ∆ˆBÁ6W76ñˆÂˆñG«¬rríbb2Á6W76ñˆ‚bb7G&ñÊrÜ“Á6W76ñˆÂˆñG«¬rríbb7G&ñÊráñ∆ˆBÁ6W76ñˆÂˆñBí”’7G&ñÊrÖ2Á6W76ñˆ‚Á6W76ñˆÂˆñG«¬rríí∞¢&WGW&‚ÁV∆√∞¢–¢6ˆÁ7B7V÷÷'í“7G&ñÊráñ∆ˆBÁ7V÷÷'ó«¬rríÁG&ñ“Çì∞¢ñbÇ7V÷÷'íí&WGW&‚ÁV∆√∞¢&WGW&‚∞¢Ü6S¢vFˆÊRr¿¢6ÜÊÊV√¢ñ∆ˆBÊ6ÜÊÊV¬«¬ÁV∆¬¿¢&˜VÊG3¢ÁV÷&W"Êó4fñÊóFRáñ∆ˆBÁ&˜VÊG2ì˜ñ∆ˆBÁ&˜VÊG3¶ÁV∆¬¿¢7V÷÷'í¿¢f∆∆&6≥¢ñ∆ˆBÊf∆∆&6≤¿¢vVÊW&FVDC¢ÁV÷&W"áñ∆ˆBÊvVÊW&FVEˆBí«¬ÁV∆¬¿¢”∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ∆∆V7DÜÊFˆfe7V÷÷'ï7FFW2Ü÷W76vW2ó∞¢6ˆÁ7B7FFW3’µ”∞¢ñbÇ'&íÊó4'&íÜ÷W76vW2íí&WGW&‚7FFW3∞¢f˜"Ü∆WBì”∂ì∆÷W76vW2Ê∆VÊwFÉ∂í≤≤ó∞¢6ˆÁ7B7FFS’ˆÜÊFˆfe7V÷÷'ï7FFTg&ˆ‘÷W76vRÜ÷W76vW5∂ï“ì∞¢ñbá7FFRí7FFW2ÁW6Çá∑7FFR¬&tñGÉ¶ó“ì∞¢–¢&WGW&‚7FFW3∞ß–¶gVÊ7Fñˆ‚ˆó46ˆÁFWáD6ˆ◊7Fñˆ‰÷W76vRÜ“ó∞¢ñbÇ◊«¬“Á&ˆ∆W«∆“Á&ˆ∆S””“wFˆˆ¬rí&WGW&‚f«6S∞¢6ˆÁ7BFWáC÷◊6t6ˆÁFVÁBÜ“ó«≈7G&ñÊrÜ“Ê6ˆÁFVÁG«¬rrì∞¢&WGW&‚ˆó46ˆÁFWáD6ˆ◊7FñˆÂFWáBáFWáBì∞ß–¶gVÊ7Fñˆ‚ˆó46ˆÁFWáD6ˆ◊7FñˆÂFWáBáFWáBó∞¢&WGW&‚ıÂ«2•≈∂6ˆÁFWáB6ˆ◊7Fñˆ‚ˆíÁFW7BÖ7G&ñÊráFWáG«¬rríí«¬ıÂ«2¶6ˆÁFWáB6ˆ◊7Fñˆ‚ˆíÁFW7BÖ7G&ñÊráFWáG«¬rríì∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊7FñˆÂ7V÷÷'ï6Vv÷VÁBáFWáBó∞¢ÚÚ÷ó'&˜"ˆbíˆ6ˆ◊&W76ñˆÂˆÊ6Ü˜"Áí6ˆ◊7FñˆÂ˜7V÷÷'ï˜6Vv÷VÁBÇí‚FÜP¢ÚÚ&W∆íF6Ç6'&ñW2FÜó2ÜV«W"6ÚóB7Fó26V∆b÷6ˆÁFñÊVBˆ‚W7G&V“‡¢6ˆÁ7B3’7G&ñÊráFWáG«¬rríÁ&W∆6RÇıÂ«2≤Ú¬rrì∞¢6ˆÁ7B∆˜s◊2ÁFÙ∆˜vW$66RÇì∞¢ñbÜ∆˜rÁ7F'G5vóFÇÇu∂6ˆÁFWáB6ˆ◊7Fñˆ‚ró«∆∆˜rÁ7F'G5vóFÇÇv6ˆÁFWáB6ˆ◊7Fñˆ‚ríí&WGW&‚3∞¢ñbÇ∆˜rÁ7F'G5vóFÇÇu∑&ñ˜"6ˆÁFWáBríí&WGW&‚ÁV∆√∞¢6ˆÁ7BFV∆ñ”÷∆˜rÊñÊFWÑˆbÇu∂VÊBˆb&ñ˜"6ˆÁFWáBrì∞¢ñbÜFV∆ñ”””“”í&WGW&‚ÁV∆√∞¢6ˆÁ7BgFW#◊2Á6∆ñ6RÜFV∆ñ“ì∞¢6ˆÁ7B6∆˜6S÷gFW"ÊñÊFWÑˆbÇu“rì∞¢ñbÜ6∆˜6S””“”í&WGW&‚ÁV∆√∞¢6ˆÁ7B6Vv÷VÁC÷gFW"Á6∆ñ6RÜ6∆˜6R≥íÁ&W∆6RÇıÂ«2≤Ú¬rrì∞¢ñbá6Vv÷VÁBÁFÙ∆˜vW$66RÇíÁ7F'G5vóFÇÇu∂6ˆÁFWáB6ˆ◊7Fñˆ‚ríí&WGW&‚6Vv÷VÁC∞¢&WGW&‚ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆó5&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D÷&∂W%FWáBáFWáBó∞¢&WGW&‚ıÂ«2•≈∑ñ˜W"7FófRF6≤∆ó7Bv2&W6W'fVB7&˜726ˆÁFWáB6ˆ◊&W76ñˆÂ≈“ˆíÁFW7BÖ7G&ñÊráFWáG«¬rríì∞ß–¶gVÊ7Fñˆ‚ˆó5&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D÷&∂W$ˆÊ«ïFWáBáFWáBó∞¢&WGW&‚ˆó5&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D÷&∂W%FWáBáFWáBê¢bb7G&ñÊráFWáG«¬rrê¢Á&W∆6RÇıÂ«2•≈∑ñ˜W"7FófRF6≤∆ó7Bv2&W6W'fVB7&˜726ˆÁFWáB6ˆ◊&W76ñˆÂ≈’«2¢ˆí¬rrê¢ÁG&ñ“Çì∞ß–¶gVÊ7Fñˆ‚ˆó5&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D÷W76vRÜ“ó∞¢ñbÇ◊«∆“Á&ˆ∆R”“wW6W"rí&WGW&‚f«6S∞¢6ˆÁ7BFWáC÷◊6t6ˆÁFVÁBÜ“ó«≈7G&ñÊrÜ“Ê6ˆÁFVÁG«¬rrì∞¢&WGW&‚ıÂ«2•≈∑ñ˜W"7FófRF6≤∆ó7Bv2&W6W'fVB7&˜726ˆÁFWáB6ˆ◊&W76ñˆÂ≈“ˆíÁFW7BáFWáBì∞ß–¶gVÊ7Fñˆ‚ˆó4÷&∂W$ˆÊ«î76ó7FÁD6ˆ◊&W76ñˆ‰÷W76vRÜ“ó∞¢ñbÇ◊«∆“Á&ˆ∆R”“v76ó7FÁBrí&WGW&‚f«6S∞¢6ˆÁ7BFWáC÷◊6t6ˆÁFVÁBÜ“ó«≈7G&ñÊrÜ“Ê6ˆÁFVÁG«¬rrì∞¢&WGW&‚ˆó5&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D÷&∂W$ˆÊ«ïFWáBáFWáBì∞ß–¶gVÊ7Fñˆ‚˜&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7E&WfñWráFWáBó∞¢6ˆÁ7B&ˆGì’7G&ñÊráFWáG«¬rrê¢Á&W∆6RÇıÂ«2•≈∑ñ˜W"7FófRF6≤∆ó7Bv2&W6W'fVB7&˜726ˆÁFWáB6ˆ◊&W76ñˆÂ≈’«2¢ˆí¬rrê¢ÁG&ñ“Çì∞¢&WGW&‚Ü&ˆGíÁ7∆óBÇı∆‚≤ÚíÊ÷Ü∆ñÊS”Ê∆ñÊRÁG&ñ“ÇííÊfñ«FW"Ñ&ˆˆ∆V‚íÁ6∆ñ6RÉ√"íÊ¶ˆñ‚Çrrí«¬BÇw&W6W'fVE˜F6µˆ∆ó7Eˆ∆&V¬ríì∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊&W76ñˆ‰÷W76vTÊ6Ü˜$∂WíÜ“ó∞¢ñbÇ◊«¬“Á&ˆ∆W«∆“Á&ˆ∆S””“wFˆˆ¬rí&WGW&‚ÁV∆√∞¢∆WB6ˆÁFVÁC“rs∞¢G'ó∞¢6ˆÁFVÁC’7G&ñÊrÜ◊6t6ˆÁFVÁBÜ“ó«¬rrì∞¢÷6F6ÇÖÚó∞¢6ˆÁFVÁC’7G&ñÊrÜ“Ê6ˆÁFVÁG«¬rrì∞¢–¢6ˆÁ7BÊ˜&”÷6ˆÁFVÁBÁ&W∆6RÇı«2≤ˆr¬rríÁG&ñ“ÇíÁ6∆ñ6RÉ√cì∞¢6ˆÁ7BG3÷“Â˜G7«∆“ÁFñ÷W7F◊«∆ÁV∆√∞¢6ˆÁ7BGF6Ü÷VÁG3‘'&íÊó4'&íÜ“ÊGF6Ü÷VÁG2ìˆ“ÊGF6Ü÷VÁG2Ê∆VÊwFÉ£∞¢ñbÇÊ˜&“bbGF6Ü÷VÁG2bbG2í&WGW&‚ÁV∆√∞¢&WGW&‚∑&ˆ∆S•7G&ñÊrÜ“Á&ˆ∆W«¬rrí¬G2¬FWáC¶Ê˜&“¬GF6Ü÷VÁG7”∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊&W76ñˆ‰Ê6Ü˜$ñÊFWÇáfó5vóFÑñGÇ¬Ê6Ü˜$∂Wí¬f∆∆&6¥ñGÉ÷ÁV∆¬ó∞¢ñbÜÊ6Ü˜$∂Wíbd'&íÊó4'&íáfó5vóFÑñGÇíó∞¢f˜"Ü∆WBì◊fó5vóFÑñGÇÊ∆VÊwFÇ”∂ì„”∂í““ó∞¢6ˆÁ7B6ÊFñFFS’ˆ6ˆ◊&W76ñˆ‰÷W76vTÊ6Ü˜$∂Wíáfó5vóFÑñGÖ∂ï“Ê“ì∞¢ñbÇ6ÊFñFFRí6ˆÁFñÁVS∞¢6ˆÁ7BÊ6Ü˜%G3’7G&ñÊrÜÊ6Ü˜$∂WíÁG3ÛÚrrì∞¢6ˆÁ7B6ÊFñFFUG3’7G&ñÊrÜ6ÊFñFFRÁG3ÛÚrrì∞¢ñbÄ¢6ÊFñFFRÁ&ˆ∆S””’7G&ñÊrÜÊ6Ü˜$∂WíÁ&ˆ∆W«¬rríb`¢ÇÊ6Ü˜%G7«¬6ÊFñFFUG7«∆6ÊFñFFUG3””÷Ê6Ü˜%G2íb`¢7G&ñÊrÜ6ÊFñFFRÁFWáG«¬rrì””’7G&ñÊrÜÊ6Ü˜$∂WíÁFWáG«¬rríb`¢ÁV÷&W"Ü6ÊFñFFRÊGF6Ü÷VÁG7«√ì””‘ÁV÷&W"ÜÊ6Ü˜$∂WíÊGF6Ü÷VÁG7«√ê¢ó∞¢&WGW&‚ì∞¢–¢–¢–¢&WGW&‚GóVˆbf∆∆&6¥ñGÉ””“vÁV÷&W"rÚf∆∆&6¥ñGÇ¢ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆ∆FW7D6ˆ◊&W76ñˆÂ&VfW&VÊ6T÷W76vRÜ÷W76vW2¬7V÷÷'ïFWáC“rró∞¢ñbÇ'&íÊó4'&íÜ÷W76vW2ó«¬÷W76vW2Ê∆VÊwFÇí&WGW&‚∂÷W76vS¶ÁV∆¬¬&tñGÉ¢””∞¢6ˆÁ7B7V÷÷'îÊ˜&”’7G&ñÊrá7V÷÷'ïFWáG«¬rríÁ&W∆6RÇı«2≤ˆr¬rríÁG&ñ“Çì∞¢f˜"Ü∆WBì÷÷W76vW2Ê∆VÊwFÇ”∂ì„”∂í““ó∞¢6ˆÁ7B”÷÷W76vW5∂ï”∞¢ñbÇˆó46ˆÁFWáD6ˆ◊7Fñˆ‰÷W76vRÜ“íí6ˆÁFñÁVS∞¢ñbÇ7V÷÷'îÊ˜&“í&WGW&‚∂÷W76vS¶“¬&tñGÉ¶ó”∞¢∆WB6ˆÁFVÁC“rs∞¢G'ó∞¢6ˆÁFVÁC’7G&ñÊrÜ◊6t6ˆÁFVÁBÜ“ó«¬rrì∞¢÷6F6ÇÖÚó∞¢6ˆÁFVÁC’7G&ñÊrÇÜ“bf“Ê6ˆÁFVÁBó«¬rrì∞¢–¢6ˆÁ7B6ˆÁFVÁDÊ˜&”÷6ˆÁFVÁBÁ&W∆6RÇı«2≤ˆr¬rríÁG&ñ“Çì∞¢ñbÜ6ˆÁFVÁDÊ˜&“ÊñÊ6«VFW2á7V÷÷'îÊ˜&“íí&WGW&‚∂÷W76vS¶“¬&tñGÉ¶ó”∞¢–¢&WGW&‚∂÷W76vS¶ÁV∆¬¬&tñGÉ¢””∞ß–¶gVÊ7Fñˆ‚˜6Ü˜V∆E6Ü˜u6WGF∆VD6ˆ◊&W76ñˆÂ&VfW&VÊ6Rá&VfW&VÊ6UFWáBó∞¢&WGW&‚7G&ñÊrá&VfW&VÊ6UFWáG«¬rríÁG&ñ“Çíbbˆó46ˆÁFWáD6ˆ◊7FñˆÂFWáBá&VfW&VÊ6UFWáBì∞ß–¢ÚÚV«G&÷6ˆ◊7BFó7∆íÉ##b”Ç”Çì¢FÜR6ˆ◊7Fñˆ‚÷&∂W"FWáB7F'G2vóFÇ¢ÚÚ∆ˆÊrfóÜVBVÁfV∆˜RˆbÜÊF∆ñÊrñÁ7G'V7FñˆÁ2‚FÜR6ˆÁfW'6Fñˆ‚6&B◊W7@¢ÚÚ&WfñWrFÜRDîtU5BÜvˆ¬˜7FFRFÜRW6W"6&W2&˜WBí¬ÊWfW"FÜRVÁfV∆˜R‡¶gVÊ7Fñˆ‚ˆ6ˆ◊7Fñˆ‰FñvW7EFWáBáFWáBó∞¢6ˆÁ7B3’7G&ñÊráFWáG«¬rrì∞¢ÚÚFÜRVÁfV∆˜R&˜6RVÊG2&ñváB&Vf˜&RFÜRfó'7B÷&∂F˜v‚ÜVFñÊrˆ‚óG0¢ÚÚ˜v‚∆ñÊR‚V˜FVBÜVFñÊw2ñÁ6ñFRFÜRVÁfV∆˜RÜRÊr‚&g&ˆ“r22Üó7F˜&ñ6¿¢ÚÚF6≤6Ê6Ü˜Br˜"Áí˜FÜW"6V7Fñˆ‚"íÊWfW"6óBB∆ñÊR7F'B‡¢6ˆÁ7B”◊2Ê÷F6ÇÇÚÖÁ≈∆‚í7≥√7“µÂ∆Â“Úì∞¢ñbÇ“í&WGW&‚2ÁG&ñ“Çì∞¢6ˆÁ7B7F'C÷“ÊñÊFWÇ≤Ü’≥”Û£ì∞¢&WGW&‚2Á6∆ñ6Rá7F'BíÁG&ñ“Çì∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊7Fñˆ‰6&E&WfñWráFWáBó∞¢6ˆÁ7BFñvW7C’ˆ6ˆ◊7Fñˆ‰FñvW7EFWáBáFWáBê¢Á&W∆6RÇı‚7≥√7“ˆv“¬rrê¢Á&W∆6RÇı‰Üó7F˜&ñ6¬F6≤6Ê6Ü˜E«2¢ˆí¬rrì∞¢&WGW&‚FñvW7BÁ7∆óBÇı∆‚≤ÚíÊ÷Ü√”Ê¬ÁG&ñ“ÇííÊfñ«FW"Ñ&ˆˆ∆V‚íÁ6∆ñ6RÉ√"íÊ¶ˆñ‚ÇrríÁ6∆ñ6RÉ√##ì∞ß–¢ÚÚ∆¬6ˆ◊7Fñˆ‚÷&∂W'2&W6VÁBñ‚FÜRƒÙDTBG&Á67&óB¬ˆ∆FW7Bfó'7B‚V6Ä¢ÚÚˆÊR&VÊFW'22óG2˜v‚6ˆ∆∆6VB6&BBóG2&V¬˜6óFñˆ‚6ÚFÜRW6W"6‡¢ÚÚ6VRvÜV‚FÜR6ˆÁFWáBv26ˆ◊7FVBÊB&V˜V‚FÜRFñvW7BñÊ∆ñÊR‡¶gVÊ7Fñˆ‚ˆ∆ˆFVD6ˆ◊7Fñˆ‰÷&∂W%&tñGá2Ü÷W76vW2ó∞¢6ˆÁ7B˜WC’µ”∞¢ñbÇ'&íÊó4'&íÜ÷W76vW2íí&WGW&‚˜WC∞¢f˜"Ü∆WBì”∂ì∆÷W76vW2Ê∆VÊwFÉ∂í≤≤ó∞¢ñbÖˆó46ˆÁFWáD6ˆ◊7Fñˆ‰÷W76vRÜ÷W76vW5∂ï“íí˜WBÁW6ÇÜíì∞¢–¢&WGW&‚˜WC∞ß–¢ÚÚ6WGF∆VB6ˆ◊7Fñˆ‚vÜ˜6R÷&∂W"6óG2&Vf˜&RFÜR6W'fW"÷∆ˆFVBFñ¬◊W7@¢ÚÚ&V÷ñ‚fó6ñ&∆RBFÜRF˜ˆbFÜBFñ¬‚Ê6Ü˜&ñÊróBFÚ‚ˆ∆B76ó7FÁB˜Fˆˆ¿¢ÚÚGW&‚6‚'W'íóBñÁ6ñFRÜñFFV‚v˜&∂∆ˆr¬vÜñ6Ç÷∂W26ˆ◊7Fñˆ‚∆ˆˆ≤'6VÁB‡¢ÚÚ7W'&VÁE7V÷÷'îf∆∆&6∂ó2G'VRvÜV‚FÜR6W76ñˆ‚w27W'&VÁB7V÷÷'í÷F6ÜW0¢ÚÚÊÚ∆ˆFVB÷&∂W"ÊBFÜW&Vf˜&R&VÊFW'22óG2˜v‚6WGF∆VB6&B‚FÜB6&Bó0¢ÚÚFÜRÊWvW7B6ˆ◊7Fñˆ‚FÜRW6W"6‚6VR¬6ÚóB˜vÁ2FÜR&W6W'fVBF6≤6&G0¢ÚÚÊBÊÚ÷&∂W"6&B÷íGF6ÇFÜV“vñ‚‡¶gVÊ7Fñˆ‚˜6V∆V7D6ˆ◊7Fñˆ‰6&E∆6V÷VÁG2Ü÷&∂W%&tñGá2∆fó'7E&VÊFW&VE&tñGÇ∆7W'&VÁE7V÷÷'îf∆∆&6≥÷f«6Ró∞¢6ˆÁ7B&UvñÊF˜t÷&∂W'3’µ”∞¢6ˆÁ7BñÊ∆ñÊT÷&∂W'3’µ”∞¢6ˆÁ7B&˜VÊF'ì‘ÁV÷&W"Êó4fñÊóFRÜfó'7E&VÊFW&VE&tñGÇìˆfó'7E&VÊFW&VE&tñGÉ¢”∞¢f˜"Ü6ˆÁ7Bf«VRˆb'&íÊó4'&íÜ÷&∂W%&tñGá2ìˆ÷&∂W%&tñGá3•µ“ó∞¢6ˆÁ7B&tñGÉ‘ÁV÷&W"áf«VRì∞¢ñbÇÁV÷&W"Êó4ñÁFVvW"á&tñGÇó««&tñGÉ√í6ˆÁFñÁVS∞¢ñbá&tñGÉ∆&˜VÊF'íí&UvñÊF˜t÷&∂W'2ÁW6Çá&tñGÇì∞¢V«6RñÊ∆ñÊT÷&∂W'2ÁW6Çá&tñGÇì∞¢–¢6ˆÁ7BF6¥˜vÊW#÷7W'&VÁE7V÷÷'îf∆∆&6∞¢˜∂∂ñÊC¢v7W'&VÁB◊7V÷÷'ír«&tñGÉ¢”–¢¶ñÊ∆ñÊT÷&∂W'2Ê∆VÊwFÄ¢˜∂∂ñÊC¢vñÊ∆ñÊRr«&tñGÉ¶ñÊ∆ñÊT÷&∂W'5∂ñÊ∆ñÊT÷&∂W'2Ê∆VÊwFÇ”◊–¢¢&UvñÊF˜t÷&∂W'2Ê∆VÊwFÄ¢ˆÁV∆¿¢ß∂∂ñÊC¢w&R◊vñÊF˜rr«&tñGÉß&UvñÊF˜t÷&∂W'5∑&UvñÊF˜t÷&∂W'2Ê∆VÊwFÇ”◊”∞¢&WGW&‚∑&UvñÊF˜t÷&∂W'2∆ñÊ∆ñÊT÷&∂W'2«F6¥˜vÊW'”∞ß–¶gVÊ7Fñˆ‚ˆñÁ6W'D6ˆ◊7Fñˆ‰6&DÊˆFW2ÜVÁG&ñW2«F6¥˜vÊW"∆ñÁ6W'DÊˆFRó∞¢6ˆÁ7BñÁ6W'FVDÊˆFW3’µ”∞¢∆WBF6¥˜vÊW$ÊˆFS÷ÁV∆√∞¢ñbÇ'&íÊó4'&íÜVÁG&ñW2ó««GóVˆbñÁ6W'DÊˆFR”“vgVÊ7Fñˆ‚rí&WGW&‚∂ñÁ6W'FVDÊˆFW2«F6¥˜vÊW$ÊˆFW”∞¢f˜"Ü6ˆÁ7BVÁG'íˆbVÁG&ñW2ó∞¢ñbÇVÁG'ìÚÊÊˆFRí6ˆÁFñÁVS∞¢6ˆÁ7BñÁ6W'FVC÷ñÁ6W'DÊˆFRÜVÁG'íÊÊˆFR∆VÁG'íÁ&tñGÇ∆VÁG'íÊ∂ñÊBí”÷f«6S∞¢ñbÇñÁ6W'FVG«¬VÁG'íÊÊˆFRÁ&VÁDV∆V÷VÁBí6ˆÁFñÁVS∞¢ñÁ6W'FVDÊˆFW2ÁW6ÇÜVÁG'íÊÊˆFRì∞¢ñbáF6¥˜vÊW"bfVÁG'íÊ∂ñÊC””◊F6¥˜vÊW"Ê∂ñÊBbfVÁG'íÁ&tñGÉ””◊F6¥˜vÊW"Á&tñGÇíF6¥˜vÊW$ÊˆFS÷VÁG'íÊÊˆFS∞¢–¢&WGW&‚∂ñÁ6W'FVDÊˆFW2«F6¥˜vÊW$ÊˆFW”∞ß–¶gVÊ7Fñˆ‚ˆñÁ6W'E&W6W'fVD6ˆ◊&W76ñˆÂF6¥f∆∆&6≤áF6¥˜vÊW$ÊˆFR«7FÊF∆ˆÊTÊˆFR∆ñÁ6W'DÊˆFRó∞¢ñbáF6¥˜vÊW$ÊˆFSÚÁ&VÁDV∆V÷VÁG«¬7FÊF∆ˆÊTÊˆFW««GóVˆbñÁ6W'DÊˆFR”“vgVÊ7Fñˆ‚rí&WGW&‚f«6S∞¢&WGW&‚ñÁ6W'DÊˆFRá7FÊF∆ˆÊTÊˆFRí”÷f«6Rbb7FÊF∆ˆÊTÊˆFRÁ&VÁDV∆V÷VÁC∞ß–¶gVÊ7Fñˆ‚˜ñ‰6ˆ◊7Fñˆ‰6&DEF˜ÜñÊÊW"∆ÊˆFRó∞¢ñbÇñÊÊW'«¬ÊˆFRí&WGW&‚f«6S∞¢ñÊÊW"ÊVÊD6Üñ∆BÜÊˆFRì∞¢&WGW&‚G'VS∞ß–¶gVÊ7Fñˆ‚˜ñÂ6WGF∆VD6ˆ◊&W76ñˆÂ&VfW&VÊ6TEF˜ÜñÊÊW"∆ÊˆFR«&VfW&VÊ6T÷W76vU&tñGÇó∞¢ñbá&VfW&VÊ6T÷W76vU&tñGÉ„”í&WGW&‚f«6S∞¢&WGW&‚˜ñ‰6ˆ◊7Fñˆ‰6&DEF˜ÜñÊÊW"∆ÊˆFRì∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊&W76ñˆÂ&VfW&VÊ6T6&DáF÷¬áFWáB¬˜V„÷f«6Ró∞¢6ˆÁ7B6˜ì’ˆVÊvñÊTv&T6ˆ◊&W76ñˆ‰6˜íÇì∞¢6ˆÁ7B&WfñWs’ˆ6ˆ◊7Fñˆ‰6&E&WfñWráFWáBó««FWáBÁ7∆óBÇı∆‚≤ÚíÊfñ«FW"Ñ&ˆˆ∆V‚íÁ6∆ñ6RÉ√"íÊ¶ˆñ‚Çrrì∞¢&WGW&‚ ¢∆Fób6∆73“'Fˆˆ¬÷6&B◊&˜r6ˆ◊&W76ñˆ‚÷6&B◊&˜r"FF÷6ˆ◊&W76ñˆ‚÷6&C“#"FF◊&r◊FWáC“"G∂W62áFWáBó“#‡¢∆Fób6∆73“'Fˆˆ¬÷6&BFˆˆ¬÷6&B÷6ˆ◊&W72◊&VfW&VÊ6RG∂˜V„Úr˜V‚s¢rw“#‡¢∆Fób6∆73“'Fˆˆ¬÷6&B÷ÜVFW""ˆÊ6∆ñ6≥“'FÜó2Ê6∆˜6W7BÇrÁFˆˆ¬÷6&BríÊ6∆74∆ó7BÁFˆvv∆RÇv˜V‚rí#‡¢«7‚6∆73“'Fˆˆ¬÷6&B÷ñ6ˆ‚#‚G∂∆íÇw7F"r√2ó”¬˜7„‡¢«7‚6∆73“'Fˆˆ¬÷6&B÷Ê÷R#‚G∂W62Ü6˜íÊ∆&V¬ó”¬˜7„‡¢«7‚6∆73“'Fˆˆ¬÷6&B◊&WfñWr#‚G∂W62Ü6˜íÁ&WfñWró“+rG∂W62á&WfñWró”¬˜7„‡¢«7‚6∆73“'Fˆˆ¬÷6&B◊Fˆvv∆R#‚G∂∆íÇv6ÜWg&ˆ‚◊&ñváBr√"ó”¬˜7„‡¢∆'WGFˆ‚6∆73“&◊6r÷6˜í÷'F‚◊6r÷7Fñˆ‚÷'F‚Fˆˆ¬÷6&B÷6˜í6ˆ◊&W76ñˆ‚◊&VfW&VÊ6R÷6˜í"FóF∆S“"G∑BÇv6˜író“"ˆÊ6∆ñ6≥“&6˜î◊6ráFÜó2ì∂WfVÁBÁ7F˜&˜vFñˆ‚Çí#‚G∂∆íÇv6˜ír√2ó”¬ˆ'WGFˆ„‡¢¬ˆFóc‡¢∆Fób6∆73“'Fˆˆ¬÷6&B÷FWFñ¬#‡¢∆Fób6∆73“'Fˆˆ¬÷6&B◊&W7V«B#‡¢«&S‚G∂W62áFWáBó”¬˜&S‡¢¬ˆFóc‡¢¬ˆFóc‡¢¬ˆFóc‡¢ ¢¬ˆFócÊ∞ß–¶gVÊ7Fñˆ‚˜&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D6&DáF÷¬Ü“¬˜V„÷f«6Ró∞¢6ˆÁ7BFWáC÷◊6t6ˆÁFVÁBÜ“ó«≈7G&ñÊrÜ“Ê6ˆÁFVÁG«¬rrì∞¢&WGW&‚ ¢∆Fób6∆73“'Fˆˆ¬÷6&B◊&˜r6ˆ◊&W76ñˆ‚÷6&B◊&˜r"FF÷6ˆ◊&W76ñˆ‚÷6&C“#"FF◊&r◊FWáC“"G∂W62áFWáBó“#‡¢Gµˆ6ˆ◊&W76ñˆÂ7FGW46&DáF÷¬á∞¢7FGW4∆&V√¢BÇw&W6W'fVE˜F6µˆ∆ó7Eˆ∆&V¬rí¿¢&WfñWuFWáC¢˜&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7E&WfñWráFWáBí¿¢FWFñ√¢FWáB¿¢ñ6ˆ„¢∆íÇv∆ó7B◊FˆFÚr√2í¿¢˜V‚¿¢f&ñÁD6∆73¢wFˆˆ¬÷6&B÷6ˆ◊&W72◊&VfW&VÊ6Rr¿¢“ó–¢¬ˆFócÊ∞ß–¶gVÊ7Fñˆ‚˜&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D6&G4áF÷¬Ü÷W76vW2ó∞¢&WGW&‚Ü÷W76vW7«≈µ“íÊ÷Ü””Â˜&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D6&DáF÷¬Ü“¬f«6RííÊ¶ˆñ‚Çrrì∞ß–¶gVÊ7Fñˆ‚ˆ∆FW7EFˆFıFˆˆƒóFV◊2Ü÷W76vW2ó∞¢f˜"Ü∆WBì“Ü÷W76vW7«≈µ“íÊ∆VÊwFÇ”∂ì„”∂í““ó∞¢6ˆÁ7B”÷÷W76vW5∂ï”∞¢ñbÇ◊«∆“Á&ˆ∆R”“wFˆˆ¬rí6ˆÁFñÁVS∞¢G'ó∞¢6ˆÁ7Bñ∆ˆC◊GóVˆb“Ê6ˆÁFVÁC””“w7G&ñÊrsÙ•4Ù‚Á'6RÜ“Ê6ˆÁFVÁBì¶“Ê6ˆÁFVÁC∞¢ñbáñ∆ˆBbd'&íÊó4'&íáñ∆ˆBÁFˆF˜2íí&WGW&‚ñ∆ˆBÁFˆF˜3∞¢÷6F6ÇÖÚó≤–¢–¢&WGW&‚ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆÜ47FófUFˆFÙóFV◊2ÜóFV◊2ó∞¢&WGW&‚'&íÊó4'&íÜóFV◊2íbbóFV◊2Á6ˆ÷RÜóFV””Á∞¢6ˆÁ7B7FGW3’7G&ñÊrÜóFV“bfóFV“Á7FGW7«¬rríÁG&ñ“ÇíÁFÙ∆˜vW$66RÇì∞¢&WGW&‚7FGW3””“wVÊFñÊrw««7FGW3””“vñÂ˜&ˆw&W72s∞¢“ì∞ß–¶gVÊ7Fñˆ‚ˆ∆FW7E&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D÷W76vW2Ü÷W76vW2ó∞¢6ˆÁ7B∆FW7C’≤‚‚‚Ü÷W76vW7«≈µ“ï“Á&WfW'6RÇíÊfñÊBÜ””Âˆó5&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D÷W76vRÜ“íì∞¢ñbÇ∆FW7Bí&WGW&‚µ”∞¢6ˆÁ7B∆FW7EFˆF˜3’ˆ∆FW7EFˆFıFˆˆƒóFV◊2Ü÷W76vW2ì∞¢ñbÑ'&íÊó4'&íÜ∆FW7EFˆF˜2íbbˆÜ47FófUFˆFÙóFV◊2Ü∆FW7EFˆF˜2íí&WGW&‚µ”∞¢&WGW&‚∂∆FW7E”∞ß–¶gVÊ7Fñˆ‚ˆó56÷T∆ˆ6ƒFíÜFFT¬FFT"ó∞¢&WGW&‚FFTÊvWDgV∆≈ñV"Çì””÷FFT"ÊvWDgV∆≈ñV"Çê¢bbFFTÊvWD÷ˆÁFÇÇì””÷FFT"ÊvWD÷ˆÁFÇÇê¢bbFFTÊvWDFFRÇì””÷FFT"ÊvWDFFRÇì∞ß–¶gVÊ7Fñˆ‚ˆf˜&÷D÷W76vTfˆ˜FW%Fñ÷W7F◊áG5f¬ó∞¢ñbÇG5f¬í&WGW&‚rs∞¢6ˆÁ7BFFS÷ÊWrFFRáG5f¬£ì∞¢6ˆÁ7BÊ˜s÷ÊWrFFRÇì∞¢ÚÚW6Rˆf˜&÷DñÂ6W'fW%G¢vÜV‚fñ∆&∆R(	BóB6˜'&V7F«íÜÊF∆W2g&7FñˆÊ¬÷Ü˜W ¢ÚÚˆfg6WG2∆ñ∂RñÊFñ≥S3FÜBWF2Ùt’B6ÊÊ˜BWá&W72‚f∆«2&6≤FÚ∆ñ‡¢ÚÚFÙ∆ˆ6∆U7G&ñÊrvÜV‚6W76ñˆÁ2Êß2Ü6‚wB∆ˆFVBñWB‡¢6ˆÁ7Bf◊C“áGóVˆbˆf˜&÷DñÂ6W'fW%G£””“vgVÊ7Fñˆ‚rìıˆf˜&÷DñÂ6W'fW%G£¶ÁV∆√∞¢ñbÖˆó56÷T∆ˆ6ƒFíÜFFR¬Ê˜ríó∞¢6ˆÁ7B˜G3◊∂Ü˜W#¢s"÷FñvóBr¬÷ñÁWFS¢s"÷FñvóBw”∞¢&WGW&‚f◊Cˆf◊BÜFFR∆˜G2ì¶FFRÁFÙ∆ˆ6∆UFñ÷U7G&ñÊrÖµ“¬˜G2ì∞¢–¢6ˆÁ7B˜G3◊∂÷ˆÁFÉ¢w6Ü˜'Br¬Fì¢vÁV÷W&ñ2r¬Ü˜W#¢vÁV÷W&ñ2r¬÷ñÁWFS¢s"÷FñvóBw”∞¢&WGW&‚f◊Cˆf◊BÜFFR∆˜G2ì¶FFRÁFÙ∆ˆ6∆U7G&ñÊrÖµ“¬˜G2ì∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊&W76ñˆ‰VÊvñÊTf˜%6W76ñˆ‚Çó∞¢&WGW&‚7G&ñÊrÄ¢Ö2Á6W76ñˆ‚bbÄ¢2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%ˆVÊvñÊP¢«¬2Á6W76ñˆ‚Ê6ˆÁFWáEˆVÊvñÊP¢íí«¬v6ˆ◊&W76˜"p¢íÁG&ñ“ÇíÁFÙ∆˜vW$66RÇí«¬v6ˆ◊&W76˜"s∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊&W76ñˆ‰÷ˆFTf˜%6W76ñˆ‚Çó∞¢&WGW&‚7G&ñÊrÄ¢Ö2Á6W76ñˆ‚be2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%ˆ÷ˆFRí«¬w7V÷÷'ïˆ6ˆ◊7Fñˆ‚p¢íÁG&ñ“ÇíÁFÙ∆˜vW$66RÇí«¬w7V÷÷'ïˆ6ˆ◊7Fñˆ‚s∞ß–¶gVÊ7Fñˆ‚ˆVÊvñÊTv&T6ˆ◊&W76ñˆ‰6˜íÜVÊvñÊS’ˆ6ˆ◊&W76ñˆ‰VÊvñÊTf˜%6W76ñˆ‚Çí¬÷ˆFS’ˆ6ˆ◊&W76ñˆ‰÷ˆFTf˜%6W76ñˆ‚Çíó∞¢ñbÜVÊvñÊS””“v∆6“w«∆÷ˆFS””“v∆˜76∆W75˜&WG&ñWf¬ró∞¢&WGW&‚∞¢∆&V√ßBÇw&WG&ñWf≈ˆ6ˆÁFWáEˆ∆&V¬rí¿¢&WfñWsßBÇw&WG&ñWf≈ˆ6ˆÁFWáE˜&WfñWrrí¿¢”∞¢–¢&WGW&‚∞¢∆&V√ßBÇv6ˆÁFWáEˆ6ˆ◊7FñˆÂˆ∆&V¬rí¿¢&WfñWsßBÇw&VfW&VÊ6UˆˆÊ«ïˆ∆&V¬rí¿¢”∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊&W76ñˆÂ7FGW46&DáF÷¬á∞¢7FGW4∆&V¬¿¢&WfñWuFWáB¿¢FWFñ¬¿¢ñ6ˆ‚¿¢˜V„÷f«6R¿¢f&ñÁD6∆73“rr¿ß“ó∞¢6ˆÁ7B7FGW4FWFñ¬“7G&ñÊrÜFWFñ¬«¬rríÁG&ñ“Çì∞¢6ˆÁ7BÜ4&ˆGí“7FGW4FWFñ√∞¢6ˆÁ7B˜V‰6∆72“˜V‚Úr˜V‚r¢rs∞¢6ˆÁ7B7FGW4ñ6ˆ‚“ñ6ˆ„∞¢6ˆÁ7B&ˆGîáF÷¬“Ü4&ˆGíÚ∆Fób6∆73“'Fˆˆ¬÷6&B÷FWFñ¬#„∆Fób6∆73“'Fˆˆ¬÷6&B◊&W7V«B#„«&S‚G∂W62á7FGW4FWFñ¬ó”¬˜&S„¬ˆFóc„¬ˆFócÊ¢rs∞¢6ˆÁ7BFˆvv∆TáF÷¬“Ü4&ˆGíÚ«7‚6∆73“'Fˆˆ¬÷6&B◊Fˆvv∆R#‚G∂∆íÇv6ÜWg&ˆ‚◊&ñváBr√"ó”¬˜7„Ê¢rs∞¢&WGW&‚ ¢∆Fób6∆73“'Fˆˆ¬÷6&BG∑f&ñÁD6∆77“G∂˜V‰6∆77“#‡¢∆Fób6∆73“'Fˆˆ¬÷6&B÷ÜVFW""ˆÊ6∆ñ6≥“'FÜó2Ê6∆˜6W7BÇrÁFˆˆ¬÷6&BríÊ6∆74∆ó7BÁFˆvv∆RÇv˜V‚rí#‡¢G∑7FGW4ñ6ˆÁ–¢«7‚6∆73“'Fˆˆ¬÷6&B÷Ê÷R#‚G∂W62á7FGW4∆&V¬ó”¬˜7„‡¢«7‚6∆73“'Fˆˆ¬÷6&B◊&WfñWr#‚G∂W62á&WfñWuFWáBó”¬˜7„‡¢G∑Fˆvv∆TáF÷«–¢¬ˆFóc‡¢G∂&ˆGîáF÷«–¢¬ˆFócÊ∞ß–¶gVÊ7Fñˆ‚ˆÜÊFˆfe7FFTf˜$7W'&VÁE6W76ñˆ‚Çó∞¢6ˆÁ7B7FFS◊vñÊF˜rÂˆÜÊFˆfeVì∞¢ñbÇ7FFW«¬2Á6W76ñˆÁ««7FFRÁ6W76ñˆ‰ñB”’2Á6W76ñˆ‚Á6W76ñˆÂˆñBí&WGW&‚ÁV∆√∞¢&WGW&‚7FFS∞ß–¶gVÊ7Fñˆ‚6∆V$ÜÊFˆfeVíÇó∞¢vñÊF˜rÂˆÜÊFˆfeVì÷ÁV∆√∞¢˜&VÊFW$÷W76vW5vóFÖ67&ˆ∆≈6Ê6Ü˜BÇì∞ß–¶gVÊ7Fñˆ‚6WDÜÊFˆfeVíá7FFRó∞¢ñbÇ7FFRó∞¢6∆V$ÜÊFˆfeVíÇì∞¢&WGW&„∞¢–¢vñÊF˜rÂˆÜÊFˆfeVì◊≤‚‚Á7FFW”∞¢˜&VÊFW$÷W76vW5vóFÖ67&ˆ∆≈6Ê6Ü˜BÇì∞ß–¶gVÊ7Fñˆ‚ˆÜÊFˆfd6&G4áF÷¬á7FFRó∞¢ñbÇ7FFRí&WGW&‚rs∞¢6ˆÁ7B6ÜÊÊV√’7G&ñÊrá7FFRÊ6ÜÊÊV««¬rríÁG&ñ“Çì∞¢6ˆÁ7B∆&V√÷6ÜÊÊV√ˆG∂6ÜÊÊV«“ÜÊFˆfb7V÷÷'ñ¢tÜÊFˆfb7V÷÷'ís∞¢6ˆÁ7Bó4W'&˜#◊7FFRÁÜ6S””“vW'&˜"s∞¢6ˆÁ7Bó4FˆÊS◊7FFRÁÜ6S””“vFˆÊRs∞¢6ˆÁ7Bó4f∆∆&6≥“7FFRÊf∆∆&6≥∞¢6ˆÁ7BFWFñ√÷ó4W'&˜ ¢Ú7G&ñÊrá7FFRÊW'&˜%FWáG«¬t6˜V∆BÊ˜BvVÊW&FR7V÷÷'í‚∆V6RG'ívñ‚‚rê¢¢ó4FˆÊP¢Ú7G&ñÊrá7FFRÁ7V÷÷'ó«¬rrê¢¢tvVÊW&FñÊrÜÊFˆfb7V÷÷'í‚‚‚s∞¢6ˆÁ7B÷WF◊GóVˆb7FFRÁ&˜VÊG3””“vÁV÷&W"p¢ÚG∑7FFRÁ&˜VÊG7“WáFW&Ê¬6ˆÁfW'6Fñˆ‚&˜VÊG6 ¢¢rs∞¢6ˆÁ7Bñ6ˆ„÷ó4W'&˜ ¢Ú∆íÇwÇr√2ê¢¢ó4FˆÊP¢Ú∆íÇv6ÜV6≤r√2ê¢¢s«7‚6∆73“'Fˆˆ¬÷6&B◊'VÊÊñÊr÷F˜B#„¬˜7„‚s∞¢6ˆÁ7B&ˆGîáF÷√÷ó4FˆÊRbbó4W'&˜ ¢ÚÄ¢G∑&VÊFW$÷BÜFWFñ¬ó“G∞¢ó4f∆∆&6∞¢Ús«6∆73“&ÜÊFˆfb◊7V÷÷'í÷f∆∆&6≤÷Ê˜FR#‰f∆∆&6≤7V÷÷'ívVÊW&FVBg&ˆ“&V6VÁBGW&Á3≤ÊÚ÷ˆFV¬÷&6VB&Ww&óFRv2W6VB„¬˜‚p¢¢rp¢÷ ¢ê¢¢«‚G∂W62ÜFWFñ¬ó”¬˜Ê∞¢&WGW&‚ ¢∆Fób6∆73“'Fˆˆ¬÷6&B◊&˜r6ˆ◊&W76ñˆ‚÷6&B◊&˜rÜÊFˆfb÷6&B◊&˜r"FF÷6ˆ◊&W76ñˆ‚÷6&C“#"FF÷ÜÊFˆfb÷6&C“##‡¢∆Fób6∆73“'Fˆˆ¬÷6&BFˆˆ¬÷6&B÷ÜÊFˆfb◊7V÷÷'íG∂ó4W'&˜#ÚrFˆˆ¬÷6&B÷6ˆ◊&W72÷W'&˜"s¢rw“˜V‚#‡¢∆Fób6∆73“'Fˆˆ¬÷6&B÷ÜVFW""ˆÊ6∆ñ6≥“'FÜó2Ê6∆˜6W7BÇrÁFˆˆ¬÷6&BríÊ6∆74∆ó7BÁFˆvv∆RÇv˜V‚rí#‡¢G∂ñ6ˆÁ–¢«7‚6∆73“'Fˆˆ¬÷6&B÷Ê÷R#‚G∂W62Ü∆&V¬ó”¬˜7„‡¢G∂÷WFˆ«7‚6∆73“'Fˆˆ¬÷6&B◊&WfñWr#‚G∂W62Ü÷WFó”¬˜7„Ê¢rw–¢«7‚6∆73“'Fˆˆ¬÷6&B◊Fˆvv∆R#‚G∂∆íÇv6ÜWg&ˆ‚◊&ñváBr√"ó”¬˜7„‡¢¬ˆFóc‡¢∆Fób6∆73“'Fˆˆ¬÷6&B÷FWFñ¬#‡¢∆Fób6∆73“'Fˆˆ¬÷6&B◊&W7V«BÜÊFˆfb◊7V÷÷'í÷&ˆGí#‚G∂&ˆGîáF÷«”¬ˆFóc‡¢¬ˆFóc‡¢¬ˆFóc‡¢¬ˆFócÊ∞ß–¶gVÊ7Fñˆ‚ˆÜÊFˆfd6&G4ÊˆFRá7FFRó∞¢6ˆÁ7Bw&÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢w&Ê6∆74Ê÷S“v6ˆ◊&W76ñˆ‚◊GW&‚ÜÊFˆfb◊GW&‚s∞¢w&ÊñÊÊW$ÖD‘√÷∆Fób6∆73“&6ˆ◊&W76ñˆ‚◊GW&‚÷&∆ˆ6∑2#‚GµˆÜÊFˆfd6&G4áF÷¬á7FFRó”¬ˆFócÊ∞¢&WGW&‚w&∞ß–¶gVÊ7Fñˆ‚ˆ6ˆÁFWáD6ˆ◊7Fñˆ‰÷W76vTáF÷¬Ü“¬G5FóF∆S“rr¬&W6W'fVD÷W76vW3’µ“ó∞¢6ˆÁ7BFWáC÷◊6t6ˆÁFVÁBÜ“ó«≈7G&ñÊrÜ“Ê6ˆÁFVÁG«¬rrì∞¢&WGW&‚∆Fób6∆73“&6ˆ◊&W76ñˆ‚◊GW&‚#„∆Fób6∆73“&6ˆ◊&W76ñˆ‚◊GW&‚÷&∆ˆ6∑2#‚Gµˆ6ˆ◊&W76ñˆÂ&VfW&VÊ6T6&DáF÷¬áFWáB¬f«6R¬G5FóF∆Ró“Gµ˜&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D6&G4áF÷¬á&W6W'fVD÷W76vW2ó”¬ˆFóc„¬ˆFócÊ∞ß–¶gVÊ7Fñˆ‚&VÊFW$6ˆ◊&W76ñˆÂVíÇó∞¢6ˆÁ7BV√“BÇv∆ófT6ˆ◊&W76ñˆ‰6&G2rì∞¢ñbÇV¬í&WGW&„∞¢V¬ÊñÊÊW$ÖD‘√“rs∞¢V¬Á7Gñ∆RÊFó7∆ì“vÊˆÊRs∞ß–¢ÚÚ6W76ñˆ‚&VÊFW"66ÜS¢fˆñG2gV∆¬÷&∂F˜v‚¥DÙ“&V'Vñ∆BvÜV‚7vóF6ÜñÊr&6∞¢ÚÚFÚ6W76ñˆ‚vÜ˜6R&VÊFW&VBG&Á67&óBñÁWG2&RVÊ6ÜÊvVB‡¢ÚÚ∂WñVB'í6W76ñˆÂˆñB‚ˆÊ«íW6VBˆ‚7&˜72◊6W76ñˆ‚ÊfñvFñˆ‚¬ÊWfW"f˜ ¢ÚÚñ‚◊6W76ñˆ‚WFFW2ÜÊWr÷W76vW2¬VFóG2¬7G&V“WfVÁG2í‡¶6ˆÁ7B˜6W76ñˆ‰áF÷ƒ66ÜS÷ÊWr÷Çì∞¶∆WB˜6W76ñˆ‰áF÷ƒ66ÜU6ñC÷ÁV∆√≤ÚÚ6W76ñˆÂˆñB7W'&VÁF«í&VÊFW&VBñ‚FÜRDÙ–¢ÚÚ3SìcbÑ6ˆFWÇc2ì¢W'6ó7BvÜñ6Ç6VBG&Á7&VÁB’7G&V“GW&Á2FÜRW6W"Ü0¢ÚÚ&WfV∆VB¬∂WñVB'íG∑6W76ñˆÂˆñG”¢G∂˜vÊW%&tñGá÷¬6Ú7vóF6Ç÷víˆ&6≤˜"¢ÚÚÊ˜&÷¬&V'Vñ∆BFˆW2‰ıB6ñ∆VÁF«í&R÷6GW&‚FÜRW6W"«&VGíWáÊFVB‚FÜP¢ÚÚDÙ“FF◊G&Á7&VÁB÷V&∆ñW"◊&WfV∆VFf∆r∆ˆÊRó2∆˜7B7&˜72FÜP¢ÚÚ˜6W76ñˆ‰áF÷ƒ66ÜRñÊÊW$ÖD‘¬&˜VÊB◊G&ó≤FÜó27W'fófW2óB‚&WfV¬«6¢ÚÚñÁf∆ñFFW2FÜB6W76ñˆ‚w266ÜVBÖD‘¬6ÚFÜR7F˜&VB÷&∑Wó6‚wB7F∆R÷6VB‡¶6ˆÁ7B˜G&Á7&VÁE&WfV∆VEGW&Á3÷ÊWr6WBÇì∞¶gVÊ7Fñˆ‚˜G&Á7&VÁE&WfVƒ∂Wíá6W76ñˆ‰ñB¬˜vÊW$ñGÇó∞¢&WGW&‚7G&ñÊrá6W76ñˆ‰ñG«¬Ö2Á6W76ñˆ‚be2Á6W76ñˆ‚Á6W76ñˆÂˆñBó«¬rrí≤s¢rµ7G&ñÊrÜ˜vÊW$ñGÇì∞ß–¶gVÊ7Fñˆ‚6∆V$÷W76vU&VÊFW$66ÜRÇó∞¢ˆ6∆V%&VÊFW$66ÜRÇì∞¢˜6W76ñˆ‰áF÷ƒ66ÜRÊ6∆V"Çì∞¢˜6W76ñˆ‰áF÷ƒ66ÜU6ñC÷ÁV∆√∞¢6∆V%fó6ñ&∆T÷W76vU&˜t66ÜRÇì∞¢ˆ6∆V$÷W76vUfó'GVƒÜVñváD66ÜRÇì∞ß–†¢ÚÚ3cììì¢fVVB7G'V7GW&VBñ∆ˆBfñV∆Bw27G&ñÊrf˜&“Fá&˜VvÇFÜRdÂb”¢ÚÚ∆ˆ˜î‚eTƒ¬¬vóFÜ˜WB÷FW&ñ∆ó¶ñÊr6∆óVB6˜ñW2˜"6∂óñÊrFÜR÷ñFF∆R‡¢ÚÚFÜR&Wfñ˜W2∆VÊwFÇ∂ÜVB∑Fñ¬6∆ó÷FR6÷R÷∆VÊwFÇ÷ñFF∆R÷ˆÊ«íVFóG0¢ÚÚáFˆˆ¬&wV÷VÁG2¬GF6Ü÷VÁB÷WFFF¬Fˆˆ¬6ÊóWG2¬6ˆ◊&W76ñˆ‚÷Ê6Ü˜ ¢ÚÚ∂Wó2í&ˆGV6RñFVÁFñ6¬6ñvÊGW&W2(	BFWFW&÷ñÊó7Fñ27F∆R÷66ÜR6ˆ∆∆ó6ñˆ‡¢ÚÚñ‚˜6W76ñˆ‰áF÷ƒ66ÜRÜ7&˜72◊6W76ñˆ‚ÊfñvFñˆ‚6W'fVBˆ∆BÖD‘¬í‚Ü6ÜñÊp¢ÚÚWfW'í6Ü&7FW"∂VW2FÜR6ñvÊGW&R6VÁ6óFófRFÚÂí6ˆÁFVÁB6ÜÊvRB¢ÚÚ6ˆÁ7FÁB÷∆∆ˆ6Fñˆ‚6˜7C¢7G&ñÊw2&R7G&V÷VB6Ü"÷'í÷6Ü"ÜÊÚ6˜ííÊ@¢ÚÚˆ&¶V7BfñV∆G2&Rv∆∂VB∂Wí÷'í÷∂Wí6ÚˆÊ«í66∆"7G&ñÊrf˜&◊2&RWfW ¢ÚÚ∆∆ˆ6FVB(	BÊÚñÁFVw&¬•4Ù‚Á7G&ñÊvñgíÇíˆbvÜˆ∆Rñ∆ˆB¬ÊBÊ¢ÚÚÜVB˜Fñ¬6∆ñ6R6˜ñW2‚˜&VÊFW$66ÜT∂Wíw2∆VÊwFÇ∂VFvW26Ü˜'F7WBó2ˆÊ«ê¢ÚÚ6fRf˜"FÜR&VÊFW"◊vñÊF˜rvVˆ÷WG'í∂Wí¬vÜW&RWV¬7‚∂VFvW2÷VÁ0¢ÚÚWV¬vñÊF˜s≤ÜW&RWV¬6ñvÊGW&R◊W7B÷V‚WV¬4ÙÂDTÂB‡¶gVÊ7Fñˆ‚ˆFD&˜VÊFVDÜ6ÇÜFB¬f«VR¬FWFÇó∞¢ñbáf«VS”÷ÁV∆¬ó≤FBÇvÁV∆¬rì≤&WGW&„≤–¢6ˆÁ7BC◊GóVˆbf«VS∞¢ñbáC””“w7G&ñÊrró≤FBáf«VRÊ∆VÊwFÇì≤FBáf«VRì≤&WGW&„≤–¢ñbáC””“vÁV÷&W"w««C””“v&ˆˆ∆V‚ró≤FBáBì≤FBáf«VRì≤&WGW&„≤–¢ñbáC””“vˆ&¶V7Bró≤ˆÜ6Ñˆ&¶V7DñÁFÚÜFB¬f«VR¬ÜFWFá«√í≥ì≤&WGW&„≤–¢FBáBì≤FBÖ7G&ñÊráf«VRíì∞ß–¶gVÊ7Fñˆ‚ˆÜ6Ñˆ&¶V7DñÁFÚÜFB¬f«VR¬FWFÇó∞¢ñbáf«VS”÷ÁV∆¬ó≤FBÇvÁV∆¬rì≤&WGW&„≤–¢ñbÜFWFÉ„cBó∞¢ÚÚFÜˆ∆ˆvñ6¬FWFÇÜRÊr‚7ñ6∆ñ27G'V7GW&R•4Ù‚Á7G&ñÊvñgív˜V∆B«6¢ÚÚ&V¶V7Bì¢6W&ñ∆ó¶RñÁFVw&∆«í6ÚÊÚfñV∆Bó26ñ∆VÁF«íG&˜VB(	BFÜP¢ÚÚWÜ7B6÷RFF7Fñ∆¬ññV∆G2FÜRWÜ7B6÷R6ñvÊGW&R‡¢G'ó≤FBÑ•4Ù‚Á7G&ñÊvñgíáf«VRíì≤÷6F6ÇÜRó≤FBÇu∑VÁ6W&ñ∆ó¶&∆U“rì≤–¢&WGW&„∞¢–¢ñbÑ'&íÊó4'&íáf«VRíó∞¢FBÇv'&írì≤FBáf«VRÊ∆VÊwFÇì∞¢f˜"Ü∆WBì”∂ì«f«VRÊ∆VÊwFÉ∂í≤≤ó≤FBÜíì≤ˆFD&˜VÊFVDÜ6ÇÜFB¬f«VU∂ï“¬FWFÇì≤–¢&WGW&„∞¢–¢FBÇvˆ&¶V7Brì∞¢ÚÚ3cììí&R÷vFS¢v∆≤∂Wó2ñ‚îÂ4U%DîÙ‚ı$DU"ÜÊWfW"6˜'FVBí6ÚFÜR66ÜP¢ÚÚ6ñvÊGW&RWV«2FÜR&VÊFW&VB&ˆ¶V7Fñˆ‚(	BFÜRFˆˆ¬÷FWFñ¬&VÊFW"Fá0¢ÚÚW6Rˆ&¶V7BÊVÁG&ñW2áF2Ê&w2í¬vÜñ6Ç&W6W'fW2ñÁ6W'Fñˆ‚˜&FW"‚6˜'FñÊrÜW&P¢ÚÚvfR˜˜6óFR÷ñÁ6W'Fñˆ‚÷˜&FW"&wV÷VÁBˆ&¶V7G2FÜR6÷R6ñvÊGW&RvÜñ∆P¢ÚÚFÜWí&VÊFW"DîddU$TÂBÖD‘¬‚FÜRWá∆ñ6óBW"÷∂WíñÊFWÇó2FÜP¢ÚÚñÁ6W'Fñˆ‚÷˜&FW"Fó67&ñ÷ñÊF˜#¢∂«Ü§¬&WF§'“ÊB∂&WF§"¬«Ü§–¢ÚÚÊ˜rÜ6ÇFñffW&VÁF«í‡¢6ˆÁ7B∂Wó3‘ˆ&¶V7BÊ∂Wó2áf«VRì∞¢FBÜ∂Wó2Ê∆VÊwFÇì∞¢f˜"Ü∆WBì”∂ì∆∂Wó2Ê∆VÊwFÉ∂í≤≤ó≤FBÜ∂Wó5∂ï“ì≤FBÜíì≤ˆFD&˜VÊFVDÜ6ÇÜFB¬f«VU∂∂Wó5∂ï’“¬FWFÇì≤–ß–†¶gVÊ7Fñˆ‚ˆ÷W76vU&VÊFW$66ÜU6ñvÊGW&RÇó∞¢∆WBÜ6É”#cc3c#c∞¢gVÊ7Fñˆ‚FBáf«VRó∞¢6ˆÁ7B3’7G&ñÊráf«VS”÷ÁV∆√Úrsßf«VRì∞¢f˜"Ü∆WBì”∂ì«2Ê∆VÊwFÉ∂í≤≤ó∞¢Ü6Ö„◊2Ê6Ü$6ˆFTBÜíì∞¢Ü6É‘÷FÇÊñ◊V¬ÜÜ6Ç√cssscíì„„„∞¢–¢Ü6Ö„”3∞¢Ü6É‘÷FÇÊñ◊V¬ÜÜ6Ç√cssscíì„„„∞¢–¢6ˆÁ7B÷W76vW3‘'&íÊó4'&íÖ2Ê÷W76vW2ìı2Ê÷W76vW3•µ”∞¢FBÜ÷W76vW2Ê∆VÊwFÇì∞¢f˜"Ü6ˆÁ7B“ˆb÷W76vW2ó∞¢ñbÇ◊««GóVˆb“”“vˆ&¶V7Bró≤FBÇv÷ó76ñÊrrì≤6ˆÁFñÁVS≤–¢FBÜ“Á&ˆ∆Rì∂FBÜ“ÁFñ÷W7F◊ì∂FBÜ“Â˜G2ì∂FBÜ“ÂˆW'&˜"ì∂FBÜ“Â˜7FGW46&Bì∞¢FBÜ◊6t6ˆÁFVÁBÜ“íì∞¢ñbÑ'&íÊó4'&íÜ“Ê6ˆÁFVÁBíó∞¢FBÇv6ˆÁFVÁB÷'&írì∞¢“Ê6ˆÁFVÁBÊf˜$V6Çá'C”Á∞¢ñbÇ'G««GóVˆb'B”“vˆ&¶V7Bró≤FBá'Bì≤&WGW&„≤–¢FBá'BÁGóRì∂FBá'BÊñBì∂FBá'BÊÊ÷Rì∂FBá'BÁFWáBì∂FBá'BÊ6ˆÁFVÁBì∞¢“ì∞¢–¢ñbÑ'&íÊó4'&íÜ“ÁFˆˆ≈ˆ6∆«2íó∞¢FBÇv÷W76vR◊Fˆˆ¬÷6∆«2rì∂FBÜ“ÁFˆˆ≈ˆ6∆«2Ê∆VÊwFÇì∞¢“ÁFˆˆ≈ˆ6∆«2Êf˜$V6ÇáF3”Á∞¢FBáF2bgF2ÊñBì∂FBáF2bgF2ÊÊ÷Rì∂FBáF2bgF2ÁGóRì∞¢FBáF2bgF2ÊgVÊ7Fñˆ‚bgF2ÊgVÊ7Fñˆ‚ÊÊ÷Rì∞¢ÚÚgVÊ7Fñˆ‚Ê&wV÷VÁG2ó2«&VGí7G&ñÊr(	B7G&V÷VBñ‚gV∆¬¬ÊÚ6˜í‡¢ˆFD&˜VÊFVDÜ6ÇÜFB¬F2bgF2ÊgVÊ7Fñˆ‚bgF2ÊgVÊ7Fñˆ‚Ê&wV÷VÁG2ì∞¢“ì∞¢–¢ñbÑ'&íÊó4'&íÜ“Â˜'Fñ≈˜Fˆˆ≈ˆ6∆«2íó∞¢FBÇw'Fñ¬◊Fˆˆ¬÷6∆«2rì∂FBÜ“Â˜'Fñ≈˜Fˆˆ≈ˆ6∆«2Ê∆VÊwFÇì∞¢“Â˜'Fñ≈˜Fˆˆ≈ˆ6∆«2Êf˜$V6ÇáF3”Á∂FBáF2bgF2ÊñBì∂FBáF2bgF2ÊÊ÷Rì∂FBáF2bgF2Á6ÊóWBì∑“ì∞¢–¢ñbÖˆ÷W76vTÜ5&V6ˆÊñÊuñ∆ˆBÜ“ííFBÜ“Á&V6ˆÊñÊw«∆“ÁFÜñÊ∂ñÊw«∆“Â˜&V6ˆÊñÊw«¬w&V6ˆÊñÊrrì∞¢ñbÑ'&íÊó4'&íÜ“ÊGF6Ü÷VÁG2íí“ÊGF6Ü÷VÁG2Êf˜$V6ÇÜ”ÂˆFD&˜VÊFVDÜ6ÇÜFB¬íì∞¢–¢6ˆÁ7BFˆˆƒ6∆«3‘'&íÊó4'&íÖ2ÁFˆˆƒ6∆«2ìı2ÁFˆˆƒ6∆«3•µ”∞¢FBÇw6WGF∆VB◊Fˆˆ¬÷6∆«2rì∂FBáFˆˆƒ6∆«2Ê∆VÊwFÇì∞¢Fˆˆƒ6∆«2Êf˜$V6ÇáF3”Á∞¢ñbÇF7««GóVˆbF2”“vˆ&¶V7Bró≤FBáF2ì≤&WGW&„≤–¢FBáF2ÁFñBì∂FBáF2ÊñBì∂FBáF2ÊÊ÷Rì∂FBáF2ÊFˆÊRì∂FBáF2Êó5ˆFñfbì∂FBáF2Ê76ó7FÁEˆ◊6uˆñGÇì∞¢ˆFD&˜VÊFVDÜ6ÇÜFB¬F2Á6ÊóWBì∞¢ˆFD&˜VÊFVDÜ6ÇÜFB¬F2Ê&w7««∑“ì∞¢“ì∞¢ñbÖ2Á6W76ñˆ‚ó∞¢FBÖ2Á6W76ñˆ‚Ê÷W76vUˆ6˜VÁBì∂FBÖ2Á6W76ñˆ‚ÁWFFVEˆBì∂FBÖ2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%˜fó6ñ&∆UˆñGÇì∞¢ˆFD&˜VÊFVDÜ6ÇÜFB¬2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%ˆ÷W76vUˆ∂Wó«∆ÁV∆¬ì∞¢FBÖ2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%˜7V÷÷'ó«¬rrì∞¢–¢&WGW&‚G∂÷W76vW2Ê∆VÊwFá”¢G∑Fˆˆƒ6∆«2Ê∆VÊwFá”¢G∂Ü6ÇÁFı7G&ñÊrÉbó÷∞ß–†¶gVÊ7Fñˆ‚ˆ6∆ó6∆ïFˆˆ≈6ÊóWBáFWáB¬÷Ñ∆V„”#ó∞¢6ˆÁ7B3’7G&ñÊráFWáG«¬rrì∞¢ñbá2Ê∆VÊwFÉ√÷÷Ñ∆V‚í&WGW&‚3∞¢&WGW&‚G∑2Á6∆ñ6RÉ∆÷Ñ∆V‚ó’∆Â∆‚‚‚‚G'VÊ6FVBG∑2Ê∆VÊwFÇ÷÷Ñ∆VÁ“6Ü'2‚‚Ê∞ß–†¶gVÊ7Fñˆ‚ˆ6∆ïFˆˆ≈&W7V«EFWáBá&ró∞¢6ˆÁ7B3’7G&ñÊrá&w«¬rrì∞¢G'ó∞¢6ˆÁ7B&C‘•4Ù‚Á'6Rá2ì∞¢ñbá&BbbGóVˆb&C””“vˆ&¶V7Bró∞¢f˜"Ü6ˆÁ7B∂Wíˆb≤v˜WGWBr¬w&W7V«Br¬vW'&˜"r¬v6ˆÁFVÁBr¬vFñfbr¬wF6Çu“ó∞¢ñbÑˆ&¶V7BÁ&˜F˜GóRÊÜ4˜vÂ&˜W'GíÊ6∆¬á&B∆∂Wííó∞¢6ˆÁ7Bc◊&E∂∂Wï”∞¢ñbác”÷ÁV∆¬í&WGW&‚rs∞¢&WGW&‚GóVˆbc””“w7G&ñÊrrÚb¢•4Ù‚Á7G&ñÊvñgíáb∆ÁV∆¬√"ì∞¢–¢–¢–¢÷6F6ÇÜRó∑–¢&WGW&‚3∞ß–†¶gVÊ7Fñˆ‚ˆ6∆î∆ˆˆ∑4∆ñ∂UF6ÑFñfbáFWáBó∞¢6ˆÁ7B3’7G&ñÊráFWáG«¬rrì∞¢ñbÇ2í&WGW&‚f«6S∞¢ñbÇı¬•¬•¬¢&Vvñ‚F6ÇÚÁFW7Bá2íí&WGW&‚G'VS∞¢ñbÇıÊFñfb“÷vóBˆ“ÁFW7Bá2íí&WGW&‚G'VS∞¢ñbÇı‰«2ˆ“ÁFW7Bá2íí&WGW&‚G'VS∞¢ñbÇÚÖÁ≈∆‚í““’«2≤ÚÁFW7Bá2íbbÚÖÁ≈∆‚ï¬µ¬µ¬µ«2≤ÚÁFW7Bá2íí&WGW&‚G'VS∞¢&WGW&‚f«6S∞ß–†¶gVÊ7Fñˆ‚ˆ6∆ïFˆˆ≈&W7V«E6ÊóWBá&ró∞¢6ˆÁ7BgV∆≈FWáC’ˆ6∆ïFˆˆ≈&W7V«EFWáBá&rì∞¢ñbÖˆ6∆î∆ˆˆ∑4∆ñ∂UF6ÑFñfbÜgV∆≈FWáBíí&WGW&‚ˆ6∆ó6∆ïFˆˆ≈6ÊóWBÜgV∆≈FWáBì∞¢&WGW&‚7G&ñÊrÜgV∆≈FWáG«¬rríÁ6∆ñ6RÉ√Cì∞ß–†¶gVÊ7Fñˆ‚˜&VfóÜVD6∆îFñfd∆ñÊW2á&VfóÇ¬f«VRó∞¢&WGW&‚7G&ñÊráf«VW«¬rríÁ7∆óBÇu∆‚ríÊ÷Ü∆ñÊS”ÊG∑&Vfóá“G∂∆ñÊW÷íÊ¶ˆñ‚Çu∆‚rì∞ß–†¶gVÊ7Fñˆ‚ˆfó'7D˜vÊVEf«VRÜˆ&¢¬∂Wó2ó∞¢f˜"Ü6ˆÁ7B∂Wíˆb∂Wó2ó∞¢ñbÜˆ&¢bbˆ&¶V7BÁ&˜F˜GóRÊÜ4˜vÂ&˜W'GíÊ6∆¬Üˆ&¢∆∂Wííí&WGW&‚ˆ&•∂∂Wï”∞¢–¢&WGW&‚VÊFVfñÊVC∞ß–†¶gVÊ7Fñˆ‚ˆ6∆ïF6Ö6ÊóWDg&ˆ‘&w2ÜÊ÷R¬&w2ó∞¢ñbÇ&w2«¬GóVˆb&w2”“vˆ&¶V7Brí&WGW&‚rs∞¢6ˆÁ7BFˆˆƒÊ÷S’7G&ñÊrÜÊ÷W«¬rríÁFÙ∆˜vW$66RÇì∞¢f˜"Ü6ˆÁ7B∂Wíˆb≤wF6Çr¬vFñfbu“ó∞¢6ˆÁ7Bc÷&w5∂∂Wï”∞¢ñbáGóVˆbc””“w7G&ñÊrrbbbÁG&ñ“Çíí&WGW&‚ˆ6∆ó6∆ïFˆˆ≈6ÊóWBábì∞¢–¢f˜"Ü6ˆÁ7B∂Wíˆb≤vñÁWBr¬v6ˆÁFVÁBu“ó∞¢6ˆÁ7Bc÷&w5∂∂Wï”∞¢ñbáGóVˆbc””“w7G&ñÊrrbbˆ6∆î∆ˆˆ∑4∆ñ∂UF6ÑFñfbábíí&WGW&‚ˆ6∆ó6∆ïFˆˆ≈6ÊóWBábì∞¢–¢6ˆÁ7Bó4VFóD∆ñ∂S◊FˆˆƒÊ÷S””“v«ï˜F6Çp¢«¬FˆˆƒÊ÷S””“wF6Çp¢«¬FˆˆƒÊ÷RÊñÊ6«VFW2ÇvVFóBrê¢«¬FˆˆƒÊ÷S””“w&W∆6Rp¢«¬FˆˆƒÊ÷S””“w7G%˜&W∆6Rs∞¢ñbÇó4VFóD∆ñ∂Rí&WGW&‚rs∞¢6ˆÁ7Bˆ∆Ef«VS’ˆfó'7D˜vÊVEf«VRÜ&w2≈≤vˆ∆E˜7G&ñÊrr¬vˆ∆E˜7G"r¬vˆ∆Br¬v&Vf˜&Ru“ì∞¢6ˆÁ7BÊWuf«VS’ˆfó'7D˜vÊVEf«VRÜ&w2≈≤vÊWu˜7G&ñÊrr¬vÊWu˜7G"r¬vÊWrr¬vgFW"u“ì∞¢ñbÜˆ∆Ef«VR”◊VÊFVfñÊVB«¬ÊWuf«VR”◊VÊFVfñÊVBó∞¢6ˆÁ7BFÉ’7G&ñÊrÖˆfó'7D˜vÊVEf«VRÜ&w2≈≤vfñ∆U˜FÇr¬wFÇr¬vfñ∆VÊ÷Ru“ó«¬rrì∞¢6ˆÁ7B∆ñÊW3’µ”∞¢ñbáFÇí∆ñÊW2ÁW6ÇáFÇì∞¢ñbÜˆ∆Ef«VR”◊VÊFVfñÊVBí∆ñÊW2ÁW6ÇÖ˜&VfóÜVD6∆îFñfd∆ñÊW2Çr“r¬ˆ∆Ef«VRíì∞¢ñbÜÊWuf«VR”◊VÊFVfñÊVBí∆ñÊW2ÁW6ÇÖ˜&VfóÜVD6∆îFñfd∆ñÊW2Çr≤r¬ÊWuf«VRíì∞¢&WGW&‚ˆ6∆ó6∆ïFˆˆ≈6ÊóWBÜ∆ñÊW2Ê¶ˆñ‚Çu∆‚ríì∞¢–¢ñbÑ'&íÊó4'&íÜ&w2ÊVFóG2íó∞¢6ˆÁ7BFÉ’7G&ñÊrÖˆfó'7D˜vÊVEf«VRÜ&w2≈≤vfñ∆U˜FÇr¬wFÇr¬vfñ∆VÊ÷Ru“ó«¬rrì∞¢6ˆÁ7B6áVÊ∑3’µ”∞¢ñbáFÇí6áVÊ∑2ÁW6ÇáFÇì∞¢&w2ÊVFóG2Á6∆ñ6RÉ√RíÊf˜$V6ÇÜVFóC”Á∞¢ñbÇVFóB«¬GóVˆbVFóB”“vˆ&¶V7Brí&WGW&„∞¢6ˆÁ7B&Vf˜&S’ˆfó'7D˜vÊVEf«VRÜVFóB≈≤vˆ∆E˜7G&ñÊrr¬vˆ∆E˜7G"r¬vˆ∆Br¬v&Vf˜&Ru“ì∞¢6ˆÁ7BgFW#’ˆfó'7D˜vÊVEf«VRÜVFóB≈≤vÊWu˜7G&ñÊrr¬vÊWu˜7G"r¬vÊWrr¬vgFW"u“ì∞¢ñbÜ&Vf˜&R”◊VÊFVfñÊVBí6áVÊ∑2ÁW6ÇÖ˜&VfóÜVD6∆îFñfd∆ñÊW2Çr“r¬&Vf˜&Ríì∞¢ñbÜgFW"”◊VÊFVfñÊVBí6áVÊ∑2ÁW6ÇÖ˜&VfóÜVD6∆îFñfd∆ñÊW2Çr≤r¬gFW"íì∞¢“ì∞¢ñbÜ6áVÊ∑2Ê∆VÊwFÇí&WGW&‚ˆ6∆ó6∆ïFˆˆ≈6ÊóWBÜ6áVÊ∑2Ê¶ˆñ‚Çu∆‚ríì∞¢–¢&WGW&‚rs∞ß–†¶gVÊ7Fñˆ‚ˆ6∆ïFˆˆƒ6&E6ÊóWBá&W7V«E6ÊóWB¬F6Ö6ÊóWBó∞¢ñbÖˆ6∆î∆ˆˆ∑4∆ñ∂UF6ÑFñfbá&W7V«E6ÊóWBíí&WGW&‚&W7V«E6ÊóWC∞¢ñbÇF6Ö6ÊóWBí&WGW&‚&W7V«E6ÊóWB«¬rs∞¢6ˆÁ7B&W7V«C’7G&ñÊrá&W7V«E6ÊóWG«¬rríÁG&ñ“Çì∞¢ñbÇ&W7V«Bí&WGW&‚F6Ö6ÊóWC∞¢6ˆÁ7BvVÊW&ñ3“ı‚á7V66W77∆ˆ∑∆FˆÊW∆FˆÊU¬Á∆WÜóB6ˆFS¢íBˆíÁFW7Bá&W7V«Bì∞¢ñbÜvVÊW&ñ2í&WGW&‚F6Ö6ÊóWC∞¢&WGW&‚G∑&W7V«E6ÊóWG’∆Â∆‚G∑F6Ö6ÊóWG÷∞ß–†¶gVÊ7Fñˆ‚ˆ6∆ïFˆˆƒ6&DÜ4Fñfe6ÊóWBá&W7V«E6ÊóWB¬F6Ö6ÊóWBó∞¢&WGW&‚F6Ö6ÊóWB«¬ˆ6∆î∆ˆˆ∑4∆ñ∂UF6ÑFñfbá&W7V«E6ÊóWBì∞ß–†¶gVÊ7Fñˆ‚ˆ76ó7FÁEFˆˆƒÊ6Ü˜$ñGÑf˜$÷W76vRÜ÷W76vW2¬&tñGÇó∞¢6ˆÁ7B∆ó7C‘'&íÊó4'&íÜ÷W76vW2ìˆ÷W76vW3•µ”∞¢6ˆÁ7B7W'&VÁC÷∆ó7E∑&tñGÖ”∞¢ñbÖˆ76ó7FÁD÷W76vTÜ5fó6ñ&∆T6ˆÁFVÁBÜ7W'&VÁBíí&WGW&‚&tñGÉ∞¢ñbÖˆ76ó7FÁE&V6ˆÊñÊuñ∆ˆEFWáBÜ7W'&VÁBíí&WGW&‚&tñGÉ∞¢f˜"Ü∆WBñGÉ◊&tñGÇ”∂ñGÉ„”∂ñGÇ““ó∞¢ñbÖˆ76ó7FÁD÷W76vTÜ5fó6ñ&∆T6ˆÁFVÁBÜ∆ó7E∂ñGÖ“íí&WGW&‚ñGÉ∞¢–¢&WGW&‚&tñGÉ∞ß–¶gVÊ7Fñˆ‚˜Fˆˆƒ&w56Ê6Ü˜BÜ&w2¬∆ñ÷óBó∞¢ñbÇ&w7««GóVˆb&w2”“vˆ&¶V7Bw«ƒ'&íÊó4'&íÜ&w2íí&WGW&‚∑”∞¢6ˆÁ7B÷É‘÷FÇÊ÷ÇÉƒÁV÷&W"Ü∆ñ÷óBó«√bì∞¢6ˆÁ7B&ñ˜&óGì’∞¢wVW'ír¬w6V&6Ö˜VW'ír¬w6V&6ÖVW'ír¬wGFW&‚r¬wr¬v∂Wóv˜&Br¬v∂Wóv˜&G2r¬wFW&“r¿¢wW&¬r¬wW&ír¬v6ˆ÷÷ÊBr¬v6÷Br¬wFÇr¬vfñ∆Rr¬vfñ∆U˜FÇr¬vfñ∆VÊ÷Rr¬vfñ∆Uˆv∆ˆ"r¿¢vv∆ˆ"r¬vˆfg6WBr¬v∆ñ÷óBr¿¢”∞¢ÚÚ6ˆÁFVÁBÚFñfb◊&V6ˆÁ7G'V7Fñˆ‚∂Wó2◊W7BÊ˜B&R6VBFÚFÜR6Ü˜'@¢ÚÚñÊ6ñFVÁF¬÷&r∆ñ÷óB¬˜"∆ˆÊr6ˆ÷÷ÊG2˜Fá2vWB7WBÊB&V6˜fW'í◊&V'Vñ«@¢ÚÚFñfg2Ü'Vñ«Bg&ˆ“ˆ∆E˜7G&ñÊrˆÊWu˜7G&ñÊr˜F6Çí'&V≤Ç3Cì#Çí‚÷ó'&˜'2FÜP¢ÚÚ&6∂VÊBıDÙÙ≈Ù$uÙ4ÙÂDTÂEÙ¥Uï2ÚıDÙÙ≈Ù$uÙ4ÙÂDTÂEÙ4‡¢6ˆÁ7B6ˆÁFVÁD∂Wó3÷ÊWr6WBÖ≤v6ˆ÷÷ÊBr¬v6÷Br¬w67&óBr¬v6ˆFRr¬wF6Çr¬vFñfbr¬vˆ∆E˜7G&ñÊrr¬vÊWu˜7G&ñÊrr¬v6ˆÁFVÁBr¬wFÇr¬vfñ∆U˜FÇu“ì∞¢6ˆÁ7B4ÙÂDTÂEÙ4”C∞¢6ˆÁ7B∂Wó3’∞¢‚‚Á&ñ˜&óGíÊfñ«FW"Ü≥”‰ˆ&¶V7BÁ&˜F˜GóRÊÜ4˜vÂ&˜W'GíÊ6∆¬Ü&w2∆≤íí¿¢‚‚‰ˆ&¶V7BÊ∂Wó2Ü&w2íÊfñ«FW"Ü≥”‚&ñ˜&óGíÊñÊ6«VFW2Ü≤íí¿¢“Á6∆ñ6RÉ∆÷Çì∞¢6ˆÁ7B˜WC◊∑”∞¢∂Wó2Êf˜$V6ÇÜ≥”Á∞¢6ˆÁ7Bc’7G&ñÊrÜ&w5∂µ“ì∞¢6ˆÁ7B6÷6ˆÁFVÁD∂Wó2ÊÜ2Ö7G&ñÊrÜ≤íÁFÙ∆˜vW$66RÇíìÙ4ÙÂDTÂEÙ4£#∞¢∆WBf√◊bÁ6∆ñ6RÉ∆6í≤ábÊ∆VÊwFÉÊ6Úr‚‚‚s¢rrì∞¢ÚÚÊ˜rFÜB6ˆÁFVÁB&w2&R&WFñÊVBWFÚC6Ü'2Ç3Cì#Çí¬6V7&WBˆ‡¢ÚÚÊˆ‚÷fó'7B∆ñÊRÚ7B6Ü"#v˜V∆B˜FÜW'vó6R&V6ÇFÜR&w2&∆ˆ6≤¿¢ÚÚFÜRgV∆¬F"¬ÊB6∆ó&ˆ&B6˜íVÁ&VF7FVB‚&VF7BBFÜR6Ê6Ü˜B6¢ÚÚWfW'íF˜vÁ7G&V“&VÊFW&W"&V6VófW2«&VGí÷÷6∂VB&w2Ç3Cì#ÇvFRí‡¢ñbáGóVˆb˜&VF7EFˆˆ≈F&vWD∆&V√””“vgVÊ7Fñˆ‚ró≤G'ó≤f√’˜&VF7EFˆˆ≈F&vWD∆&V¬áf¬ì≤÷6F6ÇÜRó∑“–¢˜WE∂µ”◊f√∞¢“ì∞¢&WGW&‚˜WC∞ß–†¶gVÊ7Fñˆ‚ˆñD∆ñÊ∂VDÜó7F˜&ñ6ƒ÷W76vUFWáBÜ÷W76vRó∞¢ñbÇ÷W76vW««GóVˆb÷W76vR”“vˆ&¶V7Brí&WGW&‚rs∞¢6ˆÁ7B6ˆÁFVÁC÷÷W76vRÊ6ˆÁFVÁC∞¢ñbáGóVˆb6ˆÁFVÁC””“w7G&ñÊrrí&WGW&‚6ˆÁFVÁC∞¢ñbÇ'&íÊó4'&íÜ6ˆÁFVÁBíí&WGW&‚rs∞¢&WGW&‚6ˆÁFVÁBÊfñ«FW"á'C”Á'BbgGóVˆb'C””“vˆ&¶V7Brbg'BÁGóS””“wFWáBríÊ÷á'C”Á∞¢ñbÇ'G««GóVˆb'B”“vˆ&¶V7Brí&WGW&‚rs∞¢&WGW&‚7G&ñÊrá'BÁFWáG««'BÊ6ˆÁFVÁG«¬rrì∞¢“íÊ¶ˆñ‚Çu∆‚rì∞ß–†¶gVÊ7Fñˆ‚ˆñD∆ñÊ∂VDÜó7F˜&ñ6ƒ÷W76vTÜ5fó6ñ&∆UFWáBÜ÷W76vRó∞¢&WGW&‚ˆñD∆ñÊ∂VDÜó7F˜&ñ6ƒ÷W76vUFWáBÜ÷W76vRíÁG&ñ“Çí”“rs∞ß–†¶gVÊ7Fñˆ‚ˆñD∆ñÊ∂VDÜó7F˜&ñ6ƒ÷W76vU&VbÜ÷W76vR¬&tñGÇó∞¢ñbÜ÷W76vRbgGóVˆb÷W76vS””“vˆ&¶V7Bró∞¢f˜"Ü6ˆÁ7B∂Wíˆb≤v÷W76vUˆñBr¬vñBr¬v∆ˆ6≈ˆñBu“ó∞¢6ˆÁ7Bf«VS÷÷W76vU∂∂Wï”∞¢ñbáGóVˆbf«VS””“w7G&ñÊrrbgf«VRÁG&ñ“Çíí&WGW&‚f«VRÁG&ñ“Çì∞¢ñbáGóVˆbf«VS””“vÁV÷&W"rbdÁV÷&W"Êó4fñÊóFRáf«VRíí&WGW&‚7G&ñÊráf«VRì∞¢–¢–¢&WGW&‚&uˆñGÉ¢G∑&tñGá÷∞ß–†¶gVÊ7Fñˆ‚ˆñD∆ñÊ∂VDÜó7F˜&ñ6≈Fˆˆƒ&wV÷VÁG2áFˆˆƒ6∆¬ó∞¢ñbÇFˆˆƒ6∆«««GóVˆbFˆˆƒ6∆¬”“vˆ&¶V7Brí&WGW&‚ÁV∆√∞¢6ˆÁ7Bf„◊Fˆˆƒ6∆¬ÊgVÊ7Fñˆ„∞¢ñbÇfÁ««GóVˆbf‚”“vˆ&¶V7Bw«ƒ'&íÊó4'&íÜf‚íí&WGW&‚ÁV∆√∞¢6ˆÁ7B&s÷f‚Ê&wV÷VÁG3∞¢ñbá&s””◊VÊFVfñÊVG««&s””÷ÁV∆«««&s””“rrí&WGW&‚ÁV∆√∞¢ñbá&rbgGóVˆb&s””“vˆ&¶V7Brbb'&íÊó4'&íá&ríí&WGW&‚&s∞¢ñbáGóVˆb&r”“w7G&ñÊrrí&WGW&‚ÁV∆√∞¢G'ó∞¢6ˆÁ7B'6VC‘•4Ù‚Á'6Rá&rì∞¢&WGW&‚'6VBbgGóVˆb'6VC””“vˆ&¶V7Brbb'&íÊó4'&íá'6VBì˜'6VC¶ÁV∆√∞¢÷6F6ÇÜRó∞¢&WGW&‚ÁV∆√∞¢–ß–†¶gVÊ7Fñˆ‚ˆñD∆ñÊ∂VDÜó7F˜&ñ6≈Fˆˆ≈&W7V«E&rÜ÷W76vRó∞¢ñbÇ÷W76vW««GóVˆb÷W76vR”“vˆ&¶V7Brí&WGW&‚ÁV∆√∞¢6ˆÁ7B6ˆÁFVÁC÷÷W76vRÊ6ˆÁFVÁC∞¢&WGW&‚GóVˆb6ˆÁFVÁC””“w7G&ñÊrsˆ6ˆÁFVÁC¶ÁV∆√∞ß–†¶gVÊ7Fñˆ‚ˆñD∆ñÊ∂VDÜó7F˜&ñ6≈&VF7E6ÊóWBáf«VRó∞¢∆WBFWáC’7G&ñÊráf«VW«¬rrì∞¢ñbÇFWáBí&WGW&‚rs∞¢ñbáGóVˆb˜&VF7EFˆˆ≈F&vWD∆&V√””“vgVÊ7Fñˆ‚ró∞¢G'ó∑FWáC’˜&VF7EFˆˆ≈F&vWD∆&V¬áFWáBì∑–¢6F6ÇÜRó∑–¢–¢&WGW&‚FWáC∞ß–†¶gVÊ7Fñˆ‚ˆñD∆ñÊ∂VDÜó7F˜&ñ6ƒÜ5fó6ñ&∆U6ñFV6"Ü÷W76vRó∞¢ñbÇ÷W76vW««GóVˆb÷W76vR”“vˆ&¶V7Brí&WGW&‚f«6S∞¢6ˆÁ7Bfó6ñ&∆T∂Wó3’≤vGF6Ü÷VÁG2r¬uˆGF6Ü÷VÁG2r¬u˜7FGW46&Br¬w7FGW5ˆ6&Br¬w7FGW46&Br¬v6&Br¬v6&G2r¬v'Fñf7Br¬v'Fñf7G2r¬vfñ∆W2r¬vñ÷vW2r¬v÷VFñu”∞¢f˜"Ü6ˆÁ7B∂Wíˆbfó6ñ&∆T∂Wó2ó∞¢ñbÇˆ&¶V7BÁ&˜F˜GóRÊÜ4˜vÂ&˜W'GíÊ6∆¬Ü÷W76vR∆∂Wííí6ˆÁFñÁVS∞¢6ˆÁ7Bf«VS÷÷W76vU∂∂Wï”∞¢ñbáf«VS””◊VÊFVfñÊVG««f«VS””÷ÁV∆«««f«VS””÷f«6Rí6ˆÁFñÁVS∞¢ñbÑ'&íÊó4'&íáf«VRíbgf«VRÊ∆VÊwFÉ”””í6ˆÁFñÁVS∞¢ñbáGóVˆbf«VS””“vˆ&¶V7Brbb'&íÊó4'&íáf«VRíbdˆ&¶V7BÊ∂Wó2áf«VRíÊ∆VÊwFÉ”””í6ˆÁFñÁVS∞¢&WGW&‚G'VS∞¢–¢&WGW&‚f«6S∞ß–†¢ÚÚ6∆ñ“∆Vv7í6WGF∆VB˜vÊW'6ÜóˆÊ«ívÜV‚FÜRG&Á67&óBóG6V∆b&˜fW2¢ÚÚ6ˆ◊∆WFR¬W6W"÷&˜VÊFVBFV6∆&Fñˆ‚˜&W7V«BˆfñÊ¬÷Á7vW"6Üñ‚‡¶gVÊ7Fñˆ‚ˆñD∆ñÊ∂VDÜó7F˜&ñ6≈GW&Â66VÊRÜ÷W76vW2¬GW&Â7F'B¬GW&‰VÊB¬˜FñˆÁ2ó∞¢6ˆÁ7B∆ó7C‘'&íÊó4'&íÜ÷W76vW2ìˆ÷W76vW3•µ”∞¢6ˆÁ7B7F'C‘÷FÇÊ÷ÇÉƒÁV÷&W"áGW&Â7F'Bó«√ì∞¢6ˆÁ7BVÊC‘÷FÇÊ÷ñ‚Ü∆ó7BÊ∆VÊwFÇƒ÷FÇÊ÷Çá7F'BƒÁV÷&W"áGW&‰VÊBó«√íì∞¢6ˆÁ7B˜G3÷˜FñˆÁ2bgGóVˆb˜FñˆÁ3””“vˆ&¶V7Bsˆ˜FñˆÁ3ß∑”∞¢6ˆÁ7B6W76ñˆ‰ñC’7G&ñÊrÜ˜G2Á6W76ñˆ‰ñG«∆˜G2Á6W76ñˆÂˆñG«¬rríÁG&ñ“Çì∞¢6ˆÁ7Bì“áGóVˆbvñÊF˜r”“wVÊFVfñÊVBrì˜vñÊF˜r‰ÜW&÷W476ó7FÁEGW&‰Ê6Ü˜'3¶ÁV∆√∞¢ñbÇ6W76ñˆ‰ñG«¬ó««GóVˆbíÁ&ˆ¶V7D76ó7FÁEGW&‰Ê6Ü˜$Üó7F˜&ñ6≈G&Á67&óE66VÊR”“vgVÊ7Fñˆ‚rí&WGW&‚ÁV∆√∞†¢6ˆÁ7BFV6∆&FñˆÁ3’µ”∞¢6ˆÁ7BFV6∆&Fñˆ‰ñG3÷ÊWr6WBÇì∞¢6ˆÁ7BFV6∆&FñˆÂ&Vg3’µ”∞¢6ˆÁ7Bfó6ñ&∆T76ó7FÁDñÊFWÜW3’µ”∞¢6ˆÁ7B76ó7FÁDñÊFWÜW3’µ”∞¢6ˆÁ7B&W7V«G4'îñC÷ÊWr÷Çì∞¢f˜"Ü∆WB&tñGÉ◊7F'C∑&tñGÉ∆VÊC∑&tñGÇ≤≤ó∞¢6ˆÁ7B÷W76vS÷∆ó7E∑&tñGÖ”∞¢ñbÇ÷W76vW««GóVˆb÷W76vR”“vˆ&¶V7Brí6ˆÁFñÁVS∞¢6ˆÁ7B&ˆ∆S÷÷W76vRÁ&ˆ∆S∞¢ñbá&ˆ∆S””“wW6W"rbg&tñGÉ””◊7F'Bí6ˆÁFñÁVS∞¢ñbÜ÷W76vRÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊRí&WGW&‚ÁV∆√∞¢ñbá&ˆ∆S””“v76ó7FÁBró∞¢76ó7FÁDñÊFWÜW2ÁW6Çá&tñGÇì∞¢6ˆÁ7BÜ5fó6ñ&∆UFWáC’ˆñD∆ñÊ∂VDÜó7F˜&ñ6ƒ÷W76vTÜ5fó6ñ&∆UFWáBÜ÷W76vRì∞¢6ˆÁ7B&V6ˆÊñÊuFWáC’ˆ76ó7FÁE&V6ˆÊñÊuñ∆ˆEFWáBÜ÷W76vRì∞¢ñbÜÜ5fó6ñ&∆UFWáBífó6ñ&∆T76ó7FÁDñÊFWÜW2ÁW6Çá&tñGÇì∞¢ñbá&V6ˆÊñÊuFWáBí&WGW&‚ÁV∆√∞¢ñbÖˆñD∆ñÊ∂VDÜó7F˜&ñ6ƒÜ5fó6ñ&∆U6ñFV6"Ü÷W76vRíí&WGW&‚ÁV∆√∞¢ñbÑ'&íÊó4'&íÜ÷W76vRÂ˜'Fñ≈˜Fˆˆ≈ˆ6∆«2íbf÷W76vRÂ˜'Fñ≈˜Fˆˆ≈ˆ6∆«2Ê∆VÊwFÇí&WGW&‚ÁV∆√∞¢ñbÑ'&íÊó4'&íÜ÷W76vRÊ6ˆÁFVÁBíbf÷W76vRÊ6ˆÁFVÁBÁ6ˆ÷Rá'C”Á'BbgGóVˆb'C””“vˆ&¶V7Brbg'BÁGóS””“wFˆˆ≈˜W6Rríí&WGW&‚ÁV∆√∞¢6ˆÁ7BFˆˆƒ6∆«3‘'&íÊó4'&íÜ÷W76vRÁFˆˆ≈ˆ6∆«2ìˆ÷W76vRÁFˆˆ≈ˆ6∆«3•µ”∞¢ñbáFˆˆƒ6∆«2Ê∆VÊwFÇbfÜ5fó6ñ&∆UFWáBí&WGW&‚ÁV∆√∞¢ñbÇFˆˆƒ6∆«2Ê∆VÊwFÇó∞¢ñbÜÜ5fó6ñ&∆UFWáBí6ˆÁFñÁVS∞¢&WGW&‚ÁV∆√∞¢–¢ñbÜ÷W76vRÊ6ˆÁFVÁB”◊VÊFVfñÊVBbf÷W76vRÊ6ˆÁFVÁB”÷ÁV∆¬bf÷W76vRÊ6ˆÁFVÁB”“rrí&WGW&‚ÁV∆√∞¢6ˆÁ7B÷W76vU&Vc’ˆñD∆ñÊ∂VDÜó7F˜&ñ6ƒ÷W76vU&VbÜ÷W76vR«&tñGÇì∞¢ñbÇFV6∆&FñˆÂ&Vg2ÊñÊ6«VFW2Ü÷W76vU&VbííFV6∆&FñˆÂ&Vg2ÁW6ÇÜ÷W76vU&Vbì∞¢f˜"Ü6ˆÁ7BFˆˆƒ6∆¬ˆbFˆˆƒ6∆«2ó∞¢6ˆÁ7B6∆ƒñC’7G&ñÊráFˆˆƒ6∆¬bgFˆˆƒ6∆¬ÊñG«¬rríÁG&ñ“Çì∞¢6ˆÁ7Bf„◊Fˆˆƒ6∆¬bgFˆˆƒ6∆¬ÊgVÊ7Fñˆ„∞¢6ˆÁ7BÊ÷S’7G&ñÊrÜf‚bff‚ÊÊ÷W«¬rríÁG&ñ“Çì∞¢6ˆÁ7B&w3’ˆñD∆ñÊ∂VDÜó7F˜&ñ6≈Fˆˆƒ&wV÷VÁG2áFˆˆƒ6∆¬ì∞¢ñbÇ6∆ƒñG«¬Ê÷W«∆&w3””÷ÁV∆««∆FV6∆&Fñˆ‰ñG2ÊÜ2Ü6∆ƒñBíí&WGW&‚ÁV∆√∞¢FV6∆&Fñˆ‰ñG2ÊFBÜ6∆ƒñBì∞¢FV6∆&FñˆÁ2ÁW6Çá∂6∆ƒñB∆Ê÷R∆&w2«&tñGÇ∆÷W76vU&Vg“ì∞¢–¢6ˆÁFñÁVS∞¢–¢ñbá&ˆ∆R”“wFˆˆ¬rí&WGW&‚ÁV∆√∞¢6ˆÁ7B6∆ƒñC’7G&ñÊrÜ÷W76vRÁFˆˆ≈ˆ6∆≈ˆñG«¬rríÁG&ñ“Çì∞¢ñbÇ6∆ƒñG«¬FV6∆&Fñˆ‰ñG2ÊÜ2Ü6∆ƒñBíí&WGW&‚ÁV∆√∞¢6ˆÁ7B÷F6ÜW3◊&W7V«G4'îñBÊvWBÜ6∆ƒñBó«≈µ”∞¢÷F6ÜW2ÁW6Çá∂÷W76vR«&tñGá“ì∞¢&W7V«G4'îñBÁ6WBÜ6∆ƒñB∆÷F6ÜW2ì∞¢–†¢ñbÇFV6∆&FñˆÁ2Ê∆VÊwFá««fó6ñ&∆T76ó7FÁDñÊFWÜW2Ê∆VÊwFÇ””í&WGW&‚ÁV∆√∞¢6ˆÁ7B˜vÊW$ñÊFWÉ◊fó6ñ&∆T76ó7FÁDñÊFWÜW5≥”∞¢ñbÜ˜vÊW$ñÊFWÇ”÷76ó7FÁDñÊFWÜW5∂76ó7FÁDñÊFWÜW2Ê∆VÊwFÇ”“í&WGW&‚ÁV∆√∞¢6ˆÁ7B˜vÊW#÷∆ó7E∂˜vÊW$ñÊFWÖ”∞¢ñbÑ'&íÊó4'&íÜ˜vÊW"ÁFˆˆ≈ˆ6∆«2íbf˜vÊW"ÁFˆˆ≈ˆ6∆«2Ê∆VÊwFÇí&WGW&‚ÁV∆√∞¢6ˆÁ7B˜vÊW%&Vc’ˆñD∆ñÊ∂VDÜó7F˜&ñ6ƒ÷W76vU&VbÜ˜vÊW"∆˜vÊW$ñÊFWÇì∞¢f˜"Ü6ˆÁ7BFV6∆&Fñˆ‚ˆbFV6∆&FñˆÁ2ó∞¢6ˆÁ7B÷F6ÜW3◊&W7V«G4'îñBÊvWBÜFV6∆&Fñˆ‚Ê6∆ƒñBó«≈µ”∞¢ñbÜ÷F6ÜW2Ê∆VÊwFÇ””«∆÷F6ÜW5≥“Á&tñGÉ√÷FV6∆&Fñˆ‚Á&tñGá«∆÷F6ÜW5≥“Á&tñGÉ„÷˜vÊW$ñÊFWÇí&WGW&‚ÁV∆√∞¢–¢ñbá&W7V«G4'îñBÁ6ó¶R”÷FV6∆&FñˆÁ2Ê∆VÊwFÇí&WGW&‚ÁV∆√∞†¢6ˆÁ7B6˜W&6U&Vg3÷FV6∆&FñˆÂ&Vg2Ê6ˆÊ6BÜ˜vÊW%&VbíÊfñ«FW"Çáf«VR∆ñÊFWÇ∆'&íì”Ê'&íÊñÊFWÑˆbáf«VRì””÷ñÊFWÇì∞¢6ˆÁ7BGW&‰ñC’≤vÜó7F˜&ñ6¬r«6W76ñˆ‰ñB∆FV6∆&FñˆÂ&Vg5≥“∆˜vÊW%&Ve“Ê¶ˆñ‚Çs¢rì∞¢6ˆÁ7B7FófóGîWfVÁG3’µ”∞¢f˜"Ü∆WBñÊFWÉ”∂ñÊFWÉ∆FV6∆&FñˆÁ2Ê∆VÊwFÉ∂ñÊFWÇ≤≤ó∞¢6ˆÁ7BFV6∆&Fñˆ„÷FV6∆&FñˆÁ5∂ñÊFWÖ”∞¢6ˆÁ7B&W7V«DVÁG'ì◊&W7V«G4'îñBÊvWBÜFV6∆&Fñˆ‚Ê6∆ƒñBï≥”∞¢6ˆÁ7B&w3’˜Fˆˆƒ&w56Ê6Ü˜BÜFV6∆&Fñˆ‚Ê&w2ì∞¢6ˆÁ7B&W7V«E&s’ˆñD∆ñÊ∂VDÜó7F˜&ñ6≈Fˆˆ≈&W7V«E&rá&W7V«DVÁG'íÊ÷W76vRì∞¢ñbá&W7V«E&s””÷ÁV∆¬í&WGW&‚ÁV∆√∞¢6ˆÁ7B&W7V«E6ÊóWC’ˆñD∆ñÊ∂VDÜó7F˜&ñ6≈&VF7E6ÊóWBÖˆ6∆ïFˆˆ≈&W7V«E6ÊóWBá&W7V«E&ríì∞¢6ˆÁ7BF6Ö6ÊóWC’ˆ6∆ïF6Ö6ÊóWDg&ˆ‘&w2ÜFV6∆&Fñˆ‚ÊÊ÷R∆&w2ì∞¢6ˆÁ7Bó4Fñfc’ˆ6∆ïFˆˆƒ6&DÜ4Fñfe6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBì∞¢6ˆÁ7B6ÊóWC’ˆñD∆ñÊ∂VDÜó7F˜&ñ6≈&VF7E6ÊóWBÖˆ6∆ïFˆˆƒ6&E6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBíì∞¢6ˆÁ7B7FGW3’7G&ñÊrá&W7V«DVÁG'íÊ÷W76vRÁ7FGW7«¬rríÁG&ñ“ÇíÁFÙ∆˜vW$66RÇì∞¢6ˆÁ7Bó4W'&˜#◊&W7V«DVÁG'íÊ÷W76vRÊó5ˆW'&˜#””◊G'VW««7FGW3””“vW'&˜"w««7FGW3””“vfñ∆VBw««7FGW3””“vfñ«W&Rs∞¢7FófóGîWfVÁG2ÁW6Çá∞¢6˜W&6U˜GóS¢wFˆˆ≈ˆ6ˆ◊∆WFRr¿¢6W¶ñÊFWÇ≥¿¢∆ˆ6≈ˆñC¶Üó7F˜&ñ6√¢G∂FV6∆&Fñˆ‚Ê÷W76vU&Vg”ßFˆˆ√¢G∂FV6∆&Fñˆ‚Ê6∆ƒñG÷¿¢ñ∆ˆCß∞¢ñC¶FV6∆&Fñˆ‚Ê6∆ƒñB¿¢FñC¶FV6∆&Fñˆ‚Ê6∆ƒñB¿¢Fˆˆ≈ˆ6∆≈ˆñC¶FV6∆&Fñˆ‚Ê6∆ƒñB¿¢Ê÷S¶FV6∆&Fñˆ‚ÊÊ÷R¿¢&w2¿¢6ˆ÷÷ÊC•7G&ñÊrÜ&w2Ê6ˆ÷÷ÊG«∆&w2Ê6÷G«¬rrí¿¢6ÊóWB¿¢FˆÊSßG'VR¿¢ó5ˆW'&˜#¶ó4W'&˜"¿¢ó5ˆFñfc¶ó4Fñfb¿¢76ó7FÁEˆ◊6uˆñGÉ¶FV6∆&Fñˆ‚Á&tñGÇ¿¢“¿¢“ì∞¢–¢∆WB66VÊS∞¢G'ó∞¢66VÊS÷íÁ&ˆ¶V7D76ó7FÁEGW&‰Ê6Ü˜$Üó7F˜&ñ6≈G&Á67&óE66VÊRá∞¢6W76ñˆÂˆñCß6W76ñˆ‰ñB¿¢GW&ÂˆñCßGW&‰ñB¿¢∆ˆ6≈ˆñC¶˜vÊW%&Vb¿¢6˜W&6Uˆ÷W76vU˜&Vg3ß6˜W&6U&Vg2¿¢7FófóGïˆWfVÁG3¶7FófóGîWfVÁG2¿¢6WGF∆VEˆ÷W76vSß∑&ˆ∆S¢v76ó7FÁBr∆ñC¶˜vÊW%&Vb∆6ˆÁFVÁC•ˆñD∆ñÊ∂VDÜó7F˜&ñ6ƒ÷W76vUFWáBÜ˜vÊW"ó“¿¢“«∂÷ˆFS¶˜G2Ê÷ˆFW«¬v6ˆ◊7E˜v˜&∂∆ˆrw“ì∞¢÷6F6ÇÜRó∞¢&WGW&‚ÁV∆√∞¢–¢ñbÇ66VÊW««66VÊRÁfW'6ñˆ‚”“v7FófóGï˜66VÊU˜cw««66VÊRÊ7FófóGï˜&˜w2Ê∆VÊwFÇ”÷FV6∆&FñˆÁ2Ê∆VÊwFÇí&WGW&‚ÁV∆√∞¢&WGW&‚∂˜vÊW$ñÊFWÇ«66VÊW”∞ß–†¶gVÊ7Fñˆ‚ˆáñG&FTñD∆ñÊ∂VDÜó7F˜&ñ6≈Fˆˆ≈66VÊW2Ü÷W76vW2¬˜FñˆÁ2ó∞¢6ˆÁ7B∆ó7C‘'&íÊó4'&íÜ÷W76vW2ìˆ÷W76vW3•µ”∞¢∆WBGW&Â7F'C“”∞¢∆WBáñG&FVC”∞¢6ˆÁ7BáñG&FUGW&„“áGW&‰VÊBì”Á∞¢ñbáGW&Â7F'C√««GW&‰VÊC√◊GW&Â7F'B≥í&WGW&„∞¢∆WBáñG&FVEGW&„∞¢G'ó∂áñG&FVEGW&„’ˆñD∆ñÊ∂VDÜó7F˜&ñ6≈GW&Â66VÊRÜ∆ó7B«GW&Â7F'B«GW&‰VÊB∆˜FñˆÁ2ì∑–¢6F6ÇÜRó∑&WGW&„∑–¢ñbÇáñG&FVEGW&‚í&WGW&„∞¢6ˆÁ7B˜vÊW#÷∆ó7E∂áñG&FVEGW&‚Ê˜vÊW$ñÊFWÖ”∞¢G'ó∂˜vÊW"ÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊS÷áñG&FVEGW&‚Á66VÊS∑–¢6F6ÇÜRó∑&WGW&„∑–¢ñbÜ˜vÊW"ÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊS””÷áñG&FVEGW&‚Á66VÊRíáñG&FVB≥”∞¢”∞¢f˜"Ü∆WB&tñGÉ”∑&tñGÉ∆∆ó7BÊ∆VÊwFÉ∑&tñGÇ≤≤ó∞¢6ˆÁ7B÷W76vS÷∆ó7E∑&tñGÖ”∞¢ñbÇ÷W76vW«∆÷W76vRÁ&ˆ∆R”“wW6W"rí6ˆÁFñÁVS∞¢áñG&FUGW&‚á&tñGÇì∞¢GW&Â7F'C◊&tñGÉ∞¢–¢áñG&FUGW&‚Ü∆ó7BÊ∆VÊwFÇì∞¢&WGW&‚áñG&FVC∞ß–†¶gVÊ7Fñˆ‚ˆ6GW&T÷W76vU67&ˆ∆≈6Ê6Ü˜BÇó∞¢6ˆÁ7BV√“BÇv÷W76vW2rì∞¢ñbÇV¬í&WGW&‚ÁV∆√∞¢6ˆÁ7B&˜GFˆ”‘÷FÇÊ÷ÇÉ∆V¬Á67&ˆ∆ƒÜVñváB÷V¬Á67&ˆ∆≈F˜÷V¬Ê6∆ñVÁDÜVñváBì∞¢6ˆÁ7B&VFW$vîg&ˆ‘&˜GFˆ”÷&˜GFˆ”„#SbbÄ¢ˆ÷W76vUW6W%VÁñÊÊVB«¿¢˜67&ˆ∆≈ñÊÊVC””÷f«6R«¿¢áGóVˆb˜&V6VÁD÷W76vU67&ˆ∆ƒñÁFVÁC””“vgVÊ7Fñˆ‚rbe˜&V6VÁD÷W76vU67&ˆ∆ƒñÁFVÁBÇíê¢ì∞¢&WGW&‚∞¢Ê6Ü˜#¢áGóVˆbˆ6GW&T÷W76vUfñWw˜'DÊ6Ü˜#””“vgVÊ7Fñˆ‚rìıˆ6GW&T÷W76vUfñWw˜'DÊ6Ü˜"Çì¶ÁV∆¬¿¢F˜¶V¬Á67&ˆ∆≈F˜¿¢&˜GFˆ“¿¢67&ˆ∆ƒÜVñváC¶V¬Á67&ˆ∆ƒÜVñváB¿¢ñÁWDvVÊW&Fñˆ„ßGóVˆbˆ÷W76vU67&ˆ∆ƒñÁWDvVÊW&Fñˆ„””“vÁV÷&W"rÚˆ÷W76vU67&ˆ∆ƒñÁWDvVÊW&Fñˆ‚¢¿¢ñÊÊVCß&VFW$vîg&ˆ‘&˜GFˆ”ˆf«6S•˜6Ü˜V∆Dfˆ∆∆˜t÷W76vW4ˆ‰Fˆ’&W∆6RÇí¿¢W6W%VÁñÊÊVCß&VFW$vîg&ˆ‘&˜GFˆ”˜G'VS•ˆ÷W76vUW6W%VÁñÊÊVB¿¢”∞ß–¶gVÊ7Fñˆ‚ˆ÷W76vU67&ˆ∆≈6Ê6Ü˜DñÁWD6ÜÊvVBá6Ê6Ü˜Bó∞¢ñbÇ6Ê6Ü˜Bí&WGW&‚f«6S∞¢6ˆÁ7B6GW&VC‘ÁV÷&W"á6Ê6Ü˜BÊñÁWDvVÊW&Fñˆ‚ì∞¢6ˆÁ7B7W'&VÁC◊GóVˆbˆ÷W76vU67&ˆ∆ƒñÁWDvVÊW&Fñˆ„””“vÁV÷&W"rÚˆ÷W76vU67&ˆ∆ƒñÁWDvVÊW&Fñˆ‚¢6GW&VC∞¢&WGW&‚ÁV÷&W"Êó4fñÊóFRÜ6GW&VBíbdÁV÷&W"Êó4fñÊóFRÜ7W'&VÁBíbf7W'&VÁB”÷6GW&VC∞ß–¶gVÊ7Fñˆ‚ˆ&ÊFˆ‰÷W76vU67&ˆ∆≈6Ê6Ü˜BÇó∞¢6ˆÁ7BV√“BÇv÷W76vW2rì∞¢ñbÇV¬ó∞¢ˆ÷W76vUW6W%VÁñÊÊVC◊G'VS∞¢˜67&ˆ∆≈ñÊÊVC÷f«6S∞¢ˆÊV$&˜GFˆ‘6˜VÁC”∞¢&WGW&„∞¢–¢ˆ∆7E67&ˆ∆≈F˜÷V¬Á67&ˆ∆≈F˜«√∞¢ˆ∆7D÷W76vT6∆ñVÁDÜVñváC÷V¬Ê6∆ñVÁDÜVñváG«√∞¢ÚÚvVÊW&Fñˆ‚÷ó6÷F6Ç&ÊFˆÁ2ˆÊ«íFÜR7F∆R6Ê6Ü˜Bw&óFR‚&V6ˆÊ6ñ∆P¢ÚÚ˜vÊW'6Üóg&ˆ“FÜR∆ófRfñWw˜'B6Ú&VFW"vÜÚ÷˜fVBF˜v‚FÚFÜRG'VP¢ÚÚ&˜GFˆ“ó2ñ÷÷VFñFV«í&R◊ñÊÊVBñÁ7FVBˆb&VñÊr7G&ÊFVB7Fñ6∑í◊VÁñÊÊVB‡¢6ˆÁ7B&˜GFˆ‘Fó7FÊ6S÷V¬Á67&ˆ∆ƒÜVñváB÷V¬Á67&ˆ∆≈F˜÷V¬Ê6∆ñVÁDÜVñváC∞¢ñbÜ&˜GFˆ‘Fó7FÊ6S√”Éó∞¢ˆ÷W76vUW6W%VÁñÊÊVC÷f«6S∞¢˜67&ˆ∆≈ñÊÊVC◊G'VS∞¢ˆÊV$&˜GFˆ‘6˜VÁC”#∞¢÷V«6W∞¢ˆ÷W76vUW6W%VÁñÊÊVC◊G'VS∞¢˜67&ˆ∆≈ñÊÊVC÷f«6S∞¢ˆÊV$&˜GFˆ‘6˜VÁC”∞¢–ß–¶gVÊ7Fñˆ‚˜&W7F˜&UñÊÊVD÷W76vU67&ˆ∆≈6Ê6Ü˜Bá6Ê6Ü˜Bó∞¢6ˆÁ7BV√“BÇv÷W76vW2rì∞¢ñbÇV««¬6Ê6Ü˜G««6Ê6Ü˜BÁñÊÊVB”◊G'VW««6Ê6Ü˜BÁW6W%VÁñÊÊVC””◊G'VRí&WGW&‚f«6S∞¢6ˆÁ7B÷ÖF˜‘÷FÇÊ÷ÇÉ∆V¬Á67&ˆ∆ƒÜVñváB÷V¬Ê6∆ñVÁDÜVñváBì∞¢6ˆÁ7B&˜GFˆ”‘ÁV÷&W"á6Ê6Ü˜BÊ&˜GFˆ“ì∞¢6ˆÁ7BF&vWC‘ÁV÷&W"Êó4fñÊóFRÜ&˜GFˆ“ìˆ÷ÖF˜‘÷FÇÊ÷ÇÉ∆&˜GFˆ“ì¶÷ÖF˜∞¢˜&ˆw&÷÷Fñ567&ˆ∆√◊G'VSµ˜&ˆw&÷÷Fñ567&ˆ∆≈6WDC◊W&f˜&÷Ê6RÊÊ˜rÇì∞¢V¬Á67&ˆ∆≈F˜‘÷FÇÊ÷ÇÉƒ÷FÇÊ÷ñ‚áF&vWB∆÷ÖF˜íì∞¢ÚÚ7ñÊ2ˆ∆7E67&ˆ∆≈F˜gFW"&ˆw&÷÷Fñ2&W7F˜&R6Ú7Fñ6∑í◊VÁñ‚FˆW2Ê˜Bf«6R◊G&ñvvW"Ç3s3í‡¢ˆ∆7E67&ˆ∆≈F˜÷V¬Á67&ˆ∆≈F˜µˆ∆7D÷W76vT6∆ñVÁDÜVñváC÷V¬Ê6∆ñVÁDÜVñváC∞¢ˆ÷W76vUW6W%VÁñÊÊVC÷f«6S∞¢˜67&ˆ∆≈ñÊÊVC◊G'VS∞¢ˆÊV$&˜GFˆ‘6˜VÁC”#∞¢ñbáGóVˆbˆFVfW$6∆V%&ˆw&÷÷Fñ567&ˆ∆√””“vgVÊ7Fñˆ‚ríˆFVfW$6∆V%&ˆw&÷÷Fñ567&ˆ∆¬Çì∞¢V«6R&WVW7DÊñ÷Fñˆ‰g&÷RÇÇì”Á≤6WEFñ÷V˜WBÇÇì”Á≤˜&ˆw&÷÷Fñ567&ˆ∆√÷f«6S≤“√ì≤“ì∞¢&WGW&‚G'VS∞ß–¶gVÊ7Fñˆ‚˜&W7F˜&T÷W76vU67&ˆ∆≈6Ê6Ü˜Bá6Ê6Ü˜Bó∞¢6ˆÁ7BV√“BÇv÷W76vW2rì∞¢ñbÇV««¬6Ê6Ü˜Bí&WGW&„∞¢6ˆÁ7B÷ÖF˜‘÷FÇÊ÷ÇÉ∆V¬Á67&ˆ∆ƒÜVñváB÷V¬Ê6∆ñVÁDÜVñváBì∞¢ÚÚñbFÜR&VFW"v2fˆ∆∆˜vñÊrFÜR∆ófRFñ¬¬&W6W'fRFÜRFñ¬◊&V∆FófR&˜GFˆ–¢ÚÚFó7FÊ6R‚FÚÊ˜B6V÷ÁFñ2÷Ê6Ü˜"FÚFÜRfó'7Bfó6ñ&∆R&˜s¢∆ófRv˜&∂∆ˆr¢ÚÚ7FófóGí&V'Vñ∆G26‚&V÷˜VÁB‚ˆ∆FW"F˜÷ˆb◊fñWw˜'BÊ6Ü˜"ÊBñÊ≤¢ÚÚñÊÊVB7G&V÷ñÊrG&Á67&óBWv&B‚6V÷ÁFñ2Ê6Ü˜'2&V÷ñ‚f˜"÷ÁV¿¢ÚÚVÁñÊÊVB&VFñÊr˜6óFñˆÁ2&V∆˜r‡¢ñbáGóVˆbˆ÷W76vU67&ˆ∆≈6Ê6Ü˜DñÁWD6ÜÊvVC””“vgVÊ7Fñˆ‚rbeˆ÷W76vU67&ˆ∆≈6Ê6Ü˜DñÁWD6ÜÊvVBá6Ê6Ü˜Bíó∞¢ñbáGóVˆbˆ&ÊFˆ‰÷W76vU67&ˆ∆≈6Ê6Ü˜C””“vgVÊ7Fñˆ‚ríˆ&ÊFˆ‰÷W76vU67&ˆ∆≈6Ê6Ü˜BÇì∞¢&WGW&„∞¢–¢ñbÖ˜&W7F˜&UñÊÊVD÷W76vU67&ˆ∆≈6Ê6Ü˜Bá6Ê6Ü˜Bíí&WGW&„∞¢∆WB&W7F˜&VEfñÊ6Ü˜#“á6Ê6Ü˜BÊÊ6Ü˜"bgGóVˆb˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜#””“vgVÊ7Fñˆ‚rê¢Ú˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜"á6Ê6Ü˜BÊÊ6Ü˜"√ê¢¢f«6S∞¢ñbÇ&W7F˜&VEfñÊ6Ü˜"bgGóVˆb˜&V÷˜VÁD÷W76vUfñWw˜'DÊ6Ü˜#””“vgVÊ7Fñˆ‚rbe˜&V÷˜VÁD÷W76vUfñWw˜'DÊ6Ü˜"á6Ê6Ü˜BÊÊ6Ü˜"íó∞¢&W7F˜&VEfñÊ6Ü˜#“áGóVˆb˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜#””“vgVÊ7Fñˆ‚rê¢Ú˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜"á6Ê6Ü˜BÊÊ6Ü˜"√ê¢¢f«6S∞¢–¢ñbÇ&W7F˜&VEfñÊ6Ü˜"ó∞¢˜&ˆw&÷÷Fñ567&ˆ∆√◊G'VSµ˜&ˆw&÷÷Fñ567&ˆ∆≈6WDC◊W&f˜&÷Ê6RÊÊ˜rÇì∞¢V¬Á67&ˆ∆≈F˜‘÷FÇÊ÷ÇÉƒ÷FÇÊ÷ñ‚ÑÁV÷&W"á6Ê6Ü˜BÁF˜ó«√∆÷ÖF˜íì∞¢–¢ÚÚ7ñÊ2ˆ∆7E67&ˆ∆≈F˜gFW"&ˆw&÷÷Fñ2&W7F˜&R6Ú7Fñ6∑í◊VÁñ‚FˆW2Ê˜Bf«6R◊G&ñvvW"Ç3s3í‡¢ˆ∆7E67&ˆ∆≈F˜÷V¬Á67&ˆ∆≈F˜µˆ∆7D÷W76vT6∆ñVÁDÜVñváC÷V¬Ê6∆ñVÁDÜVñváC∞¢ñbá6Ê6Ü˜BÁW6W%VÁñÊÊVC””◊G'VRó∞¢ˆ÷W76vUW6W%VÁñÊÊVC◊G'VS∞¢˜67&ˆ∆≈ñÊÊVC÷f«6S∞¢ˆÊV$&˜GFˆ‘6˜VÁC”∞¢÷V«6Rñbá6Ê6Ü˜BÁñÊÊVC””◊G'VRó∞¢ˆ÷W76vUW6W%VÁñÊÊVC÷f«6S∞¢˜67&ˆ∆≈ñÊÊVC◊G'VS∞¢ˆÊV$&˜GFˆ‘6˜VÁC”#∞¢÷V«6W∞¢6ˆÁ7B&˜GFˆ‘Fó7FÊ6S÷V¬Á67&ˆ∆ƒÜVñváB÷V¬Á67&ˆ∆≈F˜÷V¬Ê6∆ñVÁDÜVñváC∞¢ñbÜ&˜GFˆ‘Fó7FÊ6S„#Só∞¢ˆ÷W76vUW6W%VÁñÊÊVC◊G'VS∞¢˜67&ˆ∆≈ñÊÊVC÷f«6S∞¢ˆÊV$&˜GFˆ‘6˜VÁC”∞¢÷V«6RñbÜ&˜GFˆ‘Fó7FÊ6S√”#ó∞¢ˆ÷W76vUW6W%VÁñÊÊVC÷f«6S∞¢˜67&ˆ∆≈ñÊÊVC◊G'VS∞¢ˆÊV$&˜GFˆ‘6˜VÁC”#∞¢–¢–¢ñbÇ&W7F˜&VEfñÊ6Ü˜"ó∞¢ñbáGóVˆbˆFVfW$6∆V%&ˆw&÷÷Fñ567&ˆ∆√””“vgVÊ7Fñˆ‚ríˆFVfW$6∆V%&ˆw&÷÷Fñ567&ˆ∆¬Çì∞¢V«6R&WVW7DÊñ÷Fñˆ‰g&÷RÇÇì”Á≤6WEFñ÷V˜WBÇÇì”Á≤˜&ˆw&÷÷Fñ567&ˆ∆√÷f«6S≤“√ì≤“ì∞¢–ß–¢Ú¢†¢¢÷ˆ&ñ∆R67&ˆ∆¬÷¶Ê≤wV&C¢FV◊˜&&ñ«íFó6&∆R˜fW&f∆˜r÷Ê6Ü˜"6¢¢6á&ˆ÷óV“6ÊÊ˜B&R÷Ê6Ü˜"FÚFÜRF˜÷˜7B&˜rGW&ñÊrFÜRñÊÊW$ÖD‘√“rp¢¢vóR÷ÊB◊&V'Vñ∆Bv‚FÜR$b6∆∆&6≤&W7F˜&W2552FVfV«BgFW'v&B‡¢¢¢ÚÚ÷ˆ&ñ∆R67&ˆ∆¬ßV◊÷&6≤&ˆ˜BfóÇ‚ˆ‚F˜V6ÇFWfñ6W26÷W76vW2&W7G2@¢ÚÚ˜fW&f∆˜r÷Ê6Ü˜#¶WFÚ¬6ÚFÜR'&˜w6W"w2ÊFófR67&ˆ∆¬÷Ê6Ü˜&ñÊrVÊvñÊP¢ÚÚ&R÷6ˆ◊VÁ6FW267&ˆ∆≈F˜ñ‚FÜRƒîıUBÜ6RvÜVÊWfW"6ˆÁFVÁB&˜fRFÜP¢ÚÚfñWw˜'B6ÜÊvW2ÜVñváB(	Bv˜&∂∆ˆr∆óf^(i'6WGF∆VB6ˆ∆∆6R¬Fˆˆ¬÷6&BñÁ6W'G2¿¢ÚÚ÷VFñˆ∂FWÇ&Vf∆˜r¬fó'GV¬◊67&ˆ∆¬F˜B&V6ˆ◊WFR¬FÜR5E$T’ÙDÙ‰P¢ÚÚ◊V«Fí◊&VÊFW"6WVVÊ6R‚FÜB6ˆ◊VÁ6Fñˆ‚ÜVÁ2ñ‚FÜR'&˜w6W"w2∆ñ˜WB7FW¿¢ÚÚî‰DUT‰DTÂBˆbvÜñ6Çg&÷R˜W"•2w&˜FR67&ˆ∆≈F˜ñ‚¬6ÚW"◊w&óFR7W&W76ñˆ‡¢ÚÚÜ6ñÊv∆R◊$bwV&Bí6˜V∆BÊ˜B&V6ÇóC¢FÜR6ˆ∆∆6R˜&Vf∆˜r∆ÊG2g&÷R˜ ¢ÚÚGvÚ∆FW"¬gFW"FÜRwV&B«&VGí&V∆V6VB‚&V¬÷ˆ&ñ∆Rf∆ñváB◊&V6˜&FW"FF¢ÚÚÜ6GW&VBßV◊2vóFÇEF˜”Ú≥3SÚ≥sCÇÚ”C¬6∆¬7F6≤“$b6◊∆W"ˆÊ«í–¢ÚÚ‰Ú•2g&÷Rí6ˆÊfó&÷VBFÜR6ˆ◊VÁ6Fñˆ‚ó2FÜR'&˜w6W"VÊvñÊR¬Ê˜B˜W"67&ˆ∆¿¢ÚÚw&óFW2‡¢Ú¢ÚÚfóÉ¢DTdU"FÜR&W7F˜&R‰BG&6≤552Êñ÷FñˆÁ2‚V6Ç6∆¬&R÷&◊27W&W76ñˆ‡¢ÚÚÊB6Ê6V«2ÁíVÊFñÊr&V∆V6R¬6Ú'W'7Bˆb&VÊFW'2Ö5E$T’ÙDÙ‰Rfó&W0¢ÚÚ6WfW&¬&6≤◊FÚ÷&6≤í6Ü&W2Ù‰R7W&W76ñˆ‚vñÊF˜r‚FÜR&6RvñÊF˜ró2Gv¢ÚÚÊñ÷Fñˆ‚g&÷W2≤6WGF∆RFñ÷V˜WB¬vÜñ6Ç6˜fW'26áW&‚FÜBó2‰ıB550¢ÚÚÊñ÷Fñˆ‚áfó'GV¬F˜B&V6ˆ◊WFR¬ñ÷vR÷FV6ˆFR¬∂FWÇ÷V7W&Rí‚'WBFÜP¢ÚÚFˆ÷ñÊÁB6áW&‚ó2552÷Ç÷ÜVñváB6ˆ∆∆6RˆWáÊBÊñ÷FñˆÁ2ˆ‚v˜&∂∆ˆr&˜w2(	@¢ÚÚÊ7FófóGí÷&ˆGíÇ„3G2í¬ÁFˆˆ¬÷w&˜W÷&ˆGíÇ„72í¬ÁFˆˆ¬÷6&B÷FWFñ¬Ç„#g2í(	BvÜñ6Ä¢ÚÚ'V‚ƒÙ‰tU"FÜ‚fóÜVBvñÊF˜s≤fóÜVBvñÊF˜r∆ñgG2÷ñB÷Êñ÷Fñˆ‚ÊBFÜR&W7@¢ÚÚˆbFÜRÊñ÷Fñˆ‚7Fñ∆¬ßV◊2‚6ÚvR«6Ú&ñÊBG&Á6óFñˆÁ'V‚˜G&Á6óFñˆÊVÊBˆ‡¢ÚÚ6÷W76vW3¢‚Êñ÷Fñˆ‚7F'BÜˆ∆G27W&W76ñˆ‚Ü6Ê6V«2FÜRVÊFñÊr&V∆V6Rì∞¢ÚÚ‚Êñ÷Fñˆ‚VÊB66ÜVGV∆W26Ü˜'B6WGF∆RgFW"FÜRƒ5BˆÊR‚Ü&B÷6VB6Ú¢ÚÚ∆ˆ˜ñÊrG&Á6óFñˆ‚6‚wBñ‚˜fW&f∆˜r÷Ê6Ü˜#¶ÊˆÊRf˜&WfW"‚FW6∑F˜&W7G2@¢ÚÚÊˆÊRá&VFñ6FRf«6Rí(i"FÜRvÜˆ∆RwV&Bó2ÊÚ÷˜‡¶6ˆÁ7BÙ‘Ù$îƒUÙ‰4Ñı%Ù$4Uı4UEDƒUÙ’3”C∞¶6ˆÁ7BÙ‘Ù$îƒUÙ‰4Ñı%ıı5EıE$Â4ïDîÙÂÙ’3”ì∞¶6ˆÁ7BÙ‘Ù$îƒUÙ‰4Ñı%Ù‘ÖÙÑÙƒEÙ’3”#∞¶∆WBˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W#÷ÁV∆√∞¶∆WBˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñC”∞¶∆WBˆ÷ˆ&ñ∆TÊ6Ü˜%G&Á6óFñˆ‰∆ó7FVÊW$&˜VÊC÷f«6S∞¶∆WBˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W74&÷VDC”∞¢ÚÚñÊFWVÊFVÁBÜ&B÷6Fñ÷W"‚VÊ∆ñ∂RFÜR6WGF∆R˜$b&V∆V6RávÜñ6ÇFÜP¢ÚÚG&Á6óFñˆÁ'V‚ÜÊF∆W"4‰4T≈2FÚÜˆ∆B7&˜72‚Êñ÷Fñˆ‚í¬FÜó2ˆÊRó2‰UdU ¢ÚÚ6Ê6V∆∆VB'í&R÷&“˜"'íˆÂ'V‚(	BóBó2ˆÊ«íWfW"6∆V&VBvÜV‚7W&W76ñˆ‚ó0¢ÚÚ7GV∆«í∆ñgFVB¬ÊB&R÷&÷VBFÚg&W6ÇFVF∆ñÊRˆ‚V6ÇˆfóÑ÷ˆ&ñ∆U67&ˆ∆ƒ¶Ê∞¢ÚÚ6∆¬‚FÜó2wV&ÁFVW2˜fW&f∆˜r÷Ê6Ü˜"&WGW&Á2FÚFÜR÷ˆ&ñ∆R&W7FñÊrvWFÚp¢ÚÚWfV‚ñbUdU%íG&Á6óFñˆÊVÊB˜G&Á6óFñˆÊ6Ê6V¬ó2÷ó76VBÜÊñ÷Fñˆ‚ñÁFW''WFVB¿¢ÚÚV∆V÷VÁBFWF6ÜVB÷ñB◊G&Á6óFñˆ‚¬WF2‚í(	BFÜR3S33Ç6ˆÁG&7BFÜB÷ˆ&ñ∆R&W7G0¢ÚÚBvWFÚr◊W7BÜˆ∆BÊÚ÷GFW"vÜB‚ÑvFR÷6W'BFVfV7C¢FÜR&Wfñ˜W0¢ÚÚÙ‘Ù$îƒUÙ‰4Ñı%Ù‘ÖÙÑÙƒEÙ’2v2ˆÊ«íwV&B6∆W6RñÁ6ñFRˆÂ'V‚¬6Ú÷ó76V@¢ÚÚG&Á6óFñˆÊVÊBñÊÊVBvÊˆÊRrf˜&WfW"‚ê¶∆WBˆ÷ˆ&ñ∆TÊ6Ü˜$÷ÑÜˆ∆EFñ÷W#÷ÁV∆√∞¶gVÊ7Fñˆ‚ˆ∆ñgD÷ˆ&ñ∆TÊ6Ü˜%7W&W76ñˆ‚ÜV¬ó∞¢ñbÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W"ó≤6∆V%Fñ÷V˜WBÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W"ì≤ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W#÷ÁV∆√≤–¢ñbÖˆ÷ˆ&ñ∆TÊ6Ü˜$÷ÑÜˆ∆EFñ÷W"ó≤6∆V%Fñ÷V˜WBÖˆ÷ˆ&ñ∆TÊ6Ü˜$÷ÑÜˆ∆EFñ÷W"ì≤ˆ÷ˆ&ñ∆TÊ6Ü˜$÷ÑÜˆ∆EFñ÷W#÷ÁV∆√≤–¢ñbÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñBbgGóVˆb6Ê6VƒÊñ÷Fñˆ‰g&÷S””“vgVÊ7Fñˆ‚ró≤6Ê6VƒÊñ÷Fñˆ‰g&÷RÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñBì≤–¢ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñC”∞¢ÚÚˆÊ«í6∆V"FÜRñÊ∆ñÊRf«VRvR6WC≤6ˆÊ7W'&VÁBFÇ÷íÜfR∆VvóFñ÷FV«ê¢ÚÚ&R÷&÷VBóBÜ6ÜV6∂VBfñFÜRvÊˆÊRrwV&Bí‡¢ñbÜV¬bfV¬Á7Gñ∆RbfV¬Á7Gñ∆RÊ˜fW&f∆˜tÊ6Ü˜#””“vÊˆÊRríV¬Á7Gñ∆RÊ˜fW&f∆˜tÊ6Ü˜#“rs∞ß–¶gVÊ7Fñˆ‚ˆ&ñÊD÷ˆ&ñ∆TÊ6Ü˜%G&Á6óFñˆ‰WáFVÊFW"ÜV¬ó∞¢ñbÖˆ÷ˆ&ñ∆TÊ6Ü˜%G&Á6óFñˆ‰∆ó7FVÊW$&˜VÊG«¬V««¬V¬ÊFDWfVÁD∆ó7FVÊW"í&WGW&„∞¢ˆ÷ˆ&ñ∆TÊ6Ü˜%G&Á6óFñˆ‰∆ó7FVÊW$&˜VÊC◊G'VS∞¢ÚÚˆÊ«í7BvÜñ∆R7W&W76ñˆ‚ó27GV∆«í&÷VBÜñÊ∆ñÊRvÊˆÊRríÊBvóFÜñ‚FÜP¢ÚÚÜ&B6¬6ÚvRÊWfW"ñ‚˜fW&f∆˜r÷Ê6Ü˜#¶ÊˆÊRñÊFVfñÊóFV«í‡¢6ˆÁ7BˆÂ'V„“ÜRì”Á∞¢ñbÇW«∆RÁ&˜W'GîÊ÷R”“v÷Ç÷ÜVñváBrí&WGW&„∞¢ñbÜV¬Á7Gñ∆RÊ˜fW&f∆˜tÊ6Ü˜"”“vÊˆÊRrí&WGW&„∞¢ñbÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W74&÷VDBbbáW&f˜&÷Ê6RÊÊ˜rÇí’ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W74&÷VDBìÂÙ‘Ù$îƒUÙ‰4Ñı%Ù‘ÖÙÑÙƒEÙ’2í&WGW&„∞¢ÚÚ‚Êñ÷Fñˆ‚ó2'VÊÊñÊr(	B6Ê6V¬FÜRVÊFñÊr4UEDƒR&V∆V6R6ÚvR7Fê¢ÚÚ7W&W76VBVÁFñ¬óBVÊG2áG&Á6óFñˆÊVÊB&R◊66ÜVGV∆W2FÜR6WGF∆Rí‚FÜP¢ÚÚñÊFWVÊFVÁB÷Ç÷Üˆ∆BFñ÷W"ó2FV∆ñ&W&FV«í‰ıB6Ê6V∆∆VBÜW&R‡¢ñbÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W"ó≤6∆V%Fñ÷V˜WBÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W"ì≤ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W#÷ÁV∆√≤–¢ñbÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñBbgGóVˆb6Ê6VƒÊñ÷Fñˆ‰g&÷S””“vgVÊ7Fñˆ‚ró≤6Ê6VƒÊñ÷Fñˆ‰g&÷RÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñBì≤–¢ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñC”∞¢”∞¢6ˆÁ7Bˆ‰VÊC“ÜRì”Á∞¢ñbÇW«∆RÁ&˜W'GîÊ÷R”“v÷Ç÷ÜVñváBrí&WGW&„∞¢ñbÜV¬Á7Gñ∆RÊ˜fW&f∆˜tÊ6Ü˜"”“vÊˆÊRrí&WGW&„∞¢ÚÚFÜó2Êñ÷Fñˆ‚VÊFVC≤6WGF∆R6Ü˜'F«ígFW"ÜÊ˜FÜW"÷í7Fñ∆¬&R'VÊÊñÊr¿¢ÚÚñ‚vÜñ6Ç66RóG2˜v‚G&Á6óFñˆÁ'V‚«&VGí6Ê6V∆∆VBFÜó2Fñ÷W"í‡¢ñbÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W"ó≤6∆V%Fñ÷V˜WBÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W"ì≤–¢ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W#◊6WEFñ÷V˜WBÇÇì”Á∞¢ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W#÷ÁV∆√∞¢ˆ∆ñgD÷ˆ&ñ∆TÊ6Ü˜%7W&W76ñˆ‚ÜV¬ì∞¢“≈Ù‘Ù$îƒUÙ‰4Ñı%ıı5EıE$Â4ïDîÙÂÙ’2ì∞¢”∞¢V¬ÊFDWfVÁD∆ó7FVÊW"ÇwG&Á6óFñˆÁ'V‚r∆ˆÂ'V‚«∑76ófSßG'VW“ì∞¢V¬ÊFDWfVÁD∆ó7FVÊW"ÇwG&Á6óFñˆÁ7F'Br∆ˆÂ'V‚«∑76ófSßG'VW“ì∞¢V¬ÊFDWfVÁD∆ó7FVÊW"ÇwG&Á6óFñˆÊVÊBr∆ˆ‰VÊB«∑76ófSßG'VW“ì∞¢V¬ÊFDWfVÁD∆ó7FVÊW"ÇwG&Á6óFñˆÊ6Ê6V¬r∆ˆ‰VÊB«∑76ófSßG'VW“ì∞ß–ßvñÊF˜rÂˆfóÑ÷ˆ&ñ∆U67&ˆ∆ƒ¶Ê≥÷gVÊ7Fñˆ‚ˆfóÑ÷ˆ&ñ∆U67&ˆ∆ƒ¶Ê≤Çó∞¢6ˆÁ7BV√÷Fˆ7V÷VÁBÊvWDV∆V÷VÁD'îñBÇv÷W76vW2rì∞¢ñbÇV¬í&WGW&„∞¢ÚÚVÊvvRvÜV‚FÜR'&˜w6W"67&ˆ∆¬÷Ê6Ü˜"∆ñW"ó27FófRÜ÷ˆ&ñ∆RWFÚí¬ı"vÜV‡¢ÚÚtR&R«&VGíÜˆ∆FñÊr‚ñÊ∆ñÊR7W&W76ñˆ‚g&ˆ“&ñ˜"6∆¬ñ‚FÜR6÷P¢ÚÚ'W'7B‚FÜR&VFñ6FR&VG2FÜR4Ù’UDTBf«VR¬vÜñ6Ç˜W"˜v‚ñÊ∆ñÊP¢ÚÚ˜fW&f∆˜r÷Ê6Ü˜#¶ÊˆÊRf∆ó2FÚvÊˆÊRr(	B6Úˆ‚FÜR&ÊB‚‰ÁFÇ6∆¬ˆb¢ÚÚ5E$T’ÙDÙ‰R'W'7BFÜR&VFñ6FRv˜V∆B6íf«6RÊB6Ü˜'B÷6ó&7VóBFÜR&R÷&–¢ÚÚ&V∆˜r¬6ˆ∆∆6ñÊrFÜRvÜˆ∆R&6ˆÁ6V7WFófR&VÊFW'2WáFVÊBFÜRvñÊF˜r"&VÜfñ˜ ¢ÚÚFÚ6ñÊv∆Rfó'7B÷6∆¬vñÊF˜r‚G&VB‚ñÊ∆ñÊRvÊˆÊRrtR6WB27Fñ∆¬÷&÷V@¢ÚÚ6Ú&R÷&“7GV∆«í'VÁ2‚FW6∑F˜&W7G2B6ˆ◊WFVBvÊˆÊRrvóFÇT’EíñÊ∆ñÊR¿¢ÚÚ6Ú«&VGï7W&W76VFó2f«6RFÜW&RÊBFÜó27Fó2ÊÚ÷˜‚ÑvFR÷6W'@¢ÚÚFVfV7C¢&R÷&“v2FVB6ˆFRvóFÜ˜WBFÜó2‚ê¢6ˆÁ7B«&VGï7W&W76VC÷V¬Á7Gñ∆RÊ˜fW&f∆˜tÊ6Ü˜#””“vÊˆÊRs∞¢ñbÇ«&VGï7W&W76VBbbˆ'&˜w6W$˜fW&f∆˜tÊ6Ü˜$7FófRÜV¬íí&WGW&„∞¢V¬Á7Gñ∆RÊ˜fW&f∆˜tÊ6Ü˜#“vÊˆÊRs∞¢ˆ&ñÊD÷ˆ&ñ∆TÊ6Ü˜%G&Á6óFñˆ‰WáFVÊFW"ÜV¬ì∞¢ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W74&÷VDC◊W&f˜&÷Ê6RÊÊ˜rÇì∞¢ÚÚ&R÷&”¢6Ê6V¬ÁíVÊFñÊr&V∆V6R6Ú6ˆÁ6V7WFófR&VÊFW'2UÖDT‰B¬Ê˜B6Ü˜'FV‚¿¢ÚÚFÜR7W&W76ñˆ‚vñÊF˜ráFÜR5E$T’ÙDÙ‰R6WGF∆Rfó&W2&VÊFW$÷W76vW26WfW&¿¢ÚÚFñ÷W2&6≤◊FÚ÷&6≤¬«W2FVfW'&VB˜7E&ˆ6W72&Vf∆˜rí‡¢ñbÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W"ó≤6∆V%Fñ÷V˜WBÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W"ì≤ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W#÷ÁV∆√≤–¢ñbÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñBbgGóVˆb6Ê6VƒÊñ÷Fñˆ‰g&÷S””“vgVÊ7Fñˆ‚ró≤6Ê6VƒÊñ÷Fñˆ‰g&÷RÖˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñBì≤–¢ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñC”∞¢ÚÚñÊFWVÊFVÁBÜ&B6¢á&Rñ&“&V∆V6RFÜB‰ıDÑî‰r6Ê6V«2WÜ6WB‚7GV¿¢ÚÚ∆ñgB¬6Ú÷ó76VBG&Á6óFñˆÊVÊB6‚ÊWfW"ñ‚vÊˆÊRr7BFÜR6‡¢ñbÖˆ÷ˆ&ñ∆TÊ6Ü˜$÷ÑÜˆ∆EFñ÷W"ó≤6∆V%Fñ÷V˜WBÖˆ÷ˆ&ñ∆TÊ6Ü˜$÷ÑÜˆ∆EFñ÷W"ì≤–¢ˆ÷ˆ&ñ∆TÊ6Ü˜$÷ÑÜˆ∆EFñ÷W#◊6WEFñ÷V˜WBÇÇì”Á∞¢ˆ÷ˆ&ñ∆TÊ6Ü˜$÷ÑÜˆ∆EFñ÷W#÷ÁV∆√∞¢ˆ∆ñgD÷ˆ&ñ∆TÊ6Ü˜%7W&W76ñˆ‚ÜV¬ì∞¢“≈Ù‘Ù$îƒUÙ‰4Ñı%Ù‘ÖÙÑÙƒEÙ’2ì∞¢6ˆÁ7B&dÜ˜“Ü6"ì”Á≤ñbáGóVˆb&WVW7DÊñ÷Fñˆ‰g&÷S””“vgVÊ7Fñˆ‚rí&WGW&‚&WVW7DÊñ÷Fñˆ‰g&÷RÜ6"ì≤&WGW&‚6WEFñ÷V˜WBÜ6"√bì≤”∞¢ÚÚ&6RvñÊF˜s¢GvÚÊñ÷Fñˆ‚g&÷W2áñÁB≤˜7B◊&VÊFW"&Vf∆˜r6WGF∆RíDÑT‚¢ÚÚ6WGF∆RFñ÷V˜WB‚552÷Ç÷ÜVñváBÊñ÷FñˆÁ2&R6˜fW&VB'íFÜRG&Á6óFñˆÁ'V‚¢ÚÚG&Á6óFñˆÊVÊBWáFVÊFW"&˜fS≤FÜó2f∆ˆ˜"6˜fW'2Êˆ‚÷Êñ÷FVB6áW&‚‡¢ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñC◊&dÜ˜ÇÇì”Á∞¢ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&dñC◊&dÜ˜ÇÇì”Á∞¢ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W#◊6WEFñ÷V˜WBÇÇì”Á∞¢ˆ÷ˆ&ñ∆TÊ6Ü˜%7W&W75&V∆V6UFñ÷W#÷ÁV∆√∞¢ˆ∆ñgD÷ˆ&ñ∆TÊ6Ü˜%7W&W76ñˆ‚ÜV¬ì∞¢“≈Ù‘Ù$îƒUÙ‰4Ñı%Ù$4Uı4UEDƒUÙ’2ì∞¢“ì∞¢“ì∞ß”∞†¢ÚÚFW6∑F˜7F∆R◊6Ê6Ü˜B&W6ñGVRÜó77VR3Sc3rfˆ∆∆˜r◊Wí‚&V6ÜVBˆÊ«ívÜV‡¢ÚÚ˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜"«&VGí4Ù‰4TDTBÜÊ6Ü˜"&˜rVÁ&V6˜fW&&∆R'íóG0¢ÚÚW"◊FñW"∆ˆˆ∑WíÊBFÜRFW6∑F˜f∆∆&6≤v˜V∆B˜FÜW'vó6Rw&óFRFÜR%4Ù≈UDP¢ÚÚ6Ê6Ü˜BÁF˜(	BvÜñ6Çó27F∆RˆÊ6R&˜fR◊fñWw˜'B6ˆÁFVÁBw&Wr6ñÊ6R6GW&R¿¢ÚÚñÊ∂ñÊr7Fñ∆¬&VFW"&6∑v&B‚FÜR6˜'&V7BÜˆ∆Bó2FÜRw2˜v‚&V∆ñv‡¢ÚÚñFñˆ”¢6ÜñgBFÜR5U%$TÂB67&ˆ∆≈F˜'íÜ˜rf"FÜRÊ6Ü˜"&˜r÷˜fVB6ñÊ6R6GW&R¿¢ÚÚ67&ˆ∆≈F˜≥“Ü7W'&VÁDˆfg6WB“6GW&VDˆfg6WBñÜ÷ó'&˜'2˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜ ¢ÚÚVíÊß2ÊBˆ6ˆ◊VÁ6FU67&ˆ∆ƒf˜$÷V7W&V÷VÁDFV«Fí‚‰ıB6Ê6Ü˜BÁF˜≤FV«F¢¢ÚÚ&˜rw2ˆfg6WBó267&ˆ∆¬◊&V∆FófRá&V7BÁF˜“6ˆÁFñÊW%&V7BÁF˜“&˜t6ˆÁFVÁE˜2–¢ÚÚ67&ˆ∆≈F˜í¬6ÚˆÊ«íFV«F∆ñVBFÚFÜRƒïdR67&ˆ∆≈F˜Üˆ∆G2FÜR&˜rW@¢ÚÚ&Vv&F∆W72ˆbvÜW&R67&ˆ∆≈F˜v26'&ñVBFÚ‚&WGW&Á2FÜR&V∆ñv‚FV«FÜ÷í&P¢ÚÚí¬˜"ÁV∆¬vÜV‚FÜRÊ6Ü˜"&˜r6‚wB&R÷V7W&VBVÊFW"FÜR4‘RW"◊FñW"wV&@¢ÚÚ˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜"W6W2Ü∂Wí”‚6W76ñˆ‰ñGÇ¬ÊWfW"FÜR&tñGÄ¢ÚÚFVw&FFñˆ‚(	B&tñGÇ÷2FÚFñffW&VÁB÷W76vRgFW"fó'GV∆ó¶Fñˆ‡¢ÚÚ&R◊vñÊF˜r¬VíÊß2W"◊FñW"wV&Bí6ÚFÜR6∆∆W"6‚f∆¬&6≤FÚFÜRF˜B÷FV«F¢ÚÚñFñˆ“˜"∂VW&r&FÜW"FÜ‚wVW76ñÊr‡¶gVÊ7Fñˆ‚ˆFW6∑F˜Ê6Ü˜%&V∆ñv‰FV«FÜ6ˆÁFñÊW"¬Ê6Ü˜"ó∞¢ñbÇ6ˆÁFñÊW'«¬Ê6Ü˜'««GóVˆb6ˆÁFñÊW"ÁVW'ï6V∆V7F˜"”“vgVÊ7Fñˆ‚rí&WGW&‚ÁV∆√∞¢6ˆÁ7B6GW&VDˆfg6WC‘ÁV÷&W"ÜÊ6Ü˜"ÁF˜ˆfg6WBì∞¢ñbÇÁV÷&W"Êó4fñÊóFRÜ6GW&VDˆfg6WBíí&WGW&‚ÁV∆√∞¢6ˆÁ7BÊ6Ü˜$∂Wì’7G&ñÊrÜÊ6Ü˜"Ê∂Wó«¬rrì∞¢∆WB&˜s÷Ê6Ü˜$∂Wê¢Ú'&íÊg&ˆ“Ü6ˆÁFñÊW"ÁVW'ï6V∆V7F˜$∆¬Çu∂FF÷÷W76vR÷Ê6Ü˜"÷∂Wï“rííÊfñÊBÜV√”ÊV¬bfV¬ÊFF6WBbfV¬ÊFF6WBÊ÷W76vTÊ6Ü˜$∂Wì””÷Ê6Ü˜$∂Wíê¢¢ÁV∆√∞¢ñbá&˜rbg&˜rÊvWD6∆ñVÁE&V7G2bg&˜rÊvWD6∆ñVÁE&V7G2ÇíÊ∆VÊwFÉ”””í&˜s÷ÁV∆√∞¢6ˆÁ7B6W76ñˆ‰ñGÉ‘ÁV÷&W"ÜÊ6Ü˜"Á6W76ñˆ‰ñGÇì∞¢ñbÇ&˜rbdÁV÷&W"Êó4fñÊóFRá6W76ñˆ‰ñGÇíí&˜s÷6ˆÁFñÊW"ÁVW'ï6V∆V7F˜"Ü∂FF◊6W76ñˆ‚÷◊6r÷ñGÉ“"G∑6W76ñˆ‰ñGá“%÷ì∞¢ÚÚW"◊FñW"wV&B÷ó'&˜"áVíÊß2˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜"ì¢vVÁVñÊV«í÷vˆÊP¢ÚÚÊ6Ü˜"÷ó76W2∂Wí‰B6W76ñˆ‰ñGÇ”‚6ˆÊ6VFRÜÁV∆¬í‚FÚ‰ıBFVw&FRFÚ&tñGÇ‡¢ñbÇ&˜rí&WGW&‚ÁV∆√∞¢ñbáGóVˆb&˜rÊvWD&˜VÊFñÊt6∆ñVÁE&V7B”“vgVÊ7Fñˆ‚rí&WGW&‚ÁV∆√∞¢ñbá&˜rÊvWD6∆ñVÁE&V7G2bg&˜rÊvWD6∆ñVÁE&V7G2ÇíÊ∆VÊwFÉ”””í&WGW&‚ÁV∆√∞¢6ˆÁ7B6ˆÁFñÊW%&V7C÷6ˆÁFñÊW"ÊvWD&˜VÊFñÊt6∆ñVÁE&V7BÇì∞¢6ˆÁ7B&V7C◊&˜rÊvWD&˜VÊFñÊt6∆ñVÁE&V7BÇì∞¢6ˆÁ7B7W'&VÁDˆfg6WC◊&V7BÁF˜÷6ˆÁFñÊW%&V7BÁF˜∞¢&WGW&‚7W'&VÁDˆfg6WB÷6GW&VDˆfg6WC∞ß–¶gVÊ7Fñˆ‚˜&W7F˜&T÷W76vU67&ˆ∆≈6Ê6Ü˜E6÷Tg&÷Rá6Ê6Ü˜Bó∞¢6ˆÁ7BV√“BÇv÷W76vW2rì∞¢ñbÇV««¬6Ê6Ü˜Bí&WGW&„∞¢ÚÚ6÷R÷g&÷R∆ófRDÙ“WFFW2áFˆˆ¬˜v˜&∂∆ˆrˆ7FófóGí&˜w2í&RFÜRÜ˜BFÇf˜ ¢ÚÚ7G&V÷ñÊr‚ñÊÊVBfˆ∆∆˜vW'2◊W7B7FíFñ¬◊&V∆FófRÜW&RFˆÛ≤&W7F˜&ñÊrFÜP¢ÚÚ6V÷ÁFñ2fñWw˜'BÊ6Ü˜"ó2ˆÊ«í6fRf˜"Wá∆ñ6óF«íVÁñÊÊVB&VFW'2‡¢ñbáGóVˆbˆ÷W76vU67&ˆ∆≈6Ê6Ü˜DñÁWD6ÜÊvVC””“vgVÊ7Fñˆ‚rbeˆ÷W76vU67&ˆ∆≈6Ê6Ü˜DñÁWD6ÜÊvVBá6Ê6Ü˜Bíó∞¢ñbáGóVˆbˆ&ÊFˆ‰÷W76vU67&ˆ∆≈6Ê6Ü˜C””“vgVÊ7Fñˆ‚ríˆ&ÊFˆ‰÷W76vU67&ˆ∆≈6Ê6Ü˜BÇì∞¢&WGW&„∞¢–¢ñbÖ˜&W7F˜&UñÊÊVD÷W76vU67&ˆ∆≈6Ê6Ü˜Bá6Ê6Ü˜Bíí&WGW&„∞¢ÚÚFV∆ñVB$b&W7F˜&R◊W7BÊ˜B˜fW'w&óFR˜6óFñˆ‚FÜR&VFW"6ÜÊvV@¢ÚÚgFW"6GW&R‚&V6VÁB÷ñÁFVÁBFñ÷W7F◊2&R∆˜77ì≤FÜRvVÊW&Fñˆ‚ó0¢ÚÚ÷ˆÊ˜FˆÊñ2ÊBFÜW&Vf˜&R&W6W'fW26Ê6Ü˜B˜vÊW'6ÜóWÜ7F«í‡¢∆WB&W7F˜&VEfñÊ6Ü˜#“á6Ê6Ü˜BÊÊ6Ü˜"bgGóVˆb˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜#””“vgVÊ7Fñˆ‚rê¢Ú˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜"á6Ê6Ü˜BÊÊ6Ü˜"√ê¢¢f«6S∞¢ñbÇ&W7F˜&VEfñÊ6Ü˜"bgGóVˆb˜&V÷˜VÁD÷W76vUfñWw˜'DÊ6Ü˜#””“vgVÊ7Fñˆ‚rbe˜&V÷˜VÁD÷W76vUfñWw˜'DÊ6Ü˜"á6Ê6Ü˜BÊÊ6Ü˜"íó∞¢&W7F˜&VEfñÊ6Ü˜#“áGóVˆb˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜#””“vgVÊ7Fñˆ‚rê¢Ú˜&W7F˜&T÷W76vUfñWw˜'DÊ6Ü˜"á6Ê6Ü˜BÊÊ6Ü˜"√ê¢¢f«6S∞¢–¢ñbÇ&W7F˜&VEfñÊ6Ü˜"ó∞¢6ˆÁ7B÷ÖF˜‘÷FÇÊ÷ÇÉ∆V¬Á67&ˆ∆ƒÜVñváB÷V¬Ê6∆ñVÁDÜVñváBì∞¢6ˆÁ7B&˜GFˆ”‘ÁV÷&W"á6Ê6Ü˜BÊ&˜GFˆ“ì∞¢ÚÚ÷ˆ&ñ∆R˜F˜V6ÇfñWw˜'G2ÜfRÊFófR˜fW&f∆˜rÊ6Ü˜&ñÊrFÚÜˆ∆B‡¢ÚÚVÁñÊÊVB&VFW"7&˜72&V'Vñ∆B‚FW6∑F˜FV∆ñ&W&FV«íFó6&∆W2FÜ@¢ÚÚ'&˜w6W"&VÜfñ˜"¬6ÚóB◊W7B6ˆÁFñÁVRñÁFÚFÜRWá∆ñ6óBf∆∆&6≤&V∆˜r‡¢6ˆÁ7Bˆf%F˜V6ÑÜˆ∆C“áGóVˆbˆó5F˜V6Ñ∆ñ∂T÷W76vUfñWw˜'C””“vgVÊ7Fñˆ‚rbbˆó5F˜V6Ñ∆ñ∂T÷W76vUfñWw˜'BÜV¬íì∞¢ÚÚ3Sc3s¢vÜV‚FÜR&VFW"Ü267&ˆ∆∆VBUñÁFÚÜó7F˜'íáW6W%VÁñÊÊVBíÊBFÜP¢ÚÚ6V÷ÁFñ2Ê6Ü˜"&W7F˜&Rfñ∆VB¬FÚ‰ıB6Ê67&ˆ∆≈F˜FÚFÜR6GW&V@¢ÚÚ%4Ù≈UDR6Ê6Ü˜BÁF˜‚GW&ñÊr7G&V÷ñÊr¬FÜR∆ófR7FófóGí◊66VÊR&Vg&W6Ä¢ÚÚfó&W2FÜó2WfW'íFñ6≥≤&˜fR◊fñWw˜'BÜVñváB∂VW26ÜÊvñÊr¬6ÚFÜRˆ∆@¢ÚÚ'6ˆ«WFRF˜ÊÚ∆ˆÊvW"÷2FÚFÜR6÷R6ˆÁFVÁBÊBFÜRfñWw˜'Bó2ÁVFvV@¢ÚÚ&6∑v&B'í‚÷˜VÁBFÜBw&˜w2vóFÇ67&ˆ∆ƒÜVñváB‚∆VfñÊr67&ˆ∆≈F˜ ¢ÚÚVÁF˜V6ÜVB∆WG2FÜR'&˜w6W"w2˜v‚67&ˆ∆¬Ê6Ü˜&ñÊrÜˆ∆BFÜR&VFW"w0¢ÚÚ˜6óFñˆ‚‚ñÊÊVBÚÊV"÷&˜GFˆ“&VFW'27Fñ∆¬vWBFÜRFñ¬◊&V∆FófR&W7F˜&P¢ÚÚ&V∆˜ráFÜBFÇó26˜'&V7BÊB◊W7B'V‚í‡¢ñbá6Ê6Ü˜BÁW6W%VÁñÊÊVC””◊G'VRbg6Ê6Ü˜BÁñÊÊVB”◊G'VRbeˆf%F˜V6ÑÜˆ∆Bó∞¢ˆ∆7E67&ˆ∆≈F˜÷V¬Á67&ˆ∆≈F˜µˆ∆7D÷W76vT6∆ñVÁDÜVñváC÷V¬Ê6∆ñVÁDÜVñváC∞¢ˆ÷W76vUW6W%VÁñÊÊVC◊G'VS∞¢˜67&ˆ∆≈ñÊÊVC÷f«6S∞¢ˆÊV$&˜GFˆ‘6˜VÁC”∞¢&WGW&„∞¢–¢6ˆÁ7BF&vWC“á6Ê6Ü˜BÁñÊÊVC””◊G'VRbdÁV÷&W"Êó4fñÊóFRÜ&˜GFˆ“íê¢Ú÷ÖF˜‘÷FÇÊ÷ÇÉ∆&˜GFˆ“ê¢¢ÁV÷&W"á6Ê6Ü˜BÁF˜ó«√∞¢ÚÚ7G&V÷ñÊr7F∆R◊6Ê6Ü˜BwV&BÜó77VR3Sc3rí‚FÜRW6W%VÁñÊÊVB6ÜV6≤&˜fRó0¢ÚÚFVfVFVBvÜV‚∆ófR7G&V“&R◊ñÁ2FÜR7FFR÷6ÜñÊRÜ67&ˆ∆ƒÜVñváB÷6ˆ∆∆6P¢ÚÚ67&ˆ∆¬WfVÁBf∆ó2W6W%VÁñÊÊVB&6≤FÚf«6RWfV‚FÜ˜VvÇFÜR&VFW"ó2Wñ‡¢ÚÚÜó7F˜'íí¬6ÚFÜó2'6ˆ«WFR6Ê6Ü˜BÁF˜w&óFR7Fñ∆¬fó&W2ÊBñÊ∑27Fñ∆¿¢ÚÚ&VFW"(	B6Ê6Ü˜BÁF˜v26GW&VB&Vf˜&RFÜR7G&V÷ñÊr6áVÊ≤w&Wr&˜fR◊fñWw˜'@¢ÚÚÜVñváB¬6ÚóBó27F∆R‚÷ó'&˜"FÜR&V∆ñv‚wV&C¢ñb6ˆÁFVÁBw&Wr6ñÊ6RFÜP¢ÚÚ6Ê6Ü˜B‰BFÜW&Ró2ÊÚ&V6VÁB&V¬ñÁWBñÁFVÁB‰BFÜRw&óFRv˜V∆B÷˜fP¢ÚÚ67&ˆ∆≈F˜Êˆ‚◊G&ófñ∆«í¬&VgW6RóBÊB∆WBFÜR'&˜w6W"˜fW&f∆˜r÷Ê6Ü˜"Üˆ∆B‡¢ÚÚñÊÊVBFñ¬÷fˆ∆∆˜vW'2áF&vWBó2&˜GFˆ“◊&V∆FófR¬Ê˜B6Ê6Ü˜BÁF˜í&P¢ÚÚVÊffV7FVC≤‚7FófV«í67&ˆ∆∆ñÊr&VFW"Ü2ñÁFVÁBÊB∂VW2FÜR&W7F˜&R‡¢Ú¢ÚÚFW6∑F˜wV&BÜó77VR3Sc3rvFR6W'Bì¢∆ñ∂RFÜR&V∆ñv‚wV&B¬FÜó2&VgW6¿¢ÚÚˆÊ«íÜˆ∆G2vÜW&RFÜR'&˜w6W"w2ÊFófR˜fW&f∆˜r÷Ê6Ü˜"∆ñW"ó27FófRáF˜V6Ä¢ÚÚfñWw˜'G2¬Ê÷W76vW66ˆ◊WFW2FÚ˜fW&f∆˜r÷Ê6Ü˜#¶WFˆí‚FW6∑F˜Ê÷W76vW6 ¢ÚÚó2˜fW&f∆˜r÷Ê6Ü˜#¶ÊˆÊV¬6Ú&VgW6ñÊrFÜR'6ˆ«WFRf∆∆&6≤w&óFRFÜW&Rv˜V∆@¢ÚÚ∆VfRFÜR&VFW"VÊÜV∆B‰B∆F6Çˆ÷W76vUW6W%VÁñÊÊVC◊G'VV‚vFRˆ‡¢ÚÚˆó5F˜V6Ñ∆ñ∂T÷W76vUfñWw˜'F6ÚFW6∑F˜∂VW2óG2'6ˆ«WFR6Ê6Ü˜BÁF˜&W7F˜&R‡¢6ˆÁ7B˜6Ê4É‘ÁV÷&W"á6Ê6Ü˜BÁ67&ˆ∆ƒÜVñváBì∞¢6ˆÁ7Bˆw&Wu6ñÊ6U6Ê‘ÁV÷&W"Êó4fñÊóFRÖ˜6Ê4Çíbe˜6Ê4É„bbÜV¬Á67&ˆ∆ƒÜVñváB’˜6Ê4Çì„C∞¢6ˆÁ7Bˆf$7FófTñÁFVÁC“áGóVˆb˜&V6VÁD÷W76vU67&ˆ∆ƒñÁFVÁC””“vgVÊ7Fñˆ‚rbb˜&V6VÁD÷W76vU67&ˆ∆ƒñÁFVÁBÇíê¢«¬áGóVˆb˜&V6VÁD÷W76vUF˜V6Ö67&ˆ∆ƒñÁFVÁC””“vgVÊ7Fñˆ‚rbb˜&V6VÁD÷W76vUF˜V6Ö67&ˆ∆ƒñÁFVÁBÇíì∞¢ñbÖˆf%F˜V6ÑÜˆ∆Bbb6Ê6Ü˜BÁñÊÊVB”◊G'VRbbˆw&Wu6ñÊ6U6Êbbˆf$7FófTñÁFVÁ@¢bb÷FÇÊ'2ÇÑ÷FÇÊ÷ÇÉƒ÷FÇÊ÷ñ‚áF&vWB∆÷ÖF˜ííí÷V¬Á67&ˆ∆≈F˜ì„Çó∞¢ˆ∆7E67&ˆ∆≈F˜÷V¬Á67&ˆ∆≈F˜µˆ∆7D÷W76vT6∆ñVÁDÜVñváC÷V¬Ê6∆ñVÁDÜVñváC∞¢ˆ÷W76vUW6W%VÁñÊÊVC◊G'VS∞¢˜67&ˆ∆≈ñÊÊVC÷f«6S∞¢ˆÊV$&˜GFˆ‘6˜VÁC”∞¢&WGW&„∞¢–¢ÚÚFW6∑F˜7F∆R◊6Ê6Ü˜B&W6ñGVRfóÇÜó77VR3Sc3rfˆ∆∆˜r◊W¬"3SsC"&˜VÊB”2í‡¢ÚÚˆ‚FW6∑F˜Ü˜fW&f∆˜r÷Ê6Ü˜#¶ÊˆÊRíFÜRF˜V6Ç&VgW6¬&˜fRFˆW2‰ıB«í(	BFÜP¢ÚÚ&VFW"◊W7B&R7FófV«íÜV∆B¬6ÚvRw&óFR67&ˆ∆≈F˜‚FÜR%4Ù≈UDR6Ê6Ü˜BÁF˜ó0¢ÚÚ7F∆RˆÊ6R&˜fR◊fñWw˜'B6ˆÁFVÁBw&Wr6ñÊ6R6GW&R‚W6RFÜRw2˜v‚&V∆ñv‡¢ÚÚñFñˆ“ñÁ7FVC¢6ÜñgBFÜR5U%$TÂB67&ˆ∆≈F˜'íÜ˜rf"FÜRÊ6Ü˜"&˜r÷˜fVB6ñÊ6P¢ÚÚ6GW&R‚67&ˆ∆≈F˜≥“Ü7W'&VÁDˆfg6WB“6GW&VDˆfg6WBñÜˆ∆G2FÜR&˜rWBÊ¢ÚÚ÷GFW"vÜW&R67&ˆ∆≈F˜v26'&ñVBÜ&˜rw2ˆfg6WBó267&ˆ∆¬◊&V∆FófRí¬vÜñ6ÇFÜP¢ÚÚ7FvVB6Ê6Ü˜BÁF˜≤FV«F6ÊÊ˜B‚ÊÚ&&óFW#¢FÜR&V∆ñv‚ó2ÊÚ÷˜vÜV‡¢ÚÚ«&VGí∆ñvÊVBÜFV«F‚íÊBÜV«2vÜV‚Ê˜B‚ˆÊ«ívÜV‚FÜRÊ6Ü˜"&˜ró0¢ÚÚvVÁVñÊV«ívˆÊRáW"◊FñW"∆ˆˆ∑W6ˆÊ6VFW2¬ÊÚ&tñGÇFVw&FFñˆ‚íFÚvRf∆¬&6∞¢ÚÚFÚFÜRF˜B÷FV«FñFñˆ“¬FÜV‚FÚ&r‚ñÊÊVBˆÊV"÷&˜GFˆ“&VFW'2Fˆˆ≤FÜP¢ÚÚ&˜GFˆ“◊&V∆FófRF&vWB&˜fRÊBÊWfW"&V6ÇÜW&R2VÁñÊÊVB‡¢∆WBˆf%F&vWC‘÷FÇÊ÷ÇÉƒ÷FÇÊ÷ñ‚áF&vWB∆÷ÖF˜íì∞¢ñbÇˆf%F˜V6ÑÜˆ∆Bbb6Ê6Ü˜BÁñÊÊVB”◊G'VRó∞¢6ˆÁ7B˜&V∆ñv„’ˆFW6∑F˜Ê6Ü˜%&V∆ñv‰FV«FÜV¬¬6Ê6Ü˜BÊÊ6Ü˜"ì∞¢ñbÖ˜&V∆ñv‚”÷ÁV∆¬ó∞¢ÚÚÊ6Ü˜"&˜r÷V7W&&∆S¢&V∆ñv‚g&ˆ“FÜRƒïdR67&ˆ∆≈F˜ÜñFñˆ“í‡¢ˆf%F&vWC‘÷FÇÊ÷ÇÉƒ÷FÇÊ÷ñ‚ÜV¬Á67&ˆ∆≈F˜µ˜&V∆ñv‚¬÷ÖF˜íì∞¢÷V«6W∞¢ÚÚÊ6Ü˜"&˜rvVÁVñÊV«ívˆÊR‚÷ó'&˜"FÜRF˜B÷FV«FñFñˆ“FÜRÊ6Ü˜"«&VGê¢ÚÚ6'&ñW2áF˜D&Vf˜&Rì¢6ÜñgB'íFÜRw&˜wFÇˆbFÜRfó'GV¬F˜76W"6ñÊ6P¢ÚÚ6GW&R6ÚFÜR&VFW"ó2ÜV∆B'íFÜR6÷R÷˜VÁBFÜR6ˆÁFVÁB&˜fR÷˜fVB‡¢6ˆÁ7B˜DÊ˜s“ÜgVÊ7Fñˆ‚Çó∞¢6ˆÁ7B3÷V¬ÁVW'ï6V∆V7F˜"Çu∂FF◊fó'GV¬◊76W#“&&Vf˜&R%“rì∞¢&WGW&‚3Úá'6Tf∆ˆBá2Á7Gñ∆RÊÜVñváG«¬sró«√ì§Ê„∞¢“íÇì∞¢6ˆÁ7B˜D&Vf˜&U&s◊6Ê6Ü˜BÊÊ6Ü˜"bg6Ê6Ü˜BÊÊ6Ü˜"ÁF˜D&Vf˜&S∞¢6ˆÁ7B˜D&Vf˜&S‘ÁV÷&W"Ö˜D&Vf˜&U&rì∞¢ÚÚ&WVó&R‚5ET¬6GW&VBF˜D&Vf˜&RÜÊ˜BÁV∆¬˜VÊFVfñÊVBì¢ÁV÷&W"ÜÁV∆¬íó2¿¢ÚÚvÜñ6Çv˜V∆B˜FÜW'vó6RFBFÜRTÂDï$R7W'&VÁB76W"ÜVñváBFÚ67&ˆ∆≈F˜ÊBf∆ñÊp¢ÚÚFÜR&VFW"f"g&ˆ“FÜVó"6ˆÁFVÁBÜw&WFñ∆Rí‚ˆÊ«í«ívÜV‚óBv2&V∆«ê¢ÚÚ6GW&VC≤V«6R∂VWFÜR&rf∆∆&6≤F&vWB‡¢ñbÖ˜D&Vf˜&U&r÷ÁV∆¬bdÁV÷&W"Êó4fñÊóFRÖ˜DÊ˜ríbdÁV÷&W"Êó4fñÊóFRÖ˜D&Vf˜&Ríó∞¢ˆf%F&vWC‘÷FÇÊ÷ÇÉƒ÷FÇÊ÷ñ‚ÜV¬Á67&ˆ∆≈F˜≤Ö˜DÊ˜r’˜D&Vf˜&Rí¬÷ÖF˜íì∞¢–¢ÚÚV«6S¢ÊÚ÷V7W&&∆RÊ6Ü˜"ÊBÊÚF˜BvVˆ÷WG'í”‚∂VW&rF&vWB‡¢–¢–¢˜&ˆw&÷÷Fñ567&ˆ∆√◊G'VSµ˜&ˆw&÷÷Fñ567&ˆ∆≈6WDC◊W&f˜&÷Ê6RÊÊ˜rÇì∞¢V¬Á67&ˆ∆≈F˜’ˆf%F&vWC∞¢–¢ˆ∆7E67&ˆ∆≈F˜÷V¬Á67&ˆ∆≈F˜µˆ∆7D÷W76vT6∆ñVÁDÜVñváC÷V¬Ê6∆ñVÁDÜVñváC∞¢ñbá6Ê6Ü˜BÁñÊÊVC””◊G'VRó∞¢ˆ÷W76vUW6W%VÁñÊÊVC÷f«6S∞¢˜67&ˆ∆≈ñÊÊVC◊G'VS∞¢ˆÊV$&˜GFˆ‘6˜VÁC”#∞¢÷V«6Rñbá6Ê6Ü˜BÁW6W%VÁñÊÊVC””◊G'VRó∞¢ˆ÷W76vUW6W%VÁñÊÊVC◊G'VS∞¢˜67&ˆ∆≈ñÊÊVC÷f«6S∞¢ˆÊV$&˜GFˆ‘6˜VÁC”∞¢–¢ñbÇ&W7F˜&VEfñÊ6Ü˜"ó∞¢ñbáGóVˆbˆFVfW$6∆V%&ˆw&÷÷Fñ567&ˆ∆√””“vgVÊ7Fñˆ‚ríˆFVfW$6∆V%&ˆw&÷÷Fñ567&ˆ∆¬Çì∞¢V«6R&WVW7DÊñ÷Fñˆ‰g&÷RÇÇì”Á≤6WEFñ÷V˜WBÇÇì”Á≤˜&ˆw&÷÷Fñ567&ˆ∆√÷f«6S≤“√ì≤“ì∞¢–ß–¶gVÊ7Fñˆ‚˜&VÊFW$÷W76vW5vóFÖ67&ˆ∆≈6Ê6Ü˜BÜ˜FñˆÁ2ó∞¢ÚÚ66WB‚˜FñˆÊ¬&R÷6GW&VB67&ˆ∆¬6Ê6Ü˜Bfñ˜&W67&ˆ∆≈6Ê6Ü˜B‡¢ÚÚvÜV‚&˜fñFVB¬óBó2W6VBîÂ5DTBˆb6GW&ñÊrg&W6ÇˆÊRg&ˆ“FÜR7W'&VÁ@¢ÚÚDÙ“7FFR(	BW76VÁFñ¬f˜"FÜR5E$T’ÙDÙ‰R6ˆ∆∆6R&VÊFW#¢FÜR6∆∆W"Ü0¢ÚÚ«&VGí6GW&VBFÜR6Ê6Ü˜Bg&ˆ“FÜRƒïdRDÙ“Ü&Vf˜&R∂VW÷˜V‚v2&÷VBí¿¢ÚÚÊB&R÷6GW&ñÊrg&ˆ“FÜRñÁFW&÷VFñFRWáÊFVB◊v˜&∂∆ˆr7FFRv˜V∆B6GW&P¢ÚÚ7F∆RÊ6Ü˜'2FÜBÊÚ∆ˆÊvW"WÜó7BgFW"FÜRv˜&∂∆ˆr6ˆ∆∆6W2‚Ç3c3ÉRê¢6ˆÁ7B67&ˆ∆≈6Ê6Ü˜C“Ü˜FñˆÁ2bf˜FñˆÁ2Â˜&W67&ˆ∆≈6Ê6Ü˜Bó«≈ˆ6GW&T÷W76vU67&ˆ∆≈6Ê6Ü˜BÇì∞¢&VÊFW$÷W76vW2á≤‚‚‚Ü˜FñˆÁ7««∑“í«&W6W'fU67&ˆ∆√ßG'VW“ì∞¢˜&W7F˜&T÷W76vU67&ˆ∆≈6Ê6Ü˜E6÷Tg&÷Rá67&ˆ∆≈6Ê6Ü˜Bì∞ß–¶∆WBˆ76ó7FÁEGW&‰Ê6Ü˜%6WGF∆VDfñÊƒÁ7vW%v&ÊVC÷f«6S∞¶gVÊ7Fñˆ‚˜G&Á7&VÁE7G&V‘˜&FW&VE'G2Ü÷W76vRó∞¢ñbáGóVˆbó5G&Á7&VÁE7G&V”””“vgVÊ7Fñˆ‚rbbó5G&Á7&VÁE7G&V“Çíí&WGW&‚ÁV∆√∞¢ñbÇ÷W76vW«∆÷W76vRÁ&ˆ∆R”“v76ó7FÁBw«∆÷W76vRÂˆ∆ófW«¬'&íÊó4'&íÜ÷W76vRÊ6ˆÁFVÁBíí&WGW&‚ÁV∆√∞¢ñbÜ÷W76vRÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊRí&WGW&‚ÁV∆√∞¢6ˆÁ7B˜&FW&VC’µ”∞¢6ˆÁ7B÷W76vUG3◊GóVˆbˆfó'7Ef∆ñEFñ÷W7F◊6V6ˆÊG3””“vgVÊ7Fñˆ‚p¢Úˆfó'7Ef∆ñEFñ÷W7F◊6V6ˆÊG2Ü÷W76vRÂ˜G2¬÷W76vRÁFñ÷W7F◊¬÷W76vRÊ7&VFVEˆBê¢¢Ü÷W76vRÂ˜G7«∆÷W76vRÁFñ÷W7F◊«∆÷W76vRÊ7&VFVEˆBì∞¢∆WBÜ5FWáC÷f«6S∞¢∆WBÜ5Fˆˆ√÷f«6S∞¢f˜"Ü6ˆÁ7B'Bˆb÷W76vRÊ6ˆÁFVÁBó∞¢ñbÇ'G««GóVˆb'B”“vˆ&¶V7Brí6ˆÁFñÁVS∞¢ñbá'BÁGóS””“wFWáBró∞¢6ˆÁ7BFWáC◊GóVˆb'BÁFWáC””“w7G&ñÊrs˜'BÁFWáC¢áGóVˆb'BÊ6ˆÁFVÁC””“w7G&ñÊrs˜'BÊ6ˆÁFVÁC¢rrì∞¢ñbÇ7G&ñÊráFWáG«¬rríÁG&ñ“Çíí6ˆÁFñÁVS∞¢˜&FW&VBÁW6Çá∂∂ñÊC¢wFWáBr¬FWáG“ì∞¢Ü5FWáC◊G'VS∞¢6ˆÁFñÁVS∞¢–¢ñbá'BÁGóS””“wFˆˆ≈˜W6Rró∞¢6ˆÁ7BFˆˆ≈W6TñC’7G&ñÊrá'BÊñG«¬rríÁG&ñ“Çì∞¢ñbÇFˆˆ≈W6TñBí&WGW&‚ÁV∆√∞¢˜&FW&VBÁW6Çá∞¢∂ñÊC¢wFˆˆ¬r¿¢Fˆˆ≈W6TñB¿¢Ê÷Sß'BÊÊ÷W«¬wFˆˆ¬r¿¢ñÁWC¢á'BÊñÁWBbgGóVˆb'BÊñÁWC””“vˆ&¶V7Brì˜'BÊñÁWCß∑“¿¢G3ß'BÁG2¿¢Fñ÷W7F◊ß'BÁFñ÷W7F◊¿¢7&VFVEˆCß'BÊ7&VFVEˆB¿¢÷W76vU˜G3¶÷W76vUG2¿¢“ì∞¢Ü5Fˆˆ√◊G'VS∞¢–¢–¢&WGW&‚Ü5FWáBbfÜ5Fˆˆ√ˆ˜&FW&VC¶ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆ∆Vv7ï6WGF∆VDf∆∆&6¥Ü5Fˆˆƒ÷WFFFÜ÷W76vRó∞¢ñbÇ÷W76vW«∆÷W76vRÁ&ˆ∆R”“v76ó7FÁBw«∆÷W76vRÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊRí&WGW&‚f«6S∞¢&WGW&‚Ä¢Ñ'&íÊó4'&íÜ÷W76vRÁFˆˆ≈ˆ6∆«2íbf÷W76vRÁFˆˆ≈ˆ6∆«2Ê∆VÊwFÉ„ó«¿¢Ñ'&íÊó4'&íÜ÷W76vRÂ˜'Fñ≈˜Fˆˆ≈ˆ6∆«2íbf÷W76vRÂ˜'Fñ≈˜Fˆˆ≈ˆ6∆«2Ê∆VÊwFÉ„ó«¿¢Ñ'&íÊó4'&íÜ÷W76vRÊ6ˆÁFVÁBíbf÷W76vRÊ6ˆÁFVÁBÁ6ˆ÷Rá'C”Á'BbgGóVˆb'C””“vˆ&¶V7Brbg'BÁGóS””“wFˆˆ≈˜W6Rríê¢ì∞ß–¶gVÊ7Fñˆ‚˜G&Á7&VÁD˜&FW&VDFó7∆ïFWáBáFWáBó∞¢&WGW&‚˜7G&óv˜&∑76TFó7∆ï&VfóÇÄ¢˜7G&óGF6ÜVDfñ∆W4÷&∂W$f˜$Fó7∆íÄ¢˜7G&ó∆VFñÊt76ó7FÁEFÜñÊ∂ñÊt÷&∑WÖ7G&ñÊráFWáG«¬rríê¢ê¢ì∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ∆∆V7EFˆˆ≈&W7V«E6ÊóWG4'ïFñBÜ÷W76vW2ó∞¢6ˆÁ7B&W7V«G4'ïFñC◊∑”∞¢f˜"Ü6ˆÁ7B÷W76vRˆbÜ÷W76vW7«≈µ“íó∞¢ñbÇ÷W76vRí6ˆÁFñÁVS∞¢ñbÜ÷W76vRÁ&ˆ∆S””“wFˆˆ¬ró∞¢6ˆÁ7BFñC÷÷W76vRÁFˆˆ≈ˆ6∆≈ˆñG«∆÷W76vRÁFˆˆ≈˜W6UˆñG«¬rs∞¢ñbáFñBí&W7V«G4'ïFñE∑FñE”’ˆ6∆ïFˆˆ≈&W7V«E6ÊóWBÜ÷W76vRÊ6ˆÁFVÁBì∞¢6ˆÁFñÁVS∞¢–¢ñbÇ'&íÊó4'&íÜ÷W76vRÊ6ˆÁFVÁBíí6ˆÁFñÁVS∞¢f˜"Ü6ˆÁ7B'Bˆb÷W76vRÊ6ˆÁFVÁBó∞¢ñbÇ'G««GóVˆb'B”“vˆ&¶V7Bw««'BÁGóR”“wFˆˆ≈˜&W7V«Brí6ˆÁFñÁVS∞¢6ˆÁ7BFñC◊'BÁFˆˆ≈˜W6UˆñG«¬rs∞¢ñbÇFñBí6ˆÁFñÁVS∞¢6ˆÁ7B&s◊GóVˆb'BÊ6ˆÁFVÁC””“w7G&ñÊrp¢Ú'BÊ6ˆÁFVÁ@¢¢'&íÊó4'&íá'BÊ6ˆÁFVÁBê¢Ú'BÊ6ˆÁFVÁBÊ÷Ü3”Ê2bf2ÁFWáCˆ2ÁFWáC¢rríÊ¶ˆñ‚Çrrê¢¢rs∞¢&W7V«G4'ïFñE∑FñE”’ˆ6∆ïFˆˆ≈&W7V«E6ÊóWBá&rì∞¢–¢–¢&WGW&‚&W7V«G4'ïFñC∞ß–¶gVÊ7Fñˆ‚˜G&Á7&VÁD˜&FW&VEFˆˆƒ6∆¬á'B¬&tñGÇ¬Fˆˆƒ6∆«4'ïFñB¬&W7V«G4'ïFñB¬W'6ó7FVD'ïFñB¬÷W76vUG2ó∞¢6ˆÁ7BFñC’7G&ñÊrá'Bbg'BÁFˆˆ≈W6TñG«¬rríÁG&ñ“Çì∞¢6ˆÁ7Bfó'7Ef∆ñEFñ÷W7F◊6V6ˆÊG3◊GóVˆbˆfó'7Ef∆ñEFñ÷W7F◊6V6ˆÊG3””“vgVÊ7Fñˆ‚p¢Úˆfó'7Ef∆ñEFñ÷W7F◊6V6ˆÊG0¢¢gVÊ7Fñˆ‚Ç‚‚Áf«VW2ó∞¢f˜"Ü6ˆÁ7Bf«VRˆbf«VW2ó∞¢6ˆÁ7B7F◊‘ÁV÷&W"áf«VRì∞¢ñbÑÁV÷&W"Êó4fñÊóFRá7F◊íbg7F◊„í&WGW&‚7F◊„S#˜7F◊Ûß7F◊∞¢–¢&WGW&‚ÁV∆√∞¢”∞¢6ˆÁ7B÷W76vU7F◊÷fó'7Ef∆ñEFñ÷W7F◊6V6ˆÊG2Ü÷W76vUG2¬'Bbg'BÊ÷W76vU˜G2ì∞¢6ˆÁ7B'E7F◊÷fó'7Ef∆ñEFñ÷W7F◊6V6ˆÊG2á'Bbg'BÁG2¬'Bbg'BÁFñ÷W7F◊¬'Bbg'BÊ7&VFVEˆBì∞¢6ˆÁ7B∆ófUFˆˆ√◊FñBbgFˆˆƒ6∆«4'ïFñBbgFˆˆƒ6∆«4'ïFñBÊvWBáFñBì∞¢ñbÜ∆ófUFˆˆ¬ó∞¢6ˆÁ7BÊWáC◊≤‚‚Ê∆ófUFˆˆ«”∞¢6ˆÁ7BÜ4WfVÁE7F◊÷fó'7Ef∆ñEFñ÷W7F◊6V6ˆÊG2ÜÊWáBÁG2¬ÊWáBÁFñ÷W7F◊¬ÊWáBÊ7&VFVEˆB¬ÊWáBÁ7F'FVEˆB¬ÊWáBÊ6ˆ◊∆WFVEˆBì∞¢6ˆÁ7Bf∆∆&6µ7F◊◊'E7F◊«∆÷W76vU7F◊∞¢ñbÇÜ4WfVÁE7F◊bff∆∆&6µ7F◊ó∞¢ÊWáBÁG3÷f∆∆&6µ7F◊∞¢ÊWáBÁFñ÷W7F◊÷f∆∆&6µ7F◊∞¢ÊWáBÊ7&VFVEˆC÷f∆∆&6µ7F◊∞¢–¢6ˆÁ7B∆ófU6Êó“á&W7V«G4'ïFñBbg&W7V«G4'ïFñE∑FñE“ó«¬áW'6ó7FVD'ïFñBbgW'6ó7FVD'ïFñE∑FñE“ó«¬rs∞¢ñbÜ∆ófU6Êóó∞¢6ˆÁ7BF6Ö6ÊóWC’ˆ6∆ïF6Ö6ÊóWDg&ˆ‘&w2ÜÊWáBÊÊ÷W««'BÊÊ÷W«¬wFˆˆ¬r¬ÊWáBÊ&w7««'BÊñÁWG««∑“ì∞¢ÊWáBÁ6ÊóWC’ˆ6∆ïFˆˆƒ6&E6ÊóWBÜ∆ófU6Êó«F6Ö6ÊóWBì∞¢ÊWáBÊó5ˆFñfc’ˆ6∆ïFˆˆƒ6&DÜ4Fñfe6ÊóWBÜ∆ófU6Êó«F6Ö6ÊóWBì∞¢–¢ñbÜÊWáBÊFˆÊS””◊VÊFVfñÊVBíÊWáBÊFˆÊS◊G'VS∞¢&WGW&‚ÊWáC∞¢–¢6ˆÁ7BÊ÷S◊'Bbg'BÊÊ÷W«¬wFˆˆ¬s∞¢6ˆÁ7B&w3“á'Bbg'BÊñÁWBbgGóVˆb'BÊñÁWC””“vˆ&¶V7Brì˜'BÊñÁWCß∑”∞¢6ˆÁ7BF6Ö6ÊóWC’ˆ6∆ïF6Ö6ÊóWDg&ˆ‘&w2ÜÊ÷R∆&w2ì∞¢6ˆÁ7B&W7V«E6ÊóWC“á&W7V«G4'ïFñBbgFñBbg&W7V«G4'ïFñE∑FñE“ó«¬áW'6ó7FVD'ïFñBbgFñBbgW'6ó7FVD'ïFñE∑FñE“ó«¬rs∞¢6ˆÁ7Bf∆∆&6µ7F◊◊'E7F◊«∆÷W76vU7F◊∞¢6ˆÁ7B&ñ÷'ï7F◊÷fó'7Ef∆ñEFñ÷W7F◊6V6ˆÊG2á'Bbg'BÁG2¬'Bbg'BÁFñ÷W7F◊¬'Bbg'BÊ7&VFVEˆB¬f∆∆&6µ7F◊ì∞¢&WGW&‚∞¢Ê÷R¿¢FñB¿¢ñCßFñB¿¢76ó7FÁEˆ◊6uˆñGÉß&tñGÇ¿¢&w3•˜Fˆˆƒ&w56Ê6Ü˜BÜ&w2í¿¢6ÊóWC•ˆ6∆ïFˆˆƒ6&E6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBí¿¢ó5ˆFñfc•ˆ6∆ïFˆˆƒ6&DÜ4Fñfe6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBí¿¢FˆÊSßG'VR¿¢G3ß&ñ÷'ï7F◊««VÊFVfñÊVB¿¢Fñ÷W7F◊ß&ñ÷'ï7F◊««VÊFVfñÊVB¿¢7&VFVEˆCß&ñ÷'ï7F◊««VÊFVfñÊVB¿¢”∞ß–¶gVÊ7Fñˆ‚ˆ76ó7FÁEGW&‰Ê6Ü˜%6WGF∆VDfñÊƒÁ7vW"Ü÷W76vR¬6ˆÁFVÁB¬6ˆÁFWáBó∞¢6ˆÁ7B66VÊTfñÊ√’ˆ76ó7FÁDÊ6Ü˜%66VÊTfñÊƒÁ7vW%FWáBÜ÷W76vRì∞¢6ˆÁ7BVffV7FófT6ˆÁFVÁC’7G&ñÊrÜ6ˆÁFVÁG«¬rríÁG&ñ“Çìˆ6ˆÁFVÁCß66VÊTfñÊ√∞¢G'ó∞¢6ˆÁ7Bì“áGóVˆbvñÊF˜r”“wVÊFVfñÊVBrì˜vñÊF˜r‰ÜW&÷W476ó7FÁEGW&‰Ê6Ü˜'3¶ÁV∆√∞¢ñbÇó««GóVˆbíÁ&ˆ¶V7D76ó7FÁEGW&‰Ê6Ü˜%6WGF∆VD÷W76vTfñÊƒÁ7vW"”“vgVÊ7Fñˆ‚rí&WGW&‚7G&ñÊrá66VÊTfñÊ««¬rríÁG&ñ“Çì˜66VÊTfñÊ√¶ÁV∆√∞¢6ˆÁ7B&W7V«C÷íÁ&ˆ¶V7D76ó7FÁEGW&‰Ê6Ü˜%6WGF∆VD÷W76vTfñÊƒÁ7vW"Ü÷W76vR«∞¢6W76ñˆÂˆñC¶6ˆÁFWáBbf6ˆÁFWáBÁ6W76ñˆÂˆñB¿¢&uˆñGÉ¶6ˆÁFWáBbf6ˆÁFWáBÁ&uˆñGÇ¿¢6ˆÁFVÁC¶VffV7FófT6ˆÁFVÁB¿¢“ì∞¢6ˆÁ7BfñÊƒÁ7vW#◊&W7V«BbgGóVˆb&W7V«BÊfñÊ≈ˆÁ7vW#””“w7G&ñÊrs˜&W7V«BÊfñÊ≈ˆÁ7vW#¢rs∞¢&WGW&‚fñÊƒÁ7vW#ˆfñÊƒÁ7vW#¢Ö7G&ñÊrá66VÊTfñÊ««¬rríÁG&ñ“Çì˜66VÊTfñÊ√¶ÁV∆¬ì∞¢÷6F6ÇÜW'"ó∞¢ñbÇˆ76ó7FÁEGW&‰Ê6Ü˜%6WGF∆VDfñÊƒÁ7vW%v&ÊVBbgGóVˆb6ˆÁ6ˆ∆R”“wVÊFVfñÊVBrbf6ˆÁ6ˆ∆RÁv&‚ó∞¢ˆ76ó7FÁEGW&‰Ê6Ü˜%6WGF∆VDfñÊƒÁ7vW%v&ÊVC◊G'VS∞¢6ˆÁ6ˆ∆RÁv&‚Çv76ó7FÁBGW&‚Ê6Ü˜"6WGF∆VB÷fñÊ¬&ˆ¶V7Fñˆ‚fñ∆VBr∆W'"ì∞¢–¢&WGW&‚ÁV∆√∞¢–ß–¢ÚÚ&R÷Ê6Ü˜"ñÊÊVB˜Fñ¬÷fˆ∆∆˜vñÊr&VFW"FÚFÜR6WGF∆VB&˜GFˆ“gFW"gV∆¿¢ÚÚ&VÊFW$÷W76vW2Çí&V'Vñ∆B¬V∆ñ÷ñÊFñÊrFÜRˆÊR÷g&÷R÷ñB◊7G&V“¶óGFW"‚’U5B&R66ÜVGV∆V@¢ÚÚñ‚‘î5$ıD4≤g&ˆ“FÜRVÊBˆb&VÊFW$÷W76vW2á6VRFÜRVWVT÷ñ7&˜F6≤6∆¬6óFRí¬‰ıB'V‡¢ÚÚ7ñÊ6á&ˆÊ˜W6«í‚váì¢FÜR÷ñB◊7G&V“&R◊&VÊFW"'Vró2FÜB&VÊFW$÷W76vW2vóW26◊6tñÊÊW ¢ÚÚFÜV‚&V'Vñ∆G2¬ÊBFÜRñÊÊVBFñ¬÷fˆ∆∆˜rFÇá67&ˆ∆ƒñeñÊÊVB(i"67&ˆ∆≈FÙ&˜GFˆ“íw&óFW0¢ÚÚ67&ˆ∆≈F˜vÜñ∆R7Fñ∆¬îÂ4îDRFÜR&VÊFW"7ñÊ27F6≤¬vÜW&RFÜR'&˜w6W"&W˜'G2E$Â4îTÂ@¢ÚÚ67&ˆ∆ƒÜVñváBfWrÇ6Ü˜'BˆbFÜR6WGF∆VBf«VRÜ∆ñ˜WBó2&F6ÜVB(	BWfW'ívVˆ÷WG'í&V@¢ÚÚñÁ6ñFRFÜR7ñÊ27F6≤&WGW&Á2FÜR÷ñB◊6WGF∆RÜVñváBí‚6Ú67&ˆ∆≈FÙ&˜GFˆ“∆ÊG267&ˆ∆≈F˜¢ÚÚ∆óGF∆RÑîtÇá6Ü˜'BˆbFÜRG'VRFñ¬ì≤FÜBñÁFW&÷VFñFRó2ñÁFVBFÜó2g&÷RÊBFÜP¢ÚÚ6WGF∆R$b6˜'&V7G2óBFÜRÊWáBg&÷R(i"f7B„◊&˜r&6≤÷ÊB÷f˜'FÇ&˜VÊ6Rá„É'Çí‚¢ÚÚ÷ñ7&˜F6≤'VÁ2eDU"FÜR7ñÊ27F6≤VÁvñÊG2Ü∆ñ˜WBÜ2f«W6ÜVB¬6Ú67&ˆ∆ƒÜVñváBˆ6∆ñVÁDÜVñvá@¢ÚÚ&RFÜR6WGF∆VBf«VW2í'WB$Tdı$RFÜR'&˜w6W"ñÁG2(	B6Úw&óFñÊrFÜRÊ˜r÷6˜'&V7B6WGF∆V@¢ÚÚ÷ÇÜW&R∆ÊG2FÜRFñ¬WÜ7F«íÊBFÜR6Ü˜'BñÁFW&÷VFñFRÊWfW"&V6ÜW2FÜR67&VV‚‚ˆÊ«ê¢ÚÚfó&W2f˜"&R◊vóRFñ¬÷fˆ∆∆˜vW"∆VgB6Ü˜'BˆbFÜR6WGF∆VB÷Ç¬6Ú‚VÁñÊÊVB&VFW ¢ÚÚ&∂VBñ‚Üó7F˜'íó2ÊWfW"÷˜fVBÜ˜'FÜˆvˆÊ¬FÚFÜRVÁñÊÊVBßV◊÷&6≤6∆72í‚FÜP¢ÚÚ˜&ˆw&÷÷Fñ567&ˆ∆¬∆F6ÇÜ&÷VBBFÜRvóRí∂VW2FÜR67&ˆ∆¬∆ó7FVÊW"g&ˆ“÷ó7&VFñÊp¢ÚÚFÜó2w&óFR2÷ÁV¬VÁñ‚‚ñFV◊˜FVÁC¢ÊÚ÷˜ˆÊ6R67&ˆ∆≈F˜«&VGíWV«2FÜR÷Ç‡¶gVÊ7Fñˆ‚˜&VÊ6Ü˜%ñÊÊVEFñƒgFW%&VÊFW"áv4ÊV%Fñ¬ó∞¢ñbÇv4ÊV%Fñ¬í&WGW&„∞¢6ˆÁ7BV√“BÇv÷W76vW2rì∞¢ñbÇV¬í&WGW&„∞¢6ˆÁ7B6WGF∆VD÷É‘÷FÇÊ÷ÇÉ¬V¬Á67&ˆ∆ƒÜVñváB÷V¬Ê6∆ñVÁDÜVñváBì∞¢ñbÜV¬Á67&ˆ∆≈F˜¬6WGF∆VD÷Ç”ó∞¢˜&ˆw&÷÷Fñ567&ˆ∆√◊G'VSµ˜&ˆw&÷÷Fñ567&ˆ∆≈6WDC◊W&f˜&÷Ê6RÊÊ˜rÇì∞¢V¬Á67&ˆ∆≈F˜◊6WGF∆VD÷É∞¢ˆ∆7E67&ˆ∆≈F˜÷V¬Á67&ˆ∆≈F˜µˆ∆7D÷W76vT6∆ñVÁDÜVñváC÷V¬Ê6∆ñVÁDÜVñváC∞¢ˆÊV$&˜GFˆ‘6˜VÁC”#∞¢˜67&ˆ∆≈ñÊÊVC◊G'VS∞¢–ß–¶gVÊ7Fñˆ‚˜67&ˆ∆ƒgFW$÷W76vU&VÊFW"á&W6W'fU67&ˆ∆¬¬67&ˆ∆≈6Ê6Ü˜Bó∞¢ÚÚFW&÷ñÊ¬7G&V“&VÊFW'26‚ÜV‚gFW"2Ê7FófU7G&V‘ñBó26∆V&VB‡¢ÚÚñ‚FÜB66R¬&W6W'fU67&ˆ∆¬6∑2FÜRÊ˜&÷¬ñ‚◊7FFRÜV«W"FÚFV6ñFS†¢ÚÚñÊÊVBW6W'27FíB&˜GFˆ”≤W6W'2vÜÚ÷ÁV∆«í67&ˆ∆∆VBWvWBFÜVó ¢ÚÚ&R◊&VÊFW"67&ˆ∆≈F˜&W7F˜&VBgFW"FÜRDÙ“&W∆6V÷VÁB‡¢ñbá&W6W'fU67&ˆ∆¬ó∞¢6ˆÁ7B&VFW$vîg&ˆ‘&˜GFˆ”“Ä¢67&ˆ∆≈6Ê6Ü˜Bb`¢ÁV÷&W"Êó4fñÊóFRÑÁV÷&W"á67&ˆ∆≈6Ê6Ü˜BÊ&˜GFˆ“ííb`¢ÁV÷&W"á67&ˆ∆≈6Ê6Ü˜BÊ&˜GFˆ“ì„#S ¢ì∞¢ÚÚ∂VW÷7FW"w2fˆ∆∆˜rÜWW&ó7Fñ2f˜"ñÊÊVBÚ7Fñ∆¬÷ÊV"÷&˜GFˆ“W6W'3†¢ÚÚˆfˆ∆∆˜t÷W76vW4gFW$Fˆ’&W∆6RÇíFˆW2dı$4TB67&ˆ∆≈FÙ&˜GFˆ“Çíá7ñÊ6á&ˆÊ˜W0¢ÚÚ&˜GFˆ“w&óFR≤f˜&6VB6WGF∆Rí¬6ÚFÜRfñÊ¬6WGF∆VB&W7ˆÁ6R6‚wB∆VfR¢ÚÚñÊÊVB&VFW"fWr∆ñÊW26Ü˜'B‚ˆÊ«ívVÁVñÊV«í◊67&ˆ∆∆VB◊WáVÁñÊÊVB¬Ê˜@¢ÚÚÊV"&˜GFˆ“íW6W'2f∆¬Fá&˜VvÇFÚ∂VWFÜVó"˜6óFñˆ‚ÊBvWBFÜP¢ÚÚÊWr÷÷W76vR7VR‚ÖW6ñÊr67&ˆ∆ƒñeñÊÊVBÇíÜW&RñÁ7FVBv˜V∆B6∂óFÜRf˜&6V@¢ÚÚw&óFRVÊ∆W72Fó7FÊ6S„SÊB∆WBFÜRDÙ“◊&V'Vñ∆B67&ˆ∆¬WfVÁB6Ê6V¬FÜP¢ÚÚFV∆ñVB6WGF∆W2(	B6ˆFWÇ4ı$R6F6Çˆ‚33c3‚ê¢ñbÇ&VFW$vîg&ˆ‘&˜GFˆ“bbˆ÷W76vUW6W%VÁñÊÊVBbbˆfˆ∆∆˜t÷W76vW4gFW$Fˆ’&W∆6RÇíí&WGW&„∞¢˜&W7F˜&T÷W76vU67&ˆ∆≈6Ê6Ü˜Bá67&ˆ∆≈6Ê6Ü˜Bì∞¢ˆ÷ñ&U6Ü˜tÊWt÷W76vU67&ˆ∆ƒ7VRá67&ˆ∆≈6Ê6Ü˜Bì∞¢&WGW&„∞¢–¢ñbÖ2Ê7FófU7G&V‘ñBó∞¢ÚÚ÷ñB◊7G&V“&R◊&VÊFW"áFˆˆ¬6ˆ◊∆WFñˆ‚¬7FófóGí◊66VÊR&Vg&W6Ç¬6∆&ñgíV6ÜÚí‡¢ÚÚ&VÊFW$÷W76vW2ÇívóW26◊6tñÊÊW"ÜñÊÊW"ÊñÊÊW$ÖD‘√“rríFÜV‚&V'Vñ∆G3≤FÜBvóP¢ÚÚ6ˆ∆∆6W267&ˆ∆ƒÜVñváBF˜v&BFÜRV◊Gí◊F&∆RÜVñváB¬ÊBFÜR'&˜w6W"ó2dı$4T@¢ÚÚFÚ6∆◊6÷W76vW2Á67&ˆ∆≈F˜F˜v‚FÚFÜRÊWrÜÊV"◊¶W&Úí÷Ç‚f˜"&VFW"vÜ¢ÚÚ67&ˆ∆∆VBUñÁFÚÜó7F˜'íáVÁñÊÊVBí¬67&ˆ∆ƒñeñÊÊVBÇíó2ÊÚ÷˜(	B6ÚóBFˆW2‰ı@¢ÚÚVÊFÚFÜB6∆◊¬ÊBFÜR&VFW"ó27G&ÊFVBBFÜRF˜áFÜR67&ˆ∆¬ßV◊÷&6≤í‚FÜP¢ÚÚvóR◊FÚ÷V◊Gí6∆◊ó2'&˜w6W"&ñ÷óFófRÜFWfñ6R÷vÊ˜7Fñ3≤•2ÊWfW"w&óFW2FÜP¢ÚÚ67&ˆ∆≈F˜í¬6ÚFÜR76ófRÊÚ÷˜6ÊÊ˜B&W6W'fR˜6óFñˆ‚ÜW&R‚&VÊFW$÷W76vW2Çê¢ÚÚ6GW&VB&R◊vóR6Ê6Ü˜Bf˜"WÜ7F«íFÜó266RÜóG267&ˆ∆≈6Ê6Ü˜BñÊóBfó&W0¢ÚÚvÜV‚ˆ÷W76vUW6W%VÁñÊÊVBí¬6Ú&W7F˜&RFÜRVÁñÊÊVB&VFW"w2fñWw˜'BñÁ7FVBˆ`¢ÚÚFÜRÊÚ÷˜‚ñÊÊVB˜Fñ¬÷fˆ∆∆˜vñÊr&VFW'2∂VW67&ˆ∆ƒñeñÊÊVBÇíÜ6˜'&V7B∆ófR÷fˆ∆∆˜rí‡¢ñbÖˆ÷W76vUW6W%VÁñÊÊVBbb67&ˆ∆≈6Ê6Ü˜Bó∞¢˜&W7F˜&T÷W76vU67&ˆ∆≈6Ê6Ü˜Bá67&ˆ∆≈6Ê6Ü˜Bì∞¢ˆ÷ñ&U6Ü˜tÊWt÷W76vU67&ˆ∆ƒ7VRá67&ˆ∆≈6Ê6Ü˜Bì∞¢&WGW&„∞¢–¢67&ˆ∆ƒñeñÊÊVBÇì∞¢&WGW&„∞¢–¢ÚÚ÷ÁV¬VÁñ‚ó27Fñ6∑ì¢ˆÊ6RFÜR&VFW"67&ˆ∆«2ví¬WFˆ÷Fñ2ñF∆RˆÊˆ‚–¢ÚÚ&W6W'fR&R◊&VÊFW'2◊W7B&W7F˜&RFÜVó"fñWw˜'B&FÜW"FÜ‚6∆V&ñÊrFÜP¢ÚÚVÁñ‚7FFRvóFÇ67&ˆ∆≈FÙ&˜GFˆ“Çí‚g&W6Ç6W76ñˆ‚∆ˆBÜÊ˜BVÁñÊÊVBí7Fñ∆¿¢ÚÚ∆ÊG2BFÜR&˜GFˆ“2WáV7FVB‚Ñ6ˆFWÇ3Cbfˆ∆∆˜r◊W‚ê¢ÚÚ&VÊFW$÷W76vW2Çí6GW&W2FÜR&R◊vóR6Ê6Ü˜Bf˜"FÜó266RFˆÚá6VRóG0¢ÚÚ67&ˆ∆≈6Ê6Ü˜BñÊóBí¬6Ú&W7F˜&ñÊrÜW&R∆ÊG2FÜR&VFW"vÜW&RFÜWívW&R‡¢ñbÖˆ÷W76vUW6W%VÁñÊÊVBó∞¢˜&W7F˜&T÷W76vU67&ˆ∆≈6Ê6Ü˜Bá67&ˆ∆≈6Ê6Ü˜Bì∞¢ˆ÷ñ&U6Ü˜tÊWt÷W76vU67&ˆ∆ƒ7VRá67&ˆ∆≈6Ê6Ü˜Bì∞¢&WGW&„∞¢–¢67&ˆ∆≈FÙ&˜GFˆ“Çì∞ß–†¶gVÊ7Fñˆ‚ˆ÷ñ&U&V6˜fW%fó'GV∆ó¶VD&∆ÊµfñWw˜'BÜ˜FñˆÁ2¬&W6W'fU67&ˆ∆¬¬fó'GV≈vñÊF˜ró∞¢ñbÇ&W6W'fU67&ˆ∆««¬fó'GV≈vñÊF˜w«¬fó'GV≈vñÊF˜rÁfó'GV∆ó¶VG«¬Ü˜FñˆÁ2bf˜FñˆÁ2Â˜fó'GVƒf∆∆&6≤íí&WGW&‚f«6S∞¢ñbÖˆ÷W76vUfñWw˜'DñÁFW'6V7G5&VÊFW&VE&˜rÇíí&WGW&‚f«6S∞¢ñbÖ˜6W76ñˆ‰áF÷ƒ66ÜU6ñBbe2Á6W76ñˆ‚be2Á6W76ñˆ‚Á6W76ñˆÂˆñC””’˜6W76ñˆ‰áF÷ƒ66ÜU6ñBó∞¢˜6W76ñˆ‰áF÷ƒ66ÜRÊFV∆WFRÖ˜6W76ñˆ‰áF÷ƒ66ÜU6ñBì∞¢–¢ˆ÷W76vUfó'GV≈vñÊF˜t∂Wì“rs∞¢&VÊFW$÷W76vW2á∑&W6W'fU67&ˆ∆√ßG'VR≈˜fó'GVƒf∆∆&6≥ßG'VW“ì∞¢&WGW&‚G'VS∞ß–†¢ÚÚ3c3CS¢'6RFÜR7ñÁFÜWFñ2v∂WW&ˆGí&6≤ñÁFÚFó7∆ífñV∆G2‚÷ó'&˜'2FÜP¢ÚÚGvÚ7G'V7GW&VBíˆ&6∂w&˜VÊE˜&ˆ6W72Êf˜&÷E˜v∂WW˜&ˆ◊B6ÜW2áñÊÊVB'ê¢ÚÚFW7G2˜FW7Eˆ&6∂w&˜VÊE˜&ˆ6W75˜v∂WWˆf˜&÷BÁíì≤˜FÜW"WfVÁB∂ñÊG2&WGW&‡¢ÚÚÁV∆¬ÊB∂VWFÜR&r÷Ê˜Fñ6Rf∆∆&6≤‡¶gVÊ7Fñˆ‚˜'6U&ˆ6W75v∂WW&ˆGíáFWáBó∞¢6ˆÁ7B3’7G&ñÊráFWáG«¬rrì∞¢ÚÚÜVFW"w&˜W2&R6ñÊv∆R÷∆ñÊR'íw&÷÷#≤FÜR˜WGWBw&˜W6GW&W2FÜP¢ÚÚ&W7BfW&&Fñ“Ü∆VFñÊrñÊFVÁFFñˆ‚ÚG&ñ∆ñÊr&∆Ê≤∆ñÊW2&W6W'fVBí‚FÜP¢ÚÚvF6Ç7W&W76ñˆ‚Ê˜FRó2ñÁFVÁFñˆÊ∆«í‰ıB7∆óB˜WBˆbFÜR˜WGWB(	B&V¿¢ÚÚ&ˆ6W72˜WGWB6‚6ˆÁFñ‚FÜRñFVÁFñ6¬FWáB¬6Ú7G&óñÊróBv˜V∆BG&˜ ¢ÚÚ∆VvóFñ÷FR6ˆÁFVÁBÇ3c3S&WfñWrfñÊFñÊr"í‚óB&ñFW2∆ˆÊrñ‚˜WGWF‡¢∆WB”◊2Ê÷F6ÇÇıÂ≈¥î’ı%DÂC¢&6∂w&˜VÊB&ˆ6W72ÖµÂ∆Â“£Úí6ˆ◊∆WFVB¬ÜWÜóEˆ6ˆFS“Öµ‚ï∆Â“¢ï¬ï¬Â∆‰6ˆ÷÷ÊC¢ÖµÂ∆Â“¢ï∆‰˜WGWC•∆‚Öµ«5≈5“¢ï≈“BÚì∞¢ñbÜ“í&WGW&‚∑GóS¢v6ˆ◊∆WFñˆ‚r«F6¥ñC¶’≥“∆WÜóD6ˆFS¶’≥%“∆6ˆ÷÷ÊC¶’≥5“∆˜WGWC¶’≥E“«GFW&„¶ÁV∆«”∞¢”◊2Ê÷F6ÇÇıÂ≈¥î’ı%DÂC¢&6∂w&˜VÊB&ˆ6W72ÖµÂ∆Â“£Úí÷F6ÜVBvF6ÇGFW&‚"Ç‚¢í%¬Â∆‰6ˆ÷÷ÊC¢ÖµÂ∆Â“¢ï∆‰÷F6ÜVB˜WGWC•∆‚Öµ«5≈5“¢ï≈“BÚì∞¢ñbÜ“í&WGW&‚∑GóS¢wvF6Öˆ÷F6Çr«F6¥ñC¶’≥“«GFW&„¶’≥%“∆6ˆ÷÷ÊC¶’≥5“∆˜WGWC¶’≥E“∆WÜóD6ˆFS¶ÁV∆«”∞¢&WGW&‚ÁV∆√∞ß–¢ÚÚ6W'fW"◊7F◊VB˜v∂WWˆ÷WFÜWFÜ˜&óFFófRvÜV‚&W6VÁBí÷W&vVB˜fW"FÜP¢ÚÚ6∆ñVÁB'6S≤FÜR˜WGWB6V7Fñˆ‚ˆÊ«íWfW"6ˆ÷W2g&ˆ“FÜR'6R&V6W6RFÜP¢ÚÚ÷WFFV∆ñ&W&FV«í6'&ñW2ÜVFW"fñV∆G2ˆÊ«í‡¶gVÊ7Fñˆ‚˜&ˆ6W75v∂WWñÊfÚÜ“¬FWáBó∞¢6ˆÁ7B'6VC’˜'6U&ˆ6W75v∂WW&ˆGíáFWáBì∞¢6ˆÁ7B÷WF“Ü“bf“Â˜v∂WWˆ÷WFbgGóVˆb“Â˜v∂WWˆ÷WF””“vˆ&¶V7Brìˆ“Â˜v∂WWˆ÷WF¶ÁV∆√∞¢ñbÇ'6VBbb÷WFí&WGW&‚ÁV∆√∞¢6ˆÁ7Bñ6≥“Ü÷WF∂Wí«'6VD∂Wíì”Á∞¢ñbÜ÷WFbf÷WF∂÷WF∂Wï“÷ÁV∆¬í&WGW&‚÷WF∂÷WF∂Wï”∞¢&WGW&‚'6VC˜'6VE∑'6VD∂Wï”¶ÁV∆√∞¢”∞¢&WGW&‚∞¢GóS•7G&ñÊráñ6≤ÇwGóRr¬wGóRró«¬rrí¿¢F6¥ñC•7G&ñÊráñ6≤ÇwF6µˆñBr¬wF6¥ñBró«¬rrí¿¢6ˆ÷÷ÊC•7G&ñÊráñ6≤Çv6ˆ÷÷ÊBr¬v6ˆ÷÷ÊBró«¬rrí¿¢WÜóD6ˆFSßñ6≤ÇvWÜóEˆ6ˆFRr¬vWÜóD6ˆFRrí¿¢GFW&„ßñ6≤ÇwGFW&‚r¬wGFW&‚rí¿¢˜WGWCß'6VC˜'6VBÊ˜WGWC¶ÁV∆¬¿¢”∞ß–¶gVÊ7Fñˆ‚˜&ˆ6W75v∂WW6&DáF÷¬ÜñÊfÚ¬&uFWáB¬WáG&2ó∞¢6ˆÁ7Bó5vF6É÷ñÊfÚÁGóS””“wvF6Öˆ÷F6Çs∞¢6ˆÁ7BWÜóE7G#÷ñÊfÚÊWÜóD6ˆFS”÷ÁV∆√Úrs•7G&ñÊrÜñÊfÚÊWÜóD6ˆFRì∞¢ÚÚ6ñvÊ¬÷∂ñ∆∆VB&ˆ6W76W2&W˜'BÊVvFófRWÜóB6ˆFW2á7V'&ˆ6W72&WGW&Ê6ˆFRí‡¢6ˆÁ7BWÜóD∂Ê˜v„“ı‚”ı∆B≤BÚÁFW7BÜWÜóE7G"ì∞¢6ˆÁ7BWÜóDˆ≥÷WÜóE7G#””“ss∞¢∆WB6Üó∞¢ñbÜó5vF6Çó∞¢6Üó÷«7‚6∆73“'&ˆ6W72◊v∂WW÷6ÜóvF6Ç"FóF∆S“"G∂W62áBÇw&ˆ6W75˜v∂WWˆ÷F6ÜVBríó“#‚G∂∆íÇvWñRr√ó”∆6ˆFRFóF∆S“"G∂W62Ö7G&ñÊrÜñÊfÚÁGFW&Á«¬rríó“#‚G∂W62Ö7G&ñÊrÜñÊfÚÁGFW&Á«¬rríó”¬ˆ6ˆFS„¬˜7„Ê∞¢÷V«6W∞¢6ˆÁ7B6«3÷WÜóDˆ≥Úvˆ≤s¢ÜWÜóD∂Ê˜v„Úvfñ¬s¢vÊWWG&¬rì∞¢6ˆÁ7Bñ6ˆ„÷WÜóDˆ≥ˆ∆íÇv6ÜV6≤r√ì¢ÜWÜóD∂Ê˜v„ˆ∆íÇwÇr√ì¢rrì∞¢6Üó÷«7‚6∆73“'&ˆ6W72◊v∂WW÷6ÜóG∂6«7“#‚G∂ñ6ˆÁ”«7„ÊWÜóBG∂W62ÜWÜóE7G'«¬sÚró”¬˜7„„¬˜7„Ê∞¢–¢6ˆÁ7B6÷DáF÷√÷ñÊfÚÊ6ˆ÷÷ÊCˆ∆6ˆFR6∆73“'&ˆ6W72◊v∂WW÷6÷B"FóF∆S“"G∂W62ÜñÊfÚÊ6ˆ÷÷ÊBó“#‚G∂W62ÜñÊfÚÊ6ˆ÷÷ÊBó”¬ˆ6ˆFSÊ¢rs∞¢ÚÚ&W6W'fR˜WGWB'óFR÷f˜"÷'óFRf˜"FÜR«&S„≤G&ñ“Ù‰≈íf˜"FÜP¢ÚÚV◊GíˆÊˆ‚÷V◊GíFV6ó6ñˆ‚6Ú∆VFñÊrñÊFVÁFFñˆ‚ÊBG&ñ∆ñÊr&∆Ê≤∆ñÊW0¢ÚÚ7W'fófRÇ3c3S&WfñWrfñÊFñÊrí‡¢6ˆÁ7B˜WE&s÷ñÊfÚÊ˜WGWB÷ÁV∆√ı7G&ñÊrÜñÊfÚÊ˜WGWBì•7G&ñÊrá&uFWáG«¬rrì∞¢6ˆÁ7B˜WDáF÷√÷˜WE&rÁG&ñ“Çìˆ«&R6∆73“'&ˆ6W72◊v∂WW◊FWáB#‚G∂W62Ü˜WE&ró”¬˜&SÊ¢rs∞¢6ˆÁ7B6÷E&˜s÷ñÊfÚÊ6ˆ÷÷ÊCˆ∆Fób6∆73“'&ˆ6W72◊v∂WW÷6÷B◊&˜r#„∆6ˆFS‚G∂W62ÜñÊfÚÊ6ˆ÷÷ÊBó”¬ˆ6ˆFS„¬ˆFócÊ¢rs∞¢ÚÚFÜR6ˆ∆∆6VBvF6Ç6ÜóG'VÊ6FW2FÜRGFW&„≤7W&f6RFÜRgV∆¬¿¢ÚÚw&ñÊrf«VRñ‚FÜRWáÊFVBFWFñ¬6ÚF˜V6Çˆ∂Wñ&ˆ&BW6W'26‚&VBó@¢ÚÚvóFÜ˜WB&V«ññÊrˆ‚Ü˜fW"Fˆˆ«FóÇ3c3S&WfñWrfñÊFñÊrBí‡¢6ˆÁ7BGFW&Â&˜s“Üó5vF6ÇbfñÊfÚÁGFW&‚ìˆ∆Fób6∆73“'&ˆ6W72◊v∂WW◊GFW&‚◊&˜r#„«7‚6∆73“'&ˆ6W72◊v∂WW÷FWFñ¬÷∂Wí#‚G∂W62áBÇw&ˆ6W75˜v∂WWˆ÷F6ÜVBríó”¬˜7„„∆6ˆFS‚G∂W62Ö7G&ñÊrÜñÊfÚÁGFW&‚íó”¬ˆ6ˆFS„¬ˆFócÊ¢rs∞¢&WGW&‚∆FWFñ«26∆73“'&ˆ6W72◊v∂WW÷6&B#„«7V÷÷'í6∆73“'&ˆ6W72◊v∂WW◊7V÷÷'í#„«7‚6∆73“'&ˆ6W72◊v∂WW◊Fˆvv∆R#‚G∂∆íÇv6ÜWg&ˆ‚◊&ñváBr√"ó”¬˜7„„«7‚6∆73“'&ˆ6W72◊v∂WW÷∆&V¬#‚G∂∆íÇwFW&÷ñÊ¬r√2ó”«7„‚G∂W62áBÇw&ˆ6W75˜v∂WWˆ∆&V¬ríó”¬˜7„„¬˜7„‚G∂6÷DáF÷«“G∂6Üó“G∂WáG&2ÁFñ÷TáF÷««¬rw”¬˜7V÷÷'ì„∆Fób6∆73“'&ˆ6W72◊v∂WW÷FWFñ¬#‚G∂WáG&2Êfñ∆W4áF÷««¬rw“G∑GFW&Â&˜w“G∂6÷E&˜w”∆Fób6∆73“&◊6r÷&ˆGí&ˆ6W72◊v∂WW÷&ˆGí#‚G∂˜WDáF÷«”¬ˆFóc‚G∂WáG&2Êfˆ˜DáF÷««¬rw”¬ˆFóc„¬ˆFWFñ«3Ê∞ß–†¢ÚÚ3#S¢'6RñÁFÚ«FV◊∆FS‚ÊB÷˜fRFÜRÊˆFW2ñÁ7FVBˆbñÁ6W'DF¶6VÁDÖD‘¬(	@¢ÚÚWfW'í7FWó2ñFV◊˜FVÁB¬6ÚDÙ“‘íw&W"ÜRÊr‚‚ÁFí÷fñÊvW'&ñÁFñÊp¢ÚÚWáFVÁ6ñˆ‚íFÜBWÜV7WFW2FÜR6∆¬Gvñ6R6ÊÊ˜BGW∆ñ6FRFÜR&∆ˆ6≤‡¶gVÊ7Fñˆ‚ˆñÁ6W'E6Vv÷VÁD&∆ˆ6≤á6Vr¬áF÷¬ó∞¢ñbÇ6Vrí&WGW&„∞¢ñbáGóVˆbFˆ7V÷VÁB”“wVÊFVfñÊVBrbgGóVˆbFˆ7V÷VÁBÊ7&VFTV∆V÷VÁC””“vgVÊ7Fñˆ‚ró∞¢G'ó∞¢6ˆÁ7BG√÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇwFV◊∆FRrì∞¢ñbÇv6ˆÁFVÁBrñ‚G¬ó∞¢G¬ÊñÊÊW$ÖD‘√÷áF÷√∞¢6VrÊVÊD6Üñ∆BáG¬Ê6ˆÁFVÁBì∞¢&WGW&„∞¢–¢÷6F6ÇÖÚó≤Ú¢f∆¬Fá&˜VvÇFÚFÜR7G&ñÊrFÇ&V∆˜r¢Ú–¢–¢6VrÊñÁ6W'DF¶6VÁDÖD‘¬Çv&Vf˜&VVÊBr¬áF÷¬ì∞ß–¶gVÊ7Fñˆ‚&VÊFW$÷W76vW2Ü˜FñˆÁ2ó∞¢ˆ∆7D÷W76vU&VÊFW$C◊W&f˜&÷Ê6RÊÊ˜rÇì∞¢ÚÚGóVˆbwV&C¢ÊˆFRÜ&ÊW76W2WáG&7B&VÊFW$÷W76vW2ÇívóFÜ˜WBóG2ÜV«W'2Ç3csrí‡¢ñbÇÜ˜FñˆÁ2bf˜FñˆÁ2ÂˆñÁFW&Êƒ÷V7W&V÷VÁBíbbGóVˆb˜&W6WD÷W76vUfó'GVƒ÷V7W&V÷VÁD'W'7C””“vgVÊ7Fñˆ‚ró≤˜&W6WD÷W76vUfó'GVƒ÷V7W&V÷VÁD'W'7BÇì≤–¢6ˆÁ7B&W6W'fU67&ˆ∆√“Ü˜FñˆÁ2bf˜FñˆÁ2Á&W6W'fU67&ˆ∆¬ì∞¢6ˆÁ7Bfó'GVƒf∆∆&6≥“Ü˜FñˆÁ2bf˜FñˆÁ2Â˜fó'GVƒf∆∆&6≤ì∞¢ÚÚ6GW&RFÜR&R◊vóR67&ˆ∆¬˜6óFñˆ‚vÜV‚&W6W'fñÊrı"vÜV‚FÜR&VFW"Ü0¢ÚÚ÷ÁV∆«íVÁñÊÊVC≤&˜FÇÊVVBFÚ&W7F˜&RFÜR&VFW"w2˜6óFñˆ‚gFW"FÜRDÙ–¢ÚÚ&V'Vñ∆B&FÜW"FÜ‚6ÊFÚFÜR&˜GFˆ“‚Ñ6ˆFWÇ3Cb#2fˆ∆∆˜r◊W‚ê¢6ˆÁ7B67&ˆ∆≈6Ê6Ü˜C“á&W6W'fU67&ˆ∆««≈ˆ÷W76vUW6W%VÁñÊÊVBìıˆ6GW&T÷W76vU67&ˆ∆≈6Ê6Ü˜BÇì¶ÁV∆√∞¢6ˆÁ7BñÊÊW#“BÇv◊6tñÊÊW"rì∞¢6ˆÁ7B6ñC’2Á6W76ñˆ„ı2Á6W76ñˆ‚Á6W76ñˆÂˆñC¶ÁV∆√∞¢ñbÇ2Ê'W7íbd'&íÊó4'&íÖ2Ê÷W76vW2íbgGóVˆbˆáñG&FTñD∆ñÊ∂VDÜó7F˜&ñ6≈Fˆˆ≈66VÊW3””“vgVÊ7Fñˆ‚ró∞¢6ˆÁ7B7FófóGî÷ˆFS◊GóVˆb6ÜD7FófóGî÷ˆFS””“vgVÊ7Fñˆ‚sˆ6ÜD7FófóGî÷ˆFRÇì¢v6ˆ◊7E˜v˜&∂∆ˆrs∞¢ˆáñG&FTñD∆ñÊ∂VDÜó7F˜&ñ6≈Fˆˆ≈66VÊW2Ö2Ê÷W76vW2«∑6W76ñˆ‰ñCß6ñB∆÷ˆFS¶7FófóGî÷ˆFW“ì∞¢–¢6ˆÁ7B◊6t6˜VÁC’2Ê÷W76vW2Ê∆VÊwFÉ∞¢ÚÚGW&ñÊr6W76ñˆ‚7vóF6Ç¬2Ê÷W76vW2ó2ñÁFVÁFñˆÊ∆«í6∆V&VBvÜñ∆RFÜRgV∆¿¢ÚÚ÷W76vRfWF6Çó27Fñ∆¬ñ‚f∆ñváB‚˜FÜW"7ñÊ2WFFW26‚7Fñ∆¬6∆¿¢ÚÚ&VÊFW$÷W76vW2Çíñ‚FÜó2vñÊF˜r‚∂VWFÜRWÜó7FñÊr∆ˆFñÊr∆6VÜˆ∆FW"‡¢ñbÖˆ∆ˆFñÊu6W76ñˆ‰ñC””◊6ñBbf◊6t6˜VÁC”””bfñÊÊW"í&WGW&„∞¢ñbá6ñB”’ˆ÷W76vU&VÊFW%vñÊF˜u6ñBí˜&W6WD÷W76vU&VÊFW%vñÊF˜rá6ñBì∞¢∆WB66ÜVE&VÊFW%6ñvÊGW&S÷ÁV∆√∞¢6ˆÁ7BÜ5G&Á6ñVÁEG&Á67&óEVì“Ä¢ávñÊF˜rÂˆ6ˆ◊&W76ñˆÂVíbbÇvñÊF˜rÂˆ6ˆ◊&W76ñˆÂVíÁ6W76ñˆ‰ñG««vñÊF˜rÂˆ6ˆ◊&W76ñˆÂVíÁ6W76ñˆ‰ñC””◊6ñBíí«¿¢ávñÊF˜rÂˆÜÊFˆfeVíbbÇvñÊF˜rÂˆÜÊFˆfeVíÁ6W76ñˆ‰ñG««vñÊF˜rÂˆÜÊFˆfeVíÁ6W76ñˆ‰ñC””◊6ñBíê¢ì∞†¢6ˆÁ7B&W6W'fVD6ˆ◊&W76ñˆÂF6¥÷W76vW3’ˆ∆FW7E&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D÷W76vW2Ö2Ê÷W76vW2ì∞¢6ˆÁ7Bfó5vóFÑñGÉ’ˆvWEfó6ñ&∆T÷W76vW5vóFÑñGÇÇì∞¢BÇvV◊Gï7FFRríÁ7Gñ∆RÊFó7∆ì“áfó5vóFÑñGÇÊ∆VÊwFá««&W6W'fVD6ˆ◊&W76ñˆÂF6¥÷W76vW2Ê∆VÊwFÇìÚvÊˆÊRs¢rs∞¢6ˆÁ7Bfó'GV≈vñÊF˜s◊fó'GVƒf∆∆&6∞¢Ú∑fó'GV∆ó¶VC¶f«6R«7F'C£∆VÊCßfó5vóFÑñGÇÊ∆VÊwFÇ«F˜C£∆&˜GFˆ’C£«F˜F√ßfó5vóFÑñGÇÊ∆VÊwFÇ«Fñ≈7F'Cßfó5vóFÑñGÇÊ∆VÊwFá–¢¢ˆ7W'&VÁD÷W76vUfó'GV≈vñÊF˜ráfó5vóFÑñGÇ≈ˆ÷W76vUfó'GVƒ∂VWFñƒ6˜VÁBÇíì∞¢6ˆÁ7B&VÊFW%vñÊF˜t∂Wì’ˆ÷W76vUfó'GV≈vñÊF˜t∂Wîf˜"áfó'GV≈vñÊF˜rì∞¢6ˆÁ7BvñÊF˜u7F'C◊fó'GV≈vñÊF˜rÁ7F'C∞¢6ˆÁ7BvñÊF˜tVÊC◊fó'GV≈vñÊF˜rÊVÊC∞¢6ˆÁ7B&VÊFW$ÜVEfó5vóFÑñGÉ◊fó5vóFÑñGÇÁ6∆ñ6RávñÊF˜u7F'B¬vñÊF˜tVÊBì∞¢6ˆÁ7B&VÊFW%Fñ≈7F'C◊fó'GV≈vñÊF˜rÁfó'GV∆ó¶VCÙ÷FÇÊ÷ÇávñÊF˜tVÊB¬fó'GV≈vñÊF˜rÁFñ≈7F'BìßvñÊF˜tVÊC∞¢6ˆÁ7B&VÊFW%Fñ≈fó5vóFÑñGÉ◊fó'GV≈vñÊF˜rÁfó'GV∆ó¶VBbg&VÊFW%Fñ≈7F'C«fó5vóFÑñGÇÊ∆VÊwFÄ¢Úfó5vóFÑñGÇÁ6∆ñ6Rá&VÊFW%Fñ≈7F'Bê¢¢µ”∞¢6ˆÁ7B&VÊFW%fó5vóFÑñGÉ◊&VÊFW$ÜVEfó5vóFÑñGÇÊ6ˆÊ6Bá&VÊFW%Fñ≈fó5vóFÑñGÇì∞¢6ˆÁ7B&VÊFW%fó6ñ&∆TñGá3’∞¢‚‚Á&VÊFW$ÜVEfó5vóFÑñGÇÊ÷ÇÖÚ∆ñGÇì”ÁvñÊF˜u7F'B∂ñGÇí¿¢‚‚Á&VÊFW%Fñ≈fó5vóFÑñGÇÊ÷ÇÖÚ∆ñGÇì”Á&VÊFW%Fñ≈7F'B∂ñGÇí¿¢”∞¢6ˆÁ7BÜVE&VÊFW$6˜VÁC◊&VÊFW$ÜVEfó5vóFÑñGÇÊ∆VÊwFÉ∞†¢ÚÚf7BFÉ¢7vóF6ÜñÊr&6≤FÚ&Wfñ˜W6«í&VÊFW&VB6W76ñˆ‚vóFÇ6÷R6˜VÁB‡¢ÚÚwV&C¢6ñB”“˜6W76ñˆ‰áF÷ƒ66ÜU6ñBVÁ7W&W2ñ‚◊6W76ñˆ‚WFFW2ÜVFóG2¿¢ÚÚÊWr÷W76vW2¬Fˆˆ≈ˆ6ˆ◊∆WFRí«vó2vWBg&W6Ç&V'Vñ∆B‡¢ÚÚ6∂ó66ÜRñbFÜó26W76ñˆ‚ó27Fñ∆¬7G&V÷ñÊr(	BFÜR∆ófR6÷B'6W"w&óFW0¢ÚÚñÁFÚDÙ“ÊˆFRñÁ6ñFRFÜR66ÜVB7V'G&VS≤6W'fñÊr66ÜVBÖD‘¬FWF6ÜW2óB‡¢ÚÚ«6Ú6∂ó66ÜRf˜"G&Á6ñVÁBG&Á67&óB6&G27V6Ç2ˆ6ˆ◊&W72Ê@¢ÚÚ7&˜72÷6ÜÊÊV¬ÜÊFˆfb7V÷÷&ñW3≤˜FÜW'vó6RFÜR66ÜVBG&Á67&óB&WGW&Á0¢ÚÚ&Vf˜&RFÜ˜6R6&G26‚&RñÁ6W'FVB‡¢ñbá6ñBbg6ñB”’˜6W76ñˆ‰áF÷ƒ66ÜU6ñBbbî‰dƒîtÖE∑6ñE“bbÜ5G&Á6ñVÁEG&Á67&óEVíó∞¢6ˆÁ7B&VÊFW%6ñvÊGW&S’ˆ÷W76vU&VÊFW$66ÜU6ñvÊGW&RÇì∞¢66ÜVE&VÊFW%6ñvÊGW&S◊&VÊFW%6ñvÊGW&S∞¢6ˆÁ7B66ÜVC’˜6W76ñˆ‰áF÷ƒ66ÜRÊvWBá6ñBì∞¢ñbÜ66ÜVBbf66ÜVBÊ◊6t6˜VÁC””÷◊6t6˜VÁBbf66ÜVBÁ&VÊFW%vñÊF˜t∂Wì””◊&VÊFW%vñÊF˜t∂Wíbf66ÜVBÁ6ñvÊGW&S””◊&VÊFW%6ñvÊGW&Ró∞¢ñÊÊW"ÊñÊÊW$ÖD‘√÷66ÜVBÊáF÷√∞¢ˆ÷W76vUfó'GV≈vñÊF˜t∂Wì◊&VÊFW%vñÊF˜t∂Wì∞¢˜6W76ñˆ‰áF÷ƒ66ÜU6ñC◊6ñC∞¢˜&VáñG&FUG&Á7&VÁE7G&V‘Fˆ“ÜñÊÊW"ì∞¢˜&VáñG&FTFVfW'&VEv˜&∂∆ˆw4g&ˆ‘66ÜRÜñÊÊW"ì∞¢˜vó&T÷W76vUvñÊF˜t∆ˆDV&∆ñW$'WGFˆ‚Çì∞¢ñbáGóVˆbˆ«ï6W76ñˆ‰ÊfñvFñˆÂ&Vg3””“vgVÊ7Fñˆ‚ríˆ«ï6W76ñˆ‰ÊfñvFñˆÂ&Vg2Çì∞¢˜67&ˆ∆ƒgFW$÷W76vU&VÊFW"á&W6W'fU67&ˆ∆¬¬67&ˆ∆≈6Ê6Ü˜Bì∞¢ñbÖˆ÷ñ&U&V6˜fW%fó'GV∆ó¶VD&∆ÊµfñWw˜'BÜ˜FñˆÁ2¬&W6W'fU67&ˆ∆¬¬fó'GV≈vñÊF˜ríí&WGW&„∞¢˜WFFT÷W76vUfó'GVƒ÷V7W&V÷VÁG2á&VÊFW%fó5vóFÑñGÇ¬&VÊFW%fó6ñ&∆TñGá2¬fó'GV≈vñÊF˜rì∞¢&WVW7DÊñ÷Fñˆ‰g&÷RÇÇì”Â˜˜7E&ˆ6W75vóFÑÊ6Ü˜%7W&W76ñˆ‚ÜñÊÊW"íì∞¢ñbáGóVˆbˆñÊóD÷VFñ∆ñ&6¥ˆ'6W'fW#””“vgVÊ7Fñˆ‚ríˆñÊóD÷VFñ∆ñ&6¥ˆ'6W'fW"Çì∞¢ñbáGóVˆb∆ˆEFˆF˜3””“vgVÊ7Fñˆ‚rbfFˆ7V÷VÁBÊvWDV∆V÷VÁD'îñBÇwÊV≈FˆF˜2ríbfFˆ7V÷VÁBÊvWDV∆V÷VÁD'îñBÇwÊV≈FˆF˜2ríÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv7FófRríó∂∆ˆEFˆF˜2Çì∑–¢&WGW&„∞¢–¢–¢ÚÚ÷ñB◊7G&V“f∆ñ6∂W"fóÇÇ33Ésrì¢vÜV‚&VÊFW$÷W76vW2Çí&V'Vñ∆Bó2&V6ÜV@¢ÚÚvÜñ∆RDÑï26W76ñˆ‚ó27FófV«í7G&V÷ñÊrÜRÊr‚FÜR6∆&ñgí◊&W7ˆÁ6RV6ÜÚ@¢ÚÚ÷W76vW2Êß2¬˜"4ƒí÷ñ◊˜'B&Vg&W6Çí¬FÜRñÊÊW"ÊñÊÊW$ÖD‘√“rv&V∆˜rFWF6ÜW0¢ÚÚFÜR∆ófR6∆ófT76ó7FÁEGW&ÊÊˆFR(	BÊBFÜR6÷B'6W"∂VW2w&óFñÊrñÁF¢ÚÚFÜBÊ˜r÷˜'ÜÊVBÊˆFR¬6ÚFÜR7G&V÷VBFWáBfÊó6ÜW2VÁFñ¬FÜRÊWáB7G&V–¢ÚÚWfVÁB&V'Vñ∆G2FÜRGW&‚Ç&Fó6V'2¬FÜV‚&VV'2"í‚6GW&RFÜR∆ófP¢ÚÚGW&‚w27GV¬DÙ“ÊˆFRÜÊ˜BóG2ÖD‘¬(	BFÜR'6W"Üˆ∆G2∆ófR&VfW&VÊ6RñÁF¢ÚÚóBí6ÚóB6‚&R&R÷GF6ÜVBgFW"FÜR&V'Vñ∆B¬∂VWñÊrFÜR'6W"F&vW@¢ÚÚ6ˆÊÊV7FVBÊBFÜR7G&V÷VBFWáBfó6ñ&∆R‚ˆÊ«íf˜"FÜR7G&V÷ñÊr6W76ñˆ‚w2˜v‡¢ÚÚ∆ófRGW&„≤ÊWfW"ffV7G26WGF∆VBG&Á67&óG2‡¢∆WB˜&W6W'fVD∆ófUGW&„÷ÁV∆√∞¢ñbá6ñBbdî‰dƒîtÖE∑6ñE“ó∞¢6ˆÁ7Bˆ«C÷Fˆ7V÷VÁBÊvWDV∆V÷VÁD'îñBÇv∆ófT76ó7FÁEGW&‚rì∞¢ñbÖˆ«BbbÇˆ«BÊFF6WG«¬ˆ«BÊFF6WBÁ6W76ñˆ‰ñG«≈ˆ«BÊFF6WBÁ6W76ñˆ‰ñC””◊6ñBíó∞¢ÚÚ∆ófR◊GW&‚&W6W'fFñˆ‚&WVó&W2$ıd$ƒR∆ófR˜vÊW"(	BÊWfW"&&RDÙ–¢ÚÚ6ˆÁFVÁB‚Ç3cìCÇíFÜR∆ófRGW&‚ó2&W6W'fVB7&˜72FÜRvóRˆÊ«ívÜñ∆P¢ÚÚÜíFÜR7G&V“ó2vVÁVñÊV«í7FófRÖ2Ê7FófU7G&V‘ñB(	BFÜR33Ésp¢ÚÚ÷ñB◊7G&V“f∆ñ6∂W"66RFÜó2&W6W'fRv2w&óGFV‚f˜"í¬˜"Ü"íFÜP¢ÚÚ7W'&VÁB÷W76vR&ˆ¶V7Fñˆ‚Ö2Ê÷W76vW2í7Fñ∆¬6'&ñW2Wá∆ñ6ó@¢ÚÚ∆ófR÷76ó7FÁBWfñFVÊ6R(	B6∆ñVÁB◊6ñFRˆ∆ófRÚˆ7FófóGî'W'7DñB¢ÚÚˆ∆ófU6Vv÷VÁE6W÷&∂W"÷W&vVBñ‚g&ˆ“FÜRî‰dƒîtÖBFñ¬˜"6W'fW ¢ÚÚ¶˜W&Ê¬6Ê6Ü˜BáFÜR&V6ˆÊÊV7BÚFW&÷ñÊ¬◊&ˆ¶V7Fñˆ‚66Rí‚6WGF∆V@¢ÚÚG&Á67&óBÜ2ÊVóFÜW"¬6Ú6ˆÁFVÁFgV¬'WBDTB∆ófRÊˆFRá7G&V–¢ÚÚVÊFVB(	B2Ê7FófU7G&V‘ñB6∆V&VB(	BvÜñ∆Rî‰dƒîtÖE∑6ñE“v2Ê˜BñW@¢ÚÚ6∆VÊVBíó2ÊÚ∆ˆÊvW"&W6W'fVC¢&R÷GF6ÜñÊróB˜fW"FÜR6WGF∆V@¢ÚÚG&Á67&óBñÊÊVB6V6ˆÊB6˜íˆbFÜR6÷R76ó7FÁB÷W76vRÇ3cìCÉ∞¢ÚÚFFv2«vó26∆V‚(	B7FFRÊF"¬6ñFV6"¬ÊBˆí˜6W76ñˆ‚V6ÇÜˆ∆@¢ÚÚˆÊR&˜s≤FÜRGW∆ñ6FRWÜó7FVBˆÊ«íñ‚FÜR&VÊFW&VBDÙ“í‚FÜR3S3ì ¢ÚÚ&∆Ê≤◊GW&‚wV&BéZ˚ûä˘ﬁkhéZKíó2&W6W'fVC¢FVBT’Eí6ÜV∆¬Ü2ÊÚ∆ófP¢ÚÚ&ˆ¶V7Fñˆ‚VóFÜW"¬6ÚóBó27Fñ∆¬G&˜VBvóFÇFÜRvóRñÁ7FVBˆ`¢ÚÚñÊÊñÊr‚fF"÷ˆÊ«í&∆Ê≤GW&‚˜fW"FÜR6WGF∆VBÁ7vW"‡¢6ˆÁ7BˆÜ4∆ófT76ó7FÁE&ˆ¶V7Fñˆ„‘'&íÊó4'&íÖ2Ê÷W76vW2íbe2Ê÷W76vW2Á6ˆ÷RÜ””‡¢“bf“Á&ˆ∆S””“v76ó7FÁBrbbÜ“Âˆ∆ófW«∆“Âˆ7FófóGî'W'7DñB”◊VÊFVfñÊVG«∆“Âˆ∆ófU6Vv÷VÁE6W”◊VÊFVfñÊVBê¢ì∞¢ñbÖ2Ê7FófU7G&V‘ñB«¬ˆÜ4∆ófT76ó7FÁE&ˆ¶V7Fñˆ‚ó∞¢˜&W6W'fVD∆ófUGW&„’ˆ«C∞¢–¢–¢–¢6ˆÁ7B6ˆ◊&W76ñˆÂ7FFS“ÇÇì”Á∞¢∆WB6ˆ◊&W76ñˆÂ7FFS’ˆ6ˆ◊&W76ñˆÂ7FFTf˜$7W'&VÁE6W76ñˆ‚Çì∞¢ñbÇ2Ê'W7íbb6ˆ◊&W76ñˆÂ7FFRbb6ˆ◊&W76ñˆÂ7FFRÊWFˆ÷Fñ2ó∞¢vñÊF˜rÂˆ6ˆ◊&W76ñˆÂVì÷ÁV∆√∞¢ˆ6∆V$6ˆ◊&W76ñˆ‰V∆6VEFñ÷W"Çì∞¢˜6WD6ˆ◊&W76ñˆÂ6W76ñˆ‰∆ˆ6≤ÜÁV∆¬ì∞¢6ˆ◊&W76ñˆÂ7FFS÷ÁV∆√∞¢–¢&WGW&‚6ˆ◊&W76ñˆÂ7FFS∞¢“íÇì∞¢ñbávñÊF˜rÂˆ6ˆ◊&W76ñˆÂVíbb6ˆ◊&W76ñˆÂ7FFRí6∆V$6ˆ◊&W76ñˆÂVíÇì∞¢6ˆÁ7BÜÊFˆfe7FFS’ˆÜÊFˆfe7FFTf˜$7W'&VÁE6W76ñˆ‚Çì∞¢ñbávñÊF˜rÂˆÜÊFˆfeVíbbÜÊFˆfe7FFRívñÊF˜rÂˆÜÊFˆfeVì÷ÁV∆√∞¢6ˆÁ7B6W76ñˆ‰6ˆ◊&W76ñˆ‰Ê6Ü˜#“Ä¢2Á6W76ñˆ‚bbGóVˆb2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%˜fó6ñ&∆UˆñGÉ””“vÁV÷&W"p¢íÚ2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%˜fó6ñ&∆UˆñGÇ¢ÁV∆√∞¢6ˆÁ7B6W76ñˆ‰6ˆ◊&W76ñˆ‰Ê6Ü˜$∂Wì“Ä¢2Á6W76ñˆ‚bb2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%ˆ÷W76vUˆ∂WíbbGóVˆb2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%ˆ÷W76vUˆ∂Wì””“vˆ&¶V7Bp¢íÚ2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%ˆ÷W76vUˆ∂Wí¢ÁV∆√∞¢6ˆÁ7B6W76ñˆ‰6ˆ◊&W76ñˆÂ7V÷÷'ì“Ä¢2Á6W76ñˆ‚bbGóVˆb2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%˜7V÷÷'ì””“w7G&ñÊrp¢íÚ2Á6W76ñˆ‚Ê6ˆ◊&W76ñˆÂˆÊ6Ü˜%˜7V÷÷'íÁG&ñ“Çí¢rs∞¢6ˆÁ7Bv˜&∂∆ˆtFWFñƒFó66∆˜7W&U7FFS’ˆ6GW&Uv˜&∂∆ˆtFWFñƒFó66∆˜7W&U7FFRÜñÊÊW"ì∞¢˜&V7ñ6∆U7F6ÇÊ6∆V"Çì∞¢ñbÖˆ◊6tÊˆFU&V7ñ6∆TVÊ&∆VBó∞¢f˜"Ü6ˆÁ7B6Üñ∆Bˆb'&íÊg&ˆ“ÜñÊÊW"Ê6Üñ∆G&V‚íó∞¢6ˆÁ7B∂Wì÷6Üñ∆BÊFF6WBbbÜ6Üñ∆BÊFF6WBÁ&V7ñ6∆T∂Wó«∆6Üñ∆BÊFF6WBÊ◊6tñGÇì∞¢ñbÇ∂Wíí6ˆÁFñÁVS∞¢ñbÜ6Üñ∆BÊñC””“v∆ófT76ó7FÁEGW&‚w«∆6Üñ∆BÁVW'ï6V∆V7F˜"bf6Üñ∆BÁVW'ï6V∆V7F˜"Çr6∆ófT76ó7FÁEGW&‚ríí6ˆÁFñÁVS∞¢˜&V7ñ6∆U7F6ÇÁ6WBÑÁV÷&W"Ü∂Wíí¬6Üñ∆Bì∞¢–¢–¢ÚÚ÷ˆ&ñ∆R67&ˆ∆¬÷¶Ê≤fóÉ¢FV◊˜&&ñ«íFó6&∆R˜fW&f∆˜r÷Ê6Ü˜"6Ú6á&ˆ÷óV–¢ÚÚ6ÊÊ˜B&R÷Ê6Ü˜"FÚFÜRF˜÷˜7B&˜rGW&ñÊrFÜRDÙ“vóR÷ÊB◊&V'Vñ∆Bv‡¢ñbávñÊF˜rÂˆfóÑ÷ˆ&ñ∆U67&ˆ∆ƒ¶Ê≤ívñÊF˜rÂˆfóÑ÷ˆ&ñ∆U67&ˆ∆ƒ¶Ê≤Çì∞¢ÚÚ6GW&RvÜWFÜW"FÜR&VFW"v2BˆÊV"FÜRFñ¬$Tdı$RFÜRvóR‚Fñ¬÷fˆ∆∆˜vW ¢ÚÚÜóB'í÷ñB◊7G&V“&R◊&VÊFW"vWG2ˆÊR÷g&÷R¶óGFW#¢FÜRvóR∑&V'Vñ∆B∆ÊG2FÜP¢ÚÚ7ñÊ267&ˆ∆≈F˜w&óFRvñÁ7BG&Á6ñVÁB∆ñ˜WBvÜ˜6R&˜fR◊fñWw˜'BÜVñváBó2¢ÚÚfWrÇ6Ü˜'BˆbFÜR6WGF∆VBf«VR¬6ÚFÜR'&˜w6W"6∆◊267&ˆ∆≈F˜∆óGF∆RÜñvÉ∞¢ÚÚFÜR6WGF∆R$b6˜'&V7G2óBFÜRÊWáBg&÷R¬&ˆGV6ñÊrf7B„◊&˜r&6≤÷ÊB÷f˜'FÄ¢ÚÚ&˜VÊ6R‚vR&V÷V÷&W"FÜR&R◊vóRÊV"◊Fñ¬7FFRÜW&RÜvVˆ÷WG'í¬Ê˜B6∆˜7W&Rñ‡¢ÚÚf∆w2(	BFÜRvóRw26∆◊67&ˆ∆¬WfVÁB6‚G&Á6ñVÁF«íW'GW&"FÜ˜6Rí6ÚFÜRFñ¿¢ÚÚˆb&VÊFW$÷W76vW26‚&R÷Ê6Ü˜"FÚFÜR6WGF∆VB&˜GFˆ“&Vf˜&RFÜRñÁFW&÷VFñFRó0¢ÚÚñÁFVB‚6VR˜&VÊ6Ü˜%ñÊÊVEFñƒgFW%&VÊFW"≤óG2VWVT÷ñ7&˜F6≤6∆¬6óFR‡¢6ˆÁ7B˜&UvóTÊV%Fñ√“ÇÇì”Á∞¢6ˆÁ7Bˆ”“BÇv÷W76vW2rì∞¢ñbÇˆ“í&WGW&‚f«6S∞¢&WGW&‚Öˆ“Á67&ˆ∆ƒÜVñváB’ˆ“Á67&ˆ∆≈F˜’ˆ“Ê6∆ñVÁDÜVñváBì√”É∞¢“íÇì∞¢ÚÚ&R◊vóR6GW&S¢&VBFÜR7Fñ∆¬÷∆ñB÷˜WBW6W"&˜w2r$T¬ÜVñváG2&Vf˜&RFÜRvóR&V∆˜p¢ÚÚFW7G&˜ó2FÜV“¬ÊBW'6ó7B6ÚFÜR&V'Vñ∆B&W6W'fW2FÜR&V¬ˆfb◊67&VV‚ÜVñváB‚FÜó2ó0¢ÚÚFÜRÊˆ‚◊fó'GV∆ó¶VBÊ∆ˆrˆb3Sc3Çw2fó'GV∆ó¶VB÷V7W&R72ávÜñ6ÇÊWfW"'VÁ2vÜV‡¢ÚÚ˜fó'GV∆ó¶UG&Á67&óC””÷f«6Rí‚vóFÜ˜WBóB¬g&W6Çˆfb◊67&VV‚F∆¬W6W"&˜r&W6W'fW0¢ÚÚˆÊ«íFÜRf∆B6ˆÁFñ‚÷ñÁG&ñÁ6ñ2◊6ó¶RW7Fñ÷FR¬67&ˆ∆ƒÜVñváB6á&ñÊ∑2¬ÊBFÜR'&˜w6W ¢ÚÚ6∆◊267&ˆ∆≈F˜(i"FÜRßV◊÷&6≤‚&VFñÊr&R◊vóRÜÊ˜B˜7B◊&VÊFW"íó2vÜB÷∂W2FÜP¢ÚÚ÷V7W&V÷VÁB&V∆ñ&∆R(	BFÜRˆ∆BV∆V÷VÁG2ÜfRñÁFVB¬6ÚFÜVó"&V7BÜVñváBó2&V¬WfV‡¢ÚÚˆfb◊67&VV„≤˜7B◊&VÊFW"&VBˆbg&W6Çˆfb◊67&VV‚&˜r&WGW&Á2óG26ˆ∆∆6VB&W6W'fR‡¢ñbáGóVˆb˜&V÷V÷&W%&VÊFW&VEW6W%&˜tñÁG&ñÁ6ñ4ÜVñváG3””“vgVÊ7Fñˆ‚rí˜&V÷V÷&W%&VÊFW&VEW6W%&˜tñÁG&ñÁ6ñ4ÜVñváG2Çì∞¢ÚÚFÜRDÙ“vóR6‚'&ñVf«í6ˆ∆∆6R6◊6tñÊÊW"FÚ¶W&ÚÜVñváB¬6W6ñÊrFÜP¢ÚÚ'&˜w6W"FÚ6∆◊6÷W76vW2Á67&ˆ∆≈F˜FÚÊBV÷óB67&ˆ∆¬WfVÁB‚FÜ@¢ÚÚWfVÁBó2&VÊFW"'Fñf7B¬Ê˜BW6W"ñÁFVÁC≤ñbFÜR67&ˆ∆¬∆ó7FVÊW"6VW2ó@¢ÚÚvóFÇ˜&ˆw&÷÷Fñ567&ˆ∆√÷f«6R¬óB÷&∑2FÜR&VFW"÷ÁV∆«íVÁñÊÊVBÊ@¢ÚÚFÜR∆ófR&W«í7F˜2fˆ∆∆˜vñÊrÚV'2FÚßV◊&6∑v&B‡¢˜&ˆw&÷÷Fñ567&ˆ∆√◊G'VS∞¢˜&ˆw&÷÷Fñ567&ˆ∆≈6WDC◊W&f˜&÷Ê6RÊÊ˜rÇì∞¢ñÊÊW"ÊñÊÊW$ÖD‘√“rs∞¢6ˆÁ7B6ˆ◊&W76ñˆ‰ÊˆFS÷6ˆ◊&W76ñˆÂ7FFSıˆ6ˆ◊&W76ñˆ‰6&G4ÊˆFRÜ6ˆ◊&W76ñˆÂ7FFRì¶ÁV∆√∞¢6ˆÁ7B∂÷W76vSß&VfW&VÊ6T÷W76vR¬&tñGÉß&VfW&VÊ6T÷W76vU&tñGá”’ˆ∆FW7D6ˆ◊&W76ñˆÂ&VfW&VÊ6T÷W76vRÄ¢2Ê÷W76vW2¿¢6W76ñˆ‰6ˆ◊&W76ñˆÂ7V÷÷'ê¢ì∞¢6ˆÁ7B&VfW&VÊ6UFWáC“ÇÇì”Á∞¢ñbÇ&VfW&VÊ6T÷W76vRí&WGW&‚6W76ñˆ‰6ˆ◊&W76ñˆÂ7V÷÷'ì∞¢6ˆÁ7B&s÷◊6t6ˆÁFVÁBá&VfW&VÊ6T÷W76vRó«≈7G&ñÊrá&VfW&VÊ6T÷W76vRÊ6ˆÁFVÁG«¬rrì∞¢6ˆÁ7B6Vv÷VÁC’ˆ6ˆ◊7FñˆÂ7V÷÷'ï6Vv÷VÁBá&rì∞¢&WGW&‚6Vv÷VÁB”÷ÁV∆√˜6Vv÷VÁCß&s∞¢“íÇì∞¢ÚÚV«G&÷6ˆ◊7BFó7∆íÉ##b”Ç”Çì¢WfW'í6ˆ◊7Fñˆ‚÷&∂W"∆ˆFVBñ‚FÜP¢ÚÚG&Á67&óB&VÊFW'22óG2˜v‚6ˆ∆∆6VB6&BBóG2&V¬˜6óFñˆ‚¬6ÚFÜP¢ÚÚW6W"4TU2V6Ç6ˆ◊7Fñˆ‚ÊB6‚&V˜V‚óG2FñvW7BñÊ∆ñÊR‚÷&∂W'2ˆ∆FW ¢ÚÚFÜ‚FÜRfó'GV¬vñÊF˜r&RñÊÊVB&˜fRFÜR&VÊFW&VB&˜w2ñ‚G&Á67&ó@¢ÚÚ˜&FW#≤÷&∂W'2ñÁ6ñFRFÜRvñÊF˜r7FíñÊ∆ñÊR‡¢6ˆÁ7Bfó'7E&VÊFW&VE&tñGÉ◊&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÉ˜&VÊFW%fó5vóFÑñGÖ≥“Á&tñGÉ§ñÊfñÊóGì∞¢6ˆÁ7B∆ˆFVD6ˆ◊7FñˆÂ&tñGá3“Ç6ˆ◊&W76ñˆÂ7FFRìıˆ∆ˆFVD6ˆ◊7Fñˆ‰÷&∂W%&tñGá2Ö2Ê÷W76vW2ì•µ”∞¢ÚÚ∆ˆFVB÷&∂W"FÜB÷F6ÜW2FÜR7W'&VÁB7V÷÷'í«&VGí&VÊFW'22óG0¢ÚÚ˜v‚6&B‚vÜV‚ÊˆÊR÷F6ÜW2á&VfW&VÊ6T÷W76vU&tñGÉ√íFÜR7W'&VÁB7V÷÷'ê¢ÚÚó27Fñ∆¬WFÜ˜&óFFófRÊB◊W7B7Fífó6ñ&∆RWfV‚ÊWáBFÚ7F∆R÷&∂W'2¿¢ÚÚ6ÚFÜR6WGF∆VBf∆∆&6≤ó2FV6ñFVBÜW&R¬&Vf˜&RÁí6&BÊˆFRó2'Vñ«B¿¢ÚÚÊBF∂W2'Bñ‚FÜR6ñÊv∆R&W6W'fVB◊F6≤˜vÊW"6V∆V7Fñˆ‚‡¢6ˆÁ7B6Ü˜t7W'&VÁE7V÷÷'îf∆∆&6≥“Ç6ˆ◊&W76ñˆÂ7FFRbb&VfW&VÊ6T÷W76vU&tñGÉ√bb˜6Ü˜V∆E6Ü˜u6WGF∆VD6ˆ◊&W76ñˆÂ&VfW&VÊ6Rá&VfW&VÊ6UFWáBíbbá6W76ñˆ‰6ˆ◊&W76ñˆ‰Ê6Ü˜"”÷ÁV∆¬«¬6W76ñˆ‰6ˆ◊&W76ñˆ‰Ê6Ü˜$∂Wí«¬6W76ñˆ‰6ˆ◊&W76ñˆÂ7V÷÷'ííì∞¢6ˆÁ7B6ˆ◊7FñˆÂ∆6V÷VÁG3’˜6V∆V7D6ˆ◊7Fñˆ‰6&E∆6V÷VÁG2Ü∆ˆFVD6ˆ◊7FñˆÂ&tñGá2∆fó'7E&VÊFW&VE&tñGÇ«6Ü˜t7W'&VÁE7V÷÷'îf∆∆&6≤ì∞¢6ˆÁ7B&VfW&VÊ6TÊˆFT˜vÁ5F6∑3“á6Ü˜t7W'&VÁE7V÷÷'îf∆∆&6∞¢bb6ˆ◊7FñˆÂ∆6V÷VÁG2ÁF6¥˜vÊW ¢bb6ˆ◊7FñˆÂ∆6V÷VÁG2ÁF6¥˜vÊW"Ê∂ñÊC””“v7W'&VÁB◊7V÷÷'íp¢bb&W6W'fVD6ˆ◊&W76ñˆÂF6¥÷W76vW2Ê∆VÊwFÇì∞¢6ˆÁ7Bˆ6ˆ◊7Fñˆ‰6&DVÁG'ì“Ü÷&∂W%&tñGÇ∆∂ñÊBì”Á∞¢6ˆÁ7B÷&∂W$◊6s’2Ê÷W76vW5∂÷&∂W%&tñGÖ”∞¢∆WB&s“rs∞¢G'ó∞¢&s’7G&ñÊrÜ◊6t6ˆÁFVÁBÜ÷&∂W$◊6ró«¬rrì∞¢÷6F6ÇÖÚó∞¢&s’7G&ñÊrÇÜ÷&∂W$◊6rbf÷&∂W$◊6rÊ6ˆÁFVÁBó«¬rrì∞¢–¢6ˆÁ7B6Vv÷VÁC’ˆ6ˆ◊7FñˆÂ7V÷÷'ï6Vv÷VÁBá&rì∞¢6ˆÁ7BFWáC◊6Vv÷VÁB”÷ÁV∆√˜6Vv÷VÁCß&s∞¢6ˆÁ7B˜vÁ5F6∑3“Ü6ˆ◊7FñˆÂ∆6V÷VÁG2ÁF6¥˜vÊW ¢bb6ˆ◊7FñˆÂ∆6V÷VÁG2ÁF6¥˜vÊW"Ê∂ñÊC””÷∂ñÊ@¢bb6ˆ◊7FñˆÂ∆6V÷VÁG2ÁF6¥˜vÊW"Á&tñGÉ””÷÷&∂W%&tñGÇì∞¢6ˆÁ7B&˜s÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢&˜rÊñÊÊW$ÖD‘√÷∆Fób6∆73“&6ˆ◊&W76ñˆ‚◊GW&‚#„∆Fób6∆73“&6ˆ◊&W76ñˆ‚◊GW&‚÷&∆ˆ6∑2#‚Gµˆ6ˆ◊&W76ñˆÂ&VfW&VÊ6T6&DáF÷¬áFWáB∆f«6Ró“G∂˜vÁ5F6∑3ı˜&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D6&G4áF÷¬á&W6W'fVD6ˆ◊&W76ñˆÂF6¥÷W76vW2ì¢rw”¬ˆFóc„¬ˆFócÊ∞¢6ˆÁ7BÊˆFS◊&˜rÊfó'7DV∆V÷VÁD6Üñ∆C∞¢ñbÜÊˆFRó∞¢ÊˆFRÁ6WDGG&ñ'WFRÇvFF÷6ˆ◊7Fñˆ‚◊∆6V÷VÁBr∆∂ñÊBì∞¢ÊˆFRÁ6WDGG&ñ'WFRÇvFF÷6ˆ◊7Fñˆ‚◊&r÷ñGÇr≈7G&ñÊrÜ÷&∂W%&tñGÇíì∞¢ñbÜ˜vÁ5F6∑2bg&W6W'fVD6ˆ◊&W76ñˆÂF6¥÷W76vW2Ê∆VÊwFÇíÊˆFRÁ6WDGG&ñ'WFRÇvFF÷6ˆ◊7Fñˆ‚◊F6≤÷˜vÊW"r¬srì∞¢–¢&WGW&‚∂ÊˆFR«&tñGÉ¶÷&∂W%&tñGÇ∆∂ñÊG”∞¢”∞¢6ˆÁ7B&UvñÊF˜t6ˆ◊7Fñˆ‰6&G3÷6ˆ◊7FñˆÂ∆6V÷VÁG2Á&UvñÊF˜t÷&∂W'2Ê÷Ü÷&∂W%&tñGÉ”Âˆ6ˆ◊7Fñˆ‰6&DVÁG'íÜ÷&∂W%&tñGÇ¬w&R◊vñÊF˜rríì∞¢6ˆÁ7B6ˆ◊7Fñˆ‰6&DÊˆFW3÷6ˆ◊7FñˆÂ∆6V÷VÁG2ÊñÊ∆ñÊT÷&∂W'2Ê÷Ü÷&∂W%&tñGÉ”Âˆ6ˆ◊7Fñˆ‰6&DVÁG'íÜ÷&∂W%&tñGÇ¬vñÊ∆ñÊRríì∞¢6ˆÁ7B&VfW&VÊ6TÊˆFS◊6Ü˜t7W'&VÁE7V÷÷'îf∆∆&6∞¢ÚÇÇì”Á∂6ˆÁ7B&˜s÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∑&˜rÊñÊÊW$ÖD‘√÷∆Fób6∆73“&6ˆ◊&W76ñˆ‚◊GW&‚#„∆Fób6∆73“&6ˆ◊&W76ñˆ‚◊GW&‚÷&∆ˆ6∑2#‚Gµˆ6ˆ◊&W76ñˆÂ&VfW&VÊ6T6&DáF÷¬á&VfW&VÊ6UFWáB∆f«6Ró“G∑&VfW&VÊ6TÊˆFT˜vÁ5F6∑3ı˜&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D6&G4áF÷¬á&W6W'fVD6ˆ◊&W76ñˆÂF6¥÷W76vW2ì¢rw”¬ˆFóc„¬ˆFócÊ∂6ˆÁ7BÊˆFS◊&˜rÊfó'7DV∆V÷VÁD6Üñ∆C∂ñbÜÊˆFRbg&VfW&VÊ6TÊˆFT˜vÁ5F6∑2íÊˆFRÁ6WDGG&ñ'WFRÇvFF÷6ˆ◊7Fñˆ‚◊F6≤÷˜vÊW"r¬srì∑&WGW&‚ÊˆFS∑“íÇê¢¢ÁV∆√∞¢∆WB&VfW&VÊ6TÊˆFUñÊÊVDEF˜÷f«6S∞¢∆WB&W6W'fVD6ˆ◊&W76ñˆÂF6¥˜vÊW$ÊˆFS÷ÁV∆√∞¢6ˆÁ7B&W6W'fVD6ˆ◊&W76ñˆÂ&tñGá3’µ”∞¢∆WB&tñGÉ”∞¢f˜"Ü6ˆÁ7B“ˆb2Ê÷W76vW2ó∞¢ñbÇ◊«¬“Á&ˆ∆W«∆“Á&ˆ∆S””“wFˆˆ¬ró∑&tñGÇ≤≥∂6ˆÁFñÁVS∑–¢ñbÖˆó5&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D÷W76vRÜ“íó∑&W6W'fVD6ˆ◊&W76ñˆÂ&tñGá2ÁW6Çá&tñGÇì∑&tñGÇ≤≥∂6ˆÁFñÁVS∑–¢&tñGÇ≤≥∞¢–¢ÚÚ3cììì¢FÜRGW&‚÷6ˆÁFVÁB÷2’U5B6VRFÜReTƒ¬fó5vóFÑñGÇ¬Ê˜BFÜP¢ÚÚfó'GV¬&VÊFW"vñÊF˜r‚ˆ76ó7FÁEGW&‰fñÊ≈fó6ñ&∆T6ˆÁFVÁD÷¢ÚÚˆ76ó7FÁEGW&Âfó6ñ&∆T6ˆÁFVÁD÷FW&ófRFÜRV6ÜÚ◊7G&ó6ˆÁFWáBf˜"¢ÚÚ&VÊFW&VB76ó7FÁB&˜rg&ˆ“ƒ¬76ó7FÁB6ñ&∆ñÊw2ˆbóG2GW&‡¢ÚÚáVíÊß3£sÉ2”É3í‚vñÊF˜vVB÷˜WB6ñ&∆ñÊw2&R7Fñ∆¬ñÁWB6ˆÁFWáBWfV‡¢ÚÚFÜ˜VvÇFÜVó"˜v‚&˜w2&RÊ˜B&VC¢7WGFñÊrFá&˜VvÇ‚76ó7FÁB'V‡¢ÚÚ∆˜6W2FÜRfñÊ¬˜fó6ñ&∆RÁ7vW"W6VBFÚ7G&ó&V6ˆÊñÊrV6ÜˆW2¬Ê@¢ÚÚ6ˆÊ6FVÊFñÊrÜVB∑Fñ¬7&˜72‚ˆ÷óGFVBW6W"&˜VÊF'ív˜V∆B÷W&vP¢ÚÚFó7FñÊ7BGW&Á2ñÁFÚˆÊR'V‚ÜGW∆ñ6FRfñÊ¬÷Á7vW"ñ‚v˜&∂∆ˆrıFÜñÊ∂ñÊr¿¢ÚÚ˜"∆FW"◊GW&‚&˜6RW6VB2‚V6ÜÚ◊7G&óñÁWBí‚GW&‚6ˆÁFWáB◊W7@¢ÚÚ«vó2&R6ˆ◊∆WFR(	BÊWfW"7WB÷ñB◊'V‚¬ÊWfW"ÜVB∑Fñ¬vóFÇv‡¢6ˆÁ7B76ó7FÁEGW&‰fñÊ≈fó6ñ&∆T6ˆÁFVÁD'ï&tñGÉ’ˆ76ó7FÁEGW&‰fñÊ≈fó6ñ&∆T6ˆÁFVÁD÷áfó5vóFÑñGÇì∞¢6ˆÁ7B76ó7FÁEGW&Âfó6ñ&∆T6ˆÁFVÁD'ï&tñGÉ’ˆ76ó7FÁEGW&Âfó6ñ&∆T6ˆÁFVÁD÷áfó5vóFÑñGÇì∞¢6ˆÁ7BÜ56W'fW$ˆ∆FW#“áGóVˆbˆ÷W76vW5G'VÊ6FVB”“wVÊFVfñÊVBrbbˆ÷W76vW5G'VÊ6FVBbb2Ê÷W76vW2Ê∆VÊwFÉ„ì∞¢6ˆÁ7B6W'fW$ˆ∆FW$6˜VÁC÷Ü56W'fW$ˆ∆FW"bdÁV÷&W"Êó4fñÊóFRÑÁV÷&W"Öˆˆ∆FW7DñGÇíìÙ÷FÇÊ÷ÇÉƒÁV÷&W"Öˆˆ∆FW7DñGÇíì£∞¢ñbáGóVˆbˆ«ï6W76ñˆ‰ÊfñvFñˆÂ&Vg3””“vgVÊ7Fñˆ‚ríˆ«ï6W76ñˆ‰ÊfñvFñˆÂ&Vg2Çì∞¢ñbáfó'GV≈vñÊF˜rÁfó'GV∆ó¶VBbgfó'GV≈vñÊF˜rÁF˜C„ó∞¢ñÊÊW"ÊVÊD6Üñ∆BÖˆ÷W76vUfó'GV≈76W"áfó'GV≈vñÊF˜rÁF˜B¬v&Vf˜&Rríì∞¢–¢ñbÜÜ56W'fW$ˆ∆FW"ó∞¢6ˆÁ7BñÊFñ6F˜#÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇv'WGFˆ‚rì∞¢ñÊFñ6F˜"ÁGóS“v'WGFˆ‚s∞¢ñÊFñ6F˜"ÊñC“v∆ˆDˆ∆FW$ñÊFñ6F˜"s∞¢ñÊFñ6F˜"Ê6∆74Ê÷S“v∆ˆB÷ˆ∆FW"÷ñÊFñ6F˜"÷W76vR◊vñÊF˜r÷∆ˆB÷V&∆ñW"s∞¢ñÊFñ6F˜"ÁFWáD6ˆÁFVÁC◊6W'fW$ˆ∆FW$6˜VÁC„ ¢Ú∆ˆBV&∆ñW"÷W76vW2ÇG∑6W'fW$ˆ∆FW$6˜VÁG“ˆ∆FW"ñ ¢¢áGóVˆbC””“vgVÊ7Fñˆ‚s˜BÇv∆ˆEˆˆ∆FW%ˆ÷W76vW2rì¢t∆ˆBV&∆ñW"÷W76vW2rì∞¢ñÊÊW"ÊVÊD6Üñ∆BÜñÊFñ6F˜"ì∞¢˜vó&T÷W76vUvñÊF˜t∆ˆDV&∆ñW$'WGFˆ‚Çì∞¢ÚÚ∂VWFÜR6WGF∆VB6ˆ◊7FVB÷6ˆÁFWáB6&Bñ÷÷VFñFV«ífó6ñ&∆Rñ‚∆ˆÊr¿¢ÚÚFñ¬÷∆ˆFVB6ˆÁfW'6Fñˆ‚‚WBóBñ‚f∆˜rÜÊ˜BñÁ6ñFR‚ˆ∆BFˆˆ¬GW&‚í‡¢&VfW&VÊ6TÊˆFUñÊÊVDEF˜’˜ñÂ6WGF∆VD6ˆ◊&W76ñˆÂ&VfW&VÊ6TEF˜ÜñÊÊW"«&VfW&VÊ6TÊˆFR«&VfW&VÊ6T÷W76vU&tñGÇì∞¢ñbá&VfW&VÊ6TÊˆFUñÊÊVDEF˜bg&VfW&VÊ6TÊˆFSÚÁ&VÁDV∆V÷VÁBbg&VfW&VÊ6TÊˆFT˜vÁ5F6∑2ó∞¢&W6W'fVD6ˆ◊&W76ñˆÂF6¥˜vÊW$ÊˆFS◊&VfW&VÊ6TÊˆFS∞¢–¢–¢6ˆÁ7B&UvñÊF˜tñÁ6W'Fñˆ„’ˆñÁ6W'D6ˆ◊7Fñˆ‰6&DÊˆFW2Ä¢&UvñÊF˜t6ˆ◊7Fñˆ‰6&G2¿¢6ˆ◊7FñˆÂ∆6V÷VÁG2ÁF6¥˜vÊW"¿¢ÊˆFS”Â˜ñ‰6ˆ◊7Fñˆ‰6&DEF˜ÜñÊÊW"∆ÊˆFRê¢ì∞¢ñbá&UvñÊF˜tñÁ6W'Fñˆ‚ÁF6¥˜vÊW$ÊˆFRí&W6W'fVD6ˆ◊&W76ñˆÂF6¥˜vÊW$ÊˆFS◊&UvñÊF˜tñÁ6W'Fñˆ‚ÁF6¥˜vÊW$ÊˆFS∞¢∆WB∆7EW6W%&tñGÉ“”∞¢f˜"Ü∆WBì◊fó5vóFÑñGÇÊ∆VÊwFÇ”∂ì„”∂í““ó∞¢ñbáfó5vóFÑñGÖ∂ï“Ê“bgfó5vóFÑñGÖ∂ï“Ê“Á&ˆ∆S””“wW6W"ró∞¢∆7EW6W%&tñGÉ◊fó5vóFÑñGÖ∂ï“Á&tñGÉ∞¢'&V≥∞¢–¢–¢6ˆÁ7BñÁ6W'Fñˆ‰Ê6Ü˜$gV∆√’ˆ6ˆ◊&W76ñˆ‰Ê6Ü˜$ñÊFWÇÄ¢fó5vóFÑñGÇ¿¢6ˆ◊&W76ñˆÂ7FFRÚ6ˆ◊&W76ñˆÂ7FFRÊÊ6Ü˜$÷W76vT∂Wí¢6W76ñˆ‰6ˆ◊&W76ñˆ‰Ê6Ü˜$∂Wí¿¢6ˆ◊&W76ñˆÂ7FFP¢ÚáGóVˆb6ˆ◊&W76ñˆÂ7FFRÊÊ6Ü˜%fó6ñ&∆TñGÉ””“vÁV÷&W"rÚ6ˆ◊&W76ñˆÂ7FFRÊÊ6Ü˜%fó6ñ&∆TñGÇ¢6ˆ◊&W76ñˆÂ7FFRÊÊ6Ü˜%&tñGÇê¢¢6W76ñˆ‰6ˆ◊&W76ñˆ‰Ê6Ü˜ ¢ì∞¢∆WBñÁ6W'Fñˆ‰Ê6Ü˜#÷ÁV∆√∞¢ñbáGóVˆbñÁ6W'Fñˆ‰Ê6Ü˜$gV∆√””“vÁV÷&W"ró∞¢6ˆÁ7BÜ5fó'GV≈&VÊFW$v◊&VÊFW%fó6ñ&∆TñGá2Á6ˆ÷RÇÜñGÇ«˜2ì”ÊñGÇ”◊vñÊF˜u7F'B∑˜2ì∞¢ñbÇÜ5fó'GV≈&VÊFW$vó∞¢ñbÜñÁ6W'Fñˆ‰Ê6Ü˜$gV∆√«vñÊF˜u7F'BíñÁ6W'Fñˆ‰Ê6Ü˜#◊&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÉÛ¶ÁV∆√∞¢V«6RñbÜñÁ6W'Fñˆ‰Ê6Ü˜$gV∆√«vñÊF˜u7F'B∑&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÇíñÁ6W'Fñˆ‰Ê6Ü˜#÷ñÁ6W'Fñˆ‰Ê6Ü˜$gV∆¬◊vñÊF˜u7F'C∞¢V«6RñÁ6W'Fñˆ‰Ê6Ü˜#◊&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÉ˜&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÇ”¶ÁV∆√∞¢÷V«6Rñbá&VÊFW%fó6ñ&∆TñGá2Ê∆VÊwFÇó∞¢∆WB&Wfñ˜W5fó6ñ&∆TñGÉ“”∞¢f˜"Ü∆WBì”∂ì«&VÊFW%fó6ñ&∆TñGá2Ê∆VÊwFÉ∂í≤≤ó∞¢ñbá&VÊFW%fó6ñ&∆TñGá5∂ï”√÷ñÁ6W'Fñˆ‰Ê6Ü˜$gV∆¬í&Wfñ˜W5fó6ñ&∆TñGÉ÷ì∞¢V«6R'&V≥∞¢–¢ñÁ6W'Fñˆ‰Ê6Ü˜#◊&Wfñ˜W5fó6ñ&∆TñGÉ„”˜&Wfñ˜W5fó6ñ&∆TñGÉ£∞¢÷V«6W∞¢ñÁ6W'Fñˆ‰Ê6Ü˜#÷ÁV∆√∞¢–¢–¢∆WB˜&We6W∂Wì÷ÁV∆√∞¢∆WB7W'&VÁD76ó7FÁEGW&„÷ÁV∆√∞¢ÚÚˆÊ«í'Vñ∆BVW7FñˆÓ(i&76ó7FÁB÷ñÊrf˜"FÜRfó6ñ&∆RvñÊF˜r¬Ê˜BFÜP¢ÚÚgV∆¬fó5vóFÑñGÇ‚FÜRßV◊◊FÚ◊VW7Fñˆ‚'WGFˆ‚ó2ˆÊ«í&VÊFW&VBf˜ ¢ÚÚ76ó7FÁB÷W76vW2FÜBV"ñ‚FÜR7W'&VÁB&VÊFW"vñÊF˜rÁóví‡¢6ˆÁ7BVW7FñˆÂ&tñGÑ'î76ó7FÁE&tñGÉ÷ÊWr÷Çì∞¢∆WB∆7EVW7FñˆÂ&tñGÉ“”∞¢6ˆÁ7B&VÊFW&VE&tñGá3÷ÊWr6WBá&VÊFW%fó5vóFÑñGÇÊ÷ÜS”ÊRÁ&tñGÇíì∞¢6ˆÁ7B&VÊFW&&∆U&tñGá3÷ÊWr6WBáfó5vóFÑñGÇÊ÷ÜS”ÊRÁ&tñGÇíì∞¢f˜"Ü6ˆÁ7BVÁG'íˆbfó5vóFÑñGÇó∞¢6ˆÁ7B&ˆ∆S÷VÁG'íbfVÁG'íÊ“bfVÁG'íÊ“Á&ˆ∆S∞¢ñbá&ˆ∆S””“wW6W"rí∆7EVW7FñˆÂ&tñGÉ÷VÁG'íÁ&tñGÉ∞¢V«6Rñbá&ˆ∆S””“v76ó7FÁBrbg&VÊFW&VE&tñGá2ÊÜ2ÜVÁG'íÁ&tñGÇííVW7FñˆÂ&tñGÑ'î76ó7FÁE&tñGÇÁ6WBÜVÁG'íÁ&tñGÇ∆∆7EVW7FñˆÂ&tñGÇì∞¢–¢6ˆÁ7B76ó7FÁE&tñGÑ'ïVW7FñˆÂ&tñGÉ÷ÊWr÷Çì∞¢f˜"Ü6ˆÁ7B∂ñGÇ«ñGÖ“ˆbVW7FñˆÂ&tñGÑ'î76ó7FÁE&tñGÇó∞¢ñbÇ76ó7FÁE&tñGÑ'ïVW7FñˆÂ&tñGÇÊÜ2áñGÇíí76ó7FÁE&tñGÑ'ïVW7FñˆÂ&tñGÇÁ6WBáñGÇ∆ñGÇì∞¢–¢ÚÚ33síÜFVfV7B"ì¢'Vñ∆BW"◊GW&‚6ˆ÷&ñÊVBfó6ñ&∆R÷Á7vW"FWáB6ÚFÜP¢ÚÚFÜñÊ∂ñÊrV6ÜÚ◊7G&ó6‚FR÷GWRFÜñÊ∂ñÊr÷ˆÊ«í÷W76vRávÜ˜6R˜v‚fó6ñ&∆P¢ÚÚ&ˆGíó2V◊GíívñÁ7BFÜRÁ7vW"&˜6R6'&ñVB'í4î$ƒî‰r÷W76vRñ‚FÜP¢ÚÚ6÷RGW&‚‚GW&‚“FÜR'V‚ˆb76ó7FÁB÷W76vW2&WGvVV‚GvÚW6W"÷W76vW2‡¢ÚÚ÷WfW'í76ó7FÁB&tñGÇñ‚'V‚FÚFÜR'V‚w26ˆ÷&ñÊVBfó6ñ&∆RFWáB‡¢6ˆÁ7B˜GW&Âfó6ñ&∆UFWáD'ï&tñGÉ÷ÊWr÷Çì∞¢∞¢∆WB˜'V„’µ”≤∆WB˜'VÂFWáC’µ”∞¢6ˆÁ7Bˆf«W6É“Çì”Á∞¢ñbÖ˜'V‚Ê∆VÊwFÇó∞¢6ˆÁ7B6ˆ÷&ñÊVC’˜'VÂFWáBÊ¶ˆñ‚Çu∆Â∆‚rì∞¢f˜"Ü6ˆÁ7B&íˆb˜'V‚í˜GW&Âfó6ñ&∆UFWáD'ï&tñGÇÁ6WBá&í¬6ˆ÷&ñÊVBì∞¢–¢˜'V„’µ”≤˜'VÂFWáC’µ”∞¢”∞¢f˜"Ü6ˆÁ7BVÁG'íˆb&VÊFW%fó5vóFÑñGÇó∞¢6ˆÁ7BV”÷VÁG'íbfVÁG'íÊ”≤6ˆÁ7B&ˆ∆S÷V“bfV“Á&ˆ∆S∞¢ñbá&ˆ∆S””“v76ó7FÁBró∞¢˜'V‚ÁW6ÇÜVÁG'íÁ&tñGÇì∞¢ÚÚfó6ñ&∆R&˜6R“6ˆÁFVÁBvóFÇÁí∆VFñÊr«FÜñÊ≥Ó(
c¬˜FÜñÊ≥‚ˆ6ÜÊÊV¬◊FÜ˜Vvá@¢ÚÚ&∆ˆ6≤7G&óVBáFÜR6÷R&∆ˆ6∑2FÜRW"÷÷W76vRWáG&7F˜"&V÷˜fW2&V∆˜rí‡¢∆WBfó3◊GóVˆbV“Ê6ˆÁFVÁC””“w7G&ñÊrsˆV“Ê6ˆÁFVÁC¢rs∞¢fó3◊fó2Á&W∆6RÇıÂ«2£«FÜñÊ≥Âµ«5≈5“£Û≈¬˜FÜñÊ≥Â«2¢Ú¬rrê¢Á&W∆6RÇıÂ«2£≈«∆6ÜÊÊV≈«√ÛÁFÜ˜VváE∆„ıµ«5≈5“£Û∆6ÜÊÊV≈«√Â«2¢Ú¬rrê¢Á&W∆6RÇıÂ«2£≈««GW&Â«√ÁFÜñÊ∂ñÊu∆Âµ«5≈5“£Û«GW&Â«√Â«2¢Ú¬rríÁG&ñ“Çì∞¢ñbáfó2í˜'VÂFWáBÁW6Çáfó2ì∞¢÷V«6W∞¢ˆf«W6ÇÇì∞¢–¢–¢ˆf«W6ÇÇì∞¢–†¢6ˆÁ7B76ó7FÁE6Vv÷VÁG3÷ÊWr÷Çì∞¢6ˆÁ7B76ó7FÁEFÜñÊ∂ñÊs÷ÊWr÷Çì∞¢6ˆÁ7BW6W%&˜w3÷ÊWr÷Çì∞¢ÚÚˆÊ«í6ˆ∆∆V7BFˆˆ¬÷6∆¬76ó7FÁBñÊFñ6W2f˜"÷W76vW2FÜB&R7GV∆«ê¢ÚÚ&VÊFW&VBñ‚FÜR7W'&VÁBvñÊF˜r‚2ÁFˆˆƒ6∆«26‚w&˜r∆&vRñ‚∆ˆÊrGW&Á2¿¢ÚÚ'WBvRˆÊ«íÊVVBFÜRˆÊW2vÜ˜6R76ó7FÁEˆ◊6uˆñGÇf∆«2ñÁ6ñFRFÜRfó6ñ&∆P¢ÚÚ&ÊvR‡¢6ˆÁ7BFˆˆƒ6∆ƒ76ó7FÁDñGá3÷ÊWr6WBÇì∞¢ñbÑ'&íÊó4'&íÖ2ÁFˆˆƒ6∆«2íó∞¢f˜"Ü6ˆÁ7BF2ˆb2ÁFˆˆƒ6∆«2ó∞¢ñbÇF2í6ˆÁFñÁVS∞¢6ˆÁ7BñGÉ◊F2Ê76ó7FÁEˆ◊6uˆñGÉ∞¢ñbÜñGÇ”◊VÊFVfñÊVBbb&VÊFW&VE&tñGá2ÊÜ2ÜñGÇíó∞¢Fˆˆƒ6∆ƒ76ó7FÁDñGá2ÊFBÜñGÇì∞¢–¢–¢–¢6ˆÁ7BG&Á7&VÁD˜&FW&VEFˆˆƒñG3÷ÊWr6WBÇì∞¢6ˆÁ7BG&Á7&VÁD˜&FW&VEFˆˆƒ6∆«4'ïFñC÷ÊWr÷Çì∞¢ÚÚFÜW6R66Á2ˆÊ«ífVVBFÜRG&Á7&VÁB◊7G&V“˜&FW&VB&VÊFW"FÉ≤6∂óFÜP¢ÚÚÚÜ÷W76vW<9w'G2ív˜&≤VÁFó&V«íñ‚˜FÜW"÷ˆFW2Ñ˜W2W&bfñÊFñÊr3Cì3"í‡¢6ˆÁ7B˜G&Á7&VÁD÷ˆFT7FófS“áGóVˆbó5G&Á7&VÁE7G&V”””“vgVÊ7Fñˆ‚ríbfó5G&Á7&VÁE7G&V“Çì∞¢6ˆÁ7BG&Á7&VÁEW'6ó7FVE6ÊóWD'ïFñC◊∑”∞¢ñbÖ˜G&Á7&VÁD÷ˆFT7FófRó∞¢ñbÑ'&íÊó4'&íÖ2ÁFˆˆƒ6∆«2íó∞¢f˜"Ü6ˆÁ7BF2ˆb2ÁFˆˆƒ6∆«2ó∞¢ñbÇF7««GóVˆbF2”“vˆ&¶V7Brí6ˆÁFñÁVS∞¢6ˆÁ7BFñC◊F2ÁFñG««F2ÊñG««F2ÁFˆˆ≈ˆ6∆≈ˆñG««F2ÁFˆˆ≈˜W6UˆñG««F2Ê6∆≈ˆñG«¬rs∞¢ñbáFñBbbG&Á7&VÁD˜&FW&VEFˆˆƒ6∆«4'ïFñBÊÜ2áFñBííG&Á7&VÁD˜&FW&VEFˆˆƒ6∆«4'ïFñBÁ6WBáFñB«F2ì∞¢–¢–¢ÚÚ3Cì#rGW&&∆Rf∆∆&6≥¢FÜR˜&FW&VBFÇ◊W7B6ˆÁ7V«BFÜRW'6ó7FV@¢ÚÚ6W76ñˆ‚ÁFˆˆ≈ˆ6∆«26ÊóWB'íFñBFˆÚ¬˜"6ˆ∆B˜vñÊFVB∆ˆBvÜW&RFÜP¢ÚÚ2Ê÷W76vW2Fˆˆ≈˜&W7V«B¶ˆñ‚÷ó76W2&VÊFW'2‚V◊Gí&ˆGí(	BÊBóG2ñÊ∆ñÊP¢ÚÚ6&BFÜV‚7W&W76W2FÜR˜7B÷∆ˆ˜FW&ófVB6&BFÜBtıTƒBÜfR&V6˜fW&VB‡¢G'ó∞¢6ˆÁ7BW'6ó7FVC“Ö2Á6W76ñˆ‚bd'&íÊó4'&íÖ2Á6W76ñˆ‚ÁFˆˆ≈ˆ6∆«2íìı2Á6W76ñˆ‚ÁFˆˆ≈ˆ6∆«3•µ”∞¢W'6ó7FVBÊf˜$V6ÇáF3”Á∞¢ñbÇF7««GóVˆbF2”“vˆ&¶V7Brí&WGW&„∞¢6ˆÁ7BFñC◊F2ÁFñG««F2ÊñG««F2ÁFˆˆ≈ˆ6∆≈ˆñG««F2Ê6∆≈ˆñG«¬rs∞¢6ˆÁ7B6Êó◊F2Á6ÊóWG««F2Á&W7V«G««F2Ê˜WGWG««F2Á&WfñWw«¬rs∞¢ñbáFñBbg6ÊóbbG&Á7&VÁEW'6ó7FVE6ÊóWD'ïFñE∑FñE“íG&Á7&VÁEW'6ó7FVE6ÊóWD'ïFñE∑FñE”’7G&ñÊrá6Êóì∞¢“ì∞¢÷6F6ÇÜRó∑–¢–¢6ˆÁ7BG&Á7&VÁEFˆˆ≈&W7V«G4'ïFñC’˜G&Á7&VÁD÷ˆFT7FófSıˆ6ˆ∆∆V7EFˆˆ≈&W7V«E6ÊóWG4'ïFñBÖ2Ê÷W76vW2ìß∑”∞¢6ˆÁ7B∆FW7E&VÊFW&VD76ó7FÁE&tñGÉ“ÇÇì”Á∞¢f˜"Ü∆WBì◊&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÇ”∂ì„”∂í““ó∞¢6ˆÁ7BVÁG'ì◊&VÊFW%fó5vóFÑñGÖ∂ï”∞¢ñbÜVÁG'íbfVÁG'íÊ“bfVÁG'íÊ“Á&ˆ∆S””“v76ó7FÁBrbbVÁG'íÊ“Âˆ∆ófRí&WGW&‚VÁG'íÁ&tñGÉ∞¢–¢&WGW&‚”∞¢“íÇì∞¢ÚÚvñÊF˜vVB&VÊFW"∆ˆ˜&W∆6W2FÜR∆Vv7ígV∆¬∆ˆ˜†¢ÚÚf˜"Ü∆WBfì”∑fì«fó5vóFÑñGÇÊ∆VÊwFÉ∑fí≤≤ê¢f˜"Ü∆WBfì”∑fì«&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÉ∑fí≤≤ó∞¢ñbáfó'GV≈vñÊF˜rÁfó'GV∆ó¶VBbgfó'GV≈vñÊF˜rÊ&˜GFˆ’C„bgfì””÷ÜVE&VÊFW$6˜VÁBó∞¢ÚÚFÜRfó'GV¬v'&V∑276ó7FÁB◊GW&‚F¶6VÊ7í‚&W6WBFÜR7W'&VÁ@¢ÚÚGW&‚&Vf˜&R&VÊFW&ñÊrFÜR«vó2◊fó6ñ&∆RFñ¬6Ú76ó7FÁB6Vv÷VÁG2F¢ÚÚÊ˜B÷W&vR7&˜72FÜR76W"&˜VÊF'í‡¢7W'&VÁD76ó7FÁEGW&„÷ÁV∆√∞¢ñÊÊW"ÊVÊD6Üñ∆BÖˆ÷W76vUfó'GV≈76W"áfó'GV≈vñÊF˜rÊ&˜GFˆ’B¬vgFW"ríì∞¢–¢6ˆÁ7B∂“«&tñGá”◊&VÊFW%fó5vóFÑñGÖ∑fï”∞¢6ˆÁ7B˜G56W÷“Â˜G7«∆“ÁFñ÷W7F◊∞¢ñbÖ˜G56Wó∞¢6ˆÁ7BˆC÷ÊWrFFRÖ˜G56W£ì∞¢6ˆÁ7Bˆ∂Wì’ˆBÁFÙFFU7G&ñÊrÇì∞¢ñbÖ˜&We6W∂Wíbb˜&We6W∂Wí”’ˆ∂Wíó∞¢6ˆÁ7B6W÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢6WÊ6∆74Ê÷S“v◊6r÷FFR◊6Ws∞¢6WÁFWáD6ˆÁFVÁC’ˆf◊DFFU6WÖˆBì∞¢ñÊÊW"ÊVÊD6Üñ∆Bá6Wì∞¢–¢˜&We6W∂Wì’ˆ∂Wì∞¢–¢∆WB6ˆÁFVÁC÷“Ê6ˆÁFVÁG«¬rs∞¢∆WBFÜñÊ∂ñÊuFWáC“rs∞¢∆WB˜&FW&VEG&Á7&VÁE'G3’˜G&Á7&VÁE7G&V‘˜&FW&VE'G2Ü“ì∞¢ñbÑ'&íÊó4'&íÜ6ˆÁFVÁBíó∞¢6ˆÁFVÁC÷6ˆÁFVÁBÊfñ«FW"á”ÁbgÁGóS””“wFWáBríÊ÷á”ÁÁFWáG««Ê6ˆÁFVÁG«¬rríÊ¶ˆñ‚Çu∆‚rì∞¢–¢ñbÜ“Á&ˆ∆S””“v76ó7FÁBrbb“Âˆ∆ófRbgGóVˆb6ˆÁFVÁC””“w7G&ñÊrró∞¢6ˆÁ7BÊ6Ü˜$fñÊ√’ˆ76ó7FÁEGW&‰Ê6Ü˜%6WGF∆VDfñÊƒÁ7vW"Ü“¬6ˆÁFVÁB¬∞¢6W76ñˆÂˆñCß6ñB¿¢&uˆñGÉß&tñGÇ¿¢“ì∞¢ñbÜÊ6Ü˜$fñÊ¬”÷ÁV∆¬ó∞¢6ˆÁFVÁC÷Ê6Ü˜$fñÊ√∞¢ñbÑ'&íÊó4'&íÜ˜&FW&VEG&Á7&VÁE'G2íó∞¢f˜"Ü∆WBì÷˜&FW&VEG&Á7&VÁE'G2Ê∆VÊwFÇ”∂ì„”∂í““ó∞¢ñbÜ˜&FW&VEG&Á7&VÁE'G5∂ï“bf˜&FW&VEG&Á7&VÁE'G5∂ï“Ê∂ñÊC””“wFWáBró∞¢˜&FW&VEG&Á7&VÁE'G5∂ï”◊≤‚‚Ê˜&FW&VEG&Á7&VÁE'G5∂ï“¬FWáC¶Ê6Ü˜$fñÊ«”∞¢'&V≥∞¢–¢–¢–¢–¢–¢ñbáGóVˆb6ˆÁFVÁC””“w7G&ñÊrró∞¢ñbáGóVˆbvñÊF˜r”“wVÊFVfñÊVBrbgGóVˆbvñÊF˜rÂˆWáG&7DñÊ∆ñÊUFÜñÊ∂ñÊtg&ˆ‘6ˆÁFVÁDf˜%&VÊFW#””“vgVÊ7Fñˆ‚ró∞¢6ˆÁ7B7∆óC◊vñÊF˜rÂˆWáG&7DñÊ∆ñÊUFÜñÊ∂ñÊtg&ˆ‘6ˆÁFVÁDf˜%&VÊFW"Ü6ˆÁFVÁB¬FÜñÊ∂ñÊuFWáBì∞¢FÜñÊ∂ñÊuFWáC◊7∆óBÁ&V6ˆÊñÊw««FÜñÊ∂ñÊuFWáC∞¢6ˆÁFVÁC◊7∆óBÊ6ˆÁFVÁC∞¢÷V«6RñbÇFÜñÊ∂ñÊuFWáBó∞¢6ˆÁ7BFÜñÊ¥÷F6É÷6ˆÁFVÁBÊ÷F6ÇÇıÂ«2£«FÜñÊ≥‚Öµ«5≈5“£Úì≈¬˜FÜñÊ≥Â«2¢Úì∞¢ñbáFÜñÊ¥÷F6Çó∞¢FÜñÊ∂ñÊuFWáC◊FÜñÊ¥÷F6Ö≥“ÁG&ñ“Çì∞¢6ˆÁFVÁC÷6ˆÁFVÁBÁ&W∆6RÇıÂ«2£«FÜñÊ≥Âµ«5≈5“£Û≈¬˜FÜñÊ≥Â«2¢Ú¬rríÁG&ñ’7F'BÇì∞¢–¢ñbÇFÜñÊ∂ñÊuFWáBó∞¢6ˆÁ7BvV÷÷÷F6É÷6ˆÁFVÁBÊ÷F6ÇÇıÂ«2£≈«∆6ÜÊÊV≈«√ÛÁFÜ˜VváE∆„ÚÖµ«5≈5“£Úì∆6ÜÊÊV≈«√Â«2¢Úì∞¢ñbÜvV÷÷÷F6Çó∞¢FÜñÊ∂ñÊuFWáC÷vV÷÷÷F6Ö≥“ÁG&ñ“Çì∞¢6ˆÁFVÁC÷6ˆÁFVÁBÁ&W∆6RÇıÂ«2£≈«∆6ÜÊÊV≈«√ÛÁFÜ˜VváE∆„ıµ«5≈5“£Û∆6ÜÊÊV≈«√Â«2¢Ú¬rríÁG&ñ’7F'BÇì∞¢–¢–¢ñbÇFÜñÊ∂ñÊuFWáBó∞¢6ˆÁ7BvV÷÷GW&‰÷F6É÷6ˆÁFVÁBÊ÷F6ÇÇıÂ«2£≈««GW&Â«√ÁFÜñÊ∂ñÊu∆‚Öµ«5≈5“£Úì«GW&Â«√Â«2¢Úì∞¢ñbÜvV÷÷GW&‰÷F6Çó∞¢FÜñÊ∂ñÊuFWáC÷vV÷÷GW&‰÷F6Ö≥“ÁG&ñ“Çì∞¢6ˆÁFVÁC÷6ˆÁFVÁBÁ&W∆6RÇıÂ«2£≈««GW&Â«√ÁFÜñÊ∂ñÊu∆Âµ«5≈5“£Û«GW&Â«√Â«2¢Ú¬rríÁG&ñ’7F'BÇì∞¢–¢–¢–¢–¢6ˆÁ7Bó5&ˆ6W75v∂WW÷“bf“Â˜6˜W&6S””“w&ˆ6W75˜v∂WWs∞¢6ˆÁ7Bó5W6W#÷“Á&ˆ∆S””“wW6W"s∞¢ñbÇó5W6W"beˆó4÷&∂W$ˆÊ«î76ó7FÁD6ˆ◊&W76ñˆ‰÷W76vRÜ“íó∞¢6ˆÁFVÁC“r¢§W'&˜#¢¢¢ÊÚ&W7ˆÁ6R&V6VófVBgFW"6ˆÁFWáB6ˆ◊&W76ñˆ‚‚∆V6R&WG'í‚s∞¢–¢6ˆÁ7BFó7∆î6ˆÁFVÁC÷ó5W6W#ı˜7G&óGF6ÜVDfñ∆W4÷&∂W$f˜$Fó7∆íÖ˜7G&óv˜&∑76TFó7∆ï&VfóÇÜ6ˆÁFVÁBíì¶6ˆÁFVÁC∞¢6ˆÁ7B&˜tFó7∆î6ˆÁFVÁC÷Fó7∆î6ˆÁFVÁC∞¢ñbÇó5W6W"beˆó476ó7FÁDV◊Gï∆6VÜˆ∆FW$6ˆÁFVÁBÜ“¬Fó7∆î6ˆÁFVÁBíó∞¢6ˆÁFVÁC“rs∞¢–¢ñbÇó5W6W"bbÜó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇó«∆ó5G&Á7&VÁE7G&V“ÇííbbFÜñÊ∂ñÊuFWáBó∞¢6ˆÁ7BGW&‰fñÊ≈fó6ñ&∆T6ˆÁFVÁC÷76ó7FÁEGW&‰fñÊ≈fó6ñ&∆T6ˆÁFVÁD'ï&tñGÇÊvWBá&tñGÇó«¬rs∞¢6ˆÁ7BGW&Âfó6ñ&∆T6ˆÁFVÁG3÷76ó7FÁEGW&Âfó6ñ&∆T6ˆÁFVÁD'ï&tñGÇÊvWBá&tñGÇó«≈µ”∞¢FÜñÊ∂ñÊuFWáC’˜v˜&∂∆ˆu&V6ˆÊñÊuFWáDg&ˆ‘÷W76vRÜ“¬&tñGÇ¬Fˆˆƒ6∆ƒ76ó7FÁDñGá2¬Fó7∆î6ˆÁFVÁB¬GW&‰fñÊ≈fó6ñ&∆T6ˆÁFVÁB¬GW&Âfó6ñ&∆T6ˆÁFVÁG2ì∞¢–¢6ˆÁ7Bó4∆7D76ó7FÁC“ó5W6W"bgfì””◊&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÇ”∞¢6ˆÁ7BÊWáE&VÊFW&VC◊&VÊFW%fó5vóFÑñGÖ∑fí≥”∞¢6ˆÁ7Bó5GW&‰fñÊƒ76ó7FÁC“ó5W6W"bbÇÊWáE&VÊFW&VG«¬ÊWáE&VÊFW&VBÊ◊«∆ÊWáE&VÊFW&VBÊ“Á&ˆ∆R”“v76ó7FÁBrì∞¢∆WBfñ∆W4áF÷√“rs∞¢ñbÜ“ÊGF6Ü÷VÁG2bf“ÊGF6Ü÷VÁG2Ê∆VÊwFÇó∞¢ÚÚ7FFñ2&Vw&W76ñˆ‚FW7G2ñÁFVÁFñˆÊ∆«í∆ˆˆ≤f˜"◊6r÷÷VFñ÷ñ÷rˆ◊6r÷fñ∆R÷&FvRÊV"FÜó2'&Ê6Ç‡¢6ˆÁ7BˆGF6Ö6ñC“Ö2Á6W76ñˆ‚be2Á6W76ñˆ‚Á6W76ñˆÂˆñBó«¬rs∞¢fñ∆W4áF÷√÷∆Fób6∆73“&◊6r÷fñ∆W2#‚G∂“ÊGF6Ü÷VÁG2Ê÷Üc”Á∞¢6ˆÁ7Bd∆&V√◊GóVˆbc””“w7G&ñÊrsˆc¢ÜbbbÜbÊÊ÷W«∆bÊfñ∆VÊ÷W«∆bÁFÇíó«¬rs∞¢6ˆÁ7BfÊ÷S’7G&ñÊrÜd∆&V¬íÁ7∆óBÇrÚríÁ˜Çó«≈7G&ñÊrÜd∆&V¬ì∞¢ÚÚW6Ríˆfñ∆R˜&rvÜñ6Ç&W6ˆ«fW2fñ∆VÊ÷R&V∆FófRFÚFÜR6W76ñˆ‚v˜&∑76R‡¢6ˆÁ7Bfñ∆UW&√“víˆfñ∆R˜&s˜6W76ñˆÂˆñC“r∂VÊ6ˆFUU$î6ˆ◊ˆÊVÁBÖˆGF6Ö6ñBí≤rgFÉ“r∂VÊ6ˆFUU$î6ˆ◊ˆÊVÁBÜfÊ÷Rì∞¢&WGW&‚˜&VÊFW$GF6Ü÷VÁDáF÷¬ÜfÊ÷R∆fñ∆UW&¬ì∞¢“íÊ¶ˆñ‚Çrró”¬ˆFócÊ∞¢–¢∆WB&ˆGîáF÷¬“ˆvWD66ÜVE&VÊFW"ÜFó7∆î6ˆÁFVÁB¬ó5W6W"ì∞¢ÚÚ÷W76vR÷∆WfV¬÷VFñ6Ê6Ü˜G3¢6WGF∆VB76ó7FÁB÷W76vW26''í¢ÚÚFé(i&FñvW7B÷áw&óGFV‚B6WGF∆RFñ÷Ríg&VW¶ñÊrFÜRfñ∆R'óFW2FÜP¢ÚÚGW&‚V÷óGFVB‚7F◊óBeDU"FÜRFWáB÷∂WñVB&VÊFW"66ÜR6ÚñFVÁFñ6¿¢ÚÚFWáBvóFÇFñffW&VÁB6Ê6Ü˜G2Üˆ∆BˆÊWr6ˆ◊&ó6ˆ‚íÊWfW"6ˆ∆∆ñFW2‡¢ñbÇó5W6W"bb“bb“Âˆ÷VFñ˜6Ê6Ü˜G2bbGóVˆb“Âˆ÷VFñ˜6Ê6Ü˜G3””“vˆ&¶V7Bró∞¢&ˆGîáF÷¬“˜7F◊÷VFñ6Ê6Ü˜G2Ü&ˆGîáF÷¬¬“Âˆ÷VFñ˜6Ê6Ü˜G2ì∞¢–¢ñbÇó5W6W"bf“Á&˜fñFW%ˆFWFñ«2ó∞¢6ˆÁ7B7V÷÷'ì÷“Á&˜fñFW%ˆFWFñ«5ˆ∆&V««¬u&˜fñFW"FWFñ«2s∞¢&ˆGîáF÷¬≥“∆FWFñ«26∆73“'&˜fñFW"÷W'&˜"÷FWFñ«2#„«7V÷÷'ì‚G∂W62Ö7G&ñÊrá7V÷÷'ííó”¬˜7V÷÷'ì„«&S„∆6ˆFS‚G∂W62Ö7G&ñÊrÜ“Á&˜fñFW%ˆFWFñ«2íó”¬ˆ6ˆFS„¬˜&S„¬ˆFWFñ«3Ê∞¢–¢6ˆÁ7B&V6˜fW'ïñ∆ˆC“Çó5W6W"bf“Âˆ6ˆ◊&W76ñˆÂ&V6˜fW'íê¢Ú“Âˆ6ˆ◊&W76ñˆÂ&V6˜fW'ê¢¢Çó5W6W"bfó4∆7D76ó7FÁBbfó5GW&‰fñÊƒ76ó7FÁBbgGóVˆbˆ7FófT6ˆ◊&W76ñˆÂ&V6˜fW'ïñ∆ˆC””“vgVÊ7Fñˆ‚rÚˆ7FófT6ˆ◊&W76ñˆÂ&V6˜fW'ïñ∆ˆBÇí¢ÁV∆¬ì∞¢6ˆÁ7B&V6˜fW'îáF÷√◊&V6˜fW'ïñ∆ˆBÚˆ6ˆ◊&W76ñˆÂ&V6˜fW'îáF÷¬á&V6˜fW'ïñ∆ˆB¬Ö2Á6W76ñˆ‚be2Á6W76ñˆ‚Á6W76ñˆÂˆñBó«¬rrí¢rs∞¢ñbá&V6˜fW'îáF÷¬í&ˆGîáF÷¬≥“&V6˜fW'îáF÷√∞¢6ˆÁ7B7FGW4áF÷¬“Çó5W6W"bf“Â˜7FGW46&BíÚ˜7FGW46&DáF÷¬Ü“Â˜7FGW46&Bí¢rs∞¢6ˆÁ7Bó4VFóF&∆UW6W#÷ó5W6W"bg&tñGÉ””÷∆7EW6W%&tñGÉ∞¢6ˆÁ7BVFóD'F‚“ó4VFóF&∆UW6W"Ú∆'WGFˆ‚6∆73“&◊6r÷7Fñˆ‚÷'F‚"FóF∆S“"G∑BÇvVFóEˆ÷W76vRró“"ˆÊ6∆ñ6≥“&VFóD÷W76vRáFÜó2í#‚G∂∆íÇwVÊ6ñ¬r√2ó”¬ˆ'WGFˆ„Ê¢rs∞¢6ˆÁ7BVÊFÙ'F‚“ó4∆7D76ó7FÁBÚ∆'WGFˆ‚6∆73“&◊6r÷7Fñˆ‚÷'F‚"FóF∆S“"G∑BÇwVÊFıˆWÜ6ÜÊvRró“"ˆÊ6∆ñ6≥“'VÊFÙ∆7DWÜ6ÜÊvRÇí#‚G∂∆íÇwVÊFÚr√2ó”¬ˆ'WGFˆ„Ê¢rs∞¢6ˆÁ7B&WG'î'F‚“ó4∆7D76ó7FÁBÚ∆'WGFˆ‚6∆73“&◊6r÷7Fñˆ‚÷'F‚"FóF∆S“"G∑BÇw&VvVÊW&FRró“"ˆÊ6∆ñ6≥“'&VvVÊW&FU&W7ˆÁ6RáFÜó2í#‚G∂∆íÇw&˜FFR÷67rr√2ó”¬ˆ'WGFˆ„Ê¢rs∞¢6ˆÁ7B6˜î'F‚“∆'WGFˆ‚6∆73“&◊6r÷6˜í÷'F‚◊6r÷7Fñˆ‚÷'F‚"FóF∆S“"G∑BÇv6˜író“"ˆÊ6∆ñ6≥“&6˜î◊6ráFÜó2í#‚G∂∆íÇv6˜ír√2ó”¬ˆ'WGFˆ„Ê∞¢6ˆÁ7B&VDˆÊ«ï6W76ñˆ„◊GóVˆbˆó5&VDˆÊ«ï6W76ñˆ„””“vgVÊ7Fñˆ‚p¢Úˆó5&VDˆÊ«ï6W76ñˆ‚Ö2Á6W76ñˆ‚ê¢¢Ö2Á6W76ñˆ‚bbÖ2Á6W76ñˆ‚Á&VEˆˆÊ«ó«≈2Á6W76ñˆ‚Êó5˜&VEˆˆÊ«ííì∞¢6ˆÁ7B'&Ê6Ü&∆U&VDˆÊ«ï6W76ñˆ„◊GóVˆbˆó4'&Ê6Ü&∆U&VDˆÊ«ï6W76ñˆ„””“vgVÊ7Fñˆ‚p¢Úˆó4'&Ê6Ü&∆U&VDˆÊ«ï6W76ñˆ‚Ö2Á6W76ñˆ‚ê¢¢f«6S∞¢6ˆÁ7Bf˜&¥'F‚“á&VDˆÊ«ï6W76ñˆ‚bb'&Ê6Ü&∆U&VDˆÊ«ï6W76ñˆ‚íÚrr¢∆'WGFˆ‚6∆73“&◊6r÷7Fñˆ‚÷'F‚"FóF∆S“"G∑BÇvf˜&µˆg&ˆ’ˆÜW&Rró“"ˆÊ6∆ñ6≥“&f˜&¥g&ˆ‘÷W76vRÇG∑&tñGÇ≥“í#‚G∂∆íÇvvóB÷'&Ê6Çr√2ó”¬ˆ'WGFˆ„Ê∞¢6ˆÁ7BGG4'F‚“ó5W6W"Ú∆'WGFˆ‚6∆73“&◊6r÷7Fñˆ‚÷'F‚◊6r◊GG2÷'F‚"FóF∆S“"G∑BÇwGG5ˆ∆ó7FV‚ró«¬t∆ó7FV‚w“"ˆÊ6∆ñ6≥“'7V¥÷W76vRáFÜó2í#‚G∂∆íÇwfˆ«V÷R”"r√2ó”¬ˆ'WGFˆ„Ê¢rs∞¢6ˆÁ7BG5f√÷“Â˜G7«∆“ÁFñ÷W7F◊∞¢ÚÚˆf˜&÷DñÂ6W'fW%G¢ÜÊF∆W2g&7FñˆÊ¬÷Ü˜W"ˆfg6WG2ÑñÊFñ≥S3WF2‚ê¢ÚÚ6˜'&V7F«ífñˆfg6WB&óFÜ÷WFñ3≤&&RFÙ∆ˆ6∆U7G&ñÊró2FÜR'&˜w6W"◊G¢f∆∆&6≤‡¢6ˆÁ7Bˆf◊E7c“áGóVˆbˆf˜&÷DñÂ6W'fW%G£””“vgVÊ7Fñˆ‚rìıˆf˜&÷DñÂ6W'fW%G£¶ÁV∆√∞¢6ˆÁ7BG5FóF∆S◊G5f√ÚÖˆf◊E7cıˆf◊E7bÜÊWrFFRáG5f¬£í«∑“ì¶ÊWrFFRáG5f¬£íÁFÙ∆ˆ6∆U7G&ñÊrÇíì¢rs∞¢6ˆÁ7BG5Fñ÷S’ˆf˜&÷D÷W76vTfˆ˜FW%Fñ÷W7F◊áG5f¬ì∞¢6ˆÁ7BFñ÷TáF÷¬“G5Fñ÷RÚ«7‚6∆73“&◊6r◊Fñ÷R"FóF∆S“"G∂W62áG5FóF∆Ró“#‚G∑G5Fñ÷W”¬˜7„Ê¢rs∞¢ÚÚ33C¢6Ü˜rßV◊◊FÚ◊VW7Fñˆ‚ˆ‚WfW'í76ó7FÁB÷W76vRFÜBÜ2¢ÚÚ&W6ˆ«f&∆RVW7Fñˆ‚F&vWB¬Ê˜BßW7BFÜRGW&‚÷fñÊ¬ˆÊR‚◊V«Fí◊7FW ¢ÚÚGW&Á2áFˆˆ≈ˆ6∆¬”‚76ó7FÁB”‚Fˆˆ≈ˆ6∆¬”‚76ó7FÁBí˜FÜW'vó6P¢ÚÚ7G&óFÜR'WGFˆ‚g&ˆ“WfW'íñÁFW&÷VFñFR76ó7FÁB'V&&∆RÊBFÜP¢ÚÚW6W"∆˜6W2FÜRÊfñvFñˆ‚ff˜&FÊ6R‡¢6ˆÁ7B˜ßV◊F&vWC“Çó5W6W"bb“Âˆ∆ófRì˜VW7FñˆÂ&tñGÑ'î76ó7FÁE&tñGÇÊvWBá&tñGÇìßVÊFVfñÊVC∞¢6ˆÁ7BVW7Fñˆ‰ßV◊'F‚“Ö˜ßV◊F&vWB”◊VÊFVfñÊVBbe˜ßV◊F&vWB”÷ÁV∆¬ê¢Ú˜VW7Fñˆ‰ßV◊'WGFˆ‰áF÷¬Ö˜ßV◊F&vWB¬76ó7FÁE&tñGÑ'ïVW7FñˆÂ&tñGÇÊvWBÖ˜ßV◊F&vWBìÛ˜&tñGÇê¢¢rs∞¢6ˆÁ7Bfˆ˜DáF÷¬“∆Fób6∆73“&◊6r÷fˆ˜B#‚G∑Fñ÷TáF÷«”«7‚6∆73“&◊6r÷7FñˆÁ2#‚G∂VFóD'FÁ“G∑GG4'FÁ“G∂f˜&¥'FÁ“G∂6˜î'FÁ“G∑&WG'î'FÁ”¬˜7„‚G∑VW7Fñˆ‰ßV◊'FÁ”¬ˆFócÊ∞†¢ñbÖˆó46ˆÁFWáD6ˆ◊7Fñˆ‰÷W76vRÜ“íó∞¢6ˆÁFñÁVS∞¢–†¢ñbÜó5&ˆ6W75v∂WWó∞¢7W'&VÁD76ó7FÁEGW&„÷ÁV∆√∞¢∆WB&˜s’ˆ◊6tÊˆFU&V7ñ6∆TVÊ&∆VCı˜&V7ñ6∆U7F6ÇÊvWBá&tñGÇì¶ÁV∆√∞¢ñbá&˜rbbÇ&˜rÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv◊6r◊&˜rró««&˜rÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv76ó7FÁB◊GW&‚rííí&˜s÷ÁV∆√∞¢6ˆÁ7B&ˆ6W75FWáC’7G&ñÊrá&˜tFó7∆î6ˆÁFVÁG«¬rríÁG&ñ“Çì∞¢6ˆÁ7B&ˆ6W74fˆ˜DáF÷√÷∆Fób6∆73“&◊6r÷fˆ˜B#‚G∑Fñ÷TáF÷«”«7‚6∆73“&◊6r÷7FñˆÁ2#‚G∂6˜î'FÁ”¬˜7„„¬ˆFócÊ∞¢ÚÚ3c3CS¢7G'V7GW&VB6ˆ◊∆WFñˆÁ2˜vF6Ç÷÷F6ÜW2&VÊFW"26ˆ∆∆6V@¢ÚÚ7V÷÷'í6&C≤ÁóFÜñÊrVÁ'6V&∆R∂VW2FÜR&rÊ˜Fñ6R&V∆˜r6ÚFÜP¢ÚÚf∆∆&6≤ó2ÊWfW"v˜'6RFÜ‚FÜRˆ∆BgV∆¬◊FWáBGV◊‡¢6ˆÁ7Bv∂WWñÊfÛ’˜&ˆ6W75v∂WWñÊfÚÜ“¬&ˆ6W75FWáBì∞¢∆WBÊ˜Fñ6T6∆73“w&ˆ6W72◊v∂WW÷Ê˜Fñ6Rs∞¢∆WBÊ˜Fñ6TñÊÊW$áF÷√∞¢ñbáv∂WWñÊfÚó∞¢Ê˜Fñ6T6∆72≥“r&ˆ6W72◊v∂WW÷Ê˜Fñ6R÷6&Bs∞¢6ˆÁ7BWÜóE7G#◊v∂WWñÊfÚÊWÜóD6ˆFS”÷ÁV∆√Úrs•7G&ñÊráv∂WWñÊfÚÊWÜóD6ˆFRì∞¢ñbáv∂WWñÊfÚÁGóS””“v6ˆ◊∆WFñˆ‚rbbı‚”ı∆B≤BÚÁFW7BÜWÜóE7G"íbfWÜóE7G"”“sríÊ˜Fñ6T6∆72≥“r&ˆ6W72◊v∂WW÷fñ¬s∞¢Ê˜Fñ6TñÊÊW$áF÷√’˜&ˆ6W75v∂WW6&DáF÷¬áv∂WWñÊfÚ¬&ˆ6W75FWáB¬∑Fñ÷TáF÷¬¬fñ∆W4áF÷¬¬fˆ˜DáF÷√¶∆Fób6∆73“&◊6r÷fˆ˜B#„«7‚6∆73“&◊6r÷7FñˆÁ2#‚G∂6˜î'FÁ”¬˜7„„¬ˆFócÊ“ì∞¢÷V«6W∞¢6ˆÁ7B&ˆ6W75FWáDáF÷√◊&ˆ6W75FWáCˆ«&R6∆73“'&ˆ6W72◊v∂WW◊FWáB#‚G∂W62á&ˆ6W75FWáBó”¬˜&SÊ¢rs∞¢Ê˜Fñ6TñÊÊW$áF÷√÷∆Fób6∆73“'&ˆ6W72◊v∂WW÷∆&V¬#‚G∂∆íÇwFW&÷ñÊ¬r√2ó”«7„‚G∂W62áBÇw&ˆ6W75˜v∂WWˆ∆&V¬ríó”¬˜7„„¬ˆFóc‚G∂fñ∆W4áF÷«”∆Fób6∆73“&◊6r÷&ˆGí&ˆ6W72◊v∂WW÷&ˆGí#‚G∑&ˆ6W75FWáDáF÷«”¬ˆFóc‚G∑&ˆ6W74fˆ˜DáF÷«÷∞¢–¢6ˆÁ7BÊWáE&˜táF÷√÷∆Fób6∆73“"G∂Ê˜Fñ6T6∆77“#‚G∂Ê˜Fñ6TñÊÊW$áF÷«”¬ˆFócÊ∞¢ñbá&˜ró∞¢&˜rÊ6∆74Ê÷S“v◊6r◊&˜r&ˆ6W72◊v∂WW◊&˜rs∞¢&˜rÊñC’˜W6W$÷W76vTFˆ‘ñBá&tñGÇì∞¢&˜rÊFF6WBÊ◊6tñGÉ◊&tñGÉ∞¢&˜rÊFF6WBÁ6W76ñˆ‰◊6tñGÉ’ˆ÷W76vU6W76ñˆ‰ñÊFWÑf˜%&tñGÇá&tñGÇì∞¢&˜rÊFF6WBÊ÷W76vTÊ6Ü˜$∂Wì’ˆ÷W76vUfñWw˜'DÊ6Ü˜$∂Wîf˜$÷W76vRÜ“ì∞¢&˜rÊFF6WBÁ&ˆ∆S“w&ˆ6W75˜v∂WWs∞¢FV∆WFR&˜rÊFF6WBÊVFóFñÊs∞¢ÚÚ6ˆ◊&RvñÁ7BFÜRÖD‘¬vR∆7B4UBÜWáÊFÚí¬Ê˜B∆ófRñÊÊW$ÖD‘√†¢ÚÚW6W"÷WáÊFVB∆FWFñ«3‚6W&ñ∆ó¶W2‚˜V‚GG&ñ'WFRñÁF¢ÚÚñÊÊW$ÖD‘¬¬vÜñ6Çv˜V∆Bf˜&6R&V'Vñ∆B÷ÊB÷6ˆ∆∆6Rˆ‚WfW'ê¢ÚÚ7G&V÷ñÊr&W&VÊFW"‚FÜRWáÊFÚ6ˆ◊&ó6ˆ‚ó0¢ÚÚ6W&ñ∆ó¶Fñˆ‚÷ñÊFWVÊFVÁBvÜñ∆R7Fñ∆¬&V'Vñ∆FñÊrvÜV‚FÜR÷&∑W ¢ÚÚvVÁVñÊV«í6ÜÊvW2Ü∆ˆ6∆R˜Fñ÷W7F◊f˜&÷Bì≤˜V‚7FFRó0¢ÚÚW6W"÷G&ófV‚¬6ÚóBó26GW&VBÊB&W7F˜&VB7&˜72&V'Vñ∆G2‡¢ñbá&˜rÊFF6WBÁ&uFWáB”◊&ˆ6W75FWáG««&˜rÂ˜v∂WW&VÊFW&VDáF÷¬”÷ÊWáE&˜táF÷¬ó∞¢6ˆÁ7B˜&ñ˜$6&C◊&˜rÁVW'ï6V∆V7F˜"bg&˜rÁVW'ï6V∆V7F˜"ÇvFWFñ«2Á&ˆ6W72◊v∂WW÷6&Brì∞¢6ˆÁ7B˜v4˜V„“Ö˜&ñ˜$6&Bbe˜&ñ˜$6&BÊ˜V‚ì∞¢&˜rÊFF6WBÁ&uFWáC◊&ˆ6W75FWáC∞¢&˜rÂ˜v∂WW&VÊFW&VDáF÷√÷ÊWáE&˜táF÷√∞¢&˜rÊñÊÊW$ÖD‘√÷ÊWáE&˜táF÷√∞¢ñbÖ˜v4˜V‚ó∞¢6ˆÁ7Bˆ6&C◊&˜rÁVW'ï6V∆V7F˜"ÇvFWFñ«2Á&ˆ6W72◊v∂WW÷6&Brì∞¢ñbÖˆ6&Bíˆ6&BÊ˜V„◊G'VS∞¢–¢–¢÷V«6W∞¢&˜s÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢&˜rÊ6∆74Ê÷S“v◊6r◊&˜r&ˆ6W72◊v∂WW◊&˜rs∞¢&˜rÊñC’˜W6W$÷W76vTFˆ‘ñBá&tñGÇì∞¢&˜rÊFF6WBÊ◊6tñGÉ◊&tñGÉ∞¢&˜rÊFF6WBÁ6W76ñˆ‰◊6tñGÉ’ˆ÷W76vU6W76ñˆ‰ñÊFWÑf˜%&tñGÇá&tñGÇì∞¢&˜rÊFF6WBÊ÷W76vTÊ6Ü˜$∂Wì’ˆ÷W76vUfñWw˜'DÊ6Ü˜$∂Wîf˜$÷W76vRÜ“ì∞¢&˜rÊFF6WBÁ&ˆ∆S“w&ˆ6W75˜v∂WWs∞¢&˜rÊFF6WBÁ&uFWáC◊&ˆ6W75FWáC∞¢&˜rÂ˜v∂WW&VÊFW&VDáF÷√÷ÊWáE&˜táF÷√∞¢&˜rÊñÊÊW$ÖD‘√÷ÊWáE&˜táF÷√∞¢–¢ñÊÊW"ÊVÊD6Üñ∆Bá&˜rì∞¢W6W%&˜w2Á6WBá&tñGÇ¬&˜rì∞¢6ˆÁFñÁVS∞¢–†¢ñbÜó5W6W"ó∞¢7W'&VÁD76ó7FÁEGW&„÷ÁV∆√∞¢∆WB&˜s’ˆ◊6tÊˆFU&V7ñ6∆TVÊ&∆VCı˜&V7ñ6∆U7F6ÇÊvWBá&tñGÇì¶ÁV∆√∞¢ñbá&˜rbbÇ&˜rÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv◊6r◊&˜rró««&˜rÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv76ó7FÁB◊GW&‚rííí&˜s÷ÁV∆√∞¢6ˆÁ7BÊWu&uFWáC’7G&ñÊrÜFó7∆î6ˆÁFVÁBíÁG&ñ“Çì∞¢6ˆÁ7BÊWáE&˜táF÷√÷G∂fñ∆W4áF÷«”∆Fób6∆73“&◊6r÷&ˆGí#‚G∂&ˆGîáF÷«”¬ˆFóc‚G∂fˆ˜DáF÷«÷∞¢ñbá&˜ró∞¢&˜rÊ6∆74Ê÷S“v◊6r◊&˜rs∞¢&˜rÊñC’˜W6W$÷W76vTFˆ‘ñBá&tñGÇì∞¢&˜rÊFF6WBÊ◊6tñGÉ◊&tñGÉ∞¢&˜rÊFF6WBÁ6W76ñˆ‰◊6tñGÉ’ˆ÷W76vU6W76ñˆ‰ñÊFWÑf˜%&tñGÇá&tñGÇì∞¢&˜rÊFF6WBÊ÷W76vTÊ6Ü˜$∂Wì’ˆ÷W76vUfñWw˜'DÊ6Ü˜$∂Wîf˜$÷W76vRÜ“ì∞¢&˜rÊFF6WBÁ&ˆ∆S“wW6W"s∞¢FV∆WFR&˜rÊFF6WBÊVFóFñÊs∞¢ñbá&˜rÊFF6WBÁ&uFWáB”÷ÊWu&uFWáG««&˜rÊñÊÊW$ÖD‘¬”÷ÊWáE&˜táF÷¬ó∞¢&˜rÊFF6WBÁ&uFWáC÷ÊWu&uFWáC∞¢&˜rÊñÊÊW$ÖD‘√÷ÊWáE&˜táF÷√∞¢–¢÷V«6W∞¢&˜s÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢&˜rÊ6∆74Ê÷S“v◊6r◊&˜rs∞¢&˜rÊñC’˜W6W$÷W76vTFˆ‘ñBá&tñGÇì∞¢&˜rÊFF6WBÊ◊6tñGÉ◊&tñGÉ∞¢&˜rÊFF6WBÁ6W76ñˆ‰◊6tñGÉ’ˆ÷W76vU6W76ñˆ‰ñÊFWÑf˜%&tñGÇá&tñGÇì∞¢&˜rÊFF6WBÊ÷W76vTÊ6Ü˜$∂Wì’ˆ÷W76vUfñWw˜'DÊ6Ü˜$∂Wîf˜$÷W76vRÜ“ì∞¢&˜rÊFF6WBÁ&ˆ∆S“wW6W"s∞¢&˜rÊFF6WBÁ&uFWáC÷ÊWu&uFWáC∞¢&˜rÊñÊÊW$ÖD‘√÷ÊWáE&˜táF÷√∞¢–¢ÚÚ&W6W'fRFÜó2W6W"&˜rw2&V¬ˆfb◊67&VV‚ÜVñváBWg&ˆÁB6ÚvóR÷ÊB◊&V'Vñ∆@¢ÚÚFˆW2Ê˜B6ˆ∆∆6R67&ˆ∆ƒÜVñváBFÚFÜRf∆BìgÇW7Fñ÷FRáFÜR6ˆ∆∆6RFÜ@¢ÚÚ6∆◊2˜&R÷Ê6Ü˜'2FÜRfñWw˜'Bˆ‚÷ˆ&ñ∆R(	B3Sc3rÚ3Sc3Ç¬&˜FÇßV◊6∆76W2í‚W6W0¢ÚÚFÜR&V÷V÷&W&VB÷V7W&VBÜVñváBvÜV‚FÜó2&˜rÜ2&VV‚÷V7W&VB&Vf˜&R¬V«6R¢ÚÚ6ˆÁFVÁB÷∆VÊwFÇW7Fñ÷FS≤FÜR÷V7W&R72&VfñÊW2óBWÜ7F«íÊWáBg&÷R‚FÜP¢ÚÚGóVˆbwV&B∂VW2&VÊFW$÷W76vW2'VÊÊ&∆Rñ‚FÜRÊˆFRFW7BÜ&ÊW76W2FÜ@¢ÚÚWáG&7BóBvóFÜ˜WBFÜó2ÜV«W"áFÜWí7GV"WfW'í6ˆ∆∆&˜&F˜"'íÊ÷Rí‡¢ñbáGóVˆbˆ«ïW6W%&˜tñÁG&ñÁ6ñ4ÜVñváC””“vgVÊ7Fñˆ‚ríˆ«ïW6W%&˜tñÁG&ñÁ6ñ4ÜVñváBá&˜r¬ÊWu&uFWáBì∞¢ñÊÊW"ÊVÊD6Üñ∆Bá&˜rì∞¢W6W%&˜w2Á6WBá&tñGÇ¬&˜rì∞¢6ˆÁFñÁVS∞¢–†¢ñbÇ7W'&VÁD76ó7FÁEGW&‚ó∞¢∆WB&V7ñ6∆VC’ˆ◊6tÊˆFU&V7ñ6∆TVÊ&∆VCı˜&V7ñ6∆U7F6ÇÊvWBá&tñGÇì¶ÁV∆√∞¢ñbá&V7ñ6∆VBbb&V7ñ6∆VBÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv76ó7FÁB◊GW&‚ríí&V7ñ6∆VC÷ÁV∆√∞¢ñbá&V7ñ6∆VBó∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2á&V7ñ6∆VBì∞¢ñbÜ&∆ˆ6∑2í&∆ˆ6∑2ÊñÊÊW$ÖD‘√“rs∞¢f˜"Ü6ˆÁ7BGG"ˆb˜&V7ñ6∆U&W6WDGG'2í&V7ñ6∆VBÁ&V÷˜fTGG&ñ'WFRÜGG"ì∞¢6ˆÁ7B&ˆ∆S◊&V7ñ6∆VBÁVW'ï6V∆V7F˜"ÇrÊ◊6r◊&ˆ∆RÊ76ó7FÁBrì∞¢ñbá&ˆ∆Rí&ˆ∆RÊ˜WFW$ÖD‘√’ˆ76ó7FÁE&ˆ∆TáF÷¬áG5FóF∆R¬ó5G4Fó7∆îVÊ&∆VBÇìıˆf˜&÷EGW&ÂG2Ü“Â˜GW&ÂG2ì¢rrì∞¢7W'&VÁD76ó7FÁEGW&„◊&V7ñ6∆VC∞¢÷V«6W∞¢7W'&VÁD76ó7FÁEGW&„’ˆ7&VFT76ó7FÁEGW&‚áG5FóF∆R¬ó5G4Fó7∆îVÊ&∆VBÇìıˆf˜&÷EGW&ÂG2Ü“Â˜GW&ÂG2ì¢rrì∞¢–¢7W'&VÁD76ó7FÁEGW&‚ÊFF6WBÁ&ˆ∆S“v76ó7FÁBs∞¢ñbÖ2Á6W76ñˆ‚í7W'&VÁD76ó7FÁEGW&‚ÊFF6WBÁ6W76ñˆ‰ñC’2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢7W'&VÁD76ó7FÁEGW&‚ÊFF6WBÁ&V7ñ6∆T∂Wì◊&tñGÉ∞¢ñÊÊW"ÊVÊD6Üñ∆BÜ7W'&VÁD76ó7FÁEGW&‚ì∞¢–¢˜6WD∆FW7D76ó7FÁEGW&‰∆ÊF÷&≤Ü7W'&VÁD76ó7FÁEGW&‚¬“Âˆ∆ófRbg&tñGÉ””÷∆FW7E&VÊFW&VD76ó7FÁE&tñGÇì∞¢6ˆÁ7B6Vs÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢ñbÑ'&íÊó4'&íÜ˜&FW&VEG&Á7&VÁE'G2íbf˜&FW&VEG&Á7&VÁE'G2Ê∆VÊwFÇó∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2Ü7W'&VÁD76ó7FÁEGW&‚ì∞¢6ˆÁ7B6W76ñˆ‰◊6tñGÉ’ˆ÷W76vU6W76ñˆ‰ñÊFWÑf˜%&tñGÇá&tñGÇì∞¢6ˆÁ7B÷W76vTÊ6Ü˜$∂Wì’ˆ÷W76vUfñWw˜'DÊ6Ü˜$∂Wîf˜$÷W76vRÜ“ì∞¢6ˆÁ7B∆7EFWáE'DñGÉ“ÇÇì”Á∞¢f˜"Ü∆WBì÷˜&FW&VEG&Á7&VÁE'G2Ê∆VÊwFÇ”∂ì„”∂í““ó∞¢ñbÄ¢˜&FW&VEG&Á7&VÁE'G5∂ï“b`¢˜&FW&VEG&Á7&VÁE'G5∂ï“Ê∂ñÊC””“wFWáBrb`¢7G&ñÊrÖ˜G&Á7&VÁD˜&FW&VDFó7∆ïFWáBÜ˜&FW&VEG&Á7&VÁE'G5∂ï“ÁFWáBííÁG&ñ“Çê¢í&WGW&‚ì∞¢–¢&WGW&‚”∞¢“íÇì∞¢∆WBfó'7E6Vs÷ÁV∆√∞¢ñbáFÜñÊ∂ñÊuFWáBbgvñÊF˜rÂ˜6Ü˜uFÜñÊ∂ñÊr”÷f«6Ró∞¢ñbÇÜó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇó«∆ó5G&Á7&VÁE7G&V“Çííbeˆ76ó7FÁEFÜñÊ∂ñÊt&V∆ˆÊw4ñÂv˜&∂∆ˆrÜ“¬&tñGÇ¬Fˆˆƒ6∆ƒ76ó7FÁDñGá2íí76ó7FÁEFÜñÊ∂ñÊrÁ6WBá&tñGÇ¬FÜñÊ∂ñÊuFWáBì∞¢–¢˜&FW&VEG&Á7&VÁE'G2Êf˜$V6ÇÇá'B¬'DñGÇì”Á∞¢ñbÇ'Bí&WGW&„∞¢ñbá'BÊ∂ñÊC””“wFˆˆ¬ró∞¢6ˆÁ7BFˆˆƒ6∆√’˜G&Á7&VÁD˜&FW&VEFˆˆƒ6∆¬á'B¬&tñGÇ¬G&Á7&VÁD˜&FW&VEFˆˆƒ6∆«4'ïFñB¬G&Á7&VÁEFˆˆ≈&W7V«G4'ïFñB¬G&Á7&VÁEW'6ó7FVE6ÊóWD'ïFñBì∞¢6ˆÁ7BFˆˆ≈&˜s’ˆFV6˜&FUG&Á7&VÁDWfVÁE&˜rÜ'Vñ∆EFˆˆƒ6&BáFˆˆƒ6∆¬í«∞¢GóS¢wFˆˆ¬r¿¢Ê÷SßFˆˆƒ6∆¬bgFˆˆƒ6∆¬ÊÊ÷R¿¢7FGW3•˜G&Á7&VÁEFˆˆ≈7FGW2áFˆˆƒ6∆¬«G'VRí¿¢Fˆˆƒ6∆¬¿¢6Vv÷VÁE6WßFˆˆƒ6∆¬bgFˆˆƒ6∆¬Ê7FófóGï6Vv÷VÁE6W¿¢'W'7DñC¢áFˆˆƒ6∆¬bgFˆˆƒ6∆¬Ê7FófóGî'W'7DñBó«∆“Âˆ7FófóGî'W'7DñB¿¢“ì∞¢&∆ˆ6∑2ÊVÊD6Üñ∆BáFˆˆ≈&˜rì∞¢ñbá'BÁFˆˆ≈W6TñBíG&Á7&VÁD˜&FW&VEFˆˆƒñG2ÊFBá'BÁFˆˆ≈W6TñBì∞¢&WGW&„∞¢–¢6ˆÁ7B˜&FW&VE6Vs÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢6ˆÁ7B'DFó7∆ïFWáC’˜G&Á7&VÁD˜&FW&VDFó7∆ïFWáBá'BÁFWáBì∞¢ñbÇ7G&ñÊrá'DFó7∆ïFWáBíÁG&ñ“Çíí&WGW&„∞¢˜&FW&VE6VrÊ6∆74Ê÷S“v76ó7FÁB◊6Vv÷VÁBs∞¢˜&FW&VE6VrÊFF6WBÊ◊6tñGÉ◊&tñGÉ∞¢˜&FW&VE6VrÊFF6WBÁ6W76ñˆ‰◊6tñGÉ◊6W76ñˆ‰◊6tñGÉ∞¢˜&FW&VE6VrÊFF6WBÊ÷W76vTÊ6Ü˜$∂Wì÷÷W76vTÊ6Ü˜$∂Wì∞¢˜&FW&VE6VrÊFF6WBÁ&uFWáC’7G&ñÊrá'DFó7∆ïFWáG«¬rríÁG&ñ“Çì∞¢ñbÜ“Âˆ7FófóGî'W'7DñB”◊VÊFVfñÊVBbf“Âˆ7FófóGî'W'7DñB”÷ÁV∆¬í˜&FW&VE6VrÁ6WDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBr≈7G&ñÊrÜ“Âˆ7FófóGî'W'7DñBíì∞¢ñbÑÁV÷&W"Êó4fñÊóFRÑÁV÷&W"Ü“Âˆ∆ófU6Vv÷VÁE6Wííí˜&FW&VE6VrÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wr≈7G&ñÊrÑÁV÷&W"Ü“Âˆ∆ófU6Vv÷VÁE6Wííì∞¢ñbÖÙU%%Ù’4uı$RÁFW7BÖ7G&ñÊrá'DFó7∆ïFWáG«¬rríÁG&ñ“Çííí˜&FW&VE6VrÊFF6WBÊW'&˜#“ss∞¢ñbÇfó'7E6VrbgFÜñÊ∂ñÊuFWáBbgvñÊF˜rÂ˜6Ü˜uFÜñÊ∂ñÊr”÷f«6RbbÇÜó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇó«∆ó5G&Á7&VÁE7G&V“Çííbeˆ76ó7FÁEFÜñÊ∂ñÊt&V∆ˆÊw4ñÂv˜&∂∆ˆrÜ“¬&tñGÇ¬Fˆˆƒ6∆ƒ76ó7FÁDñGá2ííí˜&FW&VE6VrÊñÁ6W'DF¶6VÁDÖD‘¬Çv&Vf˜&VVÊBr¬˜FÜñÊ∂ñÊt6&DáF÷¬áFÜñÊ∂ñÊuFWáBíì∞¢6ˆÁ7Bó4∆7EFWáE'C◊'DñGÉ””÷∆7EFWáE'DñGÉ∞¢6ˆÁ7B'D&ˆGîáF÷√’ˆvWD66ÜVE&VÊFW"á'DFó7∆ïFWáB∆f«6Rì∞¢ÚÚ÷W76vR÷∆WfV¬÷VFñ6Ê6Ü˜G3¢G&Á7&VÁB˜&FW&VB6Vv÷VÁG26''íFÜP¢ÚÚ6÷RW"÷÷W76vRFé(i&FñvW7B÷2FÜR÷ñ‚G&Á67&óC≤7F◊óB6¢ÚÚÜó7F˜&ñ6¬&WfñWw2g&VW¶RÇg6Ê“íñÁ7FVBˆbfˆ∆∆˜vñÊr˜fW'w&óFW2‡¢ÚÚñÊ∆ñÊVBñ‚FÜRFV◊∆FRÜÊÚñÁFW&÷VFñFRf&ñ&∆Rí6ÚFW7B÷Ü&ÊW70¢ÚÚ&∆ˆ6≤WáG&7Fñˆ‚ˆbFÜR˜&FW&VB◊6Vv÷VÁB6∆ñ6R7Fó26V∆b÷6ˆÁFñÊVB‡¢ñbÜó4∆7EFWáE'Bbg7FGW4áF÷¬ó∞¢˜&FW&VE6VrÊñÁ6W'DF¶6VÁDÖD‘¬Çv&Vf˜&VVÊBr¬7FGW4áF÷¬ì∞¢–¢ˆñÁ6W'E6Vv÷VÁD&∆ˆ6≤Ü˜&FW&VE6Vr¬G∂ó4∆7EFWáE'Cˆfñ∆W4áF÷√¢rw”∆Fób6∆73“&◊6r÷&ˆGí#‚G≤áGóVˆb“”“wVÊFVfñÊVBrbf“bf“Âˆ÷VFñ˜6Ê6Ü˜G2bgGóVˆb“Âˆ÷VFñ˜6Ê6Ü˜G3””“vˆ&¶V7Brìı˜7F◊÷VFñ6Ê6Ü˜G2á'D&ˆGîáF÷¬∆“Âˆ÷VFñ˜6Ê6Ü˜G2ìß'D&ˆGîáF÷«”¬ˆFóc‚G∂ó4∆7EFWáE'Cˆfˆ˜DáF÷√¢rw÷ì∞¢&∆ˆ6∑2ÊVÊD6Üñ∆BÜ˜&FW&VE6Vrì∞¢ñbÇfó'7E6Vrífó'7E6Vs÷˜&FW&VE6Vs∞¢“ì∞¢76ó7FÁE6Vv÷VÁG2Á6WBá&tñGÇ¬fó'7E6Vw«∆ÁV∆¬ì∞¢6ˆÁFñÁVS∞¢–¢6VrÊ6∆74Ê÷S“v76ó7FÁB◊6Vv÷VÁBs∞¢6VrÊFF6WBÊ◊6tñGÉ◊&tñGÉ∞¢6VrÊFF6WBÁ6W76ñˆ‰◊6tñGÉ’ˆ÷W76vU6W76ñˆ‰ñÊFWÑf˜%&tñGÇá&tñGÇì∞¢6VrÊFF6WBÊ÷W76vTÊ6Ü˜$∂Wì’ˆ÷W76vUfñWw˜'DÊ6Ü˜$∂Wîf˜$÷W76vRÜ“ì∞¢6VrÊFF6WBÁ&uFWáC’7G&ñÊrÜ6ˆÁFVÁBíÁG&ñ“Çì∞¢ñbÜ“Âˆ7FófóGî'W'7DñB”◊VÊFVfñÊVBbf“Âˆ7FófóGî'W'7DñB”÷ÁV∆¬í6VrÁ6WDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBr≈7G&ñÊrÜ“Âˆ7FófóGî'W'7DñBíì∞¢ñbÑÁV÷&W"Êó4fñÊóFRÑÁV÷&W"Ü“Âˆ∆ófU6Vv÷VÁE6Wííí6VrÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wr≈7G&ñÊrÑÁV÷&W"Ü“Âˆ∆ófU6Vv÷VÁE6Wííì∞¢6ˆÁ7B÷W76vT&V∆ˆÊw4ñÂv˜&∂∆ˆs“2Ê'W7íbfó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇíbeˆ76ó7FÁD÷W76vT&V∆ˆÊw4ñÂv˜&∂∆ˆrÜ“¬&tñGÇ¬Fˆˆƒ6∆ƒ76ó7FÁDñGá2¬Fó7∆î6ˆÁFVÁB¬∂ó5GW&‰fñÊƒ76ó7FÁG“ì∞¢ñbÜ÷W76vT&V∆ˆÊw4ñÂv˜&∂∆ˆró∞¢6VrÊ6∆74∆ó7BÊFBÇv76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6Rrì∞¢6VrÁ6WDGG&ñ'WFRÇv&ñ÷ÜñFFV‚r¬wG'VRrì∞¢6VrÊÜñFFV„◊G'VS∞¢–¢ñbÜ“Âˆ∆ófRó∞¢7W'&VÁD76ó7FÁEGW&‚ÊñC“v∆ófT76ó7FÁEGW&‚s∞¢ÚÚ7F◊FÜR6W76ñˆ‚ñBˆ‚FÜR∆ófRGW&‚6ÚfñÊ∆ó¶UFÜñÊ∂ñÊt6&BÇê¢ÚÚÊB˜FÜW"∆FR6∆∆&6∑26‚fW&ñgíFÜWíw&R˜W&FñÊrˆ‚FÜP¢ÚÚ&ñváB6W76ñˆ‚w2DÙ“áFÜRW6W"÷íÜfR7vóF6ÜVBF'2˜6W76ñˆÁ0¢ÚÚvÜñ∆RFÜó27G&V“ó27Fñ∆¬7G&V÷ñÊrí‚6VR33cb‡¢ñbÖ2Á6W76ñˆ‚í7W'&VÁD76ó7FÁEGW&‚ÊFF6WBÁ6W76ñˆ‰ñC’2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢6VrÁ6WDGG&ñ'WFRÇvFF÷∆ófR÷76ó7FÁBr¬srì∞¢–¢ñbÖÙU%%Ù’4uı$RÁFW7BÖ7G&ñÊrÜ6ˆÁFVÁG«¬rríÁG&ñ“Çííí6VrÊFF6WBÊW'&˜#“ss∞¢ÚÚGW&‚vÜ˜6Rfó6ñ&∆R6ˆÁFVÁBó2V◊Gí'WBvÜñ6Ç6'&ñW26W&FP¢ÚÚ&V6ˆÊñÊvfñV∆BÜRÊr‚'V‚÷¶˜W&Ê¬◊&V6˜fW&VBÊ6Ü˜#¢V◊Gí6ˆÁFVÁB∞¢ÚÚ&V6ˆÊñÊr≤˜&V6˜fW&VEˆg&ˆ’˜'VÂˆ¶˜W&Ê∆íWáG&7G2‰ÚñÊ∆ñÊRFÜñÊ∂ñÊuFWá@¢ÚÚÊBv˜V∆B&VÊFW"ÊÚFÜñÊ∂ñÊr6&BB∆¬(	B6ˆ∆∆6ñÊrFÚ‚V◊GíÜñFFV‡¢ÚÚÊ6Ü˜"‚6W76ñˆ‚÷FRVÁFó&V«íˆb7V6Ç&˜w2FÜV‚ñÁG2&∆Ê≤ÜˆÊ«íFFP¢ÚÚ6W&F˜'2í(	BFÜR33ÉsR&W˜'FW"w2WÜ7B66RÑ6ˆ◊7BFˆˆ¬7FófóGíÙdb¿¢ÚÚíÊR‚∆Vv7í÷ˆFRí‚7W&f6RFÜR÷W76vRw2&V6ˆÊñÊrñ∆ˆB2FÜRFÜñÊ∂ñÊp¢ÚÚ6&B6˜W&6Rf˜"FÜW6RV◊Gí÷6ˆÁFVÁBGW&Á26ÚFÜRGW&‚ó2ÊWfW"&∆Ê≤‡¢Ú¢ÚÚƒTt5í‘‘ÙDRÙ‰≈íÇó56ñ◊∆ñfñVEFˆˆƒ6∆∆ñÊrÇíì¢FÜR6ñ◊∆ñfñVBıv˜&∂∆ˆrFÄ¢ÚÚ«&VGíFW&ófW2&V6ˆÊñÊr&˜fRÜ∆ñÊR„ÉCífñ¢ÚÚ˜v˜&∂∆ˆu&V6ˆÊñÊuFWáDg&ˆ‘÷W76vR¬vÜñ6Ç7G&ó2‚WÜ7Bfó6ñ&∆R÷Á7vW"V6Ü¢ÚÚ6Ú&V6ˆÊñÊrGW∆ñ6FñÊr6ñ&∆ñÊrÁ7vW"ó2Ê˜B&R◊6Ü˜v‚í‚&W˜V∆FñÊrFÜP¢ÚÚ&r&V6ˆÊñÊrÜW&Rv˜V∆B'ó72FÜBV6ÜÚ◊7G&óÊB&R◊&VÊFW"FÜRGW∆ñ6FP¢ÚÚ2v˜&∂∆ˆrFÜñÊ∂ñÊr6&BÑ6ˆFWÇvFR6F6Çí‚ñ‚∆Vv7í÷ˆFRFÜW&Ró2Ê¢ÚÚv˜&∂∆ˆrfˆ∆FñÊr¬6ÚFÜR&rñ∆ˆBó2FÜR6˜'&V7BFÜñÊ∂ñÊr÷6&B6˜W&6R‡¢ÚÚ7Fó2ıUBˆbFÜRñÊ∆ñÊR÷6ˆÁFVÁBFÜñÊ∂ñÊuFWáFWáG&7Fñˆ‚&∆ˆ6≤Ç3#ScRíÊ@¢ÚÚˆÊ«ífó&W2f˜"V◊Gí÷6ˆÁFVÁBˆÊÚ÷ñÊ∆ñÊR◊FÜñÊ∂ñÊrGW&Á2¬6ÚÁ7vW"÷&V&ñÊp¢ÚÚ÷W76vW2&RVÊ6ÜÊvVB‡¢ñbÇó5W6W"bb“Âˆ∆ófRbbó56ñ◊∆ñfñVEFˆˆƒ6∆∆ñÊrÇíbbFÜñÊ∂ñÊuFWáBbb7G&ñÊrÜ6ˆÁFVÁG«¬rríÁG&ñ“Çíbbfñ∆W4áF÷¬bb7FGW4áF÷¬ó∞¢6ˆÁ7B˜&V6ˆÊñÊuñ∆ˆC’ˆ76ó7FÁE&V6ˆÊñÊuñ∆ˆEFWáBÜ“ì∞¢ñbÖ˜&V6ˆÊñÊuñ∆ˆBíFÜñÊ∂ñÊuFWáC’˜&V6ˆÊñÊuñ∆ˆC∞¢–¢ñbáFÜñÊ∂ñÊuFWáBbgvñÊF˜rÂ˜6Ü˜uFÜñÊ∂ñÊr”÷f«6Ró∞¢ñbÇÜó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇó«∆ó5G&Á7&VÁE7G&V“Çííbeˆ76ó7FÁEFÜñÊ∂ñÊt&V∆ˆÊw4ñÂv˜&∂∆ˆrÜ“¬&tñGÇ¬Fˆˆƒ6∆ƒ76ó7FÁDñGá2íí76ó7FÁEFÜñÊ∂ñÊrÁ6WBá&tñGÇ¬FÜñÊ∂ñÊuFWáBì∞¢V«6RñbávñÊF˜rÂ˜6Ü˜uFÜñÊ∂ñÊr”÷f«6Rí6VrÊñÁ6W'DF¶6VÁDÖD‘¬Çv&Vf˜&VVÊBr¬˜FÜñÊ∂ñÊt6&DáF÷¬áFÜñÊ∂ñÊuFWáBíì∞¢–¢6ˆÁ7BÜ5fó6ñ&∆T&ˆGì“Ö7G&ñÊrÜ6ˆÁFVÁG«¬rríÁG&ñ“Çó«∆fñ∆W4áF÷«««&V6˜fW'îáF÷¬ì∞¢ñbá7FGW4áF÷¬ó∞¢6VrÊñÁ6W'DF¶6VÁDÖD‘¬Çv&Vf˜&VVÊBr¬7FGW4áF÷¬ì∞¢ñbÜÜ5fó6ñ&∆T&ˆGííˆñÁ6W'E6Vv÷VÁD&∆ˆ6≤á6Vr¬G∂fñ∆W4áF÷«”∆Fób6∆73“&◊6r÷&ˆGí#‚G∂&ˆGîáF÷«”¬ˆFóc‚G∂fˆ˜DáF÷«÷ì∞¢÷V«6RñbÜÜ5fó6ñ&∆T&ˆGíó∞¢ˆñÁ6W'E6Vv÷VÁD&∆ˆ6≤á6Vr¬G∂fñ∆W4áF÷«”∆Fób6∆73“&◊6r÷&ˆGí#‚G∂&ˆGîáF÷«”¬ˆFóc‚G∂fˆ˜DáF÷«÷ì∞¢÷V«6RñbÇáFÜñÊ∂ñÊuFWáBbgvñÊF˜rÂ˜6Ü˜uFÜñÊ∂ñÊr”÷f«6Rbbó56ñ◊∆ñfñVEFˆˆƒ6∆∆ñÊrÇííó∞¢6VrÊ6∆74∆ó7BÊFBÇv76ó7FÁB◊6Vv÷VÁB÷Ê6Ü˜"rì∞¢–¢ˆ76ó7FÁEGW&‰&∆ˆ6∑2Ü7W'&VÁD76ó7FÁEGW&‚íÊVÊD6Üñ∆Bá6Vrì∞¢76ó7FÁE6Vv÷VÁG2Á6WBá&tñGÇ¬6Vrì∞¢–†¢gVÊ7Fñˆ‚ˆñÁ6W'D6ˆ◊&W76ñˆ‰∆ñ∂TÊˆFRÜÊˆFR¬Ê6Ü˜$ñÊFWÇó∞¢ñbÇÊˆFRí&WGW&‚f«6S∞¢6ˆÁ7BÊ6Ü˜$ñGÉ÷Ê6Ü˜$ñÊFWÉ””◊VÊFVfñÊVCˆñÁ6W'Fñˆ‰Ê6Ü˜#¶Ê6Ü˜$ñÊFWÉ∞¢ñbÜÊ6Ü˜$ñGÇ”÷ÁV∆¬bb&VÊFW%fó5vóFÑñGÖ∂Ê6Ü˜$ñGÖ“ó∞¢6ˆÁ7BÊ6Ü˜%&tñGÉ◊&VÊFW%fó5vóFÑñGÖ∂Ê6Ü˜$ñGÖ“Á&tñGÉ∞¢6ˆÁ7BÊ6Ü˜%6Vs÷76ó7FÁE6Vv÷VÁG2ÊvWBÜÊ6Ü˜%&tñGÇì∞¢ñbÜÊ6Ü˜%6Vró∞¢6ˆÁ7BGW&„÷Ê6Ü˜%6VrÊ6∆˜6W7BÇrÊ76ó7FÁB◊GW&‚rì∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÜ&∆ˆ6∑2ó∞¢&∆ˆ6∑2ÊVÊD6Üñ∆BÜÊˆFRì∞¢&WGW&‚ÊˆFRÁ&VÁDV∆V÷VÁC∞¢–¢–¢6ˆÁ7BW6W%&˜s◊W6W%&˜w2ÊvWBÜÊ6Ü˜%&tñGÇì∞¢ñbáW6W%&˜rbbW6W%&˜rÁ&VÁDV∆V÷VÁBó∞¢W6W%&˜rÁ&VÁDV∆V÷VÁBÊñÁ6W'D&Vf˜&RÜÊˆFR¬W6W%&˜rÊÊWáE6ñ&∆ñÊrì∞¢&WGW&‚ÊˆFRÁ&VÁDV∆V÷VÁC∞¢–¢–¢ñÊÊW"ÊVÊD6Üñ∆BÜÊˆFRì∞¢&WGW&‚ÊˆFRÁ&VÁDV∆V÷VÁC∞¢–¢gVÊ7Fñˆ‚ˆñÁ6W'D6ˆ◊&W76ñˆ‰∆ñ∂TÊˆFT'ï&tñGÇÜÊˆFR¬&tñGÇó∞¢ñbÇÊˆFRí&WGW&‚f«6S∞¢ñbá&tñGÉ∆fó'7E&VÊFW&VE&tñGÇí&WGW&‚f«6S∞¢ñbÇ&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÇó∞¢ñÊÊW"ÊVÊD6Üñ∆BÜÊˆFRì∞¢&WGW&‚ÊˆFRÁ&VÁDV∆V÷VÁC∞¢–¢∆WBÊ6Ü˜$ñGÉ÷ÁV∆√∞¢f˜"Ü∆WBì”∂ì«&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÉ∂í≤≤ó∞¢ñbá&VÊFW%fó5vóFÑñGÖ∂ï“Á&tñGÇ‚&tñGÇó∞¢Ê6Ü˜$ñGÉ÷ì∞¢'&V≥∞¢–¢–¢ñbÜÊ6Ü˜$ñGÉ””÷ÁV∆¬ó∞¢ñÊÊW"ÊVÊD6Üñ∆BÜÊˆFRì∞¢&WGW&‚ÊˆFRÁ&VÁDV∆V÷VÁC∞¢–¢6ˆÁ7BÊ6Ü˜%&tñGÉ◊&VÊFW%fó5vóFÑñGÖ∂Ê6Ü˜$ñGÖ“Á&tñGÉ∞¢6ˆÁ7BÊ6Ü˜%6Vs÷76ó7FÁE6Vv÷VÁG2ÊvWBÜÊ6Ü˜%&tñGÇì∞¢ñbÜÊ6Ü˜%6Vró∞¢6ˆÁ7BGW&„÷Ê6Ü˜%6VrÊ6∆˜6W7BÇrÊ76ó7FÁB◊GW&‚rì∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÜ&∆ˆ6∑2ó∞¢&∆ˆ6∑2ÊñÁ6W'D&Vf˜&RÜÊˆFR¬Ê6Ü˜%6Vrì∞¢&WGW&‚ÊˆFRÁ&VÁDV∆V÷VÁC∞¢–¢6ˆÁ7BGW&Â&VÁC◊GW&‚bbGW&‚Á&VÁDV∆V÷VÁC∞¢ñbáGW&Â&VÁBó∞¢GW&Â&VÁBÊñÁ6W'D&Vf˜&RÜÊˆFR¬GW&‚ì∞¢&WGW&‚ÊˆFRÁ&VÁDV∆V÷VÁC∞¢–¢–¢6ˆÁ7BW6W%&˜s◊W6W%&˜w2ÊvWBÜÊ6Ü˜%&tñGÇì∞¢ñbáW6W%&˜rbbW6W%&˜rÁ&VÁDV∆V÷VÁBó∞¢W6W%&˜rÁ&VÁDV∆V÷VÁBÊñÁ6W'D&Vf˜&RÜÊˆFR¬W6W%&˜rì∞¢&WGW&‚ÊˆFRÁ&VÁDV∆V÷VÁC∞¢–¢ñÊÊW"ÊVÊD6Üñ∆BÜÊˆFRì∞¢&WGW&‚ÊˆFRÁ&VÁDV∆V÷VÁC∞¢–¢6ˆÁ7B&W6W'fVDˆÊ«îÊˆFS◊&W6W'fVD6ˆ◊&W76ñˆÂF6¥÷W76vW2Ê∆VÊwFÄ¢ÚÇÇì”Á∂6ˆÁ7B&˜s÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∑&˜rÊñÊÊW$ÖD‘√÷∆Fób6∆73“&6ˆ◊&W76ñˆ‚◊GW&‚"FF÷6ˆ◊7Fñˆ‚◊F6≤÷f∆∆&6≥“##„∆Fób6∆73“&6ˆ◊&W76ñˆ‚◊GW&‚÷&∆ˆ6∑2#‚Gµ˜&W6W'fVD6ˆ◊&W76ñˆÂF6¥∆ó7D6&G4áF÷¬á&W6W'fVD6ˆ◊&W76ñˆÂF6¥÷W76vW2ó”¬ˆFóc„¬ˆFócÊ∑&WGW&‚&˜rÊfó'7DV∆V÷VÁD6Üñ∆C∑“íÇê¢¢ÁV∆√∞¢6ˆÁ7B&W6W'fVDˆÊ«îÊ6Ü˜#◊&W6W'fVD6ˆ◊&W76ñˆÂ&tñGá2Ê∆VÊwFÄ¢ÚÇÇì”Á∂∆WBñGÉ÷ÁV∆√∂f˜"Ü∆WBì”∂ì«&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÉ∂í≤≤ó∂ñbá&VÊFW%fó5vóFÑñGÖ∂ï“Á&tñGÉ«&W6W'fVD6ˆ◊&W76ñˆÂ&tñGá5≥“íñGÉ÷ì∑◊&WGW&‚ñGÉ∑“íÇê¢¢ÁV∆√∞¢6ˆÁ7BÜÊFˆfe7V÷÷'ï7FFW3’ˆ6ˆ∆∆V7DÜÊFˆfe7V÷÷'ï7FFW2Ö2Ê÷W76vW2ì∞†¢ˆñÁ6W'D6ˆ◊&W76ñˆ‰∆ñ∂TÊˆFRÜ6ˆ◊&W76ñˆ‰ÊˆFRì∞¢6ˆÁ7BñÊ∆ñÊT6ˆ◊7Fñˆ‰ñÁ6W'Fñˆ„’ˆñÁ6W'D6ˆ◊7Fñˆ‰6&DÊˆFW2Ä¢6ˆ◊7Fñˆ‰6&DÊˆFW2¿¢6ˆ◊7FñˆÂ∆6V÷VÁG2ÁF6¥˜vÊW"¿¢ÜÊˆFR∆÷&∂W%&tñGÇì”ÂˆñÁ6W'D6ˆ◊&W76ñˆ‰∆ñ∂TÊˆFT'ï&tñGÇÜÊˆFR∆÷&∂W%&tñGÇê¢ì∞¢ñbÜñÊ∆ñÊT6ˆ◊7Fñˆ‰ñÁ6W'Fñˆ‚ÁF6¥˜vÊW$ÊˆFRí&W6W'fVD6ˆ◊&W76ñˆÂF6¥˜vÊW$ÊˆFS÷ñÊ∆ñÊT6ˆ◊7Fñˆ‰ñÁ6W'Fñˆ‚ÁF6¥˜vÊW$ÊˆFS∞¢ñbÇ&VfW&VÊ6TÊˆFUñÊÊVDEF˜ó∞¢6ˆÁ7B&VfW&VÊ6TñÁ6W'FVC◊&VfW&VÊ6TÊˆFRbg&VfW&VÊ6T÷W76vU&tñGÉ„” ¢ıˆñÁ6W'D6ˆ◊&W76ñˆ‰∆ñ∂TÊˆFT'ï&tñGÇá&VfW&VÊ6TÊˆFR«&VfW&VÊ6T÷W76vU&tñGÇê¢•ˆñÁ6W'D6ˆ◊&W76ñˆ‰∆ñ∂TÊˆFRá&VfW&VÊ6TÊˆFRì∞¢ñbá&VfW&VÊ6TñÁ6W'FVBbg&VfW&VÊ6TÊˆFSÚÁ&VÁDV∆V÷VÁBbg&VfW&VÊ6TÊˆFT˜vÁ5F6∑2ó∞¢&W6W'fVD6ˆ◊&W76ñˆÂF6¥˜vÊW$ÊˆFS◊&VfW&VÊ6TÊˆFS∞¢–¢–¢ˆñÁ6W'E&W6W'fVD6ˆ◊&W76ñˆÂF6¥f∆∆&6≤Ä¢&W6W'fVD6ˆ◊&W76ñˆÂF6¥˜vÊW$ÊˆFR¿¢&W6W'fVDˆÊ«îÊˆFR¿¢ÊˆFS”ÂˆñÁ6W'D6ˆ◊&W76ñˆ‰∆ñ∂TÊˆFRÜÊˆFR«&W6W'fVDˆÊ«îÊ6Ü˜"ê¢ì∞¢ˆñÁ6W'D6ˆ◊&W76ñˆ‰∆ñ∂TÊˆFRÜÜÊFˆfe7FFSıˆÜÊFˆfd6&G4ÊˆFRÜÜÊFˆfe7FFRì¶ÁV∆¬¬&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÉ˜&VÊFW%fó5vóFÑñGÇÊ∆VÊwFÇ”¶ÁV∆¬ì∞¢f˜"Ü6ˆÁ7BVÁG'íˆbÜÊFˆfe7V÷÷'ï7FFW2ó∞¢ñbÇVÁG'ó«¬VÁG'íÁ7FFRí6ˆÁFñÁVS∞¢ñbÜVÁG'íÁ&tñGÉ∆fó'7E&VÊFW&VE&tñGÇí6ˆÁFñÁVS∞¢ˆñÁ6W'D6ˆ◊&W76ñˆ‰∆ñ∂TÊˆFT'ï&tñGÇÖˆÜÊFˆfd6&G4ÊˆFRÜVÁG'íÁ7FFRí¬VÁG'íÁ&tñGÇì∞¢–¢&VÊFW$6ˆ◊&W76ñˆÂVíÇì∞¢6ˆÁ7BÊ6Ü˜$˜vÊVD76ó7FÁE&tñGá3÷ÊWr6WBÇì∞¢f˜"Ü6ˆÁ7B∑&tñGÇ«6Vu“ˆb76ó7FÁE6Vv÷VÁG2ó∞¢6ˆÁ7B◊6s’2Ê÷W76vW5∑&tñGÖ”∞¢ñbÇ◊6w«¬◊6rÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊW«¬6Vrí6ˆÁFñÁVS∞¢6ˆÁ7BGW&„◊6VrÊ6∆˜6W7BÇrÊ76ó7FÁB◊GW&‚rì∞¢ñbÇGW&‚í6ˆÁFñÁVS∞¢GW&‚ÁVW'ï6V∆V7F˜$∆¬ÇrÊ76ó7FÁB◊6Vv÷VÁE∂FF÷◊6r÷ñGÖ“ríÊf˜$V6ÇÜÊˆFS”Á∞¢6ˆÁ7BñGÉ‘ÁV÷&W"ÜÊˆFRÊvWDGG&ñ'WFRÇvFF÷◊6r÷ñGÇríì∞¢ñbÑÁV÷&W"Êó4fñÊóFRÜñGÇííÊ6Ü˜$˜vÊVD76ó7FÁE&tñGá2ÊFBÜñGÇì∞¢“ì∞¢–¢ÚÚñÁ6W'B6WGF∆VBFˆˆ¬6∆¬6&G2ÜÜó7F˜'ífñWrˆÊ«íí‡¢ÚÚGW&ñÊr∆ófR7G&V÷ñÊr¬Fˆˆ¬6&G2&R&VÊFW&VBñ‚6∆ófUFˆˆƒ6&G2'íFÜP¢ÚÚFˆˆ¬54RÜÊF∆W"ÊBÊWfW"÷óÜVBñÁFÚFÜR÷W76vR∆ó7BVÁFñ¬FˆÊRfó&W2‡¢Ú¢ÚÚf∆∆&6≥¢ñb2ÁFˆˆƒ6∆«2ó2V◊Gíá6W76ñˆÁ2FÜB&VFFR6W76ñˆ‚÷∆WfV¬Fˆˆ¿¢ÚÚG&6∂ñÊr¬˜"'VÁ2FÜBFñF‚wBvÚFá&˜VvÇFÜRÊ˜&÷¬7G&V÷ñÊrFÇí¬'Vñ∆@¢ÚÚFó7∆í∆ó7Bg&ˆ“W"÷÷W76vRFˆˆ≈ˆ6∆«2Ñ˜V‰íf˜&÷Bí7F˜&VBñ‚V6Ä¢ÚÚ76ó7FÁB÷W76vR‚FÜó26˜fW'2FÜR&V∆ˆB66RFW67&ñ&VBñ‚ó77VR3C‡¢6ˆÁ7BÜ4÷W76vUFˆˆƒ÷WFFF“2Ê'W7íbd'&íÊó4'&íÖ2Ê÷W76vW2íbe2Ê÷W76vW2Á6ˆ÷RÇÜ“«&tñGÇì”‡¢Ê6Ü˜$˜vÊVD76ó7FÁE&tñGá2ÊÜ2á&tñGÇíbeˆ∆Vv7ï6WGF∆VDf∆∆&6¥Ü5Fˆˆƒ÷WFFFÜ“ê¢ì∞¢ñbÇ2Ê'W7íbbÜÜ4÷W76vUFˆˆƒ÷WFFF«¬2ÁFˆˆƒ6∆«7«¬2ÁFˆˆƒ6∆«2Ê∆VÊwFÇíó∞¢ÚÚñÊFWÇFˆˆ¬˜WGWG2'íFˆˆ≈ˆ6∆≈ˆñBÚFˆˆ≈˜W6UˆñB6ÚFÜP¢ÚÚf∆∆&6≤÷'Vñ«B6&G26''íFÜVó"&W7V«B6ÊóWBÜÊ˜BßW7BFÜR6ˆ÷÷ÊBí‡¢ÚÚvóFÜ˜WBFÜó27FW4ƒí÷˜&ñvñ‚6W76ñˆÁ2&V∆ˆBvóFÇV◊GíFˆˆ¬6&G2‡¢6ˆÁ7B&W7V«G4'ïFñC◊∑”∞¢6ˆÁ7Bf∆∆&6µFˆˆ≈6˜W&6W3’µ”∞¢ÚÚGW&&∆Rf∆∆&6≥¢FÜRW'6ó7FVB6ˆ◊7B7V÷÷'íá6W76ñˆ‚ÁFˆˆ≈ˆ6∆«2¬'Vñ«@¢ÚÚ'íˆWáG&7E˜Fˆˆ≈ˆ6∆«5ˆg&ˆ’ˆ÷W76vW2í6'&ñW2&˜VÊFVB&W7V«B6ÊóWF ¢ÚÚ∂WñVB'íFñB‚ˆ‚6ˆ∆B˜vñÊFVB∆ˆBvÜW&RFÜR&ˆ∆SßFˆˆ¬&W7V«B÷÷W76vP¢ÚÚ¶ˆñ‚&V∆˜r÷ó76W2ÜñB÷ó6÷F6Ç¬&V6˜fW'í◊&V'Vñ«BGW&‚í¬W6RFÜó26ÚFÜP¢ÚÚFW&÷ñÊ¬˜WGWBÚFñfb&ˆGí7Fñ∆¬&VÊFW'2ñÁ7FVBˆbfÊó6ÜñÊrÇ3Cì#rí‡¢6ˆÁ7BW'6ó7FVE6ÊóWD'ïFñC◊∑”∞¢G'ó∞¢6ˆÁ7BW'6ó7FVC“Ö2Á6W76ñˆ‚bd'&íÊó4'&íÖ2Á6W76ñˆ‚ÁFˆˆ≈ˆ6∆«2íìı2Á6W76ñˆ‚ÁFˆˆ≈ˆ6∆«3•µ”∞¢W'6ó7FVBÊf˜$V6ÇáF3”Á∞¢ñbÇF7««GóVˆbF2”“vˆ&¶V7Brí&WGW&„∞¢6ˆÁ7BFñC◊F2ÁFñG««F2ÊñG««F2ÁFˆˆ≈ˆ6∆≈ˆñG««F2Ê6∆≈ˆñG«¬rs∞¢6ˆÁ7B6Êó◊F2Á6ÊóWG««F2Á&W7V«G««F2Ê˜WGWG««F2Á&WfñWw«¬rs∞¢ñbáFñBbg6ÊóbbW'6ó7FVE6ÊóWD'ïFñE∑FñE“íW'6ó7FVE6ÊóWD'ïFñE∑FñE”’7G&ñÊrá6Êóì∞¢“ì∞¢÷6F6ÇÜRó∑–¢2Ê÷W76vW2Êf˜$V6ÇÇÜ“«&tñGÇì”Á∞¢ñbÇ“í&WGW&„∞¢ÚÚ˜V‰íÚÜW&÷W24ƒíf˜&÷C¢&ˆ∆S◊Fˆˆ¬vóFÇFˆˆ≈ˆ6∆≈ˆñ@¢ñbÜ“Á&ˆ∆S””“wFˆˆ¬ró∞¢6ˆÁ7BFñC÷“ÁFˆˆ≈ˆ6∆≈ˆñG«∆“ÁFˆˆ≈˜W6UˆñG«¬rs∞¢ñbáFñBí&W7V«G4'ïFñE∑FñE”’ˆ6∆ïFˆˆ≈&W7V«E6ÊóWBÜ“Ê6ˆÁFVÁBì∞¢&WGW&„∞¢–¢ÚÚÁFá&˜ñ2f˜&÷C¢Fˆˆ≈˜&W7V«B&∆ˆ6∑2ñÁ6ñFRW6W"÷W76vR6ˆÁFVÁB'&ê¢ñbÑ'&íÊó4'&íÜ“Ê6ˆÁFVÁBíó∞¢“Ê6ˆÁFVÁBÊf˜$V6Çá”Á∞¢ñbÇ««GóVˆb”“vˆ&¶V7Bw««ÁGóR”“wFˆˆ≈˜&W7V«Brí&WGW&„∞¢6ˆÁ7BFñC◊ÁFˆˆ≈˜W6UˆñG«¬rs∞¢ñbÇFñBí&WGW&„∞¢6ˆÁ7B&s◊GóVˆbÊ6ˆÁFVÁC””“w7G&ñÊrs˜Ê6ˆÁFVÁ@¢§'&íÊó4'&íáÊ6ˆÁFVÁBì˜Ê6ˆÁFVÁBÊ÷Ü3”Ê2bf2ÁFWáCˆ2ÁFWáC¢rríÊ¶ˆñ‚Çrrê¢¢rs∞¢&W7V«G4'ïFñE∑FñE”’ˆ6∆ïFˆˆ≈&W7V«E6ÊóWBá&rì∞¢“ì∞¢–¢ñbÜ“Á&ˆ∆S””“v76ó7FÁBró∞¢ñbÜÊ6Ü˜$˜vÊVD76ó7FÁE&tñGá2ÊÜ2á&tñGÇíí&WGW&„∞¢ñbÖˆ∆Vv7ï6WGF∆VDf∆∆&6¥Ü5Fˆˆƒ÷WFFFÜ“ííf∆∆&6µFˆˆ≈6˜W&6W2ÁW6Çá∂“«&tñGá“ì∞¢–¢“ì∞¢6ˆÁ7BFW&ófVC’µ”∞¢6ˆÁ7B∆ófUFˆˆƒ÷WFFF‘'&íÊó4'&íÖ2Â˜6WGF∆VD∆ófUFˆˆƒ÷WFFFê¢Ú2Â˜6WGF∆VD∆ófUFˆˆƒ÷WFFF¢¢Ñ'&íÊó4'&íÖ2ÁFˆˆƒ6∆«2ìı2ÁFˆˆƒ6∆«3•µ“ì∞¢6ˆÁ7B∆ófT÷WFFF'ïFñC÷ÊWr÷Çì∞¢∆ófUFˆˆƒ÷WFFFÊf˜$V6ÇÇáF2∆ñGÇì”Á∞¢ñbÇF7««GóVˆbF2”“vˆ&¶V7Brí&WGW&„∞¢6ˆÁ7BFñC◊F2ÁFñG««F2ÊñG««F2ÁFˆˆ≈ˆ6∆≈ˆñG««F2Ê6∆≈ˆñG«¬rs∞¢ñbáFñBbb∆ófT÷WFFF'ïFñBÊÜ2áFñBíí∆ófT÷WFFF'ïFñBÁ6WBáFñB«∑F2∆ñGá“ì∞¢“ì∞¢6ˆÁ7BW6VD∆ófUFˆˆƒ÷WFFF÷ÊWr6WBÇì∞¢6ˆÁ7B6˜î∆ófUFˆˆƒ÷WFFF“ÜÊWáB∆Ê÷R«FñBì”Á∞¢∆WB÷F6ÑVÁG'ì◊FñCˆ∆ófT÷WFFF'ïFñBÊvWBáFñBì¶ÁV∆√∞¢ñbÇ÷F6ÑVÁG'íó∞¢6ˆÁ7B÷F6ÑñGÉ÷∆ófUFˆˆƒ÷WFFFÊfñÊDñÊFWÇÇáF2∆íì”ÁF2bbW6VD∆ófUFˆˆƒ÷WFFFÊÜ2ÜííbbÇÊ÷W««F2ÊÊ÷S””÷Ê÷Ríì∞¢ñbÜ÷F6ÑñGÉ„”í÷F6ÑVÁG'ì◊∑F3¶∆ófUFˆˆƒ÷WFFF∂÷F6ÑñGÖ“∆ñGÉ¶÷F6ÑñGá”∞¢–¢ñbÜ÷F6ÑVÁG'íó∞¢W6VD∆ófUFˆˆƒ÷WFFFÊFBÜ÷F6ÑVÁG'íÊñGÇì∞¢6ˆÁ7B∆ófS÷÷F6ÑVÁG'íÁF7««∑”∞¢f˜"Ü6ˆÁ7B∂Wíˆb≤v7FófóGî'W'7DñBr¬vGW&Fñˆ‚r¬w7F'FVEˆBu“ó∞¢ñbÇÜÊWáE∂∂Wï”””◊VÊFVfñÊVG«∆ÊWáE∂∂Wï”””÷ÁV∆¬íbf∆ófU∂∂Wï“”◊VÊFVfñÊVBbf∆ófU∂∂Wï“”÷ÁV∆¬íÊWáE∂∂Wï”÷∆ófU∂∂Wï”∞¢–¢–¢&WGW&‚ÊWáC∞¢”∞¢f∆∆&6µFˆˆ≈6˜W&6W2Êf˜$V6ÇÇá∂“«&tñGá“ì”Á∞¢6ˆÁ7B76ó7FÁEFˆˆƒÊ6Ü˜$ñGÉ’ˆ76ó7FÁEFˆˆƒÊ6Ü˜$ñGÑf˜$÷W76vRÖ2Ê÷W76vW2«&tñGÇì∞¢ÚÚ˜V‰íf˜&÷C¢F˜÷∆WfV¬Fˆˆ≈ˆ6∆«2fñV∆Bˆ‚FÜR76ó7FÁB÷W76vP¢Ü“ÁFˆˆ≈ˆ6∆«7«≈µ“íÊf˜$V6ÇáF3”Á∞¢ñbÇF7««GóVˆbF2”“vˆ&¶V7Brí&WGW&„∞¢6ˆÁ7Bf„◊F2ÊgVÊ7FñˆÁ««∑”∞¢6ˆÁ7BÊ÷S÷f‚ÊÊ÷W««F2ÊÊ÷W«¬wFˆˆ¬s∞¢∆WB&w3◊∑”∞¢G'ó≤&w3‘•4Ù‚Á'6RÜf‚Ê&wV÷VÁG7«¬w∑“rì≤÷6F6ÇÜRó∑–¢6ˆÁ7BFñC◊F2ÊñG««F2Ê6∆≈ˆñG«¬rs∞¢6ˆÁ7BF6Ö6ÊóWC’ˆ6∆ïF6Ö6ÊóWDg&ˆ‘&w2ÜÊ÷R∆&w2ì∞¢6ˆÁ7B&W7V«E6ÊóWC◊&W7V«G4'ïFñE∑FñE◊««W'6ó7FVE6ÊóWD'ïFñE∑FñE◊«¬rs∞¢∆WB&w56Ê’˜Fˆˆƒ&w56Ê6Ü˜BÜ&w2ì∞¢FW&ófVBÁW6ÇÜ6˜î∆ófUFˆˆƒ÷WFFFá∞¢Ê÷R¿¢6ÊóWC•ˆ6∆ïFˆˆƒ6&E6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBí¿¢ó5ˆFñfc•ˆ6∆ïFˆˆƒ6&DÜ4Fñfe6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBí¿¢FñB¿¢76ó7FÁEˆ◊6uˆñGÉ¶76ó7FÁEFˆˆƒÊ6Ü˜$ñGÇ¿¢&w3¶&w56Ê¿¢FˆÊSßG'VR¿¢“¬Ê÷R¬FñBíì∞¢“ì∞¢ÚÚvV%Tí'Fñ¬ˆ∆ófRf˜&÷C¢˜'Fñ≈˜Fˆˆ≈ˆ6∆«26Ê6Ü˜G27W'fófP¢ÚÚñÁFW''WFVB˜"FFW"◊6ÜVB6WGF∆W2WfV‚vÜV‚6W76ñˆ‚ÁFˆˆ≈ˆ6∆«2ó2V◊Gí‡¢6ˆÁ7B'Fñ≈Fˆˆƒ6∆«3‘'&íÊó4'&íÜ“Â˜'Fñ≈˜Fˆˆ≈ˆ6∆«2ìˆ“Â˜'Fñ≈˜Fˆˆ≈ˆ6∆«3•µ”∞¢'Fñ≈Fˆˆƒ6∆«2Êf˜$V6ÇáF3”Á∞¢ñbÇF7««GóVˆbF2”“vˆ&¶V7Brí&WGW&„∞¢6ˆÁ7Bf„◊F2ÊgVÊ7FñˆÁ««∑”∞¢6ˆÁ7BÊ÷S◊F2ÊÊ÷W«∆f‚ÊÊ÷W«¬wFˆˆ¬s∞¢∆WB&w3◊F2Ê&w7««F2ÊñÁWG««∑”∞¢ñbÇ&w7««GóVˆb&w2”“vˆ&¶V7Bró∞¢G'ó≤&w3‘•4Ù‚Á'6RÜf‚Ê&wV÷VÁG7«¬w∑“rì≤÷6F6ÇÜRó≤&w3◊∑”≤–¢÷V«6RñbÇˆ&¶V7BÊ∂Wó2Ü&w2íÊ∆VÊwFÇbff‚Ê&wV÷VÁG2ó∞¢G'ó≤&w3‘•4Ù‚Á'6RÜf‚Ê&wV÷VÁG7«¬w∑“rì≤÷6F6ÇÜRó∑–¢–¢6ˆÁ7BFñC◊F2ÁFñG««F2ÊñG««F2ÁFˆˆ≈ˆ6∆≈ˆñG««F2Ê6∆≈ˆñG«¬rs∞¢6ˆÁ7BF6Ö6ÊóWC’ˆ6∆ïF6Ö6ÊóWDg&ˆ‘&w2ÜÊ÷R∆&w2ì∞¢6ˆÁ7B&W7V«E6ÊóWC◊&W7V«G4'ïFñE∑FñE◊««F2Á6ÊóWG««F2Á&WfñWw««W'6ó7FVE6ÊóWD'ïFñE∑FñE◊«¬rs∞¢6ˆÁ7B&w56Ê’˜Fˆˆƒ&w56Ê6Ü˜BÜ&w2ì∞¢FW&ófVBÁW6ÇÜ6˜î∆ófUFˆˆƒ÷WFFFá∞¢Ê÷R¿¢6ÊóWC•ˆ6∆ïFˆˆƒ6&E6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBí¿¢ó5ˆFñfc•ˆ6∆ïFˆˆƒ6&DÜ4Fñfe6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBí¿¢FñB¿¢76ó7FÁEˆ◊6uˆñGÉ¶76ó7FÁEFˆˆƒÊ6Ü˜$ñGÇ¿¢&w3¶&w56Ê¿¢FˆÊSßG'VR¿¢“¬Ê÷R¬FñBíì∞¢“ì∞¢ÚÚÁFá&˜ñ2f˜&÷C¢Fˆˆ≈˜W6R&∆ˆ6∑2ñÁ6ñFR76ó7FÁB6ˆÁFVÁB'&ê¢ñbÑ'&íÊó4'&íÜ“Ê6ˆÁFVÁBíó∞¢“Ê6ˆÁFVÁBÊf˜$V6Çá”Á∞¢ñbÇ««GóVˆb”“vˆ&¶V7Bw««ÁGóR”“wFˆˆ≈˜W6Rrí&WGW&„∞¢6ˆÁ7BÊ÷S◊ÊÊ÷W«¬wFˆˆ¬s∞¢6ˆÁ7B&w3◊ÊñÁWG««∑”∞¢6ˆÁ7BFñC◊ÊñG«¬rs∞¢6ˆÁ7BF6Ö6ÊóWC’ˆ6∆ïF6Ö6ÊóWDg&ˆ‘&w2ÜÊ÷R∆&w2ì∞¢6ˆÁ7B&W7V«E6ÊóWC◊&W7V«G4'ïFñE∑FñE◊««W'6ó7FVE6ÊóWD'ïFñE∑FñE◊«¬rs∞¢6ˆÁ7B&w56Ê’˜Fˆˆƒ&w56Ê6Ü˜BÜ&w2ì∞¢FW&ófVBÁW6ÇÜ6˜î∆ófUFˆˆƒ÷WFFFá∞¢Ê÷R¿¢6ÊóWC•ˆ6∆ïFˆˆƒ6&E6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBí¿¢ó5ˆFñfc•ˆ6∆ïFˆˆƒ6&DÜ4Fñfe6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBí¿¢FñB¿¢76ó7FÁEˆ◊6uˆñGÉ¶76ó7FÁEFˆˆƒÊ6Ü˜$ñGÇ¿¢&w3¶&w56Ê¿¢FˆÊSßG'VR¿¢“¬Ê÷R¬FñBíì∞¢“ì∞¢–¢ÚÚvV%Tí÷ñÁFW&Ê¬'Fñ¬Fˆˆ¬6∆«26GW&VBˆ‚6Ê6V¬˜7F˜ ¢ÚÚá&ófFR6ÜS¢Ê÷Rˆ&w2ˆFˆÊR˜&WfñWr˜6ÊóWB¬ÊÚ˜V‰íVÁfV∆˜Rí‡¢ñbÑ'&íÊó4'&íÜ“Â˜'Fñ≈˜Fˆˆ≈ˆ6∆«2íó∞¢“Â˜'Fñ≈˜Fˆˆ≈ˆ6∆«2Êf˜$V6ÇáF3”Á∞¢ñbÇF7««GóVˆbF2”“vˆ&¶V7Brí&WGW&„∞¢6ˆÁ7BÊ÷S◊F2ÊÊ÷W«¬wFˆˆ¬s∞¢6ˆÁ7B&w3◊F2Ê&w7««∑”∞¢6ˆÁ7BFñC◊F2ÊñG««F2Ê6∆≈ˆñG««F2ÁFˆˆ≈ˆ6∆≈ˆñG««F2ÁFñG«¬rs∞¢6ˆÁ7BF6Ö6ÊóWC’ˆ6∆ïF6Ö6ÊóWDg&ˆ‘&w2ÜÊ÷R∆&w2ì∞¢6ˆÁ7B&W7V«E6ÊóWC’ˆ6∆ïFˆˆ≈&W7V«E6ÊóWBáF2Á6ÊóWG««F2Á&W7V«G««F2Ê˜WGWG««F2Á&WfñWw«¬rrì∞¢6ˆÁ7B&w56Ê’˜Fˆˆƒ&w56Ê6Ü˜BÜ&w2√Bì∞¢FW&ófVBÁW6ÇÜ6˜î∆ófUFˆˆƒ÷WFFFá∞¢Ê÷R¿¢6ÊóWC•ˆ6∆ïFˆˆƒ6&E6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBí¿¢ó5ˆFñfc•ˆ6∆ïFˆˆƒ6&DÜ4Fñfe6ÊóWBá&W7V«E6ÊóWB«F6Ö6ÊóWBí¿¢FñB¿¢76ó7FÁEˆ◊6uˆñGÉ¶76ó7FÁEFˆˆƒÊ6Ü˜$ñGÇ¿¢&w3¶&w56Ê¿¢FˆÊSßG'VR¿¢“¬Ê÷R¬FñBíì∞¢“ì∞¢–¢“ì∞¢ñbÜFW&ófVBÊ∆VÊwFÇí2ÁFˆˆƒ6∆«3÷FW&ófVC∞¢ñbÖ2Â˜6WGF∆VD∆ófUFˆˆƒ÷WFFFí2Â˜6WGF∆VD∆ófUFˆˆƒ÷WFFF÷ÁV∆√∞¢–¢ñbÇ2Ê'W7í«¬Ö2ÁFˆˆƒ6∆«2be2ÁFˆˆƒ6∆«2Ê∆VÊwFÇíó∞¢ÚÚ&V'Vñ∆B6WGF∆VBFˆˆ¬˜v˜&∂∆ˆr˜FÜñÊ∂ñÊrÊˆFW2‚FÜR«¬Ö2ÁFˆˆƒ6∆«2Ê∆VÊwFÇñ ¢ÚÚ&“ó2$UTï$TB¬Ê˜BßW7B2Ê'W7ñ¢vÜV‚&VÊFW$÷W76vW2&R◊'VÁ2GW&ñÊr‡¢ÚÚ7FófR7G&V“ÜRÊr‚7vóF6ÜñÊr&6≤FÚ‚ñ‚◊&ˆw&W726W76ñˆ‚¬'W7ì◊G'VRí¿¢ÚÚFÜRV&∆ñW"ñÊÊW$ÖD‘¬vóR&V÷˜fVBWfW'í6WGF∆VBGW&‚w2v˜&∂∆ˆr&˜fRFÜP¢ÚÚ∆ófRGW&‚‚vFñÊrW&V«íˆ‚2Ê'W7ñ6∂óVBFÜó2&V'Vñ∆BvÜñ∆R'W7íÊ@¢ÚÚ∆VgBFÜ˜6R&ñ˜"GW&Á2rFˆˆ¬6&G2vˆÊRVÁFñ¬FÜR7G&V“fñÊó6ÜVBÇ33C¢ÚÚ&Vw&W76ñˆ‚g2÷7FW#≤6÷R6ˆÁFVÁB÷∆˜72÷ˆ‚◊7vóF6Ç6∆72233ccÇí‚FÜP¢ÚÚ¶Ê˜BÖ∂FF÷∆ófR◊FÜñÊ∂ñÊs“#%“ñÚ∆ófR÷6&BwV&G2&V∆˜r∂VWFÜR7FófP¢ÚÚGW&‚w2˜v‚∆ófRÊˆFW2g&ˆ“&VñÊrF˜V&∆R÷'Vñ«B‡¢ñÊÊW"ÁVW'ï6V∆V7F˜$∆¬ÇrÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W¶Ê˜BÖ∂FF÷6ˆ◊&W76ñˆ‚÷6&E“í¬ÁFˆˆ¬÷6∆¬÷w&˜W¶Ê˜BÖ∂FF÷6ˆ◊&W76ñˆ‚÷6&E“í¬ÁFˆˆ¬÷6&B◊&˜s¶Ê˜BÖ∂FF÷6ˆ◊&W76ñˆ‚÷6&E“ì¶Ê˜BÖ∂FF÷WfVÁB◊GóS“'Fˆˆ¬%“í¬ÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊs¶Ê˜BÖ∂FF÷∆ófR◊FÜñÊ∂ñÊs“#%“ì¶Ê˜BÖ∂FF÷WfVÁB◊GóS“'FÜñÊ∂ñÊr%“í¬Áv¬◊&V6ˆÂ∂FF◊v˜&∂∆ˆr÷Ê6Ü˜"◊&V6ˆ„“#%“¬Áv¬◊&V6ˆÂ∂FF◊v˜&∂∆ˆr◊&V6ˆ‚◊6˜W&6S“'&V6ˆÊñÊr%“ríÊf˜$V6ÇÜV√”ÊV¬Á&V÷˜fRÇíì∞¢6ˆÁ7B'î7FófóGí“ÊWr÷Çì∞¢6ˆÁ7B76ó7FÁDñGá3’≤‚‚Ê76ó7FÁE6Vv÷VÁG2Ê∂Wó2Çï“Á6˜'BÇÜ∆"ì”Ê÷"ì∞¢6ˆÁ7Bˆ76ó7FÁDÊ6Ü˜$f˜$7FófóGì“ÜñGÇ«6Vv÷VÁE6W∆'W'7DñBì”Á∞¢ñbá6Vv÷VÁE6Wó∞¢f˜"Ü6ˆÁ7B6Vrˆb76ó7FÁE6Vv÷VÁG2Áf«VW2Çíó∞¢ñbá6Vrbg6VrÊvWDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wrì””’7G&ñÊrá6Vv÷VÁE6Wíí&WGW&‚6Vs∞¢–¢–¢6ˆÁ7BvÁFVD'W'7C÷'W'7DñB”◊VÊFVfñÊVBbf'W'7DñB”÷ÁV∆¬be7G&ñÊrÜ'W'7DñBí”“rrbe7G&ñÊrÜ'W'7DñBí”“ssı7G&ñÊrÜ'W'7DñBì¢rs∞¢ñbávÁFVD'W'7Bó∞¢f˜"Ü6ˆÁ7B6Vrˆb76ó7FÁE6Vv÷VÁG2Áf«VW2Çíó∞¢ñbá6Vrbg6VrÊvWDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBrì””◊vÁFVD'W'7Bí&WGW&‚6Vs∞¢–¢–¢∆WBÊ6Ü˜%&˜s÷76ó7FÁE6Vv÷VÁG2ÊvWBÜñGÇó«∆ÁV∆√∞¢ñbÇÊ6Ü˜%&˜rbf76ó7FÁDñGá2Ê∆VÊwFÇó∞¢ñbÜñGÉ∆76ó7FÁDñGá5≥“í&WGW&‚ÁV∆√∞¢6ˆÁ7Bf∆∆&6¥ñGÉ’≤‚‚Ê76ó7FÁDñGá5“Á&WfW'6RÇíÊfñÊBÜñGÉ”ÊñGÉ√÷ñGÇì∞¢Ê6Ü˜%&˜s÷f∆∆&6¥ñGÇ”◊VÊFVfñÊVCˆ76ó7FÁE6Vv÷VÁG2ÊvWBÜf∆∆&6¥ñGÇì¶76ó7FÁE6Vv÷VÁG2ÊvWBÜ76ó7FÁDñGá5∂76ó7FÁDñGá2Ê∆VÊwFÇ”“ì∞¢–¢&WGW&‚Ê6Ü˜%&˜s∞¢”∞¢6ˆÁ7B˜GW&‰GW&Fñˆ‰f˜$Ê6Ü˜#“ÜÊ6Ü˜%&˜rì”Á∞¢ñbÇÊ6Ü˜%&˜rí&WGW&‚VÊFVfñÊVC∞¢6ˆÁ7BGW&„÷Ê6Ü˜%&˜rÊ6∆˜6W7BÇrÊ76ó7FÁB◊GW&‚rì∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÇ&∆ˆ6∑2í&WGW&‚VÊFVfñÊVC∞¢∆WBGW&Fñˆ„∞¢f˜"Ü6ˆÁ7B6Vrˆb&∆ˆ6∑2ÁVW'ï6V∆V7F˜$∆¬ÇrÊ76ó7FÁB◊6Vv÷VÁBríó∞¢6ˆÁ7BñGÉ‘ÁV÷&W"á6VrÊFF6WBbg6VrÊFF6WBÊ◊6tñGÇì∞¢6ˆÁ7B◊6s‘ÁV÷&W"Êó4fñÊóFRÜñGÇìı2Ê÷W76vW5∂ñGÖ”¶ÁV∆√∞¢ñbÜ◊6rbf◊6rÂ˜GW&‰GW&Fñˆ‚”◊VÊFVfñÊVBíGW&Fñˆ„÷◊6rÂ˜GW&‰GW&Fñˆ„∞¢–¢&WGW&‚GW&Fñˆ„∞¢”∞¢6ˆÁ7BGW&Fñˆ‰76ñvÊVEGW&Á2“ÊWr6WBÇì∞¢6ˆÁ7B7FófóGî'ïGW&‚“ÊWr÷Çì∞¢6ˆÁ7B7FófóGî˜&FW"“µ”∞¢6ˆÁ7BVÁ7W&T7FófóGî'V6∂WC“Ü∂Wí∆ñGÇ«6Vv÷VÁE6W∆'W'7DñBì”Á∞¢ñbÇ'î7FófóGíÊÜ2Ü∂Wííó∞¢6ˆÁ7BVÁG'ì◊∂∂Wí∆ñGÇ«6Vv÷VÁE6Wß6Vv÷VÁE6W«¬rr∆'W'7DñC¶'W'7DñG«¬rr∆6&G3•µ“«FÜñÊ∂ñÊtñGÉ¶ÁV∆¬∆ñÊ6«VFTÊ6Ü˜%&V6ˆ„¶f«6W”∞¢'î7FófóGíÁ6WBÜ∂Wí∆VÁG'íì∞¢7FófóGî˜&FW"ÁW6ÇÜVÁG'íì∞¢–¢&WGW&‚'î7FófóGíÊvWBÜ∂Wíì∞¢”∞¢6ˆÁ7BÊ˜&÷∆ó¶UFˆ∂V„“áf«VRì”Á∞¢6ˆÁ7BÜ5f«VS◊f«VR”◊VÊFVfñÊVBbgf«VR”÷ÁV∆¬be7G&ñÊráf«VRí”“rrbe7G&ñÊráf«VRí”“ss∞¢&WGW&‚Ü5f«VSı7G&ñÊráf«VRì¢rs∞¢”∞¢6ˆÁ7B∂Ê˜v‰'W'7DñG3÷ÊWr6WBÇì∞¢f˜"Ü6ˆÁ7B2ˆb76ó7FÁE6Vv÷VÁG2Áf«VW2Çííñbá2ó∂6ˆÁ7B#◊2ÊvWDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBrì∂ñbÜ"ñ∂Ê˜v‰'W'7DñG2ÊFBÜ"ì∑–¢f˜"Ü6ˆÁ7BF2ˆbÖ2ÁFˆˆƒ6∆«7«≈µ“íó∞¢ñbÇF2í6ˆÁFñÁVS∞¢6ˆÁ7BFñC◊F2ÁFñG««F2ÊñG««F2ÁFˆˆ≈ˆ6∆≈ˆñG««F2ÁFˆˆ≈˜W6UˆñG««F2Ê6∆≈ˆñG«¬rs∞¢ñbáFñBbgG&Á7&VÁD˜&FW&VEFˆˆƒñG2ÊÜ2áFñBíí6ˆÁFñÁVS∞¢6ˆÁ7BñGÉ◊F2Ê76ó7FÁEˆ◊6uˆñGÇ”◊VÊFVfñÊVC˜'6TñÁBáF2Ê76ó7FÁEˆ◊6uˆñGÇì¢”∞¢ñbÜÊ6Ü˜$˜vÊVD76ó7FÁE&tñGá2ÊÜ2ÜñGÇíí6ˆÁFñÁVS∞¢ñbáfó'GV≈vñÊF˜rÁfó'GV∆ó¶VBbg&VÊFW&&∆U&tñGá2ÊÜ2ÜñGÇíbb&VÊFW&VE&tñGá2ÊÜ2ÜñGÇíí6ˆÁFñÁVS∞¢6ˆÁ7B6Vv÷VÁE6W÷Ê˜&÷∆ó¶UFˆ∂V‚áF2Ê7FófóGï6Vv÷VÁE6Wì∞¢6ˆÁ7B'W'7DñC÷Ê˜&÷∆ó¶UFˆ∂V‚áF2Ê7FófóGî'W'7DñBì∞¢6ˆÁ7B'W'7E&W6ˆ«f&∆S÷'W'7DñBbf∂Ê˜v‰'W'7DñG2ÊÜ2Ü'W'7DñBì∞¢6ˆÁ7B∂Wì◊6Vv÷VÁE6Wˆ6Vv÷VÁC¢G∑6Vv÷VÁE6W÷¢Ü'W'7E&W6ˆ«f&∆Sˆ'W'7C¢G∂'W'7DñG÷¶76ó7FÁC¢G∂ñGá÷ì∞¢6ˆÁ7BVÁG'ì÷VÁ7W&T7FófóGî'V6∂WBÜ∂Wí∆ñGÇ«6Vv÷VÁE6W∆'W'7DñBì∞¢VÁG'íÊ6&G2ÁW6ÇáF2ì∞¢VÁG'íÊñÊ6«VFTÊ6Ü˜%&V6ˆ„◊G'VS∞¢–¢f˜"Ü6ˆÁ7BñGÇˆb76ó7FÁEFÜñÊ∂ñÊrÊ∂Wó2Çíó∞¢ñbÜÊ6Ü˜$˜vÊVD76ó7FÁE&tñGá2ÊÜ2ÜñGÇíí6ˆÁFñÁVS∞¢ñbáfó'GV≈vñÊF˜rÁfó'GV∆ó¶VBbg&VÊFW&&∆U&tñGá2ÊÜ2ÜñGÇíbb&VÊFW&VE&tñGá2ÊÜ2ÜñGÇíí6ˆÁFñÁVS∞¢6ˆÁ7B6Vs÷76ó7FÁE6Vv÷VÁG2ÊvWBÜñGÇì∞¢6ˆÁ7B6Vv÷VÁE6W◊6Vrbg6VrÊvWDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wró«¬rs∞¢6ˆÁ7B'W'7DñC◊6Vrbg6VrÊvWDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBró«¬rs∞¢6ˆÁ7B∂Wì◊6Vv÷VÁE6Wˆ6Vv÷VÁC¢G∑6Vv÷VÁE6W÷¢Ü'W'7DñCˆ'W'7C¢G∂'W'7DñG÷¶76ó7FÁC¢G∂ñGá÷ì∞¢6ˆÁ7BVÁG'ì÷VÁ7W&T7FófóGî'V6∂WBÜ∂Wí∆ñGÇ«6Vv÷VÁE6W∆'W'7DñBì∞¢ñbÜVÁG'íÁFÜñÊ∂ñÊtñGÉ””÷ÁV∆¬íVÁG'íÁFÜñÊ∂ñÊtñGÉ÷ñGÉ∞¢–¢f˜"Ü6ˆÁ7B∂ñGÇ«6Vu“ˆb76ó7FÁE6Vv÷VÁG2ó∞¢ñbÜÊ6Ü˜$˜vÊVD76ó7FÁE&tñGá2ÊÜ2ÜñGÇíí6ˆÁFñÁVS∞¢ñbÇ6Vw«¬6VrÊ6∆74∆ó7G«¬6VrÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6Rríí6ˆÁFñÁVS∞¢ñbáfó'GV≈vñÊF˜rÁfó'GV∆ó¶VBbg&VÊFW&&∆U&tñGá2ÊÜ2ÜñGÇíbb&VÊFW&VE&tñGá2ÊÜ2ÜñGÇíí6ˆÁFñÁVS∞¢ñbÇ˜v˜&∂∆ˆu&V6ˆ‰áF÷ƒg&ˆ‘Ê6Ü˜"á6Vríí6ˆÁFñÁVS∞¢6ˆÁ7B6Vv÷VÁE6W◊6Vrbg6VrÊvWDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wró«¬rs∞¢6ˆÁ7B'W'7DñC◊6Vrbg6VrÊvWDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBró«¬rs∞¢6ˆÁ7B∂Wì◊6Vv÷VÁE6Wˆ6Vv÷VÁC¢G∑6Vv÷VÁE6W÷¢Ü'W'7DñCˆ'W'7C¢G∂'W'7DñG÷¶76ó7FÁC¢G∂ñGá÷ì∞¢6ˆÁ7BVÁG'ì÷VÁ7W&T7FófóGî'V6∂WBÜ∂Wí∆ñGÇ«6Vv÷VÁE6W∆'W'7DñBì∞¢VÁG'íÊñÊ6«VFTÊ6Ü˜%&V6ˆ„◊G'VS∞¢–¢7FófóGî˜&FW"Á6˜'BÇÜ∆"ì”Á∞¢6ˆÁ7BÊ6Ü˜$’ˆ76ó7FÁDÊ6Ü˜$f˜$7FófóGíÜÊñGÇ∆Á6Vv÷VÁE6W∆Ê'W'7DñBì∞¢6ˆÁ7BÊ6Ü˜$#’ˆ76ó7FÁDÊ6Ü˜$f˜$7FófóGíÜ"ÊñGÇ∆"Á6Vv÷VÁE6W∆"Ê'W'7DñBì∞¢6ˆÁ7BñGÑ“ÜÊ6Ü˜$bfÊ6Ü˜$Á&VÁDV∆V÷VÁBìÙ'&íÁ&˜F˜GóRÊñÊFWÑˆbÊ6∆¬ÜÊ6Ü˜$Á&VÁDV∆V÷VÁBÊ6Üñ∆G&V‚∆Ê6Ü˜$ì§ÁV÷&W"‰‘Öı4dUÙîÂDTtU#∞¢6ˆÁ7BñGÑ#“ÜÊ6Ü˜$"bfÊ6Ü˜$"Á&VÁDV∆V÷VÁBìÙ'&íÁ&˜F˜GóRÊñÊFWÑˆbÊ6∆¬ÜÊ6Ü˜$"Á&VÁDV∆V÷VÁBÊ6Üñ∆G&V‚∆Ê6Ü˜$"ì§ÁV÷&W"‰‘Öı4dUÙîÂDTtU#∞¢ñbÜñGÑ”÷ñGÑ"í&WGW&‚ñGÑ÷ñGÑ#∞¢6ˆÁ7B6W÷Á6Vv÷VÁE6W”“rsÙÁV÷&W"ÜÁ6Vv÷VÁE6Wì§ÁV÷&W"‰‘Öı4dUÙîÂDTtU#∞¢6ˆÁ7B6W#÷"Á6Vv÷VÁE6W”“rsÙÁV÷&W"Ü"Á6Vv÷VÁE6Wì§ÁV÷&W"‰‘Öı4dUÙîÂDTtU#∞¢ñbÑÁV÷&W"Êó4fñÊóFRá6WíbdÁV÷&W"Êó4fñÊóFRá6W"íbg6W”◊6W"í&WGW&‚6W◊6W#∞¢6ˆÁ7B'W'7D÷Ê'W'7DñB”“rsÙÁV÷&W"ÜÊ'W'7DñBì§ÁV÷&W"‰‘Öı4dUÙîÂDTtU#∞¢6ˆÁ7B'W'7D#÷"Ê'W'7DñB”“rsÙÁV÷&W"Ü"Ê'W'7DñBì§ÁV÷&W"‰‘Öı4dUÙîÂDTtU#∞¢ñbÑÁV÷&W"Êó4fñÊóFRÜ'W'7DíbdÁV÷&W"Êó4fñÊóFRÜ'W'7D"íbf'W'7D”÷'W'7D"í&WGW&‚'W'7D÷'W'7D#∞¢&WGW&‚ÊñGÇ÷"ÊñGÉ∞¢“ì∞¢ñbÇó5G&Á7&VÁE7G&V“Çíó∞¢f˜"Ü6ˆÁ7BVÁG'íˆb7FófóGî˜&FW"ó∞¢6ˆÁ7B∂ñGÇ«6Vv÷VÁE6W∆'W'7DñB∆6&G2«FÜñÊ∂ñÊtñGÇ∆ñÊ6«VFTÊ6Ü˜%&V6ˆÁ”÷VÁG'ì∞¢ñbÜñGÉ∆76ó7FÁDñGá5≥“í6ˆÁFñÁVS∞¢6ˆÁ7BÊ6Ü˜%&˜s’ˆ76ó7FÁDÊ6Ü˜$f˜$7FófóGíÜñGÇ«6Vv÷VÁE6W∆'W'7DñBì∞¢ñbÇÊ6Ü˜%&˜rí6ˆÁFñÁVS∞¢6ˆÁ7BÊ6Ü˜%&VÁC÷Ê6Ü˜%&˜rÁ&VÁDV∆V÷VÁC∞¢6ˆÁ7BÊ6Ü˜%&V6ˆ‰áF÷√’˜v˜&∂∆ˆu&V6ˆ‰áF÷ƒg&ˆ‘Ê6Ü˜"ÜÊ6Ü˜%&˜rì∞¢6ˆÁ7BFÜñÊ∂ñÊuFWáC◊FÜñÊ∂ñÊtñGÇ”÷ÁV∆√ˆ76ó7FÁEFÜñÊ∂ñÊrÊvWBáFÜñÊ∂ñÊtñGÇì¢rs∞¢ñbÇ6&G2Ê∆VÊwFÇbbÊ6Ü˜%&V6ˆ‰áF÷¬bbFÜñÊ∂ñÊuFWáBí6ˆÁFñÁVS∞¢6ˆÁ7BÊ6Ü˜%GW&„÷Ê6Ü˜%&˜rÊ6∆˜6W7BÇrÊ76ó7FÁB◊GW&‚rì∞¢ñbÇÊ6Ü˜%GW&‚í6ˆÁFñÁVS∞¢ÚÚÜˆó7FVB˜WBˆbFÜRñbÇ7FFRñ&∆ˆ6≤&V∆˜rá6÷RWá&W76ñˆ‚¬6÷P¢ÚÚf«VRí6ÚFÜRVÊBFÇ6‚W6RFÜR˜vÊW'6Üóf7BFÜRw&˜W ¢ÚÚ6ˆÁ7G'V7Fñˆ‚«&VGíW6W2‡¢6ˆÁ7BÊ6Ü˜$ó5v˜&∂∆ˆu6˜W&6S÷Ê6Ü˜%&˜rÊ6∆74∆ó7BbfÊ6Ü˜%&˜rÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6Rrì∞¢∆WB7FFS÷7FófóGî'ïGW&‚ÊvWBÜÊ6Ü˜%GW&‚ì∞¢ñbÇ7FFRó∞¢6ˆÁ7BñÊ6«VFUGW&‰GW&Fñˆ„“GW&Fñˆ‰76ñvÊVEGW&Á2ÊÜ2ÜÊ6Ü˜%GW&‚ì∞¢ñbÜñÊ6«VFUGW&‰GW&Fñˆ‚íGW&Fñˆ‰76ñvÊVEGW&Á2ÊFBÜÊ6Ü˜%GW&‚ì∞¢6ˆÁ7B7FófóGî∂Wì÷76ó7FÁC¢G∂ñGá÷∞¢6ˆÁ7Bw&˜W÷VÁ7W&T7FófóGîw&˜WÜÊ6Ü˜%&VÁB«∞¢6ˆ∆∆6VCßG'VR¿¢Ê6Ü˜#¶Ê6Ü˜%&˜r¿¢&Vf˜&TÊ6Ü˜#¢FÜñÊ∂ñÊuFWáBbbÊ6Ü˜$ó5v˜&∂∆ˆu6˜W&6R¿¢7ñÊ4Ê6Ü˜%&V6ˆ„¶Ê6Ü˜$ó5v˜&∂∆ˆu6˜W&6R¿¢7FófóGî∂Wí¿¢'W'7DñC¶'W'7DñG«¬rr¿¢6Vv÷VÁE6Wß6Vv÷VÁE6W«¬rr¿¢GW&‰GW&Fñˆ„¶ñÊ6«VFUGW&‰GW&Fñˆ„ı˜GW&‰GW&Fñˆ‰f˜$Ê6Ü˜"ÜÊ6Ü˜%&˜rìßVÊFVfñÊVB¿¢“ì∞¢6ˆÁ7B∆ó7C’˜Fˆˆ≈v˜&∂∆ˆt∆ó7DV¬Üw&˜Wì∞¢ñbÇ∆ó7Bí6ˆÁFñÁVS∞¢∆ó7BÊñÊÊW$ÖD‘√“rs∞¢7FFS◊∂w&˜W∆6&G3•µ“«6VVÂ&V6ˆÁ3¶ÊWr6WBÇí«6VVÂFˆˆ«3¶ÊWr6WBÇó”∞¢7FófóGî'ïGW&‚Á6WBÜÊ6Ü˜%GW&‚«7FFRì∞¢–¢7FFRÊ6&G2ÁW6ÇÇ‚‚Ê6&G2ì∞¢ˆVÊEv˜&∂∆ˆu7FWá7FFRÊw&˜W¬Ê6Ü˜%&˜r¬6&G2¬FÜñÊ∂ñÊuFWáB¬∞¢∆ófS¶f«6R¿¢ÚÚV6ÜÚ‚Ê6Ü˜"w2&˜6R2Áv¬◊&V6ˆÊ&˜rˆÊ«ívÜV‚FÜBÊ6Ü˜"v0¢ÚÚfˆ∆FVBñÁFÚFÜó2v˜&∂∆ˆr‚76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6Vó2FÜP¢ÚÚ&ˆˆb¬ÊBóG2Fó7∆ì¶ÊˆÊVó2FÜRˆÊ«í&V6ˆ‚FÜRV6ÜÚó2Ê˜B¢ÚÚ6V6ˆÊBfó6ñ&∆R6˜í‚FÜRw&˜W6ˆÁ7G'V7Fñˆ‚&˜fR«&VGí&V6ˆÁ0¢ÚÚFÜBvíÜ7ñÊ4Ê6Ü˜%&V6ˆÊì≤FÜRVÊBFÇFñBÊ˜B¬6Ú‚Ê6Ü˜ ¢ÚÚFÜBW66W2FÜRfˆ∆BáFÜRGW&‚÷fñÊ¬Á7vW"¬‚ˆW'&˜&÷W76vRê¢ÚÚÜBóG2FWáB&VÊFW&VB&˜FÇñÊ∆ñÊRÊBñÁ6ñFRFÜRv˜&∂∆ˆr‡¢ñÊ6«VFTÊ6Ü˜%&V6ˆ„¢ñÊ6«VFTÊ6Ü˜%&V6ˆ‚bbÊ6Ü˜%&V6ˆ‰áF÷¬bbÊ6Ü˜$ó5v˜&∂∆ˆu6˜W&6R¿¢FÜñÊ∂ñÊt∂WìßFÜñÊ∂ñÊuFWáCˆFÜñÊ∂ñÊs¢GµˆÊ˜&÷∆ó¶UFÜñÊ∂ñÊtV6ÜÙ6ˆ◊&RáFÜñÊ∂ñÊuFWáBó÷¢rr¿¢FÜñÊ∂ñÊtFó66∆˜7W&T∂WìßFÜñÊ∂ñÊuFWáCˆFÜñÊ∂ñÊs¢G∂VÁG'íÊ∂Wó÷¢rr¿¢6VVÂ&V6ˆÁ3ß7FFRÁ6VVÂ&V6ˆÁ2¿¢6VVÂFˆˆ«3ß7FFRÁ6VVÂFˆˆ«2¿¢“ì∞¢–¢7FófóGî'ïGW&‚Êf˜$V6Çá7FFS”Á∞¢˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íá7FFRÊw&˜Wì∞¢“ì∞¢÷V«6W∞¢ÚÚ)H)HG&Á7&VÁE˜7G&V“FÉ¢ñÊFófñGV¬WáÊF&∆RWfVÁB&˜w2)H)H ¢6ˆÁ7BG&Á7&VÁDñÁ6W'D7W'6˜'3÷ÊWr÷Çì∞¢ÚÚW"◊GW&‚FVGWˆbV6ÜˆVBFÜñÊ∂ñÊrFWáB(	B÷ó'&˜'2FÜR6ˆ◊7B◊v˜&∂∆ˆp¢ÚÚFÇw26VVÂ&V6ˆÁ66WBáFÜRG&Á7&VÁB'&Ê6Ç&Wfñ˜W6«íÜBÊˆÊR¿¢ÚÚ6ÚFÜR6÷RV6ÜˆVB&V6ˆÊñÊr&VÊFW&VBGvñ6R¬ˆÊ6R˜WBˆb6á&ˆÊˆ∆ˆvñ6¿¢ÚÚ˜6óFñˆ‚í‚∂WñVB'íFÜR76ó7FÁBGW&‚V∆V÷VÁB‚ÖG&ñfV7FfñÊFñÊrÚ‘'Vs‚ê¢6ˆÁ7BG&Á7&VÁE6VVÂFÜñÊ∂ñÊs÷ÊWr÷Çì∞¢f˜"Ü6ˆÁ7BVÁG'íˆb7FófóGî˜&FW"ó∞¢6ˆÁ7B∂ñGÇ«6Vv÷VÁE6W∆'W'7DñB∆6&G2«FÜñÊ∂ñÊtñGÇ∆ñÊ6«VFTÊ6Ü˜%&V6ˆÁ”÷VÁG'ì∞¢6ˆÁ7B6˜W&6T◊6s÷ñGÉ„”ı2Ê÷W76vW5∂ñGÖ”¶ÁV∆√∞¢6ˆÁ7BWfVÁC◊∞¢‚‚ÊVÁG'í¿¢G3ß6˜W&6T◊6rbbÇá6˜W&6T◊6rÂ˜G2”◊VÊFVfñÊVBbg6˜W&6T◊6rÂ˜G2”÷ÁV∆¬ì˜6˜W&6T◊6rÂ˜G3ß6˜W&6T◊6rÁFñ÷W7F◊í¿¢FÜñÊ∂ñÊuFWáCßFÜñÊ∂ñÊtñGÇ”÷ÁV∆√ˆ76ó7FÁEFÜñÊ∂ñÊrÊvWBáFÜñÊ∂ñÊtñGÇì¢rr¿¢”∞¢ñbÜñGÉ∆76ó7FÁDñGá5≥“í6ˆÁFñÁVS∞¢6ˆÁ7BÊ6Ü˜%&˜s’ˆ76ó7FÁDÊ6Ü˜$f˜$7FófóGíÜñGÇ«6Vv÷VÁE6W∆'W'7DñBì∞¢ñbÇÊ6Ü˜%&˜rí6ˆÁFñÁVS∞¢6ˆÁ7BÊ6Ü˜%GW&„÷Ê6Ü˜%&˜rÊ6∆˜6W7BÇrÊ76ó7FÁB◊GW&‚rì∞¢6ˆÁ7BGW&„÷Ê6Ü˜%GW&„∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2ÜÊ6Ü˜%GW&‚ì∞¢ñbÇÊ6Ü˜%GW&Á«¬&∆ˆ6∑2í6ˆÁFñÁVS∞¢6ˆÁ7BÊ6Ü˜$ó5v˜&∂∆ˆu6˜W&6S÷Ê6Ü˜%&˜rÊ6∆74∆ó7BbfÊ6Ü˜%&˜rÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6Rrì∞¢6ˆÁ7BñÁ6W'DgFW$7W'6˜#“á&˜rì”Á∞¢6ˆÁ7B7W'6˜#◊G&Á7&VÁDñÁ6W'D7W'6˜'2ÊvWBÜÊ6Ü˜%&˜ró«∆Ê6Ü˜%&˜s∞¢6ˆÁ7B&Vc÷7W'6˜"bf7W'6˜"Á&VÁDV∆V÷VÁC””÷&∆ˆ6∑3ˆ7W'6˜"ÊÊWáDV∆V÷VÁE6ñ&∆ñÊs¶ÁV∆√∞¢ñbá&Vbbg&VbÁ&VÁDV∆V÷VÁC””÷&∆ˆ6∑2í&∆ˆ6∑2ÊñÁ6W'D&Vf˜&Rá&˜r«&Vbì∞¢V«6R&∆ˆ6∑2ÊVÊD6Üñ∆Bá&˜rì∞¢G&Á7&VÁDñÁ6W'D7W'6˜'2Á6WBÜÊ6Ü˜%&˜r«&˜rì∞¢”∞¢6ˆÁ7BñÁ6W'D&Vf˜&TÊ6Ü˜#“á&˜rì”Á∞¢ñbÜÊ6Ü˜%&˜rbfÊ6Ü˜%&˜rÁ&VÁDV∆V÷VÁC””÷&∆ˆ6∑2í&∆ˆ6∑2ÊñÁ6W'D&Vf˜&Rá&˜r∆Ê6Ü˜%&˜rì∞¢V«6R&∆ˆ6∑2ÊVÊD6Üñ∆Bá&˜rì∞¢”∞¢ñbÜWfVÁBÁFÜñÊ∂ñÊuFWáBó∞¢6ˆÁ7B˜FÜñÊ¥∂Wì◊GóVˆbˆÊ˜&÷∆ó¶UFÜñÊ∂ñÊtV6ÜÙ6ˆ◊&S””“vgVÊ7Fñˆ‚p¢ÚˆÊ˜&÷∆ó¶UFÜñÊ∂ñÊtV6ÜÙ6ˆ◊&RÜWfVÁBÁFÜñÊ∂ñÊuFWáBê¢¢7G&ñÊrÜWfVÁBÁFÜñÊ∂ñÊuFWáBíÁG&ñ“Çì∞¢∆WB˜6VV„◊G&Á7&VÁE6VVÂFÜñÊ∂ñÊrÊvWBÜÊ6Ü˜%GW&‚ì∞¢ñbÇ˜6VV‚óµ˜6VV„÷ÊWr6WBÇì∑G&Á7&VÁE6VVÂFÜñÊ∂ñÊrÁ6WBÜÊ6Ü˜%GW&‚≈˜6VV‚ì∑–¢ñbÖ˜FÜñÊ¥∂Wíbe˜6VV‚ÊÜ2Ö˜FÜñÊ¥∂Wííó∞¢ÚÚV6ÜˆVB&V6ˆÊñÊr«&VGí&VÊFW&VBf˜"FÜó2GW&‚(	B6∂óFÜRGW∆ñ6FR‡¢÷V«6W∞¢ñbÖ˜FÜñÊ¥∂Wíï˜6VV‚ÊFBÖ˜FÜñÊ¥∂Wíì∞¢6ˆÁ7BFÜñÊ∂ñÊu&˜s’ˆFV6˜&FUG&Á7&VÁDWfVÁE&˜rÖ˜FÜñÊ∂ñÊt7FófóGîÊˆFRÜWfVÁBÁFÜñÊ∂ñÊuFWáB∆f«6Rí«∞¢GóS¢wFÜñÊ∂ñÊrr¿¢FWáC¶WfVÁBÁFÜñÊ∂ñÊuFWáB¿¢&WfñWs¶WfVÁBÁFÜñÊ∂ñÊuFWáB¿¢G3¶WfVÁBÁG2¿¢6Vv÷VÁE6W¿¢'W'7DñB¿¢“ì∞¢ñbÇÊ6Ü˜$ó5v˜&∂∆ˆu6˜W&6RíñÁ6W'D&Vf˜&TÊ6Ü˜"áFÜñÊ∂ñÊu&˜rì∞¢V«6RñÁ6W'DgFW$7W'6˜"áFÜñÊ∂ñÊu&˜rì∞¢–¢–¢f˜"Ü6ˆÁ7BFˆˆƒ6∆¬ˆb6&G2ó∞¢WfVÁBÁFˆˆƒ6∆√◊Fˆˆƒ6∆√∞¢6ˆÁ7BFˆˆ≈&˜s’ˆFV6˜&FUG&Á7&VÁDWfVÁE&˜rÜ'Vñ∆EFˆˆƒ6&BÜWfVÁBÁFˆˆƒ6∆¬í«∞¢GóS¢wFˆˆ¬r¿¢Ê÷S¶WfVÁBÁFˆˆƒ6∆¬bfWfVÁBÁFˆˆƒ6∆¬ÊÊ÷R¿¢7FGW3•˜G&Á7&VÁEFˆˆ≈7FGW2ÜWfVÁBÁFˆˆƒ6∆¬«G'VRí¿¢Fˆˆƒ6∆√¶WfVÁBÁFˆˆƒ6∆¬¿¢G3¶WfVÁBÁG2¿¢6Vv÷VÁE6W¿¢'W'7DñB¿¢“ì∞¢ñÁ6W'DgFW$7W'6˜"áFˆˆ≈&˜rì∞¢–¢˜7ñÊ5G&Á7&VÁDWfVÁD6ˆÁG&ˆ«2áGW&‚ì∞¢–¢–¢–¢f˜"Ü6ˆÁ7B∑&tñGÇ«6Vu“ˆb76ó7FÁE6Vv÷VÁG2ó∞¢6ˆÁ7B◊6s’2Ê÷W76vW5∑&tñGÖ”∞¢ñbÜ◊6rbf◊6rÂˆÊ6Ü˜%ˆ7FófóGï˜66VÊRó∞¢˜&VÊFW%6WGF∆VDÊ6Ü˜%66VÊTf˜$÷W76vRÜ◊6r¬6Vr¬&tñGÇì∞¢–¢–¢˜&W7F˜&Uv˜&∂∆ˆtFWFñƒFó66∆˜7W&U7FFRÜñÊÊW"¬v˜&∂∆ˆtFWFñƒFó66∆˜7W&U7FFRì∞¢ÚÚ3SÉ3ífóÉ¢FVfW'&VB6WGF∆VBv˜&∂∆ˆw2ÜfRÊÚ&˜w2ñWBB&W7F˜&RFñ÷R¬6¢ÚÚFÜRFó66∆˜7W&R&W7F˜&R&˜fR6‚wB&V6ÇFÜVó"FWFñ¬V∆V÷VÁG2‚7F6ÇFÜP¢ÚÚ6GW&VB7FFRˆ‚V6Ç7Fñ∆¬÷FVfW'&VBw&˜W≤ˆ÷FW&ñ∆ó¶TFVfW'&VEv˜&∂∆ˆu&˜w0¢ÚÚ&R÷∆ñW2óBÜ∂Wí◊66˜VB≤ñFV◊˜FVÁBíˆÊ6RFÜR&˜w2WÜó7Bˆ‚WáÊB‡¢ñbáv˜&∂∆ˆtFWFñƒFó66∆˜7W&U7FFRbgv˜&∂∆ˆtFWFñƒFó66∆˜7W&U7FFRÁ6ó¶Ró∞¢ñÊÊW"ÁVW'ï6V∆V7F˜$∆¬Çu∂FF◊v˜&∂∆ˆr◊&˜w2÷FVfW'&VC“#%“ríÊf˜$V6ÇÜw&˜W”Á∞¢w&˜WÂˆFVfW'&VEv˜&∂∆ˆtFó66∆˜7W&S◊v˜&∂∆ˆtFWFñƒFó66∆˜7W&U7FFS∞¢“ì∞¢–¢ÚÚ&VÊFW"W"◊GW&‚GW&Fñˆ‚ÊB˜FñˆÊ¬Fˆ∂V‚W6vRˆ‚76ó7FÁB÷W76vW2‡¢ÚÚGW&Fñˆ‚7Fó2fó6ñ&∆RWfV‚vÜV‚Fˆ∂V‚W6vRó2Fó6&∆VB¬&V6W6RóBÁ7vW'0¢ÚÚFÜR&6ñ2&Ü˜r∆ˆÊrFñBFÜBGW&‚F∂SÚ"UÇVW7Fñˆ‚‚ˆÊ«ív∆≤&VÊFW&V@¢ÚÚ76ó7FÁB6Vv÷VÁG26ÚÜñFFV‚÷W76vW2&˜fRFÜRDÙ“vñÊF˜r6ÊÊ˜B6∂WrFÜP¢ÚÚfˆ˜FW"◊FÚ÷÷W76vR÷ñÊr‡¢∞¢6ˆÁ7B&VÊFW&VD76ó7FÁDñGá3’≤‚‚Ê76ó7FÁE6Vv÷VÁG2Ê∂Wó2Çï“Á6˜'BÇÜ∆"ì”Ê÷"ì∞¢f˜"Ü6ˆÁ7B÷íˆb&VÊFW&VD76ó7FÁDñGá2ó∞¢6ˆÁ7B◊6s’2Ê÷W76vW5∂÷ï◊««∑”∞¢ñbÜ◊6rÁ&ˆ∆R”“v76ó7FÁBrí6ˆÁFñÁVS∞¢6ˆÁ7B&˜WFñÊs÷◊6rÂˆvFWvï&˜WFñÊw«∆ÁV∆√∞¢6ˆÁ7BvFWvïFWáC’ˆf˜&÷DvFWvî÷ˆFVƒ∆&V¬Ö7G&ñÊrÜ◊6rÂ˜W6VD÷ˆFV««¬rríÁG&ñ“Çó«¬Ö2Á6W76ñˆ‚be2Á6W76ñˆ‚Ê÷ˆFV¬ó«¬rr¬rr¬&˜WFñÊrì∞¢6ˆÁ7Bfñ∆˜fW%FWáC’ˆvFWvï&˜WFñÊtfñ∆˜fW%FWáBá&˜WFñÊrì∞¢6ˆÁ7B÷ˆFV≈v&ÊñÊuFWáC’ˆvFWvî÷ˆFV≈v&ÊñÊuFWáBá&˜WFñÊrì∞¢6ˆÁ7BÜ5GW&ÂW6vS“◊6rÂ˜GW&ÂW6vS∞¢ÚÚFÜRv˜&∂∆ˆr7V÷÷'í˜vÁ2FÜR$FˆÊRñ‚(
b"GW&Fñˆ‚vÜVÊWfW"FÜó0¢ÚÚ76ó7FÁB÷W76vR6ˆÁG&ñ'WFW2Fˆˆ¬˜"FÜñÊ∂ñÊrFWFñ¬FÚfˆ∆FV@¢ÚÚv˜&∂∆ˆr&˜fRFÜRfñÊ¬Á7vW"‡¢6ˆÁ7B6ˆ◊7Ev˜&∂∆ˆtf˜$÷W76vS÷ó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇíbbáFˆˆƒ6∆ƒ76ó7FÁDñGá2ÊÜ2Ü÷íó«∆76ó7FÁEFÜñÊ∂ñÊrÊÜ2Ü÷ííì∞¢6ˆÁ7BGW&FñˆÂFWáC÷6ˆ◊7Ev˜&∂∆ˆtf˜$÷W76vSÚrs•ˆf˜&÷EGW&‰GW&Fñˆ‚Ü◊6rÂ˜GW&‰GW&Fñˆ‚ì∞¢6ˆÁ7BW6VD÷ˆFV≈FWáC’˜W6VD÷ˆFV≈GW&‰6Üó∆&V¬Ü◊6rì∞¢ñbÇÜ5GW&ÂW6vRbbGW&FñˆÂFWáBbbvFWvïFWáBbbfñ∆˜fW%FWáBbb÷ˆFV≈v&ÊñÊuFWáBbbW6VD÷ˆFV≈FWáBí6ˆÁFñÁVS∞¢6ˆÁ7B6Vs÷76ó7FÁE6Vv÷VÁG2ÊvWBÜ÷íì∞¢6ˆÁ7B&˜s◊6Vs˜6VrÊ6∆˜6W7BÇrÊ76ó7FÁB◊GW&‚rì¶ÁV∆√∞¢6ˆÁ7Bfˆ˜FW%&˜w3◊&˜s˜&˜rÁVW'ï6V∆V7F˜$∆¬ÇrÊ◊6r÷fˆ˜Brì•µ”∞¢6ˆÁ7BF&vWDfˆ˜C÷fˆ˜FW%&˜w2Ê∆VÊwFÉˆfˆ˜FW%&˜w5∂fˆ˜FW%&˜w2Ê∆VÊwFÇ””¶ÁV∆√∞¢ñbÇF&vWDfˆ˜G««F&vWDfˆ˜BÁVW'ï6V∆V7F˜"ÇrÊ◊6r◊W6vR÷ñÊ∆ñÊR¬Ê◊6r÷GW&Fñˆ‚÷ñÊ∆ñÊR¬Ê◊6r÷vFWví÷ñÊ∆ñÊR¬ÊvFWví÷fñ∆˜fW"÷ñÊ∆ñÊR¬Ê◊6r÷÷ˆFV¬◊v&ÊñÊr÷ñÊ∆ñÊR¬Ê◊6r◊W6VB÷÷ˆFV¬÷ñÊ∆ñÊRríí6ˆÁFñÁVS∞¢6ˆÁ7Bg&v÷VÁG3’µ”∞¢ñbÜ÷ˆFV≈v&ÊñÊuFWáBó∞¢6ˆÁ7Bv&ÊñÊs÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢v&ÊñÊrÊ6∆74Ê÷S“v◊6r÷÷ˆFV¬◊v&ÊñÊr÷ñÊ∆ñÊRs∞¢v&ÊñÊrÁFWáD6ˆÁFVÁC÷÷ˆFV≈v&ÊñÊuFWáC∞¢g&v÷VÁG2ÁW6Çáv&ÊñÊrì∞¢–¢ñbÜfñ∆˜fW%FWáBó∞¢6ˆÁ7Bfñ∆˜fW#÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢fñ∆˜fW"Ê6∆74Ê÷S“vvFWví÷fñ∆˜fW"÷ñÊ∆ñÊRs∞¢fñ∆˜fW"ÁFWáD6ˆÁFVÁC÷fñ∆˜fW%FWáC∞¢g&v÷VÁG2ÁW6ÇÜfñ∆˜fW"ì∞¢–¢ñbÜvFWvïFWáBó∞¢6ˆÁ7BvFWvì÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢vFWvíÊ6∆74Ê÷S“v◊6r÷vFWví÷ñÊ∆ñÊRs∞¢vFWvíÁFWáD6ˆÁFVÁC÷vFWvïFWáC∞¢g&v÷VÁG2ÁW6ÇÜvFWvíì∞¢–¢ñbÜGW&FñˆÂFWáBó∞¢6ˆÁ7BGW&Fñˆ„÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢GW&Fñˆ‚Ê6∆74Ê÷S“v◊6r÷GW&Fñˆ‚÷ñÊ∆ñÊRs∞¢GW&Fñˆ‚ÁFWáD6ˆÁFVÁC÷FˆÊRñ‚G∂GW&FñˆÂFWáG÷∞¢g&v÷VÁG2ÁW6ÇÜGW&Fñˆ‚ì∞¢–¢ÚÚFÜRG&Á7&VÁBGW&‚fˆ˜FW"˜vÁ2FÜR÷ˆFV¬∆&V¬ÇÊ∆b÷÷ˆFV¬ívÜVÊWfW ¢ÚÚFÜRGW&‚Ü2G&Á7&VÁBWfVÁB&˜w2(	B6∂óFÜRvVÊW&ñ26ÜóFÜW&R6¢ÚÚWÜ7F«íˆÊR÷ˆFV¬∆&V¬&VÊFW'2W"GW&‚‚÷ˆFV¬6óG2gFW"GW&Fñˆ‚F¢ÚÚ÷F6ÇFÜRG&Á7&VÁBfˆ˜FW"˜&FW"ÜV∆6VB+r÷ˆFV¬+r(
bí‡¢6ˆÁ7B˜G&Á7&VÁDfˆ˜FW$˜vÁ4÷ˆFV√◊W6VD÷ˆFV≈FWáBbfó5G&Á7&VÁE7G&V“Çíbg&˜rbbÇÇì”Á∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2á&˜rì∞¢&WGW&‚Ü&∆ˆ6∑2bf&∆ˆ6∑2ÁVW'ï6V∆V7F˜"Çsß66˜R‚ÁG&Á7&VÁB÷WfVÁB◊&˜rríì∞¢“íÇì∞¢ñbáW6VD÷ˆFV≈FWáBbb˜G&Á7&VÁDfˆ˜FW$˜vÁ4÷ˆFV¬ó∞¢6ˆÁ7BW6VD÷ˆFV√÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢W6VD÷ˆFV¬Ê6∆74Ê÷S“v◊6r◊W6VB÷÷ˆFV¬÷ñÊ∆ñÊRs∞¢W6VD÷ˆFV¬ÁFWáD6ˆÁFVÁC◊W6VD÷ˆFV≈FWáC∞¢ÚÚ&W6W'fRFÜRgV∆¬áVÊ6ˆ◊7FVBí÷ˆFV¬ñBˆ‚Ü˜fW"vÜW&Rfñ∆&∆R‡¢6ˆÁ7BW6VD÷ˆFVƒgV∆√’7G&ñÊrÜ◊6rÂ˜W6VD÷ˆFV««¬rríÁG&ñ“Çì∞¢ñbáW6VD÷ˆFVƒgV∆¬bgW6VD÷ˆFVƒgV∆¬”◊W6VD÷ˆFV≈FWáBíW6VD÷ˆFV¬ÁFóF∆S◊W6VD÷ˆFVƒgV∆√∞¢g&v÷VÁG2ÁW6ÇáW6VD÷ˆFV¬ì∞¢–¢ñbávñÊF˜rÂ˜6Ü˜uFˆ∂VÂW6vRbfÜ5GW&ÂW6vRó∞¢6ˆÁ7BW6vS÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢W6vRÊ6∆74Ê÷S“v◊6r◊W6vR÷ñÊ∆ñÊRs∞¢6ˆÁ7BñÂFˆ≥÷◊6rÂ˜GW&ÂW6vRÊñÁWE˜Fˆ∂VÁ7«√∞¢6ˆÁ7B˜WEFˆ≥÷◊6rÂ˜GW&ÂW6vRÊ˜WGWE˜Fˆ∂VÁ7«√∞¢6ˆÁ7B6˜7C÷◊6rÂ˜GW&ÂW6vRÊW7Fñ÷FVEˆ6˜7C∞¢∆WBFWáC÷Gµˆf◊EFˆ∂VÁ2ÜñÂFˆ≤ó“ñ‚+rGµˆf◊EFˆ∂VÁ2Ü˜WEFˆ≤ó“˜WF∞¢ñbÜ6˜7BíFWáB≥÷+r‚BG∂6˜7C√„ˆ6˜7BÁFÙfóÜVBÉBì¶6˜7BÁFÙfóÜVBÉ"ó÷∞¢6ˆÁ7B66ÜTÜóE7C÷◊6rÂ˜GW&ÂW6vRÊ66ÜUˆÜóE˜W&6VÁC∞¢ñbÜ66ÜTÜóE7B÷ÁV∆¬íFWáB≥÷+rG∑BÇwW6vUˆ66ÜVE˜W&6VÁBr∆66ÜTÜóE7Bó÷∞¢W6vRÁFWáD6ˆÁFVÁC◊FWáC∞¢g&v÷VÁG2ÁW6ÇáW6vRì∞¢–¢ñbÜg&v÷VÁG2Ê∆VÊwFÇó∞¢F&vWDfˆ˜BÊ6∆74∆ó7BÊFBÇv◊6r÷fˆ˜B◊vóFÇ◊W6vRrì∞¢f˜"Ü∆WBì÷g&v÷VÁG2Ê∆VÊwFÇ”∂ì„”∂í““ó∞¢ÚÚwV&C¢fó'7D6Üñ∆B÷í&RÁV∆¬ÜV◊Gífˆ˜Bí˜"˜'ÜÊVB‡¢6ˆÁ7Bfó'7D6Üñ∆C◊F&vWDfˆ˜BÊfó'7D6Üñ∆C∞¢ñbÜfó'7D6Üñ∆Bbffó'7D6Üñ∆BÁ&VÁDÊˆFS””◊F&vWDfˆ˜BíF&vWDfˆ˜BÊñÁ6W'D&Vf˜&RÜg&v÷VÁG5∂ï“¬fó'7D6Üñ∆Bì∞¢V«6RF&vWDfˆ˜BÊVÊD6Üñ∆BÜg&v÷VÁG5∂ï“ì∞¢–¢–¢–¢–¢ÚÚG&Á7&VÁB÷ˆFRW"◊GW&‚vó&ñÊs¢6ˆ∆∆6ñ&∆RÜW&÷W26ÜBÊ÷RFr¬ˆ∆B÷WfVÁ@¢ÚÚfFñÊr¬ÊBFÜR&˜GFˆ“÷ˆb◊GW&‚fˆ˜FW"ÜV∆6VB+rFˆ∂VÁ2+rEDeB+r7FGW2í‡¢ÚÚ'VÁ2gFW"FÜRW"◊GW&‚GW&Fñˆ‚&∆ˆ6≤&˜fR6ÚFÜRfˆ˜FW"6‚&WW6RFÜP¢ÚÚ6ˆ◊WFVBGW&FñˆÂFWáBÚFˆ∂VÁ2ÚEDeBf˜"V6Ç6WGF∆VB76ó7FÁBGW&‚‡¢ñbÜó5G&Á7&VÁE7G&V“Çíó∞¢f˜"Ü6ˆÁ7BGW&‚ˆbñÊÊW"ÁVW'ï6V∆V7F˜$∆¬ÇrÊ76ó7FÁB◊GW&‚ríó∞¢ñbáGW&‚ÊñC””“v∆ófT76ó7FÁEGW&‚rí6ˆÁFñÁVS∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÇ&∆ˆ6∑2í6ˆÁFñÁVS∞¢6ˆÁ7BÜ5G&Á7&VÁE&˜w3÷&∆ˆ6∑2ÁVW'ï6V∆V7F˜"Çsß66˜R‚ÁG&Á7&VÁB÷WfVÁB◊&˜rrì∞¢˜vó&UG&Á7&VÁEGW&ÂFˆvv∆RáGW&‚ì∞¢ÚÚ&W7F˜&R6ˆ∆∆6R7FFRg&ˆ“FÜR÷á7W'fófW2DÙ“&V'Vñ∆Bí‡¢6ˆÁ7B6Vs◊GW&‚ÁVW'ï6V∆V7F˜"ÇrÊ76ó7FÁB◊6Vv÷VÁBrì∞¢ñbá6Vrbg6ñBó∞¢6ˆÁ7B÷ì◊6VrÊvWDGG&ñ'WFRÇvFF÷◊6r÷ñGÇrì∞¢ñbÜ÷í÷ÁV∆¬be˜G&Á7&VÁEGW&‰6ˆ∆∆6VE7FFW5∂G∑6ñG”¢G∂÷ó÷“ó∞¢GW&‚Á6WDGG&ñ'WFRÇvFF◊G&Á7&VÁB◊GW&‚÷6ˆ∆∆6VBr¬srì∞¢6ˆÁ7B&ˆ∆S◊GW&‚ÁVW'ï6V∆V7F˜"ÇrÊ◊6r◊&ˆ∆RÊ76ó7FÁBrì∞¢ñbá&ˆ∆Rí&ˆ∆RÁ6WDGG&ñ'WFRÇv&ñ÷WáÊFVBr¬vf«6Rrì∞¢–¢–¢ˆ«ïG&Á7&VÁE&˜tfFñÊráGW&‚ì∞¢ñbÜÜ5G&Á7&VÁE&˜w2ó∞¢ÚÚ&VBGW&‚÷WFFFg&ˆ“FÜRfñÊ¬÷WFFF÷&V&ñÊr76ó7FÁB6Vv÷VÁB¿¢ÚÚÊ˜BVW'ï6V∆V7F˜"w2fó'7B÷F6Ç(	BFˆˆ¬GW&‚w27FófóGí6Vv÷VÁ@¢ÚÚ&V6VFW2FÜRÁ7vW"¬ÊBFÜR÷WFFF∆ófW2ˆ‚FÜR∆7B÷W76vP¢ÚÚÇ3ccÇvFR&˜VÊB#¢◊V«Fí◊6Vv÷VÁBGW&Á2∆˜7BFÜR÷ˆFV¬∆&V¬í‡¢6ˆÁ7B◊6s’˜G&Á7&VÁEGW&‰÷WF÷W76vRáGW&‚ì∞¢∆WBGW&FñˆÂFWáC“rs∞¢∆WB÷ˆFV≈FWáC“rs∞¢∆WB÷ˆFV≈FóF∆S“rs∞¢∆WBGFgEFWáC“rs∞¢∆WBFˆ∂VÁ5FWáC“rs∞¢ñbÜ◊6ró∞¢ñbÜ◊6rÂ˜GW&‰GW&Fñˆ‚÷ÁV∆¬íGW&FñˆÂFWáC’ˆf˜&÷EGW&‰GW&Fñˆ‚Ü◊6rÂ˜GW&‰GW&Fñˆ‚ì∞¢÷ˆFV≈FWáC’˜W6VD÷ˆFV≈GW&‰6Üó∆&V¬Ü◊6rì∞¢ñbÜ÷ˆFV≈FWáBí÷ˆFV≈FóF∆S’7G&ñÊrÜ◊6rÂ˜W6VD÷ˆFV««¬rríÁG&ñ“Çì∞¢ñbÜ◊6rÂˆfó'7EFˆ∂V‰◊2÷ÁV∆¬íGFgEFWáC’ˆf˜&÷Dfó'7EFˆ∂V‚Ü◊6rÂˆfó'7EFˆ∂V‰◊2ì∞¢ñbÜ◊6rÂ˜GW&ÂW6vRó∞¢6ˆÁ7BñÂFˆ≥÷◊6rÂ˜GW&ÂW6vRÊñÁWE˜Fˆ∂VÁ7«√∞¢6ˆÁ7B˜WEFˆ≥÷◊6rÂ˜GW&ÂW6vRÊ˜WGWE˜Fˆ∂VÁ7«√∞¢Fˆ∂VÁ5FWáC÷Gµˆf◊EFˆ∂VÁ2ÜñÂFˆ≤ó“ñ‚+rGµˆf◊EFˆ∂VÁ2Ü˜WEFˆ≤ó“˜WF∞¢–¢–¢˜&VÊFW%G&Á7&VÁEGW&‰fˆ˜FW"áGW&‚«∞¢GW&FñˆÂFWáB¿¢÷ˆFV≈FWáB¿¢÷ˆFV≈FóF∆R¿¢GFgEFWáB¿¢Fˆ∂VÁ5FWáB¿¢7FGW5FWáC¢BÇvFˆÊRró«¬tFˆÊRr¿¢“ì∞¢÷V«6W∞¢ÚÚÊÚG&Á7&VÁB&˜w2(i"ÊÚfˆ˜FW"ÊVVFVB‡¢˜&VÊFW%G&Á7&VÁEGW&‰fˆ˜FW"áGW&‚«∑“ì∞¢–¢–¢–¢ÚÚfñ¬◊6fRñÁf&ñÁBÇ33ÉsRì¢6WGF∆VB76ó7FÁBGW&‚◊W7BÊWfW"&VÊFW"vóFÄ¢ÚÚ§U$Úfó6ñ&∆R6ˆÁFVÁB‚FÜRv˜&∂∆ˆr&VFW6ñv‚Ç33Cífˆ∆G2ñÁFW&÷VFñFP¢ÚÚ76ó7FÁB6Vv÷VÁG2ñÁFÚ6ˆ∆∆6VBv˜&∂∆ˆr6&BÊBÜñFW2FÜR6˜W&6R6Vv÷VÁ@¢ÚÚÜ76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6V(i"Fó7∆ì¶ÊˆÊRí‚FÜBó26˜'&V7BtÑT‚FÜP¢ÚÚGW&‚«6ÚÜ2fó6ñ&∆RfñÊ¬Á7vW"‚'WBvÜV‚GW&‚w2Ù‰≈í6ˆÁFVÁBó2fˆ∆FV@¢ÚÚñÁFÚ6ˆ∆∆6VBv˜&∂∆ˆrÜRÊr‚‚WFˆÊˆ÷˜W2ˆñÁFW''WFVB'V‚vÜ˜6RfñÊ¿¢ÚÚ76ó7FÁB÷W76vRó2V◊Gí¬˜"&V∆ˆBvÜW&R2ÁFˆˆƒ6∆«2FñF‚wBáñG&FR6ÚFÜP¢ÚÚv˜&∂∆ˆr6&B'Vñ«BvóFÇÊÚWáÊF&∆RFˆˆ¬7FW2í¬WfW'í6Vv÷VÁBó2ÜñFFV‚Ê@¢ÚÚFÜRGW&‚ñÁG22Ê˜FÜñÊr(	B∆VfñÊrFÜRG&Á67&óB&&R7F6≤ˆbFFP¢ÚÚ6W&F˜'2Ç33ÉsR'&ñ6≤í‚&WfV¬7V6ÇGW&Á26ÚFÜVó"6ˆÁFVÁBó2ÊWfW"6ñ∆VÁF«ê¢ÚÚ7v∆∆˜vVC¢WáÊBFÜRGW&‚w2v˜&∂∆ˆrw&˜Wá2ívÜV‚FÜRGW&‚Ü2ÊÚ˜FÜW ¢ÚÚfó6ñ&∆R6ˆÁFVÁB‚FÜó2‰UdU"F˜V6ÜW2GW&‚FÜBÜ2Áífó6ñ&∆R6Vv÷VÁB¬6ÚFÜP¢ÚÚñÁFVÊFVB6ˆ∆∆6VB’v˜&∂∆ˆrUÇó2&W6W'fVBvÜVÊWfW"fó6ñ&∆RÁ7vW"WÜó7G2‡¢ÚÚFÜR∆ófRGW&‚ó2WÜ6«VFVB'íóG2∆ófT76ó7FÁEGW&ÊñBÜóBG&ófW2óG2˜v‡¢ÚÚ7FFRGW&ñÊr7G&V“í¬6ÚFÜó27vVWó26fRFÚ'V‚WfV‚vÜñ∆R'W7í(	B¢ÚÚÜó7F˜&ñ6¬&∆Ê≤GW&‚◊W7BÊ˜B&R◊ñÁB&∆Ê≤GW&ñÊrfˆ∆∆˜r◊W7G&V–¢ÚÚÑ˜W2Gfó6˜"¬7FvR”3C"í‡¢∞¢6ˆÁ7B˜GW&‰Ü5fó6ñ&∆T6ˆÁFVÁC“áGW&‚ì”Á∞¢ñbáGóVˆbˆ76ó7FÁEGW&‰Ü5fó6ñ&∆U&VÊFW&VE6Vv÷VÁC””“vgVÊ7Fñˆ‚ró∞¢&WGW&‚ˆ76ó7FÁEGW&‰Ü5fó6ñ&∆U&VÊFW&VE6Vv÷VÁBáGW&‚ì””◊G'VS∞¢–¢ÚÚ∂VWFÜRWáG&7FVB&VÊFW$÷W76vW2FW7BÜ&ÊW726V∆b÷6ˆÁFñÊVB‡¢ñbÇGW&Á««GóVˆbGW&‚ÁVW'ï6V∆V7F˜$∆¬”“vgVÊ7Fñˆ‚rí&WGW&‚f«6S∞¢f˜"Ü6ˆÁ7B6VrˆbGW&‚ÁVW'ï6V∆V7F˜$∆¬ÇrÊ76ó7FÁB◊6Vv÷VÁBríó∞¢ñbá6VrÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6Rríí6ˆÁFñÁVS∞¢ñbá6VrÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv76ó7FÁB◊6Vv÷VÁB÷Ê6Ü˜"ríí6ˆÁFñÁVS∞¢ñbÇá6VrÁFWáD6ˆÁFVÁG«¬rríÁG&ñ“Çíí&WGW&‚G'VS∞¢–¢&WGW&‚f«6S∞¢”∞¢f˜"Ü6ˆÁ7BGW&‚ˆbñÊÊW"ÁVW'ï6V∆V7F˜$∆¬ÇrÊ76ó7FÁB◊GW&‚ríó∞¢ñbáGW&‚ÊñC””“v∆ófT76ó7FÁEGW&‚rí6ˆÁFñÁVS≤ÚÚ∆ófRGW&‚G&ófW2óG2˜v‚7FFP¢ñbÖ˜GW&‰Ü5fó6ñ&∆T6ˆÁFVÁBáGW&‚íí6ˆÁFñÁVS∞¢ÚÚÊÚfó6ñ&∆R6ˆÁFVÁB(	B7W&f6RFÜRfˆ∆FVBv˜&∂∆ˆr6ÚFÜRGW&‚ó6‚wB&∆Ê≤‡¢6ˆÁ7Bw&˜W3◊GW&‚ÁVW'ï6V∆V7F˜$∆¬ÇrÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W¬ÁFˆˆ¬÷6∆¬÷w&˜Wrì∞¢∆WB&WfV∆VC÷f«6S∞¢f˜"Ü6ˆÁ7Bw&˜Wˆbw&˜W2ó∞¢ÚÚ6WGF∆VBv˜&∂∆ˆrvÜ˜6R&˜w2&R7Fñ∆¬FVfW'&VBÇ3SÉ3ííÜ2‚V◊Gê¢ÚÚFWáD6ˆÁFVÁB'WBó2Ê˜BV◊Gíñ‚7V'7FÊ6R‚ßVFvñÊróBV◊GíÜW&RG&˜0¢ÚÚFá&˜VvÇFÚFÜR∆7B◊&W6˜'BV‚÷ÜñFR&V∆˜r¬ÊBFÜRFVfW'&VB&˜w2FÜV‡¢ÚÚ÷FW&ñ∆ó¶RFÜR6÷R&˜6R&W6ñFRFÜR6Vv÷VÁG2óBßW7BV‚÷ÜñB‡¢ÚÚ÷FW&ñ∆ó¶Rfó'7B¬FÜV‚ßVFvR‡¢ñbÜw&˜WÊvWDGG&ñ'WFRbfw&˜WÊvWDGG&ñ'WFRÇvFF◊v˜&∂∆ˆr◊&˜w2÷FVfW'&VBrì””“sp¢bgGóVˆbˆ÷FW&ñ∆ó¶TFVfW'&VEv˜&∂∆ˆu&˜w3””“vgVÊ7Fñˆ‚ró∞¢ˆ÷FW&ñ∆ó¶TFVfW'&VEv˜&∂∆ˆu&˜w2Üw&˜Wì∞¢–¢ñbÇÜw&˜WÁFWáD6ˆÁFVÁG«¬rríÁG&ñ“Çíí6ˆÁFñÁVS≤ÚÚV◊Gíw&˜W6‚wBÜV« ¢ñbÜw&˜WÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwFˆˆ¬÷6∆¬÷w&˜W÷6ˆ∆∆6VBríó∞¢w&˜WÊ6∆74∆ó7BÁ&V÷˜fRÇwFˆˆ¬÷6∆¬÷w&˜W÷6ˆ∆∆6VBrì∞¢w&˜WÊ6∆74∆ó7BÊFBÇv˜V‚rì∞¢6ˆÁ7B7V÷÷'ì÷w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6∆¬÷w&˜W◊7V÷÷'í¬Ê7FófóGí◊7V÷÷'írì∞¢ñbá7V÷÷'íí7V÷÷'íÁ6WDGG&ñ'WFRÇv&ñ÷WáÊFVBr¬wG'VRrì∞¢ÚÚ3SÉ3ì¢FÜó2GW&‚ó2˜FÜW'vó6R&∆Ê≤¬6Ú÷FW&ñ∆ó¶RÁíFVfW'&V@¢ÚÚ6WGF∆VB&˜w2Ê˜rFÜBvRw&Rf˜&6R÷WáÊFñÊrFÜRv˜&∂∆ˆrFÚfñ∆¬óB‡¢ñbáGóVˆbˆ÷FW&ñ∆ó¶TFVfW'&VEv˜&∂∆ˆu&˜w3””“vgVÊ7Fñˆ‚ríˆ÷FW&ñ∆ó¶TFVfW'&VEv˜&∂∆ˆu&˜w2Üw&˜Wì∞¢–¢ÚÚ&WfV∆VF÷VÁ2'FÜó2GW&‚Ü2Êˆ‚÷V◊Gív˜&∂∆ˆrw&˜WFÜBFÜRW6W ¢ÚÚ6‚6VR"(	B‰ıB'vRßW7BWáÊFVB6ˆ÷WFÜñÊr"‚‚«&VGí÷˜V‚Êˆ‚÷V◊Gê¢ÚÚw&˜Wó2óG6V∆bfó6ñ&∆RÜóB6∆ó27B˜GW&‰Ü5fó6ñ&∆T6ˆÁFVÁBˆÊ«ê¢ÚÚ&V6W6RFÜB6ÜV6≤ñÁ7V7G2Ê76ó7FÁB◊6Vv÷VÁBÊˆFW2¬Ê˜Bw&˜W&ˆFñW2í¿¢ÚÚ6ÚFÜRGW&‚ó6‚wBG'V«í&∆Ê≤ÊBFÜR∆7B◊&W6˜'BV‚÷ÜñFR&V∆˜ró0¢ÚÚVÊÊV6W76'í‚∂VWFÜó276ñvÊ÷VÁBıUE4îDRFÜRñbÜ6ˆ∆∆6VBí'&Ê6Ç‡¢&WfV∆VC◊G'VS∞¢–¢ÚÚ∆7B&W6˜'C¢ÊÚW6&∆Rv˜&∂∆ˆrw&˜WVóFÜW"¬'WBÜñFFV‚v˜&∂∆ˆr◊6˜W&6P¢ÚÚ6Vv÷VÁG26''íFÜR&V¬FWáB(	BV‚÷ÜñFRFÜV“6ÚÊ˜FÜñÊró2∆˜7B‡¢ñbÇ&WfV∆VBó∞¢f˜"Ü6ˆÁ7B6VrˆbGW&‚ÁVW'ï6V∆V7F˜$∆¬ÇrÊ76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6Rríó∞¢ñbÇá6VrÁFWáD6ˆÁFVÁG«¬rríÁG&ñ“Çíí6ˆÁFñÁVS∞¢6VrÊ6∆74∆ó7BÁ&V÷˜fRÇv76ó7FÁB◊6Vv÷VÁB◊v˜&∂∆ˆr◊6˜W&6Rrì∞¢6VrÁ&V÷˜fTGG&ñ'WFRÇv&ñ÷ÜñFFV‚rì∞¢6VrÊÜñFFV„÷f«6S∞¢–¢–¢–¢–¢ÚÚ&R÷GF6ÇFÜR&W6W'fVB∆ófRGW&‚Ç33Ésrí‚FÜR&V'Vñ∆B&˜fR&V7&VFVB¢ÚÚ∆ófRGW&‚g&ˆ“2Ê÷W76vW2¬'WBFÜR∆ófR76ó7FÁB÷W76vRw26ˆÁFVÁB∆w2FÜP¢ÚÚ7G&V“ÜóBó2ˆÊ«íW'6ó7FVBFÚ2Ê÷W76vW2ˆ‚Fá&˜GF∆VBw&óFR÷&6≤í(	B6ÚFÜP¢ÚÚg&W6ÇÊˆFRˆgFV‚6Ü˜w2ƒU527G&V÷VBFWáBFÜ‚FÜRı$îtî‰¬ÊˆFR¬vÜñ6Çó0¢ÚÚ7Fñ∆¬&VfW&VÊ6VB'íFÜR6÷B'6W"ÊBÜˆ∆G2FÜR&V¬ñ‚◊&ˆw&W72&W«í‚7v ¢ÚÚFÜR&W6W'fVBá'6W"íÊˆFR&6≤ñ‚6ÚFÜR'6W"F&vWB7Fó26ˆÊÊV7FVBÊ@¢ÚÚFÜRfó6ñ&∆RFWáBÊWfW"&∆Ê∑2‡¢Ú¢ÚÚFÜR7vfó&W2vÜV‚FÜR&W6W'fVBÊˆFR6'&ñW2B∆V7B2◊V6Ç7G&V÷VBFWá@¢ÚÚ2FÜR&V'Vñ«BˆÊRÜ˜&V'Vñ«D∆V‚√“˜&W6W'fVD∆VÊí‚FÜR√÷ÜÊ˜B∆íó0¢ÚÚ∆ˆB÷&V&ñÊs¢BFÜRFá&˜GF∆VB◊W'6ó7B&˜VÊF'íFÜR&V'Vñ«BGW&‚w2∆ófP¢ÚÚ6ˆÁFVÁB6‚UT¬FÜR&W6W'fVB∆VÊwFÇ¬ÊBFÜRˆ∆B∆wV&BFÜV‚6∂óVBFÜP¢ÚÚ7v(	B∆VfñÊrFÜR6÷B'6W"w&óFñÊrñÁFÚFÜRFWF6ÜVB˜&ñvñÊ¬ÊˆFR¬vÜñ6Ä¢ÚÚó2WÜ7F«íFÜR&W6ñGV¬&Fó6V'2¬FÜV‚&VV'2"g&÷RÇ33Ésr&V˜V‚í‚ˆ‡¢ÚÚFñRFÜR&W6W'fVBÊˆFRó27G&ñ7F«í&VfW&&∆RÜóBÜˆ∆G2FÜR∆ófR'6W ¢ÚÚ&VfW&VÊ6S≤ñFVÁFñ6¬∆VÊwFÇ÷VÁ2Ê˜FÜñÊró2∆˜7Bí‚vÜV‚FÜR&V'Vñ«BGW&‡¢ÚÚvVÁVñÊV«íÜ2‘ı$R6ˆÁFVÁBÜRÊr‚&V6ˆÊÊV7BvÜW&R2Ê÷W76vW26VváBW7@¢ÚÚFÜR'6W"í¬FÜRwV&B6˜'&V7F«í6∂ó2ÊB∆WG2FÜR'6W"&R◊&W6ˆ«fRFÚFÜP¢ÚÚgV∆∆W"ÊˆFR‡¢Ú¢ÚÚ7vBFÜR4Tt‘TÂB∆WfV¬(	B&W∆6RˆÊ«íFÜR&V'Vñ«B∆ófR6Vv÷VÁBvóFÇFÜP¢ÚÚ&W6W'fVBˆÊR(	B6Ú◊V«Fí◊6Vv÷VÁBGW&‚ÜV&∆ñW"6WGF∆VB6Vv÷VÁG2≤Fˆˆ¬¢ÚÚv˜&∂∆ˆrw&˜W2'Vñ«B'íFÜR&V'Vñ∆Bí∂VW2FÜB&V'Vñ«B÷ˆÊ«í7G'V7GW&S≤¢ÚÚvÜˆ∆R◊GW&‚&W∆6UvóFÇv˜V∆BFó66&BóBvÜV‚FÜR&W6W'fVB6Ê6Ü˜B&VFFW0¢ÚÚFÜ˜6R6Vv÷VÁG2‚f∆¬&6≤FÚvÜˆ∆R◊GW&‚&W∆6RˆÊ«ívÜV‚FÜR&V'Vñ«BGW&‚Ü0¢ÚÚÊÚ∆ófR6Vv÷VÁBFÚ7vñÁFÚ‚ÊÚ÷˜f˜"6WGF∆VBGW&‚˜"vÜV‚Ê˜FÜñÊrv0¢ÚÚ7G&V÷ñÊr‡¢ñbÖ˜&W6W'fVD∆ófUGW&‚ó∞¢6ˆÁ7B˜&V'Vñ«C÷Fˆ7V÷VÁBÊvWDV∆V÷VÁD'îñBÇv∆ófT76ó7FÁEGW&‚rì∞¢ÚÚñ6≤FÜR%4U"‘ıt‰TB∆ófR6Vv÷VÁB¬Ê˜BßW7BFÜRfó'7BˆÊR‚ˆ‚&V6ˆÊÊV7B¢ÚÚ˜7B◊Fˆˆ¬7FófóGí&˜VÊF&ñW2∆ófRGW&‚6‚6''í’T≈DïƒP¢ÚÚ∂FF÷∆ófR÷76ó7FÁC“#%“6Vv÷VÁG2¬ÊBFÜR6÷B'6W"w&óFW2ñÁFÚFÜP¢ÚÚƒ5BáFñ¬íˆÊRá6VRVÁ7W&T76ó7FÁE&˜rñ‚÷W76vW2Êß2(	BóB&R÷GF6ÜW2F¢ÚÚFÜR∆7B∆ófR6Vv÷VÁBí‚&VfW"FÜR&W6W'fVB6Vv÷VÁBvÜ˜6P¢ÚÚFF÷∆ófR◊6Vv÷VÁB◊6W÷F6ÜW2FÜR&V'Vñ«BFñ¬á6÷R∆ˆvñ6¬6Vv÷VÁBí¬FÜV‡¢ÚÚf∆¬&6≤FÚFÜR∆7B&W6W'fVB∆ófR6Vv÷VÁB‚W6ñÊrVW'ï6V∆V7F˜"ÇíÜfó'7Bê¢ÚÚÜW&Rv˜V∆B÷˜fRFÜRw&ˆÊr6Vv÷VÁBÊB∆VfRFÜR'6W"÷˜vÊVBFñ¬FWF6ÜV@¢ÚÚñ‚◊V«Fí◊6Vv÷VÁBGW&‚‡¢6ˆÁ7B˜&V'Vñ«E6Vw3’˜&V'Vñ«Cı˜&V'Vñ«BÁVW'ï6V∆V7F˜$∆¬Çu∂FF÷∆ófR÷76ó7FÁC“#%“rì¶ÁV∆√∞¢6ˆÁ7B˜&V'Vñ«E6Vs“Ö˜&V'Vñ«E6Vw2be˜&V'Vñ«E6Vw2Ê∆VÊwFÇìı˜&V'Vñ«E6Vw5µ˜&V'Vñ«E6Vw2Ê∆VÊwFÇ””¶ÁV∆√∞¢6ˆÁ7B˜&W6W'fVE6Vw3’˜&W6W'fVD∆ófUGW&‚ÁVW'ï6V∆V7F˜$∆¬Çu∂FF÷∆ófR÷76ó7FÁC“#%“rì∞¢∆WB˜&W6W'fVE6Vs’˜&W6W'fVE6Vw2Ê∆VÊwFÉı˜&W6W'fVE6Vw5µ˜&W6W'fVE6Vw2Ê∆VÊwFÇ””¶ÁV∆√∞¢6ˆÁ7B˜&V'Vñ«E6W’˜&V'Vñ«E6Vsı˜&V'Vñ«E6VrÊvWDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wrì¶ÁV∆√∞¢ñbÖ˜&V'Vñ«E6Wó∞¢f˜"Ü6ˆÁ7B˜6Vrˆb˜&W6W'fVE6Vw2ó∞¢ñbÖ˜6VrÊvWDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wrì””’˜&V'Vñ«E6Wóµ˜&W6W'fVE6Vs’˜6Vs∂'&V≥∑–¢–¢–¢6ˆÁ7B˜&W6W'fVD∆V„’ˆ∆ófT76ó7FÁE6Vv÷VÁEFWáD∆VÊwFÇÖ˜&W6W'fVE6Vw«≈˜&W6W'fVD∆ófUGW&‚ì∞¢ÚÚ7G'V7GW&¬÷&∆ˆ6≤6˜VÁG3¢∆ófRGW&‚6‚&RÑTBˆb2Ê÷W76vW2vóFÄ¢ÚÚ7FófóGí˜Fˆˆ¬˜v˜&∂∆ˆr&∆ˆ6∑2FÜBÜfV‚wBW'6ó7FVBñWB(	BWfV‚vóFÇ§U$¢ÚÚ7G&V÷VBFWáBÜRÊr‚‚7FófóGí÷ˆÊ«íGW&‚÷ñB◊Fˆˆ¬÷6∆¬í‚FÜRFWáB÷∆VÊwFÄ¢ÚÚvFR∆ˆÊRv˜V∆B6∂ó&W6W'fFñˆ‚ñ‚FÜB66R¬6Ú67&ˆ∆¬◊G&ñvvW&V@¢ÚÚ&V'Vñ∆Bˆ‚∆ˆÊráfó'GV∆ó¶VBíG&Á67&óB6˜V∆B&∆ñÊ≤FÜ˜6R∆ófR÷ˆÊ«ê¢ÚÚ&∆ˆ6∑2f˜"g&÷R‚«6Ú&W7F˜&RvÜV‚FÜR&W6W'fVBGW&‚6'&ñW2÷˜&P¢ÚÚ7G'V7GW&RFÜ‚FÜR&V'Vñ«BÜ∆vvñÊr’2Ê÷W76vW2íGW&‚‚Ç33sB6Üó◊&WfñWrê¢6ˆÁ7B˜7G'V7GW&ƒ6˜VÁC“áGW&‚ì”‚GW&„˜GW&‚ÁVW'ï6V∆V7F˜$∆¬Ä¢u∂FF÷∆ófR÷76ó7FÁC“#%“¬ÁFˆˆ¬÷6∆¬÷w&˜W¬ÁFˆˆ¬÷6&B◊&˜r¬r∞¢rÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W¬Ê∆ófR◊v˜&∂∆ˆu∂FF÷∆ófR◊v˜&∂∆ˆr◊6ÜV∆√“#%“¬r∞¢rÁv¬◊&V6ˆ‚¬ÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊr¬ÁFÜñÊ∂ñÊr÷6&B◊&˜rp¢íÊ∆VÊwFÉ£∞¢6ˆÁ7B˜&W6W'fVE7G'V7GW&S’˜7G'V7GW&ƒ6˜VÁBÖ˜&W6W'fVD∆ófUGW&‚ì∞¢6ˆÁ7B˜&V'Vñ«E7G'V7GW&S’˜7G'V7GW&ƒ6˜VÁBÖ˜&V'Vñ«Bì∞¢ñbÖ˜&W6W'fVD∆V„„«¬˜&W6W'fVE7G'V7GW&SÂ˜&V'Vñ«E7G'V7GW&Ró∞¢6ˆÁ7B˜&V'Vñ«D∆V„’˜&V'Vñ«Cıˆ∆ófT76ó7FÁE6Vv÷VÁEFWáD∆VÊwFÇÖ˜&V'Vñ«E6Vw«≈˜&V'Vñ«Bì¢”∞¢ñbÖ˜&V'Vñ«D∆V„√’˜&W6W'fVD∆V‚ó∞¢ÚÚFV6ñFR6Vv÷VÁB÷∆WfV¬g2vÜˆ∆R◊GW&‚&W7F˜&R‚6Vv÷VÁB÷∆WfV¬∂VW2FÜP¢ÚÚ&V'Vñ«BGW&‚w27G'V7GW&RÜvˆˆBvÜV‚FÜR&V'Vñ∆Bó2FÜR7G'V7GW&¿¢ÚÚ7WW'6WBí‚'WBFÜRvÜˆ∆R&V÷ó6RÜW&Ró2FÜBFÜR∆ófRDÙ“6‚&P¢ÚÚÑTBˆb2Ê÷W76vW3¢Fˆˆ¬˜v˜&∂∆ˆrw&˜W6‚∆ÊBñ‚FÜR∆ófRGW&‡¢ÚÚ&WGvVV‚FÜR∆7BFá&˜GF∆VBW'6ó7BÊBFÜó2&V'Vñ∆B¬6ÚFÜR&V'Vñ«@¢ÚÚGW&‚Ü'Vñ«Bg&ˆ“FÜR∆vvñÊr2Ê÷W76vW2í÷íÜfRdUtU"7G'V7GW&¿¢ÚÚ&∆ˆ6∑2‚ñ‚FÜB66R6Vv÷VÁB÷ˆÊ«í7vv˜V∆BG&˜FÜ˜6R∆ófR÷ˆÊ«ê¢ÚÚ&∆ˆ6∑2f˜"g&÷R(	B6Ú&W7F˜&RFÜRtÑÙƒR&W6W'fVBGW&‚ñÁ7FVB‡¢ÚÚ˜FÜW'vó6Rá&V'Vñ∆BÜ2„“FÜR&W6W'fVBGW&‚w27G'V7GW&¬&∆ˆ6∑2íF¢ÚÚFÜR&V6ó6R6Vv÷VÁB7v6Ú&V'Vñ«B÷ˆÊ«í7G'V7GW&Ró2∂WB‡¢ñbÖ˜&V'Vñ«Bbe˜&V'Vñ«E6Vrbe˜&W6W'fVE6Vrbe˜&V'Vñ«E7G'V7GW&S„’˜&W6W'fVE7G'V7GW&Ró∞¢ÚÚ&V'Vñ∆Bó2FÜR7G'V7GW&¬7WW'6WB(	B7vˆÊ«íFÜR'6W"÷˜vÊV@¢ÚÚáFñ¬í∆ófR6Vv÷VÁB¬∂VWñÊr&V'Vñ«B÷ˆÊ«í6Vv÷VÁG2ÚFˆˆ¬w&˜W2‡¢ÚÚÑÊÚFF6WBÁ6W76ñˆ‰ñB7F◊ÜW&S¢ˆÊ«íFÜR6Vv÷VÁBVÁFW'2FÜRDÙ”∞¢ÚÚFÜR&V'Vñ«BGW&‚v2«&VGí7F◊VBB'Vñ∆BFñ÷R¬6VR&˜fR‚ê¢˜&V'Vñ«E6VrÁ&W∆6UvóFÇÖ˜&W6W'fVE6Vrì∞¢÷V«6RñbÖ˜&V'Vñ«Bó∞¢ÚÚ&V'Vñ«BGW&‚∆6∑27G'V7GW&RFÜR∆ófRGW&‚«&VGíÜ2Ü∆ófR÷ˆÊ«ê¢ÚÚFˆˆ¬6&BÊ˜BñWBW'6ó7FVBí¬˜"Ü2ÊÚ∆ófR6Vv÷VÁBFÚF&vWB(	@¢ÚÚ&W7F˜&RFÜRvÜˆ∆R&W6W'fVBGW&‚6ÚÊ˜FÜñÊrFÜRW6W"6rfÊó6ÜW2‡¢ñbÖ2Á6W76ñˆ‚í˜&W6W'fVD∆ófUGW&‚ÊFF6WBÁ6W76ñˆ‰ñC’2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢˜&V'Vñ«BÁ&W∆6UvóFÇÖ˜&W6W'fVD∆ófUGW&‚ì∞¢÷V«6RñbÇáGóVˆb˜6WGF∆VEG&Á67&óD˜vÁ4∆ófUGW&„””“vgVÊ7Fñˆ‚p¢be˜6WGF∆VEG&Á67&óD˜vÁ4∆ófUGW&‚á6ñB≈˜&W6W'fVD∆ófUGW&‚ííó∞¢ÚÚ3cìCÇfˆ∆∆˜r◊WÜGW∆ñ6FR76ó7FÁBÁ7vW#≤3#Sì¢FÜó2ó2FÜP¢ÚÚˆÊ«í'&Ê6ÇFÜBDE2GW&‚(	BFÜR&V'Vñ∆B&ˆGV6VBÊÚ∆ófRGW&‚ˆ`¢ÚÚóG2˜v‚‚vÜV‚FÜR6WGF∆VBG&Á67&óB«&VGíVÊG2vóFÇDÑï27G&V“w0¢ÚÚ˜v‚Á7vW"ÊBFÜR&W6W'fVBÊˆFR6'&ñW2Ê˜FÜñÊrVÁW'6ó7FVB¬FÜP¢ÚÚÊˆFRó2FVB∆VgF˜fW"áFÜR&˜rW'6ó7FVBÊBFÜRGW&‚6WGF∆VBvÜñ∆P¢ÚÚî‰dƒîtÖE∑6ñE“v2Ê˜BñWB6∆VÊVBíÊBVÊFñÊrñÁ24T4Ù‰B6˜ê¢ÚÚFÜB&R◊&W6W'fW2óG6V∆bˆ‚WfW'í∆FW"&VÊFW"VÁFñ¬&V∆ˆB‡¢ÚÚ÷ñB◊7G&V“FÜRG&Á67&óBVÊG2vóFÇFÜRW6W"GW&‚Ü˜"7Fñ∆¬6'&ñW2¢ÚÚ∆ófR&ˆ¶V7Fñˆ‚í¬6Ú33Ésr&W6W'fFñˆ‚ó2VÁF˜V6ÜVB‡¢ñbÖ2Á6W76ñˆ‚í˜&W6W'fVD∆ófUGW&‚ÊFF6WBÁ6W76ñˆ‰ñC’2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢ñÊÊW"ÊVÊD6Üñ∆BÖ˜&W6W'fVD∆ófUGW&‚ì∞¢–¢–¢–¢–¢ÚÚˆÊ«íf˜&6R◊67&ˆ∆¬vÜV‚Ê˜B7FófV«í7G&V÷ñÊr(	B÷ñB◊7G&V“&R◊&VÊFW'0¢ÚÚáFˆˆ¬6ˆ◊∆WFñˆ‚¬6W76ñˆ‚7vóF6Çí◊W7BÊ˜B˜fW'&ñFRFÜRW6W"w267&ˆ∆¬˜6óFñˆ‚‡¢ÚÚ67&ˆ∆ƒñeñÊÊVBÇí&W7V7G2˜67&ˆ∆≈ñÊÊVB¬6ÚóBw2ÊÚ÷˜ñbW6W"67&ˆ∆∆VBW‡¢ñbáGóVˆb˜7ñÊ4∆ófU'VÂ7FGW4gFW%&VÊFW#””“vgVÊ7Fñˆ‚rí˜7ñÊ4∆ófU'VÂ7FGW4gFW%&VÊFW"Çì∞¢˜67&ˆ∆ƒgFW$÷W76vU&VÊFW"á&W6W'fU67&ˆ∆¬¬67&ˆ∆≈6Ê6Ü˜Bì∞¢ñbÖˆ÷ñ&U&V6˜fW%fó'GV∆ó¶VD&∆ÊµfñWw˜'BÜ˜FñˆÁ2¬&W6W'fU67&ˆ∆¬¬fó'GV≈vñÊF˜ríí&WGW&„∞¢ÚÚ«í7ñÁFÇÜñvÜ∆ñváFñÊrgFW"DÙ“ó2'Vñ«@¢&WVW7DÊñ÷Fñˆ‰g&÷RÇÇì”Â˜˜7E&ˆ6W75vóFÑÊ6Ü˜%7W&W76ñˆ‚ÜñÊÊW"íì∞¢ÚÚ&Vg&W6ÇFˆFÚÊV¬ñbóBw27W'&VÁF«í˜V‡¢ñbáGóVˆb∆ˆEFˆF˜3””“vgVÊ7Fñˆ‚rbbFˆ7V÷VÁBÊvWDV∆V÷VÁD'îñBÇwÊV≈FˆF˜2ríbbFˆ7V÷VÁBÊvWDV∆V÷VÁD'îñBÇwÊV≈FˆF˜2ríÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv7FófRríó∞¢∆ˆEFˆF˜2Çì∞¢–¢ÚÚ«íW'6ó7FVB∆ñ&6≤7VVBgFW"÷VFñÊˆFW2&R&VÊFW&VB‡¢ñbáGóVˆbˆ«î÷VFñ∆ñ&6µ&VfW&VÊ6W3””“vgVÊ7Fñˆ‚ríˆ«î÷VFñ∆ñ&6µ&VfW&VÊ6W2ÜñÊÊW"ì∞¢ÚÚ˜V∆FR6W76ñˆ‚66ÜR6Ú7vóF6ÜñÊr&6≤ÜW&R6∂ó2gV∆¬&V'Vñ∆B‡¢˜6W76ñˆ‰áF÷ƒ66ÜU6ñC◊6ñC∞¢ÚÚ6∂ó66ÜñÊrvÜñ∆RFÜRßW7B◊6WGF∆VB∂VW÷˜V‚Fˆ∂V‚ó2&÷VC¢FÜB&VÊFW ¢ÚÚf˜&6R÷˜VÁ2FÜR6WGF∆VBv˜&∂∆ˆrf˜"ÜVñváB◊7F&ñ∆óGí¬ÊB66ÜñÊróBv˜V∆@¢ÚÚW'6ó7BFÜRf˜&6VB÷˜V‚DÙ“7&˜726W76ñˆ‚7vóF6ÜW2Ú&W7F˜&W2¬˜fW'&ñFñÊr¢ÚÚW6W"÷6ˆ∆∆6VBv˜&∂∆ˆr‚FÜRfˆ∆∆˜r◊W6ˆ∆∆6R72ÜgFW"Fó6&“í&ˆGV6W0¢ÚÚFÜR6˜'&V7B66ÜV&∆RDÙ“ˆ‚óG2˜v‚&VÊFW"‚Ç3S#cvFR÷6W'B‚íFÜRGóVˆ`¢ÚÚwV&B∂VW27FÊF∆ˆÊR&VÊFW$÷W76vW2ÇíFW7BÜ&ÊW76W2ávÜñ6ÇFˆ‚wBFVfñÊP¢ÚÚFÜRÜV«W"ív˜&∂ñÊr(	B'6VÁBÜV«W"”“Ê˜B&÷VB”“66ÜRÊ˜&÷∆«í‡¢6ˆÁ7Bˆ∂VW˜V‰&÷VC“áGóVˆbˆó4∂VW6WGF∆VEv˜&∂∆ˆt˜V‰&÷VC””“vgVÊ7Fñˆ‚ríbeˆó4∂VW6WGF∆VEv˜&∂∆ˆt˜V‰&÷VBÇì∞¢ñbá6ñBbbî‰dƒîtÖE∑6ñE“bbÜ5G&Á6ñVÁEG&Á67&óEVíbbˆ∂VW˜V‰&÷VBó∞¢6ˆÁ7BˆáF÷√÷ñÊÊW"ÊñÊÊW$ÖD‘√∞¢ÚÚˆÊ«í66ÜR6W76ñˆÁ2vóFÇ√3¥"&VÊFW&VBÖD‘√≤Wfñ7Bˆ∆FW7B&WñˆÊBÇ6W76ñˆÁ2‡¢ñbÖˆáF÷¬Ê∆VÊwFÉ√3Ûó∞¢6ˆÁ7B&VÊFW%6ñvÊGW&S÷66ÜVE&VÊFW%6ñvÊGW&S””÷ÁV∆√ıˆ÷W76vU&VÊFW$66ÜU6ñvÊGW&RÇì¶66ÜVE&VÊFW%6ñvÊGW&S∞¢˜6W76ñˆ‰áF÷ƒ66ÜRÁ6WBá6ñB«∂áF÷√•ˆáF÷¬∆◊6t6˜VÁB«&VÊFW%vñÊF˜t∂Wí«6ñvÊGW&Sß&VÊFW%6ñvÊGW&W“ì∞¢ñbÖ˜6W76ñˆ‰áF÷ƒ66ÜRÁ6ó¶S„Çóµ˜6W76ñˆ‰áF÷ƒ66ÜRÊFV∆WFRÖ˜6W76ñˆ‰áF÷ƒ66ÜRÊ∂Wó2ÇíÊÊWáBÇíÁf«VRì∑–¢–¢–¢˜WFFT÷W76vUfó'GVƒ÷V7W&V÷VÁG2á&VÊFW%fó5vóFÑñGÇ¬&VÊFW%fó6ñ&∆TñGá2¬fó'GV≈vñÊF˜rì∞¢ÚÚ∂ñ∆¬FÜRñÊÊVB˜Fñ¬÷fˆ∆∆˜vW"÷ñB◊7G&V“¶óGFW"‚66ÜVGV∆RFÜR&R÷Ê6Ü˜"ñ‚‘î5$ıD4≤¿¢ÚÚÊ˜B7ñÊ6á&ˆÊ˜W6«ì¢ñÁ6ñFRFÜó2&VÊFW"7ñÊ27F6≤FÜR'&˜w6W"7Fñ∆¬&W˜'G2G&Á6ñVÁ@¢ÚÚ67&ˆ∆ƒÜVñváBÜ∆ñ˜WBó2&F6ÜVBí¬6Ú7ñÊ6á&ˆÊ˜W2&R÷Ê6Ü˜"v˜V∆B&VBFÜR4‘R6Ü˜'@¢ÚÚf«VR67&ˆ∆≈FÙ&˜GFˆ“«&VGí6∆◊VBvñÁ7BÊB&RÊÚ÷˜‚FÜR÷ñ7&˜F6≤'VÁ2gFW"FÜP¢ÚÚ7F6≤VÁvñÊG2á67&ˆ∆ƒÜVñváBÜ2f«W6ÜVBFÚFÜR6WGF∆VBf«VRí'WB&Vf˜&RFÜR'&˜w6W ¢ÚÚñÁG2FÜó2g&÷R¬6Úw&óFñÊrFÜR6WGF∆VB÷Ç∆ÊG2FÜRFñ¬WÜ7F«íÊBFÜR„◊&˜rÜñvÄ¢ÚÚñÁFW&÷VFñFRÊWfW"&V6ÜW2FÜR67&VV‚‚ˆÊ«í&R÷Ê6Ü˜'2&R◊vóRFñ¬÷fˆ∆∆˜vW"∆VgB6Ü˜'@¢ÚÚˆbFÜR6WGF∆VB÷Ç(	B‚VÁñÊÊVB&VFW"&∂VBñ‚Üó7F˜'íó2ÊWfW"÷˜fVBÜ˜'FÜˆvˆÊ¬F¢ÚÚFÜRVÁñÊÊVBßV◊÷&6≤6∆72í‚6VR˜&VÊ6Ü˜%ñÊÊVEFñƒgFW%&VÊFW"f˜"FÜRgV∆¬&FñˆÊ∆R‡¢ÚÚáGóVˆbwV&G2÷ó'&˜"FÜRˆFVfW$6∆V%&ˆw&÷÷Fñ567&ˆ∆¬6∆¬&V∆˜r6Ú7FÊF∆ˆÊP¢ÚÚ&VÊFW$÷W76vW2ÇíFW7BÜ&ÊW76W2FÜBFˆ‚wBFVfñÊRFÜW6RÜV«W'27Fñ∆¬'V‚‚ê¢ñbáGóVˆbVWVT÷ñ7&˜F6≥””“vgVÊ7Fñˆ‚rbbGóVˆb˜&VÊ6Ü˜%ñÊÊVEFñƒgFW%&VÊFW#””“vgVÊ7Fñˆ‚ró∞¢VWVT÷ñ7&˜F6≤ÇÇì”Â˜&VÊ6Ü˜%ñÊÊVEFñƒgFW%&VÊFW"Ö˜&UvóTÊV%Fñ¬íì∞¢–¢˜&V7ñ6∆U7F6ÇÊ6∆V"Çì∞¢ñbáGóVˆbˆFVfW$6∆V%&ˆw&÷÷Fñ567&ˆ∆√””“vgVÊ7Fñˆ‚ríˆFVfW$6∆V%&ˆw&÷÷Fñ567&ˆ∆¬Écì∞ß–†¶gVÊ7Fñˆ‚˜FˆˆƒFó7∆îÊ÷RáF2ó∞¢6ˆÁ7BÊ÷S“áF2bgF2ÊÊ÷Ró«¬wFˆˆ¬s∞¢ñbÜÊ÷S””“w7V&vVÁE˜&ˆw&W72rí&WGW&‚u7V&vVÁBs∞¢ñbÜÊ÷S””“vFV∆VvFU˜F6≤rí&WGW&‚tFV∆VvFRF6≤s∞¢ñbÜÊ÷S””“w6∂ñ∆≈˜fñWrrí&WGW&‚u6∂ñ∆¬s∞¢ñbÜÊ÷S””“w6∂ñ∆≈ˆ÷ÊvRrí&WGW&‚u6∂ñ∆¬s∞¢&WGW&‚Ê÷S∞ß–†¢ÚÚ7FófóGí◊7V÷÷'íFWFV7Fñˆ‚f˜"W'6ó7FVB÷V÷˜'í˜6∂ñ∆¬w&óFW2Ç333C¬33SCBí‡¢ÚÚ7Fñˆ‚fˆ6'V∆&ñW2÷F6ÇFÜR&V¬vVÁBFˆˆ¬VÁV◊3†¢ÚÚ÷V÷˜'íÊ7Fñˆ‚“FB¬&W∆6R¬&V÷˜fRÜFB˜&W∆6RW'6ó7B6ˆÁFVÁB(i"'6fVB"ê¢ÚÚ6∂ñ∆≈ˆ÷ÊvRÊ7Fñˆ„“7&VFR¬F6Ç¬VFóB¬FV∆WFR¬w&óFUˆfñ∆R¬&V÷˜fUˆfñ∆P¢ÚÚÜ7&VFR˜F6ÇˆVFóB˜w&óFUˆfñ∆R◊WFFR6∂ñ∆¬(i"'WFFVB"ê¢ÚÚFV∆WFñˆÁ2Ü÷V÷˜'íw&V÷˜fRr¬6∂ñ∆¬vFV∆WFRrÚw&V÷˜fUˆfñ∆Rrí&RñÁFVÁFñˆÊ∆«ê¢ÚÚWÜ6«VFVB6ÚFÜR'6fVB"Ú'WFFVB"∆&V¬fW&'27Fí67W&FS≤'VÊÊñÊrˆW'&˜&V@¢ÚÚ6∆«2&RWÜ6«VFVB6ÚˆÊ«í6ˆ◊∆WFVBw&óFW2&R6˜VÁFVB‡¶6ˆÁ7BÙ‘T‘ı%ïı4dUÙ5DîÙÂ3÷ÊWr6WBÖ≤vFBr¬w&W∆6Ru“ì∞¶6ˆÁ7Bı4¥îƒ≈ıUDDUÙ5DîÙÂ3÷ÊWr6WBÖ≤v7&VFRr¬wF6Çr¬vVFóBr¬ww&óFUˆfñ∆Ru“ì∞¶gVÊ7Fñˆ‚˜F47Fñˆ‚áF2ó∞¢&WGW&‚7G&ñÊrÇáF2bgF2Ê&w2bgF2Ê&w2Ê7Fñˆ‚ó«¬rríÁFÙ∆˜vW$66RÇì∞ß–¶gVÊ7Fñˆ‚ˆó4÷V÷˜'ï6fRáF2ó∞¢ñbÇF7««F2ÊÊ÷R”“v÷V÷˜'íw««F2ÊFˆÊS””÷f«6W««F2Êó5ˆW'&˜"í&WGW&‚f«6S∞¢&WGW&‚Ù‘T‘ı%ïı4dUÙ5DîÙÂ2ÊÜ2Ö˜F47Fñˆ‚áF2íì∞ß–¶gVÊ7Fñˆ‚ˆó56∂ñ∆≈WFFRáF2ó∞¢ñbÇF7««F2ÊÊ÷R”“w6∂ñ∆≈ˆ÷ÊvRw««F2ÊFˆÊS””÷f«6W««F2Êó5ˆW'&˜"í&WGW&‚f«6S∞¢&WGW&‚ı4¥îƒ≈ıUDDUÙ5DîÙÂ2ÊÜ2Ö˜F47Fñˆ‚áF2íì∞ß–¢ÚÚ)H)HFˆˆ¬7Fñˆ‚∆&V¬ÜV«W'2)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H ¶gVÊ7Fñˆ‚ˆFV6ˆFUFˆˆƒ∆&VƒVÁFóFñW2áf«VRó∞¢&WGW&‚7G&ñÊráf«VW«¬rrê¢Á&W∆6RÇÚgV˜C≤ˆr¬r"rê¢Á&W∆6RÇÚb33ì∑¬f˜3≤ˆr¬"r"ê¢Á&W∆6RÇÚf«C≤ˆr¬s¬rê¢Á&W∆6RÇÚfwC≤ˆr¬s‚rê¢Á&W∆6RÇÚf◊≤ˆr¬rbrì∞ß–¶gVÊ7Fñˆ‚˜&VF7EFˆˆ≈F&vWD∆&V¬áf«VRó∞¢&WGW&‚7G&ñÊráf«VW«¬rrê¢Á&W∆6RÇı∆'76á75«2≤◊«2≤ÉÛ¢%µ‚%“¢'¬uµ‚u“¢w≈≈2≤íˆví¬w76á72◊%∑&VF7FVE“"rê¢Á&W∆6RÇÚÇ“◊77v˜&BÉÛ£◊≈«2≤ííÉÛ¢%µ‚%“¢'¬uµ‚u“¢w≈≈2≤íˆví¬rC∑&VF7FVE“rê¢Á&W∆6RÇÚá77v˜&BÉÛ£◊≈«2≤ííÉÛ¢%µ‚%“¢'¬uµ‚u“¢w≈≈2≤íˆví¬rC∑&VF7FVE“rê¢ÚÚVÁb÷76ñvÊ÷VÁBÚf∆r6V7&WG2¬÷6∂VB7&˜72FÜRgV∆¬Ü◊V«Fí÷∆ñÊRíFWáB6¢ÚÚFÜRWáÊFVB6ÜV∆¬6&B6‚wB∆V≤∂Wíˆ‚Êˆ‚÷fó'7B∆ñÊRÇ3Cì#bí‚∂Wó0¢ÚÚ÷F6ÜVB66R÷ñÁ6VÁ6óFófV«ì¢¢ÖDÙ¥TÁƒïÙ¥Uóƒî¥Uó≈4T5$UG≈55tG≈55tı$G¿¢ÚÚ44U55Ù¥Uó≈$ïdDUÙ¥UóƒUDáƒ5$TDTÂDî«≈4U54îÙÂÙ¥Uóƒ4ƒîTÂEı4T5$UBí¢‡¢Á&W∆6RÇÚÖÁ≈µ«3∑¬Ö“íÖ¥’¶◊£”ïı“¢ÉÛ•DÙ¥TÁƒïµÚ’”Ù¥Uó≈4T5$UG≈55tGƒ44U55µÚ’”Ù¥Uó≈$ïdDUµÚ’”Ù¥Uóƒ5$TDTÂDî≈3˜ƒ4ƒîTÂEµÚ’”ı4T5$UG≈4U54îÙÂµÚ’”Ù¥Uíï¥’¶◊£”ïı“•«2£’«2¢íÉÛ¢%µ‚%“¢'¬uµ‚u“¢w≈≈2≤íˆví¬rCC%∑&VF7FVE“rê¢ÚÚUDÇ÷f÷ñ«íVÁb76ñvÊ÷VÁB¬'WBˆÊ«íFÜR÷f˜&“áFÜRWFÜ˜&ó¶Fñˆ„¶ ¢ÚÚÜVFW"6ˆ∆ˆ‚f˜&“ó2ÜÊF∆VB6W&FV«í&V∆˜r¬ÊB◊W7BÊ˜B&RVFV‚ÜW&Rí‡¢Á&W∆6RÇÚÖÁ≈µ«3∑¬Ö“íÖ¥’¶◊£”ïı“§UDÖ¥’¶◊£”ïı“•«2£’«2¢íÉÛ¢%µ‚%“¢'¬uµ‚u“¢w≈≈2≤íˆví¬rCC%∑&VF7FVE“rê¢ÚÚ“◊Fˆ∂V‚Ú“÷í÷∂WíÚ“◊6V7&WB7Gñ∆Rf∆w2‡¢Á&W∆6RÇÚÇ““ÉÛßFˆ∂VÁ∆ïµÚ’”ˆ∂Wó«6V7&WG∆66W75µÚ’”ˆ∂Wó∆6∆ñVÁEµÚ’”˜6V7&WG∆WFÖµÚ’”˜Fˆ∂V‚íÉÛ£◊≈«2≤ííÉÛ¢%µ‚%“¢'¬uµ‚u“¢w≈≈2≤íˆví¬rC∑&VF7FVE“rê¢ÚÚWFÜ˜&ó¶Fñˆ„¢&V&W"Ù&˜BıFˆ∂V‚«Fˆ∂V„‚ÜÜVFW"˜"7W&¬‘Çf˜&“ì†¢ÚÚ&VF7BWfW'óFÜñÊrgFW"FÜR66ÜV÷R∂Wóv˜&BWFÚFÜR6∆˜6ñÊrV˜FR˜76R‡¢Á&W∆6RÇÚÜWFÜ˜&ó¶FñˆÂ«2££ı«2¢ÉÛ¶&V&W'∆&˜G«Fˆ∂V‚ï«2≤íÉÛ¢%µ‚%“¢'¬uµ‚u“¢w≈µÂ«2r%“≤íˆví¬rC∑&VF7FVE“rê¢Á&W∆6RÇÚÇÉÛ¶WFÜ˜&ó¶FñˆÁ«Ç÷í÷∂Wíï«2£•«2≤íÉÛ¢%µ‚%“¢'¬uµ‚u“¢w≈µÂ«2r%◊≥"«“íˆví¬rC∑&VF7FVE“rê¢ÚÚ6V7&WB÷∆ˆˆ∂ñÊrU$¬VW'í&◊2É˜Fˆ∂V„“‚‚‚fïˆ∂Wì“‚‚‚f66W75˜Fˆ∂V„“‚‚‚í‡¢Á&W∆6RÇÚÖ≥Úe“ÉÛßFˆ∂VÁ∆ïµÚ’”ˆ∂Wó∆66W75µÚ’”˜Fˆ∂VÁ«6V7&WG«6ñw«6ñvÊGW&W∆∂Wíì“íÉÛ•µ‚e«2"u“≤íˆví¬rC∑&VF7FVE“rì∞ß–¶gVÊ7Fñˆ‚˜6Ü˜'EFˆˆƒ∆&V¬áf«VR¬∆ñ÷óBó∞¢6ˆÁ7BFWáC’7G&ñÊráf«VW«¬rríÁ&W∆6RÇı«2≤ˆr¬rríÁG&ñ“Çì∞¢6ˆÁ7B÷É÷∆ñ÷óG«√#∞¢ñbáFWáBÊ∆VÊwFÉ√÷÷Çí&WGW&‚FWáC∞¢6ˆÁ7BÜVC‘÷FÇÊ÷ÇÉ#B¬÷FÇÊf∆ˆ˜"Ü÷Ç¢„cÇíì∞¢6ˆÁ7BFñ√‘÷FÇÊ÷ÇÉ"¬÷Ç÷ÜVB”2ì∞¢&WGW&‚FWáBÁ6∆ñ6RÉ∆ÜVBíÁG&ñ‘VÊBÇí≤r‚‚‚r∑FWáBÁ6∆ñ6RÇ◊Fñ¬íÁG&ñ’7F'BÇì∞ß–¶gVÊ7Fñˆ‚˜FˆˆƒìÜ‚Ü∂Wí¬f∆∆&6≤ó∞¢6ˆÁ7B&w3‘'&íÁ&˜F˜GóRÁ6∆ñ6RÊ6∆¬Ü&wV÷VÁG2√"ì∞¢ñbáGóVˆbC””“vgVÊ7Fñˆ‚ró∞¢6ˆÁ7Bf«VS◊BÊ«íÜÁV∆¬≈∂∂Wï“Ê6ˆÊ6BÜ&w2íì∞¢ñbáf«VRbgf«VR”÷∂Wíí&WGW&‚f«VS∞¢–¢&WGW&‚GóVˆbf∆∆&6≥””“vgVÊ7Fñˆ‚sˆf∆∆&6≤Ê«íÜÁV∆¬∆&w2ì•7G&ñÊrÜf∆∆&6∑«¬rrì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆ≈FÑ&6VÊ÷Ráf«VRó∞¢6ˆÁ7BFWáC’7G&ñÊráf«VW«¬rríÁG&ñ“Çì∞¢ñbÇFWáBí&WGW&‚rs∞¢6ˆÁ7BÊ˜&÷∆ó¶VC◊FWáBÁ&W∆6RÇıµ≈¬ı“≤BÚ¬rrì∞¢6ˆÁ7B'G3÷Ê˜&÷∆ó¶VBÁ7∆óBÇıµ≈¬ı“≤Úì∞¢&WGW&‚'G2Á˜Çó«∆Ê˜&÷∆ó¶VC∞ß–¶gVÊ7Fñˆ‚˜Fˆˆƒ7Fñˆ‰∂ñÊBáF2ó∞¢6ˆÁ7B„’7G&ñÊráF2bgF2ÊÊ÷W«¬rríÁFÙ∆˜vW$66RÇíÁ&W∆6RÇıµÊ◊£”ï“≤ˆr¬uÚrì∞¢ñbÇ‚í&WGW&‚wVÊ∂Ê˜v‚s∞¢ñbÜ„””“w7V&vVÁE˜&ˆw&W72w«∆„””“vFV∆VvFU˜F6≤rí&WGW&‚vFV∆VvFRs∞¢ñbÜ‚ÊñÊ6«VFW2Çw6∂ñ∆¬ríí&WGW&‚w6∂ñ∆¬s∞¢ñbÜ‚ÊñÊ6«VFW2Çv÷V÷˜'íríí&WGW&‚v÷V÷˜'ís∞¢ñbÜ‚ÊñÊ6«VFW2ÇwFW&÷ñÊ¬ró«∆‚ÊñÊ6«VFW2Çw6ÜV∆¬ró«∆‚ÊñÊ6«VFW2Çv6ˆ÷÷ÊBró«∆‚ÊñÊ6«VFW2Çw&ˆ6W72ró«∆„””“vWÜV7WFUˆ6ˆFRrí&WGW&‚w6ÜV∆¬s∞¢ñbÜ‚ÊñÊ6«VFW2Çw&VBró«∆‚ÊñÊ6«VFW2ÇwfñWrró«∆‚ÊñÊ6«VFW2Çv˜V‚ró«∆„””“wfó6ñˆÂˆÊ«ó¶Rrí&WGW&‚w&VBs∞¢ñbÜ‚ÊñÊ6«VFW2Çv∆ó7Bró«∆„””“wFˆFÚrí&WGW&‚v∆ó7Bs∞¢ñbÜ‚ÊñÊ6«VFW2ÇwvV"ró«∆‚ÊñÊ6«VFW2ÇvfWF6Çró«∆‚ÊñÊ6«VFW2Çv7W&¬ró«∆‚ÊñÊ6«VFW2ÇvWáG&7Bró«∆‚ÊñÊ6«VFW2Çv'&˜w6Rró«∆‚ÊñÊ6«VFW2ÇvÊfñvFRríí&WGW&‚wvV"s∞¢ñbÜ‚ÊñÊ6«VFW2Çw6V&6Çró«∆‚ÊñÊ6«VFW2Çvw&Wró«∆‚ÊñÊ6«VFW2ÇvfñÊBríí&WGW&‚w6V&6Çs∞¢ñbÜ‚ÊñÊ6«VFW2Çww&óFRró«∆‚ÊñÊ6«VFW2ÇwF6Çró«∆‚ÊñÊ6«VFW2ÇvVFóBríí&WGW&‚ww&óFRs∞¢&WGW&‚wVÊ∂Ê˜v‚s∞ß–¶gVÊ7Fñˆ‚˜Fˆˆƒ∂ñÊDñ6ˆ‚Ü∂ñÊBó∞¢6ˆÁ7Bñ6ˆÁ3◊∞¢6ÜV∆√¢wFW&÷ñÊ¬r¿¢&VC¢vfñ∆R◊FWáBr¿¢∆ó7C¢v∆ó7Br¿¢6V&6É¢w6V&6Çr¿¢vV#¢vv∆ˆ&Rr¿¢w&óFS¢vfñ∆R◊V‚r¿¢6∂ñ∆√¢v&ˆˆ≤÷˜V‚r¿¢÷V÷˜'ì¢v'&ñ‚r¿¢FV∆VvFS¢v&˜Br¿¢VÊ∂Ê˜v„¢ww&VÊ6Çr¿¢”∞¢&WGW&‚∆íÜñ6ˆÁ5∂∂ñÊE◊«∆ñ6ˆÁ2ÁVÊ∂Ê˜v‚√Bì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆ≈F&vWD∆&V¬áF2ó∞¢6ˆÁ7B◊F2bgF2Ê&w7««∑”∞¢6ˆÁ7B∂ñÊC’˜Fˆˆƒ7Fñˆ‰∂ñÊBáF2ì∞¢∆WB&s“rs∞¢ñbÜ∂ñÊC””“w6ÜV∆¬rí&s÷Ê6÷G«∆Ê6ˆ÷÷ÊG««F2Ê6ˆ÷÷ÊG««F2Á&uˆ6ˆ÷÷ÊG««F2Ê˜&ñvñÊ≈ˆ6ˆ÷÷ÊG««F2ÊFó7∆ïˆ6ˆ÷÷ÊG«¬rs∞¢V«6RñbÜ∂ñÊC””“w6∂ñ∆¬rí&s÷ÊÊ÷W«∆Á6∂ñ∆««¬rs∞¢V«6RñbÜ∂ñÊC””“v÷V÷˜'írí&s÷ÁF&vWG«∆ÊÊ÷W«∆Ê7FñˆÁ«¬rs∞¢V«6RñbÜ∂ñÊC””“w&VBw«∆∂ñÊC””“ww&óFRrí&s÷ÁFá«∆Êfñ∆U˜Fá«∆Êfñ∆W«∆ÁF&vWG«∆ÊÊ÷W«¬rs∞¢V«6RñbÜ∂ñÊC””“w6V&6Çw«∆∂ñÊC””“wvV"rí&s÷ÁVW'ó«∆ÁGFW&Á«∆ÁW&««∆ÁW&ó«¬rs∞¢V«6R&s÷Ê6÷G«∆Ê6ˆ÷÷ÊG«∆ÁFá«∆Êfñ∆U˜Fá«∆Êfñ∆W«∆ÁW&ó«∆ÁW&««∆ÁVW'ó«∆ÁGFW&Á«∆ÊFó'«∆ÁF6∑«∆ÊÊ÷W«¬rs∞¢&WGW&‚˜&VF7EFˆˆ≈F&vWD∆&V¬ÖˆFV6ˆFUFˆˆƒ∆&VƒVÁFóFñW2Ö7G&ñÊrá&ríÁ7∆óBÇu∆‚rï≥“ÁG&ñ“Çííì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆ≈&VE&ÊvT∆&V¬áF2ó∞¢6ˆÁ7BÊ÷S’7G&ñÊráF2bgF2ÊÊ÷W«¬rríÁFÙ∆˜vW$66RÇíÁ&W∆6RÇıµÊ◊£”ï“≤ˆr¬uÚrì∞¢ñbÜÊ÷R”“w&VEˆfñ∆Rrí&WGW&‚rs∞¢6ˆÁ7B&w3◊F2bgF2Ê&w7««∑”∞¢6ˆÁ7Bˆfg6WC÷&w2Êˆfg6WC∞¢ñbÇÁV÷&W"Êó56fTñÁFVvW"Üˆfg6WBó«∆ˆfg6WC√”í&WGW&‚rs∞¢6ˆÁ7B∆ñ÷óC÷&w2Ê∆ñ÷óC∞¢ñbÜ∆ñ÷óC””◊VÊFVfñÊVBí&WGW&‚¬G∂ˆfg6WG÷∞¢ñbÇÁV÷&W"Êó56fTñÁFVvW"Ü∆ñ÷óBó«∆∆ñ÷óC√”í&WGW&‚rs∞¢ñbÜ∆ñ÷óC”””í&WGW&‚¬G∂ˆfg6WG÷∞¢6ˆÁ7B7„÷∆ñ÷óB”∞¢ñbÜˆfg6WC‰ÁV÷&W"‰‘Öı4dUÙîÂDTtU"◊7‚í&WGW&‚rs∞¢&WGW&‚¬G∂ˆfg6WG““G∂ˆfg6WB∑7Á÷∞ß–¶gVÊ7Fñˆ‚˜FˆˆƒgV∆ƒ6ˆ÷÷ÊD∆&V¬áF2ó∞¢ÚÚgV∆¬Ü◊V«Fí÷∆ñÊRí6ÜV∆¬6ˆ÷÷ÊBf˜"FÜRUÖ‰DTBFWFñ¬∆VB‚÷ó'&˜'2FÜP¢ÚÚ6ÜV∆¬&r÷WáG&7Fñˆ‚ñ‚˜Fˆˆ≈F&vWD∆&V¬'WBtïDÑıUBFÜRÁ7∆óBÇu∆‚rï≥–¢ÚÚfó'7B÷∆ñÊR6ˆ∆∆6R¬6Ú◊V«Fí÷∆ñÊR67&óB6Ü˜w2WfW'í∆ñÊRvÜV‚FÜR6&@¢ÚÚó2WáÊFVBÇ3Cì#bí‚&VF7Fñˆ‚≤VÁFóGí÷FV6ˆFR7Fñ∆¬∆ñVBFÚFÜRvÜˆ∆R‡¢6ˆÁ7B◊F2bgF2Ê&w7««∑”∞¢6ˆÁ7B&s÷Ê6÷G«∆Ê6ˆ÷÷ÊG««F2Ê6ˆ÷÷ÊG««F2Á&uˆ6ˆ÷÷ÊG««F2Ê˜&ñvñÊ≈ˆ6ˆ÷÷ÊG««F2ÊFó7∆ïˆ6ˆ÷÷ÊG«¬rs∞¢&WGW&‚˜&VF7EFˆˆ≈F&vWD∆&V¬ÖˆFV6ˆFUFˆˆƒ∆&VƒVÁFóFñW2Ö7G&ñÊrá&ríÁ&W∆6RÇı«2≤BÚ¬rrííì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆ≈fó6ñ&∆UF&vWD∆&V¬áF2¬˜G2ó∞¢˜G3÷˜G7««∑”∞¢6ˆÁ7BF&vWC’˜Fˆˆ≈F&vWD∆&V¬áF2ì∞¢ñbÇF&vWBí&WGW&‚rs∞¢6ˆÁ7B∂ñÊC’˜Fˆˆƒ7Fñˆ‰∂ñÊBáF2ì∞¢ñbÜ∂ñÊC””“w&VBw«∆∂ñÊC””“ww&óFRró∞¢∆WBFWáC’˜Fˆˆ≈FÑ&6VÊ÷RáF&vWBó««F&vWC∞¢6ˆÁ7B&ÊvS÷∂ñÊC””“w&VBsı˜Fˆˆ≈&VE&ÊvT∆&V¬áF2ì¢rs∞¢ñbá&ÊvRíFWáC÷˜G2Á&ÊvTfó'7CˆG∑&ÊvW“+rG∑FWáG÷¶G∑FWáG“+rG∑&ÊvW÷∞¢&WGW&‚˜6Ü˜'EFˆˆƒ∆&V¬áFWáB¬˜G2Ê∆ñ÷óG«√"ì∞¢–¢ñbÜ∂ñÊC””“w6∂ñ∆¬ró∞¢6ˆÁ7B7VffóÉ’˜FˆˆƒìÜ‚ÇwFˆˆ≈˜F&vWE˜6∂ñ∆≈˜7VffóÇr¬w6∂ñ∆¬rì∞¢6ˆÁ7BFWáC◊F&vWBÁFÙ∆˜vW$66RÇíÊVÊG5vóFÇÖ7G&ñÊrá7VffóÇíÁFÙ∆˜vW$66RÇíì˜F&vWC¶G∑F&vWG“G∑7Vffóá÷∞¢&WGW&‚˜6Ü˜'EFˆˆƒ∆&V¬áFWáB¬˜G2Ê∆ñ÷óG«√"ì∞¢–¢&WGW&‚˜6Ü˜'EFˆˆƒ∆&V¬áF&vWB¬˜G2Ê∆ñ÷óG«√"ì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆƒ6ˆ÷÷ÊEFóF∆RÜ6ˆ÷÷ÊBó∞¢6ˆÁ7BÊ˜&÷∆ó¶VC’7G&ñÊrÜ6ˆ÷÷ÊG«¬rríÁ&W∆6RÇı«2≤ˆr¬rríÁG&ñ“Çì∞¢ñbÇÊ˜&÷∆ó¶VBí&WGW&‚rs∞¢ñbÇıÊvóE«2∂fWF6Ö∆"ˆíÁFW7BÜÊ˜&÷∆ó¶VBíí&WGW&‚vvóBfWF6Çs∞¢ñbÇıÊvóE«2≤ÉÛß7FGW7«&Wb÷∆ó7G∆'&Ê6Çï∆"ˆíÁFW7BÜÊ˜&÷∆ó¶VBíí&WGW&‚vvóBÜVBˆ&VÜñÊBs∞¢ñbÇıÊvóE«2∂∆ˆu∆"ˆíÁFW7BÜÊ˜&÷∆ó¶VBíí&WGW&‚vvóB∆ˆrs∞¢ñbÇı∆&7W&≈∆"ˆíÁFW7BÜÊ˜&÷∆ó¶VBíbbı¬ˆÜV«FÖ∆"ˆíÁFW7BÜÊ˜&÷∆ó¶VBíí&WGW&‚vÜV«FÇ6ÜV6≤s∞¢ñbÇı∆"ÉÛß7«w&Wï∆"ˆíÁFW7BÜÊ˜&÷∆ó¶VBíí&WGW&‚w&ˆ6W726ÜV6≤s∞¢6ˆÁ7B”÷Ê˜&÷∆ó¶VBÊ÷F6ÇÇı∆&«6ˆe∆"‚¢ÉÛ¢÷ó√¢íÖ∆G≥"√W“ï∆"ˆíì∞¢ñbÜ“í&WGW&‚˜'BG∂’≥◊“6ÜV6∂∞¢ñbÇı∆&∆VÊ6Ü7F≈∆"ˆíÁFW7BÜÊ˜&÷∆ó¶VBíí&WGW&‚v∆VÊ6Ü7F¬s∞¢&WGW&‚˜6Ü˜'EFˆˆƒ∆&V¬ÜÊ˜&÷∆ó¶VB√s"ì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆ≈VW'ïFóF∆RáVW'íó∞¢6ˆÁ7BÊ˜&÷∆ó¶VC’7G&ñÊráVW'ó«¬rríÁ&W∆6RÇı«2≤ˆr¬rríÁG&ñ“Çì∞¢&WGW&‚˜6Ü˜'EFˆˆƒ∆&V¬ÜÊ˜&÷∆ó¶VB√s"ì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆƒ7Fñˆ‰∆&V≈FWáBáF2¬˜G2ó∞¢˜G3÷˜G7««∑”∞¢6ˆÁ7B∂ñÊC’˜Fˆˆƒ7Fñˆ‰∂ñÊBáF2ì∞¢6ˆÁ7BFˆÊS◊F2bgF2ÊFˆÊR”÷f«6S∞¢6ˆÁ7Bó4W'#◊F2bgF2Êó5ˆW'&˜#∞¢6ˆÁ7B7FFS÷FˆÊSÚvFˆÊRs¢w'VÊÊñÊrs∞¢∆WBF&vWC÷˜G2ÊvVÊW&ñ3Úrs•˜Fˆˆ≈fó6ñ&∆UF&vWD∆&V¬áF2¬˜G2ì∞¢ñbÇÜ∂ñÊC””“w6V&6Çw«∆∂ñÊC””“wvV"ríbgF&vWBíF&vWC’˜Fˆˆ≈VW'ïFóF∆RáF&vWBì∞¢6ˆÁ7BFó7∆ì’˜FˆˆƒFó7∆îÊ÷RáF2ì∞¢&WGW&‚˜FˆˆƒìÜ‚ÇwFˆˆ≈ˆ7FñˆÂˆ∆&V¬r¬Ü≤«2«FwB∆Fó7∆W'"ì”Á∞¢6ˆÁ7BfW&'3◊∞¢6ÜV∆√ß∑'VÊÊñÊs¢u'VÊÊñÊrr∆FˆÊS¢u&‚r∆f∆∆&6≥¢v6ˆ÷÷ÊBw“¿¢&VCß∑'VÊÊñÊs¢u&VFñÊrr∆FˆÊS¢u&VBr∆f∆∆&6≥¢vfñ∆Rw“¿¢∆ó7Cß∑'VÊÊñÊs¢t∆ó7FñÊrr∆FˆÊS¢t∆ó7FVBr∆f∆∆&6≥¢vfñ∆W2w“¿¢6V&6Éß∑'VÊÊñÊs¢u6V&6ÜñÊrf˜"r∆FˆÊS¢u6V&6ÜVBf˜"r∆f∆∆&6≥¢wv˜&∑76Rw“¿¢vV#ß∑'VÊÊñÊs¢t6ÜV6∂ñÊrr∆FˆÊS¢t6ÜV6∂VBr∆f∆∆&6≥¢wvV"FFw“¿¢w&óFSß∑'VÊÊñÊs¢uWFFñÊrr∆FˆÊS¢uWFFVBr∆f∆∆&6≥¢vfñ∆Rw“¿¢6∂ñ∆√ß∑'VÊÊñÊs¢t∆ˆFñÊrr∆FˆÊS¢t∆ˆFVBr∆f∆∆&6≥¢v6∂ñ∆¬w“¿¢÷V÷˜'ìß∑'VÊÊñÊs¢u6fñÊrr∆FˆÊS¢u6fVBr∆f∆∆&6≥¢v÷V÷˜'íw“¿¢FV∆VvFSß∑'VÊÊñÊs¢tFV∆VvFñÊrr∆FˆÊS¢tFV∆VvFVBr∆f∆∆&6≥¢vF6≤w“¿¢VÊ∂Ê˜v„ß∑'VÊÊñÊs¢u'VÊÊñÊrr∆FˆÊS¢u&‚r∆f∆∆&6≥¶Fó7«¬vFˆˆ¬w“¿¢”∞¢6ˆÁ7Bc◊fW&'5∂µ◊««fW&'2ÁVÊ∂Ê˜v„∞¢6ˆÁ7BfW&#◊e∑5◊««bÁ'VÊÊñÊs∞¢6ˆÁ7Bˆ&¶V7C◊FwG««bÊf∆∆&6∑«∆Fó7«¬wFˆˆ¬s∞¢ñbÜW'"í&WGW&‚fñ∆VBGµ7G&ñÊrábÁ'VÊÊñÊw««fW&"íÁFÙ∆˜vW$66RÇó“G∂ˆ&¶V7G÷∞¢&WGW&‚G∑fW&'“G∂ˆ&¶V7G÷∞¢“∆∂ñÊB«7FFR«F&vWB∆Fó7∆í∆ó4W'"ì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆƒ7Fñˆ‰∆&V¬áF2ó∞¢&WGW&‚W62Ö˜Fˆˆƒ7Fñˆ‰∆&V≈FWáBáF2«∂∆ñ÷óC£'“íì∞ß–¶6ˆÁ7B˜Fˆˆ≈v˜&∂∆ˆu7V÷÷&ñW3◊∑6ÜV∆√ß∑“«&VCß∑“∆∆ó7Cß∑“«6V&6Éß∑“«vV#ß∑“«w&óFSß∑“«6∂ñ∆√ß∑“∆÷V÷˜'ìß∑“∆FV∆VvFSß∑“«VÊ∂Ê˜v„ß∑◊”∞¶gVÊ7Fñˆ‚˜Fˆˆ≈v˜&∂∆ˆu7V÷÷'î∆ñÊRÜ∂ñÊB¬7FFR¬6˜VÁBó∞¢6ˆÁ7B„‘÷FÇÊ÷ÇÉƒÁV÷&W"Ü6˜VÁBó«√ì∞¢&WGW&‚˜FˆˆƒìÜ‚ÇwFˆˆ≈˜v˜&∂∆ˆu˜7V÷÷'ír¬Ü≤«2∆2ì”Á∞¢6ˆÁ7Bf˜&◊3◊∞¢6ÜV∆√ß∑'VÊÊñÊs•≤u'VÊÊñÊr6ˆ÷÷ÊBr¬u'VÊÊñÊr∂Á“6ˆ÷÷ÊG2u“∆FˆÊS•≤u&‚6ˆ÷÷ÊBr¬u&‚∂Á“6ˆ÷÷ÊG2u◊“¿¢&VCß∑'VÊÊñÊs•≤u&VFñÊrfñ∆Rr¬u&VFñÊr∂Á“fñ∆W2u“∆FˆÊS•≤u&VBfñ∆Rr¬u&VB∂Á“fñ∆W2u◊“¿¢∆ó7Cß∑'VÊÊñÊs•≤t∆ó7FñÊrfñ∆W2r¬t∆ó7FñÊr∂Á“óFV◊2u“∆FˆÊS•≤t∆ó7FVBfñ∆W2r¬t∆ó7FVB∂Á“fñ∆W2u◊“¿¢6V&6Éß∑'VÊÊñÊs•≤u6V&6ÜñÊrv˜&∑76Rr¬u6V&6ÜñÊrv˜&∑76R∂Á“Fñ÷W2u“∆FˆÊS•≤u6V&6ÜVBv˜&∑76Rr¬u6V&6ÜVBv˜&∑76R∂Á“Fñ÷W2u◊“¿¢vV#ß∑'VÊÊñÊs•≤t6ÜV6∂ñÊrvV"r¬t6ÜV6∂ñÊrvV"∂Á“Fñ÷W2u“∆FˆÊS•≤t6ÜV6∂VBFÜRvV"r¬t6ÜV6∂VBFÜRvV"∂Á“Fñ÷W2u◊“¿¢w&óFSß∑'VÊÊñÊs•≤uWFFñÊrfñ∆Rr¬uWFFñÊr∂Á“fñ∆W2u“∆FˆÊS•≤uWFFVBfñ∆Rr¬uWFFVB∂Á“fñ∆W2u◊“¿¢6∂ñ∆√ß∑'VÊÊñÊs•≤t∆ˆFñÊr6∂ñ∆¬r¬t∆ˆFñÊr∂Á“6∂ñ∆«2u“∆FˆÊS•≤t∆ˆFVB6∂ñ∆¬r¬t∆ˆFVB∂Á“6∂ñ∆«2u◊“¿¢÷V÷˜'ìß∑'VÊÊñÊs•≤u6fñÊr÷V÷˜'ír¬u6fñÊr∂Á“÷V÷˜'íWFFW2u“∆FˆÊS•≤u6fVB÷V÷˜'ír¬u6fVB∂Á“÷V÷˜'íWFFW2u◊“¿¢FV∆VvFSß∑'VÊÊñÊs•≤tFV∆VvFñÊrF6≤r¬tFV∆VvFñÊr∂Á“F6∑2u“∆FˆÊS•≤tFV∆VvFVBF6≤r¬tFV∆VvFVB∂Á“F6∑2u◊“¿¢VÊ∂Ê˜v„ß∑'VÊÊñÊs•≤u'VÊÊñÊrFˆˆ¬r¬u'VÊÊñÊr∂Á“Fˆˆ«2u“∆FˆÊS•≤u&‚Fˆˆ¬r¬u&‚∂Á“Fˆˆ«2u◊“¿¢”∞¢6ˆÁ7Bó#“ÇÜf˜&◊5∂µ◊«∆f˜&◊2ÁVÊ∂Ê˜v‚ï∑5◊«∆f˜&◊2ÁVÊ∂Ê˜v‚Á'VÊÊñÊrì∞¢&WGW&‚Ü3”””˜ó%≥”ßó%≥“íÁ&W∆6RÇw∂Á“r≈7G&ñÊrÜ2íì∞¢“∆∂ñÊB«7FFR∆‚ì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆ≈v˜&∂∆ˆt¶ˆñ‚Ü∆ñÊW2ó∞¢6ˆÁ7B'G3‘'&íÊg&ˆ“Ü∆ñÊW7«≈µ“íÊfñ«FW"Ñ&ˆˆ∆V‚ì∞¢ñbá'G2Ê∆VÊwFÉ√”í&WGW&‚'G5≥◊«¬rs∞¢&WGW&‚˜FˆˆƒìÜ‚ÇwFˆˆ≈˜7V÷÷'ïˆ¶ˆñ‚r¬ÜóFV◊2ì”ÊóFV◊2Ê¶ˆñ‚Çr¬rí«'G2ì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆ≈v˜&∂∆ˆt7FñˆÂ'G2áF2ó∞¢ñbáF2bgF2ÊÊˆFUGóS”””ó∞¢6ˆÁ7B&˜s◊F2Ê6∆74∆ó7BbgF2Ê6∆74∆ó7BÊ6ˆÁFñÁ2ÇwFˆˆ¬÷6&B◊&˜rrì˜F3ßF2Ê6∆˜6W7BbgF2Ê6∆˜6W7BÇrÁFˆˆ¬÷6&B◊&˜rrì∞¢6ˆÁ7B6&C◊F2Ê6∆74∆ó7BbgF2Ê6∆74∆ó7BÊ6ˆÁFñÁ2ÇwFˆˆ¬÷6&Brì˜F3¢á&˜rbg&˜rÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&Bríì∞¢6ˆÁ7B7Fñˆ‰∆&V√“á&˜rbg&˜rÊFF6WBÁFˆˆƒ7Fñˆ‰∆&V¬ó«¬Ü6&Bbf6&BÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&B÷Ê÷Rríbf6&BÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&B÷Ê÷RríÁFWáD6ˆÁFVÁBÁG&ñ“Çíó«¬rs∞¢6ˆÁ7B∂ñÊC“á&˜rbg&˜rÊFF6WBÁFˆˆƒ∂ñÊBó«¬wVÊ∂Ê˜v‚s∞¢6ˆÁ7Bó4FˆÊS“Çá&˜rbg&˜rÊFF6WBÁFˆˆƒFˆÊRì””“vf«6Rw«¬Ü6&Bbf6&BÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwFˆˆ¬÷6&B◊'VÊÊñÊrrííì∞¢6ˆÁ7Bó4W'#“á&˜rbg&˜rÊFF6WBÁFˆˆƒW'&˜"ì””“wG'VRw«¬Ü6&Bbf6&BÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwFˆˆ¬÷6&B÷W'&˜"ríì∞¢&WGW&‚∂∂ñÊB∆ó4FˆÊR∆ó4W'"«F&vWC¢rr∆7Fñˆ‰∆&V«”∞¢–¢6ˆÁ7B∂ñÊC’˜Fˆˆƒ7Fñˆ‰∂ñÊBáF2ì∞¢&WGW&‚∞¢∂ñÊB¿¢ó4FˆÊSßF2bgF2ÊFˆÊR”÷f«6R¿¢ó4W'#ßF2bgF2Êó5ˆW'&˜"¿¢F&vWC•˜Fˆˆ≈F&vWD∆&V¬áF2í¿¢7Fñˆ‰∆&V√•˜Fˆˆƒ7Fñˆ‰∆&V≈FWáBáF2í¿¢”∞ß–¶gVÊ7Fñˆ‚˜Fˆˆ≈v˜&∂∆ˆu7V÷÷'íáFˆˆƒ6∆«2¬˜G2ó∞¢6ˆÁ7B6&G3‘'&íÊg&ˆ“áFˆˆƒ6∆«7«≈µ“íÊfñ«FW"áF3”ÁF2ì∞¢ñbÇ6&G2Ê∆VÊwFÇí&WGW&‚Ü˜G2bf˜G2Ê∆ófRìÚu'VÊÊñÊrs¢uv˜&∂∆ˆrs∞¢ñbÜ6&G2Ê∆VÊwFÉ”””ó∞¢6ˆÁ7B'C’˜Fˆˆ≈v˜&∂∆ˆt7FñˆÂ'G2Ü6&G5≥“ì∞¢6ˆÁ7B∆ñÊS’˜Fˆˆ≈v˜&∂∆ˆu7V÷÷'î∆ñÊRá'BÊ∂ñÊB«'BÊó4FˆÊSÚvFˆÊRs¢w'VÊÊñÊrr√ì∞¢&WGW&‚'BÊó4W'#ˆG∂∆ñÊW“¬fñ∆VF¶∆ñÊS∞¢–¢6ˆÁ7B˜&FW#’≤w6ÜV∆¬r¬w&VBr¬w6V&6Çr¬ww&óFRr¬w6∂ñ∆¬r¬v÷V÷˜'ír¬wvV"r¬v∆ó7Br¬vFV∆VvFRr¬wVÊ∂Ê˜v‚u”∞¢6ˆÁ7B'VÊÊñÊt6˜VÁG3◊∑“¬FˆÊT6˜VÁG3◊∑”∞¢∆WBfñ∆VC”∞¢f˜"Ü6ˆÁ7BF2ˆb6&G2ó∞¢6ˆÁ7B'C’˜Fˆˆ≈v˜&∂∆ˆt7FñˆÂ'G2áF2ì∞¢6ˆÁ7B6˜VÁG3◊'BÊó4FˆÊSˆFˆÊT6˜VÁG3ß'VÊÊñÊt6˜VÁG3∞¢6˜VÁG5∑'BÊ∂ñÊE”“Ü6˜VÁG5∑'BÊ∂ñÊE◊«√í≥∞¢ñbá'BÊó4W'"ífñ∆VB≥”∞¢–¢6ˆÁ7BV÷óC“Ü6˜VÁG2«7FFRì”Á∞¢6ˆÁ7B˜WC’µ”∞¢f˜"Ü6ˆÁ7B∂ñÊBˆb˜&FW"ó∞¢6ˆÁ7B„÷6˜VÁG5∂∂ñÊE◊«√∞¢ñbÇ‚í6ˆÁFñÁVS∞¢˜WBÁW6ÇÖ˜Fˆˆ≈v˜&∂∆ˆu7V÷÷'î∆ñÊRÜ∂ñÊB«7FFR∆‚íì∞¢–¢&WGW&‚˜WC∞¢”∞¢6ˆÁ7B∆ñÊW3’≤‚‚ÊV÷óBá'VÊÊñÊt6˜VÁG2¬w'VÊÊñÊrrí¬‚‚ÊV÷óBÜFˆÊT6˜VÁG2¬vFˆÊRrï”∞¢ñbÜfñ∆VBí∆ñÊW2ÁW6ÇÜG∂fñ∆VG“fñ∆VFì∞¢&WGW&‚∆ñÊW2Ê∆VÊwFÉı˜Fˆˆ≈v˜&∂∆ˆt¶ˆñ‚Ü∆ñÊW2ì•˜Fˆˆƒ7Fñˆ‰∆&V¬Ü6&G5≥“ì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆ≈v˜&∂∆ˆt∆ó7DV¬Üw&˜Wó∞¢ñbÇw&˜Wí&WGW&‚ÁV∆√∞¢&WGW&‚w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬◊v˜&∂∆ˆr÷∆ó7Brí«¬w&˜WÁVW'ï6V∆V7F˜"ÇrÊ7FófóGí÷&ˆGírí«¬w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6∆¬÷w&˜W÷&ˆGírì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆ≈v˜&∂∆ˆuFˆˆ«4V¬Üw&˜Wó∞¢6ˆÁ7B∆ó7C’˜Fˆˆ≈v˜&∂∆ˆt∆ó7DV¬Üw&˜Wì∞¢ñbÇ∆ó7Bí&WGW&‚ÁV∆√∞¢∆WBFˆˆ«3÷∆ó7BÁVW'ï6V∆V7F˜"Çsß66˜R‚Áv¬◊7FW◊Fˆˆ«5∂FF◊v˜&∂∆ˆr◊Fˆˆ«3“#%“rì∞¢ñbÇFˆˆ«2ó∞¢Fˆˆ«3÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢Fˆˆ«2Ê6∆74Ê÷S“wv¬◊7FW◊Fˆˆ«2Fˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ«2s∞¢Fˆˆ«2Á6WDGG&ñ'WFRÇvFF◊v˜&∂∆ˆr◊Fˆˆ«2r¬srì∞¢∆ó7BÊVÊD6Üñ∆BáFˆˆ«2ì∞¢–¢&WGW&‚Fˆˆ«3∞ß–¶gVÊ7Fñˆ‚ˆ∆ófUFˆˆ≈7FWV¬Üw&˜Wó∞¢6ˆÁ7B∆ó7C’˜Fˆˆ≈v˜&∂∆ˆt∆ó7DV¬Üw&˜Wì∞¢ñbÇ∆ó7Bí&WGW&‚ÁV∆√∞¢6ˆÁ7B∆7C÷∆ó7BÊ∆7DV∆V÷VÁD6Üñ∆C∞¢ñbÜ∆7Bbf∆7BÊ6∆74∆ó7Bbf∆7BÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çwv¬◊7FW◊Fˆˆ«2ríbf∆7BÊvWDGG&ñ'WFRÇvFF◊v˜&∂∆ˆr◊Fˆˆ«2rì””“srí&WGW&‚∆7C∞¢6ˆÁ7BFˆˆ«3÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢Fˆˆ«2Ê6∆74Ê÷S“wv¬◊7FW◊Fˆˆ«2Fˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ«2s∞¢Fˆˆ«2Á6WDGG&ñ'WFRÇvFF◊v˜&∂∆ˆr◊Fˆˆ«2r¬srì∞¢∆ó7BÊVÊD6Üñ∆BáFˆˆ«2ì∞¢&WGW&‚Fˆˆ«3∞ß–¶gVÊ7Fñˆ‚ˆFó&V7Ev˜&∂∆ˆuFˆˆ≈&˜w2Ü∆ó7Bó∞¢ñbÇ∆ó7Bí&WGW&‚µ”∞¢6ˆÁ7B&˜w3’µ”∞¢'&íÊg&ˆ“Ü∆ó7BÊ6Üñ∆G&V‚íÊf˜$V6ÇÜ6Üñ∆C”Á∞¢ñbÜ6Üñ∆BÊ6∆74∆ó7Bbf6Üñ∆BÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwFˆˆ¬÷6&B◊&˜rríí&˜w2ÁW6ÇÜ6Üñ∆Bì∞¢V«6RñbÜ6Üñ∆BÊ6∆74∆ó7BbbÜ6Üñ∆BÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwFˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ¬÷w&˜Wró«∆6Üñ∆BÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwFˆˆ¬÷w&˜Wrííí&˜w2ÁW6ÇÇ‚‚‰'&íÊg&ˆ“Ü6Üñ∆BÁVW'ï6V∆V7F˜$∆¬ÇrÁFˆˆ¬÷6&B◊&˜rrííì∞¢“ì∞¢&WGW&‚&˜w3∞ß–¶gVÊ7Fñˆ‚˜VÁw&ÊW7FVEFˆˆƒw&˜W2áFˆˆ«2ó∞¢ñbÇFˆˆ«2í&WGW&„∞¢Fˆˆ«2ÁVW'ï6V∆V7F˜$∆¬Çsß66˜R‚ÁFˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ¬÷w&˜W√ß66˜R‚ÁFˆˆ¬÷w&˜WríÊf˜$V6ÇÜV√”ÊV¬Á&V÷˜fRÇíì∞ß–¶gVÊ7Fñˆ‚˜Fˆˆƒw&˜W&ñ÷'î∂ñÊBá&˜w2ó∞¢6ˆÁ7B6˜VÁG3‘ˆ&¶V7BÊ7&VFRÜÁV∆¬ì∞¢'&íÊg&ˆ“á&˜w7«≈µ“íÊf˜$V6Çá&˜s”Á∞¢6ˆÁ7B∂ñÊC◊&˜rbg&˜rÊFF6WBbg&˜rÊFF6WBÁFˆˆƒ∂ñÊC˜&˜rÊFF6WBÁFˆˆƒ∂ñÊC¢wVÊ∂Ê˜v‚s∞¢6˜VÁG5∂∂ñÊE”“Ü6˜VÁG5∂∂ñÊE◊«√í≥∞¢“ì∞¢6ˆÁ7B˜&FW#’≤w6V&6Çr¬w6ÜV∆¬r¬w&VBr¬ww&óFRr¬w6∂ñ∆¬r¬v÷V÷˜'ír¬wvV"r¬v∆ó7Br¬vFV∆VvFRr¬wVÊ∂Ê˜v‚u”∞¢f˜"Ü6ˆÁ7B∂ñÊBˆb˜&FW"ó∞¢ñbÜ6˜VÁG5∂∂ñÊE“í&WGW&‚∂ñÊC∞¢–¢&WGW&‚wVÊ∂Ê˜v‚s∞ß–¶gVÊ7Fñˆ‚˜Fˆˆƒw&˜Wñ6ˆ‚á&˜w2ó∞¢&WGW&‚˜Fˆˆƒ∂ñÊDñ6ˆ‚Ö˜Fˆˆƒw&˜W&ñ÷'î∂ñÊBá&˜w2íì∞ß–¶gVÊ7Fñˆ‚˜7ñÊ5Fˆˆ≈&˜w46ˆÁFñÊW"áFˆˆ«2¬ó4∆ófUv˜&∂∆ˆró∞¢ñbÇFˆˆ«2í&WGW&„∞¢6ˆÁ7BWÜó7FñÊtw&˜W◊Fˆˆ«2ÁVW'ï6V∆V7F˜"Çsß66˜R‚ÁFˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ¬÷w&˜W√ß66˜R‚ÁFˆˆ¬÷w&˜W∂FF◊Fˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ¬÷w&˜W“#%“rì∞¢6ˆÁ7Bv4˜V„“ÜWÜó7FñÊtw&˜WbfWÜó7FñÊtw&˜WÊ6∆74∆ó7BbfWÜó7FñÊtw&˜WÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv˜V‚ríì∞¢6ˆÁ7B&˜w3’ˆFó&V7Ev˜&∂∆ˆuFˆˆ≈&˜w2áFˆˆ«2ì∞¢˜VÁw&ÊW7FVEFˆˆƒw&˜W2áFˆˆ«2ì∞¢&˜w2Êf˜$V6Çá&˜s”Á≤ñbá&˜rÁ&VÁDV∆V÷VÁBí&˜rÁ&V÷˜fRÇì≤“ì∞¢Fˆˆ«2ÁVW'ï6V∆V7F˜$∆¬Çsß66˜R‚ÁFˆˆ¬÷6&B◊&˜rríÊf˜$V6Çá&˜s”Á&˜rÁ&V÷˜fRÇíì∞¢6ˆÁ7B6Ü˜V∆Dw&˜W◊Fˆˆ«2Ê6∆74∆ó7BÊ6ˆÁFñÁ2Çwv¬◊7FW◊Fˆˆ«2ríbb&˜w2Ê∆VÊwFÉ„∞¢ñbÇ6Ü˜V∆Dw&˜Wó∞¢&˜w2Êf˜$V6Çá&˜s”ÁFˆˆ«2ÊVÊD6Üñ∆Bá&˜ríì∞¢&WGW&„∞¢–¢6ˆÁ7B6Ü˜V∆D˜V„◊v4˜VÁ«≈˜v˜&∂∆ˆtFWFñ«4WáÊFVDFVfV«BÇì∞¢6ˆÁ7Bw&˜W÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢w&˜WÊ6∆74Ê÷S“wFˆˆ¬÷w&˜Wr≤á6Ü˜V∆D˜V„Úr˜V‚s¢rFˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ¬÷w&˜W÷6ˆ∆∆6VBrì∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF◊Fˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ¬÷w&˜Wr¬srì∞¢∆WBw&˜W∂Wì“vw&˜Ws∞¢ñbáFˆˆ«2Á&VÁDV∆V÷VÁBó∞¢6ˆÁ7B7FW3‘'&íÊg&ˆ“áFˆˆ«2Á&VÁDV∆V÷VÁBÊ6Üñ∆G&V‚íÊfñ«FW"Ü6Üñ∆C”Ê6Üñ∆BÊ6∆74∆ó7Bbf6Üñ∆BÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çwv¬◊7FW◊Fˆˆ«2ríbf6Üñ∆BÊvWDGG&ñ'WFRÇvFF◊v˜&∂∆ˆr◊Fˆˆ«2rì””“srì∞¢6ˆÁ7B7FWñGÉ◊7FW2ÊñÊFWÑˆbáFˆˆ«2ì∞¢ñbá7FWñGÉ„”íw&˜W∂Wì÷7FW¢G∑7FWñGá÷∞¢–¢w&˜WÁ6WDGG&ñ'WFRÇvFF◊Fˆˆ¬÷w&˜W÷Fó66∆˜7W&R÷∂Wír∆w&˜W∂Wíì∞¢6ˆÁ7B7V÷÷'ì’˜Fˆˆ≈v˜&∂∆ˆu7V÷÷'íá&˜w2«∂∆ófS¶ó4∆ófUv˜&∂∆ˆr¬Fˆˆƒ6˜VÁCß&˜w2Ê∆VÊwFá“ì∞¢w&˜WÊñÊÊW$ÖD‘√÷∆'WGFˆ‚GóS“&'WGFˆ‚"6∆73“'Fˆˆ¬÷w&˜W÷ÜVBFˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ¬÷w&˜W÷ÜVB"&ñ÷WáÊFVC“"G∑6Ü˜V∆D˜V„ÚwG'VRs¢vf«6Rw“"ˆÊ6∆ñ6≥“%˜Fˆvv∆UFˆˆ≈v˜&∂∆ˆtw&˜WáFÜó2í#„«7‚6∆73“'Fˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ¬÷w&˜W÷ñ6ˆ‚Fr÷ñ6ˆ‚#‚Gµ˜Fˆˆƒw&˜Wñ6ˆ‚á&˜w2ó”¬˜7„„«7‚6∆73“'Fr◊7V“Fˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ¬÷w&˜W÷∆&V¬#‚G∂W62á7V÷÷'íó”¬˜7„„«7‚6∆73“'Fˆˆ¬÷6∆¬÷w&˜W÷6ÜWg&ˆ‚Fr÷6&WB#‚G∂∆íÇv6ÜWg&ˆ‚◊&ñváBr√"ó”¬˜7„„¬ˆ'WGFˆ„„∆Fób6∆73“'Fˆˆ¬÷w&˜W÷&ˆGíFˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ¬÷w&˜W÷&ˆGí#„∆Fób6∆73“'Fr◊&˜w2Fˆˆ¬◊v˜&∂∆ˆr◊Fˆˆ¬÷w&˜W◊&˜w2#„¬ˆFóc„¬ˆFócÊ∞¢6ˆÁ7B&ˆGì÷w&˜WÁVW'ï6V∆V7F˜"ÇrÁFr◊&˜w2rì∞¢&˜w2Êf˜$V6Çá&˜s”Ê&ˆGíÊVÊD6Üñ∆Bá&˜ríì∞¢Fˆˆ«2ÊVÊD6Üñ∆BÜw&˜Wì∞ß–¶gVÊ7Fñˆ‚˜7ñÊ5Fˆˆ≈v˜&∂∆ˆuFˆˆƒw&˜WÜw&˜Wó∞¢6ˆÁ7B∆ó7C’˜Fˆˆ≈v˜&∂∆ˆt∆ó7DV¬Üw&˜Wì∞¢ñbÇ∆ó7Bí&WGW&„∞¢6ˆÁ7Bó4∆ófUv˜&∂∆ˆs“Üw&˜WÊvWDGG&ñ'WFRÇvFF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜Wrì””“sr«¬w&˜WÊvWDGG&ñ'WFRÇvFF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜Wrì””“srì∞¢6ˆÁ7B7FW3‘'&íÊg&ˆ“Ü∆ó7BÁVW'ï6V∆V7F˜$∆¬Çsß66˜R‚Áv¬◊7FW◊Fˆˆ«5∂FF◊v˜&∂∆ˆr◊Fˆˆ«3“#%“ríì∞¢ñbÇ7FW2Ê∆VÊwFÇó∞¢6ˆÁ7BVÊFñÊu&˜w3’ˆFó&V7Ev˜&∂∆ˆuFˆˆ≈&˜w2Ü∆ó7Bì∞¢ñbÇVÊFñÊu&˜w2Ê∆VÊwFÇí&WGW&„∞¢6ˆÁ7BFˆˆ«3’˜Fˆˆ≈v˜&∂∆ˆuFˆˆ«4V¬Üw&˜Wì∞¢ñbÇFˆˆ«2í&WGW&„∞¢VÊFñÊu&˜w2Êf˜$V6Çá&˜s”ÁFˆˆ«2ÊVÊD6Üñ∆Bá&˜ríì∞¢˜7ñÊ5Fˆˆ≈&˜w46ˆÁFñÊW"áFˆˆ«2∆ó4∆ófUv˜&∂∆ˆrì∞¢&WGW&„∞¢–¢7FW2Êf˜$V6ÇáFˆˆ«3”Â˜7ñÊ5Fˆˆ≈&˜w46ˆÁFñÊW"áFˆˆ«2∆ó4∆ófUv˜&∂∆ˆríì∞ß–¶gVÊ7Fñˆ‚Fˆˆƒñ6ˆ‚ÜÊ÷Ró∞¢6ˆÁ7B&s’7G&ñÊrÜÊ÷W«¬rrì∞¢ñbá&rÁ7F'G5vóFÇÇv÷7ıÚró««&rÁ7F'G5vóFÇÇv÷7‚ríí&WGW&‚∆íÇw«Vrrì∞¢6ˆÁ7Bñ6ˆÁ3◊∞¢FW&÷ñÊ√¢∆íÇwFW&÷ñÊ¬rí¿¢&VEˆfñ∆S¢∆íÇvfñ∆R◊FWáBrí¿¢w&óFUˆfñ∆S¢∆íÇvfñ∆R◊V‚rí¿¢6V&6Öˆfñ∆W3¢∆íÇw6V&6Çrí¿¢vV%˜6V&6É¢∆íÇvv∆ˆ&Rrí¿¢vV%ˆWáG&7C¢∆íÇvv∆ˆ&Rrí¿¢WÜV7WFUˆ6ˆFS¢∆íÇw∆írí¿¢F6É¢∆íÇww&VÊ6Çrí¿¢÷V÷˜'ì¢∆íÇv'&ñ‚rí¿¢6∂ñ∆≈˜fñWs¢∆íÇv&ˆˆ≤÷˜V‚rí¿¢6∂ñ∆≈ˆ÷ÊvS¢∆íÇv&ˆˆ≤÷˜V‚rí¿¢FˆFÛ¢∆íÇv∆ó7B◊FˆFÚrí¿¢7&ˆÊ¶ˆ#¢∆íÇv6∆ˆ6≤rí¿¢FV∆VvFU˜F6≥¢∆íÇv&˜Brí¿¢6VÊEˆ÷W76vS¢∆íÇv÷W76vR◊7V&Rrí¿¢'&˜w6W%ˆÊfñvFS¶∆íÇvv∆ˆ&Rrí¿¢fó6ñˆÂˆÊ«ó¶S¢∆íÇvWñRrí¿¢7V&vVÁE˜&ˆw&W73¶∆íÇw6áVff∆Rrí¿¢”∞¢&WGW&‚ñ6ˆÁ5∂Ê÷U◊«∆∆íÇww&VÊ6Çrì∞ß–†¶gVÊ7Fñˆ‚˜Fˆˆƒ&u&WfñWuf«VRáf«VRó∞¢ñbáf«VS””÷ÁV∆«««f«VS””◊VÊFVfñÊVBí&WGW&‚rs∞¢ñbÑ'&íÊó4'&íáf«VRíó∞¢ñbÇf«VRÊ∆VÊwFÇí&WGW&‚uµ“s∞¢ñbáf«VRÊ∆VÊwFÉ√”2bgf«VRÊWfW'íác”Ác””÷ÁV∆««≈≤w7G&ñÊrr¬vÁV÷&W"r¬v&ˆˆ∆V‚u“ÊñÊ6«VFW2áGóVˆbbííó∞¢&WGW&‚f«VRÊ÷ác”Â7G&ñÊrábííÊ¶ˆñ‚Çr¬rì∞¢–¢&WGW&‚G∑f«VRÊ∆VÊwFá“óFV◊6∞¢–¢ñbáGóVˆbf«VS””“vˆ&¶V7Brí&WGW&‚vˆ&¶V7Bs∞¢&WGW&‚7G&ñÊráf«VRíÁ&W∆6RÇı«2≤ˆr¬rríÁG&ñ“Çì∞ß–¢ÚÚ6V7&WB˜6VÁ6óFófR÷&rwV&Bf˜"6ˆ∆∆6VBFˆˆ¬÷6&B&WfñWw2‚WÜ7B÷Ê÷RÜñFñÊp¢ÚÚ∆ˆÊR÷ó76W26÷Vƒ66RÚf&ñÁB7V∆∆ñÊw2Üî∂Wí¬66W75˜Fˆ∂V‚¬6∆ñVÁE6V7&WB¿¢ÚÚWFÜ˜&ó¶Fñˆ‚¬(
bí¬6ÚÊ˜&÷∆ó¶VB7V'7G&ñÊr6ÜV6≤'VÁ2fó'7B6Ú6V7&WB◊6ÜV@¢ÚÚ&wV÷VÁBÊ÷W2&RÊWfW"7W&f6VBñ‚FÜR«vó2◊fó6ñ&∆R6ˆ∆∆6VBÜVFW"Ç33#crí‡¶gVÊ7Fñˆ‚˜Fˆˆƒ&u&WfñWt∂Wîó4ÜñFFV‚Ü∂Wíó∞¢6ˆÁ7B≥’7G&ñÊrÜ∂Wó«¬rríÁFÙ∆˜vW$66RÇíÁ&W∆6RÇıµÊ◊£”ï“ˆr¬rrì∞¢ÚÚfW&&˜6R÷'WB÷Ê˜B◊6V7&WB&ˆFñW2vR∂VW˜WBˆbFÜR6ˆ◊7B&WfñWp¢6ˆÁ7BfW&&˜6S’≤v6ˆÁFVÁBr¬vfñ∆V6ˆÁFVÁBr¬vÊWw7G&ñÊrr¬vˆ∆G7G&ñÊrr¬wF6Çr¬wFWáBr¬v÷W76vRr¬w&ˆ◊Br¬v6ˆFRr¬w67&óBr¬v6ˆˆ∂ñW2r¬vÜVFW'2u”∞¢ñbáfW&&˜6RÊñÊ6«VFW2Ü≤íí&WGW&‚G'VS∞¢ÚÚ6V7&WB◊6ÜVB7V'7G&ñÊw2Ü6˜fW'2ïˆ∂Wíˆî∂Wí¬66W75˜Fˆ∂V‚ˆWFÖ˜Fˆ∂V‚ˆ&V&W"¿¢ÚÚ6∆ñVÁE˜6V7&WB¬77v˜&B¬7&VFVÁFñ¬¬&ófFUˆ∂Wí¬WFÜ˜&ó¶Fñˆ‚¬WF2‚ê¢&WGW&‚ÚÜñ∂Wó«Fˆ∂VÁ«6V7&WG«77v˜&G«77vG∆7&VFVÁFñ«∆WFÜ˜&ó¶FñˆÁ≈∆&WFÖ∆'∆WFÇG≈ÊWFá∆&V&W'«&ófFV∂Wó∆66W76∂Wó«6W76ñˆÊ∂Wó«6ñvÊñÊv∂Wó∆6ˆˆ∂ñRíÚÁFW7BÜ≤ê¢«¬≥””“vWFÇr«¬≥””“v∂Wír«¬≥””“wBs∞ß–¶gVÊ7Fñˆ‚ˆf˜&÷EFˆˆƒ&u&WfñWrÜ&w2ó∞¢ñbÇ&w7««GóVˆb&w2”“vˆ&¶V7Brí&WGW&‚rs∞¢6ˆÁ7B&VfW'&VC’≤wFÇr¬vfñ∆U˜FÇr¬wF&vWBr¬wGFW&‚r¬wVW'ír¬wW&¬r¬wW&«2r¬vÊ÷Rr¬w&Vbr¬v6ˆ÷÷ÊBr¬v7Fñˆ‚r¬v÷ˆFRr¬w66ÜVGV∆Rr¬wv˜&∂Fó"u”∞¢6ˆÁ7B∂Wó3’µ”∞¢f˜"Ü6ˆÁ7B∂Wíˆb&VfW'&VBó∞¢ñbÑˆ&¶V7BÁ&˜F˜GóRÊÜ4˜vÂ&˜W'GíÊ6∆¬Ü&w2∆∂Wííbb˜Fˆˆƒ&u&WfñWt∂Wîó4ÜñFFV‚Ü∂Wííí∂Wó2ÁW6ÇÜ∂Wíì∞¢–¢f˜"Ü6ˆÁ7B∂Wíˆbˆ&¶V7BÊ∂Wó2Ü&w2íó∞¢ñbÜ∂Wó2Ê∆VÊwFÉ„”2í'&V≥∞¢ñbÜ∂Wó2ÊñÊ6«VFW2Ü∂Wíó«≈˜Fˆˆƒ&u&WfñWt∂Wîó4ÜñFFV‚Ü∂Wííí6ˆÁFñÁVS∞¢∂Wó2ÁW6ÇÜ∂Wíì∞¢–¢6ˆÁ7B'G3’µ”∞¢f˜"Ü6ˆÁ7B∂Wíˆb∂Wó2ó∞¢6ˆÁ7B&s’˜Fˆˆƒ&u&WfñWuf«VRÜ&w5∂∂Wï“ì∞¢ñbÇ&rí6ˆÁFñÁVS∞¢6ˆÁ7Bf√◊&rÊ∆VÊwFÉ„ìcˆG∑&rÁ6∆ñ6RÉ√ì2óﬁ(
fß&s∞¢'G2ÁW6ÇÜG∂∂Wó”“G∑f«÷ì∞¢ñbá'G2Ê¶ˆñ‚Çr+rríÊ∆VÊwFÉ„”Sí'&V≥∞¢–¢6ˆÁ7B˜WC◊'G2Ê¶ˆñ‚Çr+rrì∞¢&WGW&‚˜WBÊ∆VÊwFÉ„ÉˆG∂˜WBÁ6∆ñ6RÉ√sróﬁ(
f¶˜WC∞ß–¶gVÊ7Fñˆ‚˜Fˆˆ≈&W7V«DˆÊT∆ñÊW"á&WfñWró∞¢ñbÇ&WfñWrí&WGW&‚rs∞¢6ˆÁ7Bfó'7C◊&WfñWrÁ7∆óBÇu∆‚ríÊfñÊBÜ√”Ê¬ÁG&ñ“Çíó«¬rs∞¢6ˆÁ7BG&ñ÷÷VC÷fó'7BÁG&ñ“Çì∞¢ñbÇG&ñ÷÷VBí&WGW&‚rs∞¢ñbáG&ñ÷÷VE≥”””“w≤rí&WGW&‚rs∞¢ñbáG&ñ÷÷VE≥”””“u≤ró∑G'ó¥•4Ù‚Á'6RáG&ñ÷÷VBì∑&WGW&‚rs∑÷6F6ÇÜRó≤Ú¢Ê˜B•4Ù‚¢˜◊–¢&WGW&‚G&ñ÷÷VBÊ∆VÊwFÉ„É˜G&ñ÷÷VBÁ6∆ñ6RÉ√srí≤~(
bsßG&ñ÷÷VC∞ß–¶gVÊ7Fñˆ‚˜Fˆˆƒ6&E&WfñWuFWáBáF2¬Fó7∆ï6ÊóWBó∞¢6ˆÁ7BWá∆ñ6óE&WfñWs’7G&ñÊráF2bgF2Á&WfñWw«¬rríÁG&ñ“Çì∞¢ñbáF2bgF2ÊFˆÊS””÷f«6RbfWá∆ñ6óE&WfñWrí&WGW&‚Wá∆ñ6óE&WfñWs∞¢6ˆÁ7B&W7V«E6˜W&6S÷Wá∆ñ6óE&WfñWw«≈7G&ñÊráF2bgF2Á6ÊóWG«¬rríÁG&ñ“Çì∞¢6ˆÁ7B&W7V«D∆ñÊS’˜Fˆˆ≈&W7V«DˆÊT∆ñÊW"á&W7V«E6˜W&6Rì∞¢ñbáF2bgF2ÊFˆÊR”÷f«6Rbg&W7V«D∆ñÊRí&WGW&‚&W7V«D∆ñÊS∞¢6ˆÁ7B&u&WfñWs’ˆf˜&÷EFˆˆƒ&u&WfñWráF2bgF2Ê&w2ì∞¢ñbÜ&u&WfñWrí&WGW&‚&u&WfñWs∞¢ñbáF2bgF2ÊFˆÊS””÷f«6Rí&WGW&‚u'VÊÊñÊrs∞¢ñbáF2bgF2Êó5ˆW'&˜"í&WGW&‚tfñ∆VBs∞¢&WGW&‚t6ˆ◊∆WFVBs∞ß–¶gVÊ7Fñˆ‚˜Fˆˆƒ6&D∆∆˜w4FWFñ¬Ü∂ñÊB¬F2ó∞¢6ˆÁ7BñÊfÙ∂ñÊG3◊∑&VC£«6V&6É£∆∆ó7C£«vV#£”∞¢ñbÜñÊfÙ∂ñÊG5∂∂ñÊE“bbáF2bgF2Êó5ˆW'&˜"íí&WGW&‚f«6S∞¢&WGW&‚G'VS∞ß–¶gVÊ7Fñˆ‚˜FˆˆƒFWFñƒ∆VD∆&V¬Ü∂ñÊBó∞¢ñbÜ∂ñÊC””“w6ÜV∆¬rí&WGW&‚u6ÜV∆¬s∞¢ñbÜ∂ñÊC””“ww&óFRrí&WGW&‚uF&vWBs∞¢&WGW&‚tñÁWBs∞ß–¶gVÊ7Fñˆ‚˜FˆˆƒFWFñƒ∆VEFWáBÜ∂ñÊB¬F2ó∞¢6ˆÁ7BF&vWC’˜Fˆˆ≈F&vWD∆&V¬áF2ì∞¢ñbÜ∂ñÊC””“w6ÜV∆¬ró∞¢ÚÚWáÊFVB6&B6Ü˜w2FÜReTƒ¬◊V«Fí÷∆ñÊR6ˆ÷÷ÊB¬Ê˜BßW7BFÜRÜVFW"w0¢ÚÚfó'7B∆ñÊRÇ3Cì#bí‚f∆¬&6≤FÚFÜRfó'7B÷∆ñÊRF&vWBñbgV∆¬ó2V◊Gí‡¢6ˆÁ7BgV∆√’˜FˆˆƒgV∆ƒ6ˆ÷÷ÊD∆&V¬áF2ì∞¢6ˆÁ7B6÷C÷gV∆«««F&vWC∞¢&WGW&‚6÷CˆBG∂6÷G÷¢rs∞¢–¢ñbÇF&vWBí&WGW&‚rs∞¢&WGW&‚F&vWC∞ß–¶gVÊ7Fñˆ‚'Vñ∆EFˆˆƒ6&BáF2ó∞¢6ˆÁ7B&˜s÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢&˜rÊ6∆74Ê÷S“wFˆˆ¬÷6&B◊&˜rs∞¢ñbÇ&˜rÊFF6WBí&˜rÊFF6WC◊∑”∞¢&˜rÊFF6WBÁFˆˆƒÊ÷S’7G&ñÊráF2bgF2ÊÊ÷W«¬wFˆˆ¬rì∞¢6ˆÁ7BFˆˆƒ∂ñÊC◊GóVˆb˜Fˆˆƒ7Fñˆ‰∂ñÊC””“vgVÊ7Fñˆ‚sı˜Fˆˆƒ7Fñˆ‰∂ñÊBáF2ì¢wVÊ∂Ê˜v‚s∞¢&˜rÊFF6WBÁFˆˆƒ∂ñÊC◊Fˆˆƒ∂ñÊC∞¢&˜rÊFF6WBÁFˆˆƒFˆÊS’7G&ñÊráF2bgF2ÊFˆÊR”÷f«6Rì∞¢&˜rÊFF6WBÁFˆˆƒW'&˜#’7G&ñÊrÇáF2bgF2Êó5ˆW'&˜"íì∞¢&˜rÊFF6WBÁFˆˆƒ7Fñˆ‰∆&V√◊GóVˆb˜Fˆˆƒ7Fñˆ‰∆&V≈FWáC””“vgVÊ7Fñˆ‚sı˜Fˆˆƒ7Fñˆ‰∆&V≈FWáBáF2ì•˜FˆˆƒFó7∆îÊ÷RáF2ì∞¢6ˆÁ7BFó66∆˜7W&T∂Wì◊GóVˆb˜FˆˆƒFó66∆˜7W&TñFVÁFóGì””“vgVÊ7Fñˆ‚sı˜FˆˆƒFó66∆˜7W&TñFVÁFóGíáF2ì¢rs∞¢ñbÜFó66∆˜7W&T∂Wíí&˜rÁ6WDGG&ñ'WFRÇvFF◊Fˆˆ¬÷Fó66∆˜7W&R÷∂Wír¬Fó66∆˜7W&T∂Wíì∞¢6ˆÁ7Bñ6ˆ„◊Fˆˆƒñ6ˆ‚áF2ÊÊ÷Rì∞¢6ˆÁ7BÜ5&tFWFñ√“áF2Á6ÊóWBó«¬áF2Ê&w2bdˆ&¶V7BÊ∂Wó2áF2Ê&w2íÊ∆VÊwFÉ„ì∞¢6ˆÁ7B∆∆˜w4FWFñ√◊GóVˆb˜Fˆˆƒ6&D∆∆˜w4FWFñ√””“vgVÊ7Fñˆ‚sı˜Fˆˆƒ6&D∆∆˜w4FWFñ¬áFˆˆƒ∂ñÊB«F2ìßG'VS∞¢6ˆÁ7BÜ4FWFñ√÷Ü5&tFWFñ¬bf∆∆˜w4FWFñ√∞¢∆WBFó7∆ï6ÊóWC“rs∞¢ñbáF2Á6ÊóWBó∞¢6ˆÁ7B3◊F2Á6ÊóWC∞¢ñbá2Ê∆VÊwFÉ√”Éó∂Fó7∆ï6ÊóWC◊3∑–¢V«6W∞¢6ˆÁ7B7WFˆfc◊2Á6∆ñ6RÉ√Éì∞¢6ˆÁ7B∆7D'&V≥‘÷FÇÊ÷ÇÜ7WFˆfbÊ∆7DñÊFWÑˆbÇr‚rí∆7WFˆfbÊ∆7DñÊFWÑˆbÇu∆‚rí∆7WFˆfbÊ∆7DñÊFWÑˆbÇs≤ríì∞¢Fó7∆ï6ÊóWC÷∆7D'&V≥„É˜2Á6∆ñ6RÉ∆∆7D'&V≤≥ì¶7WFˆfc∞¢–¢–¢6ˆÁ7BÜ4÷˜&S◊F2Á6ÊóWBbgF2Á6ÊóWBÊ∆VÊwFÉÊFó7∆ï6ÊóWBÊ∆VÊwFÉ∞¢6ˆÁ7B÷˜&T∆&V√◊F2Êó5ˆFñfcÚu6Ü˜rFñfbs¢u6Ü˜r÷˜&Rs∞¢6ˆÁ7B∆W74∆&V√◊F2Êó5ˆFñfcÚtÜñFRFñfbs¢u6Ü˜r∆W72s∞¢6ˆÁ7B'V‰ñÊFñ6F˜#◊F2ÊFˆÊS””÷f«6SÚs«7‚6∆73“'Fˆˆ¬÷6&B◊'VÊÊñÊr÷F˜B#„¬˜7„‚s¢rs∞¢6ˆÁ7Bó57V&vVÁC◊F2ÊÊ÷S””“w7V&vVÁE˜&ˆw&W72s∞¢6ˆÁ7Bó4FV∆VvFñˆ„◊F2ÊÊ÷S””“vFV∆VvFU˜F6≤s∞¢6ˆÁ7B˜V‰6∆73“rs∞¢6ˆÁ7B6&D6∆73“wFˆˆ¬÷6&Br≤áF2ÊFˆÊS””÷f«6SÚrFˆˆ¬÷6&B◊'VÊÊñÊrs¢rrí≤Üó57V&vVÁCÚrFˆˆ¬÷6&B◊7V&vVÁBs¢rrí≤ÜÜ4FWFñ√Úrs¢rFˆˆ¬÷6&B÷ÊÚ÷FWFñ¬rí∂˜V‰6∆73∞¢6ˆÁ7BÜVFW$6∆ñ6≥÷Ü4FWFñ√ÚrˆÊ6∆ñ6≥“'FÜó2Ê6∆˜6W7BÖ¬rÁFˆˆ¬÷6&E¬ríÊ6∆74∆ó7BÁFˆvv∆RÖ¬v˜VÂ¬rí"s¢rs∞¢ÚÚ6∆V‚W∆Vv7í7V&vVÁB&VfóÜW26ñÊ6RFÜR«V6ñFRñ6ˆ‚«&VGí6Ü˜w2ó@¢∆WBFó7∆îÊ÷S◊GóVˆb˜Fˆˆƒ7Fñˆ‰∆&V≈FWáC””“vgVÊ7Fñˆ‚sı˜Fˆˆƒ7Fñˆ‰∆&V≈FWáBáF2«∂∆ñ÷óC£'“ì•˜FˆˆƒFó7∆îÊ÷RáF2ì∞¢∆WBvVÊW&ñ4Ê÷S◊GóVˆb˜Fˆˆƒ7Fñˆ‰∆&V≈FWáC””“vgVÊ7Fñˆ‚sı˜Fˆˆƒ7Fñˆ‰∆&V≈FWáBáF2«∂vVÊW&ñ3ßG'VR∆∆ñ÷óC£'“ì•˜FˆˆƒFó7∆îÊ÷RáF2ì∞¢∆WB&WfñWuFWáC’˜Fˆˆƒ6&E&WfñWuFWáBáF2¬Fó7∆ï6ÊóWBì∞¢6ˆÁ7B&u&WfñWs’ˆf˜&÷EFˆˆƒ&u&WfñWráF2bgF2Ê&w2ì∞¢ñbáFˆˆƒ∂ñÊC””“w6ÜV∆¬w««&WfñWuFWáC””÷&u&WfñWw««&WfñWuFWáC””“t6ˆ◊∆WFVBw««&WfñWuFWáC””“u'VÊÊñÊrw««&WfñWuFWáC””“tfñ∆VBrí&WfñWuFWáC“rs∞¢ñbÜó57V&vVÁBí&WfñWuFWáC◊&WfñWuFWáBÁ&W∆6RÇı‚ÉÛ•«W≥cS◊Œ(k2ï«2¢˜R¬rrì∞¢6ˆÁ7BFWFñƒ∆VEFWáC÷Ü4FWFñ¬bgGóVˆb˜FˆˆƒFWFñƒ∆VEFWáC””“vgVÊ7Fñˆ‚sı˜FˆˆƒFWFñƒ∆VEFWáBáFˆˆƒ∂ñÊB«F2ì¢rs∞¢6ˆÁ7BFWFñƒ∆VD∆&V√◊GóVˆb˜FˆˆƒFWFñƒ∆VD∆&V√””“vgVÊ7Fñˆ‚sı˜FˆˆƒFWFñƒ∆VD∆&V¬áFˆˆƒ∂ñÊBì¢áFˆˆƒ∂ñÊC””“w6ÜV∆¬sÚu6ÜV∆¬s¢tñÁWBrì∞¢6ˆÁ7BFWFñƒ∆VC÷FWFñƒ∆VEFWáCˆ∆Fób6∆73“'Fˆˆ¬÷6&B÷FWFñ¬÷∆VB#„∆Fób6∆73“'Fˆˆ¬÷6&B÷FWFñ¬÷∆VB÷∆&V¬#‚G∂W62ÜFWFñƒ∆VD∆&V¬ó”¬ˆFóc„«&S‚G∂W62ÜFWFñƒ∆VEFWáBó”¬˜&S„¬ˆFócÊ¢rs∞¢6ˆÁ7B&w4VÁG&ñW3◊F2Ê&w2bdˆ&¶V7BÊ∂Wó2áF2Ê&w2íÊ∆VÊwFÉÙˆ&¶V7BÊVÁG&ñW2áF2Ê&w2ì•µ”∞¢6ˆÁ7Bfó6ñ&∆T&w3“ÜFWFñƒ∆VEFWáBbgFˆˆƒ∂ñÊC””“w6ÜV∆¬rìıµ”¶&w4VÁG&ñW3∞¢&˜rÊñÊÊW$ÖD‘√÷ ¢∆Fób6∆73“"G∂6&D6∆77“#‡¢∆Fób6∆73“'Fˆˆ¬÷6&B÷ÜVFW""G∂ÜVFW$6∆ñ6∑”‡¢G∑'V‰ñÊFñ6F˜'–¢«7‚6∆73“'Fˆˆ¬÷6&B÷ñ6ˆ‚#‚G∂ñ6ˆÁ”¬˜7„‡¢«7‚6∆73“'Fˆˆ¬÷6&B÷Ê÷R#„«7‚6∆73“'Fˆˆ¬÷6&B÷Ê÷R÷∆&V¬#‚G∂W62ÜFó7∆îÊ÷Ró”¬˜7„„«7‚6∆73“'Fˆˆ¬÷6&B÷Ê÷R÷vVÊW&ñ2#‚G∂W62ÜvVÊW&ñ4Ê÷Ró”¬˜7„„¬˜7„‡¢«7‚6∆73“'Fˆˆ¬÷6&B◊&WfñWr#‚G∂W62á&WfñWuFWáBó”¬˜7„‡¢G∂Ü4FWFñ√ˆ«7‚6∆73“'Fˆˆ¬÷6&B◊Fˆvv∆R#‚G∂∆íÇv6ÜWg&ˆ‚◊&ñváBr√"ó”¬˜7„Ê¢rw–¢¬ˆFóc‡¢G∂Ü4FWFñ√ˆ∆Fób6∆73“'Fˆˆ¬÷6&B÷FWFñ¬#‡¢G∂FWFñƒ∆VG–¢G∑fó6ñ&∆T&w2Ê∆VÊwFÉˆ∆Fób6∆73“'Fˆˆ¬÷6&B÷&w2#‚G∞¢fó6ñ&∆T&w2Ê÷ÇÖ∂≤«e“ì”Á∞¢∆WB7c’7G&ñÊrábì∞¢ñbáGóVˆb˜&VF7EFˆˆ≈F&vWD∆&V√””“vgVÊ7Fñˆ‚ró≤G'ó≤7c’˜&VF7EFˆˆ≈F&vWD∆&V¬á7bì≤÷6F6ÇÜRó∑“–¢&WGW&‚∆Fób6∆73“'Fˆˆ¬÷&r◊ó"#„«7‚6∆73“'Fˆˆ¬÷&r÷∂Wí#‚G∂W62Ü≤ó”¬˜7„„«7‚6∆73“'Fˆˆ¬÷&r◊f¬#‚G∂W62á7bó”¬˜7„„¬ˆFócÊ∞¢“íÊ¶ˆñ‚Çrrê¢”¬ˆFócÊ¢rw–¢G∂Fó7∆ï6ÊóWCˆ∆Fób6∆73“'Fˆˆ¬÷6&B◊&W7V«B#‡¢«&S‚G∑F2Êó5ˆFñfg«≈˜6ÊóWD∆ˆˆ∑4∆ñ∂TFñfbÜFó7∆ï6ÊóWBìˆ∆6ˆFR6∆73“&Fñfb÷&∆ˆ6≤"FF÷ÜñvÜ∆ñváFVC“##‚Gµˆ6ˆ∆˜$Fñfd∆ñÊW2ÜFó7∆ï6ÊóWBó”¬ˆ6ˆFSÊ¶W62ÜFó7∆ï6ÊóWBó”¬˜&S‡¢G∂Ü4÷˜&Sˆ∆'WGFˆ‚6∆73“'Fˆˆ¬÷6&B÷÷˜&R"FF÷gV∆√“"G∂W62áF2Á6ÊóWG«¬rríÁ&W∆6RÇÚ"ˆr¬rgV˜C≤ró“"FF◊6Ü˜'C“"G∂W62ÜFó7∆ï6ÊóWG«¬rríÁ&W∆6RÇÚ"ˆr¬rgV˜C≤ró“"FF÷ó2÷Fñfc“"G∑F2Êó5ˆFñfg«≈˜6ÊóWD∆ˆˆ∑4∆ñ∂TFñfbÜFó7∆ï6ÊóWBìÛ£“"FF÷÷˜&R÷∆&V√“"G∂W62Ü÷˜&T∆&V¬ó“"FF÷∆W72÷∆&V√“"G∂W62Ü∆W74∆&V¬ó“"ˆÊ6∆ñ6≥“&WfVÁBÁ7F˜&˜vFñˆ‚Çìµ˜Fˆvv∆UFˆˆƒFñfbáFÜó2í#‚G∂W62Ü÷˜&T∆&V¬ó”¬ˆ'WGFˆ„Ê¢rw–¢¬ˆFócÊ¢rw–¢¬ˆFócÊ¢rw–¢¬ˆFócÊ∞¢&˜rÂ˜F4FF“F3∞¢ÚÚGW&&∆R6∆76ñfñ6Fñˆ‚f∆w3¢˜F4FFÜ•2&˜W'GííFˆW2‰ıB7W'fófRFÜP¢ÚÚ˜WFW$ÖD‘¬ˆñÊÊW$ÖD‘¬6Ê6Ü˜B∑&W7F˜&RFÜR∆ófRFˆˆ¬÷6∆¬w&˜WW6W2ˆ‚6W76ñˆ‡¢ÚÚ7vóF6Ç˜&W7F˜&R¬vÜñ6Çv˜V∆B÷∂R˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'í&R÷6˜VÁB&W7F˜&V@¢ÚÚ÷V÷˜'í˜6∂ñ∆¬&˜w22vVÊW&ñ2Fˆˆ«2ÊB6ñ∆VÁF«íG&˜FÜR7VffóÇ‚÷ó'&˜"FÜP¢ÚÚ6∆76ñfñ6Fñˆ‚ˆÁFÚFF“¢GG&ñ'WFW26ÚóB7W'fófW26W&ñ∆ó¶Fñˆ‚‚Ç33SCBê¢ñbÖˆó4÷V÷˜'ï6fRáF2íó∑&˜rÁ6WDGG&ñ'WFRÇvFF÷÷V÷˜'í◊6fRr¬srì∑&˜rÁ&V÷˜fTGG&ñ'WFRÇvFF◊6∂ñ∆¬◊WFFRrì∑–¢V«6RñbÖˆó56∂ñ∆≈WFFRáF2íó∑&˜rÁ6WDGG&ñ'WFRÇvFF◊6∂ñ∆¬◊WFFRr¬srì∑&˜rÁ&V÷˜fTGG&ñ'WFRÇvFF÷÷V÷˜'í◊6fRrì∑–¢V«6R∑&˜rÁ&V÷˜fTGG&ñ'WFRÇvFF÷÷V÷˜'í◊6fRrì∑&˜rÁ&V÷˜fTGG&ñ'WFRÇvFF◊6∂ñ∆¬◊WFFRrì∑–¢&WGW&‚&˜s∞ß–†¶gVÊ7Fñˆ‚ˆ6ˆ∆˜$Fñfd∆ñÊW2áFWáBó∞¢ñbáGóVˆbFWáB”“w7G&ñÊrrí&WGW&‚W62Ö7G&ñÊráFWáG«¬rríì∞¢&WGW&‚W62áFWáBíÁ7∆óBÇu∆‚ríÊ÷Ü∆ñÊS”Á∞¢ñbÜ∆ñÊRÁ7F'G5vóFÇÇtríí&WGW&‚«7‚6∆73“&Fñfb÷∆ñÊRFñfb÷áVÊ≤#‚G∂∆ñÊW”¬˜7„Ê∞¢ñbÜ∆ñÊRÁ7F'G5vóFÇÇr≤ríbb∆ñÊRÁ7F'G5vóFÇÇr≤≤≤ríí&WGW&‚«7‚6∆73“&Fñfb÷∆ñÊRFñfb◊«W2#‚G∂∆ñÊW”¬˜7„Ê∞¢ñbÜ∆ñÊRÁ7F'G5vóFÇÇr“ríbb∆ñÊRÁ7F'G5vóFÇÇr“““ríí&WGW&‚«7‚6∆73“&Fñfb÷∆ñÊRFñfb÷÷ñÁW2#‚G∂∆ñÊW”¬˜7„Ê∞¢&WGW&‚«7‚6∆73“&Fñfb÷∆ñÊR#‚G∂∆ñÊW”¬˜7„Ê∞¢“íÊ¶ˆñ‚Çu∆‚rì∞ß–†¢ÚÚFWFV7BñbFWáB∆ˆˆ∑2∆ñ∂RVÊñfñVBFñfbÜÜ2áVÊ≤ÜVFW'2ÊB≤Ú“∆ñÊW2í‡¶gVÊ7Fñˆ‚˜6ÊóWD∆ˆˆ∑4∆ñ∂TFñfbáFWáBó∞¢ñbáGóVˆbFWáB”“w7G&ñÊrw««FWáBÊ∆VÊwFÉ√í&WGW&‚f«6S∞¢ñbÇı‰«2ÚÁFW7BáFWáBíí&WGW&‚f«6S∞¢6ˆÁ7B∆ñÊW3◊FWáBÁ7∆óBÇu∆‚rì∞¢∆WB«W4÷ñÁW3”∞¢f˜"Ü∆WBì”∂ì∆∆ñÊW2Ê∆VÊwFÇbfì√S∂í≤≤ó∞¢6ˆÁ7B√÷∆ñÊW5∂ï”∞¢ñbÜ¬Á7F'G5vóFÇÇr≤ró«∆¬Á7F'G5vóFÇÇr“ríí«W4÷ñÁW2≤≥∞¢–¢&WGW&‚«W4÷ñÁW3„”#∞ß–†¶gVÊ7Fñˆ‚˜Fˆvv∆UFˆˆƒFñfbÜ'F‚ó∞¢6ˆÁ7B&S÷'F‚Ê6∆˜6W7BÇrÁFˆˆ¬÷6&B◊&W7V«BrìÚÁVW'ï6V∆V7F˜"Çw&Rrì∞¢ñbÇ&Rí&WGW&„∞¢6ˆÁ7Bó4Fñfc÷'F‚ÊFF6WBÊó4Fñfc””“ss∞¢6ˆÁ7BWáÊFVC÷'F‚ÁFWáD6ˆÁFVÁC””÷'F‚ÊFF6WBÊ÷˜&T∆&V√∞¢6ˆÁ7B&s÷WáÊFVCˆ'F‚ÊFF6WBÊgV∆√¶'F‚ÊFF6WBÁ6Ü˜'C∞¢ñbÜó4Fñfbó∞¢∆WB6ˆFS◊&RÁVW'ï6V∆V7F˜"Çv6ˆFRrì∞¢ñbÇ6ˆFRó∂6ˆFS÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇv6ˆFRrì∂6ˆFRÊ6∆74Ê÷S“vFñfb÷&∆ˆ6≤s∑&RÁFWáD6ˆÁFVÁC“rs∑&RÊVÊD6Üñ∆BÜ6ˆFRì∑–¢6ˆFRÊñÊÊW$ÖD‘√’ˆ6ˆ∆˜$Fñfd∆ñÊW2á&rì∞¢÷V«6W∞¢&RÁFWáD6ˆÁFVÁC◊&s∞¢–¢'F‚ÁFWáD6ˆÁFVÁC÷WáÊFVCˆ'F‚ÊFF6WBÊ∆W74∆&V√¶'F‚ÊFF6WBÊ÷˜&T∆&V√∞ß–†¶gVÊ7Fñˆ‚˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íÜw&˜Wó∞¢ñbÇw&˜Wí&WGW&„∞¢ñbÜw&˜WÊvWDGG&ñ'WFRÇvFF◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜Wrì””“srí˜7ñÊ5Fˆˆ≈v˜&∂∆ˆuFˆˆƒw&˜WÜw&˜Wì∞¢6ˆÁ7B6&G3‘'&íÊg&ˆ“ÇÖ˜Fˆˆ≈v˜&∂∆ˆt∆ó7DV¬Üw&˜Wó«∆w&˜WíÁVW'ï6V∆V7F˜$∆¬ÇrÁFˆˆ¬÷6&B◊&˜rÁFˆˆ¬÷6&B¬ÁFˆˆ¬÷6&B◊&˜rÁF¬ríì∞¢6ˆÁ7BFˆˆƒ6˜VÁC÷6&G2Ê∆VÊwFÉ∞¢6ˆÁ7B∆&V√÷w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬◊v˜&∂∆ˆr÷∆&V¬rí«¬w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6∆¬÷w&˜W÷∆&V¬rì∞¢6ˆÁ7Bó5v˜&∂∆ˆtw&˜W“Üw&˜WÊvWDGG&ñ'WFRÇvFF◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜Wrì””“srì∞¢6ˆÁ7Bó4∆ófUv˜&∂∆ˆs“Üw&˜WÊvWDGG&ñ'WFRÇvFF÷∆ófR◊Fˆˆ¬◊v˜&∂∆ˆr÷w&˜Wrì””“sr«¬w&˜WÊvWDGG&ñ'WFRÇvFF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜Wrì””“srì∞¢6ˆÁ7BÜ5'VÊÊñÊuFˆˆ√÷6&G2Á6ˆ÷RÜ6&C”Ê6&BÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwFˆˆ¬÷6&B◊'VÊÊñÊrríì∞¢ñbÜó5v˜&∂∆ˆtw&˜Wó∞¢ñbÜÜ5'VÊÊñÊuFˆˆ¬íw&˜WÁ6WDGG&ñ'WFRÇvFF◊Fˆˆ¬◊v˜&∂∆ˆr◊'VÊÊñÊrr¬srì∞¢V«6Rw&˜WÁ&V÷˜fTGG&ñ'WFRÇvFF◊Fˆˆ¬◊v˜&∂∆ˆr◊'VÊÊñÊrrì∞¢–¢6ˆÁ7BGW&Fñˆ‰V√÷w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6∆¬÷w&˜W÷GW&Fñˆ‚rì∞¢ñbÜ∆&V¬ó∞¢ñbÜw&˜WÊvWDGG&ñ'WFRÇvFF◊'V‚÷7FófóGí÷w&˜Wrì””“sró∞¢∆&V¬ÁFWáD6ˆÁFVÁC◊Fˆˆƒ6˜VÁCı˜Fˆˆ≈v˜&∂∆ˆu7V÷÷'íÜ6&G2«∂∆ófS¶ó4∆ófUv˜&∂∆ˆr¬Fˆˆƒ6˜VÁG“ì¢u'VÊÊñÊrs∞¢÷V«6RñbÜó5v˜&∂∆ˆtw&˜Wó∞¢6ˆÁ7B&ˆ6W76VD∆&V√÷ó4∆ófUv˜&∂∆ˆp¢Úˆ7FófóGï&ˆ6W76VDV∆6VD∆&V¬Üw&˜Wê¢¢ˆ7FófóGï6WGF∆VE&ˆ6W76VD∆&V¬Üw&˜Wì∞¢∆&V¬ÁFWáD6ˆÁFVÁC◊&ˆ6W76VD∆&V«««BÇw&ˆ6W76VEˆV∆6VBr¬rrì∞¢÷V«6W∞¢6ˆÁ7B&˜w3‘'&íÊg&ˆ“Üw&˜WÁVW'ï6V∆V7F˜$∆¬ÇrÁFˆˆ¬÷6&B◊&˜rríì∞¢ÚÚ&VfW"FÜR∆ófR˜F4FF6∆76ñfñ6Fñˆ„≤f∆¬&6≤FÚFÜRGW&&∆RFF“†¢ÚÚf∆w2f˜"&˜w2&W7F˜&VBg&ˆ“‚ÖD‘¬6Ê6Ü˜BávÜñ6ÇG&˜2•2&˜W'FñW2í‡¢6ˆÁ7Bó4÷V”◊#”Âˆó4÷V÷˜'ï6fRá"Â˜F4FFó««"ÊvWDGG&ñ'WFRÇvFF÷÷V÷˜'í◊6fRrì””“ss∞¢6ˆÁ7Bó56∂ñ∆√◊#”Âˆó56∂ñ∆≈WFFRá"Â˜F4FFó««"ÊvWDGG&ñ'WFRÇvFF◊6∂ñ∆¬◊WFFRrì””“ss∞¢6ˆÁ7B÷V‘6˜VÁC◊&˜w2Êfñ«FW"Üó4÷V“íÊ∆VÊwFÉ∞¢6ˆÁ7B6∂ñ∆ƒ6˜VÁC◊&˜w2Êfñ«FW"á#”‚ó4÷V“á"íbfó56∂ñ∆¬á"ííÊ∆VÊwFÉ∞¢6ˆÁ7B˜FÜW$6˜VÁC‘÷FÇÊ÷ÇÉ¬Fˆˆƒ6˜VÁB÷÷V‘6˜VÁB◊6∂ñ∆ƒ6˜VÁBì∞¢∆WB7VffóÉ“rs∞¢ñbÜ÷V‘6˜VÁBí7VffóÇ≥÷¬G∂÷V‘6˜VÁG“G∂÷V‘6˜VÁC”””Úv÷V÷˜'ís¢v÷V÷˜&ñW2w“6fVF∞¢ñbá6∂ñ∆ƒ6˜VÁBí7VffóÇ≥÷¬G∑6∂ñ∆ƒ6˜VÁG“G∑6∂ñ∆ƒ6˜VÁC”””Úw6∂ñ∆¬s¢w6∂ñ∆«2w“WFFVF∞¢6ˆÁ7BFˆˆ«5'C÷˜FÜW$6˜VÁCˆG∂˜FÜW$6˜VÁG“Fˆˆ¬G∂˜FÜW$6˜VÁC”””Úrs¢w2w÷¢rs∞¢ñbÜw&˜WÊvWDGG&ñ'WFRÇvFF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜Wrì””“sró∞¢ñbáFˆˆ«5'Bí∆&V¬ÁFWáD6ˆÁFVÁC÷7FófóGì¢G∑Fˆˆ«5'G“G∑7Vffóá÷∞¢V«6Rñbá7VffóÇí∆&V¬ÁFWáD6ˆÁFVÁC÷7FófóGì¢G∑7VffóÇÁ6∆ñ6RÉ"ó÷∞¢V«6R∆&V¬ÁFWáD6ˆÁFVÁC“u'VÊÊñÊrs∞¢÷V«6RñbáFˆˆ«5'G««7VffóÇó∞¢∆&V¬ÁFWáD6ˆÁFVÁC◊Fˆˆ«5'Cˆ7FófóGì¢G∑Fˆˆ«5'G“G∑7Vffóá÷¶7FófóGì¢G∑7VffóÇÁ6∆ñ6RÉ"ó÷∞¢÷V«6R∆&V¬ÁFWáD6ˆÁFVÁC“t7FófóGís∞¢–¢∆&V¬Á6WDGG&ñ'WFRÇvFF◊7vVW÷∆&V¬r¬∆&V¬ÁFWáD6ˆÁFVÁBì∞¢–¢ñbÜGW&Fñˆ‰V¬ó∞¢ñbÜw&˜WÊvWDGG&ñ'WFRÇvFF◊'V‚÷7FófóGí÷w&˜Wrì””“sró∞¢6ˆÁ7BGW&FñˆÂFWáC’ˆf˜&÷EGW&‰GW&Fñˆ‚Üw&˜WÊFF6WBÁGW&‰GW&Fñˆ‚ì∞¢6ˆÁ7B∆&V√÷GW&FñˆÂFWáCÚrs•ˆ7FófóGîV∆6VD∆&V¬Üw&˜Wì∞¢GW&Fñˆ‰V¬ÁFWáD6ˆÁFVÁC÷GW&FñˆÂFWáCˆFˆÊRñ‚G∂GW&FñˆÂFWáG÷¢Ü∆&V√ˆv˜&∂ñÊrf˜"G∂∆&V«÷¢rrì∞¢GW&Fñˆ‰V¬Á7Gñ∆RÊFó7∆ì÷GW&Fñˆ‰V¬ÁFWáD6ˆÁFVÁCÚrs¢vÊˆÊRs∞¢÷V«6RñbÜw&˜WÊvWDGG&ñ'WFRÇvFF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜Wrì””“sró∞¢6ˆÁ7B7FófUFWáC’ˆ7FófóGîV∆6VD∆&V¬Üw&˜Wì∞¢ñbÜ7FófUFWáBíw&˜WÁ6WDGG&ñ'WFRÇvFF÷7FófR◊GW&‚÷V∆6VBr∆7FófUFWáBì∞¢V«6Rw&˜WÁ&V÷˜fTGG&ñ'WFRÇvFF÷7FófR◊GW&‚÷V∆6VBrì∞¢GW&Fñˆ‰V¬ÁFWáD6ˆÁFVÁC“rs∞¢GW&Fñˆ‰V¬Á7Gñ∆RÊFó7∆ì“vÊˆÊRs∞¢÷V«6RñbÜó5v˜&∂∆ˆtw&˜Wó∞¢GW&Fñˆ‰V¬ÁFWáD6ˆÁFVÁC“rs∞¢GW&Fñˆ‰V¬Á7Gñ∆RÊFó7∆ì“vÊˆÊRs∞¢÷V«6W∞¢6ˆÁ7BGW&FñˆÂFWáC’ˆf˜&÷EGW&‰GW&Fñˆ‚Üw&˜WÊFF6WBÁGW&‰GW&Fñˆ‚ì∞¢GW&Fñˆ‰V¬ÁFWáD6ˆÁFVÁC÷GW&FñˆÂFWáCˆFˆÊRñ‚G∂GW&FñˆÂFWáG÷¢rs∞¢GW&Fñˆ‰V¬Á7Gñ∆RÊFó7∆ì÷GW&FñˆÂFWáCÚrs¢vÊˆÊRs∞¢–¢–ß–†¶gVÊ7Fñˆ‚ˆ7FófóGï&ˆw&W74∆&Vƒf˜%FˆˆƒÊ÷RÜÊ÷Ró∞¢6ˆÁ7B∂Wì’7G&ñÊrÜÊ÷W«¬rríÁFÙ∆˜vW$66RÇíÁ&W∆6RÇıµÊ◊£”ï“≤ˆr¬uÚrì∞¢ñbÇ∂Wíí&WGW&‚uv˜&∂ñÊrs∞¢ñbÜ∂WíÊñÊ6«VFW2Çw6V&6Çró«∆∂WíÊñÊ6«VFW2Çvw&Wríí&WGW&‚u6V&6ÜñÊrv˜&∑76Rs∞¢ñbÜ∂WíÊñÊ6«VFW2Çw&VBró«∆∂WíÊñÊ6«VFW2ÇwfñWrró«∆∂WíÊñÊ6«VFW2Çv˜V‚ríí&WGW&‚u&VFñÊrfñ∆W2s∞¢ñbÜ∂WíÊñÊ6«VFW2Çww&óFRró«∆∂WíÊñÊ6«VFW2ÇwF6Çró«∆∂WíÊñÊ6«VFW2ÇvVFóBríí&WGW&‚uWFFñÊrfñ∆W2s∞¢ñbÜ∂WíÊñÊ6«VFW2ÇwFW&÷ñÊ¬ró«∆∂WíÊñÊ6«VFW2Çw6ÜV∆¬ró«∆∂WíÊñÊ6«VFW2Çv6ˆ÷÷ÊBró«∆∂WíÊñÊ6«VFW2Çw&ˆ6W72ríí&WGW&‚u'VÊÊñÊr6ˆ÷÷ÊBs∞¢ñbÜ∂WíÊñÊ6«VFW2ÇwvV"ró«∆∂WíÊñÊ6«VFW2ÇvfWF6Çró«∆∂WíÊñÊ6«VFW2Çv7W&¬ríí&WGW&‚t6ÜV6∂ñÊrvV"FFs∞¢ñbÜ∂WíÊñÊ6«VFW2ÇwFˆFÚró«∆∂WíÊñÊ6«VFW2Çw∆‚ríí&WGW&‚u∆ÊÊñÊrÊWáB7FW2s∞¢&WGW&‚uv˜&∂ñÊrs∞ß–†¶gVÊ7Fñˆ‚˜Fˆˆƒ6&Efó6ñ&∆TÊ÷UFWáBÜÊ÷TV¬ó∞¢ñbÇÊ÷TV¬í&WGW&‚rs∞¢6ˆÁ7B7V6ñfñ3÷Ê÷TV¬ÁVW'ï6V∆V7F˜"bfÊ÷TV¬ÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&B÷Ê÷R÷∆&V¬rì∞¢6ˆÁ7BvVÊW&ñ3÷Ê÷TV¬ÁVW'ï6V∆V7F˜"bfÊ÷TV¬ÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&B÷Ê÷R÷vVÊW&ñ2rì∞¢ñbá7V6ñfñ2bfvVÊW&ñ2ó∞¢6ˆÁ7B6&C÷Ê÷TV¬Ê6∆˜6W7BbfÊ÷TV¬Ê6∆˜6W7BÇrÁFˆˆ¬÷6&Brì∞¢6ˆÁ7B&VfW'&VC“Ü6&Bbf6&BÊ6∆74∆ó7Bbf6&BÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv˜V‚ríìˆvVÊW&ñ3ß7V6ñfñ3∞¢&WGW&‚7G&ñÊrá&VfW'&VBÁFWáD6ˆÁFVÁG«¬rríÁG&ñ“Çì∞¢–¢&WGW&‚7G&ñÊrÜÊ÷TV¬ÁFWáD6ˆÁFVÁG«¬rríÁG&ñ“Çì∞ß–†¶gVÊ7Fñˆ‚ˆ7FófóGî∆FW7EFˆˆƒÊ÷RÜw&˜Wó∞¢ñbÇw&˜Wí&WGW&‚rs∞¢6ˆÁ7B'VÊÊñÊs÷w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&BÁFˆˆ¬÷6&B◊'VÊÊñÊrÁFˆˆ¬÷6&B÷Ê÷Rrì∞¢6ˆÁ7B∆FW7C◊'VÊÊñÊr«¬'&íÊg&ˆ“Üw&˜WÁVW'ï6V∆V7F˜$∆¬ÇrÁFˆˆ¬÷6&B÷Ê÷RrííÁ˜Çì∞¢&WGW&‚˜Fˆˆƒ6&Efó6ñ&∆TÊ÷UFWáBÜ∆FW7Bì∞ß–†¶gVÊ7Fñˆ‚ˆ7FófóGïvóFñÊtFWFñ¬Üw&˜W∆∆&V√“rró∞¢6ˆÁ7BFˆˆƒÊ÷S’ˆ7FófóGî∆FW7EFˆˆƒÊ÷RÜw&˜Wì∞¢ñbáFˆˆƒÊ÷Ró∞¢6ˆÁ7B7Fñˆ„’ˆ7FófóGï&ˆw&W74∆&Vƒf˜%FˆˆƒÊ÷RáFˆˆƒÊ÷Rì∞¢ñbÜw&˜Wbfw&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&BÁFˆˆ¬÷6&B◊'VÊÊñÊrríí&WGW&‚G∂7FñˆÁ”¢G∑FˆˆƒÊ÷W“‚&W7V«G2vñ∆¬V"ÜW&RÊ∞¢&WGW&‚∆7B7FW¢G∂7FñˆÁ“ÇG∑FˆˆƒÊ÷W“ì≤Ê˜r6Üˆ˜6ñÊrFÜRÊWáB7Fñˆ‚˜"6ˆ◊˜6ñÊr&W7ˆÁ6RÊ∞¢–¢ñbÖ7G&ñÊrÜ∆&V««¬rríÁFÙ∆˜vW$66RÇíÊñÊ6«VFW2Çv÷ˆFV¬ríí&WGW&‚u&WfñWvñÊrFÜR&ˆ◊BÊB6ˆÁFWáB¬FÜV‚6Üˆ˜6ñÊrFÜRÊWáB7Fñˆ‚˜"6ˆ◊˜6ñÊrFÜR&W7ˆÁ6R‚s∞¢&WGW&‚uFÜRvVÁBó2'VÊÊñÊs≤Fˆˆ¬&W7V«G2ÊB&W7ˆÁ6RFWáBvñ∆¬V"ÜW&R‚s∞ß–†¶gVÊ7Fñˆ‚ˆ7FófóGî∆ófU&ˆw&W74∆&V¬Üw&˜Wó∞¢ñbÇw&˜W«∆w&˜WÊvWDGG&ñ'WFRÇvFF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜Wrí”“srí&WGW&‚rs∞¢6ˆÁ7BñF∆TvS’ˆ7FófóGî∆7Dˆ'6W'fVDvRÜw&˜Wì∞¢ñbÜñF∆TvR”÷ÁV∆¬bfñF∆TvS„”ìí&WGW&‚ÊÚ&V6VÁB7FófóGíf˜"Gµˆf˜&÷D7FófTV∆6VEFñ÷W"ÜñF∆TvRó÷∞¢6ˆÁ7B'VÊÊñÊs÷w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&BÁFˆˆ¬÷6&B◊'VÊÊñÊrÁFˆˆ¬÷6&B÷Ê÷Rrì∞¢6ˆÁ7B∆FW7C◊'VÊÊñÊsı˜Fˆˆƒ6&Efó6ñ&∆TÊ÷UFWáBá'VÊÊñÊrì•ˆ7FófóGî∆FW7EFˆˆƒÊ÷RÜw&˜Wì∞¢6ˆÁ7BvóFñÊs÷w&˜WÁVW'ï6V∆V7F˜"ÇrÊvVÁB÷7FófóGí◊7FGW2◊vóFñÊrÊvVÁB÷7FófóGí◊7FGW2÷∆&V¬rì∞¢ñbÜ∆FW7Bí&WGW&‚ˆ7FófóGï&ˆw&W74∆&Vƒf˜%FˆˆƒÊ÷RÜ∆FW7Bì∞¢ñbávóFñÊrbgvóFñÊrÁFWáD6ˆÁFVÁBbe7G&ñÊrávóFñÊrÁFWáD6ˆÁFVÁBíÁFÙ∆˜vW$66RÇíÊñÊ6«VFW2Çv÷ˆFV¬ríí&WGW&‚u&WfñWvñÊr&ˆ◊BÊB6ˆÁFWáBs∞¢ñbávóFñÊrbgvóFñÊrÁFWáD6ˆÁFVÁBí&WGW&‚vóFñÊrÁFWáD6ˆÁFVÁC∞¢&WGW&‚u7F'FñÊrvVÁBs∞ß–†¢ÚÚ)H)H∆ófRFˆˆ¬6&BÜV«W'2Ü6∆∆VBGW&ñÊr54R7G&V÷ñÊrí)H)H ¢ÚÚ∆ófR6&G2&RñÁ6W'FVBî‰ƒî‰RñÁ6ñFR6◊6tñÊÊW"áFvvVBvóFÇFF÷∆ófR◊FñBê¢ÚÚ6ÚFÜR7G&V÷ñÊr∆ñ˜WB÷F6ÜW2FÜR6WGF∆VB∆ñ˜WB&ˆGV6VB'í&VÊFW$÷W76vW0¢ÚÚáW6W"(i"FÜñÊ∂ñÊr(i"Fˆˆ¬6&G2(i"&W7ˆÁ6Rí‚FÜR∆Vv7í6∆ófUFˆˆƒ6&G0¢ÚÚ6ñ&∆ñÊr6ˆÁFñÊW"ó2ÊÚ∆ˆÊvW"W6VBf˜"∆6V÷VÁB(	B∂VWñÊrFÜR6&G2ñ‚FÜP¢ÚÚ÷W76vR6ˆ«V÷‚V∆ñ÷ñÊFW2FÜRfó6ñ&∆R&ßV◊"W6W'26rvÜV‚&VÊFW$÷W76vW0¢ÚÚfó&VBˆ‚FÜRFˆÊRWfVÁB‡¶gVÊ7Fñˆ‚VÊD∆ófUFˆˆƒ6&BáF2ó∞¢ÚÚwV&C¢ñvÊ˜&Rñb6W76ñˆ‚v27vóF6ÜVB‚&WfVÁG27F∆RFˆˆ¬WfVÁG2g&ˆ–¢ÚÚ&Wfñ˜W26W76ñˆ‚w254R7G&V“g&ˆ“÷ÊóV∆FñÊrFÜRÊWr6W76ñˆ‚w2DÙ“‡¢ñbÇ2Á6W76ñˆÁ«¬2Ê7FófU7G&V‘ñBí&WGW&„∞¢6ˆÁ7B˜G3÷&wV÷VÁG5≥◊««∑”∞¢ñbÜ˜G2Á6W76ñˆ‰ñBbe2Á6W76ñˆ‚Á6W76ñˆÂˆñB”÷˜G2Á6W76ñˆ‰ñBí&WGW&„∞¢ñbÜ˜G2Á7G&V‘ñBbe2Ê7FófU7G&V‘ñB”÷˜G2Á7G&V‘ñBí&WGW&„∞¢ñbáGóVˆbó4fñÊƒÁ7vW$ˆÊ«î÷ˆFS””“vgVÊ7Fñˆ‚rbfó4fñÊƒÁ7vW$ˆÊ«î÷ˆFRÇíí&WGW&„∞¢ñbÜó4∆ófTÊ6Ü˜$7FófóGï66VÊT˜vÊW"Ü˜G2Á7G&V‘ñG«≈2Ê7FófU7G&V‘ñBíó∞¢˜&VÊFW$∆ófTÊ6Ü˜$7FófóGï66VÊTf˜%7G&V“Ü˜G2Á7G&V‘ñG«≈2Ê7FófU7G&V‘ñB¬˜G2Á6W76ñˆ‰ñG«≈2Á6W76ñˆ‚Á6W76ñˆÂˆñBì∞¢&WGW&„∞¢–¢∆WBGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢ñbÇGW&‚ó∞¢GW&„’ˆ7&VFT76ó7FÁEGW&‚Çì∞¢GW&‚ÊñC“v∆ófT76ó7FÁEGW&‚s∞¢ñbÖ2Á6W76ñˆ‚íGW&‚ÊFF6WBÁ6W76ñˆ‰ñC’2Á6W76ñˆ‚Á6W76ñˆÂˆñC≤ÚÚ6VR33c`¢BÇv◊6tñÊÊW"ríÊVÊD6Üñ∆BáGW&‚ì∞¢–¢6ˆÁ7BñÊÊW#’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÇñÊÊW"í&WGW&„∞¢6ˆÁ7BFñC◊F2ÁFñG««F2ÊñG««F2ÁFˆˆ≈ˆ6∆≈ˆñG««F2ÁFˆˆ≈˜W6UˆñG««F2Ê6∆≈ˆñG«¬rs∞¢6ˆÁ7B6Üñ∆G&V„‘'&íÊg&ˆ“ÜñÊÊW"Ê6Üñ∆G&V‚ì∞¢6ˆÁ7B'W'7DñC◊F2Ê7FófóGî'W'7DñB”◊VÊFVfñÊVBbgF2Ê7FófóGî'W'7DñB”÷ÁV∆¬be7G&ñÊráF2Ê7FófóGî'W'7DñBí”“ssı7G&ñÊráF2Ê7FófóGî'W'7DñBì¢rs∞¢6ˆÁ7B6Vv÷VÁE6W◊F2Ê7FófóGï6Vv÷VÁE6W”◊VÊFVfñÊVBbgF2Ê7FófóGï6Vv÷VÁE6W”÷ÁV∆¬be7G&ñÊráF2Ê7FófóGï6Vv÷VÁE6Wí”“ssı7G&ñÊráF2Ê7FófóGï6Vv÷VÁE6Wì¢rs∞¢6ˆÁ7B6Vv÷VÁDÊ6Ü˜#◊6Vv÷VÁE6WıˆfñÊD∆ófT76ó7FÁDÊ6Ü˜$f˜%6Vv÷VÁBÜñÊÊW"¬6Vv÷VÁE6Wì¶ÁV∆√∞¢6ˆÁ7B'W'7DÊ6Ü˜#÷'W'7DñCıˆfñÊD∆FW7Efó6ñ&∆T∆ófT76ó7FÁD'î'W'7BÜñÊÊW"¬'W'7DñBì¶ÁV∆√∞¢6ˆÁ7BÊ6Ü˜#◊6Vv÷VÁDÊ6Ü˜'«∆'W'7DÊ6Ü˜'«≈ˆfñÊD∆FW7Efó6ñ&∆T∆ófT76ó7FÁBÜñÊÊW"ó«∆6Üñ∆G&V‚Êfñ«FW"ÜV√”ÊV¬Ê÷F6ÜW2Çu∂FF÷∆ófR÷76ó7FÁC“#%“rííÁ˜Çì∞¢6ˆÁ7BVffV7FófU6Vv÷VÁE6W÷Ê6Ü˜"bfÊ6Ü˜"ÊvWDGG&ñ'WFSˆÊ6Ü˜"ÊvWDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wró««6Vv÷VÁE6Wß6Vv÷VÁE6W∞¢ñbÜó5G&Á7&VÁE7G&V“Çíó∞¢6ˆÁ7BñÁ6W'EG&Á7&VÁE&˜s“á&˜rì”Á∞¢6ˆÁ7B∆ófTfˆ˜FW#÷ñÊÊW"ÁVW'ï6V∆V7F˜"Çr6∆ófU'VÂ7FGW2rì∞¢ñbÜ∆ófTfˆ˜FW"bf∆ófTfˆ˜FW"Á&VÁDV∆V÷VÁC””÷ñÊÊW"ó∞¢ñÊÊW"ÊñÁ6W'D&Vf˜&Rá&˜r∆∆ófTfˆ˜FW"ì∞¢÷V«6W∞¢ñÊÊW"ÊVÊD6Üñ∆Bá&˜rì∞¢–¢”∞¢ñbáFñBó∞¢6ˆÁ7BWÜó7FñÊs÷ñÊÊW"ÁVW'ï6V∆V7F˜"ÜÁG&Á7&VÁB÷WfVÁB◊&˜u∂FF÷∆ófR◊FñC“"G¥552ÊW66RáFñBó“%“¬ÁFˆˆ¬÷6&B◊&˜u∂FF÷∆ófR◊FñC“"G¥552ÊW66RáFñBó“%÷ì∞¢ñbÜWÜó7FñÊró∞¢6ˆÁ7B&W∆6V÷VÁEG3’˜G&Á7&VÁDWfVÁEFñ÷W7F◊6V6ˆÊG2ÜWÜó7FñÊr«∑Fˆˆƒ6∆√ßF7“ì∞¢6ˆÁ7B&W∆6V÷VÁC’ˆFV6˜&FUG&Á7&VÁDWfVÁE&˜rÜ'Vñ∆EFˆˆƒ6&BáF2í«∞¢GóS¢wFˆˆ¬r¿¢Ê÷SßF2bgF2ÊÊ÷R¿¢7FGW3•˜G&Á7&VÁEFˆˆ≈7FGW2áF2í¿¢Fˆˆƒ6∆√ßF2¿¢G3ß&W∆6V÷VÁEG2¿¢∆ófSßG'VR¿¢6Vv÷VÁE6W¶VffV7FófU6Vv÷VÁE6W¿¢'W'7DñB¿¢“ì∞¢&W∆6V÷VÁBÊFF6WBÊ∆ófUFñC◊FñC∞¢ÚÚ&W6W'fRFÜRW6W"w2WáÊB7FFR≤FWFñ¬F"7&˜72Fˆˆ¬6ˆ◊∆WFñˆ„†¢ÚÚFÜR'VÊÊñÊr&˜ró2&V'Vñ«Bg&W6Çˆ‚Fˆˆƒ6ˆ◊∆WFR¬vÜñ6Çv˜V∆B˜FÜW'vó6P¢ÚÚ6Ê‚WáÊFVB&˜r6áWBÊB&W6WBóG2gV∆¬Ù˜WGWBF"‚FÜRFWFñ¬÷÷ˆFP¢ÚÚó2&W6W'fVB&Vv&F∆W72ˆb˜V‚7FFRÜW6W"vÜÚñ6∂VB˜WGWBFÜV‡¢ÚÚ6ˆ∆∆6VB6Ü˜V∆B7Fñ∆¬vWB˜WGWBˆ‚&R÷˜V‚í‚ÖG&ñfV7FÚ‘'Vs"≤#"‚ê¢G'ó∞¢6ˆÁ7Bˆˆ∆D6&C÷WÜó7FñÊrÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&B¬ÁFÜñÊ∂ñÊr÷6&Brì∞¢6ˆÁ7BˆÊWt6&C◊&W∆6V÷VÁBÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&B¬ÁFÜñÊ∂ñÊr÷6&Brì∞¢6ˆÁ7Bˆˆ∆DFWFñ√÷WÜó7FñÊrÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&B÷FWFñ¬rì∞¢6ˆÁ7BˆÊWtFWFñ√◊&W∆6V÷VÁBÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&B÷FWFñ¬rì∞¢6ˆÁ7Bˆ÷ˆFS’ˆˆ∆DFWFñ¬beˆˆ∆DFWFñ¬ÊvWDGG&ñ'WFRÇvFF◊G&Á7&VÁB÷FWFñ¬÷÷ˆFRrì∞¢ñbÖˆÊWtFWFñ¬beˆ÷ˆFRó∞¢6ˆÁ7B˜F#’ˆÊWtFWFñ¬ÁVW'ï6V∆V7F˜"ÜÁG&Á7&VÁB÷FWFñ¬÷÷ˆFU∂FF÷÷ˆFS“"Gµˆ÷ˆFW“%÷ì∞¢ñbÖ˜F"í˜6WEG&Á7&VÁDFWFñƒ÷ˆFRÖ˜F"≈ˆ÷ˆFRì∞¢–¢ñbÖˆˆ∆D6&BbeˆÊWt6&Bbeˆˆ∆D6&BÊ6∆74∆ó7BÊ6ˆÁFñÁ2Çv˜V‚ríó∞¢˜6WEG&Á7&VÁD6&D˜V‚ÖˆÊWt6&B«G'VRì∞¢–¢÷6F6ÇÖÚó≤Ú¢Êˆ‚÷fF√¢6ˆ◊∆WFñˆ‚7Fñ∆¬&VÊFW'2¬ßW7B6ˆ∆∆6VB¢Ú–¢WÜó7FñÊrÁ&W∆6UvóFÇá&W∆6V÷VÁBì∞¢˜7ñÊ5G&Á7&VÁDWfVÁD6ˆÁG&ˆ«2áGW&‚ì∞¢ˆ÷˜fT∆ófU'VÂ7FGW5FıGW&‰VÊBÇì∞¢ñbáGóVˆb67&ˆ∆ƒñeñÊÊVC””“vgVÊ7Fñˆ‚rí67&ˆ∆ƒñeñÊÊVBÇì∞¢&WGW&„∞¢–¢–¢6ˆÁ7B&˜s’ˆFV6˜&FUG&Á7&VÁDWfVÁE&˜rÜ'Vñ∆EFˆˆƒ6&BáF2í«∞¢GóS¢wFˆˆ¬r¿¢Ê÷SßF2bgF2ÊÊ÷R¿¢7FGW3•˜G&Á7&VÁEFˆˆ≈7FGW2áF2í¿¢Fˆˆƒ6∆√ßF2¿¢∆ófSßG'VR¿¢6Vv÷VÁE6W¶VffV7FófU6Vv÷VÁE6W¿¢'W'7DñB¿¢“ì∞¢ñbáFñBí&˜rÊFF6WBÊ∆ófUFñC◊FñC∞¢ñÁ6W'EG&Á7&VÁE&˜rá&˜rì∞¢˜7ñÊ5G&Á7&VÁDWfVÁD6ˆÁG&ˆ«2áGW&‚ì∞¢ˆ÷˜fT∆ófU'VÂ7FGW5FıGW&‰VÊBÇì∞¢ñbáGóVˆb67&ˆ∆ƒñeñÊÊVC””“vgVÊ7Fñˆ‚rí67&ˆ∆ƒñeñÊÊVBÇì∞¢&WGW&„∞¢–¢ñbÜÊ6Ü˜"í˜&V÷˜fTV◊Gî∆ófUv˜&∂∆ˆu6ÜV∆«2ÜñÊÊW"ì∞¢6ˆÁ7Bw&˜W÷VÁ7W&T∆ófUv˜&∂∆ˆt6ˆÁFñÊW"ÜñÊÊW"«∞¢Ê6Ü˜"¿¢7FófóGî∂Wì•ˆ7FófóGî∂Wîf˜$∆ófUGW&‚Çí¿¢6Vv÷VÁE6W¶VffV7FófU6Vv÷VÁE6W¿¢'W'7DñB¿¢“ì∞¢6ˆÁ7B∆ó7C’ˆ∆ófUFˆˆ≈7FWV¬Üw&˜Wì∞¢ñbÇ∆ó7Bí&WGW&„∞¢ÚÚFˆˆƒ6ˆ◊∆WFR6‚&W∆6RFÜRWÜó7FñÊr∆ófR6&BvóFÇFÜR6÷RFñB‡¢ñbáFñBó∞¢6ˆÁ7BWÜó7FñÊs÷w&˜WÁVW'ï6V∆V7F˜"ÜÁFˆˆ¬÷6&B◊&˜u∂FF÷∆ófR◊FñC“"G¥552ÊW66RáFñBó“%÷ì∞¢ñbÜWÜó7FñÊró∞¢6ˆÁ7B&W∆6V÷VÁC÷'Vñ∆EFˆˆƒ6&BáF2ì∞¢&W∆6V÷VÁBÊFF6WBÊ∆ófUFñC◊FñC∞¢WÜó7FñÊrÁ&W∆6UvóFÇá&W∆6V÷VÁBì∞¢˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íÜw&˜Wì∞¢ˆ÷˜fT∆ófU'VÂ7FGW5FıGW&‰VÊBÇì∞¢ñbáGóVˆb67&ˆ∆ƒñeñÊÊVC””“vgVÊ7Fñˆ‚rí67&ˆ∆ƒñeñÊÊVBÇì∞¢&WGW&„∞¢–¢–¢6ˆÁ7Bv˜&∂∆ˆs’˜Fˆˆ≈v˜&∂∆ˆt∆ó7DV¬Üw&˜Wí«¬∆ó7C∞¢6ˆÁ7BvóFñÊs◊v˜&∂∆ˆrÁVW'ï6V∆V7F˜"ÇrÊvVÁB÷7FófóGí◊7FGW5∂FF÷7FófóGí÷WfVÁB÷ñC“'FÜñÊ∂ñÊr◊∆6VÜˆ∆FW"%“ÊvVÁB÷7FófóGí◊7FGW2÷∆&V¬rì∞¢ñbávóFñÊrbgF2ÊFˆÊS””÷f«6RívóFñÊrÁFWáD6ˆÁFVÁC“uvóFñÊrˆ‚Fˆˆ¬&W7V«Bs∞¢6ˆÁ7B&˜s÷'Vñ∆EFˆˆƒ6&BáF2ì∞¢ñbáFñBí&˜rÊFF6WBÊ∆ófUFñC◊FñC∞¢∆ó7BÊVÊD6Üñ∆Bá&˜rì∞¢˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íÜw&˜Wì∞¢ˆ÷˜fT∆ófU'VÂ7FGW5FıGW&‰VÊBÇì∞¢ñbáGóVˆb67&ˆ∆ƒñeñÊÊVC””“vgVÊ7Fñˆ‚rí67&ˆ∆ƒñeñÊÊVBÇì∞ß–†¶gVÊ7Fñˆ‚ˆfñÊD∆FW7D∆ófT76ó7FÁD'î'W'7BÜñÊÊW"¬'W'7DñBó∞¢ñbÇñÊÊW"«¬'W'7DñBí&WGW&‚ÁV∆√∞¢6ˆÁ7B6ÊFñFFW3‘'&íÊg&ˆ“ÜñÊÊW"ÁVW'ï6V∆V7F˜$∆¬Ü∂FF÷∆ófR÷76ó7FÁC“#%’∂FF÷7FófóGí÷'W'7B÷ñC“"G¥552ÊW66RÖ7G&ñÊrÜ'W'7DñBíó“%÷íê¢Êfñ«FW"ÜV√”ÊV¬Êó46ˆÊÊV7FVB”÷f«6Rì∞¢&WGW&‚6ÊFñFFW5∂6ÊFñFFW2Ê∆VÊwFÇ”“«¬ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆfñÊD∆FW7D∆ófT76ó7FÁD'ï6Vv÷VÁBÜñÊÊW"¬6Vv÷VÁE6Wó∞¢ñbÇñÊÊW"«¬6Vv÷VÁE6Wí&WGW&‚ÁV∆√∞¢6ˆÁ7B6ÊFñFFW3‘'&íÊg&ˆ“ÜñÊÊW"ÁVW'ï6V∆V7F˜$∆¬Ü∂FF÷∆ófR÷76ó7FÁC“#%’∂FF÷∆ófR◊6Vv÷VÁB◊6W“"G¥552ÊW66RÖ7G&ñÊrá6Vv÷VÁE6Wíó“%÷ííÊfñ«FW"ÜV√”ÊV¬Êó46ˆÊÊV7FVB”÷f«6Rì∞¢&WGW&‚6ÊFñFFW5∂6ÊFñFFW2Ê∆VÊwFÇ”“«¬ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆ∆ófT76ó7FÁDÜ5fó6ñ&∆UFWáBÜV¬ó∞¢ñbÇV««¬V¬Ê÷F6ÜW7«¬V¬Ê÷F6ÜW2Çu∂FF÷∆ófR÷76ó7FÁC“#%“ríí&WGW&‚f«6S∞¢6ˆÁ7B&ˆGì÷V¬ÁVW'ï6V∆V7F˜"bfV¬ÁVW'ï6V∆V7F˜"ÇrÊ◊6r÷&ˆGírì∞¢6ˆÁ7BFWáC“Ü&ˆGìˆ&ˆGíÁFWáD6ˆÁFVÁC¶V¬ÁFWáD6ˆÁFVÁBó«∆V¬ÊFF6WBbfV¬ÊFF6WBÁ&uFWáG«¬rs∞¢&WGW&‚7G&ñÊráFWáG«¬rríÁG&ñ“Çì∞ß–¶gVÊ7Fñˆ‚ˆfñÊE&Wfñ˜W5fó6ñ&∆T∆ófT76ó7FÁBÜñÊÊW"¬&Vf˜&TÊˆFRó∞¢ñbÇñÊÊW"í&WGW&‚ÁV∆√∞¢∆WBÊˆFS÷&Vf˜&TÊˆFRbf&Vf˜&TÊˆFRÁ&Wfñ˜W4V∆V÷VÁE6ñ&∆ñÊs∞¢vÜñ∆RÜÊˆFRó∞¢ñbÖˆ∆ófT76ó7FÁDÜ5fó6ñ&∆UFWáBÜÊˆFRíí&WGW&‚ÊˆFS∞¢ÊˆFS÷ÊˆFRÁ&Wfñ˜W4V∆V÷VÁE6ñ&∆ñÊs∞¢–¢&WGW&‚ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆfñÊD∆FW7Efó6ñ&∆T∆ófT76ó7FÁBÜñÊÊW"ó∞¢ñbÇñÊÊW"í&WGW&‚ÁV∆√∞¢6ˆÁ7B6ÊFñFFW3‘'&íÊg&ˆ“ÜñÊÊW"ÁVW'ï6V∆V7F˜$∆¬Çu∂FF÷∆ófR÷76ó7FÁC“#%“rííÊfñ«FW"ÜV√”ÊV¬Êó46ˆÊÊV7FVB”÷f«6Rbeˆ∆ófT76ó7FÁDÜ5fó6ñ&∆UFWáBÜV¬íì∞¢&WGW&‚6ÊFñFFW5∂6ÊFñFFW2Ê∆VÊwFÇ”“«¬ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆfñÊD∆FW7Efó6ñ&∆T∆ófT76ó7FÁD'î'W'7BÜñÊÊW"¬'W'7DñBó∞¢ñbÇñÊÊW"«¬'W'7DñBí&WGW&‚ÁV∆√∞¢6ˆÁ7B6ÊFñFFW3‘'&íÊg&ˆ“ÜñÊÊW"ÁVW'ï6V∆V7F˜$∆¬Ü∂FF÷∆ófR÷76ó7FÁC“#%’∂FF÷7FófóGí÷'W'7B÷ñC“"G¥552ÊW66RÖ7G&ñÊrÜ'W'7DñBíó“%÷íê¢Êfñ«FW"ÜV√”ÊV¬Êó46ˆÊÊV7FVB”÷f«6Rbeˆ∆ófT76ó7FÁDÜ5fó6ñ&∆UFWáBÜV¬íì∞¢&WGW&‚6ÊFñFFW5∂6ÊFñFFW2Ê∆VÊwFÇ”“«¬ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆfñÊD∆ófT76ó7FÁDÊ6Ü˜$f˜%6Vv÷VÁBÜñÊÊW"¬6Vv÷VÁE6Wó∞¢6ˆÁ7BWÜ7C’ˆfñÊD∆FW7D∆ófT76ó7FÁD'ï6Vv÷VÁBÜñÊÊW"¬6Vv÷VÁE6Wì∞¢ñbÜWÜ7Bbeˆ∆ófT76ó7FÁDÜ5fó6ñ&∆UFWáBÜWÜ7Bíí&WGW&‚WÜ7C∞¢&WGW&‚ˆfñÊE&Wfñ˜W5fó6ñ&∆T∆ófT76ó7FÁBÜñÊÊW"¬WÜ7Bí«¬ˆfñÊD∆FW7Efó6ñ&∆T∆ófT76ó7FÁBÜñÊÊW"í«¬WÜ7C∞ß–†¶gVÊ7Fñˆ‚6∆V$∆ófUFˆˆƒ6&G2Çó∞¢6ˆÁ7B&W6W'fTFˆ”“Ü&wV÷VÁG5≥“bf&wV÷VÁG5≥“Á&W6W'fTFˆ“ì∞¢ñbáGóVˆbˆ6∆V$7FófóGîV∆6VEFñ÷W#””“vgVÊ7Fñˆ‚ríˆ6∆V$7FófóGîV∆6VEFñ÷W"Çì∞¢ñbÇ&W6W'fTFˆ“ó∞¢6ˆÁ7BñÊÊW#’ˆ76ó7FÁEGW&‰&∆ˆ6∑2ÇBÇv∆ófT76ó7FÁEGW&‚ríì∞¢ñbÜñÊÊW"íñÊÊW"ÁVW'ï6V∆V7F˜$∆¬ÇrÊ∆ófR◊v˜&∂∆ˆu∂FF÷∆ófR◊v˜&∂∆ˆr◊6ÜV∆≈“¬ÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“¬ÁFˆˆ¬÷6&B◊&˜u∂FF÷∆ófR◊FñE”¶Ê˜BÇÁG&Á7&VÁB÷WfVÁB◊&˜rí≈∂FF÷Ê6Ü˜"◊66VÊR÷˜vÊW#“#%“≈∂FF÷Ê6Ü˜"◊66VÊR◊&˜s“#%“ríÊf˜$V6ÇÜV√”ÊV¬Á&V÷˜fRÇíì∞¢–¢ÚÚ&W6WBFÜRW"◊GW&‚W6W"WáÊBñÁFVÁB6ÚFÜRÊWáBGW&‚7F'G2BFÜP¢ÚÚFVfV«B6ˆ∆∆6VB7FFRÇ3#ìÇí‡¢ñbáGóVˆbˆ6∆V$∆ófT7FófóGïW6W$ñÁFVÁC””“vgVÊ7Fñˆ‚ríˆ6∆V$∆ófT7FófóGïW6W$ñÁFVÁBÇì∞¢ÚÚ∆Vv7í6∆ófUFˆˆƒ6&G26ˆÁFñÊW"6∆VÁWá6ñ&∆ñÊrFÚFÜR6WGF∆VB◊&VÊFW&V@¢ÚÚ7V'G&VRí‚«vó26∆V"ˆÜñFRóBFÚfˆñB∆V∂ñÊr7F∆Rf∆∆&6≤6ˆÁFVÁB‡¢6ˆÁ7B6ˆÁFñÊW#“BÇv∆ófUFˆˆƒ6&G2rì∞¢ñbÜ6ˆÁFñÊW"ó∂6ˆÁFñÊW"ÊñÊÊW$ÖD‘√“rs∂6ˆÁFñÊW"Á7Gñ∆RÊFó7∆ì“vÊˆÊRs∑–ß–¶gVÊ7Fñˆ‚ˆÜñFT∆ófT7FófóGîf˜$fñÊƒÁ7vW$ˆÊ«íÇó∞¢6∆V$∆ófUFˆˆƒ6&G2Çì∞¢ñbáGóVˆb&V÷˜fUFÜñÊ∂ñÊs””“vgVÊ7Fñˆ‚rí&V÷˜fUFÜñÊ∂ñÊrÇì∞¢6ˆÁ7BGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢6ˆÁ7BñÊÊW#’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÜñÊÊW"ó∞¢ñÊÊW"ÁVW'ï6V∆V7F˜$∆¬ÇrÁG&Á7&VÁB÷WfVÁB◊&˜r¬ÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊr¬Áv¬◊&V6ˆ‚¬6∆ófU'VÂ7FGW2¬Ê∆ófR◊v˜&∂∆ˆu∂FF÷∆ófR◊v˜&∂∆ˆr◊6ÜV∆≈“¬ÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“¬ÁFˆˆ¬÷6&B◊&˜u∂FF÷∆ófR◊FñE“≈∂FF÷Ê6Ü˜"◊66VÊR÷˜vÊW#“#%“≈∂FF÷Ê6Ü˜"◊66VÊR◊&˜s“#%“ríÊf˜$V6ÇÜV√”ÊV¬Á&V÷˜fRÇíì∞¢–¢6ˆÁ7B∆Vv7ïFÜñÊ∂ñÊs“BÇwFÜñÊ∂ñÊu&˜rrì∞¢ñbÜ∆Vv7ïFÜñÊ∂ñÊrí∆Vv7ïFÜñÊ∂ñÊrÁ&V÷˜fRÇì∞¢ñbáGW&‚bfñÊÊW"bbñÊÊW"Ê6Üñ∆G&V‚Ê∆VÊwFÇíGW&‚Á&V÷˜fRÇì∞ß–¶ñbáGóVˆbvñÊF˜r”“wVÊFVfñÊVBrívñÊF˜rÂˆÜñFT∆ófT7FófóGîf˜$fñÊƒÁ7vW$ˆÊ«ì’ˆÜñFT∆ófT7FófóGîf˜$fñÊƒÁ7vW$ˆÊ«ì∞¶gVÊ7Fñˆ‚˜&V÷˜fTV◊Gî∆ófUv˜&∂∆ˆu6ÜV∆«2ÜñÊÊW"ó∞¢ñbÇñÊÊW"í&WGW&„∞¢ñÊÊW"ÁVW'ï6V∆V7F˜$∆¬ÇrÊ∆ófR◊v˜&∂∆ˆu∂FF÷∆ófR◊v˜&∂∆ˆr◊6ÜV∆√“#%“¬ÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷∆ófR◊v˜&∂∆ˆr◊6ÜV∆√“#%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊v˜&∂∆ˆr◊6ÜV∆√“#%“ríÊf˜$V6ÇÜw&˜W”Á∞¢ñbÇw&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&B◊&˜r¬Áv¬◊&V6ˆ‚¬ÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊrrííw&˜WÁ&V÷˜fRÇì∞¢“ì∞ß–¶gVÊ7Fñˆ‚˜6WD∆ófUv˜&∂∆ˆuFÜñÊ∂ñÊu∆6VÜˆ∆FW"Üw&˜Wó∞¢ñbÇw&˜Wí&WGW&„∞¢w&˜WÁ6WDGG&ñ'WFRÇvFF◊&W7F'B◊FÜñÊ∂ñÊrr¬srì∞¢6ˆÁ7B∆&V√÷w&˜WÁVW'ï6V∆V7F˜"bbÄ¢w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬◊v˜&∂∆ˆr÷∆&V¬rí«¬w&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6∆¬÷w&˜W÷∆&V¬rê¢ì∞¢ñbÜ∆&V¬ó∞¢6ˆÁ7BFWáC◊GóVˆbC””“vgVÊ7Fñˆ‚s˜BÇwv˜&∂∆ˆu˜FÜñÊ∂ñÊrrì¢uFÜñÊ∂ñÊrs∞¢∆&V¬ÁFWáD6ˆÁFVÁC◊FWáC∞¢∆&V¬Á6WDGG&ñ'WFRÇvFF◊7vVW÷∆&V¬r¬FWáBì∞¢–¢6ˆÁ7BGW&Fñˆ‰V√÷w&˜WÁVW'ï6V∆V7F˜"bfw&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6∆¬÷w&˜W÷GW&Fñˆ‚rì∞¢ñbÜGW&Fñˆ‰V¬ó∞¢GW&Fñˆ‰V¬ÁFWáD6ˆÁFVÁC“rs∞¢GW&Fñˆ‰V¬Á7Gñ∆RÊFó7∆ì“vÊˆÊRs∞¢–ß–¶gVÊ7Fñˆ‚VÁ7W&T∆ófUv˜&∂∆ˆu6ÜV∆¬Çó∞¢ñbÇ2Á6W76ñˆ‚í&WGW&‚ÁV∆√∞¢ñbáGóVˆbó4fñÊƒÁ7vW$ˆÊ«î÷ˆFS””“vgVÊ7Fñˆ‚rbfó4fñÊƒÁ7vW$ˆÊ«î÷ˆFRÇíí&WGW&‚ÁV∆√∞¢6ˆÁ7B7FófU7G&V‘ñC’2Ê7FófU7G&V‘ñG«¬rs∞¢ñbÜ7FófU7G&V‘ñBbgGóVˆb˜&VÊFW$∆ófTÊ6Ü˜$7FófóGï66VÊTf˜%7G&V”””“vgVÊ7Fñˆ‚rbe˜&VÊFW$∆ófTÊ6Ü˜$7FófóGï66VÊTf˜%7G&V“Ü7FófU7G&V‘ñB¬2Á6W76ñˆ‚Á6W76ñˆÂˆñBíó∞¢ˆFVGWT∆ófU&ˆ6W76VEv˜&∂∆ˆtÊ6Ü˜'2ÇBÇv∆ófT76ó7FÁEGW&‚ríì∞¢&WGW&‚BÇv∆ófT76ó7FÁEGW&‚rì∞¢–¢ñbÜ7FófU7G&V‘ñBbfó4∆ófTÊ6Ü˜$7FófóGï66VÊT˜vÊW"Ü7FófU7G&V‘ñBíó∞¢˜&VÊFW$∆ófTÊ6Ü˜$7FófóGï66VÊTf˜%7G&V“Ü7FófU7G&V‘ñB¬2Á6W76ñˆ‚Á6W76ñˆÂˆñBì∞¢ˆFVGWT∆ófU&ˆ6W76VEv˜&∂∆ˆtÊ6Ü˜'2ÇBÇv∆ófT76ó7FÁEGW&‚ríì∞¢&WGW&‚BÇv∆ófT76ó7FÁEGW&‚rì∞¢–¢BÇvV◊Gï7FFRríÁ7Gñ∆RÊFó7∆ì“vÊˆÊRs∞¢6ˆÁ7B6ˆ◊7Ev˜&∂∆ˆs◊GóVˆbó46ˆ◊7Ev˜&∂∆ˆt÷ˆFS””“vgVÊ7Fñˆ‚rbfó46ˆ◊7Ev˜&∂∆ˆt÷ˆFRÇì∞¢ñbÇ6ˆ◊7Ev˜&∂∆ˆrbbó56ñ◊∆ñfñVEFˆˆƒ6∆∆ñÊrÇíó∞¢VÊEFÜñÊ∂ñÊrÇì∞¢&WGW&‚BÇwFÜñÊ∂ñÊu&˜rrì∞¢–¢∆WBGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢ñbÇGW&‚ó∞¢GW&„’ˆ7&VFT76ó7FÁEGW&‚Çì∞¢GW&‚ÊñC“v∆ófT76ó7FÁEGW&‚s∞¢ñbÖ2Á6W76ñˆ‚íGW&‚ÊFF6WBÁ6W76ñˆ‰ñC’2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢BÇv◊6tñÊÊW"ríÊVÊD6Üñ∆BáGW&‚ì∞¢–¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÇ&∆ˆ6∑2í&WGW&‚ÁV∆√∞¢ñbÜó5G&Á7&VÁE7G&V“Çíó∞¢ˆ÷˜fT∆ófU'VÂ7FGW5FıGW&‰VÊBÇì∞¢67&ˆ∆ƒñeñÊÊVBÇì∞¢&WGW&‚&∆ˆ6∑3∞¢–¢6ˆÁ7Bw&˜W÷VÁ7W&T7FófóGîw&˜WÜ&∆ˆ6∑2«∞¢∆ófSßG'VR¿¢6ˆ∆∆6VC¶f«6R¿¢7FófóGî∂Wì•ˆ7FófóGî∂Wîf˜$∆ófUGW&‚Çí¿¢GW&Â7F'FVDC•2Á6W76ñˆ‚be2Á6W76ñˆ‚ÁVÊFñÊu˜7F'FVEˆB¿¢“ì∞¢ñbÇw&˜Wí&WGW&‚ÁV∆√∞¢ñbÜ7FófU7G&V‘ñBó∞¢w&˜WÁ&V÷˜fTGG&ñ'WFRÇvFF◊&W7F'B◊FÜñÊ∂ñÊrrì∞¢ñbáGóVˆb˜7F'D7FófóGîV∆6VEFñ÷W#””“vgVÊ7Fñˆ‚rí˜7F'D7FófóGîV∆6VEFñ÷W"Üw&˜Wì∞¢÷V«6W∞¢˜6WD∆ófUv˜&∂∆ˆuFÜñÊ∂ñÊu∆6VÜˆ∆FW"Üw&˜Wì∞¢–¢ˆ÷˜fT∆ófU'VÂ7FGW5FıGW&‰VÊBÇì∞¢ˆFVGWT∆ófU&ˆ6W76VEv˜&∂∆ˆtÊ6Ü˜'2áGW&‚ì∞¢67&ˆ∆ƒñeñÊÊVBÇì∞¢&WGW&‚w&˜W∞ß–†¢ÚÚ)H)HVFóB≤&VvVÊW&FR)H)H †¶gVÊ7Fñˆ‚VFóD÷W76vRÜ'F‚í∞¢ñbÖ2Ê'W7íí&WGW&„∞¢6ˆÁ7B&˜r“'F‚Ê6∆˜6W7BÇu∂FF÷◊6r÷ñGÖ“rì∞¢ñbÇ&˜rí&WGW&„∞¢6ˆÁ7B◊6tñGÇ“'6TñÁBá&˜rÊFF6WBÊ◊6tñGÇ¬ì∞¢6ˆÁ7B˜&ñvñÊ≈FWáB“&˜rÊFF6WBÁ&uFWáB«¬rs∞¢6ˆÁ7B&ˆGí“&˜rÁVW'ï6V∆V7F˜"ÇrÊ◊6r÷&ˆGírì∞¢ñbÇ&ˆGí«¬&˜rÊFF6WBÊVFóFñÊrí&WGW&„∞¢&˜rÊFF6WBÊVFóFñÊr“ss∞†¢ÚÚ&W∆6R◊6r÷&ˆGívóFÇ‚VFóF&∆RFWáF&V¢6ˆÁ7BF“Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇwFWáF&Vrì∞¢FÊ6∆74Ê÷R“v◊6r÷VFóB÷&Vs∞¢FÁf«VR“˜&ñvñÊ≈FWáC∞¢&ˆGíÁ&W∆6UvóFÇáFì∞¢ÚÚ&W6ó¶RgFW"DÙ“ñÁ6W'Fñˆ‚6Ú67&ˆ∆ƒÜVñváBó26˜'&V7@¢&WVW7DÊñ÷Fñˆ‰g&÷RÇÇí”‚≤WFı&W6ó¶UFWáF&VáFì≤FÊfˆ7W2Çì≤FÁ6WE6V∆V7FñˆÂ&ÊvRáFÁf«VRÊ∆VÊwFÇ¬FÁf«VRÊ∆VÊwFÇì≤“ì∞¢FÊFDWfVÁD∆ó7FVÊW"ÇvñÁWBr¬Çí”‚WFı&W6ó¶UFWáF&VáFíì∞†¢ÚÚ7Fñˆ‚&"&V∆˜rFÜRFWáF&V¢6ˆÁ7B&"“Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢&"Ê6∆74Ê÷R“v◊6r÷VFóB÷&"s∞¢&"ÊñÊÊW$ÖD‘¬“∆'WGFˆ‚6∆73“&◊6r÷VFóB◊6VÊB#Â6VÊBVFóC¬ˆ'WGFˆ„„∆'WGFˆ‚6∆73“&◊6r÷VFóB÷6Ê6V¬#‰6Ê6V√¬ˆ'WGFˆ„Ê∞¢FÊgFW"Ü&"ì∞†¢&"ÁVW'ï6V∆V7F˜"ÇrÊ◊6r÷VFóB◊6VÊBríÊˆÊ6∆ñ6≤“7ñÊ2Çí”‚∞¢6ˆÁ7BÊWuFWáB“FÁf«VRÁG&ñ“Çì∞¢ñbÇÊWuFWáBí&WGW&„∞¢vóB7V&÷óDVFóBÜ◊6tñGÇ¬ÊWuFWáBì∞¢”∞¢&"ÁVW'ï6V∆V7F˜"ÇrÊ◊6r÷VFóB÷6Ê6V¬ríÊˆÊ6∆ñ6≤“Çí”‚6Ê6VƒVFóBá&˜r¬˜&ñvñÊ≈FWáB¬&ˆGíì∞†¢FÊFDWfVÁD∆ó7FVÊW"Çv∂WñF˜v‚r¬R”‚∞¢ñbÜRÊ∂Wì””“tVÁFW"rbbRÁ6ÜñgD∂Wíí≤ñbávñÊF˜rÂˆó4ñ÷TVÁFW"bgvñÊF˜rÂˆó4ñ÷TVÁFW"ÜRíí&WGW&„≤RÁ&WfVÁDFVfV«BÇì≤&"ÁVW'ï6V∆V7F˜"ÇrÊ◊6r÷VFóB◊6VÊBríÊ6∆ñ6≤Çì≤–¢ñbÜRÊ∂Wì””“tW66Rrí≤RÁ&WfVÁDFVfV«BÇì≤6Ê6VƒVFóBá&˜r¬˜&ñvñÊ≈FWáB¬&ˆGíì≤–¢“ì∞ß–†¶gVÊ7Fñˆ‚6Ê6VƒVFóBá&˜r¬˜&ñvñÊ≈FWáB¬˜&ñvñÊƒ&ˆGíí∞¢FV∆WFR&˜rÊFF6WBÊVFóFñÊs∞¢6ˆÁ7BF“&˜rÁVW'ï6V∆V7F˜"ÇrÊ◊6r÷VFóB÷&Vrì∞¢6ˆÁ7B&"“&˜rÁVW'ï6V∆V7F˜"ÇrÊ◊6r÷VFóB÷&"rì∞¢ñbáFíFÁ&W∆6UvóFÇÜ˜&ñvñÊƒ&ˆGíì∞¢ñbÜ&"í&"Á&V÷˜fRÇì∞ß–†¶gVÊ7Fñˆ‚WFı&W6ó¶UFWáF&VáFí∞¢FÁ7Gñ∆RÊÜVñváB“vWFÚs∞¢FÁ7Gñ∆RÊÜVñváB“÷FÇÊ÷ñ‚áFÁ67&ˆ∆ƒÜVñváB¬3í≤wÇs∞ß–†¢ÚÚ3#ÉBfˆ∆∆˜r◊W¢7V&÷óDVFóBó2&R÷VÁG&ÁBÊBFW7G'V7FófR¬ÊBÊ˜FÜñÊr7F˜V@¢ÚÚ6V6ˆÊBñÁfˆ6Fñˆ‚‚óG2ˆÊ«íwV&Bv22Ê'W7í¬vÜñ6Ç6VÊBÇíFˆW2Ê˜B6WBVÁFñ¿¢ÚÚFÜRƒ5B∆ñÊR(	BgFW"GvÚ◊V«Fí◊6V6ˆÊBvóG2ÖˆVÁ7W&T∆ƒ÷W76vW4∆ˆFVBˆ‚∆ˆÊp¢ÚÚ6W76ñˆ‚¬FÜV‚FÜRG'VÊ6FR&˜VÊB◊G&óí‚ˆ‚∆vwíñÁ7FÊ6RFÜB∆VfW22∞¢ÚÚvñÊF˜rñ‚vÜñ6ÇWfW'ígW'FÜW"6∆ñ6≤ˆ‚%6VÊBVFóB"7F'G2Ê˜FÜW"gV∆¬G'VÊ6FR‡¢ÚÚˆ'6W'fVBñ‚FÜRvñ∆C¢6WfV‚6ˆÊ7W'&VÁBı5Bˆí˜6W76ñˆ‚˜G'VÊ6FR¬R„"”Ç„W2V6Ç¿¢ÚÚg&ˆ“ˆÊRW6W"6∆ñ6∂ñÊr&WVFVF«í&V6W6RFÜRTíÜBÊ˜BñWB6∂Ê˜v∆VFvVBFÜRfó'7B‡¢Ú¢ÚÚFÜBó2Ê˜B÷W&V«ív7FVgV¬¬óBó2FF÷∆˜72Ü¶&B‚'6ˆ«WFT∂VW6˜VÁFó0¢ÚÚFV∆ñ&W&FV«í6GW&VB$Tdı$RFÜRvóG2á6VRFW7E˜7V&÷óEˆVFóEˆ6GW&W5ˆ'6ˆ«WFU¢ÚÚ&Vf˜&UˆvóBì¢B6∆ñ6≤Fñ÷Rˆˆ∆FW7DñGÜó2FÜR∆ˆFVBvñÊF˜rw2ˆfg6WBÊ@¢ÚÚ◊6tñGÜó2vñÊF˜r◊&V∆FófR¬6ÚFÜVó"7V“ó2FÜRG'VR'6ˆ«WFRñÊFWÇ‚'WBFÜP¢ÚÚfó'7B6∆¬w2ˆVÁ7W&T∆ƒ÷W76vW4∆ˆFVBÇí6WG2ˆˆ∆FW7DñGÇ“¬6Ú4T4Ù‰B6∆¿¢ÚÚVÁFW&ñÊrgFW'v&G26ˆ◊WFW2≤◊6tñGÜg&ˆ“7Fñ∆¬◊vñÊF˜r◊&V∆FófR◊6tñGÇ(	@¢ÚÚf"6÷∆∆W"∂VWˆ6˜VÁB‚G'VÊ6FñÊr#÷÷W76vR6W76ñˆ‚FÚFÜBv˜V∆BFV∆WFP¢ÚÚ÷˜7BˆbóG2Üó7F˜'í‡¢Ú¢ÚÚwV&B&R÷VÁG'íBFÜRgVÊ7Fñˆ‚óG6V∆b&FÜW"FÜ‚BFÜR6∆ñ6≤ÜÊF∆W#¢FÜRVFó@¢ÚÚ6‚«6Ú&R7V&÷óGFVBvóFÇVÁFW"áFÜR∂WñF˜v‚ÜÊF∆W"6∆ñ6∑2FÜR'WGFˆ‚í¬ÊB¢ÚÚgWGW&R6∆∆W"v˜V∆B6ñ∆VÁF«í&V˜V‚FÜRÜˆ∆R‚6∆V&VBñ‚fñÊ∆«í6Ú‚V&«ê¢ÚÚ&WGW&‚˜"Fá&˜r6ÊÊ˜BvVFvRVFóFñÊrˆfbf˜"FÜR&W7BˆbFÜRvRw2∆ñfR‡¶∆WB˜7V&÷óDVFóDñ‰f∆ñváB“f«6S∞¶7ñÊ2gVÊ7Fñˆ‚7V&÷óDVFóBÜ◊6tñGÇ¬ÊWuFWáBí∞¢ñbÇ2Á6W76ñˆ‚«¬2Ê'W7í«¬˜7V&÷óDVFóDñ‰f∆ñváBí&WGW&„∞¢˜7V&÷óDVFóDñ‰f∆ñváB“G'VS∞¢G'í∞¢6ˆÁ7BñÊóFñ≈6ñB“2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢6ˆÁ7B'6ˆ«WFT∂VW6˜VÁB“ˆˆ∆FW7DñGÇ≤◊6tñGÉ∞¢ÚÚ3Sì#C¢6GW&RFÜRFV∆ñ&W&FR◊ñ6≤6ñvÊ¬Wg&ˆÁBá&R÷ÊWGv˜&≤í¬66˜VBF¢ÚÚñÊóFñ≈6ñB(	BÊˆ‚÷FVfV«B6W76ñˆ‚÷ˆFV¬ág2&ˆfñ∆RFVfV«Bí¬vÜñ6Çó0¢ÚÚñÊfW&VÊ6R÷g&VRÊB7W'fófW2FÜRfñ∆VB6VÊBw2÷&∂W"6ˆÁ7V◊Fñˆ‚‚6VP¢ÚÚˆFV∆ñ&W&FU6W76ñˆ‰÷ˆFV≈ñ6≤‚ÁV∆¬(i"ÊÚ&R÷&“(i"6W'fW"&W6ˆ«WFñˆ‚'VÁ2‡¢6ˆÁ7B˜&V6˜fW'ïñ6≥’ˆFV∆ñ&W&FU6W76ñˆ‰÷ˆFV≈ñ6≤ÜñÊóFñ≈6ñBì∞¢ñbáGóVˆbˆVÁ7W&T∆ƒ÷W76vW4∆ˆFVC””“vgVÊ7Fñˆ‚ró∞¢vóBˆVÁ7W&T∆ƒ÷W76vW4∆ˆFVBÇì∞¢–¢ñbÇ2Á6W76ñˆ‚«¬2Á6W76ñˆ‚Á6W76ñˆÂˆñB”“ñÊóFñ≈6ñBí&WGW&„∞¢G'í∞¢vóBíÇrˆí˜6W76ñˆ‚˜G'VÊ6FRr¬∂÷WFÜˆC¢uı5Br¬&ˆGì§•4Ù‚Á7G&ñÊvñgíá∞¢6W76ñˆÂˆñC¢ñÊóFñ≈6ñB¿¢∂VWˆ6˜VÁC¢'6ˆ«WFT∂VW6˜VÁ@¢“ó“ì∞¢ÚÚ3Sì#B4îƒTÂB◊&6RwV&C¢6W76ñˆ‚7vóF6ÇGW&ñÊrFÜRG'VÊ6FRvóB◊W7BÊ˜@¢ÚÚ∆WBFÜó2&V6˜fW'í«í6W76ñˆ‚w2ñÁFVÁBáG'VÊ6FR˜&R÷&“˜6VÊBíFÚFÜP¢ÚÚÊWv«í◊fó6ñ&∆R6W76ñˆ‚‡¢ñbÇ2Á6W76ñˆ‚«¬2Á6W76ñˆ‚Á6W76ñˆÂˆñB”“ñÊóFñ≈6ñBí&WGW&„∞¢2Ê÷W76vW2“2Ê÷W76vW2Á6∆ñ6RÉ¬'6ˆ«WFT∂VW6˜VÁBì∞¢&VÊFW$÷W76vW2Çì∞¢BÇv◊6rríÁf«VR“ÊWuFWáC∞¢ÚÚ3Sì#BÑf6WB≤f6WBBì¢VFóB◊&W7V&÷óBó2&V6˜fW'í6VÊB‚&R÷&“FÜP¢ÚÚ&R÷&“FÜR6ñÊv∆R◊6Ü˜BWá∆ñ6óB◊ñ6≤÷&∂W"g&ˆ“FÜR6GW&VBÊˆ‚÷FVfV«@¢ÚÚñ6≤(	BˆÊ«íñb7Fñ∆¬6fRBfó&RFñ÷Rá6W76ñˆ‚VÊ6ÜÊvVB¬7W'&VÁB÷ˆFV¿¢ÚÚ7Fñ∆¬÷F6ÜW2¬ÊÚÊWvW"ˆÊ6ÜÊvR÷&∂W"FÚ6∆ˆ&&W"í‚6VR˜&T&’&V6˜fW'ïñ6≤‡¢˜&T&’&V6˜fW'ïñ6≤ÜñÊóFñ≈6ñB¬˜&V6˜fW'ïñ6≤ì∞¢vóB6VÊBÇì∞¢“6F6ÇÜRí≤6WE7FGW2áBÇvVFóEˆfñ∆VBrí≤RÊ÷W76vRì≤–¢“fñÊ∆«í∞¢˜7V&÷óDVFóDñ‰f∆ñváB“f«6S∞¢–ß–†¶7ñÊ2gVÊ7Fñˆ‚&VvVÊW&FU&W7ˆÁ6RÜ'F‚í∞¢ñbÇ2Á6W76ñˆ‚«¬2Ê'W7íí&WGW&„∞¢6ˆÁ7B&˜s÷'F‚bf'F‚Ê6∆˜6W7Bbf'F‚Ê6∆˜6W7BÇu∂FF÷◊6r÷ñGÖ“rì∞¢ñbÇ&˜ró&WGW&„∞¢6ˆÁ7B6∆ñ6∂VD'6ˆ«WFTñÊFWÉ’ˆˆ∆FW7DñGÇ∑'6TñÁBá&˜rÊFF6WBÊ◊6tñGÇ√ì∞¢6ˆÁ7BñÊóFñ≈6ñB“2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢ñbáGóVˆbˆVÁ7W&T∆ƒ÷W76vW4∆ˆFVC””“vgVÊ7Fñˆ‚ró∞¢vóBˆVÁ7W&T∆ƒ÷W76vW4∆ˆFVBÇì∞¢–¢ñbÇ2Á6W76ñˆ‚«¬2Á6W76ñˆ‚Á6W76ñˆÂˆñB”“ñÊóFñ≈6ñBí&WGW&„∞¢ñbÇ2Á6W76ñˆ‚Á&VvVÊW&FñˆÂ˜&Wfó6ñˆ‚ó≤6WE7FGW2áBÇw&VvVÂˆfñ∆VBríì≤&WGW&„≤–¢∆WB∆FW7D76ó7FÁDñÊFWÉ“”∞¢f˜"Ü∆WBì’2Ê÷W76vW2Ê∆VÊwFÇ”∂ì„”∂í““ó∞¢ñbÖ2Ê÷W76vW5∂ï”ÚÁ&ˆ∆S””“v76ó7FÁBró∂∆FW7D76ó7FÁDñÊFWÉ÷ì∂'&V≥∑–¢–¢ñbÜ6∆ñ6∂VD'6ˆ«WFTñÊFWÇ”÷∆FW7D76ó7FÁDñÊFWÇó∞¢6WE7FGW2áBÇw&VvVÂˆfñ∆VBríì∞¢&WGW&„∞¢–¢G'í∞¢vóB7F'E&VvVÊW&Fñˆ‚ÜñÊóFñ≈6ñB¬2Á6W76ñˆ‚Á&VvVÊW&FñˆÂ˜&Wfó6ñˆ‚ì∞¢“6F6ÇÜRí≤6WE7FGW2áBÇw&VvVÂˆfñ∆VBrí≤RÊ÷W76vRì≤–ß–†¢ÚÚ˜7E&ˆ6W75&VÊFW&VD÷W76vW2Çí'VÁ2ˆÊRg&÷ReDU"FÜR&VÊFW"≤•267&ˆ∆¿¢ÚÚ&W7F˜&RÜóBó266ÜVGV∆VBfñ&WVW7DÊñ÷Fñˆ‰g&÷Rí‚óBW&f˜&◊27ñÁFÄ¢ÚÚÜñvÜ∆ñváFñÊr¬ñÊ∆ñÊRFñfbˆ77b˜FbˆáF÷¬ˆWÜ6∆ñG&ráñG&Fñˆ‚¬÷W&÷ñBˆ∂FWÄ¢ÚÚ&VÊFW&ñÊr(	B∆¬ˆbvÜñ6Ç6‚4Ñ‰tRFÜRÜVñváBˆb&˜w2&˜fRFÜRfñWw˜'B‡¢Ú¢ÚÚˆ‚÷ˆ&ñ∆RFÜR67&ˆ∆∆W"&W7G2B˜fW&f∆˜r÷Ê6Ü˜#¶WFÚ¬6ÚÁí&˜fR◊fñWw˜'@¢ÚÚÜVñváB6ÜÊvRñ‚FÜó2˜7B◊&VÊFW"g&÷R÷∂W2FÜR'&˜w6W"w2ÊFófRÊ6Ü˜ ¢ÚÚVÊvñÊR6ˆ◊VÁ6FR67&ˆ∆≈F˜4T4Ù‰BFñ÷R(	BgFW"FÜR•2&W7F˜&R«&VGê¢ÚÚ6WGF∆VBFÜR&VFW"w2˜6óFñˆ‚(	BñÊ∂ñÊrFÜV“FÚ‚VÁ&V∆FVBGW&‚Ç.[ËYπÓZJ~ã{2"í‡¢ÚÚFÜR7ñÊ6á&ˆÊ˜W2ˆfóÑ÷ˆ&ñ∆U67&ˆ∆ƒ¶Ê≤Ú˜7W&W74'&˜w6W$˜fW&f∆˜tÊ6Ü˜"wV&G0¢ÚÚˆÊ«í6˜fW"FÜR&VÊFW"g&÷RóG6V∆c≤FÜWíÜfR«&VGí&V∆V6VB'íFÜRFñ÷P¢ÚÚFÜó2$bfó&W2‚w&FÜR˜7B◊&ˆ6W72ÜÊBFÜR÷VFñ◊&Vf∆˜rg&÷R&ñváBgFW ¢ÚÚóBíñ‚FÜR6÷R7W&W76ñˆ‚6ÚFÜR'&˜w6W"∆ñW"6ÊÊ˜B&R÷Ê6Ü˜"GW&ñÊrFÜP¢ÚÚ7ñÊ26WGF∆RvñÊF˜r‚FW6∑F˜&W7G2BÊˆÊV¬6ÚFÜó2ó2ÊÚ÷˜FÜW&R‡¶gVÊ7Fñˆ‚˜˜7E&ˆ6W75vóFÑÊ6Ü˜%7W&W76ñˆ‚Ü6ˆÁFñÊW"ó∞¢6ˆÁ7B67&ˆ∆∆W#“BÇv÷W76vW2rì∞¢6ˆÁ7B&V∆V6S“á67&ˆ∆∆W"bgGóVˆb˜7W&W74'&˜w6W$˜fW&f∆˜tÊ6Ü˜#””“vgVÊ7Fñˆ‚rê¢Ú˜7W&W74'&˜w6W$˜fW&f∆˜tÊ6Ü˜"á67&ˆ∆∆W"í¢ÁV∆√∞¢G'ó∞¢˜7E&ˆ6W75&VÊFW&VD÷W76vW2Ü6ˆÁFñÊW"ì∞¢÷fñÊ∆«ó∞¢ÚÚÜˆ∆B7W&W76ñˆ‚7&˜72Ù‰R÷˜&Rg&÷R6Ú∆FR÷VFñˆ∆ñ˜WB&Vf∆˜p¢ÚÚÜñ÷vRFV6ˆFR¬∂FWÇˆ÷W&÷ñB÷V7W&Rí6ÊÊ˜B&R÷Ê6Ü˜"VóFÜW"¬FÜV‚∆W@¢ÚÚ˜7W&W74'&˜w6W$˜fW&f∆˜tÊ6Ü˜"w2˜v‚$b÷FVfW'&VB&W7F˜&R'V‚‡¢ñbá&V∆V6Ró∞¢ñbáGóVˆb&WVW7DÊñ÷Fñˆ‰g&÷S””“vgVÊ7Fñˆ‚rí&WVW7DÊñ÷Fñˆ‰g&÷Rá&V∆V6Rì∞¢V«6R&V∆V6RÇì∞¢–¢–ß–¶gVÊ7Fñˆ‚˜7E&ˆ6W75&VÊFW&VD÷W76vW2Ü6ˆÁFñÊW"í∞¢ÜñvÜ∆ñváD6ˆFRÜ6ˆÁFñÊW"ì∞¢FD6˜î'WGFˆÁ2Ü6ˆÁFñÊW"ì∞¢∆ˆDFñfdñÊ∆ñÊRÜ6ˆÁFñÊW"ì∞¢∆ˆD77dñÊ∆ñÊRÜ6ˆÁFñÊW"ì∞¢∆ˆDWÜ6∆ñG&tñÊ∆ñÊRÜ6ˆÁFñÊW"ì∞¢∆ˆEFdñÊ∆ñÊRÜ6ˆÁFñÊW"ì∞¢∆ˆDáF÷ƒñÊ∆ñÊRÜ6ˆÁFñÊW"ì∞¢&VÊFW$÷W&÷ñD&∆ˆ6∑2Ü6ˆÁFñÊW"ì∞¢&VÊFW$∂FWÑ&∆ˆ6∑2Ü6ˆÁFñÊW"ì∞¢ñÊóEG&VUfñWw2Ü6ˆÁFñÊW"ì∞ß–†¶gVÊ7Fñˆ‚ÜñvÜ∆ñváD6ˆFRÜ6ˆÁFñÊW"í∞¢ÚÚ«í&ó6“Êß27ñÁFÇÜñvÜ∆ñváFñÊrˆÊ«íFÚ¶ÊWr¢6ˆFR&∆ˆ6∑2‡¢ÚÚ&Wfñ˜W6«íWfW'í&VÊFW$÷W76vW2Çí6∆∆VB&ó6“ÊÜñvÜ∆ñváD∆≈VÊFW"ÇívÜñ6Ä¢ÚÚ&R◊66ÊÊVBÊB&R÷ÜñvÜ∆ñváFVBWfW'í«&S‚ñ‚FÜR6ˆÁFñÊW"(	BWáVÁ6ófRñ‡¢ÚÚ∆ˆÊr6W76ñˆÁ2vóFÇF˜¶VÁ2ˆb6ˆFR&∆ˆ6∑2‚Ê˜rvRˆÊ«íF˜V6Ç&∆ˆ6∑2FÜ@¢ÚÚFˆ‚wB«&VGíÜfRFÜRFF÷ÜñvÜ∆ñváFVB÷&∂W"‡¢ñbáGóVˆb&ó6“””“wVÊFVfñÊVBrí&WGW&„∞¢6ˆÁ7BV¬“6ˆÁFñÊW"«¬BÇv◊6tñÊÊW"rì∞¢ñbÇV¬í&WGW&„∞¢ÚÚ&VfW"W"÷V∆V÷VÁBÜñvÜ∆ñváBÜfˆñG2FÜRgV∆¬DÙ“v∆≤ˆbÜñvÜ∆ñváD∆≈VÊFW"ê¢6ˆÁ7B&∆ˆ6∑2“V¬ÁVW'ï6V∆V7F˜$∆¬Çw&R6ˆFS¶Ê˜BÖ∂FF÷ÜñvÜ∆ñváFVE“írì∞¢ñbÜ&∆ˆ6∑2Ê∆VÊwFÇ””“í&WGW&„∞¢f˜"Ü∆WBí“≤í¬&∆ˆ6∑2Ê∆VÊwFÉ≤í≤≤ó∞¢6ˆÁ7B&∆ˆ6≤“&∆ˆ6∑5∂ï”∞¢ñbáGóVˆb&ó6“ÊÜñvÜ∆ñváDV∆V÷VÁB””“vgVÊ7Fñˆ‚rí&ó6“ÊÜñvÜ∆ñváDV∆V÷VÁBÜ&∆ˆ6≤ì∞¢&∆ˆ6≤ÊFF6WBÊÜñvÜ∆ñváFVB“ss∞¢–ß–†¢ÚÚ∆ßí∆ˆBß2◊ñ÷¬f˜"î‘¬G&VRfñWr7W˜'@¶∆WBˆß7ñ÷ƒ∆ˆFñÊs÷f«6S∞¶gVÊ7Fñˆ‚ˆ∆ˆDß7ñ÷≈FÜV‚Ü6"ó∞¢ñbáGóVˆbß7ñ÷¬”“wVÊFVfñÊVBró≤6"Çì≤&WGW&„≤–¢ñbÖˆß7ñ÷ƒ∆ˆFñÊró≤6WEFñ÷V˜WBÇÇì”Âˆ∆ˆDß7ñ÷≈FÜV‚Ü6"í√ì≤&WGW&„≤–¢ˆß7ñ÷ƒ∆ˆFñÊs◊G'VS∞¢6ˆÁ7B3÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw67&óBrì∞¢2Á7&3“w7FFñ2˜fVÊF˜"ˆß2◊ñ÷¬ÛB„„ˆß2◊ñ÷¬Ê÷ñ‚Êß2s∞¢2ÊñÁFVw&óGì“w6Ü3ÉB“∑Üî„eCwóg'óT¶‘St”ïÉwïóCVTF"µßwwd§ˆBÛF&S$6ñSRÙóUeÜu"ı3ñGRs∞¢2Ê7&˜74˜&ñvñ„“vÊˆÁñ÷˜W2s∞¢2ÊˆÊ∆ˆC“Çì”Á≤ˆß7ñ÷ƒ∆ˆFñÊs÷f«6S≤6"Çì≤”∞¢2ÊˆÊW'&˜#“Çì”Á≤ˆß7ñ÷ƒ∆ˆFñÊs÷f«6S≤”≤ÚÚ4D‚&∆ˆ6∂VB¬f∆¬&6≤FÚ&p¢Fˆ7V÷VÁBÊÜVBÊVÊD6Üñ∆Bá2ì∞ß–†¢ÚÚ)H)H•4Ù‚ıî‘¬7G'V7GW&VB6ˆFR÷&∆ˆ6≤FVfV«B◊fñWr6ˆÊfñwW&Fñˆ‚Ç3CÉBí)H)H ¢ÚÚ&VBFÜRW6W"w26ˆÊfñwW&VBFVfV«B◊fñWr÷ˆFRf˜"f∆ñB•4Ù‚ıî‘¬fVÊ6V@¢ÚÚ&∆ˆ6∑2‚f∆«2&6≤FÚvWFÚrf˜"Áí÷ó76ñÊrˆñÁf∆ñBf«VR6ÚFÜR&VÊFW&W ¢ÚÚ7Fó26fR&Vf˜&R6WGFñÊw2∆ˆBÊBñ‚Êˆ‚÷'&˜w6W"FW7B6ˆÁFWáG2‡¶gVÊ7Fñˆ‚˜7G'V7GW&VD6ˆFT÷ˆFRÇó∞¢6ˆÁ7B”“áGóVˆbvñÊF˜r”“wVÊFVfñÊVBrì˜vñÊF˜rÂ˜7G'V7GW&VD6ˆFTFVfV«EfñWsßVÊFVfñÊVC∞¢&WGW&‚Ü”””“vˆ‚w«∆”””“vˆfbw«∆”””“vWFÚrìˆ”¢vWFÚs∞ß–¢ÚÚ&VBFÜR6ˆÊfñwW&VBvWFÚr÷÷ˆFR∆ñÊRFá&W6Üˆ∆B¬6∆◊VBFÚ6ÊRñÁFVvW ¢ÚÚ&ÊvR‚ñÁf∆ñBˆ÷ó76ñÊrf«VW2f∆¬&6≤FÚáFÜR˜&ñvñÊ¬Ü&F6ˆFVBf«VRí‡¶gVÊ7Fñˆ‚˜7G'V7GW&VD6ˆFUFá&W6Üˆ∆BÇó∞¢6ˆÁ7B&s“áGóVˆbvñÊF˜r”“wVÊFVfñÊVBrì˜vñÊF˜rÂ˜7G'V7GW&VD6ˆFTWFıG&VT∆ñÊW3ßVÊFVfñÊVC∞¢6ˆÁ7B„◊'6TñÁBá&r√ì∞¢&WGW&‚ÑÁV÷&W"Êó4fñÊóFRÜ‚íbf„„”bf„√”ìˆ„£∞ß–¢ÚÚW&RFV6ó6ñˆ‚ÜV«W#¢6Ü˜V∆B7G'V7GW&VB&∆ˆ6≤FVfV«BFÚG&VRfñWs¢ÚÚf7F˜&VB˜WB6ÚFÜRÜ÷ˆFR¬Fá&W6Üˆ∆B¬∆ñÊT6˜VÁBí6ˆÁG&7Bó2VÊóB◊FW7F&∆R‡¢ÚÚ÷ˆFRvˆ‚r”‚«vó2G&VP¢ÚÚ÷ˆFRvˆfbr”‚«vó2&p¢ÚÚ÷ˆFRvWFÚr”‚G&VRˆÊ«ívÜV‚∆ñÊT6˜VÁB„“Fá&W6Üˆ∆BáFá&W6Üˆ∆B6ÊóFó¶VB¿¢ÚÚf∆∆&6≤ê¶gVÊ7Fñˆ‚˜7G'V7GW&VD6ˆFU6Ü˜uG&VRÜ÷ˆFR«Fá&W6Üˆ∆B∆∆ñÊT6˜VÁBó∞¢ñbÜ÷ˆFS””“vˆ‚rí&WGW&‚G'VS∞¢ñbÜ÷ˆFS””“vˆfbrí&WGW&‚f«6S∞¢6ˆÁ7BFÉ“ÑÁV÷&W"Êó4fñÊóFRáFá&W6Üˆ∆BíbgFá&W6Üˆ∆C„”bgFá&W6Üˆ∆C√”ì˜Fá&W6Üˆ∆C£∞¢&WGW&‚∆ñÊT6˜VÁC„◊FÉ∞ß–†¶gVÊ7Fñˆ‚ñÊóEG&VUfñWw2Ü6ˆÁFñÊW"ó∞¢6ˆÁ7B&ˆ˜C÷6ˆÁFñÊW'«∆Fˆ7V÷VÁC∞¢&ˆ˜BÁVW'ï6V∆V7F˜$∆¬ÇrÊ6ˆFR◊G&VR◊w&¶Ê˜BÖ∂FF◊G&VR÷ñÊóE“íríÊf˜$V6Çáw&”Á∞¢6ˆÁ7B&uFWáC◊w&ÊFF6WBÁ&s∞¢6ˆÁ7B∆Ês◊w&ÊFF6WBÊ∆Ês∞¢∆WB'6VC÷ÁV∆√∞¢∆WB'6Tfñ∆VC÷f«6S∞¢ÚÚG'í•4Ù‚'6P¢G'ó≤'6VC‘•4Ù‚Á'6Rá&uFWáBì≤÷6F6ÇÜRó≤'6Tfñ∆VC“Ü∆Ês””“vß6ˆ‚rì≤–¢ÚÚî‘√¢∆ßí÷∆ˆBß2◊ñ÷¬ñbÊVVFV@¢ñbÇ'6VBbb∆Ês””“wñ÷¬ró∞¢ñbáGóVˆbß7ñ÷¬”“wVÊFVfñÊVBró∞¢G'ó≤'6VC÷ß7ñ÷¬Ê∆ˆBá&uFWáBì≤÷6F6ÇÜRó≤'6Tfñ∆VC◊G'VS≤–¢÷V«6W∞¢ÚÚFVfW#¢&V÷˜fRñÊóB÷&∂W"6ÚvR&WG'ígFW"∆ˆB‡¢ÚÚÊ˜FS¢ñb4D‚∆ˆBfñ«2¬2ÊˆÊW'&˜"FˆW2‰ıB6∆¬&6≤(	@¢ÚÚFÜRw&7Fó2V‚÷ñÊóFñ∆ó6VBá&rfñWrˆÊ«íí¬vÜñ6Çó26fR‡¢w&Á&V÷˜fTGG&ñ'WFRÇvFF◊G&VR÷ñÊóBrì∞¢ˆ∆ˆDß7ñ÷≈FÜV‚ÜñÊóEG&VUfñWw2ì∞¢&WGW&„∞¢–¢–¢ÚÚ÷&≤2ñÊóFñ∆ó6VBˆÊ«ígFW"vRwfR6ˆ÷÷óGFVBFÚ&VÊFW"FV6ó6ñˆ‡¢w&Á6WDGG&ñ'WFRÇvFF◊G&VR÷ñÊóBr¬srì∞¢ñbÇ'6VB«¬GóVˆb'6VB”“vˆ&¶V7Bró∞¢ÚÚÊÚG&VRfñWrf˜"Êˆ‚÷ˆ&¶V7Bf«VW2˜"VÁ'6V&∆R6ˆÁFVÁB‚ƒƒ◊2ˆgFV‡¢ÚÚV÷óB•4Ù‚g&v÷VÁG2Ü&&R&∂Wí#¢'f¬"∆ñÊR¬6ÊóWG2vóFÇ‚‚‚¬WF2‚ê¢ÚÚFÜB∆VvóFñ÷FV«ífñ¬•4Ù‚Á'6S≤7W&f6ñÊr''6Rfñ∆VB"Ê˜FRf˜ ¢ÚÚFÜ˜6Rv2W&RÊˆó6R‚FÜR&∆ˆ6≤7Fñ∆¬&VÊFW'227ñÁFÇ÷ÜñvÜ∆ñváFVB&r¿¢ÚÚ6ÚßW7Bf∆¬Fá&˜VvÇ6ñ∆VÁF«í‚á'6Tfñ∆VBó2&WFñÊVBf˜"6∆&óGí‚ê¢fˆñB'6Tfñ∆VC∞¢&WGW&„≤ÚÚ∆VfR2&rfñWp¢–¢6ˆÁ7B∆ñÊT6˜VÁC◊&uFWáBÁ7∆óBÇu∆‚ríÊ∆VÊwFÉ∞¢ÚÚFVfV«BfñWró2W6W"÷6ˆÊfñwW&&∆RÇ3CÉBfˆ∆∆˜r◊Wí‚vˆ‚r”‚«vó2G&VR¿¢ÚÚvˆfbr”‚«vó2&r¬vWFÚr”‚G&VRˆÊ«ívÜV‚FÜR&∆ˆ6≤ó2„“FÜP¢ÚÚ6ˆÊfñwW&VB∆ñÊRFá&W6Üˆ∆BÜFVfV«B¬&W6W'fñÊrFÜR˜&ñvñÊ¬&VÜfñ˜"í‡¢ÚÚFÜRW"÷&∆ˆ6≤&rıG&VRFˆvv∆R&V∆˜r«vó2&V÷ñÁ2fñ∆&∆R&Vv&F∆W72‡¢6ˆÁ7B6Ü˜uG&VS’˜7G'V7GW&VD6ˆFU6Ü˜uG&VRÖ˜7G'V7GW&VD6ˆFT÷ˆFRÇí≈˜7G'V7GW&VD6ˆFUFá&W6Üˆ∆BÇí∆∆ñÊT6˜VÁBì∞¢ÚÚ'Vñ∆BG&VRDÙ–¢6ˆÁ7BG&VTFóc÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢G&VTFóbÊ6∆74Ê÷S“wG&VR◊fñWrr≤á6Ü˜uG&VSÚrs¢rG&VR÷ÜñFFV‚rì∞¢G&VTFóbÊVÊD6Üñ∆BÖˆ'Vñ∆EG&VTDÙ“á'6VB¬íì∞¢ÚÚFˆvv∆R'WGFˆ‚ñ‚ÜVFW ¢6ˆÁ7BÜVFW#◊w&ÁVW'ï6V∆V7F˜"ÇrÁ&R÷ÜVFW"rì∞¢ñbÜÜVFW"ó∞¢6ˆÁ7BFˆvv∆S÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇv'WGFˆ‚rì∞¢Fˆvv∆RÊ6∆74Ê÷S“wG&VR◊Fˆvv∆R÷'F‚s∞¢Fˆvv∆RÁFWáD6ˆÁFVÁC◊6Ü˜uG&VS˜BÇw&u˜fñWrrìßBÇwG&VU˜fñWrrì∞¢Fˆvv∆RÊˆÊ6∆ñ6≥“ÜRì”Á∞¢RÁ7F˜&˜vFñˆ‚Çì∞¢6ˆÁ7Bó5G&VTÜñFFV„◊G&VTFóbÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwG&VR÷ÜñFFV‚rì∞¢G&VTFóbÊ6∆74∆ó7BÁFˆvv∆RÇwG&VR÷ÜñFFV‚r¬ó5G&VTÜñFFV‚ì∞¢6ˆÁ7B&u&S◊w&ÁVW'ï6V∆V7F˜"ÇrÁG&VR◊&r◊fñWrrì∞¢ñbá&u&Rí&u&RÁ7Gñ∆RÊFó7∆ì÷ó5G&VTÜñFFV„ÚvÊˆÊRs¢rs∞¢Fˆvv∆RÁFWáD6ˆÁFVÁC÷ó5G&VTÜñFFV„˜BÇw&u˜fñWrrìßBÇwG&VU˜fñWrrì∞¢”∞¢ÜVFW"Á7Gñ∆RÊFó7∆ì“vf∆WÇs∞¢ÜVFW"Á7Gñ∆RÊßW7Fñgî6ˆÁFVÁC“w76R÷&WGvVV‚s∞¢ÜVFW"Á7Gñ∆RÊ∆ñv‰óFV◊3“v6VÁFW"s∞¢ÜVFW"ÊVÊD6Üñ∆BáFˆvv∆Rì∞¢–¢ñbÇ6Ü˜uG&VRó∞¢6ˆÁ7B&u&S◊w&ÁVW'ï6V∆V7F˜"ÇrÁG&VR◊&r◊fñWrrì∞¢ñbá&u&Rí&u&RÁ7Gñ∆RÊFó7∆ì“rs∞¢“V«6R∞¢6ˆÁ7B&u&S◊w&ÁVW'ï6V∆V7F˜"ÇrÁG&VR◊&r◊fñWrrì∞¢ñbá&u&Rí&u&RÁ7Gñ∆RÊFó7∆ì“vÊˆÊRs∞¢–¢w&ÊVÊD6Üñ∆BáG&VTFóbì∞¢“ì∞ß–†¶gVÊ7Fñˆ‚ˆ'Vñ∆EG&VTDÙ“áf¬¬FWFÇó∞¢6ˆÁ7BV√÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢V¬Ê6∆74Ê÷S“wG&VR÷ÊˆFRs∞¢ñbáf√””÷ÁV∆¬ó≤V¬ÊñÊÊW$ÖD‘√÷«7‚6∆73“'G&VR◊f¬G&VR÷ÁV∆¬#ÊÁV∆√¬˜7„Ê≤&WGW&‚V√≤–¢ñbáGóVˆbf√””“v&ˆˆ∆V‚ró≤V¬ÊñÊÊW$ÖD‘√÷«7‚6∆73“'G&VR◊f¬G&VR÷&ˆˆ¬#‚G∑f«”¬˜7„Ê≤&WGW&‚V√≤–¢ñbáGóVˆbf√””“vÁV÷&W"ró≤V¬ÊñÊÊW$ÖD‘√÷«7‚6∆73“'G&VR◊f¬G&VR÷ÁV“#‚G∑f«”¬˜7„Ê≤&WGW&‚V√≤–¢ñbáGóVˆbf√””“w7G&ñÊrró≤V¬ÊñÊÊW$ÖD‘√÷«7‚6∆73“'G&VR◊f¬G&VR◊7G"#‚gV˜C≤G∂W62áf¬ó“gV˜C≥¬˜7„Ê≤&WGW&‚V√≤–¢ñbÑ'&íÊó4'&íáf¬íó∞¢V¬Ê6∆74∆ó7BÊFBÇwG&VR÷'&írì∞¢6ˆÁ7B6ˆ∆∆6VC÷FWFÉ„”#∞¢6ˆÁ7BÜVFW#÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢ÜVFW"Ê6∆74Ê÷S“wG&VR÷6ˆ∆∆6ñ&∆Rs∞¢ÜVFW"ÊñÊÊW$ÖD‘√“Ü6ˆ∆∆6VCÚ~)kÇs¢~)k‚rí∂«7‚6∆73“'G&VR÷'&6∂WB#Â≥¬˜7„„«7‚6∆73“'G&VR÷6˜VÁB#‚G∑f¬Ê∆VÊwFá”¬˜7„„«7‚6∆73“'G&VR÷'&6∂WB#Â”¬˜7„Ê∞¢6ˆÁ7B&ˆGì÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢&ˆGíÊ6∆74Ê÷S“wG&VR÷6Üñ∆G&V‚r≤Ü6ˆ∆∆6VCÚrG&VR÷6ˆ∆∆6VBs¢rrì∞¢f¬Êf˜$V6ÇÇÜóFV“∆íì”Á∞¢6ˆÁ7B6Üñ∆C÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢6Üñ∆BÊ6∆74Ê÷S“wG&VR÷óFV“s∞¢6Üñ∆BÊVÊD6Üñ∆BÖˆ'Vñ∆EG&VTDÙ“ÜóFV“¬FWFÇ≥íì∞¢ñbÜì«f¬Ê∆VÊwFÇ”í6Üñ∆BÊñÊÊW$ÖD‘¬≥“s«7‚6∆73“'G&VR÷6ˆ÷÷#‚√¬˜7„‚s∞¢&ˆGíÊVÊD6Üñ∆BÜ6Üñ∆Bì∞¢“ì∞¢V¬ÊVÊD6Üñ∆BÜÜVFW"ì∞¢V¬ÊVÊD6Üñ∆BÜ&ˆGíì∞¢ÜVFW"ÊˆÊ6∆ñ6≥“ÇÇì”Á∂6ˆÁ7B3÷&ˆGíÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwG&VR÷6ˆ∆∆6VBrì≤&ˆGíÊ6∆74∆ó7BÁFˆvv∆RÇwG&VR÷6ˆ∆∆6VBrì≤ÜVFW"ÊñÊÊW$ÖD‘√“Ü3Ú~)k‚s¢~)kÇrí∂«7‚6∆73“'G&VR÷'&6∂WB#Â≥¬˜7„„«7‚6∆73“'G&VR÷6˜VÁB#‚G∑f¬Ê∆VÊwFá”¬˜7„„«7‚6∆73“'G&VR÷'&6∂WB#Â”¬˜7„Ê∑“ì∞¢&WGW&‚V√∞¢–¢ñbáGóVˆbf√””“vˆ&¶V7Bró∞¢V¬Ê6∆74∆ó7BÊFBÇwG&VR÷ˆ&¶V7Brì∞¢6ˆÁ7B∂Wó3‘ˆ&¶V7BÊ∂Wó2áf¬ì∞¢6ˆÁ7B6ˆ∆∆6VC÷FWFÉ„”#∞¢6ˆÁ7BÜVFW#÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢ÜVFW"Ê6∆74Ê÷S“wG&VR÷6ˆ∆∆6ñ&∆Rs∞¢ÜVFW"ÊñÊÊW$ÖD‘√“Ü6ˆ∆∆6VCÚ~)kÇs¢~)k‚rí∂«7‚6∆73“'G&VR÷'&6∂WB#Á≥¬˜7„„«7‚6∆73“'G&VR÷6˜VÁB#‚G∂∂Wó2Ê∆VÊwFá”¬˜7„„«7‚6∆73“'G&VR÷'&6∂WB#Á”¬˜7„Ê∞¢6ˆÁ7B&ˆGì÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢&ˆGíÊ6∆74Ê÷S“wG&VR÷6Üñ∆G&V‚r≤Ü6ˆ∆∆6VCÚrG&VR÷6ˆ∆∆6VBs¢rrì∞¢∂Wó2Êf˜$V6ÇÇÜ∂Wí∆íì”Á∞¢6ˆÁ7B6Üñ∆C÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢6Üñ∆BÊ6∆74Ê÷S“wG&VR÷óFV“s∞¢6Üñ∆BÊñÊÊW$ÖD‘√÷«7‚6∆73“'G&VR÷∂Wí#‚gV˜C≤G∂W62Ü∂Wíó“gV˜C≥¬˜7„„«7‚6∆73“'G&VR÷6ˆ∆ˆ‚#„¢¬˜7„Ê∞¢6Üñ∆BÊVÊD6Üñ∆BÖˆ'Vñ∆EG&VTDÙ“áf≈∂∂Wï“¬FWFÇ≥íì∞¢ñbÜì∆∂Wó2Ê∆VÊwFÇ”í6Üñ∆BÊñÊÊW$ÖD‘¬≥“s«7‚6∆73“'G&VR÷6ˆ÷÷#‚√¬˜7„‚s∞¢&ˆGíÊVÊD6Üñ∆BÜ6Üñ∆Bì∞¢“ì∞¢V¬ÊVÊD6Üñ∆BÜÜVFW"ì∞¢V¬ÊVÊD6Üñ∆BÜ&ˆGíì∞¢ÜVFW"ÊˆÊ6∆ñ6≥“ÇÇì”Á∂6ˆÁ7B3÷&ˆGíÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇwG&VR÷6ˆ∆∆6VBrì≤&ˆGíÊ6∆74∆ó7BÁFˆvv∆RÇwG&VR÷6ˆ∆∆6VBrì≤ÜVFW"ÊñÊÊW$ÖD‘√“Ü3Ú~)k‚s¢~)kÇrí∂«7‚6∆73“'G&VR÷'&6∂WB#Á≥¬˜7„„«7‚6∆73“'G&VR÷6˜VÁB#‚G∂∂Wó2Ê∆VÊwFá”¬˜7„„«7‚6∆73“'G&VR÷'&6∂WB#Á”¬˜7„Ê∑“ì∞¢&WGW&‚V√∞¢–¢V¬ÊñÊÊW$ÖD‘√÷«7‚6∆73“'G&VR◊f¬#‚G∂W62Ö7G&ñÊráf¬íó”¬˜7„Ê∞¢&WGW&‚V√∞ß–†¶gVÊ7Fñˆ‚FD6˜î'WGFˆÁ2Ü6ˆÁFñÊW"ó∞¢6ˆÁ7BV√÷6ˆÁFñÊW'«¬BÇv◊6tñÊÊW"rì∞¢ñbÇV¬í&WGW&„∞¢V¬ÁVW'ï6V∆V7F˜$∆¬Çw&R‚6ˆFRríÊf˜$V6ÇÜ6ˆFTV√”Á∞¢6ˆÁ7B&S÷6ˆFTV¬Á&VÁDV∆V÷VÁC∞¢6ˆÁ7BÜVFW#◊&RÁ&Wfñ˜W4V∆V÷VÁE6ñ&∆ñÊs∞¢ñbá&RÁVW'ï6V∆V7F˜"ÇrÊ6ˆFR÷6˜í÷'F‚ró«¬ÜÜVFW"bfÜVFW"Ê6∆74∆ó7BÊ6ˆÁFñÁ2Çw&R÷ÜVFW"ríbfÜVFW"ÁVW'ï6V∆V7F˜"ÇrÊ6ˆFR÷6˜í÷'F‚rííí&WGW&„∞¢6ˆÁ7B'F„÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇv'WGFˆ‚rì∞¢'F‚Ê6∆74Ê÷S“v6ˆFR÷6˜í÷'F‚s∞¢'F‚ÁFWáD6ˆÁFVÁC◊BÇv6˜írì∞¢'F‚ÊˆÊ6∆ñ6≥“ÜRì”Á∞¢RÁ7F˜&˜vFñˆ‚Çì∞¢ˆ6˜ïFWáBÜ6ˆFTV¬ÁFWáD6ˆÁFVÁBíÁFÜV‚ÇÇì”Á∞¢'F‚ÁFWáD6ˆÁFVÁC◊BÇv6˜ñVBrì∞¢6WEFñ÷V˜WBÇÇì”Á∂'F‚ÁFWáD6ˆÁFVÁC◊BÇv6˜írì∑“√Sì∞¢“íÊ6F6ÇÇÇì”Á∂'F‚ÁFWáD6ˆÁFVÁC◊BÇv6˜ïˆfñ∆VBrì∑6WEFñ÷V˜WBÇÇì”Á∂'F‚ÁFWáD6ˆÁFVÁC◊BÇv6˜írì∑“√Sì∑“ì∞¢”∞¢ñbÜÜVFW"bfÜVFW"Ê6∆74∆ó7BÊ6ˆÁFñÁ2Çw&R÷ÜVFW"ríó∞¢ÜVFW"Á7Gñ∆RÊFó7∆ì“vf∆WÇs∞¢ÜVFW"Á7Gñ∆RÊßW7Fñgî6ˆÁFVÁC“w76R÷&WGvVV‚s∞¢ÜVFW"Á7Gñ∆RÊ∆ñv‰óFV◊3“v6VÁFW"s∞¢ÜVFW"ÊVÊD6Üñ∆BÜ'F‚ì∞¢÷V«6W∞¢&RÁ7Gñ∆RÁ˜6óFñˆ„“w&V∆FófRs∞¢'F‚Á7Gñ∆RÊ775FWáC“w˜6óFñˆ„¶'6ˆ«WFS∑F˜£gÉ∑&ñváC£gÉ≤s∞¢&RÊVÊD6Üñ∆BÜ'F‚ì∞¢–¢“ì∞ß–†¶∆WBˆ÷W&÷ñD∆ˆFñÊs÷f«6S∞¶∆WBˆ÷W&÷ñE&VGì÷f«6S∞†¶gVÊ7Fñˆ‚∆ˆDFñfdñÊ∆ñÊRÜ6ˆÁFñÊW"ó∞¢6ˆÁ7BDîdeÙ‘Öı4ï§S”S"£#C≤ÚÚS"¥"6f˜"ñÊ∆ñÊRFñfb&VÊFW&ñÊp¢6ˆÁ7B&ˆ˜C÷6ˆÁFñÊW'«∆Fˆ7V÷VÁC∞¢&ˆ˜BÁVW'ï6V∆V7F˜$∆¬ÇrÊFñfb÷ñÊ∆ñÊR÷∆ˆC¶Ê˜BÖ∂FF÷∆ˆFVE“íríÊf˜$V6ÇÜV√”Á∞¢V¬Á6WDGG&ñ'WFRÇvFF÷∆ˆFVBr¬srì∞¢6ˆÁ7BFÉ÷V¬ÊFF6WBÁFÉ∞¢6ˆÁ7B6Ê’ˆ÷VFñ6ÊVW'íÜV¬íÁ&W∆6RÇı‚g6Ê“Ú¬rrì∞¢fWF6ÇÖˆ÷VFñ&WfñWuW&¬áFÇ«∑6Êß6Ê««VÊFVfñÊVG“íê¢ÁFÜV‚á#”Á∂ñbÇ"Êˆ≤íFá&˜rÊWrW'&˜"á"Á7FGW2ì∑&WGW&‚"ÁFWáBÇì∑“ê¢ÁFÜV‚áFWáC”Á∞¢ñbáFWáBÊ∆VÊwFÉ‰DîdeÙ‘Öı4ï§Ró∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“&Fñfb÷ñÊ∆ñÊR÷W'&˜"#‚G∂W62áFÇÁ7∆óBÇrÚríÁ˜Çíó”∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÇvFñfe˜Fˆıˆ∆&vRró”¬˜7„„¬ˆFócÊ∞¢&WGW&„∞¢–¢6ˆÁ7B∆ñÊW3◊FWáBÁ7∆óBÇu∆‚ríÊ÷Ü∆ñÊS”Á∞¢6ˆÁ7BS÷W62Ü∆ñÊRì∞¢ñbÜRÁ7F'G5vóFÇÇtríí&WGW&‚«7‚6∆73“&Fñfb÷∆ñÊRFñfb÷áVÊ≤#‚G∂W”¬˜7„Ê∞¢ñbÜRÁ7F'G5vóFÇÇr≤ríí&WGW&‚«7‚6∆73“&Fñfb÷∆ñÊRFñfb◊«W2#‚G∂W”¬˜7„Ê∞¢ñbÜRÁ7F'G5vóFÇÇr“ríí&WGW&‚«7‚6∆73“&Fñfb÷∆ñÊRFñfb÷÷ñÁW2#‚G∂W”¬˜7„Ê∞¢&WGW&‚«7‚6∆73“&Fñfb÷∆ñÊR#‚G∂W”¬˜7„Ê∞¢“íÊ¶ˆñ‚Çu∆‚rì∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“&Fñfb÷ñÊ∆ñÊR#„∆Fób6∆73“'&R÷ÜVFW"#‚G∂W62áFÇÁ7∆óBÇrÚríÁ˜Çíó”¬ˆFóc„«&R6∆73“&Fñfb÷&∆ˆ6≤#„∆6ˆFS‚G∂∆ñÊW7”¬ˆ6ˆFS„¬˜&S„¬ˆFócÊ∞¢“ê¢Ê6F6ÇÇÇì”Á∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“&Fñfb÷ñÊ∆ñÊR÷W'&˜"#‚G∂W62áFÇÁ7∆óBÇrÚríÁ˜Çíó”∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÇvFñfeˆW'&˜"ró”¬˜7„„¬ˆFócÊ∞¢“ì∞¢“ì∞ß–†¶6ˆÁ7B55eÙ‘Öı4ï§S”#Sb£#C≤ÚÚ#Sb¥"6f˜"ñÊ∆ñÊR55b&VÊFW&ñÊp†¶gVÊ7Fñˆ‚ˆ÷VFñ6W76ñˆÂVW'íÇó∞¢6ˆÁ7B÷VFñ6W76ñˆ‰ñC“áGóVˆb2”“wVÊFVfñÊVBrbe2be2Á6W76ñˆ‚be2Á6W76ñˆ‚Á6W76ñˆÂˆñBìı7G&ñÊrÖ2Á6W76ñˆ‚Á6W76ñˆÂˆñBì¢rs∞¢&WGW&‚÷VFñ6W76ñˆ‰ñCÚrg6W76ñˆÂˆñC“r∂VÊ6ˆFUU$î6ˆ◊ˆÊVÁBÜ÷VFñ6W76ñˆ‰ñBì¢rs∞ß–†¢ÚÚ÷W76vR÷∆WfV¬÷VFñ6Ê6Ü˜G3¢∆ßí&WfñWr∆ˆFW'2áFbˆáF÷¬ˆ77bˆFñfb¢ÚÚWÜ6∆ñG&rí'Vñ∆BFÜVó"fWF6ÇU$¬g&ˆ“FF◊FÇB∆ˆBFñ÷R‚FÜR7F◊ñÊp¢ÚÚ72ñ‚˜7F◊÷VFñ6Ê6Ü˜G26'&ñW2FÜR6ˆÁFVÁBFñvW7Bñ‚FF◊6Ê∞¢ÚÚVÊBóBÜW&R6ÚFÜR&WfñWr6Ü˜w2FÜRfñ∆R2FÜR÷W76vRV÷óGFVBóB‡¶gVÊ7Fñˆ‚ˆ÷VFñ6ÊVW'íÜV¬ó∞¢6ˆÁ7B6Ê÷V¬bfV¬ÊFF6WCˆV¬ÊFF6WBÁ6Ê¢rs∞¢&WGW&‚á6ÊbbıÂ≥”ñ÷e◊≥cG“BÚÁFW7Bá6ÊíìÚÇrg6Ê“r∑6Êì¢rs∞ß–†¶gVÊ7Fñˆ‚ˆ÷VFñ&WfñWuW&¬áFÇ¬˜G3◊∑“ó∞¢∆WBW&√“víˆ÷VFñ˜FÉ“r∂VÊ6ˆFUU$î6ˆ◊ˆÊVÁBáFÇíµˆ÷VFñ6W76ñˆÂVW'íÇì∞¢ñbÜ˜G2Á6ÊíW&¬≥“rg6Ê“r∂VÊ6ˆFUU$î6ˆ◊ˆÊVÁBÜ˜G2Á6Êì∞¢ñbÜ˜G2ÊñÊ∆ñÊRíW&¬≥“rfñÊ∆ñÊS”s∞¢ñbÜ˜G2ÊF˜vÊ∆ˆBíW&¬≥“rfF˜vÊ∆ˆC”s∞¢&WGW&‚W&√∞ß–†¶gVÊ7Fñˆ‚ˆ77d÷VFñW&¬áFÇ¬˜G3◊∑“ó∞¢&WGW&‚ˆ÷VFñ&WfñWuW&¬áFÇ¬˜G2ì∞ß–†¶gVÊ7Fñˆ‚'Vñ∆D77eF&∆U&WfñWráFÇ¬FWáB¬F˜vÊ∆ˆEW&√“rró∞¢ñbáGóVˆbFWáB”“w7G&ñÊrrí&WGW&‚∂W'&˜$∂Wì¢v77eˆW'&˜"w”∞¢ñbáFWáBÊ∆VÊwFÉ‰55eÙ‘Öı4ï§Rí&WGW&‚∂W'&˜$∂Wì¢v77e˜Fˆıˆ∆&vRw”∞¢6ˆÁ7B&˜w3◊FWáBÁ&W∆6RÇı«%∆‚ˆr¬u∆‚ríÁ&W∆6RÇı«"ˆr¬u∆‚ríÁ7∆óBÇu∆‚ríÊfñ«FW"á#”Á"ÁG&ñ“Çíì∞¢ñbá&˜w2Ê∆VÊwFÉ√"í&WGW&‚∂W'&˜$∂Wì¢v77eˆÊıˆFFw”∞¢ÚÚWFÚ÷FWFV7B6W&F˜"Ü6ˆ÷÷¬6V÷ñ6ˆ∆ˆ‚¬F"ê¢ÚÚÜWW&ó7Fñ3¢W6W2FÜRfó'7B6W&F˜"f˜VÊBñ‚FÜRÜVFW"&˜r‚VFvR66S†¢ÚÚV˜FVBfñV∆G26ˆÁFñÊñÊr6ˆ÷÷2vóFÜ˜WBÊˆ‚◊V˜FVB6ˆ÷÷2ñ‚FÜRÜVFW ¢ÚÚ6˜V∆B6W6R÷ó6FWFV7Fñˆ‚(	B66WF&∆RG&FR÷ˆfbf˜"&WfñWr&VÊFW&W"‡¢6ˆÁ7Bfó'7D∆ñÊS◊&˜w5≥”∞¢6ˆÁ7B6W&F˜'3’≤r¬r¬s≤r¬u«Bu”∞¢6ˆÁ7B6W◊6W&F˜'2ÊfñÊBá3”Êfó'7D∆ñÊRÊñÊ6«VFW2á2íó«¬r¬s∞¢6ˆÁ7BÜVFW'3◊&˜w5≥“Á7∆óBá6WíÊ÷Ü3”Ê2ÁG&ñ“ÇíÁ&W∆6RÇıÂ≤"u◊≈≤"u“Bˆr¬rríì∞¢6ˆÁ7B&ˆGï&˜w3◊&˜w2Á6∆ñ6RÉíÊ÷á#”‚s«G#‚r∑"Á7∆óBá6WíÊ÷Ü3”Ê«FC‚G∂W62Ü2ÁG&ñ“ÇíÁ&W∆6RÇıÂ≤"u◊≈≤"u“Bˆr¬rríó”¬˜FCÊíÊ¶ˆñ‚Çrrí≤s¬˜G#‚ríÊ¶ˆñ‚Çrrì∞¢6ˆÁ7BÜVFW%&˜s÷ÜVFW'2Ê÷ÜÉ”Ê«FÉ‚G∂W62ÜÇó”¬˜FÉÊíÊ¶ˆñ‚Çrrì∞¢6ˆÁ7BfÊ÷S◊FÇÁ7∆óBÇrÚríÁ˜Çó««FÉ∞¢6ˆÁ7BF˜vÊ∆ˆD∆ñÊ≥÷F˜vÊ∆ˆEW&¿¢Ú∆6∆73“&77b÷F˜vÊ∆ˆB÷∆ñÊ≤◊6r÷÷VFñ÷∆ñÊ≤"á&Vc“"G∂W62ÜF˜vÊ∆ˆEW&¬ó“"F˜vÊ∆ˆC“"G∂W62ÜfÊ÷Ró“#Ô	˘8‚G∂W62ÜfÊ÷Ró”¬ˆÊ ¢¢rs∞¢&WGW&‚∞¢áF÷√¶∆Fób6∆73“&77b◊F&∆R◊w&#„∆Fób6∆73“'&R÷ÜVFW"77b◊&WfñWr÷ÜVFW"#„«7‚6∆73“&77b◊&WfñWr◊FóF∆R#‚G∂W62ÜfÊ÷Ró“«7‚7Gñ∆S“&˜6óGì¢„S∂fˆÁB◊6ó¶S£Ç#‚G∑BÇv77eˆÜVFW%ˆÊ˜FRró”¬˜7„„¬˜7„‚G∂F˜vÊ∆ˆD∆ñÊ∑”¬ˆFóc„«F&∆R6∆73“&77b◊F&∆R#„«FÜVC„«G#‚G∂ÜVFW%&˜w”¬˜G#„¬˜FÜVC„«F&ˆGì‚G∂&ˆGï&˜w7”¬˜F&ˆGì„¬˜F&∆S„¬ˆFócÊ¿¢”∞ß–†¶gVÊ7Fñˆ‚ˆ77e&WfñWtW'&˜$áF÷¬áFÇ¬W'&˜$∂Wí¬F˜vÊ∆ˆEW&√’ˆ77d÷VFñW&¬áFÇ«∂F˜vÊ∆ˆCßG'VW“íó∞¢6ˆÁ7BfÊ÷S◊FÇÁ7∆óBÇrÚríÁ˜Çó««FÉ∞¢&WGW&‚∆Fób6∆73“&Fñfb÷ñÊ∆ñÊR÷W'&˜"#‚G∂W62ÜfÊ÷Ró”∆'#„∆6∆73“&◊6r÷÷VFñ÷∆ñÊ≤"á&Vc“"G∂W62ÜF˜vÊ∆ˆEW&¬ó“"F˜vÊ∆ˆC“"G∂W62ÜfÊ÷Ró“#Ô	˘8‚G∂W62ÜfÊ÷Ró”¬ˆ„∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÜW'&˜$∂Wíó”¬˜7„„¬ˆFócÊ∞ß–†¶gVÊ7Fñˆ‚∆ˆD77dñÊ∆ñÊRÜ6ˆÁFñÊW"ó∞¢6ˆÁ7B&ˆ˜C÷6ˆÁFñÊW'«∆Fˆ7V÷VÁC∞¢&ˆ˜BÁVW'ï6V∆V7F˜$∆¬ÇrÊ77b÷ñÊ∆ñÊR÷∆ˆC¶Ê˜BÖ∂FF÷∆ˆFVE“íríÊf˜$V6ÇÜV√”Á∞¢V¬Á6WDGG&ñ'WFRÇvFF÷∆ˆFVBr¬srì∞¢6ˆÁ7BFÉ÷V¬ÊFF6WBÁFÉ∞¢6ˆÁ7B6Ê’ˆ÷VFñ6ÊVW'íÜV¬íÁ&W∆6RÇı‚g6Ê“Ú¬rrì∞¢6ˆÁ7B÷VFñW&√’ˆ77d÷VFñW&¬áFÇ«∑6Êß6Ê««VÊFVfñÊVG“ì∞¢6ˆÁ7BF˜vÊ∆ˆEW&√’ˆ77d÷VFñW&¬áFÇ«∂F˜vÊ∆ˆCßG'VR«6Êß6Ê««VÊFVfñÊVG“ì∞¢fWF6ÇÜ÷VFñW&¬ê¢ÁFÜV‚á#”Á∂ñbÇ"Êˆ≤íFá&˜rÊWrW'&˜"á"Á7FGW2ì∑&WGW&‚"ÁFWáBÇì∑“ê¢ÁFÜV‚áFWáC”Á∞¢6ˆÁ7B&WfñWs÷'Vñ∆D77eF&∆U&WfñWráFÇ¬FWáB¬F˜vÊ∆ˆEW&¬ì∞¢V¬Ê˜WFW$ÖD‘√◊&WfñWrÊáF÷««≈ˆ77e&WfñWtW'&˜$áF÷¬áFÇ¬&WfñWrÊW'&˜$∂Wó«¬v77eˆW'&˜"r¬F˜vÊ∆ˆEW&¬ì∞¢“ê¢Ê6F6ÇÇÇì”Á∞¢V¬Ê˜WFW$ÖD‘√’ˆ77e&WfñWtW'&˜$áF÷¬áFÇ¬v77eˆW'&˜"r¬F˜vÊ∆ˆEW&¬ì∞¢“ì∞¢“ì∞ß–†¶gVÊ7Fñˆ‚∆ˆDWÜ6∆ñG&tñÊ∆ñÊRÜ6ˆÁFñÊW"ó∞¢6ˆÁ7BUÑ4ƒîE$uÙ‘Öı4ï§S”S"£#C≤ÚÚS"¥"6 ¢6ˆÁ7B&ˆ˜C÷6ˆÁFñÊW'«∆Fˆ7V÷VÁC∞¢&ˆ˜BÁVW'ï6V∆V7F˜$∆¬ÇrÊWÜ6∆ñG&r÷ñÊ∆ñÊR÷∆ˆC¶Ê˜BÖ∂FF÷∆ˆFVE“íríÊf˜$V6ÇÜV√”Á∞¢V¬Á6WDGG&ñ'WFRÇvFF÷∆ˆFVBr¬srì∞¢6ˆÁ7BFÉ÷V¬ÊFF6WBÁFÉ∞¢6ˆÁ7B6Ê’ˆ÷VFñ6ÊVW'íÜV¬íÁ&W∆6RÇı‚g6Ê“Ú¬rrì∞¢6ˆÁ7BF˜vÊ∆ˆEW&√’ˆ÷VFñ&WfñWuW&¬áFÇ«∂F˜vÊ∆ˆCßG'VR«6Êß6Ê««VÊFVfñÊVG“ì∞¢fWF6ÇÖˆ÷VFñ&WfñWuW&¬áFÇ«∑6Êß6Ê««VÊFVfñÊVG“íê¢ÁFÜV‚á#”Á∂ñbÇ"Êˆ≤íFá&˜rÊWrW'&˜"á"Á7FGW2ì∑&WGW&‚"ÁFWáBÇì∑“ê¢ÁFÜV‚áFWáC”Á∞¢ñbáFWáBÊ∆VÊwFÉ‰UÑ4ƒîE$uÙ‘Öı4ï§Ró∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“&Fñfb÷ñÊ∆ñÊR÷W'&˜"#‚G∂W62áFÇÁ7∆óBÇrÚríÁ˜Çíó”∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÇvWÜ6∆ñG&u˜Fˆıˆ∆&vRró”¬˜7„„¬ˆFócÊ∞¢&WGW&„∞¢–¢ÚÚf∆ñFFRóB∆ˆˆ∑2∆ñ∂RWÜ6∆ñG&r•4Ù‡¢∆WBFF∞¢G'ó∂FF‘•4Ù‚Á'6RáFWáBì∑÷6F6ÇÜRó∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“&Fñfb÷ñÊ∆ñÊR÷W'&˜"#‚G∂W62áFÇÁ7∆óBÇrÚríÁ˜Çíó”∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÇvWÜ6∆ñG&uˆñÁf∆ñBró”¬˜7„„¬ˆFócÊ∞¢&WGW&„∞¢–¢ñbÇFFÁGóW«∆FFÁGóR”“vWÜ6∆ñG&rró∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“&Fñfb÷ñÊ∆ñÊR÷W'&˜"#‚G∂W62áFÇÁ7∆óBÇrÚríÁ˜Çíó”∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÇvWÜ6∆ñG&uˆñÁf∆ñBró”¬˜7„„¬ˆFócÊ∞¢&WGW&„∞¢–¢6ˆÁ7BfÊ÷S÷W62áFÇÁ7∆óBÇrÚríÁ˜Çíì∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“&WÜ6∆ñG&r÷V÷&VB◊w&"FóF∆S“"G∑BÇvWÜ6∆ñG&u˜6ñ◊∆ñfñVBró“#‡¢∆Fób6∆73“&◊6r÷'Fñf7B÷ÜVFW"#‡¢«7‚6∆73“&◊6r÷÷VFñ÷∆&V¬#‚G∑BÇvWÜ6∆ñG&uˆ∆&V¬ró”¬˜7„‡¢∆6∆73“&WÜ6∆ñG&r÷˜V‚÷∆ñÊ≤"á&Vc“"G∂F˜vÊ∆ˆEW&«“"F˜vÊ∆ˆC“"G∂fÊ÷W“#‚G∑BÇvWÜ6∆ñG&uˆF˜vÊ∆ˆBró“G∂fÊ÷W”¬ˆ‡¢¬ˆFóc‡¢∆Fób6∆73“&WÜ6∆ñG&r÷6Áf2"FF÷WÜ6∆ñG&s“rG∂W62áFWáBó“s„¬ˆFóc‡£¬ˆFócÊ∞¢ÚÚ∆ßí÷ñÊóBWÜ6∆ñG&r&VÊFW"gFW"DÙ“ñÁ6W'Fñˆ‡¢&WVW7DÊñ÷Fñˆ‰g&÷RÇÇì”Â˜&VÊFW$WÜ6∆ñG&t6Áf6W2Çíì∞¢“ê¢Ê6F6ÇÇÇì”Á∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“&Fñfb÷ñÊ∆ñÊR÷W'&˜"#‚G∂W62áFÇÁ7∆óBÇrÚríÁ˜Çíó”∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÇvWÜ6∆ñG&uˆW'&˜"ró”¬˜7„„¬ˆFócÊ∞¢“ì∞¢“ì∞ß–†¶∆WBˆWÜ6∆ñG&u67&óD∆ˆFVC÷f«6S∞¶gVÊ7Fñˆ‚˜&VÊFW$WÜ6∆ñG&t6Áf6W2Çó∞¢Fˆ7V÷VÁBÁVW'ï6V∆V7F˜$∆¬ÇrÊWÜ6∆ñG&r÷6Áf3¶Ê˜BÖ∂FF◊&VÊFW&VE“íríÊf˜$V6ÇÜV√”Á∞¢V¬Á6WDGG&ñ'WFRÇvFF◊&VÊFW&VBr¬srì∞¢6ˆÁ7BFF7G#÷V¬ÊvWDGG&ñ'WFRÇvFF÷WÜ6∆ñG&rrì∞¢ñbÇFF7G"í&WGW&„∞¢ÚÚ&VÊFW"6ñ◊∆R5dr&WfñWrW6ñÊrFÜRWÜ6∆ñG&rV∆V÷VÁG0¢G'ó∞¢6ˆÁ7BFF‘•4Ù‚Á'6RÜFF7G"ì∞¢6ˆÁ7BV∆V÷VÁG3÷FFÊV∆V÷VÁG7«≈µ”∞¢ñbÇV∆V÷VÁG2Ê∆VÊwFÇó∂V¬ÊñÊÊW$ÖD‘√÷∆Fób6∆73“&WÜ6∆ñG&r÷V◊Gí#‚G∑BÇvWÜ6∆ñG&uˆV◊Gíró”¬ˆFócÊ∑&WGW&„∑–¢ÚÚ6∆7V∆FR&˜VÊG0¢∆WB÷ñÂÉ‘ñÊfñÊóGí∆÷ñÂì‘ñÊfñÊóGí∆÷ÖÉ“‘ñÊfñÊóGí∆÷Öì“‘ñÊfñÊóGì∞¢V∆V÷VÁG2Êf˜$V6ÇÜV√”Á∞¢6ˆÁ7B#’∂V¬Áá«√∆V¬Áó«√¬ÜV¬Áá«√í≤ÜV¬ÁvñGFá«√í¬ÜV¬Áó«√í≤ÜV¬ÊÜVñváG«√ï”∞¢÷ñÂÉ‘÷FÇÊ÷ñ‚Ü÷ñÂÇ∆%≥“ì∂÷ñÂì‘÷FÇÊ÷ñ‚Ü÷ñÂí∆%≥“ì∞¢÷ÖÉ‘÷FÇÊ÷ÇÜ÷ÖÇ∆%≥%“ì∂÷Öì‘÷FÇÊ÷ÇÜ÷Öí∆%≥5“ì∞¢“ì∞¢6ˆÁ7BC”#∂÷ñÂÇ”◊C∂÷ñÂí”◊C∂÷ÖÇ≥◊C∂÷Öí≥◊C∞¢6ˆÁ7Bs‘÷FÇÊ÷ÇÜ÷ÖÇ÷÷ñÂÇ√#ì∂6ˆÁ7BÉ‘÷FÇÊ÷ÇÜ÷Öí÷÷ñÂí√Sì∞¢ÚÚ5drGG&ñ'WFW2&R&VÊFW&VBfññÊÊW$ÖD‘¬&V∆˜r¬6ÚGF6∂W"÷6ˆÁG&ˆ∆∆V@¢ÚÚf«VW2g&ˆ“•4Ù‚ÜRÊr‚7G&ˆ∂T6ˆ∆˜#“w&VB"Û„«67&óC‚‚‚‚rív˜V∆B'&V≤˜W@¢ÚÚˆbFÜRGG&ñ'WFR‚W66R7G&ñÊw3≤6ˆW&6RÁV÷W&ñ72‡¢6ˆÁ7B˜6◊c”Â7G&ñÊrác”÷ÁV∆√ÚrsßbíÁ&W∆6RÇÚbˆr¬rf◊≤ríÁ&W∆6RÇÚ"ˆr¬rgV˜C≤ríÁ&W∆6RÇÛ¬ˆr¬rf«C≤ríÁ&W∆6RÇÛ‚ˆr¬rfwC≤rì∞¢6ˆÁ7BˆÁV”“áb∆f"ì”Á∂6ˆÁ7B„‘ÁV÷&W"ábì∑&WGW&‚ÁV÷&W"Êó4fñÊóFRÜ‚ìˆ„¶f#∑”∞¢6ˆÁ7B7fu'G3’∂«7frÜ÷∆Á3“&áGG¢Ú˜wwrÁs2Ê˜&rÛ#˜7fr"fñWt&˜É“"GµˆÁV“Ü÷ñÂÇ√ó“GµˆÁV“Ü÷ñÂí√ó“GµˆÁV“ár√#ó“GµˆÁV“ÜÇ√Só“"6∆73“&WÜ6∆ñG&r◊7fr#Ê”∞¢V∆V÷VÁG2Êf˜$V6ÇÜV√”Á∞¢6ˆÁ7B7G&ˆ∂S’˜6ÜV¬Á7G&ˆ∂T6ˆ∆˜'«¬r3SSRrì∞¢6ˆÁ7Bfñ∆√’˜6ÜV¬Ê&6∂w&˜VÊD6ˆ∆˜'«¬wG&Á7&VÁBrì∞¢6ˆÁ7B7s’ˆÁV“ÜV¬Á7G&ˆ∂UvñGFÇ√"ì∞¢6ˆÁ7BÉ’ˆÁV“ÜV¬ÁÇ√í«ì’ˆÁV“ÜV¬Áí√í«s’ˆÁV“ÜV¬ÁvñGFÇ√í∆É’ˆÁV“ÜV¬ÊÜVñváB√ì∞¢ñbÜV¬ÁGóS””“w&V7FÊv∆Rró∞¢7fu'G2ÁW6ÇÜ«&V7BÉ“"G∑á“"ì“"G∑ó“"vñGFÉ“"G∑w“"ÜVñváC“"G∂á“"7G&ˆ∂S“"G∑7G&ˆ∂W“"7G&ˆ∂R◊vñGFÉ“"G∑7w“"fñ∆√“"G∂fñ∆«“"'É“"G∂V¬Á&˜VÊFÊW73ÚÁGóS”””3ÛÉ£“"ÛÊì∞¢÷V«6RñbÜV¬ÁGóS””“vFñ÷ˆÊBró∞¢6ˆÁ7B7É◊Ç∑rÛ"∆7ì◊í∂ÇÛ#∞¢7fu'G2ÁW6ÇÜ«ˆ«ñvˆ‚ˆñÁG3“"G∂7á“¬G∑ó“G∑Ç∑w“¬G∂7ó“G∂7á“¬G∑í∂á“G∑á“¬G∂7ó“"7G&ˆ∂S“"G∑7G&ˆ∂W“"7G&ˆ∂R◊vñGFÉ“"G∑7w“"fñ∆√“"G∂fñ∆«“"ÛÊì∞¢÷V«6RñbÜV¬ÁGóS””“vV∆∆ó6Rró∞¢7fu'G2ÁW6ÇÜ∆V∆∆ó6R7É“"G∑Ç∑rÛ'“"7ì“"G∑í∂ÇÛ'“"'É“"G∑rÛ'“"'ì“"G∂ÇÛ'“"7G&ˆ∂S“"G∑7G&ˆ∂W“"7G&ˆ∂R◊vñGFÉ“"G∑7w“"fñ∆√“"G∂fñ∆«“"ÛÊì∞¢÷V«6RñbÜV¬ÁGóS””“v∆ñÊRró∞¢6ˆÁ7BG3“ÜV¬ÁˆñÁG7«≈µ“íÊfñ«FW"á”‰'&íÊó4'&íáíbgÊ∆VÊwFÉ„”"ì∞¢ñbÇG2Ê∆VÊwFÇí&WGW&„∞¢∆WBC÷“GµˆÁV“áÇµˆÁV“áG5≥’≥“√í√ó“GµˆÁV“áíµˆÁV“áG5≥’≥“√í√ó÷∞¢f˜"Ü∆WBì”∂ì«G2Ê∆VÊwFÉ∂í≤≤íB≥÷¬GµˆÁV“áÇµˆÁV“áG5∂ï’≥“√í√ó“GµˆÁV“áíµˆÁV“áG5∂ï’≥“√í√ó÷∞¢7fu'G2ÁW6ÇÜ«FÇC“"G∂G“"7G&ˆ∂S“"G∑7G&ˆ∂W“"7G&ˆ∂R◊vñGFÉ“"G∑7w“"fñ∆√“&ÊˆÊR"7G&ˆ∂R÷∆ñÊV6“'&˜VÊB"7G&ˆ∂R÷∆ñÊV¶ˆñ„“'&˜VÊB"ÛÊì∞¢÷V«6RñbÜV¬ÁGóS””“v'&˜rró∞¢6ˆÁ7BG3“ÜV¬ÁˆñÁG7«≈µ“íÊfñ«FW"á”‰'&íÊó4'&íáíbgÊ∆VÊwFÉ„”"ì∞¢ñbÇG2Ê∆VÊwFÇí&WGW&„∞¢∆WBC÷“GµˆÁV“áÇµˆÁV“áG5≥’≥“√í√ó“GµˆÁV“áíµˆÁV“áG5≥’≥“√í√ó÷∞¢f˜"Ü∆WBì”∂ì«G2Ê∆VÊwFÉ∂í≤≤íB≥÷¬GµˆÁV“áÇµˆÁV“áG5∂ï’≥“√í√ó“GµˆÁV“áíµˆÁV“áG5∂ï’≥“√í√ó÷∞¢7fu'G2ÁW6ÇÜ«FÇC“"G∂G“"7G&ˆ∂S“"G∑7G&ˆ∂W“"7G&ˆ∂R◊vñGFÉ“"G∑7w“"fñ∆√“&ÊˆÊR"7G&ˆ∂R÷∆ñÊV6“'&˜VÊB"7G&ˆ∂R÷∆ñÊV¶ˆñ„“'&˜VÊB"÷&∂W"÷VÊC“'W&¬Ç6'&˜vÜVBí"ÛÊì∞¢÷V«6RñbÜV¬ÁGóS””“wFWáBró∞¢6ˆÁ7BfˆÁE6ó¶S’ˆÁV“ÜV¬ÊfˆÁE6ó¶R√#ì∞¢6ˆÁ7BGáC’7G&ñÊrÜV¬ÁFWáC”÷ÁV∆√Úrs¶V¬ÁFWáBì∞¢6ˆÁ7B∆ñÊW3◊GáBÁ7∆óBÇu∆‚rì∞¢∆ñÊW2Êf˜$V6ÇÇÜ∆ñÊR∆íì”Á∞¢7fu'G2ÁW6ÇÜ«FWáBÉ“"G∑á“"ì“"G∑í∂í¶fˆÁE6ó¶R£„"∂fˆÁE6ó¶W“"fñ∆√“"G∑7G&ˆ∂W“"fˆÁB◊6ó¶S“"G∂fˆÁE6ó¶W“"fˆÁB÷f÷ñ«ì“%fó&vñ¬¬6VvˆRTíV÷ˆ¶í¬6Á2◊6W&ñb#‚G∂W62Ü∆ñÊRó”¬˜FWáCÊì∞¢“ì∞¢÷V«6RñbÜV¬ÁGóS””“vG&rró∞¢6ˆÁ7BG3“ÜV¬ÁˆñÁG7«≈µ“íÊfñ«FW"á”‰'&íÊó4'&íáíbgÊ∆VÊwFÉ„”"ì∞¢ñbáG2Ê∆VÊwFÉ„ó∞¢∆WBC÷“GµˆÁV“áÇµˆÁV“áG5≥’≥“√í√ó“GµˆÁV“áíµˆÁV“áG5≥’≥“√í√ó÷∞¢f˜"Ü∆WBì”∂ì«G2Ê∆VÊwFÉ∂í≤≤íB≥÷¬GµˆÁV“áÇµˆÁV“áG5∂ï’≥“√í√ó“GµˆÁV“áíµˆÁV“áG5∂ï’≥“√í√ó÷∞¢7fu'G2ÁW6ÇÜ«FÇC“"G∂G“"7G&ˆ∂S“"G∑7G&ˆ∂W“"7G&ˆ∂R◊vñGFÉ“"G∑7w“"fñ∆√“&ÊˆÊR"7G&ˆ∂R÷∆ñÊV6“'&˜VÊB"7G&ˆ∂R÷∆ñÊV¶ˆñ„“'&˜VÊB"ÛÊì∞¢–¢–¢ÚÚVÊ∂Ê˜v‚V∆V÷VÁBGóW2ÜRÊr‚ñ÷vR¬g&÷R¬w&˜W¬g&VVG&rí&P¢ÚÚ6ñ∆VÁF«í6∂óVBFÚfˆñB'&V∂ñÊrFÜR&VÊFW"‚FÜó2ó26ñ◊∆ñfñV@¢ÚÚ5dr&WfñWr¬Ê˜BóÜV¬÷ñFVÁFñ6¬WÜ6∆ñG&r6Áf2&W&ˆGV7Fñˆ‚‡¢“ì∞¢ÚÚ'&˜r÷&∂W"FVfñÊóFñˆ‡¢7fu'G2ÁVÁ6ÜñgBÜ∆FVg3„∆÷&∂W"ñC“&'&˜vÜVB"÷&∂W%vñGFÉ“#"÷&∂W$ÜVñváC“#r"&VeÉ“#"&Veì“#2„R"˜&ñVÁC“&WFÚ#„«ˆ«ñvˆ‚ˆñÁG3“#¬2„R¬r"fñ∆√“"3SSR"Û„¬ˆ÷&∂W#„¬ˆFVg3Êì∞¢7fu'G2ÁW6ÇÇs¬˜7fs‚rì∞¢V¬ÊñÊÊW$ÖD‘√◊7fu'G2Ê¶ˆñ‚Çrrì∞¢÷6F6ÇÜRó∞¢V¬ÊñÊÊW$ÖD‘√÷∆Fób6∆73“&WÜ6∆ñG&r÷V◊Gí#‚G∑BÇvWÜ6∆ñG&u˜&VÊFW%ˆW'&˜"ró”¬ˆFócÊ∞¢–¢“ì∞ß–†¢ÚÚ)H)HDbñÊ∆ñÊR&WfñWrÜfó'7BvRí)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H ¢ÚÚ‰ıDS¢DbÊß2ó2∆ˆFVBg&ˆ“4D‚Üß6FV∆óg"í‚ˆff∆ñÊRˆó"÷vVBFW∆˜ñ÷VÁG0¢ÚÚvñ∆¬Ê˜BvWBñÊ∆ñÊR&WfñWw3≤FÜRR2f∆∆&6≤Fñ÷V˜WBFVw&FW2FÚ¢ÚÚF˜vÊ∆ˆB∆ñÊ≤ñ‚FÜB66R‚FÜRB‘"6ó¶R6ó26ÜV6∂VB6∆ñVÁB◊6ñFRgFW ¢ÚÚFÜRgV∆¬'VffW"ó2&V6VófVB(	BñFV∆«íFÜR6W'fW"v˜V∆BVÊf˜&6RóB&Vf˜&P¢ÚÚ7G&V÷ñÊrÜ˜WBˆb66˜Rf˜"FÜó26∆ñVÁB◊6ñFR"í‡¶∆WB˜Ffß5&VGì÷f«6R¬˜Ffß4∆ˆFñÊs÷f«6S∞¶gVÊ7Fñˆ‚∆ˆEFdñÊ∆ñÊRÜ6ˆÁFñÊW"ó∞¢6ˆÁ7BDeÙ‘Öı4ï§S”B£#B£#C≤ÚÚB‘"6f˜"ñÊ∆ñÊRDb&WfñWp¢6ˆÁ7B&ˆ˜C÷6ˆÁFñÊW'«∆Fˆ7V÷VÁC∞¢&ˆ˜BÁVW'ï6V∆V7F˜$∆¬ÇrÁFb◊&WfñWr÷∆ˆC¶Ê˜BÖ∂FF÷∆ˆFVE“íríÊf˜$V6ÇÜV√”Á∞¢V¬Á6WDGG&ñ'WFRÇvFF÷∆ˆFVBr¬srì∞¢6ˆÁ7BFÉ÷V¬ÊFF6WBÁFÉ∞¢6ˆÁ7BfÊ÷S◊FÇÁ7∆óBÇrÚríÁ˜Çó««FÉ∞¢6ˆÁ7B6Ê’ˆ÷VFñ6ÊVW'íÜV¬íÁ&W∆6RÇı‚g6Ê“Ú¬rrì∞¢6ˆÁ7B÷VFñW&√’ˆ÷VFñ&WfñWuW&¬áFÇ«∑6Êß6Ê««VÊFVfñÊVG“ì∞¢ÚÚg&VW¶R7Fñˆ‚U$«2∆ˆÊw6ñFRFÜRfWF6É¢6∆∆&6∑2÷í'V‚ñ‚Ê˜FÜW"6W76ñˆ‚‡¢6ˆÁ7BF≈W&√’ˆ÷VFñ&WfñWuW&¬áFÇ«∂F˜vÊ∆ˆCßG'VR«6Êß6Ê««VÊFVfñÊVG“ì∞¢6ˆÁ7B∆ˆEFc“áFfß4∆ñ"ì”Á∞¢fWF6ÇÜ÷VFñW&¬ê¢ÁFÜV‚á#”Á∂ñbÇ"Êˆ≤íFá&˜rÊWrW'&˜"á"Á7FGW2ì≤&WGW&‚"Ê'&î'VffW"Çì∑“ê¢ÁFÜV‚Ü'Vc”Á∞¢ñbÜ'VbÊ'óFT∆VÊwFÉÂDeÙ‘Öı4ï§Ró∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“'Fb◊&WfñWr÷f∆∆&6≤#„∆6∆73“&◊6r÷÷VFñ÷∆ñÊ≤"á&Vc“"G∂F≈W&«“"F˜vÊ∆ˆC“"G∂W62ÜfÊ÷Ró“#Ô	˘8‚G∂W62ÜfÊ÷Ró”¬ˆ„∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÇwFe˜Fˆıˆ∆&vRró”¬˜7„„¬ˆFócÊ∞¢&WGW&„∞¢–¢&WGW&‚Ffß4∆ñ"ÊvWDFˆ7V÷VÁBá∂FF¶'Vb¬ó4Wf≈7W˜'FVC¶f«6W“íÁ&ˆ÷ó6S∞¢“ê¢ÁFÜV‚áFc”Á∞¢ñbÇFbí&WGW&„∞¢6ˆÁ7BF˜F√◊FbÊÁV’vW3∞¢6ˆÁ7BvW4∆&V√◊F˜F√„ˆ+rG∑F˜F«“vW6¢rs∞¢6ˆÁ7Bw&÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢w&Ê6∆74Ê÷S“wFb◊&WfñWr◊w&s∞¢w&ÊñÊÊW$ÖD‘√÷∆Fób6∆73“'Fb◊&WfñWr÷ÜVFW"#„«7„Ô	˘8BG∂W62ÜfÊ÷Ró“G∑vW4∆&V«”¬˜7„„∆á&Vc“"G∂F≈W&«“"F˜vÊ∆ˆC“"G∂W62ÜfÊ÷Ró“"6∆73“'Fb÷F˜vÊ∆ˆB÷∆ñÊ≤#‚G∑BÇwFeˆF˜vÊ∆ˆBró“(i3¬ˆ„¬ˆFóc„∆Fób6∆73“'Fb◊&WfñWr÷&ˆGí#„¬ˆFócÊ∞¢6ˆÁ7B&ˆGì◊w&ÁVW'ï6V∆V7F˜"ÇrÁFb◊&WfñWr÷&ˆGírì∞¢V¬Á&W∆6UvóFÇáw&ì∞¢ÚÚ&VÊFW"WfW'ívRÜ6VBí6WVVÁFñ∆«íFÚ∆ñ÷óB÷V÷˜'ì≤FÜP¢ÚÚ6Áf6W27F6≤fW'Fñ6∆«íñ‚FÜR67&ˆ∆∆&∆R&WfñWr&ˆGí‡¢6ˆÁ7B‘ÖıtU3”#∞¢6ˆÁ7B„‘÷FÇÊ÷ñ‚áF˜F¬ƒ‘ÖıtU2ì∞¢ñbáF˜F√‰‘ÖıtU2ó∞¢6ˆÁ7BÊ˜Fñ6S÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢Ê˜Fñ6RÊ6∆74Ê÷S“wFb◊&WfñWr◊G'VÊ6FVBs∞¢Ê˜Fñ6RÁFWáD6ˆÁFVÁC◊BÇwFe˜G'VÊ6FVBrƒ‘ÖıtU2«F˜F¬ì∞¢&ˆGíÊVÊD6Üñ∆BÜÊ˜Fñ6Rì∞¢–¢ÚÚˆ‚W"◊vRfñ«W&R¬6∂óFÜBvRÊB6ˆÁFñÁVR6ÚˆÊR÷∆f˜&÷V@¢ÚÚvR6‚wB6ñ∆VÁF«íÜ«BFÜR&WfñWr˜"7W&f6R‚VÊÜÊF∆V@¢ÚÚ&ˆ÷ó6R&V¶V7Fñˆ‚á&VÊFW%vR'VÁ2˜WG6ñFRFÜR˜WFW"Ê6F6Ç6Üñ‚í‡¢6ˆÁ7B&VÊFW%vS“Üíì”Á∞¢ñbÜìÊ‚í&WGW&„∞¢FbÊvWEvRÜííÁFÜV‚ávS”Á∞¢6ˆÁ7B6Áf3÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇv6Áf2rì∞¢6ˆÁ7B66∆S”„S∞¢6ˆÁ7BfñWw˜'C◊vRÊvWEfñWw˜'Bá∑66∆W“ì∞¢6Áf2ÁvñGFÉ◊fñWw˜'BÁvñGFÉ∞¢6Áf2ÊÜVñváC◊fñWw˜'BÊÜVñváC∞¢6Áf2Ê6∆74Ê÷S“wFb◊&WfñWr÷6Áf2s∞¢ÚÚGF6ÇˆÊ«ígFW"7V66W76gV¬&VÊFW"¬6Ú&VÊFW"&V¶V7Fñˆ‡¢ÚÚÜ6˜''WBvRFF¬ÁV∆¬&B6ˆÁFWáBí6‚wB∆VfR&∆Ê≤6Áf0¢ÚÚ&VÜñÊB(	BFÜRÊ6F6ÇFÜV‚6ñ◊«í6∂ó2FÚFÜRÊWáBvR‡¢&WGW&‚vRÁ&VÊFW"á∂6Áf46ˆÁFWáC¶6Áf2ÊvWD6ˆÁFWáBÇs&Brí«fñWw˜'G“íÁ&ˆ÷ó6RÁFÜV‚ÇÇì”Á≤&ˆGíÊVÊD6Üñ∆BÜ6Áf2ì≤“ì∞¢“íÁFÜV‚ÇÇì”Á&VÊFW%vRÜí≥ííÊ6F6ÇÇÇì”Á&VÊFW%vRÜí≥íì∞¢”∞¢&VÊFW%vRÉì∞¢“ê¢Ê6F6ÇÇÇì”Á∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“'Fb◊&WfñWr÷f∆∆&6≤#„∆6∆73“&◊6r÷÷VFñ÷∆ñÊ≤"á&Vc“"G∂F≈W&«“"F˜vÊ∆ˆC“"G∂W62ÜfÊ÷Ró“#Ô	˘8‚G∂W62ÜfÊ÷Ró”¬ˆ„∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÇwFeˆW'&˜"ró”¬˜7„„¬ˆFócÊ∞¢“ì∞¢”∞¢ñbÖ˜Ffß5&VGíó∞¢∆ˆEFbávñÊF˜rÂ˜Ffß4∆ñ"ì∞¢“V«6RñbÇ˜Ffß4∆ˆFñÊró∞¢˜Ffß4∆ˆFñÊs◊G'VS∞¢6ˆÁ7B˜Fe7&3“váGG3¢Úˆ6F‚Êß6FV∆óg"ÊÊWBˆÁ“˜Ffß2÷Fó7DB„í„SRˆ'Vñ∆B˜FbÊ÷ñ‚Ê÷ß2s∞¢6ˆÁ7B˜Fev˜&∂W#“váGG3¢Úˆ6F‚Êß6FV∆óg"ÊÊWBˆÁ“˜Ffß2÷Fó7DB„í„SRˆ'Vñ∆B˜FbÁv˜&∂W"Ê÷ñ‚Ê÷ß2s∞¢6ˆÁ7B˜Fd&∆ˆ#÷ÊWr&∆ˆ"Ö∂ñ◊˜'B¶2g&ˆ“rGµ˜Fe7&7“s∑‰v∆ˆ&≈v˜&∂W$˜FñˆÁ2Áv˜&∂W%7&3“rGµ˜Fev˜&∂W'“s∑vñÊF˜rÂ˜Ffß4∆ñ#◊∑vñÊF˜rÂ˜Ffß5&VGì◊G'VS∑vñÊF˜rÊFó7F6ÑWfVÁBÜÊWrWfVÁBÇwFfß2◊&VGíríì∂“«∑GóS¢v∆ñ6Fñˆ‚ˆ¶f67&óBw“ì∞¢6ˆÁ7B3÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw67&óBrì∞¢2ÁGóS“v÷ˆGV∆Rs∞¢6ˆÁ7B˜Fd&∆ˆ%W&√’U$¬Ê7&VFTˆ&¶V7EU$¬Ö˜Fd&∆ˆ"ì∞¢2Á7&3’˜Fd&∆ˆ%W&√∞¢2ÊˆÊ∆ˆC“Çì”ÂU$¬Á&Wfˆ∂Tˆ&¶V7EU$¬Ö˜Fd&∆ˆ%W&¬ì∞¢Fˆ7V÷VÁBÊÜVBÊVÊD6Üñ∆Bá2ì∞¢vñÊF˜rÊFDWfVÁD∆ó7FVÊW"ÇwFfß2◊&VGír¬Çì”Á≤˜Ffß5&VGì◊G'VS≤∆ˆEFbávñÊF˜rÂ˜Ffß4∆ñ"ì≤“«∂ˆÊ6SßG'VW“ì∞¢6WEFñ÷V˜WBÇÇì”Á∞¢ñbÇ˜Ffß5&VGíó∞¢ñbÜV¬Á&VÁDÊˆFRó∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“'Fb◊&WfñWr÷f∆∆&6≤#„∆6∆73“&◊6r÷÷VFñ÷∆ñÊ≤"á&Vc“"G∂F≈W&«“"F˜vÊ∆ˆC“"G∂W62ÜfÊ÷Ró“#Ô	˘8‚G∂W62ÜfÊ÷Ró”¬ˆ„∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÇwFeˆW'&˜"ró”¬˜7„„¬ˆFócÊ∞¢–¢–¢“√Sì∞¢“V«6R∞¢vñÊF˜rÊFDWfVÁD∆ó7FVÊW"ÇwFfß2◊&VGír¬Çì”Á≤∆ˆEFbávñÊF˜rÂ˜Ffß4∆ñ"ì≤“«∂ˆÊ6SßG'VW“ì∞¢–¢“ì∞ß–†¢ÚÚ)H)HÖD‘¬ñÊ∆ñÊR&WfñWrá6ÊF&˜ÜVBñg&÷Rí)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H ¶gVÊ7Fñˆ‚∆ˆDáF÷ƒñÊ∆ñÊRÜ6ˆÁFñÊW"ó∞¢6ˆÁ7BÖD‘≈Ù‘Öı4ï§S”#Sb£#C≤ÚÚ#Sb¥"6f˜"ñÊ∆ñÊRÖD‘¬&WfñWp¢6ˆÁ7B&ˆ˜C÷6ˆÁFñÊW'«∆Fˆ7V÷VÁC∞¢&ˆ˜BÁVW'ï6V∆V7F˜$∆¬ÇrÊáF÷¬◊&WfñWr÷∆ˆC¶Ê˜BÖ∂FF÷∆ˆFVE“íríÊf˜$V6ÇÜV√”Á∞¢V¬Á6WDGG&ñ'WFRÇvFF÷∆ˆFVBr¬srì∞¢6ˆÁ7BFÉ÷V¬ÊFF6WBÁFÉ∞¢6ˆÁ7BfÊ÷S◊FÇÁ7∆óBÇrÚríÁ˜Çó««FÉ∞¢6ˆÁ7B6Ê’ˆ÷VFñ6ÊVW'íÜV¬íÁ&W∆6RÇı‚g6Ê“Ú¬rrì∞¢6ˆÁ7B÷VFñW&√’ˆ÷VFñ&WfñWuW&¬áFÇ«∑6Êß6Ê««VÊFVfñÊVG“ì∞¢6ˆÁ7B˜VÂW&√’ˆ÷VFñ&WfñWuW&¬áFÇ«∂ñÊ∆ñÊSßG'VR«6Êß6Ê««VÊFVfñÊVG“ì∞¢6ˆÁ7BF≈W&√’ˆ÷VFñ&WfñWuW&¬áFÇ«∂F˜vÊ∆ˆCßG'VR«6Êß6Ê««VÊFVfñÊVG“ì∞¢fWF6ÇÜ÷VFñW&¬¬∂66ÜS¢vÊÚ◊7F˜&Rw“ê¢ÁFÜV‚á#”Á∂ñbÇ"Êˆ≤íFá&˜rÊWrW'&˜"á"Á7FGW2ì≤&WGW&‚"ÁFWáBÇì∑“ê¢ÁFÜV‚ÜáF÷√”Á∞¢ñbÜáF÷¬Ê∆VÊwFÉ‰ÖD‘≈Ù‘Öı4ï§Ró∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“&áF÷¬◊&WfñWr÷f∆∆&6≤#„∆6∆73“&◊6r÷÷VFñ÷∆ñÊ≤"á&Vc“"G∂˜VÂW&«“"F&vWC“%ˆ&∆Ê≤"&V√“&Êˆ˜VÊW"#Ô	˘8‚G∂W62ÜfÊ÷Ró”¬ˆ„∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÇváF÷≈˜Fˆıˆ∆&vRró”¬˜7„„¬ˆFócÊ∞¢&WGW&„∞¢–¢6ˆÁ7B6fTáF÷√÷áF÷¬Á&W∆6RÇÚbˆr¬rf◊≤ríÁ&W∆6RÇÚ"ˆr¬rgV˜C≤ríÁ&W∆6RÇÛ¬ˆr¬rf«C≤ríÁ&W∆6RÇÛ‚ˆr¬rfwC≤rì∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“&áF÷¬◊&WfñWr◊w&#„∆Fób6∆73“&áF÷¬◊&WfñWr÷ÜVFW"#„«7„‚G∑BÇváF÷≈˜6ÊF&˜Öˆ∆&V¬ró”¬˜7„„∆á&Vc“"G∂˜VÂW&«“"F&vWC“%ˆ&∆Ê≤"&V√“&Êˆ˜VÊW""6∆73“&áF÷¬÷˜V‚÷∆ñÊ≤#‚G∑BÇváF÷≈ˆ˜VÂˆgV∆¬ró“(is¬ˆ„¬ˆFóc„∆ñg&÷R7&6Fˆ3“"G∑6fTáF÷«“"6ÊF&˜É“&∆∆˜r◊67&óG2"6∆73“&áF÷¬◊&WfñWr÷ñg&÷R"∆ˆFñÊs“&∆ßí#„¬ˆñg&÷S„¬ˆFócÊ∞¢“ê¢Ê6F6ÇÇÇì”Á∞¢V¬Ê˜WFW$ÖD‘√÷∆Fób6∆73“&áF÷¬◊&WfñWr÷f∆∆&6≤#„∆6∆73“&◊6r÷÷VFñ÷∆ñÊ≤"á&Vc“"G∂F≈W&«“"F˜vÊ∆ˆC“"G∂W62ÜfÊ÷Ró“#Ô	˘8‚G∂W62ÜfÊ÷Ró”¬ˆ„∆'#„«7‚7Gñ∆S“&6ˆ∆˜#ßf"Ç“÷◊WFVBì∂fˆÁB◊6ó¶S£'Ç#‚G∑BÇváF÷≈ˆW'&˜"ró”¬˜7„„¬ˆFócÊ∞¢“ì∞¢“ì∞ß–†¶gVÊ7Fñˆ‚&VÊFW$÷W&÷ñD&∆ˆ6∑2Ü6ˆÁFñÊW"ó∞¢6ˆÁ7B&ˆ˜C÷6ˆÁFñÊW'«∆Fˆ7V÷VÁC∞¢6ˆÁ7B&∆ˆ6∑3◊&ˆ˜BÁVW'ï6V∆V7F˜$∆¬ÇrÊ÷W&÷ñB÷&∆ˆ6≥¶Ê˜BÖ∂FF◊&VÊFW&VE“írì∞¢ñbÇ&∆ˆ6∑2Ê∆VÊwFÇí&WGW&„∞¢ñbÇˆ÷W&÷ñE&VGíó∞¢ñbÇˆ÷W&÷ñD∆ˆFñÊró∞¢ˆ÷W&÷ñD∆ˆFñÊs◊G'VS∞¢6ˆÁ7B67&óC÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw67&óBrì∞¢67&óBÁ7&3“váGG3¢Úˆ6F‚Êß6FV∆óg"ÊÊWBˆÁ“ˆ÷W&÷ñD„í„2ˆFó7Bˆ÷W&÷ñBÊ÷ñ‚Êß2s∞¢67&óBÊñÁFVw&óGì“w6Ü3ÉB’#c7¶d÷e7t§cGÑ5#uÜñíµW6&î$ñFîG§F'GÜñs&ÙuvfµCutÑ¶f‘BÙíˆVTÖßïBs∞¢67&óBÊ7&˜74˜&ñvñ„“vÊˆÁñ÷˜W2s∞¢67&óBÊˆÊ∆ˆC“Çì”Á∞¢ñbáGóVˆb÷W&÷ñB”“wVÊFVfñÊVBró∞¢÷W&÷ñBÊñÊóFñ∆ó¶Rá∑7F'Dˆ‰∆ˆC¶f«6R«FÜV÷S¶Fˆ7V÷VÁBÊFˆ7V÷VÁDV∆V÷VÁBÊ6∆74∆ó7BÊ6ˆÁFñÁ2ÇvF&≤rìÚvF&≤s¢vFVfV«Br«FÜV÷Uf&ñ&∆W3ß∞¢fˆÁDf÷ñ«ì¢vñÊÜW&óBr∆fˆÁE6ó¶S¢sGÇr¿¢&ñ÷'î6ˆ∆˜#¢r3FffRr«&ñ÷'ïFWáD6ˆ∆˜#¢r6S&SÜcr∆∆ñÊT6ˆ∆˜#¢r3sÉìbr¿¢6V6ˆÊF'î6ˆ∆˜#¢r3&C3sCÇr«FW'Fñ'î6ˆ∆˜#¢r3#&2r«&ñ÷'î&˜&FW$6ˆ∆˜#¢r3FSScÇr¿¢◊“ì∞¢ˆ÷W&÷ñE&VGì◊G'VS∞¢&VÊFW$÷W&÷ñD&∆ˆ6∑2Çì∞¢–¢”∞¢Fˆ7V÷VÁBÊÜVBÊVÊD6Üñ∆Bá67&óBì∞¢–¢&WGW&„∞¢–¢&∆ˆ6∑2Êf˜$V6ÇÜ7ñÊ2Ü&∆ˆ6≤ì”Á∞¢&∆ˆ6≤ÊFF6WBÁ&VÊFW&VC“wG'VRs∞¢6ˆÁ7B6ˆFS÷&∆ˆ6≤ÁFWáD6ˆÁFVÁC∞¢6ˆÁ7BñC÷&∆ˆ6≤ÊFF6WBÊ÷W&÷ñDñG«¬Çv““r¥÷FÇÁ&ÊFˆ“ÇíÁFı7G&ñÊrÉ3bíÁ6∆ñ6RÉ"íì∞¢G'ó∞¢6ˆÁ7B∑7fw”÷vóB÷W&÷ñBÁ&VÊFW"ÜñB∆6ˆFRì∞¢6ˆÁ7BF◊÷Fˆ7V÷VÁBÊvWDV∆V÷VÁD'îñBÇvBr∂ñBì∞¢ñbáF◊íF◊Á&V÷˜fRÇì∞¢&∆ˆ6≤ÊñÊÊW$ÖD‘√◊7fs∞¢6ˆÁ7B&VÊFW&VE7fr“&∆ˆ6≤ÁVW'ï6V∆V7F˜"Çw7frrì∞¢ñbá&VÊFW&VE7fríˆ÷˜VÁD÷W&÷ñEfñWvW"á&VÊFW&VE7fr¬∂÷ˆFS¢vñÊ∆ñÊRw“ì∞¢&∆ˆ6≤Ê6∆74∆ó7BÊFBÇv÷W&÷ñB◊&VÊFW&VBrì∞¢÷6F6ÇÜRó∞¢6ˆÁ7BF◊÷Fˆ7V÷VÁBÊvWDV∆V÷VÁD'îñBÇvBr∂ñBì∞¢ñbáF◊íF◊Á&V÷˜fRÇì∞¢ÚÚf∆¬&6≤FÚ6Ü˜vñÊr26ˆFR&∆ˆ6≤‚&V÷˜fRFÜR÷W&÷ñB÷&∂W"6Ú¢ÚÚ∆FW"&VÊFW"726ÊÊ˜B&WG'íFÜó2«&VGí÷fñ∆VB&∆ˆ6≤‡¢&∆ˆ6≤Ê6∆74∆ó7BÁ&V÷˜fRÇv÷W&÷ñB÷&∆ˆ6≤rì∞¢&∆ˆ6≤Ê6∆74∆ó7BÊFBÇw&Ww&rì∞¢&∆ˆ6≤ÊñÊÊW$ÖD‘√÷∆Fób6∆73“'&R÷ÜVFW"#Ê÷W&÷ñC¬ˆFóc„«&S„∆6ˆFS‚G∂W62Ü6ˆFRó”¬ˆ6ˆFS„¬˜&SÊ∞¢–¢“ì∞ß–†¶∆WBˆ∂FWÑ∆ˆFñÊs÷f«6S∞¶∆WBˆ∂FWÖ&VGì÷f«6S∞†¶gVÊ7Fñˆ‚ˆó57G&V÷ñÊtWVFñˆÂVÊFñÊrÜV¬«&ˆ˜Bó∞¢6ˆÁ7BFtÊ÷S“ÜV¬bfV¬ÁFtÊ÷W«¬rríÁFÙ∆˜vW$66RÇì∞¢ñbáFtÊ÷R”“vWVFñˆ‚÷&∆ˆ6≤rbgFtÊ÷R”“vWVFñˆ‚÷ñÊ∆ñÊRrí&WGW&‚f«6S∞¢ÚÚ7G&V÷ñÊr÷÷&∂F˜v‚fñ∆«27W7Fˆ“WVFñˆ‚V∆V÷VÁG2vÜñ∆RFÜR'6W"˜vÁ2FÜP¢ÚÚ˜V‚ÊˆFR‚ñbFÜRWVFñˆ‚ó27W'&VÁF«íFÜR∆7BFW66VÊFÁBˆbFÜR∆ófP¢ÚÚ76ó7FÁB&ˆGí¬vR6ÊÊ˜BFV∆¬vÜWFÜW"÷˜&RFUÇó27Fñ∆¬6ˆ÷ñÊr‚6∂óó@¢ÚÚGW&ñÊr∆ófRFV&˜VÊ6R76W26Ú'Fñ¬6˜W&6Ró2Ê˜BW&÷ÊVÁF«í÷&∂V@¢ÚÚFF◊&VÊFW&VB&Vf˜&RFÜRfñÊ¬'6W%ˆVÊBf«W6Ç‡¢∆WBÊˆFS÷V√∞¢vÜñ∆RÜÊˆFRbfÊˆFR”◊&ˆ˜Bó∞¢ñbÜÊˆFRÊÊWáE6ñ&∆ñÊrí&WGW&‚f«6S∞¢ÊˆFS÷ÊˆFRÁ&VÁDÊˆFS∞¢–¢&WGW&‚&ˆˆ∆V‚ÜÊˆFS””◊&ˆ˜Bì∞ß–†¶gVÊ7Fñˆ‚&VÊFW$∂FWÑ&∆ˆ6∑2Ü6ˆÁFñÊW"∆˜FñˆÁ2ó∞¢6ˆÁ7B&ˆ˜C÷6ˆÁFñÊW'«∆Fˆ7V÷VÁC∞¢6ˆÁ7B7G&V÷ñÊs‘&ˆˆ∆V‚Ü˜FñˆÁ2bf˜FñˆÁ2Á7G&V÷ñÊrì∞¢6ˆÁ7B&∆ˆ6∑3◊&ˆ˜BÁVW'ï6V∆V7F˜$∆¬Ä¢rÊ∂FWÇ÷&∆ˆ6≥¶Ê˜BÖ∂FF◊&VÊFW&VE“í¬Ê∂FWÇ÷ñÊ∆ñÊS¶Ê˜BÖ∂FF◊&VÊFW&VE“í¬r∞¢vWVFñˆ‚÷&∆ˆ6≥¶Ê˜BÖ∂FF◊&VÊFW&VE“í∆WVFñˆ‚÷ñÊ∆ñÊS¶Ê˜BÖ∂FF◊&VÊFW&VE“íp¢ì∞¢ñbÇ&∆ˆ6∑2Ê∆VÊwFÇí&WGW&„∞¢ñbÇˆ∂FWÖ&VGíó∞¢ñbÇˆ∂FWÑ∆ˆFñÊró∞¢ˆ∂FWÑ∆ˆFñÊs◊G'VS∞¢6ˆÁ7B67&óC÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw67&óBrì∞¢67&óBÁ7&3“w7FFñ2˜fVÊF˜"ˆ∂FWÇÛ„b„#"ˆ∂FWÇÊ÷ñ‚Êß2s∞¢67&óBÊñÁFVw&óGì“w6Ü3ÉB÷4÷∑fDCÑ∆˜Ög§tbı%T¥7f÷”Cîe˜átDc4$t∑DEÜ4V2µC#$‚∑FVÇÙÙ¶gSß#bs∞¢67&óBÊ7&˜74˜&ñvñ„“vÊˆÁñ÷˜W2s∞¢67&óBÊˆÊ∆ˆC“Çì”Á∞¢ñbáGóVˆb∂FWÇ”“wVÊFVfñÊVBró∞¢ˆ∂FWÖ&VGì◊G'VS∞¢&VÊFW$∂FWÑ&∆ˆ6∑2Çì∞¢–¢”∞¢Fˆ7V÷VÁBÊÜVBÊVÊD6Üñ∆Bá67&óBì∞¢–¢&WGW&„∞¢–¢&∆ˆ6∑2Êf˜$V6ÇÜV√”Á∞¢ñbá7G&V÷ñÊrbeˆó57G&V÷ñÊtWVFñˆÂVÊFñÊrÜV¬«&ˆ˜Bíí&WGW&„∞¢V¬ÊFF6WBÁ&VÊFW&VC“wG'VRs∞¢6ˆÁ7B7&3÷V¬ÁFWáD6ˆÁFVÁG«¬rs∞¢6ˆÁ7BFtÊ÷S“ÜV¬ÁFtÊ÷W«¬rríÁFÙ∆˜vW$66RÇì∞¢6ˆÁ7BFó7∆î÷ˆFS÷V¬ÊFF6WBÊ∂FWÉ””“vFó7∆íw««FtÊ÷S””“vWVFñˆ‚÷&∆ˆ6≤s∞¢G'ó∞¢∂FWÇÁ&VÊFW"á7&2∆V¬«∞¢Fó7∆î÷ˆFR¿¢Fá&˜tˆ‰W'&˜#¶f«6R¿¢G'W7C¶f«6R¿¢7G&ñ7C¢vñvÊ˜&Rr¿¢“ì∞¢÷6F6ÇÜRó∞¢ÚÚ∆VfR2&rFWáBñ‚6ˆFR7‚ˆ‚fñ«W&P¢V¬Ê˜WFW$ÖD‘√÷∆6ˆFS‚G∂W62á7&2ó”¬ˆ6ˆFSÊ∞¢–¢“ì∞ß–†¶gVÊ7Fñˆ‚˜FÜñÊ∂ñÊt÷&∑WáFWáC“rró∞¢6ˆÁ7B6∆V„’˜6ÊóFó¶UFÜñÊ∂ñÊtFó7∆ïFWáBáFWáBì∞¢6ˆÁ7B˜V‰6∆73’˜v˜&∂∆ˆtFWFñ«4WáÊFVDFVfV«BÇìÚr˜V‚s¢rs∞¢&WGW&‚Ü6∆V‚be7G&ñÊrÜ6∆V‚íÁG&ñ“Çíê¢Ú∆Fób6∆73“'FÜñÊ∂ñÊr÷6&BG∂˜V‰6∆77“#„∆Fób6∆73“'FÜñÊ∂ñÊr÷6&B÷ÜVFW""ˆÊ6∆ñ6≥“'FÜó2Á&VÁDV∆V÷VÁBÊ6∆74∆ó7BÁFˆvv∆RÇv˜V‚rí#„«7‚6∆73“'FÜñÊ∂ñÊr÷6&B÷ñ6ˆ‚#‚G∂∆íÇv∆ñváF'V∆"r√Bó”¬˜7„„«7‚6∆73“'FÜñÊ∂ñÊr÷6&B÷∆&V¬#‚G∑BÇwFÜñÊ∂ñÊrró”¬˜7„„«7‚6∆73“'FÜñÊ∂ñÊr÷6&B◊Fˆvv∆R#‚G∂∆íÇv6ÜWg&ˆ‚◊&ñváBr√"ó”¬˜7„„¬ˆFóc„∆Fób6∆73“'FÜñÊ∂ñÊr÷6&B÷&ˆGí#„«&S‚G∂W62Ö7G&ñÊrÜ6∆V‚íÁG&ñ“Çíó”¬˜&S„¬ˆFóc„¬ˆFócÊ ¢¢∆Fób6∆73“'FÜñÊ∂ñÊr#„∆Fób6∆73“&F˜B#„¬ˆFóc„∆Fób6∆73“&F˜B#„¬ˆFóc„∆Fób6∆73“&F˜B#„¬ˆFóc„¬ˆFócÊ∞ß–¶gVÊ7Fñˆ‚˜&VÊFW%FÜñÊ∂ñÊtñÁFÚá&˜r«FWáC“rró∞¢ñbÇ&˜rí&WGW&„∞¢6ˆÁ7B6∆V„’˜6ÊóFó¶UFÜñÊ∂ñÊtFó7∆ïFWáBáFWáBì∞¢ñbÇ6∆V‚ó∞¢&˜rÊñÊÊW$ÖD‘√’˜FÜñÊ∂ñÊt÷&∑WáFWáBì∞¢&WGW&„∞¢–¢6ˆÁ7B&S◊&˜rÁVW'ï6V∆V7F˜"ÇrÁFÜñÊ∂ñÊr÷6&B÷&ˆGí&Rrì∞¢ñbá&Ró∞¢&RÁFWáD6ˆÁFVÁC÷6∆V„∞¢&WGW&„∞¢–¢&˜rÊñÊÊW$ÖD‘√’˜FÜñÊ∂ñÊt÷&∑WáFWáBì∞ß–¶gVÊ7Fñˆ‚fñÊ∆ó¶UFÜñÊ∂ñÊt6&BÇó∞¢ÚÚwV&C¢ˆÊ«ífñÊ∆ó¶RFÜñÊ∂ñÊr6&BñbvRw&R∆ˆˆ∂ñÊrBFÜR6W76ñˆ‚FÜB7F'FVBóB‡¢ÚÚvóFÜ˜WBFÜó26ÜV6≤¬7vóF6ÜñÊrF'2vÜñ∆R7G&V“ó2'VÊÊñÊr6W6W2fñÊ∆ó¶UFÜñÊ∂ñÊt6&@¢ÚÚFÚ&V÷˜fRˆ÷ˆFñgíFÜRFÜñÊ∂ñÊr6&BDÙ“ˆbFÜRw&ˆÊr6W76ñˆ‚(	BFÜR6&B&V∆ˆÊw2FÚFÜP¢ÚÚ7G&V“FÜB7F'FVBóB¬Ê˜BFÜR6W76ñˆ‚7W'&VÁF«íFó7∆ñVB‡¢6ˆÁ7BˆwV&EGW&‚“BÇv∆ófT76ó7FÁEGW&‚rì∞¢ñbÖˆwV&EGW&‚bb2Á6W76ñˆ‚bbˆwV&EGW&‚ÊFF6WBÁ6W76ñˆ‰ñB”“2Á6W76ñˆ‚Á6W76ñˆÂˆñBí&WGW&„∞¢ñbÜó5G&Á7&VÁE7G&V“Çíó∞¢6ˆÁ7B&˜s“BÇwFÜñÊ∂ñÊu&˜rrì∞¢ñbá&˜ró∞¢&˜rÁ&V÷˜fTGG&ñ'WFRÇvñBrì∞¢&˜rÁ&V÷˜fTGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRrì∞¢&˜rÁ&V÷˜fTGG&ñ'WFRÇvFF÷∆ófR◊FÜñÊ∂ñÊrrì∞¢–¢&WGW&„∞¢–¢ñbÇó56ñ◊∆ñfñVEFˆˆƒ6∆∆ñÊrÇíó∞¢6ˆÁ7B&˜s“BÇwFÜñÊ∂ñÊu&˜rrì∞¢ñbÇ&˜rí&WGW&„∞¢ÚÚñbFÜR&˜ró27Fñ∆¬ßW7B7ñÊÊW"ÜÊÚFÜñÊ∂ñÊr6ˆÁFVÁB&VÊFW&VBí¿¢ÚÚ&V÷˜fRóBVÁFó&V«í(	BóBw2FÜRñÊóFñ¬vóFñÊrF˜G2‡¢6ˆÁ7BÜ46ˆÁFVÁC“&˜rÁVW'ï6V∆V7F˜"ÇrÁFÜñÊ∂ñÊr÷6&Brì∞¢ñbÇÜ46ˆÁFVÁBbb&˜rÊvWDGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRrì””“sró∞¢&˜rÁ&V÷˜fRÇì∞¢&WGW&„∞¢–¢ÚÚñbFÜRW6W"v2vF6ÜñÊrá67&ˆ∆¬ñÊÊVB“B&˜GFˆ“í¬67&ˆ∆¬FÜRFÜñÊ∂ñÊp¢ÚÚ6&B&6≤FÚFÜRF˜6ÚFÜR6ˆ◊∆WFVB&W7ˆÁ6Ró2fó6ñ&∆RVÊFW&ÊVFÇvóFÜ˜W@¢ÚÚFÜRFÜñÊ∂ñÊr6ˆÁFVÁB&∆ˆ6∂ñÊróB‚ñbFÜWí67&ˆ∆∆VBWFÚ&VBÜó7F˜'í¿¢ÚÚ∆VfRFÜVó"67&ˆ∆¬˜6óFñˆ‚ñÁF7B‡¢ñbÖ˜67&ˆ∆≈ñÊÊVBó∞¢6ˆÁ7B&ˆGì◊&˜rbg&˜rÁVW'ï6V∆V7F˜"ÇrÁFÜñÊ∂ñÊr÷6&B÷&ˆGírì∞¢ñbÜ&ˆGíí&ˆGíÁ67&ˆ∆≈F˜”∞¢–¢&˜rÁ&V÷˜fTGG&ñ'WFRÇvñBrì∞¢&˜rÁ&V÷˜fTGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRrì∞¢&WGW&„∞¢–¢6ˆÁ7BGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢6ˆÁ7Bw&˜W◊GW&‚bgGW&‚ÁVW'ï6V∆V7F˜"ÇrÊ∆ófR◊v˜&∂∆ˆu∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%“¬ÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%“¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%“rì∞¢ñbÜw&˜Wó∞¢6ˆÁ7B7FófU&V6ˆ„◊GW&‚ÁVW'ï6V∆V7F˜"ÇrÁv¬◊&V6ˆÂ∂FF◊v˜&∂∆ˆr◊&V6ˆ‚÷7FófS“#%“rì∞¢ñbÜ7FófU&V6ˆ‚í7FófU&V6ˆ‚Á&V÷˜fTGG&ñ'WFRÇvFF◊v˜&∂∆ˆr◊&V6ˆ‚÷7FófRrì∞¢GW&‚ÁVW'ï6V∆V7F˜$∆¬ÇrÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊu∂FF◊FÜñÊ∂ñÊr÷7FófS“#%“ríÊf˜$V6ÇÜ7FófS”Á∞¢7FófRÁ&V÷˜fTGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRrì∞¢7FófRÁ&V÷˜fTGG&ñ'WFRÇvFF÷∆ófR◊FÜñÊ∂ñÊrrì∞¢“ì∞¢˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íÜw&˜Wì∞¢–ß–¶gVÊ7Fñˆ‚VÊEFÜñÊ∂ñÊráFWáC“rr¬˜FñˆÁ2ó∞¢ÚÚwV&C¢ñvÊ˜&Rñb6W76ñˆ‚v27vóF6ÜVBGW&ñÊr‚7ñÊ254R7G&V“‡¢ÚÚFÜRˆ∆B7G&V“w2&V6ˆÊñÊrWfVÁG26‚7Fñ∆¬fó&RgFW"7vóF6É∞¢ÚÚvóFÜ˜WBFÜó26ÜV6≤FÜWív˜V∆Bˆ∆«WFRFÜRÊWr6W76ñˆ‚w2DÙ“‡¢˜FñˆÁ3÷˜FñˆÁ7««∑”∞¢6ˆÁ7B∆∆˜uVÊFñÊu∆6VÜˆ∆FW#“Ü˜FñˆÁ2bf˜FñˆÁ2ÁVÊFñÊs””◊G'VRì∞¢6ˆÁ7BÊ6Ü˜%&VÊFW$f∆∆&6≥“Ü˜FñˆÁ2bf˜FñˆÁ2ÊÊ6Ü˜%&VÊFW$f∆∆&6≥””◊G'VRì∞¢ñbáGóVˆbó4fñÊƒÁ7vW$ˆÊ«î÷ˆFS””“vgVÊ7Fñˆ‚rbfó4fñÊƒÁ7vW$ˆÊ«î÷ˆFRÇíí&WGW&„∞¢ñbÇ2Á6W76ñˆÁ«¬Ç2Ê7FófU7G&V‘ñBbb∆∆˜uVÊFñÊu∆6VÜˆ∆FW"íí&WGW&„∞¢ñbÜ˜FñˆÁ2Á6W76ñˆ‰ñBbe7G&ñÊrÜ˜FñˆÁ2Á6W76ñˆ‰ñBí”’7G&ñÊrÖ2Á6W76ñˆ‚Á6W76ñˆÂˆñG«¬rríí&WGW&„∞¢ñbÜ˜FñˆÁ2Á7G&V‘ñBbe7G&ñÊrÜ˜FñˆÁ2Á7G&V‘ñBí”’7G&ñÊrÖ2Ê7FófU7G&V‘ñG«¬rríí&WGW&„∞¢6ˆÁ7BWÜó7FñÊt∆ófUGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢ñbÜÊ6Ü˜%&VÊFW$f∆∆&6≤bfWÜó7FñÊt∆ófUGW&‚bfWÜó7FñÊt∆ófUGW&‚ÊFF6WBb`¢WÜó7FñÊt∆ófUGW&‚ÊFF6WBÁ6W76ñˆ‰ñBb`¢7G&ñÊrÜWÜó7FñÊt∆ófUGW&‚ÊFF6WBÁ6W76ñˆ‰ñBí”’7G&ñÊrÖ2Á6W76ñˆ‚Á6W76ñˆÂˆñG«¬rríó∞¢ñbÇ˜&W6WD÷ó6÷F6ÜVD∆ófT76ó7FÁEGW&‰f˜%6W76ñˆ‚ÜWÜó7FñÊt∆ófUGW&‚¬2Á6W76ñˆ‚Á6W76ñˆÂˆñBíí&WGW&„∞¢–¢ñbÜÊ6Ü˜%&VÊFW$f∆∆&6≤bfWÜó7FñÊt∆ófUGW&‚be˜WFFT∆ófTÊ6Ü˜%&V6ˆÊñÊu&˜tf˜$f∆∆&6≤ÜWÜó7FñÊt∆ófUGW&‚«FWáB∆˜FñˆÁ2íí&WGW&„∞¢ñbÇ∆∆˜uVÊFñÊu∆6VÜˆ∆FW"bbÊ6Ü˜%&VÊFW$f∆∆&6≤bfó4∆ófTÊ6Ü˜$7FófóGï66VÊT˜vÊW"Ö2Ê7FófU7G&V‘ñBíó∞¢˜&VÊFW$∆ófTÊ6Ü˜$7FófóGï66VÊTf˜%7G&V“Ö2Ê7FófU7G&V‘ñB¬2Á6W76ñˆ‚Á6W76ñˆÂˆñBì∞¢&WGW&„∞¢–¢6ˆÁ7BV◊Gì“BÇvV◊Gï7FFRrì∞¢ñbÜV◊GííV◊GíÁ7Gñ∆RÊFó7∆ì“vÊˆÊRs∞¢ñbÇó56ñ◊∆ñfñVEFˆˆƒ6∆∆ñÊrÇíó∞¢∆WB&˜s“BÇwFÜñÊ∂ñÊu&˜rrì∞¢ñbÇ&˜ró∞¢&˜s÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢&˜rÊñC“wFÜñÊ∂ñÊu&˜rs∞¢&˜rÊ6∆74Ê÷S“wFÜñÊ∂ñÊr÷6&B◊&˜rs∞¢6ˆÁ7BñÊÊW#“BÇv◊6tñÊÊW"rì∞¢ñbÜñÊÊW"íñÊÊW"ÊVÊD6Üñ∆Bá&˜rì∞¢–¢&˜rÁ6WDGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRr¬srì∞¢˜&VÊFW%FÜñÊ∂ñÊtñÁFÚá&˜r«FWáBì∞¢ñbáGóVˆb67&ˆ∆ƒñeñÊÊVC””“vgVÊ7Fñˆ‚rí67&ˆ∆ƒñeñÊÊVBÇì∞¢&WGW&„∞¢–¢∆WBGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢ñbÇGW&‚ó∞¢GW&„’ˆ7&VFT76ó7FÁEGW&‚Çì∞¢GW&‚ÊñC“v∆ófT76ó7FÁEGW&‚s∞¢ñbÖ2Á6W76ñˆ‚íGW&‚ÊFF6WBÁ6W76ñˆ‰ñC’2Á6W76ñˆ‚Á6W76ñˆÂˆñC∞¢6ˆÁ7BñÊÊW#“BÇv◊6tñÊÊW"rì∞¢ñbÜñÊÊW"íñÊÊW"ÊVÊD6Üñ∆BáGW&‚ì∞¢–¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÇ&∆ˆ6∑2í&WGW&„∞¢6ˆÁ7B6∆V„’˜6ÊóFó¶UFÜñÊ∂ñÊtFó7∆ïFWáBáFWáBì∞¢ñbÜ6∆V‚bgvñÊF˜rÂ˜6Ü˜uFÜñÊ∂ñÊr”÷f«6Ró∞¢6ˆÁ7B6Vv÷VÁE6W÷˜FñˆÁ2Á6Vv÷VÁE6W”◊VÊFVfñÊVBbf˜FñˆÁ2Á6Vv÷VÁE6W”÷ÁV∆√ı7G&ñÊrÜ˜FñˆÁ2Á6Vv÷VÁE6Wì¢rs∞¢6ˆÁ7B'W'7DñC÷˜FñˆÁ2Ê'W'7DñB”◊VÊFVfñÊVBbf˜FñˆÁ2Ê'W'7DñB”÷ÁV∆√ı7G&ñÊrÜ˜FñˆÁ2Ê'W'7DñBì¢rs∞¢6ˆÁ7BFÜñÊ∂ñÊt∂Wì’7G&ñÊrÜ˜FñˆÁ2ÁFÜñÊ∂ñÊt∂Wó«¬Ä¢6Vv÷VÁE6Wˆ6Vv÷VÁC¢G∑6Vv÷VÁE6W÷†¢'W'7DñCˆ'W'7C¢G∂'W'7DñG÷†¢wGW&‚p¢íì∞¢ñbÜó5G&Á7&VÁE7G&V“Çíó∞¢∆WB&˜s÷&∆ˆ6∑2ÁVW'ï6V∆V7F˜"ÜÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊu∂FF÷∆ófR◊FÜñÊ∂ñÊs“#%’∂FF÷∆ófR◊FÜñÊ∂ñÊr÷∂Wì“"G¥552ÊW66RáFÜñÊ∂ñÊt∂Wíó“%÷ì∞¢ñbÇ&˜ró∞¢&˜s’˜FÜñÊ∂ñÊt7FófóGîÊˆFRÜ6∆V‚¬f«6Rì∞¢&˜rÊñC“wFÜñÊ∂ñÊu&˜rs∞¢&˜rÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊FÜñÊ∂ñÊrr¬srì∞¢&˜rÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊FÜñÊ∂ñÊr÷∂Wír«FÜñÊ∂ñÊt∂Wíì∞¢ñbá6Vv÷VÁE6Wí&˜rÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wr«6Vv÷VÁE6Wì∞¢ñbÜ'W'7DñBí&˜rÁ6WDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBr∆'W'7DñBì∞¢&∆ˆ6∑2ÁVW'ï6V∆V7F˜$∆¬ÇrÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊu∂FF◊FÜñÊ∂ñÊr÷7FófS“#%“ríÊf˜$V6ÇÜV√”Á∞¢ñbÜV¬”◊&˜ró∞¢V¬Á&V÷˜fTGG&ñ'WFRÇvñBrì∞¢V¬Á&V÷˜fTGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRrì∞¢V¬Á&V÷˜fTGG&ñ'WFRÇvFF÷∆ófR◊FÜñÊ∂ñÊrrì∞¢–¢“ì∞¢&˜rÁ6WDGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRr¬srì∞¢6ˆÁ7B∆ófTfˆ˜FW#÷&∆ˆ6∑2ÁVW'ï6V∆V7F˜"Çr6∆ófU'VÂ7FGW2rì∞¢ñbÜ∆ófTfˆ˜FW"bf∆ófTfˆ˜FW"Á&VÁDV∆V÷VÁC””÷&∆ˆ6∑2í&∆ˆ6∑2ÊñÁ6W'D&Vf˜&Rá&˜r∆∆ófTfˆ˜FW"ì∞¢V«6R&∆ˆ6∑2ÊVÊD6Üñ∆Bá&˜rì∞¢÷V«6W∞¢˜&VÊFW%FÜñÊ∂ñÊtñÁFÚá&˜r¬6∆V‚ì∞¢–¢&˜rÊñC“wFÜñÊ∂ñÊu&˜rs∞¢&˜rÁ6WDGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRr¬srì∞¢6ˆÁ7BWÜó7FñÊtWfVÁDC◊&˜rÊvWDGG&ñ'WFRÇvFF÷WfVÁB÷Brì∞¢6ˆÁ7BÊWáEG3’ˆfó'7Ef∆ñEFñ÷W7F◊6V6ˆÊG2Ä¢˜FñˆÁ2ÁG2¿¢˜FñˆÁ2ÁFñ÷W7F◊¿¢˜FñˆÁ2Ê7&VFVEˆB¿¢WÜó7FñÊtWfVÁD@¢ì∞¢ˆFV6˜&FUG&Á7&VÁDWfVÁE&˜rá&˜r«∞¢GóS¢wFÜñÊ∂ñÊrr¿¢FWáC¶6∆V‚¿¢&WfñWs¶6∆V‚¿¢G3¶ÊWáEG7««VÊFVfñÊVB¿¢∆ófSßG'VR¿¢6Vv÷VÁE6W¿¢'W'7DñB¿¢“ì∞¢˜7ñÊ5G&Á7&VÁDWfVÁD6ˆÁG&ˆ«2áGW&‚ì∞¢ñbáGóVˆb67&ˆ∆ƒñeñÊÊVC””“vgVÊ7Fñˆ‚rí67&ˆ∆ƒñeñÊÊVBÇì∞¢&WGW&„∞¢–¢6ˆÁ7Bw&˜W÷VÁ7W&T∆ófUv˜&∂∆ˆt6ˆÁFñÊW"Ü&∆ˆ6∑2«∞¢7FófóGî∂Wì¶˜FñˆÁ2Ê7FófóGî∂Wó«¬Ö2Ê7FófU7G&V‘ñCÚv∆ófS¢rµ2Ê7FófU7G&V‘ñC¶ÁV∆¬í¿¢“ì∞¢6ˆÁ7B∆ó7C’˜Fˆˆ≈v˜&∂∆ˆt∆ó7DV¬Üw&˜Wì∞¢ñbÜ∆ó7Bó∞¢∆WB&˜s÷∆ó7BÁVW'ï6V∆V7F˜"ÜÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊu∂FF÷∆ófR◊FÜñÊ∂ñÊs“#%’∂FF÷∆ófR◊FÜñÊ∂ñÊr÷∂Wì“"G¥552ÊW66RáFÜñÊ∂ñÊt∂Wíó“%÷ì∞¢ñbÇ&˜ró∞¢&˜s’˜FÜñÊ∂ñÊt7FófóGîÊˆFRÜ6∆V‚¬f«6R¬FÜñÊ∂ñÊt∂Wíì∞¢&˜rÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊FÜñÊ∂ñÊrr¬srì∞¢&˜rÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊FÜñÊ∂ñÊr÷∂Wír«FÜñÊ∂ñÊt∂Wíì∞¢ñbá6Vv÷VÁE6Wí&˜rÁ6WDGG&ñ'WFRÇvFF÷∆ófR◊6Vv÷VÁB◊6Wr«6Vv÷VÁE6Wì∞¢ñbÜ'W'7DñBí&˜rÁ6WDGG&ñ'WFRÇvFF÷7FófóGí÷'W'7B÷ñBr∆'W'7DñBì∞¢∆ó7BÁVW'ï6V∆V7F˜$∆¬ÇrÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊu∂FF◊FÜñÊ∂ñÊr÷7FófS“#%“ríÊf˜$V6ÇÜV√”Á∞¢ñbÜV¬”◊&˜ró∞¢V¬Á&V÷˜fTGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRrì∞¢V¬Á&V÷˜fTGG&ñ'WFRÇvFF÷∆ófR◊FÜñÊ∂ñÊrrì∞¢–¢“ì∞¢&˜rÁ6WDGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRr¬srì∞¢∆ó7BÊVÊD6Üñ∆Bá&˜rì∞¢÷V«6W∞¢˜&VÊFW%FÜñÊ∂ñÊtñÁFÚá&˜r¬6∆V‚ì∞¢–¢&˜rÁ6WDGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRr¬srì∞¢˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íÜw&˜Wì∞¢–¢–¢ñbáGóVˆb67&ˆ∆ƒñeñÊÊVC””“vgVÊ7Fñˆ‚rí67&ˆ∆ƒñeñÊÊVBÇì∞ß–¶gVÊ7Fñˆ‚WFFUFÜñÊ∂ñÊráFWáC“rr¬˜FñˆÁ2ó∂VÊEFÜñÊ∂ñÊráFWáB¬˜FñˆÁ2ì∑–¶gVÊ7Fñˆ‚&V÷˜fUFÜñÊ∂ñÊrÇó∞¢ñbÜó5G&Á7&VÁE7G&V“Çíó∞¢6ˆÁ7B∆ófUGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2Ü∆ófUGW&‚ì∞¢ñbÜ&∆ˆ6∑2í&∆ˆ6∑2ÁVW'ï6V∆V7F˜$∆¬ÇrÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊu∂FF◊FÜñÊ∂ñÊr÷7FófS“#%“ríÊf˜$V6Çá&˜s”Á∞¢&˜rÁ&V÷˜fTGG&ñ'WFRÇvñBrì∞¢&˜rÁ&V÷˜fTGG&ñ'WFRÇvFF◊FÜñÊ∂ñÊr÷7FófRrì∞¢&˜rÁ&V÷˜fTGG&ñ'WFRÇvFF÷∆ófR◊FÜñÊ∂ñÊrrì∞¢“ì∞¢ñbÜ∆ófUGW&‚bf&∆ˆ6∑2bb&∆ˆ6∑2Ê6Üñ∆G&V‚Ê∆VÊwFÇí∆ófUGW&‚Á&V÷˜fRÇì∞¢&WGW&„∞¢–¢ñbÇó56ñ◊∆ñfñVEFˆˆƒ6∆∆ñÊrÇíó∞¢6ˆÁ7BV√“BÇwFÜñÊ∂ñÊu&˜rrì∞¢ñbÜV¬íV¬Á&V÷˜fRÇì∞¢6ˆÁ7B∆ófUGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2Ü∆ófUGW&‚ì∞¢ñbÜ∆ófUGW&‚bf&∆ˆ6∑2bb&∆ˆ6∑2Ê6Üñ∆G&V‚Ê∆VÊwFÇí∆ófUGW&‚Á&V÷˜fRÇì∞¢&WGW&„∞¢–¢6ˆÁ7BGW&„“BÇv∆ófT76ó7FÁEGW&‚rì∞¢6ˆÁ7B&∆ˆ6∑3’ˆ76ó7FÁEGW&‰&∆ˆ6∑2áGW&‚ì∞¢ñbÜ&∆ˆ6∑2í&∆ˆ6∑2ÁVW'ï6V∆V7F˜$∆¬ÇrÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊs¶Ê˜BÖ∂FF÷Ê6Ü˜"◊66VÊR◊&˜s“#%“íríÊf˜$V6ÇÜV√”ÊV¬Á&V÷˜fRÇíì∞¢ñbÜ&∆ˆ6∑2í&∆ˆ6∑2ÁVW'ï6V∆V7F˜$∆¬ÇrÁv¬◊&V6ˆÂ∂FF◊v˜&∂∆ˆr÷Ê6Ü˜"◊&V6ˆ„“#%“¬Áv¬◊&V6ˆÂ∂FF◊v˜&∂∆ˆr◊&V6ˆ‚◊6˜W&6S“'&V6ˆÊñÊr%“ríÊf˜$V6ÇÜV√”ÊV¬Á&V÷˜fRÇíì∞¢ñbÜ&∆ˆ6∑2í&∆ˆ6∑2ÁVW'ï6V∆V7F˜$∆¬ÇrÊ∆ófR◊v˜&∂∆ˆu∂FF÷∆ófR◊v˜&∂∆ˆr◊6ÜV∆√“#%“¬ÁFˆˆ¬◊v˜&∂∆ˆr÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%”¶Ê˜BÖ∂FF÷Ê6Ü˜"◊66VÊR÷˜vÊW#“#%“í¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷∆ófR◊Fˆˆ¬÷6∆¬÷w&˜W“#%”¶Ê˜BÖ∂FF÷Ê6Ü˜"◊66VÊR÷˜vÊW#“#%“í¬ÁFˆˆ¬÷6∆¬÷w&˜W∂FF÷vVÁB÷7FófóGí÷w&˜W“#%”¶Ê˜BÖ∂FF÷Ê6Ü˜"◊66VÊR÷˜vÊW#“#%“íríÊf˜$V6ÇÜw&˜W”Á∞¢˜7ñÊ5Fˆˆƒ6∆ƒw&˜W7V÷÷'íÜw&˜Wì∞¢ñbÇw&˜WÁVW'ï6V∆V7F˜"ÇrÁFˆˆ¬÷6&B◊&˜r¬ÊvVÁB÷7FófóGí◊FÜñÊ∂ñÊr¬Áv¬◊&V6ˆ‚ríó∞¢ñbáGóVˆbˆ6∆V$7FófóGîV∆6VEFñ÷W#””“vgVÊ7Fñˆ‚ríˆ6∆V$7FófóGîV∆6VEFñ÷W"Çì∞¢w&˜WÁ&V÷˜fRÇì∞¢–¢“ì∞¢ñbáGW&‚bf&∆ˆ6∑2bb&∆ˆ6∑2Ê6Üñ∆G&V‚Ê∆VÊwFÇíGW&‚Á&V÷˜fRÇì∞ß–†¶gVÊ7Fñˆ‚fñ∆Tñ6ˆ‚ÜÊ÷R¬GóRó∞¢ñbáGóS””“vFó"rí&WGW&‚∆íÇvfˆ∆FW"r√Bì∞¢6ˆÁ7BS÷fñ∆TWáBÜÊ÷Rì∞¢ñbÑî‘tUÙUÖE2ÊÜ2ÜRíí&WGW&‚∆íÇvñ÷vRr√Bì∞¢ñbÑ‘EÙUÖE2ÊÜ2ÜRíí&WGW&‚∆íÇvfñ∆R◊FWáBr√Bì∞¢ñbáGóVˆbDıt‰ƒÙEÙUÖE2”“wVÊFVfñÊVBrbdDıt‰ƒÙEÙUÖE2ÊÜ2ÜRíí&WGW&‚∆íÇvF˜vÊ∆ˆBr√Bì∞¢ñbÜS””“rÁírí&WGW&‚∆íÇvfñ∆R÷6ˆFRr√Bì∞¢ñbÜS””“rÊß2w«∆S””“rÁG2w«∆S””“rÊß7Çw«∆S””“rÁG7Çrí&WGW&‚∆íÇw¶r√Bì∞¢ñbÜS””“rÊß6ˆ‚w«∆S””“rÁñ÷¬w«∆S””“rÁñ÷¬w«∆S””“rÁFˆ÷¬rí&WGW&‚∆íÇw6WGFñÊw2r√Bì∞¢ñbÜS””“rÁ6Çw«∆S””“rÊ&6Çrí&WGW&‚∆íÇwFW&÷ñÊ¬r√Bì∞¢ñbÜS””“rÁFbrí&WGW&‚∆íÇvF˜vÊ∆ˆBr√Bì∞¢&WGW&‚∆íÇvfñ∆R◊FWáBr√Bì∞ß–†¶gVÊ7Fñˆ‚&VÊFW$'&VF7'V÷"Çó∞¢6ˆÁ7B&#“BÇv'&VF7'V÷$&"rì∞¢6ˆÁ7BW'F„“BÇv'FÂWFó"rì∞¢ñbÇ&"ó&WGW&„∞¢ñbÖ2Ê7W'&VÁDFó#””“r‚ró∞¢&"Á7Gñ∆RÊFó7∆ì“vÊˆÊRs∞¢ñbáW'F‚óW'F‚Á7Gñ∆RÊFó7∆ì“vÊˆÊRs∞¢&WGW&„∞¢–¢&"Á7Gñ∆RÊFó7∆ì“vf∆WÇs∞¢ñbáW'F‚óW'F‚Á7Gñ∆RÊFó7∆ì“rs∞¢&"ÊñÊÊW$ÖD‘√“rs∞¢ÚÚ&ˆ˜B6Vv÷VÁ@¢6ˆÁ7B&ˆ˜C÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢&ˆ˜BÊ6∆74Ê÷S“v'&VF7'V÷"◊6Vr'&VF7'V÷"÷∆ñÊ≤s∞¢&ˆ˜BÁFWáD6ˆÁFVÁC“w‚s∞¢&ˆ˜BÊˆÊ6∆ñ6≥“Çì”Ê∆ˆDFó"Çr‚rì∞¢ˆ&ñÊEv˜&∑76T÷˜fTG&˜F&vWBá&ˆ˜B¬r‚rì∞¢ˆ&ñÊEv˜&∑76T˜5W∆ˆDG&˜F&vWBá&ˆ˜B¬r‚rì∞¢&"ÊVÊD6Üñ∆Bá&ˆ˜Bì∞¢ÚÚFÇ6Vv÷VÁG0¢6ˆÁ7B'G3’2Ê7W'&VÁDFó"Á7∆óBÇrÚrì∞¢∆WB67V◊V∆FVC“rs∞¢f˜"Ü∆WBì”∂ì«'G2Ê∆VÊwFÉ∂í≤≤ó∞¢6ˆÁ7B6W÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢6WÊ6∆74Ê÷S“v'&VF7'V÷"◊6Ws∑6WÁFWáD6ˆÁFVÁC“rÚs∞¢&"ÊVÊD6Üñ∆Bá6Wì∞¢67V◊V∆FVB≥“Ü67V◊V∆FVCÚrÚs¢rrí∑'G5∂ï”∞¢6ˆÁ7B6Vs÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢6VrÁFWáD6ˆÁFVÁC◊'G5∂ï”∞¢ñbÜì«'G2Ê∆VÊwFÇ”ó∞¢6VrÊ6∆74Ê÷S“v'&VF7'V÷"◊6Vr'&VF7'V÷"÷∆ñÊ≤s∞¢6ˆÁ7BF&vWC÷67V◊V∆FVC∞¢6VrÊˆÊ6∆ñ6≥“Çì”Ê∆ˆDFó"áF&vWBì∞¢ˆ&ñÊEv˜&∑76T÷˜fTG&˜F&vWBá6Vr«F&vWBì∞¢ˆ&ñÊEv˜&∑76T˜5W∆ˆDG&˜F&vWBá6Vr«F&vWBì∞¢“V«6R∞¢6VrÊ6∆74Ê÷S“v'&VF7'V÷"◊6Vr'&VF7'V÷"÷7W'&VÁBs∞¢–¢&"ÊVÊD6Üñ∆Bá6Vrì∞¢–ß–†¶6ˆÁ7Btı$µ54UÙÑîDDTÂÙdîƒUÙ‰‘U3÷ÊWr6WBÖ∞¢r‰E5ı7F˜&Rr¬rÂÚ‰E5ı7F˜&Rr¬r‰∆TF˜V&∆Rr¬rÂ7˜F∆ñváB’cr¬rÂG&6ÜW2r¬rÊg6WfVÁG6Br¿¢uFáV÷'2ÊF"r¬tFW6∑F˜ÊñÊír¬vVáFáV÷'2ÊF"r¬rE$T5î4ƒR‰$î‚r¿¢rÊFó&V7F˜'ír¬rÊvóBr¬rÁ7f‚r¬rÊÜrr¬vÊˆFUˆ÷ˆGV∆W2r¬uı˜ñ66ÜUıÚr¿¢rÁóFW7Eˆ66ÜRr¬rÊ◊óïˆ66ÜRr¬rÁ'Vfeˆ66ÜRr¬rÁF˜Çr¬rÁfVÁbr¬wfVÁbp•“ì∞¶6ˆÁ7Btı$µ54UÙÑîDDTÂÙdîƒUı$TdïÑU3’≤rÂÚr¬rÂG&6Ç“u”∞¶gVÊ7Fñˆ‚˜v˜&∑76U6Ü˜V∆DÜñFTVÁG'íÜóFV“ó∞¢ñbÇóFV◊«≈2Á6Ü˜tÜñFFVÂv˜&∑76Tfñ∆W2ó&WGW&‚f«6S∞¢6ˆÁ7BÊ÷S’7G&ñÊrÜóFV“ÊÊ÷W«¬rrì∞¢ñbÇÊ÷Ró&WGW&‚f«6S∞¢ñbÖtı$µ54UÙÑîDDTÂÙdîƒUÙ‰‘U2ÊÜ2ÜÊ÷Ríó&WGW&‚G'VS∞¢&WGW&‚tı$µ54UÙÑîDDTÂÙdîƒUı$TdïÑU2Á6ˆ÷Rá&VfóÉ”ÊÊ÷RÁ7F'G5vóFÇá&VfóÇíì∞ß–¶gVÊ7Fñˆ‚˜fó6ñ&∆Uv˜&∑76TVÁG&ñW2ÜVÁG&ñW2ó∞¢6ˆÁ7B∆ó7C‘'&íÊó4'&íÜVÁG&ñW2ìˆVÁG&ñW3•µ”∞¢&WGW&‚2Á6Ü˜tÜñFFVÂv˜&∑76Tfñ∆W3ˆ∆ó7C¶∆ó7BÊfñ«FW"ÜóFV””‚˜v˜&∑76U6Ü˜V∆DÜñFTVÁG'íÜóFV“íì∞ß–¶6ˆÁ7Btı$µ54Uı4ı%EÙ¥Uï3’≤vÊ÷R÷62r¬vÊ÷R÷FW62r¬v7&VFVB÷FW62r¬v÷ˆFñfñVB÷FW62u”∞¶6ˆÁ7Btı$µ54Uı4ı%EÙDTdT≈C“vÊ÷R÷62s∞¶gVÊ7Fñˆ‚ˆÊ˜&÷∆ó¶Uv˜&∑76U6˜'D∂Wíáf«VRó∞¢&WGW&‚tı$µ54Uı4ı%EÙ¥Uï2ÊñÊ6«VFW2áf«VRì˜f«VS•tı$µ54Uı4ı%EÙDTdT≈C∞ß–¶gVÊ7Fñˆ‚˜v˜&∑76TVÁG'ï&Ê≤ÜóFV“ó∞¢6ˆÁ7B&Ê≥÷óFV“bfóFV“Áv˜&∑76U˜6˜'E˜&Ê≥∞¢&WGW&‚&Ê≥”””««&Ê≥”””««&Ê≥”””#˜&Ê≥£∞ß–¶gVÊ7Fñˆ‚˜v˜&∑76TVÁG'ïFñ÷W7F◊∂WíÜóFV“∆fñV∆Bó∞¢6ˆÁ7B&s÷óFV“bfóFV’∂fñV∆E”∞¢ñbáGóVˆb&s””“w7G&ñÊrró∞¢6ˆÁ7Bf«VS◊&rÁG&ñ“Çì∞¢ñbÇıÂ≤≤’”ı∆B≤BÚÁFW7Báf«VRíó&WGW&‚ÁV∆√∞¢6ˆÁ7BÊVvFófS◊f«VU≥”””“r“s∞¢6ˆÁ7BFñvóG3◊f«VRÁ&W∆6RÇıÂ≤≤’“Ú¬rríÁ&W∆6RÇı„≤ÉÛ’∆BíÚ¬rrì∞¢ñbÜFñvóG3””“sró&WGW&‚ss∞¢&WGW&‚ÊVvFófSÚr“r∂FñvóG3¶FñvóG3∞¢–¢ñbáGóVˆb&s””“vÁV÷&W"rí&WGW&‚ÁV÷&W"Êó4ñÁFVvW"á&rìı7G&ñÊrá&rì¶ÁV∆√∞¢ñbáGóVˆb&s””“v&ñvñÁBrí&WGW&‚7G&ñÊrá&rì∞¢&WGW&‚ÁV∆√∞ß–¶gVÊ7Fñˆ‚ˆ6ˆ◊&Uv˜&∑76UFñ÷W7F◊FW62Ü∆"∆fñV∆Bó∞¢6ˆÁ7Bc’˜v˜&∑76TVÁG'ïFñ÷W7F◊∂WíÜ∆fñV∆Bí∆'c’˜v˜&∑76TVÁG'ïFñ÷W7F◊∂WíÜ"∆fñV∆Bì∞¢ñbÜc”÷ÁV∆¬bf'c”÷ÁV∆¬ó&WGW&‚∞¢ñbÜc”÷ÁV∆¬ó&WGW&‚∞¢ñbÜ'c”÷ÁV∆¬ó&WGW&‚”∞¢6ˆÁ7B„÷e≥”””“r“r∆&„÷'e≥”””“r“s∞¢ñbÜ‚”÷&‚ó&WGW&‚„Û¢”∞¢6ˆÁ7B÷„ˆbÁ6∆ñ6RÉì¶b∆&#÷&„ˆ'bÁ6∆ñ6RÉì¶'c∞¢ñbÜÊ∆VÊwFÇ”÷&"Ê∆VÊwFÇó&WGW&‚„ˆÊ∆VÊwFÇ÷&"Ê∆VÊwFÉ¶&"Ê∆VÊwFÇ÷Ê∆VÊwFÉ∞¢&WGW&‚””÷&#Û¢Ü„ÚÜ∆&#Ú”£ì¢Ü∆&#Û¢”íì∞ß–¶gVÊ7Fñˆ‚˜v˜&∑76U6˜'D6ˆ◊&F˜"Ü∂Wíó∞¢ñbÜ∂Wì””“vÊ÷R÷FW62rí&WGW&‚Ü∆"ì”Â7G&ñÊrÜ"ÊÊ÷W«¬rríÁFÙ∆˜vW$66RÇíÊ∆ˆ6∆T6ˆ◊&RÖ7G&ñÊrÜÊÊ÷W«¬rríÁFÙ∆˜vW$66RÇíì∞¢6ˆÁ7BfñV∆C÷∂Wì””“v7&VFVB÷FW62sÚv&ó'FáFñ÷UˆÁ2s¢v◊Fñ÷UˆÁ2s∞¢&WGW&‚Ü∆"ì”Âˆ6ˆ◊&Uv˜&∑76UFñ÷W7F◊FW62Ü∆"∆fñV∆Bì∞ß–¶gVÊ7Fñˆ‚˜v˜&∑76T7&VFVE6˜'Dfñ∆&∆RÇó∑&WGW&‚Ö2Á6W76ñˆ‚be2Á6W76ñˆ‚Áv˜&∑76Rbe2Â˜v˜&∑76T&ó'FáFñ÷U6VV‚ì∑–¶gVÊ7Fñˆ‚ˆVffV7FófUv˜&∑76U6˜'D∂WíÇó∞¢6ˆÁ7B∂Wì’ˆÊ˜&÷∆ó¶Uv˜&∑76U6˜'D∂WíÖ2Áv˜&∑76U6˜'D∂Wíì∞¢&WGW&‚∂Wì””“v7&VFVB÷FW62rbb˜v˜&∑76T7&VFVE6˜'Dfñ∆&∆RÇìıtı$µ54Uı4ı%EÙDTdT≈C¶∂Wì∞ß–¶gVÊ7Fñˆ‚˜v˜&∑76TVÁG&ñW4f˜%&VÊFW"ÜVÁG&ñW2ó∞¢6ˆÁ7B∆ó7C’˜fó6ñ&∆Uv˜&∑76TVÁG&ñW2ÜVÁG&ñW2ì∞¢6ˆÁ7B∂Wì’ˆVffV7FófUv˜&∑76U6˜'D∂WíÇì∞¢ñbÜ∂Wì””’tı$µ54Uı4ı%EÙDTdT≈Bó&WGW&‚∆ó7C∞¢6ˆÁ7B6◊’˜v˜&∑76U6˜'D6ˆ◊&F˜"Ü∂Wíì∞¢&WGW&‚∆ó7BÁ6∆ñ6RÇíÁ6˜'BÇÜ∆"ì”Á∞¢6ˆÁ7B&Ê≥’˜v˜&∑76TVÁG'ï&Ê≤Üí’˜v˜&∑76TVÁG'ï&Ê≤Ü"ì∞¢&WGW&‚&Ê≤””˜&Ê≥¶6◊Ü∆"ì∞¢“ì∞ß–¶gVÊ7Fñˆ‚˜&W6WEv˜&∑76T&ó'FáFñ÷U7W˜'Bá66˜S“rró∞¢2Â˜v˜&∑76T&ó'FáFñ÷U6VV„÷f«6S∞¢2Â˜v˜&∑76T&ó'FáFñ÷Uv˜&∑76S’7G&ñÊrá66˜W«¬rrì∞¢˜7ñÊ5v˜&∑76U&Vg4ñÊFñ6F˜'2Çì∞¢˜7ñÊ5v˜&∑76U6˜'D÷VÁU7FFRÇì∞ß–¶gVÊ7Fñˆ‚˜7ñÊ5v˜&∑76T&ó'FáFñ÷U7W˜'E66˜Rá66˜S“rró∞¢6ˆÁ7BÊWáC’7G&ñÊrá66˜W«¬rrì∞¢ñbÖ2Â˜v˜&∑76T&ó'FáFñ÷Uv˜&∑76R”÷ÊWáBí˜&W6WEv˜&∑76T&ó'FáFñ÷U7W˜'BÜÊWáBì∞ß–¶gVÊ7Fñˆ‚ˆÊ˜FUv˜&∑76T&ó'FáFñ÷U7W˜'BÜVÁG&ñW2ó∞¢ñbÖ2Â˜v˜&∑76T&ó'FáFñ÷U6VV‚ó&WGW&„∞¢ñbÇÑ'&íÊó4'&íÜVÁG&ñW2ìˆVÁG&ñW3•µ“íÁ6ˆ÷RÜS”Â˜v˜&∑76TVÁG'ïFñ÷W7F◊∂WíÜR¬v&ó'FáFñ÷UˆÁ2rí”÷ÁV∆¬íó∞¢2Â˜v˜&∑76T&ó'FáFñ÷U6VV„◊G'VS∞¢2Â˜v˜&∑76T&ó'FáFñ÷Uv˜&∑76S’7G&ñÊrÇÖ2Á6W76ñˆ‚be2Á6W76ñˆ‚Áv˜&∑76Ró«≈2Â˜v˜&∑76T&ó'FáFñ÷Uv˜&∑76W«¬rrì∞¢˜7ñÊ5v˜&∑76U&Vg4ñÊFñ6F˜'2Çì∞¢˜7ñÊ5v˜&∑76U6˜'D÷VÁU7FFRÇì∞¢–ß–¶gVÊ7Fñˆ‚˜7ñÊ5v˜&∑76U&Vg4ñÊFñ6F˜'2ÜñÊC“BÇwv˜&∑76TÜñFFV‰ñÊFñ6F˜"rí∆F˜C“BÇwv˜&∑76U&Vg4F˜Bríó∞¢ñbÜñÊBó∞¢ñbÖ2Á6Ü˜tÜñFFVÂv˜&∑76Tfñ∆W2ó∂ñÊBÊÜñFFV„÷f«6S∂ñÊBÁ&V÷˜fTGG&ñ'WFRÇvÜñFFV‚rì∑–¢V«6W∂ñÊBÊÜñFFV„◊G'VS∂ñÊBÁ6WDGG&ñ'WFRÇvÜñFFV‚r¬rrì∑–¢–¢ñbÜF˜Bó∞¢6ˆÁ7B7FófS’2Á6Ü˜tÜñFFVÂv˜&∑76Tfñ∆W7«≈ˆVffV7FófUv˜&∑76U6˜'D∂WíÇí”’tı$µ54Uı4ı%EÙDTdT≈C∞¢ñbÜ7FófRó∂F˜BÊÜñFFV„÷f«6S∂F˜BÁ&V÷˜fTGG&ñ'WFRÇvÜñFFV‚rì∑–¢V«6W∂F˜BÊÜñFFV„◊G'VS∂F˜BÁ6WDGG&ñ'WFRÇvÜñFFV‚r¬rrì∑–¢–ß–¶gVÊ7Fñˆ‚˜7ñÊ5v˜&∑76U6˜'D÷VÁU7FFRÜ÷VÁS’˜v˜&∑76U&Vg4÷VÁRó∞¢ñbÇ÷VÁW«¬÷VÁRÁVW'ï6V∆V7F˜$∆¬ó&WGW&„∞¢6ˆÁ7B7FófS’ˆVffV7FófUv˜&∑76U6˜'D∂WíÇì∞¢6ˆÁ7B7&VFVDˆ≥’˜v˜&∑76T7&VFVE6˜'Dfñ∆&∆RÇì∞¢÷VÁRÁVW'ï6V∆V7F˜$∆¬ÇrÁv˜&∑76R◊&Vg2÷óFV““◊&FñÚríÊf˜$V6Çá&˜s”Á∞¢6ˆÁ7BñÁWC◊&˜rbg&˜rÁVW'ï6V∆V7F˜#˜&˜rÁVW'ï6V∆V7F˜"ÇvñÁWE∂Ê÷S“'v˜&∑76U6˜'D∂Wí%“rì¶ÁV∆√∞¢ñbÇñÁWBó&WGW&„∞¢6ˆÁ7B6ÜV6∂VC÷ñÁWBÁf«VS””÷7FófS∞¢6ˆÁ7BFó6&∆VC÷ñÁWBÁf«VS””“v7&VFVB÷FW62rbb7&VFVDˆ≥∞¢ñÁWBÊ6ÜV6∂VC÷6ÜV6∂VC∞¢ñÁWBÊFó6&∆VC÷Fó6&∆VC∞¢&˜rÁ6WDGG&ñ'WFRÇv&ñ÷6ÜV6∂VBr∆6ÜV6∂VCÚwG'VRs¢vf«6Rrì∞¢&˜rÁ6WDGG&ñ'WFRÇv&ñ÷Fó6&∆VBr∆Fó6&∆VCÚwG'VRs¢vf«6Rrì∞¢ñbá&˜rÊ6∆74∆ó7Bbg&˜rÊ6∆74∆ó7BÁFˆvv∆Rí&˜rÊ6∆74∆ó7BÁFˆvv∆RÇvó2÷Fó6&∆VBr∆Fó6&∆VBì∞¢ñbÜñÁWBÁf«VS””“v7&VFVB÷FW62ró∞¢6ˆÁ7B6˜ì◊&˜rÁVW'ï6V∆V7F˜"ÇrÁv˜&∑76R◊&Vg2÷6˜írì∞¢∆WB÷WF◊&˜rÁVW'ï6V∆V7F˜"ÇrÁv˜&∑76R◊&Vg2÷÷WFrì∞¢ñbÜFó6&∆VBó∞¢ñbÇ÷WFbf6˜íbgGóVˆbFˆ7V÷VÁB”“wVÊFVfñÊVBró∞¢÷WF÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢÷WFÊ6∆74Ê÷S“wv˜&∑76R◊&Vg2÷÷WFs∞¢6˜íÊVÊD6Üñ∆BÜ÷WFì∞¢–¢ñbÜ÷WFñ÷WFÁFWáD6ˆÁFVÁC◊GóVˆbC””“vgVÊ7Fñˆ‚s˜BÇwv˜&∑76U˜6˜'Eˆ7&VFVE˜VÊfñ∆&∆Rrì¢t7&VFñˆ‚Fñ÷Ró2Ê˜B&W˜'FVB'íFÜó26W'fW"˜"∆Ff˜&“‚s∞¢÷V«6RñbÜ÷WFñ÷WFÁ&V÷˜fRÇì∞¢–¢“ì∞¢ñbÜ÷VÁS””’˜v˜&∑76U&Vg4÷VÁRbgGóVˆb˜v˜&∑76U&Vg4Ê6Ü˜"”“wVÊFVfñÊVBrbe˜v˜&∑76U&Vg4Ê6Ü˜"bgGóVˆb˜˜6óFñˆÂv˜&∑76U&Vg4÷VÁS””“vgVÊ7Fñˆ‚rí˜˜6óFñˆÂv˜&∑76U&Vg4÷VÁRÖ˜v˜&∑76U&Vg4Ê6Ü˜"ì∞ß–¶gVÊ7Fñˆ‚˜7ñÊ5v˜&∑76TÜñFFVÂFˆvv∆RÇó∞¢6ˆÁ7BV√“BÇwv˜&∑76U6Ü˜tÜñFFV‰fñ∆W2rì∞¢ñbÜV¬ñV¬Ê6ÜV6∂VC“2Á6Ü˜tÜñFFVÂv˜&∑76Tfñ∆W3∞¢˜7ñÊ5v˜&∑76U&Vg4ñÊFñ6F˜'2ÇBÇwv˜&∑76TÜñFFV‰ñÊFñ6F˜"rí¬BÇwv˜&∑76U&Vg4F˜Bríì∞ß–¶gVÊ7Fñˆ‚Fˆvv∆Uv˜&∑76TÜñFFV‰fñ∆W2áf«VRó∞¢2Á6Ü˜tÜñFFVÂv˜&∑76Tfñ∆W3“f«VS∞¢G'ó∂∆ˆ6≈7F˜&vRÁ6WDóFV“ÇvÜW&÷W2◊v˜&∑76R◊6Ü˜r÷ÜñFFV‚÷fñ∆W2r≈2Á6Ü˜tÜñFFVÂv˜&∑76Tfñ∆W3Úss¢srì∑÷6F6ÇÖÚó∑–¢˜7ñÊ5v˜&∑76TÜñFFVÂFˆvv∆RÇì∞¢&VÊFW$fñ∆UG&VRÇì∞ß–ßG'óµ2Á6Ü˜tÜñFFVÂv˜&∑76Tfñ∆W3÷∆ˆ6≈7F˜&vRÊvWDóFV“ÇvÜW&÷W2◊v˜&∑76R◊6Ü˜r÷ÜñFFV‚÷fñ∆W2rì””“ss∑÷6F6ÇÖÚó∑–ßG'óµ2Áv˜&∑76U6˜'D∂Wì’ˆÊ˜&÷∆ó¶Uv˜&∑76U6˜'D∂WíÜ∆ˆ6≈7F˜&vRÊvWDóFV“ÇvÜW&÷W2◊v˜&∑76R◊6˜'B÷∂Wíríì∑÷6F6ÇÖÚóµ2Áv˜&∑76U6˜'D∂Wì’tı$µ54Uı4ı%EÙDTdT≈C∑–¶gVÊ7Fñˆ‚6WEv˜&∑76U6˜'D∂Wíáf«VRó∞¢2Áv˜&∑76U6˜'D∂Wì’ˆÊ˜&÷∆ó¶Uv˜&∑76U6˜'D∂Wíáf«VRì∞¢G'ó∂∆ˆ6≈7F˜&vRÁ6WDóFV“ÇvÜW&÷W2◊v˜&∑76R◊6˜'B÷∂Wír≈2Áv˜&∑76U6˜'D∂Wíì∑÷6F6ÇÖÚó≤–¢˜7ñÊ5v˜&∑76U&Vg4ñÊFñ6F˜'2Çì∞¢˜7ñÊ5v˜&∑76U6˜'D÷VÁU7FFRÇì∞¢&VÊFW$fñ∆UG&VRÇì∞ß–†¢ÚÚ)H)Hv˜&∑76R&VfW&VÊ6W2∂V&"÷VÁRÇ3sì2UÇ&VfñÊV÷VÁBí)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H ¢ÚÚFÜR%6Ü˜rÜñFFV‚fñ∆W2"Fˆvv∆RW6VBFÚ∆ófR2W&÷ÊVÁBñÊ∆ñÊR&˜p¢ÚÚ&V∆˜rFÜR'&VF7'V÷"&"‚FÜBFR„3'ÇˆbfW'Fñ6¬76Rˆ‚WfW'ê¢ÚÚÊV¬fñWrá&ˆ˜B¬7V&Fó"¬fñ∆R&WfñWrí¬WfV‚FÜ˜VvÇFÜRFˆvv∆Ró2¢ÚÚ6WB÷ˆÊ6R&VfW&VÊ6R(	B÷˜7BW6W'2f∆óóBˆÊ6R˜"ÊWfW"‚÷˜fñÊrFÜP¢ÚÚ6ˆÁG&ˆ¬ñÁFÚ∂V&"G&˜F˜v‚&V6∆ñ◊2FÜR76S≤FÜR6÷∆¬"ÜÜñFFV‡¢ÚÚfñ∆W2fó6ñ&∆Rí"ñÊFñ6F˜"ˆ‚FÜRÜVFñÊr&Vf∆V7G2FÜRÊˆ‚÷FVfV«B7FFP¢ÚÚ6ÚFÜRff˜&FÊ6Ró6‚wB∆˜7B‡¶∆WB˜v˜&∑76U&Vg4÷VÁR“ÁV∆√∞¶∆WB˜v˜&∑76U&Vg4Ê6Ü˜"“ÁV∆√∞¶gVÊ7Fñˆ‚ˆ6∆˜6Uv˜&∑76U&Vg4÷VÁRÇó∞¢ñbÖ˜v˜&∑76U&Vg4÷VÁRó≤˜v˜&∑76U&Vg4÷VÁRÁ&V÷˜fRÇì≤˜v˜&∑76U&Vg4÷VÁS÷ÁV∆√≤–¢ñbÖ˜v˜&∑76U&Vg4Ê6Ü˜"ó∞¢˜v˜&∑76U&Vg4Ê6Ü˜"Ê6∆74∆ó7BÁ&V÷˜fRÇv7FófRrì∞¢˜v˜&∑76U&Vg4Ê6Ü˜"Á6WDGG&ñ'WFRÇv&ñ÷WáÊFVBr¬vf«6Rrì∞¢˜v˜&∑76U&Vg4Ê6Ü˜#÷ÁV∆√∞¢–ß–¶gVÊ7Fñˆ‚˜˜6óFñˆÂv˜&∑76U&Vg4÷VÁRÜÊ6Ü˜$V¬ó∞¢ñbÇ˜v˜&∑76U&Vg4÷VÁW«¬Ê6Ü˜$V¬í&WGW&„∞¢6ˆÁ7B&V7C÷Ê6Ü˜$V¬ÊvWD&˜VÊFñÊt6∆ñVÁE&V7BÇì∞¢6ˆÁ7B÷VÁUs‘÷FÇÊ÷ñ‚É#c¬÷FÇÊ÷ÇÉ##¬˜v˜&∑76U&Vg4÷VÁRÁ67&ˆ∆≈vñGFá«√##íì∞¢∆WB∆VgC◊&V7BÁ&ñváB÷÷VÁUs∞¢ñbÜ∆VgC√Çí∆VgC”É∞¢ñbÜ∆VgB∂÷VÁUsÁvñÊF˜rÊñÊÊW%vñGFÇ”Çí∆VgC◊vñÊF˜rÊñÊÊW%vñGFÇ÷÷VÁUr”É∞¢∆WBF˜◊&V7BÊ&˜GFˆ“≥c∞¢6ˆÁ7B÷VÁTÉ’˜v˜&∑76U&Vg4÷VÁRÊˆfg6WDÜVñváG«√∞¢ñbáF˜∂÷VÁTÉÁvñÊF˜rÊñÊÊW$ÜVñváB”Çbb&V7BÁF˜Ê÷VÁTÇ≥"íF˜◊&V7BÁF˜÷÷VÁTÇ”c∞¢ñbáF˜√ÇíF˜”É∞¢˜v˜&∑76U&Vg4÷VÁRÁ7Gñ∆RÊ∆VgC÷∆VgB≤wÇs∞¢˜v˜&∑76U&Vg4÷VÁRÁ7Gñ∆RÁF˜◊F˜≤wÇs∞ß–¶gVÊ7Fñˆ‚ˆ'Vñ∆Ev˜&∑76U&Vg4÷VÁRÇó∞¢6ˆÁ7B÷VÁS÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢÷VÁRÊ6∆74Ê÷S“wv˜&∑76R◊&Vg2÷÷VÁR˜V‚s∞¢÷VÁRÁ6WDGG&ñ'WFRÇw&ˆ∆Rr¬v÷VÁRrì∞¢6ˆÁ7Bw&˜W÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢w&˜WÊ6∆74Ê÷S“wv˜&∑76R◊&Vg2÷w&˜Ws∞¢w&˜WÁ6WDGG&ñ'WFRÇw&ˆ∆Rr¬vw&˜Wrì∞¢6ˆÁ7Bw&˜W∆&V√“áGóVˆbC””“vgVÊ7Fñˆ‚s˜BÇwv˜&∑76U˜6˜'Eˆ'írì¢u6˜'B'írì∞¢w&˜WÁ6WDGG&ñ'WFRÇv&ñ÷∆&V¬r∆w&˜W∆&V¬ì∞¢6ˆÁ7BÜVC÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢ÜVBÊ6∆74Ê÷S“wv˜&∑76R◊&Vg2÷w&˜W∆&V¬s∞¢ÜVBÁFWáD6ˆÁFVÁC÷w&˜W∆&V√∞¢w&˜WÊVÊD6Üñ∆BÜÜVBì∞¢6ˆÁ7B7&VFVDˆ≥’˜v˜&∑76T7&VFVE6˜'Dfñ∆&∆RÇì∞¢6ˆÁ7B7FófS’ˆVffV7FófUv˜&∑76U6˜'D∂WíÇì∞¢µ≤vÊ÷R÷62r¬wv˜&∑76U˜6˜'EˆÊ÷Uˆ62u“≈≤vÊ÷R÷FW62r¬wv˜&∑76U˜6˜'EˆÊ÷UˆFW62u“≈≤v7&VFVB÷FW62r¬wv˜&∑76U˜6˜'Eˆ7&VFVEˆFW62u“≈≤v÷ˆFñfñVB÷FW62r¬wv˜&∑76U˜6˜'Eˆ÷ˆFñfñVEˆFW62u’“Êf˜$V6ÇÇÖ∂∂Wí∆ìÜ‰∂Wï“ì”Á∞¢6ˆÁ7BFó6&∆VC÷∂Wì””“v7&VFVB÷FW62rbb7&VFVDˆ≥∞¢6ˆÁ7B&˜s÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇv∆&V¬rì∞¢&˜rÊ6∆74Ê÷S“wv˜&∑76R◊&Vg2÷óFV“v˜&∑76R◊&Vg2÷óFV““◊&FñÚr≤ÜFó6&∆VCÚró2÷Fó6&∆VBs¢rrì∞¢&˜rÁ6WDGG&ñ'WFRÇw&ˆ∆Rr¬v÷VÁVóFV◊&FñÚrì∞¢&˜rÁ6WDGG&ñ'WFRÇv&ñ÷6ÜV6∂VBr∆7FófS””÷∂WìÚwG'VRs¢vf«6Rrì∞¢&˜rÁ6WDGG&ñ'WFRÇv&ñ÷Fó6&∆VBr∆Fó6&∆VCÚwG'VRs¢vf«6Rrì∞¢&˜rÊñÊÊW$ÖD‘√“s∆ñÁWBGóS“'&FñÚ"Ê÷S“'v˜&∑76U6˜'D∂Wí"f«VS“"r∂W62Ü∂Wíí≤r"ñC“'v˜&∑76U6˜'EÚr∂W62Ü∂Wíí≤r"r≤ÜFó6&∆VCÚrFó6&∆VBs¢rrí≤rˆÊ6ÜÊvS“'6WEv˜&∑76U6˜'D∂WíáFÜó2Áf«VRí#‚r∞¢s«7‚6∆73“'v˜&∑76R◊&Vg2÷6˜í#„«7‚6∆73“'v˜&∑76R◊&Vg2÷Ê÷R#‚r∂W62áGóVˆbC””“vgVÊ7Fñˆ‚s˜BÜìÜ‰∂Wíì¶∂Wíí≤s¬˜7„‚r∞¢ÜFó6&∆VCÚs«7‚6∆73“'v˜&∑76R◊&Vg2÷÷WF#‚r∂W62áGóVˆbC””“vgVÊ7Fñˆ‚s˜BÇwv˜&∑76U˜6˜'Eˆ7&VFVE˜VÊfñ∆&∆Rrì¢t7&VFñˆ‚Fñ÷Ró2Ê˜B&W˜'FVB'íFÜó26W'fW"˜"∆Ff˜&“‚rí≤s¬˜7„‚s¢rrí≤s¬˜7„‚s∞¢6ˆÁ7BñÁWC◊&˜rÁVW'ï6V∆V7F˜"ÇvñÁWBrì∞¢ñbÜñÁWBññÁWBÊ6ÜV6∂VC÷7FófS””÷∂Wì∞¢w&˜WÊVÊD6Üñ∆Bá&˜rì∞¢“ì∞¢÷VÁRÊVÊD6Üñ∆BÜw&˜Wì∞¢6ˆÁ7B6W÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢6WÊ6∆74Ê÷S“wv˜&∑76R◊&Vg2◊6Ws∞¢÷VÁRÊVÊD6Üñ∆Bá6Wì∞¢ÚÚFÜR6ÜV6∂&˜Ç∂VW2ñC“'v˜&∑76U6Ü˜tÜñFFV‰fñ∆W2"6ÚWÜó7FñÊr6∆¿¢ÚÚ6óFW2ÜÊBFÜRWÜó7FñÊrFW7Eˆó77VSsì5ˆfñ∆U˜G&VUˆ7'VgEˆfñ«FW"FW7Bê¢ÚÚ6‚fñÊBóBFÜR6÷Rví2&Vf˜&R‚ˆÊ«íFÜR&VÁB6ˆÁFñÊW"÷˜fW2‡¢6ˆÁ7B∆&V≈GáB“áGóVˆbC””“vgVÊ7Fñˆ‚rÚBÇwv˜&∑76U˜6Ü˜uˆÜñFFVÂˆfñ∆W2rí¢u6Ü˜rÜñFFV‚fñ∆W2rì∞¢6ˆÁ7BFW65GáB“áGóVˆbC””“vgVÊ7Fñˆ‚rÚBÇwv˜&∑76U˜6Ü˜uˆÜñFFVÂˆfñ∆W5ˆFW62rí¢tñÊ6«VFR‰E5ı7F˜&R¬ÊvóB¬ÊˆFUˆ÷ˆGV∆W2¬ÊB˜FÜW"ÜñFFV‚Ú7ó7FV“fñ∆W2ñ‚FÜRfñ∆RG&VR‚rì∞¢6ˆÁ7B&˜s÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇv∆&V¬rì∞¢&˜rÊ6∆74Ê÷S“wv˜&∑76R◊&Vg2÷óFV“s∞¢&˜rÁ6WDGG&ñ'WFRÇw&ˆ∆Rr¬v÷VÁVóFV÷6ÜV6∂&˜Çrì∞¢&˜rÊñÊÊW$ÖD‘√–¢s∆ñÁWBGóS“&6ÜV6∂&˜Ç"ñC“'v˜&∑76U6Ü˜tÜñFFV‰fñ∆W2"r∞¢vˆÊ6ÜÊvS“'Fˆvv∆Uv˜&∑76TÜñFFV‰fñ∆W2áFÜó2Ê6ÜV6∂VBí#‚r∞¢s«7‚6∆73“'v˜&∑76R◊&Vg2÷6˜í#‚r∞¢s«7‚6∆73“'v˜&∑76R◊&Vg2÷Ê÷R#‚r∂W62Ü∆&V≈GáBí≤s¬˜7„‚r∞¢s«7‚6∆73“'v˜&∑76R◊&Vg2÷÷WF#‚r∂W62ÜFW65GáBí≤s¬˜7„‚r∞¢s¬˜7„‚s∞¢6ˆÁ7B6#◊&˜rÁVW'ï6V∆V7F˜"ÇvñÁWBrì∞¢ñbÜ6"í6"Ê6ÜV6∂VC“2Á6Ü˜tÜñFFVÂv˜&∑76Tfñ∆W3∞¢÷VÁRÊVÊD6Üñ∆Bá&˜rì∞¢&WGW&‚÷VÁS∞ß–¶gVÊ7Fñˆ‚Fˆvv∆Uv˜&∑76U&Vg4÷VÁRÜRó∞¢ñbÜRbfRÁ&WfVÁDFVfV«BíRÁ&WfVÁDFVfV«BÇì∞¢ñbÜRbfRÁ7F˜&˜vFñˆ‚íRÁ7F˜&˜vFñˆ‚Çì∞¢ÚÚÊ6Ü˜"&VfW&VÊ6S¢FÜR∂V&"'WGFˆ‚‚FÜRñÊFñ6F˜"6Üó6‚«6Ú˜V‡¢ÚÚFÜR6÷R÷VÁRÜ6∆ñ6≤ˆ‚"ÜÜñFFV‚fó6ñ&∆Rí"í¬'WBÊ6Ü˜"˜6óFñˆÊñÊp¢ÚÚ«vó2&VfW&VÊ6W2FÜR∂V&"6ÚFÜR÷VÁR∆ÊG2ñ‚FÜR6÷R∆6R‡¢6ˆÁ7BÊ6Ü˜#“BÇv'FÂv˜&∑76U&Vg2ró«¬ÜRbfRÊ7W'&VÁEF&vWBó«∆ÁV∆√∞¢ñbÖ˜v˜&∑76U&Vg4÷VÁRbe˜v˜&∑76U&Vg4Ê6Ü˜#””÷Ê6Ü˜"ó≤ˆ6∆˜6Uv˜&∑76U&Vg4÷VÁRÇì≤&WGW&„≤–¢ˆ6∆˜6Uv˜&∑76U&Vg4÷VÁRÇì∞¢6ˆÁ7B÷VÁS’ˆ'Vñ∆Ev˜&∑76U&Vg4÷VÁRÇì∞¢Fˆ7V÷VÁBÊ&ˆGíÊVÊD6Üñ∆BÜ÷VÁRì∞¢˜v˜&∑76U&Vg4÷VÁS÷÷VÁS∞¢˜v˜&∑76U&Vg4Ê6Ü˜#÷Ê6Ü˜#∞¢ñbÜÊ6Ü˜"ó≤Ê6Ü˜"Ê6∆74∆ó7BÊFBÇv7FófRrì≤Ê6Ü˜"Á6WDGG&ñ'WFRÇv&ñ÷WáÊFVBr¬wG'VRrì≤–¢˜˜6óFñˆÂv˜&∑76U&Vg4÷VÁRÜÊ6Ü˜"ì∞ß–¶Fˆ7V÷VÁBÊFDWfVÁD∆ó7FVÊW"Çv6∆ñ6≤r∆S”Á∞¢ñbÇ˜v˜&∑76U&Vg4÷VÁRí&WGW&„∞¢ñbÖ˜v˜&∑76U&Vg4÷VÁRÊ6ˆÁFñÁ2ÜRÁF&vWBíí&WGW&„∞¢ñbÖ˜v˜&∑76U&Vg4Ê6Ü˜"be˜v˜&∑76U&Vg4Ê6Ü˜"Ê6ˆÁFñÁ2ÜRÁF&vWBíí&WGW&„∞¢ÚÚñÊFñ6F˜"6Üóó2«6Ú‚˜VÊW"(	B6∆ñ6∂ñÊróB6Ü˜V∆BFˆvv∆R¬Ê˜B6∆˜6R‡¢6ˆÁ7BñÊC“BÇwv˜&∑76TÜñFFV‰ñÊFñ6F˜"rì∞¢ñbÜñÊBbfñÊBÊ6ˆÁFñÁ2ÜRÁF&vWBíí&WGW&„∞¢ˆ6∆˜6Uv˜&∑76U&Vg4÷VÁRÇì∞ß“ì∞¶Fˆ7V÷VÁBÊFDWfVÁD∆ó7FVÊW"Çv∂WñF˜v‚r∆S”Á∞¢ñbÜRÊ∂Wì””“tW66Rrbe˜v˜&∑76U&Vg4÷VÁRíˆ6∆˜6Uv˜&∑76U&Vg4÷VÁRÇì∞ß“ì∞ßvñÊF˜rÊFDWfVÁD∆ó7FVÊW"Çw&W6ó¶Rr¬Çì”Á∞¢ñbÖ˜v˜&∑76U&Vg4÷VÁRbe˜v˜&∑76U&Vg4Ê6Ü˜"í˜˜6óFñˆÂv˜&∑76U&Vg4÷VÁRÖ˜v˜&∑76U&Vg4Ê6Ü˜"ì∞ß“ì∞†¶ñbÜFˆ7V÷VÁBÁ&VGï7FFS””“v∆ˆFñÊrrñFˆ7V÷VÁBÊFDWfVÁD∆ó7FVÊW"ÇtDÙ‘6ˆÁFVÁD∆ˆFVBr≈˜7ñÊ5v˜&∑76TÜñFFVÂFˆvv∆Rì∞¶V«6R˜7ñÊ5v˜&∑76TÜñFFVÂFˆvv∆RÇì∞†¶gVÊ7Fñˆ‚&ñÊEv˜&∑76TÜVFñÊt7FñˆÁ2Çó∞¢6ˆÁ7BÜVFñÊs“BÇwv˜&∑76UÊVƒÜVFñÊrrì∞¢ñbÇÜVFñÊw«∆ÜVFñÊrÊFF6WBÊ&˜VÊC””“sró&WGW&„∞¢ÜVFñÊrÊFF6WBÊ&˜VÊC“ss∞¢6ˆÁ7Bvı&ˆ˜C“Çì”Á∞¢ñbÖ2Á6W76ñˆ‚be2Á6W76ñˆ‚Áv˜&∑76Rí∆ˆDFó"Çr‚rì∞¢”∞¢ÜVFñÊrÊˆÊ6∆ñ6≥÷vı&ˆ˜C∞¢ÜVFñÊrÊˆÊ∂WñF˜v„“ÜRì”Á∞¢ñbÇÖ2Á6W76ñˆ‚be2Á6W76ñˆ‚Áv˜&∑76Ríí&WGW&„∞¢ñbÜRÊ∂Wì””“tVÁFW"w«∆RÊ∂Wì””“rró∞¢RÁ&WfVÁDFVfV«BÇì∞¢vı&ˆ˜BÇì∞¢–¢”∞¢ÜVFñÊrÊˆÊ6ˆÁFWáF÷VÁS“ÜRì”Á∞¢ñbÇÖ2Á6W76ñˆ‚be2Á6W76ñˆ‚Áv˜&∑76Ríí&WGW&„∞¢RÁ&WfVÁDFVfV«BÇì∞¢RÁ7F˜&˜vFñˆ‚Çì∞¢˜6Ü˜uv˜&∑76U&ˆ˜D6ˆÁFWáD÷VÁRÜRì∞¢”∞¢˜7ñÊ5v˜&∑76TÜVFñÊu7FFRÇì∞ß–†¶gVÊ7Fñˆ‚˜7ñÊ5v˜&∑76TÜVFñÊu7FFRÇó∞¢6ˆÁ7BÜVFñÊs“BÇwv˜&∑76UÊVƒÜVFñÊrrì∞¢ñbÇÜVFñÊrí&WGW&„∞¢6ˆÁ7BVÊ&∆VC“Ö2Á6W76ñˆ‚be2Á6W76ñˆ‚Áv˜&∑76Rì∞¢ÜVFñÊrÊ6∆74∆ó7BÁFˆvv∆RÇwv˜&∑76R◊ÊV¬÷ÜVFñÊr“÷VÊ&∆VBr∆VÊ&∆VBì∞¢ñbÜVÊ&∆VBó∞¢ÜVFñÊrÁ6WDGG&ñ'WFRÇw&ˆ∆Rr¬v'WGFˆ‚rì∞¢ÜVFñÊrÁ6WDGG&ñ'WFRÇwF&ñÊFWÇr¬srì∞¢ÜVFñÊrÁ6WDGG&ñ'WFRÇv&ñ÷Fó6&∆VBr¬vf«6Rrì∞¢ÜVFñÊrÁFóF∆S“uv˜&∑76R&ˆ˜Bs∞¢“V«6R∞¢ÜVFñÊrÁ&V÷˜fTGG&ñ'WFRÇw&ˆ∆Rrì∞¢ÜVFñÊrÁ&V÷˜fTGG&ñ'WFRÇwF&ñÊFWÇrì∞¢ÜVFñÊrÁ6WDGG&ñ'WFRÇv&ñ÷Fó6&∆VBr¬wG'VRrì∞¢ÜVFñÊrÁFóF∆S◊BÇvÊı˜v˜&∑76Rrì∞¢–ß–¶ñbÜFˆ7V÷VÁBÁ&VGï7FFS””“v∆ˆFñÊrríFˆ7V÷VÁBÊFDWfVÁD∆ó7FVÊW"ÇtDÙ‘6ˆÁFVÁD∆ˆFVBr∆&ñÊEv˜&∑76TÜVFñÊt7FñˆÁ2ì∞¶V«6R&ñÊEv˜&∑76TÜVFñÊt7FñˆÁ2Çì∞†¶gVÊ7Fñˆ‚˜v˜&∑76T6ˆÁFWáD÷VÁTóFV“Ü∆&V¬¬ˆ‰6∆ñ6≤¬˜G3◊∑“ó∞¢6ˆÁ7BóFV”÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢óFV“ÁFWáD6ˆÁFVÁC÷∆&V√∞¢óFV“Á7Gñ∆RÊ775FWáC“wFFñÊs£wÇGÉ∂7W'6˜#ßˆñÁFW#∂fˆÁB◊6ó¶S£7É∂6ˆ∆˜#¢r≤Ü˜G2ÊFÊvW#Úwf"Ç“÷W'&˜"¬6SìCScís¢wf"Ç“◊FWáBírí≤s≤s∞¢óFV“ÊˆÊ÷˜W6VVÁFW#“Çì”ÊóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“wf"Ç“÷Ü˜fW"÷&rís∞¢óFV“ÊˆÊ÷˜W6V∆VfS“Çì”ÊóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“rs∞¢óFV“ÊˆÊ6∆ñ6≥÷ˆ‰6∆ñ6≥∞¢&WGW&‚óFV”∞ß–†¶gVÊ7Fñˆ‚ˆ6˜ïFWáEvóFÑf∆∆&6≤áFWáB¬7V66W74◊6r¬fñ«W&U&VfóÇó∞¢6ˆÁ7BFˆÊS“Çì”Á6Ü˜uFˆ7Bá7V66W74◊6rì∞¢6ˆÁ7Bfñ√“ÜW'"ì”Á6Ü˜uFˆ7BÜfñ«W&U&VfóÇ≤ÜW'"bfW'"Ê÷W76vSˆW'"Ê÷W76vS•7G&ñÊrÜW''«¬rrííì∞¢ñbÜÊfñvF˜"Ê6∆ó&ˆ&BbfÊfñvF˜"Ê6∆ó&ˆ&BÁw&óFUFWáBó∞¢&WGW&‚ÊfñvF˜"Ê6∆ó&ˆ&BÁw&óFUFWáBáFWáBíÁFÜV‚ÜFˆÊRíÊ6F6ÇÜW'#”Á∞¢6ˆÁ7BF÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇwFWáF&Vrì∞¢FÁf«VS◊FWáC∞¢FÁ7Gñ∆RÊ775FWáC“w˜6óFñˆ„¶fóÜVC∂∆VgC¢”ìììóÉ∑F˜¢”ìììóÉ≤s∞¢Fˆ7V÷VÁBÊ&ˆGíÊVÊD6Üñ∆BáFì∞¢FÁ6V∆V7BÇì∞¢∆WB6˜ñVC÷f«6S∞¢G'ó∂6˜ñVC÷Fˆ7V÷VÁBÊWÜV46ˆ÷÷ÊBÇv6˜írì∑÷6F6ÇÖÚó∑–¢FÁ&V÷˜fRÇì∞¢ñbÜ6˜ñVBíFˆÊRÇì≤V«6Rfñ¬ÜW'"ì∞¢“ì∞¢–¢6ˆÁ7BF÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇwFWáF&Vrì∞¢FÁf«VS◊FWáC∞¢FÁ7Gñ∆RÊ775FWáC“w˜6óFñˆ„¶fóÜVC∂∆VgC¢”ìììóÉ∑F˜¢”ìììóÉ≤s∞¢Fˆ7V÷VÁBÊ&ˆGíÊVÊD6Üñ∆BáFì∞¢FÁ6V∆V7BÇì∞¢∆WB6˜ñVC÷f«6S∞¢G'ó∂6˜ñVC÷Fˆ7V÷VÁBÊWÜV46ˆ÷÷ÊBÇv6˜írì∑÷6F6ÇÜW'"ó∑FÁ&V÷˜fRÇì∂fñ¬ÜW'"ì∑&WGW&‚&ˆ÷ó6RÁ&W6ˆ«fRÇì∑–¢FÁ&V÷˜fRÇì∞¢ñbÜ6˜ñVBíFˆÊRÇì≤V«6Rfñ¬Çv6∆ó&ˆ&BVÊfñ∆&∆Rrì∞¢&WGW&‚&ˆ÷ó6RÁ&W6ˆ«fRÇì∞ß–†¶gVÊ7Fñˆ‚˜v˜&∑76T7&VFUF&vWD∆&V¬áF&vWDFó"ó∞¢&WGW&‚F&vWDFó"bbF&vWDFó"”“r‚rÚF&vWDFó"¢BÇwv˜&∑76U˜&ˆ˜Brì∞ß–†¶gVÊ7Fñˆ‚˜v˜&∑76T¶ˆñÂF&vWEFÇáF&vWDFó"¬Ê÷Ró∞¢6ˆÁ7B6∆V‰Ê÷S’7G&ñÊrÜÊ÷W«¬rríÁG&ñ“Çì∞¢ñbÇ6∆V‰Ê÷Rí&WGW&‚rs∞¢&WGW&‚ÇF&vWDFó'««F&vWDFó#””“r‚ríÚ6∆V‰Ê÷R¢G∑F&vWDFó'“ÚG∂6∆V‰Ê÷W÷∞ß–†¶gVÊ7Fñˆ‚˜6Ü˜uv˜&∑76U&ˆ˜D6ˆÁFWáD÷VÁRÜRó∞¢Fˆ7V÷VÁBÁVW'ï6V∆V7F˜$∆¬ÇrÊfñ∆R÷7GÇ÷÷VÁRríÊf˜$V6ÇÜV√”ÊV¬Á&V÷˜fRÇíì∞¢6ˆÁ7B÷VÁS÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢÷VÁRÊ6∆74Ê÷S“vfñ∆R÷7GÇ÷÷VÁRv˜&∑76R◊&ˆ˜B÷7GÇ÷÷VÁRs∞¢÷VÁRÁ7Gñ∆RÊ775FWáC“w˜6óFñˆ„¶fóÜVC∂&6∂w&˜VÊCßf"Ç“◊7W&f6Rì∂&˜&FW#£Ç6ˆ∆ñBf"Ç“÷&˜&FW"ì∂&˜&FW"◊&FóW3£áÉ∑FFñÊs£gÇ∑¢÷ñÊFWÉ£ìììì∂÷ñ‚◊vñGFÉ£cÉ∂&˜Ç◊6ÜF˜s£GÇgÇ&v&É√√¬„3Rì≤s∞¢6ˆÁ7Bgs◊vñÊF˜rÊñÊÊW%vñGFÇ«fÉ◊vñÊF˜rÊñÊÊW$ÜVñváC∞¢÷VÁRÁ7Gñ∆RÊ∆VgC“ÜRÊ6∆ñVÁEÇ≥cÁgsˆRÊ6∆ñVÁEÇ”s¶RÊ6∆ñVÁEÇí≤wÇs∞¢÷VÁRÁ7Gñ∆RÁF˜“ÜRÊ6∆ñVÁEí≥ÉÁfÉˆRÊ6∆ñVÁEí”É¶RÊ6∆ñVÁEíí≤wÇs∞†¢÷VÁRÊVÊD6Üñ∆BÖ˜v˜&∑76T6ˆÁFWáD÷VÁTóFV“áBÇvÊWuˆfñ∆Rrí∆7ñÊ2Çì”Á∞¢÷VÁRÁ&V÷˜fRÇì∞¢vóB&ˆ◊DÊWtfñ∆RÇr‚rì∞¢“íì∞†¢÷VÁRÊVÊD6Üñ∆BÖ˜v˜&∑76T6ˆÁFWáD÷VÁTóFV“áBÇvÊWuˆfˆ∆FW"rí∆7ñÊ2Çì”Á∞¢÷VÁRÁ&V÷˜fRÇì∞¢vóB&ˆ◊DÊWtfˆ∆FW"Çr‚rì∞¢“íì∞†¢6ˆÁ7B7&VFU6W÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvá"rì∞¢7&VFU6WÁ7Gñ∆RÊ775FWáC“v&˜&FW#¶ÊˆÊS∂&˜&FW"◊F˜£Ç6ˆ∆ñBf"Ç“÷&˜&FW"ì∂÷&vñ„£GÇ≤s∞¢÷VÁRÊVÊD6Üñ∆BÜ7&VFU6Wì∞†¢÷VÁRÊVÊD6Üñ∆BÖ˜v˜&∑76T6ˆÁFWáD÷VÁTóFV“áBÇw&WfV≈ˆñÂˆfñÊFW"rí∆7ñÊ2Çì”Á∞¢÷VÁRÁ&V÷˜fRÇì∞¢G'ó∂vóBíÇrˆíˆfñ∆R˜&WfV¬r«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉ¢r‚w“ó“ì∑–¢6F6ÇÜW'"ó∑6Ü˜uFˆ7BáBÇw&WfV≈ˆfñ∆VBrí≤ÜW'"Ê÷W76vW«∆W'"íì∑–¢“íì∞†¢÷VÁRÊVÊD6Üñ∆BÖ˜v˜&∑76T6ˆÁFWáD÷VÁTóFV“áBÇv˜VÂˆñÂ˜g66ˆFRrí∆7ñÊ2Çì”Á∞¢÷VÁRÁ&V÷˜fRÇì∞¢G'ó∂vóBíÇrˆíˆfñ∆Rˆ˜V‚◊g66ˆFRr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉ¢r‚w“ó“ì∑–¢6F6ÇÜW'"ó∑6Ü˜uFˆ7BáBÇv˜VÂˆñÂ˜g66ˆFUˆfñ∆VBrí≤ÜW'"Ê÷W76vW«∆W'"íì∑–¢“íì∞†¢÷VÁRÊVÊD6Üñ∆BÖ˜v˜&∑76T6ˆÁFWáD÷VÁTóFV“áBÇv6˜ïˆfñ∆U˜FÇrí∆7ñÊ2Çì”Á∞¢÷VÁRÁ&V÷˜fRÇì∞¢G'ó∞¢6ˆÁ7B#÷vóBíÇrˆíˆfñ∆R˜FÇr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉ¢r‚w“ó“ì∞¢vóBˆ6˜ïFWáEvóFÑf∆∆&6≤Çá"bg"ÁFÇó«¬r‚r«BÇwFÖˆ6˜ñVBrí«BÇwFÖˆ6˜ïˆfñ∆VBríì∞¢÷6F6ÇÜW'"ó∑6Ü˜uFˆ7BáBÇwFÖˆ6˜ïˆfñ∆VBrí≤ÜW'"Ê÷W76vW«∆W'"íì∑–¢“íì∞†¢Fˆ7V÷VÁBÊ&ˆGíÊVÊD6Üñ∆BÜ÷VÁRì∞¢6ˆÁ7BFó6÷ó73“Çì”Á∂÷VÁRÁ&V÷˜fRÇì∂Fˆ7V÷VÁBÁ&V÷˜fTWfVÁD∆ó7FVÊW"Çv6∆ñ6≤r∆Fó6÷ó72ì∑”∞¢6WEFñ÷V˜WBÇÇì”ÊFˆ7V÷VÁBÊFDWfVÁD∆ó7FVÊW"Çv6∆ñ6≤r∆Fó6÷ó72í√ì∞ß–†¢ÚÚG&6≤WáÊFVBFó&V7F˜&ñW2f˜"G&VRfñWp¶ñbÇ2ÂˆWáÊFVDFó'2í2ÂˆWáÊFVDFó'3÷ÊWr6WBÇì∞¢ÚÚ66ÜRˆbfWF6ÜVBFó&V7F˜'í6ˆÁFVÁG3¢FÇ”‚VÁG&ñW5µ–¶ñbÇ2ÂˆFó$66ÜRí2ÂˆFó$66ÜS◊∑”∞†¶gVÊ7Fñˆ‚&VÊFW$fñ∆UG&VRÇó∞¢6ˆÁ7B&˜É“BÇvfñ∆UG&VRrì∞¢ÚÚ3ScSs¢6GW&RFÜR67&ˆ∆¬˜6óFñˆ‚&Vf˜&RvóñÊrFÜR6ˆÁFñÊW"‚&˜ÇÊñÊÊW$ÖD‘√“rp¢ÚÚFWF6ÜW2WfW'í&˜r¬6ˆ∆∆6ñÊr67&ˆ∆ƒÜVñváB6ÚFÜR'&˜w6W"6∆◊267&ˆ∆≈F˜FÚ∞¢ÚÚvóFÜ˜WBFÜó2¬WfW'íWáÊBˆ6ˆ∆∆6R¬'&VF7'V÷"Êb¬&Vg&W6Ç¬ÊBÜñFFV‚÷fñ∆W0¢ÚÚFˆvv∆RFÜB&R◊'VÁ2&VÊFW$fñ∆UG&VRÇíFV∆W˜'G2FÜR&VFW"&6≤FÚFÜRF˜ˆb¢ÚÚ∆ˆÊrG&VR‚&W7F˜&VBˆÊ«ígFW"FÜRÊ˜&÷¬&VÊFW"Fñ¬&V∆˜r(	BFÜRGvÚV&«í◊&WGW&‡¢ÚÚFá2ÜÊÚ◊v˜&∑76RÜñFW2FÜR&˜É≤V◊Gí÷Fó"Ü2Ê˜FÜñÊrFÚ67&ˆ∆¬í∆VvóFñ÷FV«ê¢ÚÚ&W6WB‚∆ñ‚67&ˆ∆≈F˜&W7F˜&R7Vffñ6W2ÜW&S¢WáÊBˆ6ˆ∆∆6RñÁ6W'B˜&V÷˜fR&˜w0¢ÚÚ$TƒırFÜR6∆ñ6∂VBFó66∆˜7W&R¬6ÚFÜR6∆ñ6∂VB&˜r∂VW2óG2ˆfg6WBg&ˆ“FÜRF˜ÜÊ¢ÚÚvWD&˜VÊFñÊt6∆ñVÁE&V7BÊ6Ü˜"FV«FÊVVFVB(	BFÜBw2ˆÊ«íf˜"&WVÊB÷&˜fR66W2í‡¢6ˆÁ7B&We67&ˆ∆≈F˜÷&˜Éˆ&˜ÇÁ67&ˆ∆≈F˜£∞¢&˜ÇÊñÊÊW$ÖD‘√“rs∞¢ÚÚ66ÜR7W'&VÁBFó"VÁG&ñW0¢2ÂˆFó$66ÜUµ2Ê7W'&VÁDFó'«¬r‚u”’2ÊVÁG&ñW3∞¢ÚÚ6Ü˜rV◊Gí◊7FFRvÜV‚ÊÚv˜&∑76Ró26WB˜"FÜRFó&V7F˜'íó2V◊GíÇ3s2ê¢6ˆÁ7BV◊GîV√“BÇww4V◊Gï7FFRrì∞¢6ˆÁ7BÜ5v˜&∑76S“Ö2Á6W76ñˆ‚be2Á6W76ñˆ‚Áv˜&∑76Rì∞¢ñbÇÜ5v˜&∑76Ró∞¢˜7ñÊ5v˜&∑76T&ó'FáFñ÷U7W˜'E66˜RÇrrì∞¢ñbÜV◊GîV¬ó∂V◊GîV¬ÁFWáD6ˆÁFVÁC◊BÇwv˜&∑76UˆV◊GïˆÊı˜FÇrì∂V◊GîV¬Á7Gñ∆RÊFó7∆ì“vf∆WÇs∑–¢&˜ÇÁ7Gñ∆RÊFó7∆ì“vÊˆÊRs∞¢&WGW&„∞¢–¢ˆÊ˜FUv˜&∑76T&ó'FáFñ÷U7W˜'BÖ2ÊVÁG&ñW2ì∞¢ñbÜV◊GîV¬íV◊GîV¬Á7Gñ∆RÊFó7∆ì“vÊˆÊRs∞¢&˜ÇÁ7Gñ∆RÊFó7∆ì“rs∞¢6ˆÁ7Bfó6ñ&∆TVÁG&ñW3’˜v˜&∑76TVÁG&ñW4f˜%&VÊFW"Ö2ÊVÁG&ñW2ì∞¢ñbÇfó6ñ&∆TVÁG&ñW2Ê∆VÊwFÇó∞¢ñbÜV◊GîV¬ó∂V◊GîV¬ÁFWáD6ˆÁFVÁC◊BÇwv˜&∑76UˆV◊GïˆFó"rì∂V◊GîV¬Á7Gñ∆RÊFó7∆ì“vf∆WÇs∑–¢&WGW&„∞¢–¢˜&VÊFW%G&VTóFV◊2Ü&˜Ç¬fó6ñ&∆TVÁG&ñW2¬ì∞¢ÚÚ3ScSs¢&W7F˜&RFÜR&R◊vóR67&ˆ∆¬˜6óFñˆ‚Ê˜rFÜBFÜRG&VRó2F∆¬vñ‚‡¢ñbÜ&˜Çí&˜ÇÁ67&ˆ∆≈F˜◊&We67&ˆ∆≈F˜∞ß–†¶∆WB˜w47FófTG&uFÉ÷ÁV∆√∞¶∆WB˜w47FófTG&uGóS÷ÁV∆√∞¶gVÊ7Fñˆ‚˜6WEw4G&tFFÜR∆óFV“ó∞¢RÊFFG&Á6fW"Á6WDFFÇv∆ñ6Fñˆ‚˜w2◊FÇr∆óFV“ÁFÇì∞¢RÊFFG&Á6fW"Á6WDFFÇv∆ñ6Fñˆ‚˜w2◊GóRr∆óFV“ÁGóRì∞¢RÊFFG&Á6fW"Á6WDFFÇwFWáB˜∆ñ‚r∆óFV“ÁFÇì∞¢˜w47FófTG&uFÉ÷óFV“ÁFÉ∞¢˜w47FófTG&uGóS÷óFV“ÁGóS∞ß–¶gVÊ7Fñˆ‚ˆ6∆V%w4G&tFFÇó∞¢˜w47FófTG&uFÉ÷ÁV∆√∞¢˜w47FófTG&uGóS÷ÁV∆√∞ß–¢ÚÚvñÊF˜r÷∆WfV¬f∆∆&6≤6∆VÁW¢ñbv˜&∑76RG&ró2&ÊFˆÊVBvóFÜ˜WBFÜP¢ÚÚ&˜rw2ˆÊG&vVÊBfó&ñÊrÜG&r6Ê6V∆∆VB¬G&˜VB˜WG6ñFRÁíF&vWB¬F ¢ÚÚ&«W'&VBˆÜñFFV‚÷ñB÷G&rí¬FÜR7FófR÷G&rf∆r◊W7BÊ˜B7W'fófR(	B˜FÜW'vó6R¢ÚÚ∆FW"dı$Tît‚FWáB˜∆ñ‚G&r6˜V∆B&R÷ó7&VB2v˜&∑76R÷˜fR‡¶ñbáGóVˆbvñÊF˜r”“wVÊFVfñÊVBrbbvñÊF˜rÂ˜w4G&t6∆VÁW&˜VÊBó∞¢vñÊF˜rÂ˜w4G&t6∆VÁW&˜VÊC◊G'VS∞¢vñÊF˜rÊFDWfVÁD∆ó7FVÊW"ÇvG&vVÊBr≈ˆ6∆V%w4G&tFF«G'VRì∞¢ÚÚFVfW"FÜRG&˜6∆VÁWFñ6≥¢FÜó26GW&R◊Ü6RvñÊF˜r∆ó7FVÊW"fó&W0¢ÚÚ$Tdı$RFÜRF&vWBV∆V÷VÁBw2ˆÊG&˜¬6Ú6∆V&ñÊr7ñÊ6á&ˆÊ˜W6«íÜW&Rv˜V∆@¢ÚÚvóR˜w47FófTG&uFÇ&Vf˜&Rˆó5v˜&∑76UG&VT÷˜fTG&rÇíı˜w4G&u7&5FÇÇê¢ÚÚ'V‚ñ‚FÜRF&vWBÜÊF∆W"(	B&R÷'&V∂ñÊrFÜR÷4ı27G&óVB‘‘î‘R÷˜fR‚FÜP¢ÚÚ6WEFñ÷V˜WB∆WG2FÜR&V¬G&˜ÜÊF∆W"6ˆ◊∆WFR¬FÜV‚6∆V'2FÜR∆ñÊvW&ñÊrf∆r‡¢vñÊF˜rÊFDWfVÁD∆ó7FVÊW"ÇvG&˜r¬Çì”Á6WEFñ÷V˜WBÖˆ6∆V%w4G&tFF√í«G'VRì∞¢vñÊF˜rÊFDWfVÁD∆ó7FVÊW"ÇwvVÜñFRr≈ˆ6∆V%w4G&tFFì∞¢vñÊF˜rÊFDWfVÁD∆ó7FVÊW"Çv&«W"r≈ˆ6∆V%w4G&tFFì∞ß–¶gVÊ7Fñˆ‚ˆó5v˜&∑76UG&VT÷˜fTG&rÜRó∞¢ñbÜRÊFFG&Á6fW"bfRÊFFG&Á6fW"ÁGóW2bfRÊFFG&Á6fW"ÁGóW2ÊñÊ6«VFW2Çtfñ∆W2ríí&WGW&‚f«6S∞¢ñbÜRÊFFG&Á6fW"bfRÊFFG&Á6fW"ÁGóW2bfRÊFFG&Á6fW"ÁGóW2ÊñÊ6«VFW2Çv∆ñ6Fñˆ‚˜w2◊FÇríí&WGW&‚G'VS∞¢ÚÚ7G&óVB‘‘î‘RÜ÷4ı2vV$∂óBíf∆∆&6≥¢66WBFWáB˜∆ñ‚Ù‰≈ívÜñ∆R¢ÚÚv˜&∑76RG&ró2vVÁVñÊV«íñ‚f∆ñváB‚G&v˜fW"ˆG&˜WfVÁG26‚wB&VBFÜP¢ÚÚñ∆ˆB¬6ÚvFRˆ‚FÜR7FófRf∆r∆ˆÊRÜW&S≤FÜRG&˜ÜÊF∆W"FFóFñˆÊ∆«ê¢ÚÚ&˜fW2FWáB˜∆ñ‚””“˜w47FófTG&uFÇ&Vf˜&RW&f˜&÷ñÊrFÜR÷˜fR‡¢&WGW&‚Ö˜w47FófTG&uFÇbfRÊFFG&Á6fW"bfRÊFFG&Á6fW"ÁGóW2bfRÊFFG&Á6fW"ÁGóW2ÊñÊ6«VFW2ÇwFWáB˜∆ñ‚ríì∞ß–¶gVÊ7Fñˆ‚˜w4G&u7&5FÇÜRó∞¢6ˆÁ7B7W7Fˆ”÷RÊFFG&Á6fW"ÊvWDFFÇv∆ñ6Fñˆ‚˜w2◊FÇrì∞¢ñbÜ7W7Fˆ“í&WGW&‚7W7Fˆ”∞¢ÚÚ7G&óVB‘‘î‘Rf∆∆&6≥¢ˆÊ«íG'W7BFÜR7FófRf∆rvÜV‚FÜRG&˜w2˜v‡¢ÚÚFWáB˜∆ñ‚÷F6ÜW2óB‚f˜&Vñv‚FWáB˜∆ñ‚G&rÜFñffW&VÁBˆV◊Gí6ˆÁFVÁBê¢ÚÚ◊W7B‰ıB&W6ˆ«fRFÚ˜W"G&6∂VBv˜&∑76RFÇWfV‚ñbFÜRf∆r∆ñÊvW&VB‡¢6ˆÁ7B∆ñ„÷RÊFFG&Á6fW"ÊvWDFFÇwFWáB˜∆ñ‚ró«¬rs∞¢ñbÖ˜w47FófTG&uFÇbg∆ñ„””’˜w47FófTG&uFÇí&WGW&‚˜w47FófTG&uFÉ∞¢&WGW&‚rs∞ß–¶gVÊ7Fñˆ‚˜w4G&u7&5GóRÜRó∞¢6ˆÁ7B7W7Fˆ”÷RÊFFG&Á6fW"ÊvWDFFÇv∆ñ6Fñˆ‚˜w2◊GóRrì∞¢ñbÜ7W7Fˆ“í&WGW&‚7W7Fˆ”∞¢&WGW&‚˜w47FófTG&uGóW«¬vfñ∆Rs∞ß–†¶gVÊ7Fñˆ‚˜v˜&∑76U&VÁDFó"á&V≈FÇó∞¢ñbÇ&V≈Fá««&V≈FÉ””“r‚ró&WGW&‚r‚s∞¢6ˆÁ7BñGÉ◊&V≈FÇÊ∆7DñÊFWÑˆbÇrÚrì∞¢&WGW&‚ñGÉ””“”Úr‚sß&V≈FÇÁ7V'7G&ñÊrÉ∆ñGÇì∞ß–†¶gVÊ7Fñˆ‚ˆ6∆V%v˜&∑76T÷˜fTG&t˜fW"Çó∞¢Fˆ7V÷VÁBÁVW'ï6V∆V7F˜$∆¬ÇrÊfñ∆R÷óFV“ÊG&r÷˜fW"¬Ê'&VF7'V÷"◊6VrÊG&r÷˜fW"ríÊf˜$V6ÇÜV√”ÊV¬Ê6∆74∆ó7BÁ&V÷˜fRÇvG&r÷˜fW"ríì∞ß–†¶gVÊ7Fñˆ‚˜&V÷v˜&∑76T66ÜW4gFW$÷˜fRÜˆ∆EFÇ∆ÊWuFÇ∆ó4Fó"ó∞¢ñbÜó4Fó"be2ÂˆWáÊFVDFó'2ó∞¢ñbÖ2ÂˆWáÊFVDFó'2ÊÜ2Üˆ∆EFÇíó∞¢2ÂˆWáÊFVDFó'2ÊFV∆WFRÜˆ∆EFÇì∞¢2ÂˆWáÊFVDFó'2ÊFBÜÊWuFÇì∞¢–¢f˜"Ü6ˆÁ7BWáÊFVEFÇˆb≤‚‚Â2ÂˆWáÊFVDFó'5“ó∞¢ñbÜWáÊFVEFÇÁ7F'G5vóFÇÜˆ∆EFÇ≤rÚríó∞¢2ÂˆWáÊFVDFó'2ÊFV∆WFRÜWáÊFVEFÇì∞¢2ÂˆWáÊFVDFó'2ÊFBÜÊWuFÇ∂WáÊFVEFÇÁ6∆ñ6RÜˆ∆EFÇÊ∆VÊwFÇíì∞¢–¢–¢ñbÖ2ÂˆFó$66ÜU∂ˆ∆EFÖ“ó∞¢2ÂˆFó$66ÜU∂ÊWuFÖ”’2ÂˆFó$66ÜU∂ˆ∆EFÖ”∞¢FV∆WFR2ÂˆFó$66ÜU∂ˆ∆EFÖ”∞¢–¢f˜"Ü6ˆÁ7B66ÜUFÇˆbˆ&¶V7BÊ∂Wó2Ö2ÂˆFó$66ÜRíó∞¢ñbÜ66ÜUFÇÁ7F'G5vóFÇÜˆ∆EFÇ≤rÚríó∞¢6ˆÁ7B&V÷VC÷ÊWuFÇ∂66ÜUFÇÁ6∆ñ6RÜˆ∆EFÇÊ∆VÊwFÇì∞¢2ÂˆFó$66ÜU∑&V÷VE”’2ÂˆFó$66ÜU∂66ÜUFÖ”∞¢FV∆WFR2ÂˆFó$66ÜU∂66ÜUFÖ”∞¢–¢–¢ñbáGóVˆb˜6fTWáÊFVDFó'3””“vgVÊ7Fñˆ‚rï˜6fTWáÊFVDFó'2Çì∞¢–¢FV∆WFR2ÂˆFó$66ÜUµ˜v˜&∑76U&VÁDFó"Üˆ∆EFÇï”∞¢FV∆WFR2ÂˆFó$66ÜUµ˜v˜&∑76U&VÁDFó"ÜÊWuFÇï”∞¢ñbáGóVˆb˜&WfñWt7W'&VÁEFÇ”“wVÊFVfñÊVBrbe˜&WfñWt7W'&VÁEFÇó∞¢ñbÖ˜&WfñWt7W'&VÁEFÉ””÷ˆ∆EFÇï˜&WfñWt7W'&VÁEFÉ÷ÊWuFÉ∞¢V«6RñbÖ˜&WfñWt7W'&VÁEFÇÁ7F'G5vóFÇÜˆ∆EFÇ≤rÚríï˜&WfñWt7W'&VÁEFÉ÷ÊWuFÇµ˜&WfñWt7W'&VÁEFÇÁ6∆ñ6RÜˆ∆EFÇÊ∆VÊwFÇì∞¢–ß–†¶7ñÊ2gVÊ7Fñˆ‚˜W&f˜&’v˜&∑76T÷˜fRá7&5FÇ∆FW7DFó"∆ó4Fó"ó∞¢ñbÇ2Á6W76ñˆÁ«¬7&5FÇó&WGW&„∞¢6ˆÁ7BÊ˜&‘FW7C÷FW7DFó'«¬r‚s∞¢ñbá7&5FÉ””÷Ê˜&‘FW7Bó&WGW&„∞¢ñbÜÊ˜&‘FW7BÁ7F'G5vóFÇá7&5FÇ≤rÚríó&WGW&„∞¢ñbÖ˜v˜&∑76U&VÁDFó"á7&5FÇì””÷Ê˜&‘FW7Bó&WGW&„∞¢G'ó∞¢6ˆÁ7BFF÷vóBíÇrˆíˆfñ∆Rˆ÷˜fRr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∞¢6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉß7&5FÇ∆FW7EˆFó#¶Ê˜&‘FW7@¢“ó“ì∞¢6ˆÁ7B÷˜fVDÊ÷S÷FFÊÊWu˜FÇÊñÊ6«VFW2ÇrÚrìˆFFÊÊWu˜FÇÁ6∆ñ6RÜFFÊÊWu˜FÇÊ∆7DñÊFWÑˆbÇrÚrí≥ì¶FFÊÊWu˜FÉ∞¢6Ü˜uFˆ7BÇáBÇv÷˜fVE˜FÚró«¬t÷˜fVBFÚrí∂÷˜fVDÊ÷Rì∞¢˜&V÷v˜&∑76T66ÜW4gFW$÷˜fRÜFFÊˆ∆E˜Fá««7&5FÇ∆FFÊÊWu˜Fá««7&5FÇ∆ó4Fó"ì∞¢vóB∆ˆDFó"Ö2Ê7W'&VÁDFó"ì∞¢ñbáGóVˆb&Vg&W6Ñ˜VÂ&WfñWtñd◊WFFVC””“vgVÊ7Fñˆ‚rñvóB&Vg&W6Ñ˜VÂ&WfñWtñd◊WFFVBÇì∞¢÷6F6ÇÜW'"ó∞¢6Ü˜uFˆ7BÇáBÇv÷˜fUˆfñ∆VBró«¬t÷˜fRfñ∆VC¢rí∂W'"Ê÷W76vR√S¬vW'&˜"rì∞¢–ß–†¶gVÊ7Fñˆ‚ˆ&ñÊEv˜&∑76T÷˜fTG&˜F&vWBÜV¬∆FW7DFó"ó∞¢V¬ÊˆÊG&vVÁFW#“ÜRì”Á∞¢ñbÇˆó5v˜&∑76UG&VT÷˜fTG&rÜRíó&WGW&„∞¢RÁ&WfVÁDFVfV«BÇì∂RÁ7F˜&˜vFñˆ‚Çì∞¢V¬Ê6∆74∆ó7BÊFBÇvG&r÷˜fW"rì∞¢”∞¢V¬ÊˆÊG&v˜fW#“ÜRì”Á∞¢ñbÇˆó5v˜&∑76UG&VT÷˜fTG&rÜRíó&WGW&„∞¢RÁ&WfVÁDFVfV«BÇì∂RÁ7F˜&˜vFñˆ‚Çì∞¢RÊFFG&Á6fW"ÊG&˜VffV7C“v÷˜fRs∞¢V¬Ê6∆74∆ó7BÊFBÇvG&r÷˜fW"rì∞¢”∞¢V¬ÊˆÊG&v∆VfS“ÜRì”Á∞¢ñbÜV¬Ê6ˆÁFñÁ2ÜRÁ&V∆FVEF&vWBíó&WGW&„∞¢V¬Ê6∆74∆ó7BÁ&V÷˜fRÇvG&r÷˜fW"rì∞¢”∞¢V¬ÊˆÊG&˜÷7ñÊ2ÜRì”Á∞¢ñbÇˆó5v˜&∑76UG&VT÷˜fTG&rÜRíó&WGW&„∞¢RÁ&WfVÁDFVfV«BÇì∂RÁ7F˜&˜vFñˆ‚Çì∞¢V¬Ê6∆74∆ó7BÁ&V÷˜fRÇvG&r÷˜fW"rì∞¢G'ó∞¢6ˆÁ7B7&5FÉ’˜w4G&u7&5FÇÜRì∞¢ñbÇ7&5FÇó&WGW&„∞¢6ˆÁ7B7&5GóS’˜w4G&u7&5GóRÜRì∞¢vóB˜W&f˜&’v˜&∑76T÷˜fRá7&5FÇ∆FW7DFó"«7&5GóS””“vFó"rì∞¢÷fñÊ∆«ó∞¢ˆ6∆V%w4G&tFFÇì∞¢–¢”∞ß–†¶gVÊ7Fñˆ‚V∆ñFT÷ñFF∆Rá7G"¬÷Ñ∆V‚“cí∞¢ñbá7G"Ê∆VÊwFÇ√“÷Ñ∆V‚í&WGW&‚7G#∞¢6ˆÁ7BÜ∆b“÷FÇÊf∆ˆ˜"ÇÜ÷Ñ∆V‚“2íÚ"ì∞¢&WGW&‚7G"Á6∆ñ6RÉ¬Ü∆bí≤r‚‚‚r≤7G"Á6∆ñ6Rá7G"Ê∆VÊwFÇ“Ü∆bì∞ß–†¶gVÊ7Fñˆ‚˜&VÊFW%G&VTóFV◊2Ü6ˆÁFñÊW"¬VÁG&ñW2¬FWFÇó∞¢f˜"Ü6ˆÁ7BóFV“ˆbVÁG&ñW2ó∞¢6ˆÁ7BV√÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∂V¬Ê6∆74Ê÷S“vfñ∆R÷óFV“s∞¢V¬Á7Gñ∆RÁFFñÊt∆VgC“ÉÇ∂FWFÇ£bí≤wÇs∞¢V¬Á6WDGG&ñ'WFRÇvG&vv&∆Rr¬wG'VRrì∞¢V¬ÊFF6WBÁw5GóS÷óFV“ÁGóS∞¢V¬ÊˆÊ6ˆÁFWáF÷VÁS“ÜRì”Á∞¢6ˆÁ7Bw&ÁC◊GóVˆb˜v˜&∑76TW66Tw&ÁDf˜%FÉ””“vgVÊ7Fñˆ‚rÚ˜v˜&∑76TW66Tw&ÁDf˜%FÇÜóFV“ÁFÇí¢ÁV∆√∞¢6ˆÁ7Bó4Fó%&˜s÷óFV“ÁGóS””“vFó"w«¬ÜóFV“ÁGóS””“w7ñ÷∆ñÊ≤rbfóFV“Êó5ˆFó"ì∞¢ñbÜw&ÁBbbó4Fó%&˜ró∂RÁ&WfVÁDFVfV«BÇì∂RÁ7F˜&˜vFñˆ‚Çì∑&WGW&„∑–¢RÁ&WfVÁDFVfV«BÇì∂RÁ7F˜&˜vFñˆ‚Çìµ˜6Ü˜tfñ∆T6ˆÁFWáD÷VÁRÜR∆óFV“ì∞¢”∞¢V¬ÊˆÊG&w7F'C“ÜRì”Áµ˜6WEw4G&tFFÜR∆óFV“ì∂RÊFFG&Á6fW"ÊVffV7D∆∆˜vVC“v6˜ís∂V¬Ê6∆74∆ó7BÊFBÇvG&vvñÊrrì∑”∞¢V¬ÊˆÊG&vVÊC“Çì”Á∂V¬Ê6∆74∆ó7BÁ&V÷˜fRÇvG&vvñÊrrìµˆ6∆V%v˜&∑76T÷˜fTG&t˜fW"Çìµˆ6∆V%w4G&tFFÇì∑”∞†¢6ˆÁ7Bó4∆≤“óFV“ÁGóR””“w7ñ÷∆ñÊ≤s∞¢6ˆÁ7Bó4WáFW&Êƒ∆ñÊ≤“ó4∆≤bbóFV“ÁF&vWEˆ˜WG6ñFU˜v˜&∑76S∞¢6ˆÁ7BW66Tw&ÁB“GóVˆb˜v˜&∑76TW66Tw&ÁDf˜%FÇ””“vgVÊ7Fñˆ‚rÚ˜v˜&∑76TW66Tw&ÁDf˜%FÇÜóFV“ÁFÇí¢ÁV∆√∞¢6ˆÁ7BWÜ7DW66Tw&ÁB“GóVˆb˜v˜&∑76TW66TWÜ7Dw&ÁB””“vgVÊ7Fñˆ‚rÚ˜v˜&∑76TW66TWÜ7Dw&ÁBÜóFV“ÁFÇí¢ÁV∆√∞¢6ˆÁ7Bó5&VDˆÊ«îW66R“W66Tw&ÁC∞¢6ˆÁ7Bó4ÊW7FVDW66R“W66Tw&ÁBbbWÜ7DW66Tw&ÁC∞¢ÚÚWáFW&Ê¬7ñ÷∆ñÊ∑2&RFó7∆í÷ˆÊ«ì¢Ê˜BWáÊF&∆R¬Ê˜B˜VÊ&∆R‡¢ÚÚFÜR&VBvFRá6fU˜&W6ˆ«fU˜w2í7Fñ∆¬&∆ˆ6∑2ÊfñvFñˆ‚Fá&˜VvÇFÜV“‡¢6ˆÁ7Bó4Fó$∆ñ∂R“ó4WáFW&Êƒ∆ñÊ≤bbÜóFV“ÁGóR””“vFó"r«¬Üó4∆≤bbóFV“Êó5ˆFó"íì∞¢6ˆÁ7Bó4fñ∆T∆ñ∂R“ó4WáFW&Êƒ∆ñÊ≤bbó4Fó$∆ñ∂S∞¢V¬ÊFF6WBÁw4ó4Fó"“7G&ñÊrÜó4Fó$∆ñ∂Rì∞¢ñbÜó4WáFW&Êƒ∆ñÊ≤«¬ó5&VDˆÊ«îW66Ró∂V¬Á&V÷˜fTGG&ñ'WFRÇvG&vv&∆Rrì∂V¬ÊˆÊG&w7F'C÷ÁV∆√∑–†¢ñbÜó4Fó$∆ñ∂Ró∞¢ÚÚFˆvv∆R'&˜rf˜"Fó&V7F˜&ñW0¢6ˆÁ7B'&˜s÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢'&˜rÊ6∆74Ê÷S“vfñ∆R◊G&VR◊Fˆvv∆Rs∞¢6ˆÁ7Bó4WáÊFVC’2ÂˆWáÊFVDFó'2ÊÜ2ÜóFV“ÁFÇì∞¢'&˜rÁFWáD6ˆÁFVÁC÷ó4WáÊFVCÚu«S#T$Rs¢u«S#T#Çs∞¢V¬ÊVÊD6Üñ∆BÜ'&˜rì∞¢÷V«6W∞¢ÚÚ∂VWfñ∆Rñ6ˆÁ2∆ñvÊVBvóFÇ6ñ&∆ñÊrFó&V7F˜&ñW2FÜBˆ67WíFÜó0¢ÚÚ6∆˜BvóFÇFÜRWáÊBˆ6ˆ∆∆6RFˆvv∆R‚3#SS@¢6ˆÁ7B76W#÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢76W"Ê6∆74Ê÷S“vfñ∆R◊G&VR◊Fˆvv∆R◊∆6VÜˆ∆FW"s∞¢76W"Á6WDGG&ñ'WFRÇv&ñ÷ÜñFFV‚r¬wG'VRrì∞¢V¬ÊVÊD6Üñ∆Bá76W"ì∞¢–†¢ÚÚñ6ˆ‡¢6ˆÁ7Bñ6ˆ‰V√÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢ñ6ˆ‰V¬Ê6∆74Ê÷S“vfñ∆R÷ñ6ˆ‚s∞¢ñ6ˆ‰V¬ÊñÊÊW$ÖD‘¬“ó4WáFW&Êƒ∆ñÊ∞¢Ú∆íÇvWáFW&Ê¬÷∆ñÊ≤r¬Bê¢¢ó4Fó$∆ñ∂P¢ÚÜó4∆≤Ú∆íÇv∆ñÊ≤r¬Bí¢∆íÇvfˆ∆FW"r¬Bíê¢¢Üó4∆≤Ú∆íÇv∆ñÊ≤r¬Bí¢fñ∆Tñ6ˆ‚ÜóFV“ÊÊ÷R¬óFV“ÁGóRíì∞¢V¬ÊVÊD6Üñ∆BÜñ6ˆ‰V¬ì∞†¢ÚÚÊ÷P¢6ˆÁ7BÊ÷TV√÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢Ê÷TV¬Ê6∆74Ê÷S“vfñ∆R÷Ê÷Rs∂Ê÷TV¬ÁFWáD6ˆÁFVÁC÷óFV“ÊÊ÷S∞¢ÚÚFˆˆ«FóˆÊ«íˆ‚dîƒU2(	BF&∆6∆ñ6≤&VÊ÷W2FÜV“‚ˆ‚Fó&V7F˜&ñW2¬F&∆6∆ñ6∞¢ÚÚÊfñvFW2ñÁFÚFÜRfˆ∆FW#≤&VÊ÷R∆ófW2ñ‚FÜR&ñváB÷6∆ñ6≤6ˆÁFWáB÷VÁP¢ÚÚáFÜR$F˜V&∆R÷6∆ñ6≤FÚ&VÊ÷R"ÜñÁBÜW&Rv˜V∆B&R÷ó6∆VFñÊrí‚3s‡¢ñbÜó4∆≤bbóFV“ÁF&vWBê¢Ê÷TV¬ÁFóF∆R“BÇw7ñ÷∆ñÊµˆ∆ñÊµ˜FÚríÁ&W∆6RÇw∑F&vWG“r¬Çí”‚V∆ñFT÷ñFF∆RÜóFV“ÁF&vWBíì∞¢V«6RñbÜó4WáFW&Êƒ∆ñÊ≤ê¢Ê÷TV¬ÁFóF∆R“áGóVˆbó5&VDˆÊ«îW66R”“wVÊFVfñÊVBp¢Úó5&VDˆÊ«îW66P¢¢áGóVˆb˜v˜&∑76TW66Tw&ÁDf˜%FÉ””“vgVÊ7Fñˆ‚rÚ˜v˜&∑76TW66Tw&ÁDf˜%FÇÜóFV“ÁFÇí¢f«6Ríê¢ÚBÇvWáFW&Ê≈ˆ∆ñÊµ˜&VEˆˆÊ«írê¢¢BÇvWáFW&Ê≈ˆ∆ñÊµˆ˜VÂˆ6ˆÊfó&“rì∞¢V«6RñbáGóVˆbó5&VDˆÊ«îW66R”“wVÊFVfñÊVBp¢Úó5&VDˆÊ«îW66P¢¢áGóVˆb˜v˜&∑76TW66Tw&ÁDf˜%FÉ””“vgVÊ7Fñˆ‚rÚ˜v˜&∑76TW66Tw&ÁDf˜%FÇÜóFV“ÁFÇí¢f«6Ríê¢Ê÷TV¬ÁFóF∆R“BÇvWáFW&Ê≈ˆ∆ñÊµ˜&VEˆˆÊ«írì∞¢V«6RñbÇó4Fó$∆ñ∂Rê¢Ê÷TV¬ÁFóF∆R“BÇvF˜V&∆Uˆ6∆ñ6µ˜&VÊ÷Rrì∞¢6ˆÁ7BÊ÷Tó5&VDˆÊ«îW66S◊GóVˆbó5&VDˆÊ«îW66R”“wVÊFVfñÊVBp¢Úó5&VDˆÊ«îW66P¢¢áGóVˆb˜v˜&∑76TW66Tw&ÁDf˜%FÉ””“vgVÊ7Fñˆ‚rÚ˜v˜&∑76TW66Tw&ÁDf˜%FÇÜóFV“ÁFÇí¢f«6Rì∞¢ÚÚ6ñÊv∆R÷6∆ñ6≤˜VÁ2Üfñ∆Rí˜"WáÊB◊Fˆvv∆W2ÜFó"í'WBó2FV&˜VÊ6VB3◊26Ú¢ÚÚF˜V&∆R÷6∆ñ6≤6‚6Ê6V¬óBÊBG&ñvvW"&VÊ÷RñÁ7FVB‚vóFÜ˜WBFÜRFV&˜VÊ6R¬FÜP¢ÚÚ6∆ñ6≤'V&&∆W2FÚV¬ÊˆÊ6∆ñ6≤&Vf˜&RF&∆6∆ñ6≤6‚fó&R(	BFÜBw23cìÇ‚vóFÜ˜WBFÜP¢ÚÚ&W7F˜&VB7FófFñˆ‚¬6ñÊv∆R÷6∆ñ6≤ˆ‚FÜRfñ∆VÊ÷RFˆW2Ê˜FÜñÊr(	BFÜBw23sr‡¢∆WBˆÊ÷T6∆ñ6µFñ÷W#÷ÁV∆√∞¢Ê÷TV¬ÊˆÊ6∆ñ6≥“ÜRì”Á∞¢RÁ7F˜&˜vFñˆ‚Çì∞¢ñbÖˆÊ÷T6∆ñ6µFñ÷W"ó∂6∆V%Fñ÷V˜WBÖˆÊ÷T6∆ñ6µFñ÷W"ìµˆÊ÷T6∆ñ6µFñ÷W#÷ÁV∆√∑–¢ˆÊ÷T6∆ñ6µFñ÷W#◊6WEFñ÷V˜WBÇÇì”Á∞¢ˆÊ÷T6∆ñ6µFñ÷W#÷ÁV∆√∞¢ÚÚFV∆VvFRFÚFÜR&˜rw2WÜó7FñÊr6ñÊv∆R÷6∆ñ6≤ÜÊF∆W"Ü˜V‰fñ∆RÚFó"Fˆvv∆Rí‡¢ñbáGóVˆbV¬ÊˆÊ6∆ñ6≥””“vgVÊ7Fñˆ‚rñV¬ÊˆÊ6∆ñ6≤ÜRì∞¢“√3ì∞¢”∞¢Ê÷TV¬ÊˆÊF&∆6∆ñ6≥“ÜRì”Á∞¢RÁ7F˜&˜vFñˆ‚Çì∞¢ñbÖˆÊ÷T6∆ñ6µFñ÷W"ó∂6∆V%Fñ÷V˜WBÖˆÊ÷T6∆ñ6µFñ÷W"ìµˆÊ÷T6∆ñ6µFñ÷W#÷ÁV∆√∑–¢ÚÚf˜"Fó&V7F˜&ñW2¬F˜V&∆R÷6∆ñ6≤ÊfñvFW2Ü'&VF7'V÷"fñWrê¢ñbÜó4Fó$∆ñ∂Ró∂∆ˆDFó"ÜóFV“ÁFÇì∑&WGW&„∑–¢ÚÚW66R◊&ˆ˜B&˜w2&V÷ñ‚'&˜w6R÷ˆÊ«í¬ÊW7FVBW66R&˜w27FíFó7∆í÷ˆÊ«í‡¢ñbÜÊ÷Tó5&VDˆÊ«îW66Ró∞¢ñbÜó4WáFW&Êƒ∆ñÊ≤ó∂ñbáGóVˆbV¬ÊˆÊ6∆ñ6≥””“vgVÊ7Fñˆ‚rñV¬ÊˆÊ6∆ñ6≤ÜRì∑&WGW&„∑–¢˜V‰fñ∆RÜóFV“ÁFÇì∞¢&WGW&„∞¢–¢6ˆÁ7BñÁ÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvñÁWBrì∞¢ñÁÊ6∆74Ê÷S“vfñ∆R◊&VÊ÷R÷ñÁWBs∂ñÁÁf«VS÷óFV“ÊÊ÷S∞¢ñÁÊˆÊ6∆ñ6≥“ÜS"ì”ÊS"Á7F˜&˜vFñˆ‚Çì∞¢6ˆÁ7BfñÊó6É÷7ñÊ2á6fRì”Á∞¢ñÁÊˆÊ&«W#÷ÁV∆√∞¢ñbá6fRó∞¢6ˆÁ7BÊWtÊ÷S÷ñÁÁf«VRÁG&ñ“Çì∞¢ñbÜÊWtÊ÷RbfÊWtÊ÷R”÷óFV“ÊÊ÷Ró∞¢G'ó∞¢vóBíÇrˆíˆfñ∆R˜&VÊ÷Rr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∞¢6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉ¶óFV“ÁFÇ∆ÊWuˆÊ÷S¶ÊWtÊ÷P¢“ó“ì∞¢6Ü˜uFˆ7BáBÇw&VÊ÷VE˜FÚrí∂ÊWtÊ÷Rì∞¢ÚÚWFFRWáÊFVBFó'266ÜR∂Wíñb&VÊ÷ñÊrFó&V7F˜'ê¢ñbÜó4Fó$∆ñ∂Rbe2ÂˆWáÊFVDFó'2ó∞¢2ÂˆWáÊFVDFó'2ÊFV∆WFRÜóFV“ÁFÇì∞¢6ˆÁ7B&VÁC÷óFV“ÁFÇÊñÊ6«VFW2ÇrÚrìˆóFV“ÁFÇÁ7V'7G&ñÊrÉ∆óFV“ÁFÇÊ∆7DñÊFWÑˆbÇrÚríì¢r‚s∞¢6ˆÁ7BÊWuFÉ◊&VÁC””“r‚sˆÊWtÊ÷Sß&VÁB≤rÚr∂ÊWtÊ÷S∞¢2ÂˆWáÊFVDFó'2ÊFBÜÊWuFÇì∞¢ñbÖ2ÂˆFó$66ÜU∂óFV“ÁFÖ“óµ2ÂˆFó$66ÜU∂ÊWuFÖ”’2ÂˆFó$66ÜU∂óFV“ÁFÖ”∂FV∆WFR2ÂˆFó$66ÜU∂óFV“ÁFÖ”∑–¢ñbáGóVˆb˜6fTWáÊFVDFó'3””“vgVÊ7Fñˆ‚rï˜6fTWáÊFVDFó'2Çì∞¢–¢ÚÚñÁf∆ñFFR66ÜRÊB&R◊&VÊFW ¢FV∆WFR2ÂˆFó$66ÜUµ2Ê7W'&VÁDFó%”∞¢vóB∆ˆDFó"Ö2Ê7W'&VÁDFó"ì∞¢÷6F6ÇÜW'"ó∑6Ü˜uFˆ7BáBÇw&VÊ÷Uˆfñ∆VBrí∂W'"Ê÷W76vRì∑–¢–¢–¢ñÁÁ&W∆6UvóFÇÜÊ÷TV¬ì∞¢”∞¢ñÁÊˆÊ∂WñF˜v„“ÜS"ì”Á∞¢ñbÜS"Ê∂Wì””“tVÁFW"ró∞¢ñbávñÊF˜rÂˆó4ñ÷TVÁFW"bgvñÊF˜rÂˆó4ñ÷TVÁFW"ÜS"íó∑&WGW&„∑–¢S"Á&WfVÁDFVfV«BÇì∞¢fñÊó6ÇáG'VRì∞¢–¢ñbÜS"Ê∂Wì””“tW66Rró∂S"Á&WfVÁDFVfV«BÇì∂fñÊó6ÇÜf«6Rì∑–¢”∞¢ñÁÊˆÊ&«W#“Çì”ÊfñÊó6ÇÜf«6Rì∞¢Ê÷TV¬Á&W∆6UvóFÇÜñÁì∞¢6WEFñ÷V˜WBÇÇì”Á∂ñÁÊfˆ7W2Çì∂ñÁÁ6V∆V7BÇì∑“√ì∞¢”∞¢V¬ÊVÊD6Üñ∆BÜÊ÷TV¬ì∞†¢ÚÚ6ó¶R““f˜"&V¬fñ∆W2ÊB7ñ÷∆ñÊ∑2FÜB&W6ˆ«fRFÚfñ∆W0¢ñbÜó4fñ∆T∆ñ∂RbfóFV“Á6ó¶Ró∞¢6ˆÁ7B6ó¶TV√÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇw7‚rì∞¢6ó¶TV¬Ê6∆74Ê÷S“vfñ∆R◊6ó¶Rs∞¢6ó¶TV¬ÁFWáD6ˆÁFVÁC÷G≤ÜóFV“Á6ó¶RÛ#BíÁFÙfóÜVBÉó÷∂∞¢V¬ÊVÊD6Üñ∆Bá6ó¶TV¬ì∞¢–†¢ÚÚFV∆WFR'WGFˆ‚““f˜"fñ∆R÷∆ñ∂R&˜w2ÊBFó&V7F˜'í÷∆ñ∂R&˜w0¢ñbÜó4fñ∆T∆ñ∂Ró∞¢ñbÇó5&VDˆÊ«îW66Ró∞¢6ˆÁ7BFV√÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇv'WGFˆ‚rì∞¢FV¬Ê6∆74Ê÷S“vfñ∆R÷FV¬÷'F‚s∂FV¬ÁFóF∆S◊BÇvFV∆WFU˜FóF∆Rrì∂FV¬ÁFWáD6ˆÁFVÁC“u«SCrs∞¢FV¬ÊˆÊ6∆ñ6≥÷7ñÊ2ÜRì”Á∂RÁ7F˜&˜vFñˆ‚Çì∂vóBFV∆WFUv˜&∑76Tfñ∆RÜóFV“ÁFÇ∆óFV“ÊÊ÷Rì∑”∞¢V¬ÊVÊD6Üñ∆BÜFV¬ì∞¢–¢÷V«6RñbÜó4Fó$∆ñ∂Rbbó5&VDˆÊ«îW66Ró∞¢6ˆÁ7BFV√÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇv'WGFˆ‚rì∞¢FV¬Ê6∆74Ê÷S“vfñ∆R÷FV¬÷'F‚s∂FV¬ÁFóF∆S◊BÇvFV∆WFU˜FóF∆Rrì∂FV¬ÁFWáD6ˆÁFVÁC“u«SCrs∞¢FV¬ÊˆÊ6∆ñ6≥÷7ñÊ2ÜRì”Á∂RÁ7F˜&˜vFñˆ‚Çì∂vóBFV∆WFUv˜&∑76TFó"ÜóFV“ÁFÇ∆óFV“ÊÊ÷Rì∑”∞¢V¬ÊVÊD6Üñ∆BÜFV¬ì∞¢–†¢ñbÜó4Fó$∆ñ∂Ró∞¢ñbÇó5&VDˆÊ«îW66Ró∞¢ˆ&ñÊEv˜&∑76T÷˜fTG&˜F&vWBÜV¬∆óFV“ÁFÇì∞¢ˆ&ñÊEv˜&∑76T˜5W∆ˆDG&˜F&vWBÜV¬∆óFV“ÁFÇì∞¢–¢ÚÚ6ñÊv∆R÷6∆ñ6≤Fˆvv∆W2WáÊBˆ6ˆ∆∆6P¢V¬ÊˆÊ6∆ñ6≥÷7ñÊ2ÜRì”Á∞¢RÁ7F˜&˜vFñˆ‚Çì∞¢ñbÖ2ÂˆWáÊFVDFó'2ÊÜ2ÜóFV“ÁFÇíó∞¢2ÂˆWáÊFVDFó'2ÊFV∆WFRÜóFV“ÁFÇì∞¢ñbáGóVˆb˜6fTWáÊFVDFó'3””“vgVÊ7Fñˆ‚rï˜6fTWáÊFVDFó'2Çì∞¢&VÊFW$fñ∆UG&VRÇì∞¢÷V«6W∞¢2ÂˆWáÊFVDFó'2ÊFBÜóFV“ÁFÇì∞¢ñbáGóVˆb˜6fTWáÊFVDFó'3””“vgVÊ7Fñˆ‚rï˜6fTWáÊFVDFó'2Çì∞¢ÚÚfWF6Ç6Üñ∆G&V‚ñbÊ˜B66ÜV@¢ñbÇ2ÂˆFó$66ÜU∂óFV“ÁFÖ“ó∞¢G'ó∞¢6ˆÁ7BFF÷vóBíÖ˜v˜&∑76U&˜WFTf˜%FÇÜóFV“ÁFÇ¬v∆ó7Bríì∞¢2ÂˆFó$66ÜU∂óFV“ÁFÖ”÷FFÊVÁG&ñW7«≈µ”∞¢÷6F6ÇÜS"óµ2ÂˆFó$66ÜU∂óFV“ÁFÖ”’µ”∑–¢–¢&VÊFW$fñ∆UG&VRÇì∞¢–¢”∞¢÷V«6RñbÜó4WáFW&Êƒ∆ñÊ≤ó∞¢ÚÚFó7∆í÷ˆÊ«ì¢FÜR∆ñÊ≤ˆñÁG2˜WG6ñFRFÜRv˜&∑76R‚vRFÚ‰ıBFó66∆˜6P¢ÚÚFÜR&W6ˆ«fVB˜WG6ñFRFÇÇ3CSÉÜ&FVÊñÊríÊBFÚ‰ıB&V7W'6ófV«ê¢ÚÚWFÜ˜&ó¶RÊW7FVBW66R&˜w2VÊFW"‚«&VGí÷WFÜ˜&ó¶VBWáFW&Ê¬&ˆ˜B‡¢V¬ÊˆÊ6∆ñ6≥÷7ñÊ2ÜRì”Á∞¢RÁ7F˜&˜vFñˆ‚Çì∞¢ñbÜó4ÊW7FVDW66Ró∞¢vóB6Ü˜t6ˆÊfó&‘Fñ∆ˆrá∞¢FóF∆S¶óFV“ÊÊ÷R¿¢÷W76vSßBÇvWáFW&Ê≈ˆ∆ñÊµ˜&VEˆˆÊ«írí¿¢6ˆÊfó&‘∆&V√ßBÇvFñ∆ˆuˆ6ˆÊfó&’ˆ'F‚rí¿¢FÊvW#¶f«6R¿¢ÜñFT6Ê6V√ßG'VR¿¢fˆ7W46Ê6V√¶f«6R¿¢“ì∞¢&WGW&„∞¢–¢6ˆÁ7Bw&ÁB“vóBWFÜ˜&ó¶Uv˜&∑76TW66TÊfñvFñˆ‚ÜóFV“ì∞¢ñbÇw&ÁBí&WGW&„∞¢ñbÜw&ÁBÊó4Fó"ívóB∆ˆDFó"ÜóFV“ÁFÇì∞¢V«6RvóB˜V‰fñ∆RÜóFV“ÁFÇì∞¢”∞¢÷V«6W∞¢V¬ÊˆÊ6∆ñ6≥÷7ñÊ2Çì”Ê˜V‰fñ∆RÜóFV“ÁFÇì∞¢–†¢6ˆÁFñÊW"ÊVÊD6Üñ∆BÜV¬ì∞†¢ÚÚ&VÊFW"6Üñ∆G&V‚ñbFó&V7F˜'íó2WáÊFV@¢ñbÜó4Fó$∆ñ∂Rbe2ÂˆWáÊFVDFó'2ÊÜ2ÜóFV“ÁFÇíó∞¢6ˆÁ7B6Üñ∆G&V„’˜v˜&∑76TVÁG&ñW4f˜%&VÊFW"Ö2ÂˆFó$66ÜU∂óFV“ÁFÖ◊«≈µ“ì∞¢ñbÜ6Üñ∆G&V‚Ê∆VÊwFÇó∞¢˜&VÊFW%G&VTóFV◊2Ü6ˆÁFñÊW"¬6Üñ∆G&V‚¬FWFÇ≥ì∞¢÷V«6W∞¢6ˆÁ7BV◊Gì÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢V◊GíÊ6∆74Ê÷S“vfñ∆R÷óFV“fñ∆R÷V◊Gís∞¢V◊GíÁ7Gñ∆RÁFFñÊt∆VgC“ÉÇ≤ÜFWFÇ≥í£bí≤wÇs∞¢V◊GíÁFWáD6ˆÁFVÁC◊BÇvV◊GïˆFó"rì∞¢6ˆÁFñÊW"ÊVÊD6Üñ∆BÜV◊Gíì∞¢–¢–¢–ß–†¶7ñÊ2gVÊ7Fñˆ‚FV∆WFUv˜&∑76TFó"á&V≈FÇ¬Ê÷Ró∞¢ñbÇ2Á6W76ñˆ‚ó&WGW&„∞¢ñbáGóVˆb˜v˜&∑76UFÑó5&VDˆÊ«ì””“vgVÊ7Fñˆ‚rbe˜v˜&∑76UFÑó5&VDˆÊ«íá&V≈FÇíó∞¢6Ü˜uFˆ7BáBÇvWáFW&Ê≈ˆ∆ñÊµ˜&VEˆˆÊ«írí¬#ì∞¢&WGW&„∞¢–¢6ˆÁ7Bˆ≥÷vóB6Ü˜t6ˆÊfó&‘Fñ∆ˆrá∑FóF∆SßBÇvFV∆WFUˆFó%ˆ6ˆÊfó&“r∆Ê÷Rí∆÷W76vS¢rr∆6ˆÊfó&‘∆&V√¢tFV∆WFRr∆FÊvW#ßG'VR∆fˆ7W46Ê6V√ßG'VW“ì∞¢ñbÇˆ≤ó&WGW&„∞¢G'ó∞¢vóBíÇrˆíˆfñ∆RˆFV∆WFRr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉß&V≈FÇ«&V7W'6ófSßG'VW“ó“ì∞¢6Ü˜uFˆ7BáBÇvFV∆WFVBrí∂Ê÷Rì∞¢ÚÚ&V÷˜fRg&ˆ“WáÊFVBFó'266ÜP¢ñbÖ2ÂˆWáÊFVDFó'2óµ2ÂˆWáÊFVDFó'2ÊFV∆WFRá&V≈FÇì∂ñbáGóVˆb˜6fTWáÊFVDFó'3””“vgVÊ7Fñˆ‚rï˜6fTWáÊFVDFó'2Çì∑–¢FV∆WFR2ÂˆFó$66ÜU∑&V≈FÖ”∞¢vóB∆ˆDFó"Ö2Ê7W'&VÁDFó"ì∞¢÷6F6ÇÜRó∑6WE7FGW2áBÇvFV∆WFUˆfñ∆VBrí∂RÊ÷W76vRì∑–ß–†¶gVÊ7Fñˆ‚˜6Ü˜tfñ∆T6ˆÁFWáD÷VÁRÜR¬óFV“ó∞¢Fˆ7V÷VÁBÁVW'ï6V∆V7F˜$∆¬ÇrÊfñ∆R÷7GÇ÷÷VÁRríÊf˜$V6ÇÜV√”ÊV¬Á&V÷˜fRÇíì∞¢6ˆÁ7B÷VÁS÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢÷VÁRÊ6∆74Ê÷S“vfñ∆R÷7GÇ÷÷VÁRs∞¢÷VÁRÁ7Gñ∆RÊ775FWáC“w˜6óFñˆ„¶fóÜVC∂&6∂w&˜VÊCßf"Ç“◊7W&f6Rì∂&˜&FW#£Ç6ˆ∆ñBf"Ç“÷&˜&FW"ì∂&˜&FW"◊&FóW3£áÉ∑FFñÊs£gÇ∑¢÷ñÊFWÉ£ìììì∂÷ñ‚◊vñGFÉ£CÉ∂&˜Ç◊6ÜF˜s£GÇgÇ&v&É√√¬„3Rì≤s∞¢ÚÚ∂VW÷VÁRvóFÜñ‚fñWw˜'@¢6ˆÁ7Bgs◊vñÊF˜rÊñÊÊW%vñGFÇ«fÉ◊vñÊF˜rÊñÊÊW$ÜVñváC∞¢÷VÁRÁ7Gñ∆RÊ∆VgC“ÜRÊ6∆ñVÁEÇ≥CÁgsˆRÊ6∆ñVÁEÇ”S¶RÊ6∆ñVÁEÇí≤wÇs∞¢÷VÁRÁ7Gñ∆RÁF˜“ÜRÊ6∆ñVÁEí≥ÁfÉˆRÊ6∆ñVÁEí”¶RÊ6∆ñVÁEíí≤wÇs∞¢6ˆÁ7Bó4Fó$∆ñ∂S÷óFV“ÁGóS””“vFó"w«¬ÜóFV“ÁGóS””“w7ñ÷∆ñÊ≤rbfóFV“Êó5ˆFó"ì∞¢6ˆÁ7BF&vWDFó#÷ó4Fó$∆ñ∂RÚóFV“ÁFÇ¢˜v˜&∑76U&VÁDFó"ÜóFV“ÁFÇì∞¢6ˆÁ7Bó5&VDˆÊ«îW66S◊GóVˆb˜v˜&∑76TW66Tw&ÁDf˜%FÉ””“vgVÊ7Fñˆ‚rÚ˜v˜&∑76TW66Tw&ÁDf˜%FÇÜóFV“ÁFÇí¢f«6S∞†¢ñbÇó5&VDˆÊ«îW66Ró∞¢÷VÁRÊVÊD6Üñ∆BÖ˜v˜&∑76T6ˆÁFWáD÷VÁTóFV“áBÇvÊWuˆfñ∆Rrí∆7ñÊ2Çì”Á∞¢÷VÁRÁ&V÷˜fRÇì∞¢vóB&ˆ◊DÊWtfñ∆RáF&vWDFó"ì∞¢“íì∞†¢÷VÁRÊVÊD6Üñ∆BÖ˜v˜&∑76T6ˆÁFWáD÷VÁTóFV“áBÇvÊWuˆfˆ∆FW"rí∆7ñÊ2Çì”Á∞¢÷VÁRÁ&V÷˜fRÇì∞¢vóB&ˆ◊DÊWtfˆ∆FW"áF&vWDFó"ì∞¢“íì∞†¢6ˆÁ7B7&VFU6W÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvá"rì∞¢7&VFU6WÁ7Gñ∆RÊ775FWáC“v&˜&FW#¶ÊˆÊS∂&˜&FW"◊F˜£Ç6ˆ∆ñBf"Ç“÷&˜&FW"ì∂÷&vñ„£GÇ≤s∞¢÷VÁRÊVÊD6Üñ∆BÜ7&VFU6Wì∞†¢ÚÚ&VÊ÷P¢6ˆÁ7B&VÊ÷TóFV”÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢&VÊ÷TóFV“ÁFWáD6ˆÁFVÁC◊BÇw&VÊ÷U˜FóF∆Rrì∞¢&VÊ÷TóFV“Á7Gñ∆RÊ775FWáC“wFFñÊs£wÇGÉ∂7W'6˜#ßˆñÁFW#∂fˆÁB◊6ó¶S£7É∂6ˆ∆˜#ßf"Ç“◊FWáBì≤s∞¢&VÊ÷TóFV“ÊˆÊ÷˜W6VVÁFW#“Çì”Á&VÊ÷TóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“wf"Ç“÷Ü˜fW"÷&rís∞¢&VÊ÷TóFV“ÊˆÊ÷˜W6V∆VfS“Çì”Á&VÊ÷TóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“rs∞¢&VÊ÷TóFV“ÊˆÊ6∆ñ6≥“Çì”Á∂÷VÁRÁ&V÷˜fRÇìµˆñÊ∆ñÊU&VÊ÷Tfñ∆TóFV“ÜóFV“ì∑”∞¢÷VÁRÊVÊD6Üñ∆Bá&VÊ÷TóFV“ì∞†¢ÚÚ&WfV¬ñ‚fñ∆R÷ÊvW ¢6ˆÁ7B&WfVƒóFV”÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢&WfVƒóFV“ÁFWáD6ˆÁFVÁC◊BÇw&WfV≈ˆñÂˆfñÊFW"rì∞¢&WfVƒóFV“Á7Gñ∆RÊ775FWáC“wFFñÊs£wÇGÉ∂7W'6˜#ßˆñÁFW#∂fˆÁB◊6ó¶S£7É∂6ˆ∆˜#ßf"Ç“◊FWáBì≤s∞¢&WfVƒóFV“ÊˆÊ÷˜W6VVÁFW#“Çì”Á&WfVƒóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“wf"Ç“÷Ü˜fW"÷&rís∞¢&WfVƒóFV“ÊˆÊ÷˜W6V∆VfS“Çì”Á&WfVƒóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“rs∞¢&WfVƒóFV“ÊˆÊ6∆ñ6≥÷7ñÊ2Çì”Á∂÷VÁRÁ&V÷˜fRÇì∑G'ó∂vóBíÇrˆíˆfñ∆R˜&WfV¬r«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉ¶óFV“ÁFá“ó“ì∑÷6F6ÇÜW'"ó∑6Ü˜uFˆ7BáBÇw&WfV≈ˆfñ∆VBrí≤ÜW'"Ê÷W76vW«∆W'"íì∑◊”∞¢÷VÁRÊVÊD6Üñ∆Bá&WfVƒóFV“ì∞†¢ÚÚ˜V‚ñ‚e26ˆFRÇ3#s3Rê¢6ˆÁ7Bg66ˆFTóFV”÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢g66ˆFTóFV“ÁFWáD6ˆÁFVÁC◊BÇv˜VÂˆñÂ˜g66ˆFRrì∞¢g66ˆFTóFV“Á7Gñ∆RÊ775FWáC“wFFñÊs£wÇGÉ∂7W'6˜#ßˆñÁFW#∂fˆÁB◊6ó¶S£7É∂6ˆ∆˜#ßf"Ç“◊FWáBì≤s∞¢g66ˆFTóFV“ÊˆÊ÷˜W6VVÁFW#“Çì”Ág66ˆFTóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“wf"Ç“÷Ü˜fW"÷&rís∞¢g66ˆFTóFV“ÊˆÊ÷˜W6V∆VfS“Çì”Ág66ˆFTóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“rs∞¢g66ˆFTóFV“ÊˆÊ6∆ñ6≥÷7ñÊ2Çì”Á∂÷VÁRÁ&V÷˜fRÇì∑G'ó∂vóBíÇrˆíˆfñ∆Rˆ˜V‚◊g66ˆFRr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉ¶óFV“ÁFá“ó“ì∑÷6F6ÇÜW'"ó∑6Ü˜uFˆ7BáBÇv˜VÂˆñÂ˜g66ˆFUˆfñ∆VBrí≤ÜW'"Ê÷W76vW«∆W'"íì∑◊”∞¢÷VÁRÊVÊD6Üñ∆Bág66ˆFTóFV“ì∞†¢ÚÚ6˜ífñ∆RFÇ(	B&W6ˆ«fW2FÜR'6ˆ«WFRˆ‚÷Fó6≤FÇˆ‚FÜR6W'fW"á6ÚFÜP¢ÚÚW6W"vWG2FÜRgV∆¬ˆÜˆ÷RÚ‚‚‚˜v˜&∑76RˆfˆÚÁí&FÜW"FÜ‚FÜR&V∆FófP¢ÚÚFÇFÜRfñ∆RG&VR6Ü˜w2íÊBw&óFW2óBFÚFÜRı26∆ó&ˆ&B‚W6VgV¬f˜ ¢ÚÚ7FñÊrñÁFÚFW&÷ñÊ«2¬VFóF˜'2¬˜"˜FÜW"2vóFÜ˜WBF∂ñÊrFÜR6∆˜vW ¢ÚÚ&WfV¬÷ñ‚‘fñÊFW"&˜VÊBG&ó‡¢6ˆÁ7B6˜ïFÑóFV”÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢6˜ïFÑóFV“ÁFWáD6ˆÁFVÁC◊BÇv6˜ïˆfñ∆U˜FÇrì∞¢6˜ïFÑóFV“Á7Gñ∆RÊ775FWáC“wFFñÊs£wÇGÉ∂7W'6˜#ßˆñÁFW#∂fˆÁB◊6ó¶S£7É∂6ˆ∆˜#ßf"Ç“◊FWáBì≤s∞¢6˜ïFÑóFV“ÊˆÊ÷˜W6VVÁFW#“Çì”Ê6˜ïFÑóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“wf"Ç“÷Ü˜fW"÷&rís∞¢6˜ïFÑóFV“ÊˆÊ÷˜W6V∆VfS“Çì”Ê6˜ïFÑóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“rs∞¢6˜ïFÑóFV“ÊˆÊ6∆ñ6≥÷7ñÊ2Çì”Á∞¢÷VÁRÁ&V÷˜fRÇì∞¢G'ó∞¢6ˆÁ7B#÷vóBíÇrˆíˆfñ∆R˜FÇr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉ¶óFV“ÁFá“ó“ì∞¢6ˆÁ7B'3“á"bg"ÁFÇó«∆óFV“ÁFÉ∞¢G'ó∞¢vóBÊfñvF˜"Ê6∆ó&ˆ&BÁw&óFUFWáBÜ'2ì∞¢6Ü˜uFˆ7BáBÇwFÖˆ6˜ñVBríì∞¢÷6F6ÇÜ6∆óW'"ó∞¢6ˆÁ7BF÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇwFWáF&Vrì∞¢FÁf«VS÷'3∞¢FÁ7Gñ∆RÊ775FWáC“w˜6óFñˆ„¶fóÜVC∂∆VgC¢”ìììóÉ∑F˜¢”ìììóÉ≤s∞¢Fˆ7V÷VÁBÊ&ˆGíÊVÊD6Üñ∆BáFì∞¢FÁ6V∆V7BÇì∞¢∆WB6˜ñVC÷f«6S∞¢G'ó∂6˜ñVC÷Fˆ7V÷VÁBÊWÜV46ˆ÷÷ÊBÇv6˜írì∑÷6F6ÇÖÚó∑–¢FÁ&V÷˜fRÇì∞¢ñbÜ6˜ñVBí6Ü˜uFˆ7BáBÇwFÖˆ6˜ñVBríì∞¢V«6R6Ü˜uFˆ7BáBÇwFÖˆ6˜ïˆfñ∆VBrí≤Ü6∆óW'"bf6∆óW'"Ê÷W76vSˆ6∆óW'"Ê÷W76vS•7G&ñÊrÜ6∆óW'"ííì∞¢–¢÷6F6ÇÜW'"ó∞¢6Ü˜uFˆ7BáBÇwFÖˆ6˜ïˆfñ∆VBrí≤ÜW'"Ê÷W76vW«∆W'"íì∞¢–¢”∞¢÷VÁRÊVÊD6Üñ∆BÜ6˜ïFÑóFV“ì∞†¢6ˆÁ7B6˜ï&V≈FÑóFV”÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢6˜ï&V≈FÑóFV“ÁFWáD6ˆÁFVÁC◊BÇv6˜ï˜&V∆FófU˜FÇrì∞¢6˜ï&V≈FÑóFV“Á7Gñ∆RÊ775FWáC“wFFñÊs£wÇGÉ∂7W'6˜#ßˆñÁFW#∂fˆÁB◊6ó¶S£7É∂6ˆ∆˜#ßf"Ç“◊FWáBì≤s∞¢6˜ï&V≈FÑóFV“ÊˆÊ÷˜W6VVÁFW#“Çì”Ê6˜ï&V≈FÑóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“wf"Ç“÷Ü˜fW"÷&rís∞¢6˜ï&V≈FÑóFV“ÊˆÊ÷˜W6V∆VfS“Çì”Ê6˜ï&V≈FÑóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“rs∞¢6˜ï&V≈FÑóFV“ÊˆÊ6∆ñ6≥÷7ñÊ2Çì”Á∞¢÷VÁRÁ&V÷˜fRÇì∞¢G'ó∞¢6ˆÁ7B&V√’ˆÊ˜&÷∆ó¶Uv˜&∑76U&V≈FÇÜóFV“ÁFÇó«∆óFV“ÁFÉ∞¢vóBˆ6˜ïFWáEvóFÑf∆∆&6≤á&V¬«BÇwFÖˆ6˜ñVBrí«BÇwFÖˆ6˜ïˆfñ∆VBríì∞¢÷6F6ÇÜW'"ó∞¢6Ü˜uFˆ7BáBÇwFÖˆ6˜ïˆfñ∆VBrí≤ÜW'"Ê÷W76vW«∆W'"íì∞¢–¢”∞¢÷VÁRÊVÊD6Üñ∆BÜ6˜ï&V≈FÑóFV“ì∞¢–†¢ñbÜó4Fó$∆ñ∂Ró∞¢6ˆÁ7BFƒóFV”÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢FƒóFV“ÁFWáD6ˆÁFVÁC◊BÇvF˜vÊ∆ˆEˆfˆ∆FW"rì∞¢FƒóFV“Á7Gñ∆RÊ775FWáC“wFFñÊs£wÇGÉ∂7W'6˜#ßˆñÁFW#∂fˆÁB◊6ó¶S£7É∂6ˆ∆˜#ßf"Ç“◊FWáBì≤s∞¢FƒóFV“ÊˆÊ÷˜W6VVÁFW#“Çì”ÊFƒóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“wf"Ç“÷Ü˜fW"÷&rís∞¢FƒóFV“ÊˆÊ÷˜W6V∆VfS“Çì”ÊFƒóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“rs∞¢FƒóFV“ÊˆÊ6∆ñ6≥“Çì”Á∞¢÷VÁRÁ&V÷˜fRÇì∞¢6ˆÁ7B&V√“rˆíˆfˆ∆FW"ˆF˜vÊ∆ˆC˜6W76ñˆÂˆñC“r∂VÊ6ˆFUU$î6ˆ◊ˆÊVÁBÖ2Á6W76ñˆ‚Á6W76ñˆÂˆñBê¢≤rgFÉ“r∂VÊ6ˆFUU$î6ˆ◊ˆÊVÁBÜóFV“ÁFá«¬rrì∞¢vñÊF˜rÊ∆ˆ6Fñˆ‚Êá&Vc÷ÊWrU$¬á&V¬Á6∆ñ6RÉí¬Fˆ7V÷VÁBÊ&6UU$ó«∆∆ˆ6Fñˆ‚Êá&VbíÊá&Vc∞¢”∞¢÷VÁRÊVÊD6Üñ∆BÜFƒóFV“ì∞¢–†¢ñbÇó5&VDˆÊ«îW66Ró∞¢6ˆÁ7B6W÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvá"rì∞¢6WÁ7Gñ∆RÊ775FWáC“v&˜&FW#¶ÊˆÊS∂&˜&FW"◊F˜£Ç6ˆ∆ñBf"Ç“÷&˜&FW"ì∂÷&vñ„£GÇ≤s∞¢÷VÁRÊVÊD6Üñ∆Bá6Wì∞¢6ˆÁ7BFVƒóFV”÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∞¢FVƒóFV“ÁFWáD6ˆÁFVÁC◊BÇvFV∆WFU˜FóF∆Rrì∞¢FVƒóFV“Á7Gñ∆RÊ775FWáC“wFFñÊs£wÇGÉ∂7W'6˜#ßˆñÁFW#∂fˆÁB◊6ó¶S£7É∂6ˆ∆˜#ßf"Ç“÷W'&˜"¬6SìCScì≤s∞¢FVƒóFV“ÊˆÊ÷˜W6VVÁFW#“Çì”ÊFVƒóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“wf"Ç“÷Ü˜fW"÷&rís∞¢FVƒóFV“ÊˆÊ÷˜W6V∆VfS“Çì”ÊFVƒóFV“Á7Gñ∆RÊ&6∂w&˜VÊC“rs∞¢FVƒóFV“ÊˆÊ6∆ñ6≥“Çì”Á∂÷VÁRÁ&V÷˜fRÇì∂ñbÜó4Fó$∆ñ∂RñFV∆WFUv˜&∑76TFó"ÜóFV“ÁFÇ∆óFV“ÊÊ÷Rì∂V«6RFV∆WFUv˜&∑76Tfñ∆RÜóFV“ÁFÇ∆óFV“ÊÊ÷Rì∑”∞¢÷VÁRÊVÊD6Üñ∆BÜFVƒóFV“ì∞¢–†¢Fˆ7V÷VÁBÊ&ˆGíÊVÊD6Üñ∆BÜ÷VÁRì∞¢6ˆÁ7BFó6÷ó73“Çì”Á∂÷VÁRÁ&V÷˜fRÇì∂Fˆ7V÷VÁBÁ&V÷˜fTWfVÁD∆ó7FVÊW"Çv6∆ñ6≤r∆Fó6÷ó72ì∑”∞¢6WEFñ÷V˜WBÇÇì”ÊFˆ7V÷VÁBÊFDWfVÁD∆ó7FVÊW"Çv6∆ñ6≤r∆Fó6÷ó72í√ì∞ß–†¶7ñÊ2gVÊ7Fñˆ‚ˆñÊ∆ñÊU&VÊ÷Tfñ∆TóFV“ÜóFV“ó∞¢ñbÇ2Á6W76ñˆ‚ó&WGW&„∞¢ñbáGóVˆb˜v˜&∑76UFÑó5&VDˆÊ«ì””“vgVÊ7Fñˆ‚rbe˜v˜&∑76UFÑó5&VDˆÊ«íÜóFV“ÁFÇíó∞¢6Ü˜uFˆ7BáBÇvWáFW&Ê≈ˆ∆ñÊµ˜&VEˆˆÊ«írí¬#ì∞¢&WGW&„∞¢–¢6ˆÁ7Bó4Fó$∆ñ∂S÷óFV“ÁGóS””“vFó"w«¬ÜóFV“ÁGóS””“w7ñ÷∆ñÊ≤rbfóFV“Êó5ˆFó"ì∞¢ÚÚ&R÷fñ∆¬FÜRñÁWBvóFÇFÜR7W'&VÁBÊ÷RÊB6V∆V7BßW7BFÜR7FV–¢ÚÚÜWfW'óFÜñÊr&Vf˜&RFÜR∆7Br‚rí6ÚFÜRW6W"6‚ñ÷÷VFñFV«í&WGóRFÜP¢ÚÚ&6VÊ÷RvÜñ∆R&W6W'fñÊrFÜRWáFVÁ6ñˆ‚(	B÷F6ÜW2÷4ı2fñÊFW"‚f˜ ¢ÚÚFó&V7F˜&ñW2˜"Ê÷W2vóFÇÊÚr‚r¬FÜRÜV«W"6V∆V7G2FÜRgV∆¬f«VR‡¢ÚÚ6V∆V7E7FV÷«6ÚÜÊF∆W2F˜Ffñ∆W2ÇrÊvóFñvÊ˜&Rrí'ígV∆¬◊6V∆V7FñÊr‡¢6ˆÁ7BÊWtÊ÷S÷vóB6Ü˜u&ˆ◊DFñ∆ˆrá∞¢÷W76vSßBÇw&VÊ÷U˜&ˆ◊Brí¿¢f«VS¶óFV“ÊÊ÷R¿¢6ˆÊfó&‘∆&V√ßBÇw&VÊ÷U˜FóF∆Rrí¿¢6V∆V7E7FV”¢ó4Fó$∆ñ∂R¿¢6V∆V7D∆√¶ó4Fó$∆ñ∂P¢“ì∞¢ñbÇÊWtÊ÷W«∆ÊWtÊ÷S””÷óFV“ÊÊ÷Ró&WGW&„∞¢G'ó∞¢vóBíÇrˆíˆfñ∆R˜&VÊ÷Rr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉ¶óFV“ÁFÇ∆ÊWuˆÊ÷S¶ÊWtÊ÷W“ó“ì∞¢6Ü˜uFˆ7BáBÇw&VÊ÷VE˜FÚrí∂ÊWtÊ÷Rì∞¢ÚÚWFFRWáÊFVBFó'266ÜR∂Wíñb&VÊ÷ñÊrFó&V7F˜'ê¢ñbÜó4Fó$∆ñ∂Rbe2ÂˆWáÊFVDFó'2ó∞¢2ÂˆWáÊFVDFó'2ÊFV∆WFRÜóFV“ÁFÇì∞¢6ˆÁ7B&VÁC÷óFV“ÁFÇÊñÊ6«VFW2ÇrÚrìˆóFV“ÁFÇÁ7V'7G&ñÊrÉ∆óFV“ÁFÇÊ∆7DñÊFWÑˆbÇrÚríì¢r‚s∞¢6ˆÁ7BÊWuFÉ◊&VÁC””“r‚sˆÊWtÊ÷Sß&VÁB≤rÚr∂ÊWtÊ÷S∞¢2ÂˆWáÊFVDFó'2ÊFBÜÊWuFÇì∞¢ñbÖ2ÂˆFó$66ÜU∂óFV“ÁFÖ“óµ2ÂˆFó$66ÜU∂ÊWuFÖ”’2ÂˆFó$66ÜU∂óFV“ÁFÖ”∂FV∆WFR2ÂˆFó$66ÜU∂óFV“ÁFÖ”∑–¢ñbáGóVˆb˜6fTWáÊFVDFó'3””“vgVÊ7Fñˆ‚rï˜6fTWáÊFVDFó'2Çì∞¢–¢FV∆WFR2ÂˆFó$66ÜUµ2Ê7W'&VÁDFó%”∞¢vóB∆ˆDFó"Ö2Ê7W'&VÁDFó"ì∞¢÷6F6ÇÜW'"ó∑6Ü˜uFˆ7BáBÇw&VÊ÷Uˆfñ∆VBrí∂W'"Ê÷W76vRì∑–ß–†¶7ñÊ2gVÊ7Fñˆ‚FV∆WFUv˜&∑76Tfñ∆Rá&V≈FÇ¬Ê÷Ró∞¢ñbÇ2Á6W76ñˆ‚ó&WGW&„∞¢ñbáGóVˆb˜v˜&∑76UFÑó5&VDˆÊ«ì””“vgVÊ7Fñˆ‚rbe˜v˜&∑76UFÑó5&VDˆÊ«íá&V≈FÇíó∞¢6Ü˜uFˆ7BáBÇvWáFW&Ê≈ˆ∆ñÊµ˜&VEˆˆÊ«írí¬#ì∞¢&WGW&„∞¢–¢6ˆÁ7BˆFVƒfñ∆S÷vóB6Ü˜t6ˆÊfó&‘Fñ∆ˆrá∑FóF∆SßBÇvFV∆WFUˆ6ˆÊfó&“r∆Ê÷Rí∆÷W76vS¢rr∆6ˆÊfó&‘∆&V√¢tFV∆WFRr∆FÊvW#ßG'VR∆fˆ7W46Ê6V√ßG'VW“ì∞¢ñbÇˆFVƒfñ∆Rí&WGW&„∞¢G'ó∞¢vóBíÇrˆíˆfñ∆RˆFV∆WFRr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉß&V≈Fá“ó“ì∞¢6Ü˜uFˆ7BáBÇvFV∆WFVBrí∂Ê÷Rì∞¢ÚÚ6∆˜6R&WfñWrñbvRßW7BFV∆WFVBFÜRfñWvVBfñ∆P¢ñbÇBÇw&WfñWuFÖFWáBríÁFWáD6ˆÁFVÁC””◊&V≈FÇíBÇv'F‰6∆V%&WfñWrríÊˆÊ6∆ñ6≤Çì∞¢vóB∆ˆDFó"Ö2Ê7W'&VÁDFó"ì∞¢÷6F6ÇÜRó∑6WE7FGW2áBÇvFV∆WFUˆfñ∆VBrí∂RÊ÷W76vRì∑–ß–†¶7ñÊ2gVÊ7Fñˆ‚&ˆ◊DÊWtfñ∆RáF&vWDFó"“2Ê7W'&VÁDFó"«¬r‚ró∞¢ñbÇ2Á6W76ñˆ‚ó∞¢6ˆÁ7Bw3“áGóVˆb2Â˜&ˆfñ∆TFVfV«Ev˜&∑76S””“w7G&ñÊrrbe2Â˜&ˆfñ∆TFVfV«Ev˜&∑76Ró«¬rs∞¢ñbÇw2í&WGW&„∞¢G'ó∞¢ÚÚ7ó7FV“÷÷ñÁFVB6W76ñˆ‚Ç3c#"ì¢Wá∆ñ6óBv˜&∑G&VS¶f«6R(	B7&VFñÊr¢ÚÚfñ∆Rg&ˆ“&∆Ê≤vR◊W7BÊ˜BñÊÜW&óBFÜR6ˆÊfñrv˜&∑G&VRFVfV«B‡¢6ˆÁ7B#÷vóBíÇrˆí˜6W76ñˆ‚ˆÊWrr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑v˜&∑76Sßw2«v˜&∑G&VS¶f«6W“ó“ì∞¢ñbá"bg"Á6W76ñˆ‚óµ2Â˜VÊFñÊu6W76ñˆÂFˆˆ«6WG3÷ÁV∆√µ2Á6W76ñˆ„◊"Á6W76ñˆ„∂ñbáGóVˆbˆF˜E&VvVÊW&FñˆÂ&Wfó6ñˆ„””“vgVÊ7Fñˆ‚ríˆF˜E&VvVÊW&FñˆÂ&Wfó6ñˆ‚á"Á6W76ñˆ‚ìµ2Ê÷W76vW3’µ”∑7ñÊ5F˜&"Çì∑&VÊFW$÷W76vW2Çì∂vóB&VÊFW%6W76ñˆ‰∆ó7BÇì∑–¢÷6F6ÇÜRó∑6WE7FGW2áBÇv7&VFUˆfñ∆VBrí∂RÊ÷W76vRì∑&WGW&„∑–¢–¢ñbÇ2Á6W76ñˆ‚ó&WGW&„∞¢ñbáGóVˆb˜v˜&∑76UFÑó5&VDˆÊ«ì””“vgVÊ7Fñˆ‚rbe˜v˜&∑76UFÑó5&VDˆÊ«íáF&vWDFó"íó∞¢6Ü˜uFˆ7BáBÇvWáFW&Ê≈ˆ∆ñÊµ˜&VEˆˆÊ«írí¬#ì∞¢&WGW&„∞¢–¢6ˆÁ7BF&vWD∆&V√’˜v˜&∑76T7&VFUF&vWD∆&V¬áF&vWDFó"ì∞¢6ˆÁ7BÊ÷S÷vóB6Ü˜u&ˆ◊DFñ∆ˆrá∞¢FóF∆SßBÇvÊWuˆfñ∆U˜&ˆ◊E˜FóF∆Rr¬F&vWD∆&V¬í¿¢∆6VÜˆ∆FW#¢vfñ∆VÊ÷RÁGáBr¿¢6ˆÊfó&‘∆&V√ßBÇv7&VFRrê¢“ì∞¢ñbÇÊ÷W«¬Ê÷RÁG&ñ“Çíí&WGW&„∞¢6ˆÁ7B&V≈FÉ’˜v˜&∑76T¶ˆñÂF&vWEFÇáF&vWDFó"∆Ê÷Rì∞¢G'ó∞¢vóBíÇrˆíˆfñ∆Rˆ7&VFRr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉß&V≈FÇ∆6ˆÁFVÁC¢rw“ó“ì∞¢6Ü˜uFˆ7BáBÇv7&VFVBrí∂Ê÷RÁG&ñ“Çíì∞¢FV∆WFR2ÂˆFó$66ÜU∑F&vWDFó"«¬r‚u”∞¢vóB∆ˆDFó"Ö2Ê7W'&VÁDFó"ì∞¢˜V‰fñ∆Rá&V≈FÇì∞¢÷6F6ÇÜRó∑6WE7FGW2áBÇv7&VFUˆfñ∆VBrí∂RÊ÷W76vRì∑–ß–†¶7ñÊ2gVÊ7Fñˆ‚&ˆ◊DÊWtfˆ∆FW"áF&vWDFó"“2Ê7W'&VÁDFó"«¬r‚ró∞¢ñbÇ2Á6W76ñˆ‚ó∞¢6ˆÁ7Bw3“áGóVˆb2Â˜&ˆfñ∆TFVfV«Ev˜&∑76S””“w7G&ñÊrrbe2Â˜&ˆfñ∆TFVfV«Ev˜&∑76Ró«¬rs∞¢ñbÇw2í&WGW&„∞¢G'ó∞¢ÚÚ7ó7FV“÷÷ñÁFVB6W76ñˆ‚Ç3c#"ì¢Wá∆ñ6óBv˜&∑G&VS¶f«6R(	B7&VFñÊr¢ÚÚfˆ∆FW"g&ˆ“&∆Ê≤vR◊W7BÊ˜BñÊÜW&óBFÜR6ˆÊfñrv˜&∑G&VRFVfV«B‡¢6ˆÁ7B#÷vóBíÇrˆí˜6W76ñˆ‚ˆÊWrr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑v˜&∑76Sßw2«v˜&∑G&VS¶f«6W“ó“ì∞¢ñbá"bg"Á6W76ñˆ‚óµ2Â˜VÊFñÊu6W76ñˆÂFˆˆ«6WG3÷ÁV∆√µ2Á6W76ñˆ„◊"Á6W76ñˆ„∂ñbáGóVˆbˆF˜E&VvVÊW&FñˆÂ&Wfó6ñˆ„””“vgVÊ7Fñˆ‚ríˆF˜E&VvVÊW&FñˆÂ&Wfó6ñˆ‚á"Á6W76ñˆ‚ìµ2Ê÷W76vW3’µ”∑7ñÊ5F˜&"Çì∑&VÊFW$÷W76vW2Çì∂vóB&VÊFW%6W76ñˆ‰∆ó7BÇì∑–¢÷6F6ÇÜRó∑6WE7FGW2áBÇvfˆ∆FW%ˆ7&VFUˆfñ∆VBrí∂RÊ÷W76vRì∑&WGW&„∑–¢–¢ñbÇ2Á6W76ñˆ‚ó&WGW&„∞¢ñbáGóVˆb˜v˜&∑76UFÑó5&VDˆÊ«ì””“vgVÊ7Fñˆ‚rbe˜v˜&∑76UFÑó5&VDˆÊ«íáF&vWDFó"íó∞¢6Ü˜uFˆ7BáBÇvWáFW&Ê≈ˆ∆ñÊµ˜&VEˆˆÊ«írí¬#ì∞¢&WGW&„∞¢–¢6ˆÁ7BF&vWD∆&V√’˜v˜&∑76T7&VFUF&vWD∆&V¬áF&vWDFó"ì∞¢6ˆÁ7BÊ÷S÷vóB6Ü˜u&ˆ◊DFñ∆ˆrá∞¢FóF∆SßBÇvÊWuˆfˆ∆FW%˜&ˆ◊E˜FóF∆Rr¬F&vWD∆&V¬í¿¢∆6VÜˆ∆FW#¢vfˆ∆FW"÷Ê÷Rr¿¢6ˆÊfó&‘∆&V√ßBÇv7&VFRrê¢“ì∞¢ñbÇÊ÷W«¬Ê÷RÁG&ñ“Çíí&WGW&„∞¢6ˆÁ7B&V≈FÉ’˜v˜&∑76T¶ˆñÂF&vWEFÇáF&vWDFó"∆Ê÷Rì∞¢G'ó∞¢vóBíÇrˆíˆfñ∆Rˆ7&VFR÷Fó"r«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑6W76ñˆÂˆñC•2Á6W76ñˆ‚Á6W76ñˆÂˆñB«FÉß&V≈Fá“ó“ì∞¢6Ü˜uFˆ7BáBÇvfˆ∆FW%ˆ7&VFVBrí∂Ê÷RÁG&ñ“Çíì∞¢FV∆WFR2ÂˆFó$66ÜU∑F&vWDFó"«¬r‚u”∞¢vóB∆ˆDFó"Ö2Ê7W'&VÁDFó"ì∞¢6ˆÁ7B'5FÉ’2Á6W76ñˆ‚Áv˜&∑76SÚáF&vWDFó#””“r‚sˆGµ2Á6W76ñˆ‚Áv˜&∑76W“ÚG∂Ê÷RÁG&ñ“Çó÷¶Gµ2Á6W76ñˆ‚Áv˜&∑76W“ÚG∑F&vWDFó'“ÚG∂Ê÷RÁG&ñ“Çó÷ì¶ÁV∆√∞¢ñbÜ'5FÇó∞¢6ˆÁ7BFD576S÷vóB6Ü˜t6ˆÊfó&‘Fñ∆ˆrá∞¢FóF∆SßBÇvfˆ∆FW%ˆFEˆ5˜76U˜FóF∆Rrí¿¢÷W76vSßBÇvfˆ∆FW%ˆFEˆ5˜76Uˆ◊6rrí¿¢6ˆÊfó&‘∆&V√ßBÇvfˆ∆FW%ˆFEˆ5˜76Uˆ'F‚rí¿¢6Ê6Vƒ∆&V√ßBÇw7FGW5ˆÊÚrí¿¢fˆ7W46Ê6V√ßG'VP¢“ì∞¢ñbÜFD576Ró∞¢G'ó∞¢6ˆÁ7BFF÷vóBíÇrˆí˜v˜&∑76W2ˆFBr«∂÷WFÜˆC¢uı5Br∆&ˆGì§•4Ù‚Á7G&ñÊvñgíá∑FÉ¶'5Fá“ó“ì∞¢ñbáGóVˆb˜v˜&∑76T∆ó7B”“wVÊFVfñÊVBrï˜v˜&∑76T∆ó7C÷FFÁv˜&∑76W7«≈˜v˜&∑76T∆ó7G«≈µ”∞¢ñbáGóVˆb&VÊFW%v˜&∑76W5ÊV√””“vgVÊ7Fñˆ‚ró&VÊFW%v˜&∑76W5ÊV¬Ö˜v˜&∑76T∆ó7Bì∞¢6Ü˜uFˆ7BáBÇwv˜&∑76UˆFFVBríì∞¢÷6F6ÇÜS"ó∑6WE7FGW2ÇáBÇvW'&˜%˜&VfóÇró«¬tW'&˜#¢rí∂S"Ê÷W76vRì∑–¢–¢–¢÷6F6ÇÜRó∑6WE7FGW2áBÇvfˆ∆FW%ˆ7&VFUˆfñ∆VBrí∂RÊ÷W76vRì∑–ß–†¶gVÊ7Fñˆ‚&VÊFW%G&íÇó≤ÚÚÊˆ‚÷÷VFñfñ∆W2W6RW&6∆ó6Üó ¢6ˆÁ7BG&ì“BÇvGF6ÖG&írì∑G&íÊñÊÊW$ÖD‘√“rs∞¢ñbÇ2ÁVÊFñÊtfñ∆W2Ê∆VÊwFÇó∑G&íÊ6∆74∆ó7BÁ&V÷˜fRÇvÜ2÷fñ∆W2rì∑WFFU6VÊD'F‚Çì∑&WGW&„∑–¢G&íÊ6∆74∆ó7BÊFBÇvÜ2÷fñ∆W2rì∞¢WFFU6VÊD'F‚Çì∞¢2ÁVÊFñÊtfñ∆W2Êf˜$V6ÇÇÜb∆íì”Á∞¢6ˆÁ7B6Üó÷Fˆ7V÷VÁBÊ7&VFTV∆V÷VÁBÇvFóbrì∂6ÜóÊ6∆74Ê÷S“vGF6Ç÷6Üós∞¢6ˆÁ7B÷VFñ∂ñÊC’ˆ÷VFñ∂ñÊDf˜$Ê÷RÜbÊÊ÷Rì∞¢ñbÖÙî‘tUÙUÖE2ÁFW7BÜbÊÊ÷Ró«∆÷VFñ∂ñÊC””“vVFñÚw«∆÷VFñ∂ñÊC””“wfñFVÚró∞¢6ˆÁ7B&∆ˆ%W&√’U$¬Ê7&VFTˆ&¶V7EU$¬Übì∞¢6ÜóÊ6∆74Ê÷S“vGF6Ç÷6ÜóGF6Ç÷6Üó“÷÷VFñGF6Ç÷6Üó““r∂÷VFñ∂ñÊC≤ÚÚGF6Ç÷6Üó“÷VFñÚGF6Ç÷6Üó“◊fñFV¢6ÜóÊFF6WBÊ&∆ˆ%W&√÷&∆ˆ%W&√∞¢ñbÜ÷VFñ∂ñÊC””“vñ÷vRró∞¢6ÜóÊñÊÊW$ÖD‘√÷∆ñ÷r6∆73“&GF6Ç◊FáV÷""7&3“"G∂W62Ü&∆ˆ%W&¬ó“"«C“"G∂W62ÜbÊÊ÷Ró“"FóF∆S“"G∂W62ÜbÊÊ÷Ró“#„∆'WGFˆ‚FóF∆S“"G∑BÇw&V÷˜fU˜FóF∆Rró“#‚G∂∆íÇwÇr√"ó”¬ˆ'WGFˆ„Ê∞¢“V«6RñbÖı5duÙUÖE2ÁFW7BÜbÊÊ÷Ríó∞¢6ÜóÊñÊÊW$ÖD‘√÷∆ñ÷r6∆73“&GF6Ç◊FáV÷"GF6Ç◊FáV÷"“◊7fr"7&3“"G∂W62Ü&∆ˆ%W&¬ó“"«C“"G∂W62ÜbÊÊ÷Ró“"FóF∆S“"G∂W62ÜbÊÊ÷Ró“#„∆'WGFˆ‚FóF∆S“"G∑BÇw&V÷˜fU˜FóF∆Rró“#‚G∂∆íÇwÇr√"ó”¬ˆ'WGFˆ„Ê∞¢“V«6RñbÜ÷VFñ∂ñÊC””“vVFñÚró∞¢6ÜóÊñÊÊW$ÖD‘√÷«7‚6∆73“&GF6Ç÷6Üó÷÷VFñ#Ô	¯ÎRG∂W62ÜbÊÊ÷Ró”¬˜7„„∆VFñÚ6ˆÁG&ˆ«2&V∆ˆC“&÷WFFF"7&3“"G∂W62Ü&∆ˆ%W&¬ó“#„¬ˆVFñÛ„∆'WGFˆ‚FóF∆S“"G∑BÇw&V÷˜fU˜FóF∆Rró“#‚G∂∆íÇwÇr√"ó”¬ˆ'WGFˆ„Ê∞¢“V«6RñbÜ÷VFñ∂ñÊC””“wfñFVÚró∞¢6ÜóÊñÊÊW$ÖD‘√÷«7‚6∆73“&GF6Ç÷6Üó÷÷VFñ#Ô	¯Í¬G∂W62ÜbÊÊ÷Ró”¬˜7„„«fñFVÚ6ˆÁG&ˆ«2&V∆ˆC“&÷WFFF"7&3“"G∂W62Ü&∆ˆ%W&¬ó“#„¬˜fñFVÛ„∆'WGFˆ‚FóF∆S“"G∑BÇw&V÷˜fU˜FóF∆Rró“#‚G∂∆íÇwÇr√"ó”¬ˆ'WGFˆ„Ê∞¢–¢“V«6R∞¢6ÜóÊñÊÊW$ÖD‘√÷G∂∆íÇwW&6∆ór√"ó“G∂W62ÜbÊÊ÷Ró“∆'WGFˆ‚FóF∆S“"G∑BÇw&V÷˜fU˜FóF∆Rró“#‚G∂∆íÇwÇr√"ó”¬ˆ'WGFˆ„Ê∞¢–¢6ÜóÁVW'ï6V∆V7F˜"Çv'WGFˆ‚ríÊˆÊ6∆ñ6≥“Çì”Á∞¢ÚÚ&Wfˆ∂R&∆ˆ"U$¬FÚfˆñB÷V÷˜'í∆V≤&Vf˜&R&V÷˜fñÊp¢ñbÜ6ÜóÊFF6WBÊ&∆ˆ%W&¬íU$¬Á&Wfˆ∂Tˆ&¶V7EU$¬Ü6ÜóÊFF6WBÊ&∆ˆ%W&¬ì∞¢2ÁVÊFñÊtfñ∆W2Á7∆ñ6RÜí√ì∑&VÊFW%G&íÇì∞¢”∞¢G&íÊVÊD6Üñ∆BÜ6Üóì∞¢“ì∞ß–¶gVÊ7Fñˆ‚˜W∆ˆEFˆÙ∆&vT÷W76vRÜfñ∆Ró∞¢6ˆÁ7Bfñ∆U6ó¶T÷#‘÷FÇÊ6Vñ¬ÇÇÜfñ∆Rbffñ∆RÁ6ó¶Ró«√íÛ#BÛ#Bì∞¢&WGW&‚BÇwW∆ˆE˜Fˆıˆ∆&vRrƒ‘ÖıUƒÙEÙ‘"∆fñ∆U6ó¶T÷"ì∞ß–¶gVÊ7Fñˆ‚˜6Ü˜uW∆ˆEFˆÙ∆&vRÜfñ∆Ró∞¢6ˆÁ7B÷W76vS÷G∑BÇwW∆ˆEˆfñ∆VBró“G∂fñ∆Rbffñ∆RÊÊ÷Sˆfñ∆RÊÊ÷S¢vfñ∆Rw“«S#BGµ˜W∆ˆEFˆÙ∆&vT÷W76vRÜfñ∆Ró÷∞¢ñbáGóVˆb6WE7FGW3””“vgVÊ7Fñˆ‚ró6WE7FGW2Ü«S#sF2G∂÷W76vW÷ì∞¢V«6RñbáGóVˆb6Ü˜uFˆ7C””“vgVÊ7Fñˆ‚ró6Ü˜uFˆ7BÜ÷W76vR√S¬vW'&˜"rì∞ß–¶gVÊ7Fñˆ‚FDfñ∆W2Üfñ∆W2ó∞¢f˜"Ü6ˆÁ7Bbˆbfñ∆W2ó∞¢ñbÜbbfbÁ6ó¶S‰‘ÖıUƒÙEÙ%ïDU2óµ˜6Ü˜uW∆ˆEFˆÙ∆&vRÜbì∂6ˆÁFñÁVS∑–¢ñbÇ2ÁVÊFñÊtfñ∆W2ÊfñÊBá”ÁÊÊ÷S””÷bÊÊ÷Ríï2ÁVÊFñÊtfñ∆W2ÁW6ÇÜbì∞¢–¢&VÊFW%G&íÇì∞ß–¶6ˆÁ7B˜W∆ˆEVÊFñÊtfñ∆W5&ˆw&W74'ï6W76ñˆ„÷ÊWr÷Çì∞¶gVÊ7Fñˆ‚˜W∆ˆEVÊFñÊtfñ∆W47W'&VÁE6W76ñˆ‚á6W76ñˆ‰ñBó∞¢&WGW&‚Ç6W76ñˆ‰ñG«¬Ö2Á6W76ñˆ‚be2Á6W76ñˆ‚Á6W76ñˆÂˆñC””◊6W76ñˆ‰ñBíì∞ß–¶gVÊ7Fñˆ‚˜W∆ˆEVÊFñÊtfñ∆W4ÜñFU&ˆw&W74&"Çó∞¢6ˆÁ7B&#“BÇwW∆ˆD&"rì∂6ˆÁ7B&%w&“BÇwW∆ˆD&%w&rì∞¢ñbÇ&'«¬&%w&ó&WGW&„∞¢&%w&Ê6∆74∆ó7BÁ&V÷˜fRÇv7FófRrì∞¢&"Á7Gñ∆RÁvñGFÉ“sRs∞¢ñbÜ&%w&ÊFF6WBñFV∆WFR&%w&ÊFF6WBÁW∆ˆE6W76ñˆ‰ñC∞ß–¶gVÊ7Fñˆ‚˜W∆ˆEVÊFñÊtfñ∆W56Ü˜u&ˆw&W74&"Ü˜vÊW"«W&6VÁBó∞¢6ˆÁ7B&#“BÇwW∆ˆD&"rì∂6ˆÁ7B&%w&“BÇwW∆ˆD&%w&rì∞¢ñbÇ&'«¬&%w&ó&WGW&„∞¢ñbÜ&%w&ÊFF6WBñ&%w&ÊFF6WBÁW∆ˆE6W76ñˆ‰ñC÷˜vÊW#∞¢&%w&Ê6∆74∆ó7BÊFBÇv7FófRrì∞¢&"Á7Gñ∆RÁvñGFÉ÷G¥÷FÇÊ÷ÇÉƒ÷FÇÊ÷ñ‚ÉƒÁV÷&W"áW&6VÁBó«√íó“V∞ß–¶gVÊ7Fñˆ‚˜W∆ˆEVÊFñÊtfñ∆W57ñÊ5&ˆw&W74f˜%6W76ñˆ‚á6W76ñˆ‰ñBó∞¢6ˆÁ7B˜vÊW#’7G&ñÊrá6W76ñˆ‰ñG«¬rrì∞¢6ˆÁ7B7FFS÷˜vÊW#ı˜W∆ˆEVÊFñÊtfñ∆W5&ˆw&W74'ï6W76ñˆ‚ÊvWBÜ˜vÊW"ì¶ÁV∆√∞¢ñbá7FFRóµ˜W∆ˆEVÊFñÊtfñ∆W56Ü˜u&ˆw&W74&"Ü˜vÊW"«7FFRÁW&6VÁBì∑&WGW&„∑–¢˜W∆ˆEVÊFñÊtfñ∆W4ÜñFU&ˆw&W74&"Çì∞ß–¶gVÊ7Fñˆ‚˜W∆ˆEVÊFñÊtfñ∆W5WFFU&ˆw&W72á6W76ñˆ‰ñB«W&6VÁBó∞¢6ˆÁ7B&#“BÇwW∆ˆD&"rì∂6ˆÁ7B&%w&“BÇwW∆ˆD&%w&rì∞¢ñbÇ&'«¬&%w&ó&WGW&„∞¢6ˆÁ7B˜vÊW#’7G&ñÊrá6W76ñˆ‰ñG«¬rrì∞¢6ˆÁ7B7FófTf˜$˜vÊW#÷&%w&ÊFF6WBbf&%w&ÊFF6WBÁW∆ˆE6W76ñˆ‰ñC””÷˜vÊW#∞¢ñbáW&6VÁC””÷ÁV∆¬ó∞¢ñbÜ˜vÊW"ï˜W∆ˆEVÊFñÊtfñ∆W5&ˆw&W74'ï6W76ñˆ‚ÊFV∆WFRÜ˜vÊW"ì∞¢ñbÜ7FófTf˜$˜vÊW"ó∞¢˜W∆ˆEVÊFñÊtfñ∆W4ÜñFU&ˆw&W74&"Çì∞¢–¢&WGW&„∞¢–¢6ˆÁ7B6∆◊VC‘÷FÇÊ÷ÇÉƒ÷FÇÊ÷ñ‚ÉƒÁV÷&W"áW&6VÁBó«√íì∞¢ñbÜ˜vÊW"ï˜W∆ˆEVÊFñÊtfñ∆W5&ˆw&W74'ï6W76ñˆ‚Á6WBÜ˜vÊW"«∑W&6VÁC¶6∆◊VG“ì∞¢ñbÇ˜W∆ˆEVÊFñÊtfñ∆W47W'&VÁE6W76ñˆ‚á6W76ñˆ‰ñBíó∞¢ñbÜ7FófTf˜$˜vÊW"ï˜W∆ˆEVÊFñÊtfñ∆W4ÜñFU&ˆw&W74&"Çì∞¢&WGW&„∞¢–¢˜W∆ˆEVÊFñÊtfñ∆W56Ü˜u&ˆw&W74&"Ü˜vÊW"∆6∆◊VBì∞ß–¶7ñÊ2gVÊ7Fñˆ‚W∆ˆEVÊFñÊtfñ∆W2Ü˜FñˆÁ3◊∑“ó∞¢6ˆÁ7B˜G3÷˜FñˆÁ7««∑”∞¢6ˆÁ7BVÊFñÊtfñ∆W3‘'&íÊó4'&íÜ˜G2Êfñ∆W2ìˆ˜G2Êfñ∆W2Êfñ«FW"Ñ&ˆˆ∆V‚ì•≤‚‚‚Ö2ÁVÊFñÊtfñ∆W7«≈µ“ï”∞¢6ˆÁ7B6W76ñˆ‰ñC’7G&ñÊrÜ˜G2Á6W76ñˆ‰ñG«¬Ö2Á6W76ñˆ‚be2Á6W76ñˆ‚Á6W76ñˆÂˆñBó«¬rrì∞¢ñbÇVÊFñÊtfñ∆W2Ê∆VÊwFá«¬6W76ñˆ‰ñBó&WGW&Âµ”∞¢6ˆÁ7B6∆V%VÊFñÊs“Ü˜G2bf˜G2Ê6∆V%VÊFñÊs””÷f«6Rì∞¢6ˆÁ7BÊ÷W3’µ”∂∆WBfñ«W&W3”∞¢˜W∆ˆEVÊFñÊtfñ∆W5WFFU&ˆw&W72á6W76ñˆ‰ñB√ì∞¢6ˆÁ7BF˜F√◊VÊFñÊtfñ∆W2Ê∆VÊwFÉ∞¢f˜"Ü∆WBì”∂ì«F˜F√∂í≤≤ó∞¢6ˆÁ7Bc◊VÊFñÊtfñ∆W5∂ï”∞¢G'ó∞¢ñbÜbbfbÁ6ó¶S‰‘ÖıUƒÙEÙ%ïDU2óFá&˜rÊWrW'&˜"Ö˜W∆ˆEFˆÙ∆&vT÷W76vRÜbíì∞¢6ˆÁ7BfC÷ÊWrf˜&‘FFÇì∞¢fBÊVÊBÇw6W76ñˆÂˆñBr«6W76ñˆ‰ñBì∂fBÊVÊBÇvfñ∆Rr∆b∆bÊÊ÷Rì∞¢6ˆÁ7Bó4&6ÜófS’Ù$4ÑïdUÙUÖE2ÁFW7BÜbÊÊ÷Rì∞¢6ˆÁ7BW&√÷ÊWrU$¬Üó4&6ÜófSÚví˜W∆ˆBˆWáG&7Bs¢ví˜W∆ˆBr∆Fˆ7V÷VÁBÊ&6UU$ó«∆∆ˆ6Fñˆ‚Êá&VbíÊá&Vc∞¢6ˆÁ7B&W3÷vóBfWF6ÇáW&¬«∂÷WFÜˆC¢uı5Br∆7&VFVÁFñ«3¢vñÊ6«VFRr∆&ˆGì¶fG“ì∞¢ñbÖ˜&VFó&V7DñeVÊWFÇá&W2íí&WGW&„∞¢ñbÇ&W2Êˆ≤ó∂6ˆÁ7BW'#÷vóB&W2ÁFWáBÇì∑Fá&˜rÊWrW'&˜"ÜW'"ì∑–¢6ˆÁ7BFF÷vóB&W2Êß6ˆ‚Çì∞¢ñbÜFFÊW'&˜"óFá&˜rÊWrW'&˜"ÜFFÊW'&˜"ì∞¢ñbÜó4&6ÜófRó∞¢Ê÷W2ÁW6Çá∂Ê÷S¢FFÊFW7B¬FÉ¢FFÊFW7B¬WáG&7FVC¢FFÊWáG&7FVG“ì∞¢ñbáGóVˆb∆ˆDFó#””“vgVÊ7Fñˆ‚rbe˜W∆ˆEVÊFñÊtfñ∆W47W'&VÁE6W76ñˆ‚á6W76ñˆ‰ñBíñ∆ˆDFó"Ö2Ê7W'&VÁDFó'«¬r‚rì∞¢÷V«6W∞¢Ê÷W2ÁW6Çá∂Ê÷S¢FFÊfñ∆VÊ÷R¬FÉ¢FFÁFÇ¬÷ñ÷S¢FFÊ÷ñ÷R¬6ó¶S¢FFÁ6ó¶R¬ó5ˆñ÷vS¢FFÊó5ˆñ÷vW“ì∞¢–¢÷6F6ÇÜRó∂fñ«W&W2≤≥∑6WE7FGW2Ü«S#sF2G∑BÇwW∆ˆEˆfñ∆VBró“G∂bÊÊ÷W“«S#BG∂RÊ÷W76vW÷ì∑–¢˜W∆ˆEVÊFñÊtfñ∆W5WFFU&ˆw&W72á6W76ñˆ‰ñBƒ÷FÇÁ&˜VÊBÇÜí≥í˜F˜F¬£íì∞¢–¢˜W∆ˆEVÊFñÊtfñ∆W5WFFU&ˆw&W72á6W76ñˆ‰ñB∆ÁV∆¬ì∞¢ñbÜ6∆V%VÊFñÊrbe˜W∆ˆEVÊFñÊtfñ∆W47W'&VÁE6W76ñˆ‚á6W76ñˆ‰ñBíóµ2ÁVÊFñÊtfñ∆W3’µ”∑&VÊFW%G&íÇì∑–¢V«6RñbáGóVˆb&VÊFW%G&ì””“vgVÊ7Fñˆ‚rbe˜W∆ˆEVÊFñÊtfñ∆W47W'&VÁE6W76ñˆ‚á6W76ñˆ‰ñBíó&VÊFW%G&íÇì∞¢ñbÜfñ«W&W3””◊F˜F¬bgF˜F√„óFá&˜rÊWrW'&˜"áBÇv∆≈˜W∆ˆG5ˆfñ∆VBr«F˜F¬íì∞¢ÚÚ6Ü˜rWáG&7Fñˆ‚7V÷÷'ê¢6ˆÁ7BWáG&7FVC÷Ê÷W2Êfñ«FW"Ü„”Ê‚ÊWáG&7FVBì∞¢ñbÜWáG&7FVBÊ∆VÊwFÇó6Ü˜uFˆ7BáBÇv&6ÜófUˆWáG&7FVBr∆WáG&7FVBÁ&VGV6RÇá2∆‚ì”Á2∂‚ÊWáG&7FVB√í∆WáG&7FVBÊ∆VÊwFÇíì∞¢&WGW&‚Ê÷W3∞ß–