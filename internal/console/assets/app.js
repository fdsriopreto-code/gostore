"use strict";
/* gostore console — dependency-free SPA. Every request is AWS SigV4 signed in
   the browser with Web Crypto, same-origin with the S3 + admin API. */

const REGION = "us-east-1";
const te = new TextEncoder();

// ---------- crypto / SigV4 ---------------------------------------------------

async function hmac(key, msg) {
  const k = await crypto.subtle.importKey("raw", key, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await crypto.subtle.sign("HMAC", k, typeof msg === "string" ? te.encode(msg) : msg));
}
const hexstr = (b) => [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, "0")).join("");
const sha256hex = async (s) => hexstr(await crypto.subtle.digest("SHA-256", te.encode(s)));
async function signingKey(secret, ds) {
  let k = te.encode("AWS4" + secret);
  for (const p of [ds, REGION, "s3", "aws4_request"]) k = await hmac(k, p);
  return k;
}
const encComp = (s) =>
  encodeURIComponent(s).replace(/[!*'()]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());
const encPath = (p) => p.split("/").map(encComp).join("/");
const canonQuery = (q) =>
  Object.keys(q).sort().map((k) => encComp(k) + "=" + encComp(q[k] ?? "")).join("&");

const session = {
  get ak() { return sessionStorage.getItem("gs_ak") || ""; },
  get sk() { return sessionStorage.getItem("gs_sk") || ""; },
  set(ak, sk) { sessionStorage.setItem("gs_ak", ak); sessionStorage.setItem("gs_sk", sk); },
  clear() { sessionStorage.clear(); },
};

async function sign(method, path, { query = {}, contentType, extraHeaders = {} } = {}) {
  const amzDate = new Date().toISOString().replace(/[:-]|\.\d{3}/g, "");
  const ds = amzDate.slice(0, 8);
  const host = location.host;
  const payloadHash = "UNSIGNED-PAYLOAD";
  const hdr = { "x-amz-date": amzDate, "x-amz-content-sha256": payloadHash, ...extraHeaders };
  if (contentType) hdr["content-type"] = contentType;
  const signed = [...Object.keys(hdr).map((h) => h.toLowerCase()), "host"].sort();
  const canonHeaders = signed
    .map((h) => h + ":" + (h === "host" ? host : String(hdr[h]).trim()) + "\n").join("");
  const cr = [method, encPath(path), canonQuery(query), canonHeaders, signed.join(";"), payloadHash].join("\n");
  const scope = `${ds}/${REGION}/s3/aws4_request`;
  const sts = ["AWS4-HMAC-SHA256", amzDate, scope, await sha256hex(cr)].join("\n");
  const sig = hexstr(await hmac(await signingKey(session.sk, ds), sts));
  hdr["Authorization"] =
    `AWS4-HMAC-SHA256 Credential=${session.ak}/${scope}, SignedHeaders=${signed.join(";")}, Signature=${sig}`;
  const qs = canonQuery(query);
  return { url: location.origin + encPath(path) + (qs ? "?" + qs : ""), headers: hdr };
}

async function api(method, path, opts = {}) {
  const { url, headers } = await sign(method, path, opts);
  return fetch(url, { method, headers, body: opts.body });
}

// signed XHR — used for uploads so we get progress events
async function upload(path, file, onProgress) {
  const { url, headers } = await sign("PUT", "/" + path, {
    contentType: file.type || "application/octet-stream",
  });
  return new Promise((resolve, reject) => {
    const x = new XMLHttpRequest();
    x.open("PUT", url);
    for (const [k, v] of Object.entries(headers)) x.setRequestHeader(k, v);
    x.upload.onprogress = (e) => e.lengthComputable && onProgress(e.loaded / e.total);
    x.onload = () => (x.status < 300 ? resolve() : reject(new Error(extractErr(x.responseText, x.status))));
    x.onerror = () => reject(new Error("network error"));
    x.send(file);
  });
}

// presigned GET URL for sharing
async function presignGet(path, expires = 3600) {
  const amzDate = new Date().toISOString().replace(/[:-]|\.\d{3}/g, "");
  const ds = amzDate.slice(0, 8);
  const scope = `${ds}/${REGION}/s3/aws4_request`;
  const q = {
    "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
    "X-Amz-Credential": `${session.ak}/${scope}`,
    "X-Amz-Date": amzDate,
    "X-Amz-Expires": String(expires),
    "X-Amz-SignedHeaders": "host",
  };
  const canonHeaders = "host:" + location.host + "\n";
  const cr = ["GET", encPath("/" + path), canonQuery(q), canonHeaders, "host", "UNSIGNED-PAYLOAD"].join("\n");
  const sts = ["AWS4-HMAC-SHA256", amzDate, scope, await sha256hex(cr)].join("\n");
  q["X-Amz-Signature"] = hexstr(await hmac(await signingKey(session.sk, ds), sts));
  return location.origin + encPath("/" + path) + "?" + canonQuery(q);
}

// ---------- dom helpers --------------------------------------------------

const $ = (s, r = document) => r.querySelector(s);
const el = (tag, props = {}, ...kids) => {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (v == null) continue;
    if (k === "class") n.className = v;
    else if (k === "html") n.innerHTML = v;
    else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
    else n.setAttribute(k, v);
  }
  for (const c of kids.flat()) if (c != null) n.append(c.nodeType ? c : document.createTextNode(c));
  return n;
};
const ic = (d, w = 2) =>
  el("span", { class: "iconwrap", html:
    `<svg class="i" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="${w}" stroke-linecap="round" stroke-linejoin="round">${d}</svg>` }).firstChild;
const ICON = {
  bucket: '<path d="M5 7h14l-1.5 12.5A2 2 0 0 1 15.5 21h-7a2 2 0 0 1-2-1.5L5 7z"/><path d="M8 7V5a4 4 0 0 1 8 0v2"/>',
  folder: '<path d="M3 7a2 2 0 0 1 2-2h4l2 3h8a2 2 0 0 1 2 2v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>',
  file: '<path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"/><path d="M14 3v5h5"/>',
  up: '<path d="M12 19V5M5 12l7-7 7 7"/>',
  down: '<path d="M12 5v14M19 12l-7 7-7-7"/>',
  trash: '<path d="M4 7h16M10 11v6M14 11v6M6 7l1 13a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2l1-13M9 7V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v3"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  link: '<path d="M10 13a5 5 0 0 0 7 0l2-2a5 5 0 0 0-7-7l-1 1"/><path d="M14 11a5 5 0 0 0-7 0l-2 2a5 5 0 0 0 7 7l1-1"/>',
  copy: '<rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/>',
  gear: '<circle cx="12" cy="12" r="3"/><path d="M19 12a7 7 0 0 0-.1-1l2-1.6-2-3.4-2.3 1a7 7 0 0 0-1.7-1l-.3-2.5h-4l-.3 2.5a7 7 0 0 0-1.7 1l-2.3-1-2 3.4 2 1.6a7 7 0 0 0 0 2l-2 1.6 2 3.4 2.3-1a7 7 0 0 0 1.7 1l.3 2.5h4l.3-2.5a7 7 0 0 0 1.7-1l2.3 1 2-3.4-2-1.6a7 7 0 0 0 .1-1z"/>',
  search: '<circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/>',
  refresh: '<path d="M21 12a9 9 0 1 1-3-6.7M21 4v5h-5"/>',
};

function toast(msg, kind = "") {
  const t = el("div", { class: kind }, msg);
  $("#toast").append(t);
  setTimeout(() => { t.style.opacity = "0"; setTimeout(() => t.remove(), 200); }, kind === "err" ? 6000 : 3200);
}
const fmtSize = (n) => {
  if (n == null || n === "") return "";
  n = Number(n); const u = ["B", "KB", "MB", "GB", "TB"]; let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return (i ? n.toFixed(1) : n) + " " + u[i];
};
const fmtDate = (s) => (s ? new Date(s).toLocaleString() : "");
const relTime = (s) => {
  if (!s) return "";
  const d = (Date.now() - new Date(s)) / 1000;
  if (d < 60) return "just now";
  if (d < 3600) return Math.floor(d / 60) + "m ago";
  if (d < 86400) return Math.floor(d / 3600) + "h ago";
  if (d < 2592000) return Math.floor(d / 86400) + "d ago";
  return new Date(s).toLocaleDateString();
};
const parseXml = (s) => new DOMParser().parseFromString(s, "application/xml");
function extractErr(body, status) {
  const m = (body || "").match(/<Message>([^<]+)<\/Message>/) || (body || "").match(/"error"\s*:\s*"([^"]+)"/);
  return m ? m[1] : `HTTP ${status}`;
}
async function must(resp) {
  if (!resp.ok && !(resp.status === 204)) throw new Error(extractErr(await resp.text(), resp.status));
  return resp;
}
function copyText(t) {
  navigator.clipboard?.writeText(t).then(() => toast("Copied", "ok"), () => toast("Copy failed", "err"));
}

function modal(title, hint, fields, onOK, okLabel = "Create") {
  const d = $("#modal"); d.innerHTML = "";
  d.append(el("h3", {}, title));
  if (hint) d.append(el("p", { class: "hint" }, hint));
  const inp = {};
  for (const f of fields) {
    d.append(el("label", {}, f.label));
    let n;
    if (f.type === "select") { n = el("select"); for (const o of f.options) n.append(el("option", { value: o.value ?? o }, o.label ?? o)); n.value = f.value ?? ""; }
    else if (f.type === "textarea") n = el("textarea", {}, f.value ?? "");
    else n = el("input", { type: f.type === "password" ? "password" : "text", value: f.value ?? "", spellcheck: "false", readonly: f.readonly ? "" : null });
    inp[f.name] = n; d.append(n);
  }
  const row = el("div", { class: "row" });
  row.append(el("button", { class: "ghost", onclick: () => d.close() }, "Cancel"));
  const ok = el("button", { class: "primary" }, okLabel);
  ok.addEventListener("click", async () => {
    const v = {}; for (const k in inp) v[k] = inp[k].value.trim();
    ok.disabled = true; ok.textContent = "…";
    try { await onOK(v); d.close(); } catch (e) { toast(e.message, "err"); }
    finally { ok.disabled = false; ok.textContent = okLabel; }
  });
  row.append(ok); d.append(row); d.showModal();
  setTimeout(() => d.querySelector("input,textarea,select")?.focus(), 30);
}

// ---------- drawer ----------------------------------------------------

function closeDrawer() { $("#drawer").classList.remove("on"); $("#scrim").classList.remove("on"); }
$("#scrim").addEventListener("click", closeDrawer);
document.addEventListener("keydown", (e) => e.key === "Escape" && closeDrawer());

function openDrawer(build) {
  const d = $("#drawer"); d.innerHTML = "";
  build(d);
  d.classList.add("on"); $("#scrim").classList.add("on");
}

// ---------- state / routing -----------------------------------------

const state = { view: "buckets", bucket: null, prefix: "", tab: "objects" };
const main = () => $("#main");

function crumbs() {
  const c = $("#crumbs"); c.innerHTML = "";
  if (state.view !== "buckets" || !state.bucket) {
    c.append(el("span", { class: "cur" }, { buckets: "Buckets", identity: "Access Keys", info: "Monitoring" }[state.view]));
    return;
  }
  const link = (label, fn) => { const a = el("a", { href: "#" }, label); a.onclick = (e) => { e.preventDefault(); fn(); }; return a; };
  c.append(link("Buckets", () => { state.bucket = null; render(); }));
  c.append(ic('<path d="M9 6l6 6-6 6"/>'));
  c.append(link(state.bucket, () => { state.prefix = ""; state.tab = "objects"; render(); }));
  let acc = "";
  for (const seg of state.prefix.split("/").filter(Boolean)) {
    acc += seg + "/"; const a = acc;
    c.append(ic('<path d="M9 6l6 6-6 6"/>'));
    c.append(link(seg, () => { state.prefix = a; render(); }));
  }
}

function setView(v) {
  state.view = v; state.bucket = null;
  for (const a of document.querySelectorAll("nav a[data-view]")) a.classList.toggle("active", a.dataset.view === v);
  render();
}

async function render() {
  crumbs();
  main().innerHTML = "";
  main().append(el("div", { class: "empty" }, el("span", { class: "spin" })));
  try {
    if (state.view === "buckets" && !state.bucket) await viewBuckets();
    else if (state.view === "buckets") await viewBucket();
    else if (state.view === "identity") await viewIdentity();
    else if (state.view === "info") await viewInfo();
  } catch (e) {
    main().innerHTML = "";
    main().append(el("div", { class: "empty" }, el("div", {}, ic(ICON.trash, 1.6)), "Error: " + e.message));
    toast(e.message, "err");
  }
}

function pageHeader(title, sub, actions) {
  main().innerHTML = "";
  const h = el("div", { class: "page-h" }, el("h2", {}, title));
  if (sub) h.append(el("span", { class: "sub" }, sub));
  main().append(h);
  if (actions) main().append(actions);
}

// ---------- buckets list -------------------------------------------

async function viewBuckets() {
  const doc = parseXml(await (await must(await api("GET", "/"))).text());
  const names = [...doc.getElementsByTagName("Name")].map((n) => n.textContent);
  const dates = [...doc.getElementsByTagName("CreationDate")].map((n) => n.textContent);

  const tb = el("tbody");
  names.forEach((n, i) => {
    tb.append(el("tr", { class: "clk", onclick: () => { state.bucket = n; state.prefix = ""; state.tab = "objects"; render(); } },
      el("td", {}, el("span", { class: "name folder" }, ic(ICON.bucket), n)),
      el("td", { class: "muted" }, relTime(dates[i])),
      el("td", { class: "act" }, el("button", {
        class: "danger sm", onclick: async (e) => {
          e.stopPropagation();
          if (!confirm(`Delete bucket "${n}"?  It must be empty.`)) return;
          try { await must(await api("DELETE", "/" + n)); toast("Bucket deleted", "ok"); render(); }
          catch (err) { toast(err.message, "err"); }
        },
      }, ic(ICON.trash, 1.8), "Delete"))));
  });

  pageHeader("Buckets", names.length + (names.length === 1 ? " bucket" : " buckets"),
    el("div", { class: "toolbar" },
      el("div", { class: "grow" }),
      el("button", { class: "ghost", onclick: render }, ic(ICON.refresh), "Refresh"),
      el("button", {
        class: "primary", onclick: () => modal("Create bucket", "3–63 chars, lowercase letters, digits, - and .",
          [{ name: "name", label: "Name" }], async (v) => { await must(await api("PUT", "/" + v.name)); toast("Bucket created", "ok"); render(); }),
      }, ic(ICON.plus), "Create bucket")));

  if (!names.length) main().append(el("div", { class: "empty" }, el("div", {}, ic(ICON.bucket, 1.4)), "No buckets yet."));
  else main().append(el("div", { class: "card" }, el("table", {},
    el("thead", {}, el("tr", {}, el("th", {}, "Name"), el("th", {}, "Created"), el("th", {}))), tb)));
}

// ---------- one bucket: objects + settings -------------------------

async function viewBucket() {
  const b = state.bucket;
  const tabBtn = (id, label) => el("button", {
    class: "ghost sm" + (state.tab === id ? " primary" : ""),
    onclick: () => { state.tab = id; render(); },
  }, label);

  pageHeader(b, null, el("div", { class: "toolbar" },
    tabBtn("objects", "Objects"), tabBtn("settings", "Settings"),
    el("div", { class: "grow" })));

  if (state.tab === "settings") return bucketSettings(b);

  // objects toolbar
  const bar = $(".toolbar");
  const fi = el("input", { type: "file", multiple: "true", style: "display:none" });
  fi.onchange = () => doUpload([...fi.files]);
  bar.append(
    el("div", { class: "search" }, ic(ICON.search),
      el("input", { placeholder: "Filter this folder…", oninput: (e) => filterRows(e.target.value) })),
    el("button", { class: "ghost", onclick: render }, ic(ICON.refresh)),
    el("button", { class: "primary", onclick: () => fi.click() }, ic(ICON.up), "Upload"), fi);

  const drop = el("div", { id: "drop" });
  main().append(drop);
  ["dragenter", "dragover"].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.add("hot"); }));
  ["dragleave", "drop"].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.remove("hot"); }));
  drop.addEventListener("drop", (e) => doUpload([...e.dataTransfer.files]));
  drop.append(el("div", { id: "uplist" }));

  const res = await must(await api("GET", "/" + b, {
    query: { "list-type": "2", delimiter: "/", prefix: state.prefix, "max-keys": "1000" },
  }));
  const doc = parseXml(await res.text());
  const prefixes = [...doc.getElementsByTagName("CommonPrefixes")].map((n) => n.getElementsByTagName("Prefix")[0].textContent);
  const objs = [...doc.getElementsByTagName("Contents")].map((c) => ({
    key: c.getElementsByTagName("Key")[0].textContent,
    size: c.getElementsByTagName("Size")[0]?.textContent,
    lm: c.getElementsByTagName("LastModified")[0]?.textContent,
    etag: (c.getElementsByTagName("ETag")[0]?.textContent || "").replace(/"/g, ""),
  })).filter((o) => o.key !== state.prefix);
  const truncated = doc.getElementsByTagName("IsTruncated")[0]?.textContent === "true";

  const tb = el("tbody");
  const selectAll = el("input", { type: "checkbox", class: "chk" });
  selectAll.onchange = () => tb.querySelectorAll(".rowchk").forEach((c) => { c.checked = selectAll.checked; syncBulk(); });

  if (state.prefix) {
    const parent = state.prefix.replace(/[^/]+\/$/, "");
    tb.append(el("tr", { class: "clk", onclick: () => { state.prefix = parent; render(); } },
      el("td", {}), el("td", {}, el("span", { class: "name folder" }, ic(ICON.up), "..")), el("td", {}), el("td", {}), el("td", {})));
  }
  for (const p of prefixes) {
    tb.append(el("tr", { class: "clk", onclick: () => { state.prefix = p; render(); } },
      el("td", {}),
      el("td", {}, el("span", { class: "name folder" }, ic(ICON.folder), p.slice(state.prefix.length))),
      el("td", {}), el("td", {}), el("td", {})));
  }
  for (const o of objs) {
    const name = o.key.slice(state.prefix.length);
    const chk = el("input", { type: "checkbox", class: "chk rowchk", onclick: (e) => e.stopPropagation(), onchange: syncBulk });
    chk.dataset.key = o.key;
    tb.append(el("tr", { class: "clk", "data-name": name.toLowerCase(), onclick: () => objectDrawer(b, o) },
      el("td", {}, chk),
      el("td", {}, el("span", { class: "name" }, ic(ICON.file), name)),
      el("td", { class: "muted" }, relTime(o.lm)),
      el("td", { class: "num" }, fmtSize(o.size)),
      el("td", { class: "act" },
        el("button", { class: "ghost sm", onclick: (e) => { e.stopPropagation(); downloadObject(b, o.key); }, title: "Download" }, ic(ICON.down, 1.8)),
        el("button", { class: "ghost sm", onclick: async (e) => { e.stopPropagation(); copyText(await presignGet(b + "/" + o.key)); }, title: "Copy share link" }, ic(ICON.link, 1.8)))));
  }

  const bulk = el("div", { class: "toolbar hidden", id: "bulkbar" },
    el("span", { class: "muted", id: "bulkn" }),
    el("button", { class: "danger sm", onclick: bulkDelete }, ic(ICON.trash, 1.8), "Delete selected"));
  main().append(bulk);

  if (!prefixes.length && !objs.length)
    main().append(el("div", { class: "empty" }, el("div", {}, ic(ICON.folder, 1.4)),
      "This folder is empty. Drag files here or use Upload."));
  else
    main().append(el("div", { class: "card" }, el("table", {},
      el("thead", {}, el("tr", {}, el("th", { style: "width:34px" }, selectAll),
        el("th", {}, "Name"), el("th", {}, "Modified"), el("th", { class: "num" }, "Size"), el("th", {}))), tb)));

  if (truncated) main().append(el("div", { class: "toolbar" },
    el("span", { class: "muted" }, "Showing first 1000 entries.")));

  function syncBulk() {
    const n = tb.querySelectorAll(".rowchk:checked").length;
    bulk.classList.toggle("hidden", n === 0);
    $("#bulkn").textContent = n + " selected";
  }
  async function bulkDelete() {
    const keys = [...tb.querySelectorAll(".rowchk:checked")].map((c) => c.dataset.key);
    if (!confirm(`Delete ${keys.length} object(s)?`)) return;
    const body = `<Delete>${keys.map((k) => `<Object><Key>${k.replace(/&/g, "&amp;").replace(/</g, "&lt;")}</Key></Object>`).join("")}</Delete>`;
    await must(await api("POST", "/" + b, { query: { delete: "" }, contentType: "application/xml", body }));
    toast("Deleted " + keys.length, "ok"); render();
  }
}

function filterRows(q) {
  q = q.toLowerCase();
  document.querySelectorAll("#main tbody tr[data-name]").forEach((tr) => {
    tr.style.display = !q || tr.dataset.name.includes(q) ? "" : "none";
  });
}

async function doUpload(files) {
  if (!files.length) return;
  const list = $("#uplist") || main();
  for (const f of files) {
    const key = state.prefix + f.name;
    const row = el("div", { class: "up" }, ic(ICON.file), el("span", {}, f.name),
      el("span", { class: "bar" }, el("span", {})));
    list.append(row);
    const barFill = row.querySelector(".bar>span");
    try {
      await upload(state.bucket + "/" + key, f, (p) => (barFill.style.width = (p * 100).toFixed(0) + "%"));
      barFill.style.width = "100%"; row.style.opacity = ".5";
    } catch (e) { toast(f.name + ": " + e.message, "err"); row.remove(); }
  }
  toast("Upload complete", "ok");
  setTimeout(render, 400);
}

async function downloadObject(b, key) {
  try {
    const r = await must(await api("GET", "/" + b + "/" + key));
    const blob = await r.blob();
    const a = el("a", { href: URL.createObjectURL(blob), download: key.split("/").pop() });
    document.body.append(a); a.click(); a.remove();
  } catch (e) { toast(e.message, "err"); }
}

async function objectDrawer(b, o) {
  const name = o.key.split("/").pop();
  openDrawer((d) => {
    d.append(el("div", { class: "dh" }, ic(ICON.file), el("h3", {}, name),
      el("button", { class: "ghost sm", onclick: closeDrawer }, "✕")));
    const tabs = el("div", { class: "tabs" });
    const body = el("div", { class: "db" });
    d.append(tabs, body);
    const tab = (id, label, fn) => {
      const btn = el("button", { class: (id === "details" ? "on" : ""), onclick: () => { tabs.querySelectorAll("button").forEach((x) => x.classList.remove("on")); btn.classList.add("on"); body.innerHTML = ""; fn(body); } }, label);
      tabs.append(btn); return btn;
    };
    tab("details", "Details", (c) => {
      c.append(el("div", { class: "kv" },
        el("div", { class: "k" }, "Key"), el("div", { class: "v" }, o.key),
        el("div", { class: "k" }, "Size"), el("div", { class: "v" }, fmtSize(o.size) + ` (${o.size} B)`),
        el("div", { class: "k" }, "Modified"), el("div", { class: "v" }, fmtDate(o.lm)),
        el("div", { class: "k" }, "ETag"), el("div", { class: "v" }, el("code", {}, o.etag))));
      c.append(el("button", { class: "primary", onclick: () => downloadObject(b, o.key) }, ic(ICON.down, 1.8), "Download"));
      c.append(el("button", {
        class: "danger", style: "margin-left:8px", onclick: async () => {
          if (!confirm("Delete " + o.key + "?")) return;
          try { await must(await api("DELETE", "/" + b + "/" + o.key)); toast("Deleted", "ok"); closeDrawer(); render(); }
          catch (e) { toast(e.message, "err"); }
        },
      }, ic(ICON.trash, 1.8), "Delete"));
    });
    tab("share", "Share", (c) => {
      c.append(el("div", { class: "field" },
        el("label", {}, "Presigned GET link expires in"),
        (() => { const s = el("select"); for (const [l, v] of [["15 minutes", 900], ["1 hour", 3600], ["24 hours", 86400], ["7 days", 604800]]) s.append(el("option", { value: v }, l)); s.id = "exp"; return s; })()));
      const out = el("textarea", { readonly: "" }); out.style.minHeight = "90px";
      c.append(el("button", {
        class: "primary", onclick: async () => { out.value = await presignGet(b + "/" + o.key, Number($("#exp").value)); },
      }, ic(ICON.link, 1.8), "Generate link"));
      c.append(el("div", { class: "field" }, out,
        el("button", { class: "ghost sm", style: "margin-top:6px", onclick: () => copyText(out.value) }, ic(ICON.copy, 1.8), "Copy")));
    });
    tab("tags", "Tags", async (c) => {
      c.append(el("div", { class: "empty" }, el("span", { class: "spin" })));
      try {
        const doc = parseXml(await (await api("GET", "/" + b + "/" + o.key, { query: { tagging: "" } })).text());
        const tags = [...doc.getElementsByTagName("Tag")].map((t) => [t.getElementsByTagName("Key")[0].textContent, t.getElementsByTagName("Value")[0].textContent]);
        c.innerHTML = "";
        const ta = el("textarea", { placeholder: "key=value\nenv=prod" }, tags.map(([k, v]) => `${k}=${v}`).join("\n"));
        c.append(el("div", { class: "field" }, el("label", {}, "One key=value per line"), ta));
        c.append(el("button", {
          class: "primary", onclick: async () => {
            const set = ta.value.split("\n").map((l) => l.split("=")).filter((p) => p[0].trim());
            const xml = `<Tagging><TagSet>${set.map(([k, v]) => `<Tag><Key>${k.trim()}</Key><Value>${(v || "").trim()}</Value></Tag>`).join("")}</TagSet></Tagging>`;
            try { await must(await api("PUT", "/" + b + "/" + o.key, { query: { tagging: "" }, contentType: "application/xml", body: xml })); toast("Tags saved", "ok"); }
            catch (e) { toast(e.message, "err"); }
          },
        }, "Save tags"));
      } catch (e) { c.innerHTML = ""; c.append(el("div", { class: "muted" }, e.message)); }
    });
  });
}

// ---------- bucket settings --------------------------------------

async function bucketSettings(b) {
  const wrap = el("div");
  main().append(wrap);
  const sec = (title, node) => { wrap.append(el("h3", { style: "margin:22px 0 8px;font-size:15px" }, title)); wrap.append(node); };

  // policy
  let polDoc = "";
  try {
    const t = await (await api("GET", "/" + b, { query: { policy: "" } })).text();
    if (!t.startsWith("<")) polDoc = JSON.stringify(JSON.parse(t), null, 2);
  } catch {}
  const polTa = el("textarea", { placeholder: "No bucket policy. Paste a JSON policy document." }, polDoc);
  sec("Access Policy", el("div", {},
    polTa,
    el("div", { class: "toolbar" },
      el("button", { class: "ghost sm", onclick: () => polTa.value = JSON.stringify({ Version: "2012-10-17", Statement: [{ Effect: "Allow", Principal: "*", Action: ["s3:GetObject"], Resource: [`arn:aws:s3:::${b}/*`] }] }, null, 2) }, "Public read preset"),
      el("button", { class: "primary sm", onclick: async () => { try { await must(await api("PUT", "/" + b, { query: { policy: "" }, contentType: "application/json", body: polTa.value })); toast("Policy saved", "ok"); } catch (e) { toast(e.message, "err"); } } }, "Save policy"),
      el("button", { class: "danger sm", onclick: async () => { await api("DELETE", "/" + b, { query: { policy: "" } }); polTa.value = ""; toast("Policy removed", "ok"); } }, "Remove"))));

  // CORS
  let corsDoc = "";
  try { const r = await api("GET", "/" + b, { query: { cors: "" } }); if (r.ok) corsDoc = await r.text(); } catch {}
  const corsTa = el("textarea", { placeholder: '<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>' }, corsDoc.startsWith("<CORS") ? corsDoc : "");
  sec("CORS", el("div", {},
    corsTa,
    el("div", { class: "toolbar" },
      el("button", { class: "primary sm", onclick: async () => { try { await must(await api("PUT", "/" + b, { query: { cors: "" }, contentType: "application/xml", body: corsTa.value })); toast("CORS saved", "ok"); } catch (e) { toast(e.message, "err"); } } }, "Save CORS"),
      el("button", { class: "danger sm", onclick: async () => { await api("DELETE", "/" + b, { query: { cors: "" } }); corsTa.value = ""; toast("CORS removed", "ok"); } }, "Remove"))));

  // notifications
  let notif = { webhooks: [] };
  try { const r = await api("GET", "/" + b, { query: { notification: "" } }); if (r.ok) notif = await r.json(); } catch {}
  const nTa = el("textarea", {}, JSON.stringify(notif, null, 2));
  sec("Event notifications (webhooks)", el("div", {},
    el("p", { class: "muted", style: "font-size:12.5px;margin:.2em 0" }, 'e.g. {"webhooks":[{"id":"w1","url":"https://…","events":["s3:ObjectCreated:*"],"prefix":"","suffix":".jpg"}]}'),
    nTa,
    el("div", { class: "toolbar" },
      el("button", { class: "primary sm", onclick: async () => { try { await must(await api("PUT", "/" + b, { query: { notification: "" }, contentType: "application/json", body: nTa.value })); toast("Notifications saved", "ok"); } catch (e) { toast(e.message, "err"); } } }, "Save"))));
}

// ---------- identity --------------------------------------------

async function viewIdentity() {
  const [ur, sr] = await Promise.all([api("GET", "/gostore/admin/v1/users"), api("GET", "/gostore/admin/v1/service-accounts")]);
  if (ur.status === 403) { pageHeader("Access Keys"); main().append(el("div", { class: "empty" }, "Your account does not have admin permission.")); return; }
  const users = await (await must(ur)).json() || [];
  const svcs = sr.ok ? (await sr.json() || []) : [];

  pageHeader("Access Keys", users.length + " users, " + svcs.length + " service accounts",
    el("div", { class: "toolbar" }, el("div", { class: "grow" }),
      el("button", { class: "ghost", onclick: render }, ic(ICON.refresh), "Refresh"),
      el("button", {
        onclick: async () => {
          try {
            const j = await (await must(await api("POST", "/gostore/admin/v1/service-accounts", { contentType: "application/json", body: "{}" }))).json();
            modal("Service account created", "Copy the secret now — it is not shown again.",
              [{ name: "a", label: "Access key", value: j.accessKey, readonly: true }, { name: "s", label: "Secret key", value: j.secretKey, readonly: true }],
              async () => {}, "Done");
            render();
          } catch (e) { toast(e.message, "err"); }
        },
      }, ic(ICON.plus), "Service account"),
      el("button", {
        class: "primary", onclick: () => modal("Create user", "Access key ≥3 chars, secret ≥8 chars.", [
          { name: "accessKey", label: "Access key" },
          { name: "secretKey", label: "Secret key", type: "password" },
          { name: "policy", label: "Policy", type: "select", value: "readwrite", options: ["readwrite", "readonly", "writeonly", "consoleAdmin", "diagnostics"] },
        ], async (v) => {
          await must(await api("PUT", "/gostore/admin/v1/users", { contentType: "application/json", body: JSON.stringify({ accessKey: v.accessKey, secretKey: v.secretKey, policies: [v.policy] }) }));
          toast("User created", "ok"); render();
        }),
      }, ic(ICON.plus), "Create user")));

  const tb = el("tbody");
  for (const u of users) tb.append(el("tr", {},
    el("td", {}, el("span", { class: "name" }, ic(ICON.copy, 1.8), el("code", {}, u.accessKey))),
    el("td", { class: "muted" }, "user"),
    el("td", {}, (u.policies || []).map((p) => el("span", { class: "pill" }, p))),
    el("td", {}, el("span", { class: "pill" + (u.status !== "disabled" ? " ok" : "") }, u.status || "enabled")),
    el("td", { class: "act" }, el("button", {
      class: "danger sm", onclick: async () => {
        if (!confirm("Delete user " + u.accessKey + "?")) return;
        await api("DELETE", "/gostore/admin/v1/users", { query: { accessKey: u.accessKey } });
        toast("Deleted", "ok"); render();
      },
    }, ic(ICON.trash, 1.8)))));
  for (const s of svcs) tb.append(el("tr", {},
    el("td", {}, el("span", { class: "name" }, ic(ICON.copy, 1.8), el("code", {}, s.accessKey))),
    el("td", { class: "muted" }, "service account"),
    el("td", {}, el("span", { class: "pill" }, "parent: " + s.parentUser)),
    el("td", {}, el("span", { class: "pill ok" }, s.status || "enabled")),
    el("td", { class: "act" }, el("button", {
      class: "danger sm", onclick: async () => {
        await api("DELETE", "/gostore/admin/v1/service-accounts", { query: { accessKey: s.accessKey } });
        toast("Deleted", "ok"); render();
      },
    }, ic(ICON.trash, 1.8)))));

  main().append(el("div", { class: "card" }, el("table", {},
    el("thead", {}, el("tr", {}, el("th", {}, "Access key"), el("th", {}, "Type"), el("th", {}, "Policy"), el("th", {}, "Status"), el("th", {}))), tb)));
}

// ---------- monitoring -----------------------------------------

async function viewInfo() {
  const j = await (await must(await api("GET", "/gostore/admin/v1/info"))).json();
  pageHeader("Monitoring", j.version);
  const grid = el("div", { class: "stat-grid" });
  const stat = (k, v) => grid.append(el("div", { class: "stat" }, el("div", { class: "k" }, k), el("div", { class: "v" }, String(v))));
  stat("Mode", j.mode);
  stat("Drives", j.drives);
  stat("Parity", j.parity ?? "—");
  stat("Total space", fmtSize(j.totalSpace) || "—");
  stat("Free space", fmtSize(j.freeSpace) || "—");
  stat("Users", j.users);
  stat("Policies", j.policies);
  stat("Region", j.region);
  main().append(grid);

  if (j.mode === "erasure") {
    main().append(el("button", {
      onclick: async (e) => {
        e.target.disabled = true; e.target.textContent = "Healing…";
        try {
          const rep = await (await must(await api("POST", "/gostore/admin/v1/heal"))).json();
          toast(`Heal: ${rep.objectsHealed}/${rep.objectsScanned} objects, ${rep.shardsRewritten} shards, ${rep.metaRewritten} meta`, "ok");
        } catch (err) { toast(err.message, "err"); }
        e.target.disabled = false; e.target.textContent = "Run heal";
      },
    }, ic(ICON.refresh), "Run heal"));
  }
}

// ---------- boot ---------------------------------------------

async function verifyCreds() {
  const r = await api("GET", "/");
  if (!r.ok) { const t = await r.text(); throw new Error(extractErr(t, r.status)); }
}
function showApp() {
  $("#login").classList.add("hidden");
  $("#app").classList.remove("hidden");
  $("#whoami").textContent = session.ak;
  api("GET", "/gostore/admin/v1/info").then((r) => r.ok && r.json()).then((j) => j && ($("#ver").textContent = j.version)).catch(() => {});
  setView("buckets");
}

$("#loginBtn").addEventListener("click", async () => {
  const ak = $("#ak").value.trim(), sk = $("#sk").value.trim();
  $("#loginErr").textContent = "";
  const btn = $("#loginBtn"); btn.disabled = true; btn.textContent = "Signing in…";
  session.set(ak, sk);
  try { await verifyCreds(); showApp(); }
  catch (e) { session.clear(); $("#loginErr").textContent = e.message; }
  finally { btn.disabled = false; btn.textContent = "Sign in"; }
});
$("#sk").addEventListener("keydown", (e) => e.key === "Enter" && $("#loginBtn").click());
$("#logout").addEventListener("click", () => { session.clear(); location.reload(); });
for (const a of document.querySelectorAll("nav a[data-view]"))
  a.addEventListener("click", (e) => { e.preventDefault(); setView(a.dataset.view); });

if (session.ak && session.sk) api("GET", "/").then((r) => (r.ok ? showApp() : session.clear()));
