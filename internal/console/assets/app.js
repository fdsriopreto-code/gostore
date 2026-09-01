"use strict";
/* gostore console — dependency-free SPA. Signs every request with AWS SigV4
   using Web Crypto, same-origin with the S3 + admin API. */

const REGION = "us-east-1";
const enc = new TextEncoder();

// ---------- crypto / SigV4 -------------------------------------------------

async function hmac(key, msg) {
  const k = await crypto.subtle.importKey("raw", key, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await crypto.subtle.sign("HMAC", k, enc.encode(msg)));
}
async function hmacRaw(key, msgBytes) {
  const k = await crypto.subtle.importKey("raw", key, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await crypto.subtle.sign("HMAC", k, msgBytes));
}
function hex(buf) {
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
async function sha256hex(str) {
  return hex(await crypto.subtle.digest("SHA-256", enc.encode(str)));
}
async function signingKey(secret, dateStamp) {
  let k = enc.encode("AWS4" + secret);
  k = await hmac(k, dateStamp);
  k = await hmac(k, REGION);
  k = await hmac(k, "s3");
  k = await hmac(k, "aws4_request");
  return k;
}

// RFC3986 encoding matching the server's auth.EncodePath / query encoder.
function encPath(p) {
  return p.split("/").map(encComp).join("/");
}
function encComp(s) {
  return encodeURIComponent(s).replace(/[!*'()]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());
}
function canonicalQuery(q) {
  const keys = Object.keys(q).sort();
  return keys.map((k) => encComp(k) + "=" + encComp(q[k] ?? "")).join("&");
}

const session = {
  get ak() { return sessionStorage.getItem("gs_ak") || ""; },
  get sk() { return sessionStorage.getItem("gs_sk") || ""; },
  set(ak, sk) { sessionStorage.setItem("gs_ak", ak); sessionStorage.setItem("gs_sk", sk); },
  clear() { sessionStorage.removeItem("gs_ak"); sessionStorage.removeItem("gs_sk"); },
};

// signedFetch(method, path, {query, body, contentType, headers})
async function signedFetch(method, path, opts = {}) {
  const q = opts.query || {};
  const now = new Date();
  const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, "");
  const dateStamp = amzDate.slice(0, 8);
  const host = location.host;
  const payloadHash = "UNSIGNED-PAYLOAD";

  const hdr = { "x-amz-date": amzDate, "x-amz-content-sha256": payloadHash };
  if (opts.contentType) hdr["content-type"] = opts.contentType;
  Object.assign(hdr, opts.headers || {});

  const signedHeaders = Object.keys(hdr).map((h) => h.toLowerCase()).sort();
  signedHeaders.splice(signedHeaders.indexOf("x-amz-date"), 0, "host"); // insert host in order
  signedHeaders.sort();
  const canonHeaders =
    signedHeaders
      .map((h) => h + ":" + (h === "host" ? host : String(hdr[h]).trim()) + "\n")
      .join("");

  const canonReq = [
    method,
    encPath(path),
    canonicalQuery(q),
    canonHeaders,
    signedHeaders.join(";"),
    payloadHash,
  ].join("\n");

  const scope = `${dateStamp}/${REGION}/s3/aws4_request`;
  const sts = ["AWS4-HMAC-SHA256", amzDate, scope, await sha256hex(canonReq)].join("\n");
  const sig = hex(await hmacRaw(await signingKey(session.sk, dateStamp), enc.encode(sts)));
  hdr["Authorization"] =
    `AWS4-HMAC-SHA256 Credential=${session.ak}/${scope}, SignedHeaders=${signedHeaders.join(";")}, Signature=${sig}`;

  const qs = canonicalQuery(q);
  const url = location.origin + encPath(path) + (qs ? "?" + qs : "");
  return fetch(url, { method, headers: hdr, body: opts.body });
}

// ---------- helpers ------------------------------------------------------

const $ = (s, r = document) => r.querySelector(s);
const el = (tag, props = {}, ...kids) => {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (k === "class") n.className = v;
    else if (k === "html") n.innerHTML = v;
    else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
    else if (v != null) n.setAttribute(k, v);
  }
  for (const c of kids) if (c != null) n.append(c.nodeType ? c : document.createTextNode(c));
  return n;
};
function toast(msg, isErr) {
  const t = el("div", { class: isErr ? "err" : "" }, msg);
  $("#toast").append(t);
  setTimeout(() => t.remove(), isErr ? 6000 : 3500);
}
function fmtSize(n) {
  if (n == null) return "";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0; n = Number(n);
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return (i ? n.toFixed(1) : n) + " " + u[i];
}
const fmtDate = (s) => (s ? new Date(s).toLocaleString() : "");
const xml = (s) => new DOMParser().parseFromString(s, "application/xml");
const txt = (node, tag) => [...node.getElementsByTagName(tag)].map((n) => n.textContent);

async function apiErr(resp) {
  const body = await resp.text();
  let msg = resp.status + " " + resp.statusText;
  const m = body.match(/<Message>([^<]+)<\/Message>/) || body.match(/"error":"([^"]+)"/);
  if (m) msg = m[1];
  return new Error(msg);
}

function modal(title, fields, onSubmit) {
  const d = $("#modal");
  d.innerHTML = "";
  d.append(el("h3", {}, title));
  const inputs = {};
  for (const f of fields) {
    d.append(el("label", {}, f.label));
    const inp = el(f.type === "select" ? "select" : "input", {
      type: f.type === "password" ? "password" : "text", value: f.value || "",
    });
    if (f.type === "select") for (const o of f.options) inp.append(el("option", { value: o }, o));
    inputs[f.name] = inp;
    d.append(inp);
  }
  const row = el("div", { class: "row" });
  row.append(el("button", { onclick: () => d.close() }, "Cancel"));
  row.append(el("button", {
    class: "primary",
    onclick: async () => {
      const vals = {};
      for (const k in inputs) vals[k] = inputs[k].value.trim();
      try { await onSubmit(vals); d.close(); } catch (e) { toast(e.message, true); }
    },
  }, "OK"));
  d.append(row);
  d.showModal();
}

// ---------- views ------------------------------------------------------

const state = { view: "buckets", bucket: null, prefix: "" };
const main = () => $("#main");

function setView(v) {
  state.view = v;
  for (const a of document.querySelectorAll("nav a[data-view]"))
    a.classList.toggle("active", a.dataset.view === v);
  render();
}

async function render() {
  main().innerHTML = "";
  try {
    if (state.view === "buckets" && !state.bucket) await viewBuckets();
    else if (state.view === "buckets") await viewObjects();
    else if (state.view === "identity") await viewIdentity();
    else if (state.view === "info") await viewInfo();
  } catch (e) {
    main().append(el("div", { class: "empty" }, "Error: " + e.message));
    toast(e.message, true);
  }
}

async function viewBuckets() {
  const resp = await signedFetch("GET", "/");
  if (!resp.ok) throw await apiErr(resp);
  const doc = xml(await resp.text());
  const names = txt(doc, "Name");
  const dates = txt(doc, "CreationDate");

  main().append(el("h2", {}, "Buckets"));
  const bar = el("div", { class: "bar" });
  bar.append(el("div", { class: "grow" }));
  bar.append(el("button", {
    class: "primary",
    onclick: () => modal("Create bucket", [{ name: "name", label: "Bucket name (3-63 chars, a-z 0-9 - .)" }], async (v) => {
      const r = await signedFetch("PUT", "/" + v.name);
      if (!r.ok) throw await apiErr(r);
      toast("Bucket created"); render();
    }),
  }, "Create bucket"));
  main().append(bar);

  if (!names.length) { main().append(el("div", { class: "empty" }, "No buckets yet.")); return; }
  const tb = el("tbody");
  names.forEach((n, i) => {
    tb.append(el("tr", {},
      el("td", {}, el("a", { href: "#", onclick: (e) => { e.preventDefault(); openBucket(n); } }, n)),
      el("td", {}, fmtDate(dates[i])),
      el("td", { class: "actions" },
        el("button", {
          class: "danger",
          onclick: async () => {
            if (!confirm(`Delete bucket "${n}"? It must be empty.`)) return;
            const r = await signedFetch("DELETE", "/" + n);
            if (!r.ok) return toast((await apiErr(r)).message, true);
            toast("Deleted"); render();
          },
        }, "Delete")),
    ));
  });
  main().append(el("table", {}, el("thead", {}, el("tr", {}, el("th", {}, "Name"), el("th", {}, "Created"), el("th", {}))), tb));
}

function openBucket(name) { state.bucket = name; state.prefix = ""; setView("buckets"); }

async function viewObjects() {
  const b = state.bucket, prefix = state.prefix;
  main().append(el("h2", {}, b));

  const crumbs = el("div", { class: "crumbs" });
  crumbs.append(el("a", { href: "#", onclick: (e) => { e.preventDefault(); state.bucket = null; render(); } }, "‹ all buckets"));
  crumbs.append(document.createTextNode("  /  "));
  crumbs.append(el("a", { href: "#", onclick: (e) => { e.preventDefault(); state.prefix = ""; render(); } }, b));
  let acc = "";
  for (const seg of prefix.split("/").filter(Boolean)) {
    acc += seg + "/";
    const a = acc;
    crumbs.append(document.createTextNode(" / "));
    crumbs.append(el("a", { href: "#", onclick: (e) => { e.preventDefault(); state.prefix = a; render(); } }, seg));
  }
  main().append(crumbs);

  const bar = el("div", { class: "bar" });
  const fileInput = el("input", { type: "file", multiple: "true", style: "display:none" });
  fileInput.addEventListener("change", () => uploadFiles(fileInput.files));
  bar.append(el("button", { class: "primary", onclick: () => fileInput.click() }, "Upload"));
  bar.append(fileInput);
  bar.append(el("div", { class: "grow" }));
  main().append(bar);

  const resp = await signedFetch("GET", "/" + b, { query: { "list-type": "2", delimiter: "/", prefix } });
  if (!resp.ok) throw await apiErr(resp);
  const doc = xml(await resp.text());

  const tb = el("tbody");
  for (const cp of doc.getElementsByTagName("CommonPrefixes")) {
    const p = cp.getElementsByTagName("Prefix")[0].textContent;
    const label = p.slice(prefix.length);
    tb.append(el("tr", {},
      el("td", {}, el("a", { href: "#", onclick: (e) => { e.preventDefault(); state.prefix = p; render(); } }, "📁 " + label)),
      el("td", {}, ""), el("td", { class: "num" }, ""), el("td", {})));
  }
  for (const c of doc.getElementsByTagName("Contents")) {
    const key = c.getElementsByTagName("Key")[0].textContent;
    if (key === prefix) continue;
    const name = key.slice(prefix.length);
    const size = c.getElementsByTagName("Size")[0]?.textContent;
    const lm = c.getElementsByTagName("LastModified")[0]?.textContent;
    tb.append(el("tr", {},
      el("td", {}, name),
      el("td", {}, fmtDate(lm)),
      el("td", { class: "num" }, fmtSize(size)),
      el("td", { class: "actions" },
        el("button", { onclick: () => downloadObject(b, key) }, "Download"),
        el("button", {
          class: "danger",
          onclick: async () => {
            if (!confirm("Delete " + key + "?")) return;
            const r = await signedFetch("DELETE", "/" + b + "/" + key);
            if (r.status !== 204 && !r.ok) return toast((await apiErr(r)).message, true);
            toast("Deleted"); render();
          },
        }, "Delete")),
    ));
  }
  if (!tb.children.length) main().append(el("div", { class: "empty" }, "Empty."));
  else main().append(el("table", {}, el("thead", {}, el("tr", {},
    el("th", {}, "Name"), el("th", {}, "Modified"), el("th", { class: "num" }, "Size"), el("th", {}))), tb));
}

async function uploadFiles(files) {
  for (const f of files) {
    const key = state.prefix + f.name;
    toast("Uploading " + f.name + "…");
    try {
      const r = await signedFetch("PUT", "/" + state.bucket + "/" + key, {
        body: f, contentType: f.type || "application/octet-stream",
      });
      if (!r.ok) throw await apiErr(r);
      toast("Uploaded " + f.name);
    } catch (e) { toast(f.name + ": " + e.message, true); }
  }
  render();
}

async function downloadObject(b, key) {
  const r = await signedFetch("GET", "/" + b + "/" + key);
  if (!r.ok) return toast((await apiErr(r)).message, true);
  const blob = await r.blob();
  const a = el("a", { href: URL.createObjectURL(blob), download: key.split("/").pop() });
  document.body.append(a); a.click(); a.remove();
}

async function viewIdentity() {
  main().append(el("h2", {}, "Access Keys"));
  main().append(el("div", { class: "crumbs" }, "Users and service accounts. Requires admin permission."));

  const bar = el("div", { class: "bar" });
  bar.append(el("div", { class: "grow" }));
  bar.append(el("button", {
    class: "primary",
    onclick: () => modal("Create user", [
      { name: "accessKey", label: "Access key (>=3 chars)" },
      { name: "secretKey", label: "Secret key (>=8 chars)", type: "password" },
      { name: "policy", label: "Policy", type: "select", options: ["readwrite", "readonly", "writeonly", "consoleAdmin", "diagnostics"] },
    ], async (v) => {
      const r = await signedFetch("PUT", "/gostore/admin/v1/users", {
        contentType: "application/json",
        body: JSON.stringify({ accessKey: v.accessKey, secretKey: v.secretKey, policies: [v.policy] }),
      });
      if (!r.ok) throw await apiErr(r);
      toast("User created"); render();
    }),
  }, "Create user"));
  bar.append(el("button", {
    onclick: async () => {
      const r = await signedFetch("POST", "/gostore/admin/v1/service-accounts", {
        contentType: "application/json", body: JSON.stringify({}),
      });
      if (!r.ok) return toast((await apiErr(r)).message, true);
      const j = await r.json();
      modal("Service account created", [
        { name: "a", label: "Access key", value: j.accessKey },
        { name: "s", label: "Secret key (copy it now)", value: j.secretKey },
      ], async () => {});
      render();
    },
  }, "New service account"));
  main().append(bar);

  const [ur, sr] = await Promise.all([
    signedFetch("GET", "/gostore/admin/v1/users"),
    signedFetch("GET", "/gostore/admin/v1/service-accounts"),
  ]);
  if (!ur.ok) throw await apiErr(ur);
  const users = await ur.json();
  const svcs = sr.ok ? await sr.json() : [];

  const tb = el("tbody");
  for (const u of users || []) {
    tb.append(el("tr", {},
      el("td", {}, el("code", {}, u.accessKey)),
      el("td", {}, "user"),
      el("td", {}, (u.policies || []).map((p) => el("span", { class: "tag" }, p))),
      el("td", {}, u.status || "enabled"),
      el("td", { class: "actions" }, el("button", {
        class: "danger",
        onclick: async () => {
          if (!confirm("Delete user " + u.accessKey + "?")) return;
          const r = await signedFetch("DELETE", "/gostore/admin/v1/users", { query: { accessKey: u.accessKey } });
          if (r.status !== 204 && !r.ok) return toast((await apiErr(r)).message, true);
          toast("Deleted"); render();
        },
      }, "Delete")),
    ));
  }
  for (const s of svcs || []) {
    tb.append(el("tr", {},
      el("td", {}, el("code", {}, s.accessKey)),
      el("td", {}, "service account"),
      el("td", {}, el("span", { class: "tag" }, "parent: " + s.parentUser)),
      el("td", {}, s.status || "enabled"),
      el("td", { class: "actions" }, el("button", {
        class: "danger",
        onclick: async () => {
          const r = await signedFetch("DELETE", "/gostore/admin/v1/service-accounts", { query: { accessKey: s.accessKey } });
          if (r.status !== 204 && !r.ok) return toast((await apiErr(r)).message, true);
          toast("Deleted"); render();
        },
      }, "Delete")),
    ));
  }
  main().append(el("table", {}, el("thead", {}, el("tr", {},
    el("th", {}, "Access key"), el("th", {}, "Type"), el("th", {}, "Policy"), el("th", {}, "Status"), el("th", {}))), tb));
}

async function viewInfo() {
  main().append(el("h2", {}, "Monitoring"));
  const r = await signedFetch("GET", "/gostore/admin/v1/info");
  if (!r.ok) throw await apiErr(r);
  const j = await r.json();
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
      onclick: async () => {
        toast("Healing… this may take a while");
        const hr = await signedFetch("POST", "/gostore/admin/v1/heal");
        if (!hr.ok) return toast((await apiErr(hr)).message, true);
        const rep = await hr.json();
        toast(`Heal done: ${rep.objectsHealed}/${rep.objectsScanned} objects, ${rep.shardsRewritten} shards rewritten`);
      },
    }, "Run heal"));
  }
}

// ---------- boot ------------------------------------------------------

async function tryLogin(ak, sk) {
  sessionStorage.setItem("gs_ak", ak);
  sessionStorage.setItem("gs_sk", sk);
  const r = await signedFetch("GET", "/");
  if (!r.ok) { session.clear(); throw await apiErr(r); }
}

function showApp() {
  $("#login").classList.add("hidden");
  $("#app").classList.remove("hidden");
  $("#whoami").textContent = session.ak;
  setView("buckets");
}

$("#loginBtn").addEventListener("click", async () => {
  const ak = $("#ak").value.trim(), sk = $("#sk").value.trim();
  $("#loginErr").textContent = "";
  try { await tryLogin(ak, sk); showApp(); }
  catch (e) { $("#loginErr").textContent = e.message; }
});
$("#sk").addEventListener("keydown", (e) => { if (e.key === "Enter") $("#loginBtn").click(); });
$("#logout").addEventListener("click", (e) => { e.preventDefault(); session.clear(); location.reload(); });
for (const a of document.querySelectorAll("nav a[data-view]"))
  a.addEventListener("click", (e) => { e.preventDefault(); state.bucket = null; setView(a.dataset.view); });

if (session.ak && session.sk) {
  signedFetch("GET", "/").then((r) => { if (r.ok) showApp(); else session.clear(); });
}
