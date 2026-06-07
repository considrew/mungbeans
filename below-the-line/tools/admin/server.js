#!/usr/bin/env node
/*
 * The Book — local position authoring server.
 *
 * Serves a form at http://localhost:8088 that writes Layer-A reasoning
 * (positions + cash) directly into the repo's data files, then optionally
 * commits + pushes so Netlify rebuilds.
 *
 * Hard constraints (see build brief Rev 2 §C / §15):
 *   - No credentials, no brokerage, no PDF parsing.
 *   - No browser storage anywhere — authoring writes repo files on disk.
 *   - Asset types are exactly: stock | call | put | cash.
 *   - A position with an empty thesis is written status: draft (publish gate).
 */

'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { execFile } = require('child_process');
const yaml = require('js-yaml');

// ---- Paths -----------------------------------------------------------------
const HUGO_ROOT = path.resolve(__dirname, '..', '..'); // below-the-line/
const DATA_DIR = path.join(HUGO_ROOT, 'data');
const POSITIONS_DIR = path.join(DATA_DIR, 'positions');
const CASH_FILE = path.join(DATA_DIR, 'cash.yml');
const INDEX_FILE = path.join(__dirname, 'index.html');

const PORT = process.env.ADMIN_PORT ? Number(process.env.ADMIN_PORT) : 8088;
const ASSET_TYPES = ['stock', 'call', 'put'];
const EVENT_ACTIONS = ['open', 'add', 'trim', 'close', 'note'];

// ---- Small helpers ---------------------------------------------------------
function ensureDirs() {
  fs.mkdirSync(POSITIONS_DIR, { recursive: true });
}

function isBlank(s) {
  return s === undefined || s === null || String(s).trim() === '';
}

function slugify(s) {
  return String(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function num(v) {
  if (v === undefined || v === null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function dumpYaml(obj) {
  // lineWidth -1 keeps theses/notes from getting hard-wrapped; insertion order
  // is preserved (sortKeys false), multiline strings become block scalars.
  return yaml.dump(obj, { lineWidth: -1, noRefs: true, sortKeys: false });
}

function readPosition(file) {
  try {
    const doc = yaml.load(fs.readFileSync(file, 'utf8'));
    return doc && typeof doc === 'object' ? doc : null;
  } catch (e) {
    return { _parse_error: e.message, id: path.basename(file, '.yml') };
  }
}

function listPositions() {
  if (!fs.existsSync(POSITIONS_DIR)) return [];
  return fs
    .readdirSync(POSITIONS_DIR)
    .filter((f) => f.endsWith('.yml') || f.endsWith('.yaml'))
    .map((f) => {
      const p = readPosition(path.join(POSITIONS_DIR, f));
      if (p && !p.id) p.id = path.basename(f, path.extname(f));
      return p;
    })
    .filter(Boolean)
    .sort((a, b) => String(a.id).localeCompare(String(b.id)));
}

function readCash() {
  if (!fs.existsSync(CASH_FILE)) return { balances: [] };
  try {
    const doc = yaml.load(fs.readFileSync(CASH_FILE, 'utf8'));
    if (doc && Array.isArray(doc.balances)) return doc;
    return { balances: [] };
  } catch (e) {
    return { balances: [], _parse_error: e.message };
  }
}

// ---- Status logic (publish gate) ------------------------------------------
function computeStatus(thesis, events) {
  if (isBlank(thesis)) return 'draft'; // publish gate: no thesis => not published
  const hasClose = (events || []).some((e) => e.action === 'close');
  return hasClose ? 'closed' : 'open';
}

// ---- Build a clean, ordered position object --------------------------------
function buildPositionObject(existing, body) {
  const assetType = body.asset_type;
  const ticker = String(body.ticker || '').trim().toUpperCase();

  // Position-level metadata: take the new value when provided, else keep prior.
  const thesis = body.thesis !== undefined ? body.thesis : (existing && existing.thesis);
  const article = body.article !== undefined ? body.article : (existing && existing.article);
  const verdict = body.verdict !== undefined ? body.verdict : (existing && existing.verdict);

  // Events: keep prior, append the new one if present.
  const events = Array.isArray(existing && existing.events) ? existing.events.slice() : [];
  if (body.event) {
    const ev = body.event;
    const cleaned = { date: ev.date, action: ev.action };
    if (assetType === 'stock') {
      if (ev.action !== 'note') cleaned.shares = num(ev.shares);
    } else if (ev.action !== 'note') {
      cleaned.contracts = num(ev.contracts);
    }
    if (ev.action !== 'note') cleaned.price = num(ev.price);
    if (!isBlank(ev.note)) cleaned.note = String(ev.note);
    events.push(cleaned);
  }

  const status = computeStatus(thesis, events);

  // Ordered for readability — matches the brief's example layout.
  const out = {};
  out.id = body.id || (existing && existing.id);
  out.asset_type = assetType;
  out.ticker = ticker;
  out.status = status;
  out.thesis = isBlank(thesis) ? '' : String(thesis);
  if (!isBlank(article)) out.article = String(article);
  if (!isBlank(verdict)) out.verdict = String(verdict);

  if (assetType === 'call' || assetType === 'put') {
    const strike = body.strike !== undefined ? num(body.strike) : (existing && existing.strike);
    const expiry = body.expiry !== undefined ? body.expiry : (existing && existing.expiry);
    const mark = body.current_mark !== undefined ? num(body.current_mark) : (existing && existing.current_mark);
    if (strike !== null && strike !== undefined) out.strike = strike;
    if (!isBlank(expiry)) out.expiry = expiry;
    if (mark !== null && mark !== undefined) out.current_mark = mark;
  }

  out.events = events;
  return out;
}

// ---- Validation ------------------------------------------------------------
function validatePosition(body) {
  const errors = [];
  if (!ASSET_TYPES.includes(body.asset_type)) {
    errors.push(`asset_type must be one of ${ASSET_TYPES.join(', ')}`);
  }
  if (isBlank(body.ticker)) errors.push('ticker is required');

  const ev = body.event;
  if (ev) {
    if (isBlank(ev.date)) errors.push('event date is required');
    if (!EVENT_ACTIONS.includes(ev.action)) {
      errors.push(`event action must be one of ${EVENT_ACTIONS.join(', ')}`);
    }
    if (ev.action !== 'note') {
      if (num(ev.price) === null) errors.push('a price (share price or per-share premium) is required for this action');
      if (body.asset_type === 'stock' && num(ev.shares) === null) {
        errors.push('shares is required for a stock action');
      }
      if ((body.asset_type === 'call' || body.asset_type === 'put') && num(ev.contracts) === null) {
        errors.push('contracts is required for an option action');
      }
    }
  }

  if ((body.asset_type === 'call' || body.asset_type === 'put')) {
    // Strike/expiry expected for a brand-new option (no existing file).
    if (!body._editing) {
      if (num(body.strike) === null) errors.push('strike is required for a new option position');
      if (isBlank(body.expiry)) errors.push('expiry is required for a new option position');
    }
  }
  return errors;
}

function uniqueId(base) {
  let id = base;
  let n = 2;
  while (fs.existsSync(path.join(POSITIONS_DIR, `${id}.yml`))) {
    id = `${base}-${n++}`;
  }
  return id;
}

// ---- Handlers --------------------------------------------------------------
function handleSavePosition(body, send) {
  const errors = validatePosition(body);
  if (errors.length) return send(400, { ok: false, errors });

  ensureDirs();

  let existing = null;
  let id = body.id;

  if (id) {
    const file = path.join(POSITIONS_DIR, `${id}.yml`);
    if (fs.existsSync(file)) existing = readPosition(file);
  }
  if (!id) {
    // Generate a stable slug: ticker + month of the (first) event date.
    const d = (body.event && body.event.date) || '';
    const ym = /^\d{4}-\d{2}/.test(d) ? d.slice(0, 7) : '';
    const base = slugify(`${body.ticker}-${ym}`) || slugify(body.ticker) || 'position';
    id = uniqueId(base);
  }
  body.id = id;

  const obj = buildPositionObject(existing, body);
  const file = path.join(POSITIONS_DIR, `${id}.yml`);
  fs.writeFileSync(file, dumpYaml(obj), 'utf8');

  send(200, {
    ok: true,
    id,
    status: obj.status,
    draft: obj.status === 'draft',
    file: path.relative(HUGO_ROOT, file),
    position: obj,
  });
}

function handleSaveCash(body, send) {
  if (isBlank(body.date)) return send(400, { ok: false, errors: ['date is required'] });
  if (num(body.amount) === null) return send(400, { ok: false, errors: ['amount must be a number'] });

  ensureDirs();
  const cash = readCash();
  const entry = { date: body.date, amount: num(body.amount) };
  if (!isBlank(body.note)) entry.note = String(body.note);
  cash.balances.push(entry);
  // Keep chronological; build script treats latest-dated as current.
  cash.balances.sort((a, b) => String(a.date).localeCompare(String(b.date)));
  delete cash._parse_error;
  fs.writeFileSync(CASH_FILE, dumpYaml({ balances: cash.balances }), 'utf8');
  send(200, { ok: true, file: path.relative(HUGO_ROOT, CASH_FILE), balances: cash.balances });
}

function handleCommit(body, send) {
  // Guarded: only ever stages the repo's data/ directory. Uses whatever git
  // identity the machine already has — no tokens handled here.
  const msg = isBlank(body.message)
    ? `Update The Book data (${new Date().toISOString().slice(0, 10)})`
    : String(body.message).slice(0, 200);
  const push = body.push !== false;

  execFile('git', ['rev-parse', '--show-toplevel'], { cwd: HUGO_ROOT }, (err, topOut) => {
    if (err) return send(500, { ok: false, errors: ['not a git repository: ' + err.message] });
    const top = topOut.trim();
    const steps = [
      ['add', '--', DATA_DIR],
      ['commit', '-m', msg],
    ];
    if (push) steps.push(['push']);

    const log = [];
    const runNext = (i) => {
      if (i >= steps.length) return send(200, { ok: true, message: msg, pushed: push, log });
      execFile('git', steps[i], { cwd: top }, (e, out, errOut) => {
        log.push({ cmd: 'git ' + steps[i].join(' '), stdout: (out || '').trim(), stderr: (errOut || '').trim() });
        if (e) {
          // "nothing to commit" is not a real failure — report and stop cleanly.
          const combined = ((out || '') + (errOut || '')).toLowerCase();
          if (steps[i][0] === 'commit' && combined.includes('nothing to commit')) {
            return send(200, { ok: true, message: 'nothing to commit', pushed: false, log });
          }
          return send(500, { ok: false, errors: [e.message], log });
        }
        runNext(i + 1);
      });
    };
    runNext(0);
  });
}

// ---- HTTP plumbing ---------------------------------------------------------
function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => {
      data += c;
      if (data.length > 5e6) reject(new Error('body too large'));
    });
    req.on('end', () => {
      if (!data) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch (e) {
        reject(e);
      }
    });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const send = (code, obj) => sendJson(res, code, obj);

  try {
    if (req.method === 'GET' && url.pathname === '/') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(fs.readFileSync(INDEX_FILE, 'utf8'));
    }

    if (req.method === 'GET' && url.pathname === '/api/state') {
      return send(200, {
        ok: true,
        positions: listPositions(),
        cash: readCash(),
        data_dir: path.relative(HUGO_ROOT, DATA_DIR),
        asset_types: ASSET_TYPES,
        event_actions: EVENT_ACTIONS,
      });
    }

    if (req.method === 'POST' && url.pathname === '/api/position') {
      const body = await readBody(req);
      return handleSavePosition(body, send);
    }

    if (req.method === 'POST' && url.pathname === '/api/cash') {
      const body = await readBody(req);
      return handleSaveCash(body, send);
    }

    if (req.method === 'POST' && url.pathname === '/api/commit') {
      const body = await readBody(req);
      return handleCommit(body, send);
    }

    send(404, { ok: false, errors: ['not found'] });
  } catch (e) {
    send(500, { ok: false, errors: [e.message] });
  }
});

server.listen(PORT, '127.0.0.1', () => {
  // eslint-disable-next-line no-console
  console.log(`\n  The Book authoring tool → http://localhost:${PORT}`);
  console.log(`  Writing to: ${path.relative(process.cwd(), DATA_DIR)}/  (positions/*.yml, cash.yml)`);
  console.log('  Ctrl-C to stop.\n');
});
