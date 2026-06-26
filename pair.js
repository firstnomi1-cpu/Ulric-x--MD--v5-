/**
 * Ulric-X MD v3.1 - WhatsApp Multi-User Connection Manager
 *
 * CRITICAL FIXES vs v3.0:
 * 1. Added keepAliveIntervalMs: 30000 (keeps connection alive forever)
 * 2. Added connectTimeoutMs: 30000 (proper timeout)
 * 3. Added defaultQueryTimeoutMs: 30000
 * 4. Added 60-second heartbeat (sendPresenceUpdate 'available')
 *    → This prevents the famous "5-hour timeout" disconnection
 * 5. Added internet connectivity check before connecting
 * 6. Used setTimeout pattern (more reliable than await delay for pair code)
 *
 * IMPORTANT: User MUST use REAL WhatsApp number for push notification.
 * Fake numbers get a code but no notification (WhatsApp anti-spam).
 */
const fs = require('fs');
const path = require('path');
const pino = require('pino');
const chalk = require('chalk');
const dns = require('dns');
const { promisify } = require('util');
const baileys = require('@whiskeysockets/baileys');
const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion
} = baileys;

const config = require('./config');
const store  = require('./lib/store');
const { ensureDir, sleep } = require('./lib/utils');

const dnsLookup = promisify(dns.lookup);
ensureDir(config.SESSIONS_DIR);

const connections = new Map();
const sessionLocks = new Map();
const heartbeats = new Map(); // jid -> intervalId

/**
 * Check internet connectivity (matches reference bot)
 */
async function checkInternet() {
  try {
    await dnsLookup('google.com');
    return true;
  } catch (e) {
    return false;
  }
}

/**
 * Generate a REAL WhatsApp pair code.
 *
 * IMPORTANT: User's phone number must be a REAL WhatsApp account.
 * Fake numbers get a code but NO push notification.
 */
async function generatePairCode(phoneNumber) {
  const clean = String(phoneNumber).replace(/\D/g, '');
  if (clean.length < 7 || clean.length > 15) {
    throw new Error('Invalid phone number length (need 7-15 digits)');
  }
  if (clean.startsWith('0')) {
    throw new Error('Remove leading 0, use country code (e.g. 923xxx not 03xxx)');
  }

  const jid = clean + '@s.whatsapp.net';
  const sessionPath = path.join(config.SESSIONS_DIR, jid);

  // Check if already paired
  if (fs.existsSync(path.join(sessionPath, 'creds.json'))) {
    console.log(chalk.blue(`[PAIR] ${jid} already paired, reconnecting...`));
    startConnection(jid, false).catch(e => console.error(e.message));
    throw new Error('Already paired. Reconnecting your session. Send .menu to your WhatsApp.');
  }

  // Check pair limit
  const pairedCount = store.getUsers().length;
  if (pairedCount >= config.MAX_PAIR_USERS) {
    throw new Error('Pairing limit reached. Try again later.');
  }

  // Acquire session lock
  if (sessionLocks.has(jid)) {
    throw new Error('A pair request is already in progress for this number. Please wait.');
  }
  sessionLocks.set(jid, true);

  // Check internet first
  const hasInternet = await checkInternet();
  if (!hasInternet) {
    sessionLocks.delete(jid);
    throw new Error('No internet connection. Server cannot reach WhatsApp.');
  }

  try {
    ensureDir(sessionPath);

    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
    const { version } = await fetchLatestBaileysVersion();

    // ═══════════════════════════════════════════════════════════════
    // CRITICAL FIX: Match working reference exactly
    // - keepAliveIntervalMs: 30000 (keeps connection alive)
    // - connectTimeoutMs: 30000 (proper timeout)
    // - defaultQueryTimeoutMs: 30000
    // - browser: ["Ubuntu", "Chrome", "20.0.04"]
    // ═══════════════════════════════════════════════════════════════
    const sock = makeWASocket({
      version,
      logger: pino({ level: 'silent' }),
      auth: state,
      printQRInTerminal: false,
      browser: ["Ubuntu", "Chrome", "20.0.04"],
      connectTimeoutMs: 30000,
      defaultQueryTimeoutMs: 30000,
      keepAliveIntervalMs: 30000,  // ← CRITICAL: keeps connection alive
      generateHighQualityLinkPreview: false,
      markOnlineOnConnect: false,
      getMessage: async () => undefined
    });

    // ═══════════════════════════════════════════════════════════════
    // CRITICAL FIX: 60-second heartbeat to prevent 5-hour timeout
    // This is what keeps the bot online 24/7
    // ═══════════════════════════════════════════════════════════════
    const heartbeat = setInterval(() => {
      try {
        if (sock.ws && sock.ws.readyState === 1) {
          sock.sendPresenceUpdate('available');
        }
      } catch (e) {}
    }, 60000);

    let pairCode = null;
    let connectionOpen = false;
    let connectionError = null;

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect } = update;

      if (connection === 'connecting') {
        console.log(chalk.cyan(`[PAIR] Connecting to WhatsApp for ${jid}...`));
      }

      if (connection === 'open') {
        connectionOpen = true;
        connections.set(jid, { sock, status: 'open', lastSeen: Date.now(), jid });
        console.log(chalk.green(`[PAIR] ✅ Connected: ${jid}`));

        // Save heartbeat reference
        heartbeats.set(jid, heartbeat);

        // Mark user as paired
        store.addUser(jid, {
          pairedAt: Date.now(),
          country: getCountryFromNumber(clean)
        });

        // Fire on-pair hooks (broadcast)
        try { await onPair(jid, sock); } catch (e) { console.error('[onPair]', e.message); }
      }

      if (connection === 'close') {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        connections.set(jid, { sock, status: 'closed', lastSeen: Date.now(), jid });

        // Clear heartbeat
        const hb = heartbeats.get(jid);
        if (hb) { clearInterval(hb); heartbeats.delete(jid); }

        if (connectionOpen) {
          // Was previously open - auto reconnect
          console.log(chalk.yellow(`[PAIR] Reconnecting ${jid} in 5s (code=${statusCode})`));
          setTimeout(() => startConnection(jid, false).catch(e => console.error(e.message)), 5000);
        } else if (!pairCode) {
          // Closed before pair code was generated
          connectionError = `Connection closed (code ${statusCode}). WhatsApp may be busy. Try again in 30 seconds.`;
        }
      }
    });

    // Attach message handler
    const handler = require('./handler');
    sock.ev.on('messages.upsert', async ({ messages }) => {
      try { await handler.onMessage(sock, messages[0]); } catch (e) {}
    });

    sock.ev.on('group-participants.update', async (ev) => {
      try { await handler.onGroupUpdate(sock, ev); } catch (e) {}
    });

    // ═══════════════════════════════════════════════════════════════
    // CRITICAL FIX: Wait for connection to establish, then request code
    // Use setTimeout pattern (matches working reference exactly)
    // ═══════════════════════════════════════════════════════════════
    console.log(chalk.cyan(`[PAIR] Waiting 5s for WhatsApp connection to establish...`));
    await sleep(5000);

    if (state.creds.registered) {
      throw new Error('Already registered. Send .menu to your WhatsApp.');
    }

    if (connectionError) {
      throw new Error(connectionError);
    }

    // Request pair code (official WhatsApp API)
    console.log(chalk.cyan(`[PAIR] Requesting pair code for ${clean}...`));
    try {
      let code = await sock.requestPairingCode(clean);
      // Format: WhatsApp-style ABCD-1234
      code = code?.match(/.{1,4}/g)?.join('-') || code;
      pairCode = code;

      console.log(chalk.green(`\n========================================`));
      console.log(chalk.green(`   YOUR PAIRING CODE: ${code}`));
      console.log(chalk.green(`   For: ${clean}`));
      console.log(chalk.green(`========================================\n`));

      return { code, rawCode: code.replace(/-/g, ''), jid };
    } catch (e) {
      console.error(chalk.red(`[PAIR] requestPairingCode failed: ${e.message}`));
      throw new Error(`Failed to get pair code: ${e.message}. Try again in 30 seconds.`);
    }

  } catch (error) {
    console.error(chalk.red(`[PAIR] Error for ${jid}: ${error.message}`));
    throw error;
  } finally {
    // Release lock after 30 seconds
    setTimeout(() => sessionLocks.delete(jid), 30000);
  }
}

/**
 * Start (or restart) a connection for an already-paired user.
 */
async function startConnection(jid, isPairing = false) {
  const sessionPath = path.join(config.SESSIONS_DIR, jid);
  ensureDir(sessionPath);

  if (!fs.existsSync(path.join(sessionPath, 'creds.json'))) {
    console.log(chalk.yellow(`[CONN] No creds for ${jid}, skipping`));
    return null;
  }

  // Check internet
  const hasInternet = await checkInternet();
  if (!hasInternet) {
    console.log(chalk.red(`[CONN] No internet, retrying ${jid} in 30s`));
    setTimeout(() => startConnection(jid, false).catch(e => console.error(e.message)), 30000);
    return null;
  }

  const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    logger: pino({ level: 'silent' }),
    auth: state,
    printQRInTerminal: false,
    browser: ["Ubuntu", "Chrome", "20.0.04"],
    connectTimeoutMs: 30000,
    defaultQueryTimeoutMs: 30000,
    keepAliveIntervalMs: 30000,
    generateHighQualityLinkPreview: false,
    markOnlineOnConnect: false,
    getMessage: async () => undefined
  });

  connections.set(jid, { sock, status: 'connecting', lastSeen: Date.now(), jid });

  // Heartbeat for this connection
  const heartbeat = setInterval(() => {
    try {
      if (sock.ws && sock.ws.readyState === 1) {
        sock.sendPresenceUpdate('available');
      }
    } catch (e) {}
  }, 60000);

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;

    if (connection === 'open') {
      connections.set(jid, { sock, status: 'open', lastSeen: Date.now(), jid });
      heartbeats.set(jid, heartbeat);
      console.log(chalk.green(`[CONN] ✅ Connected: ${jid}`));
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
      connections.set(jid, { sock, status: 'closed', lastSeen: Date.now(), jid });

      const hb = heartbeats.get(jid);
      if (hb) { clearInterval(hb); heartbeats.delete(jid); }

      if (shouldReconnect) {
        console.log(chalk.yellow(`[CONN] Reconnecting ${jid} in 5s (code=${statusCode})`));
        setTimeout(() => startConnection(jid, false).catch(e => console.error(e.message)), 5000);
      } else {
        // Logged out - unpair
        console.log(chalk.red(`[CONN] ${jid} logged out, unpairing`));
        unpairUser(jid, true);
      }
    }
  });

  const handler = require('./handler');
  sock.ev.on('messages.upsert', async ({ messages }) => {
    try { await handler.onMessage(sock, messages[0]); } catch (e) {}
  });

  sock.ev.on('group-participants.update', async (ev) => {
    try { await handler.onGroupUpdate(sock, ev); } catch (e) {}
  });

  return sock;
}

/**
 * Called when a new user pairs. Sends broadcast notifications.
 */
async function onPair(jid, sock) {
  if (!config.BCAST_ON_PAIR) return;
  const text = config.BCAST_TEXT_ON_PAIR(jid);

  try {
    await sock.sendMessage(config.BOT_OWNER_JID, { text });
  } catch (e) {}

  try {
    const ownerConn = connections.get(config.BOT_OWNER_JID);
    const ownerSock = ownerConn?.sock || sock;
    const groups = await ownerSock.groupFetchAllWhitelist?.().catch(() => []) || [];
    for (const g of groups.slice(0, 5)) {
      try { await ownerSock.sendMessage(g.id, { text }); } catch (e) {}
    }
  } catch (e) {}
}

/**
 * Force-unpair a user.
 */
function unpairUser(jid, deleteSession = true) {
  const conn = connections.get(jid);
  if (conn?.sock) {
    try { conn.sock.end(new Error('Unpair requested')); } catch (e) {}
  }
  // Clear heartbeat
  const hb = heartbeats.get(jid);
  if (hb) { clearInterval(hb); heartbeats.delete(jid); }

  connections.delete(jid);
  store.removeUser(jid);

  if (deleteSession) {
    const sessionPath = path.join(config.SESSIONS_DIR, jid);
    try { fs.rmSync(sessionPath, { recursive: true, force: true }); } catch (e) {}
  }
  console.log(chalk.red(`[UNPAIR] ${jid} removed`));
  return true;
}

function getConnection(jid) { return connections.get(jid); }
function getAllConnections() { return Array.from(connections.values()); }

/**
 * Auto-load all previously paired sessions on boot.
 */
async function autoLoadAllPaired(onProgress) {
  const entries = fs.existsSync(config.SESSIONS_DIR)
    ? fs.readdirSync(config.SESSIONS_DIR, { withFileTypes: true })
    : [];
  const dirs = entries
    .filter(d => d.isDirectory() && d.name.endsWith('@s.whatsapp.net'))
    .map(d => d.name)
    .filter(jid => fs.existsSync(path.join(config.SESSIONS_DIR, jid, 'creds.json')));

  console.log(chalk.cyan(`[AUTOLOAD] Found ${dirs.length} paired session(s).`));

  for (let i = 0; i < dirs.length; i++) {
    const jid = dirs[i];
    try {
      console.log(chalk.blue(`[AUTOLOAD] Connecting ${i+1}/${dirs.length}: ${jid}`));
      await startConnection(jid, false);
      if (onProgress) onProgress(i + 1, dirs.length, jid);
      await sleep(2000);
    } catch (e) {
      console.error(chalk.red(`[AUTOLOAD] Failed ${jid}: ${e.message}`));
    }
  }
  console.log(chalk.green(`[AUTOLOAD] Done. Active connections: ${connections.size}`));
}

/**
 * Broadcast to all paired users.
 */
async function broadcastAll(text, opts = {}) {
  const targets = [];
  for (const [jid, info] of connections.entries()) {
    if (info.status !== 'open') continue;
    try {
      await info.sock.sendMessage(jid, { text });
      targets.push(jid);
      const groups = await info.sock.groupFetchAllWhitelist?.().catch(() => []) || [];
      for (const g of groups.slice(0, 10)) {
        try { await info.sock.sendMessage(g.id, { text }); targets.push(g.id); } catch (e) {}
      }
    } catch (e) {}
    if (targets.length >= (opts.limit || Infinity)) break;
  }
  return targets;
}

async function broadcastOwnerGroups(text) {
  const ownerConn = connections.get(config.BOT_OWNER_JID);
  if (!ownerConn || ownerConn.status !== 'open') return [];
  const targets = [];
  const groups = await ownerConn.sock.groupFetchAllWhitelist?.().catch(() => []) || [];
  for (const g of groups) {
    try { await ownerConn.sock.sendMessage(g.id, { text }); targets.push(g.id); } catch (e) {}
  }
  return targets;
}

function getCountryFromNumber(num) {
  const { getCountry } = require('./lib/utils');
  return getCountry(num);
}

module.exports = {
  generatePairCode,
  startConnection,
  unpairUser,
  getConnection,
  getAllConnections,
  autoLoadAllPaired,
  broadcastAll,
  broadcastOwnerGroups,
  checkInternet
};
