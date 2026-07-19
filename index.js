// Servico isolado: numero secundario/descartavel via Baileys puro (com
// limitador de velocidade proprio, sem depender de pacote de terceiro).
// NAO toca na Evolution API oficial nem no numero +55 11 5304-6520 — roda como
// um servico Railway totalmente separado, so pra validar o fluxo do Typebot
// enquanto o problema da WABA oficial nao se resolve.

import { rm } from 'node:fs/promises';
import { webcrypto } from 'node:crypto';
import * as baileysNS from 'baileys';
import { Boom } from '@hapi/boom';
import express from 'express';
import qrcode from 'qrcode';

// Garante que "crypto" existe globalmente, independente da versao do Node
// que o Railway acabar usando — a Baileys precisa disso pra conectar.
if (!globalThis.crypto) globalThis.crypto = webcrypto;

// A causa do crash: em algumas versoes a Baileys nao exporta makeWASocket
// como "default" puro (vem dentro de outro nivel). Isso tenta os formatos
// mais comuns e, se nao achar, imprime o formato real no log em vez de
// travar sem explicacao.
const makeWASocket =
  typeof baileysNS.default === 'function' ? baileysNS.default :
  typeof baileysNS.makeWASocket === 'function' ? baileysNS.makeWASocket :
  typeof baileysNS.default?.default === 'function' ? baileysNS.default.default :
  null;

const useMultiFileAuthState = baileysNS.useMultiFileAuthState ?? baileysNS.default?.useMultiFileAuthState;
const DisconnectReason = baileysNS.DisconnectReason ?? baileysNS.default?.DisconnectReason;
const fetchLatestBaileysVersion = baileysNS.fetchLatestBaileysVersion ?? baileysNS.default?.fetchLatestBaileysVersion;

if (!makeWASocket) {
  console.error('Nao encontrei makeWASocket no pacote baileys.');
  console.error('Chaves disponiveis em baileysNS:', Object.keys(baileysNS));
  console.error('Chaves disponiveis em baileysNS.default:', baileysNS.default ? Object.keys(baileysNS.default) : 'sem default');
  process.exit(1);
}

// Nome trocado de proposito: "N8N_WEBHOOK_URL" e uma variavel que o
// proprio n8n ja usa internamente, e isso causou confusao (a variavel foi
// configurada no servico errado). Esse nome novo nao bate com nada do n8n.
const FORWARD_TO_URL = process.env.FORWARD_TO_URL;
const PORT = process.env.PORT || 3001;

let sock = null;
let currentQR = null;
let connectionState = 'iniciando'; // iniciando | aguardando_qr | conectado | desconectado

// Deduplicacao de mensagem — apos reconectar/escanear QR de novo, a Baileys
// as vezes reemite mensagens recentes durante a sincronizacao, o que gerava
// resposta em dobro. Guarda os IDs vistos nos ultimos 5 min e ignora repetidos.
const mensagensProcessadas = new Set();
const JANELA_DEDUP_MS = 5 * 60 * 1000;

function jaProcessada(id) {
  if (!id) return false; // sem ID, nao da pra deduplicar, deixa passar
  if (mensagensProcessadas.has(id)) return true;
  mensagensProcessadas.add(id);
  setTimeout(() => mensagensProcessadas.delete(id), JANELA_DEDUP_MS);
  return false;
}

async function connectToWhatsApp() {
  // Caminho absoluto dentro do volume persistente do Railway (mount path
  // "/data") — NAO usar caminho relativo aqui, ele some a cada redeploy e
  // forca escanear QR de novo toda vez.
  const { state, saveCreds } = await useMultiFileAuthState('/data/auth_info');

  // O WhatsApp rejeita conexao de versao desatualizada do protocolo —
  // busca a versao atual em vez de usar a que veio junto com o pacote.
  let version;
  if (fetchLatestBaileysVersion) {
    ({ version } = await fetchLatestBaileysVersion());
    console.log('Usando versao do WhatsApp Web:', version);
  }

  const rawSock = makeWASocket({
    auth: state,
    ...(version ? { version } : {}),
  });
  sock = rawSock;

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      currentQR = qr;
      connectionState = 'aguardando_qr';
      console.log('Novo QR code disponivel em /qr');
    }

    if (connection === 'close') {
      connectionState = 'desconectado';
      const statusCode = lastDisconnect?.error instanceof Boom
        ? lastDisconnect.error.output.statusCode
        : undefined;
      const motivo = lastDisconnect?.error?.message || 'sem mensagem de erro';
      const isLoggedOut = statusCode === DisconnectReason.loggedOut;
      console.log(`Conexao fechada — codigo: ${statusCode} — motivo: ${motivo} — deslogado: ${isLoggedOut}`);

      setTimeout(async () => {
        if (isLoggedOut) {
          // Numero/aparelho foi desvinculado de vez — a sessao salva nao
          // serve mais. Apaga e reconecta do zero pra gerar um QR novo.
          console.log('Sessao encerrada — limpando dados antigos pra gerar QR novo.');
          await rm('/data/auth_info', { recursive: true, force: true }).catch((e) =>
            console.error('Erro ao limpar auth_info:', e.message)
          );
        }
        connectToWhatsApp();
      }, isLoggedOut ? 2000 : 5000);
    } else if (connection === 'open') {
      currentQR = null;
      connectionState = 'conectado';
      console.log('Conectado ao WhatsApp (numero secundario).');
    }
  });

  // Repassa mensagens recebidas pro n8n — payload simples e proprio,
  // NAO é o mesmo formato que a Evolution manda, por isso o n8n precisa
  // de um fluxo (ou branch) separado pra tratar isso.
  sock.ev.on('messages.upsert', async ({ messages }) => {
    for (const msg of messages) {
      if (msg.key.fromMe || !msg.message) continue;

      if (jaProcessada(msg.key.id)) {
        console.log(`Mensagem ${msg.key.id} ja processada — ignorando duplicata (comum logo apos reconectar).`);
        continue;
      }

      const from = msg.key.remoteJid;
      const text = msg.message.conversation
        || msg.message.extendedTextMessage?.text
        || '';

      if (!text) continue;

      console.log(`Mensagem recebida de ${from}: "${text}"`);

      if (!FORWARD_TO_URL) {
        console.error('FORWARD_TO_URL nao configurada — mensagem NAO foi repassada pra lugar nenhum. Configure essa variavel no Railway, no servico do Baileys (nao no do n8n).');
        continue;
      }

      // Mostra "digitando..." JA, assim que a mensagem chega — antes de
      // chamar o n8n/IA, nao depois. E isso, nao o MS_POR_CARACTER la
      // embaixo, que decide quando o indicador COMECA a aparecer.
      await sock.sendPresenceUpdate('composing', from).catch((e) =>
        console.error('Falha ao mostrar "digitando":', e.message)
      );

      try {
        await fetch(FORWARD_TO_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            from,
            text,
            source: 'baileys-secundario',
            timestamp: msg.messageTimestamp,
          }),
        });
        console.log(`Repassada pro n8n com sucesso. Destino: ${FORWARD_TO_URL}`);
      } catch (err) {
        console.error('Falha ao repassar mensagem pro n8n:', err.message);
      }
    }
  });

  return sock;
}

connectToWhatsApp().catch((err) => {
  // Se isso falhar de novo por outro motivo, o servidor continua de pe
  // (o /health mostra o erro) em vez de derrubar o processo inteiro.
  console.error('Falha ao conectar no WhatsApp:', err);
});

// Limitador de velocidade proprio e simples — nao depende de nenhum pacote
// externo. No maximo 5 mensagens por minuto, com um espaco minimo de 4s
// entre cada uma, pra nao mandar tudo de rajada.
let sentTimestamps = [];
let lastSendAt = 0;
const MAX_PER_MINUTE = 5;
const MIN_GAP_MS = 4000;

// Delay de digitacao realista, proporcional ao tamanho da mensagem. O
// indicador de "digitando" em si ja comeca la em cima, assim que a
// mensagem chega — isso aqui so garante um tempinho final antes do envio,
// e re-mostra o "composing" caso os ~10s de validade dele ja tenham
// expirado enquanto a IA processava.
function calcularDelayDigitacao(texto) {
  const MS_POR_CARACTER = 80;    // ajuste pra mais lento/rapido
  const MIN_MS = 1200;           // piso — nem mensagem curta sai instantanea
  const MAX_MS = 8000;           // teto — mensagem longa nao trava o chat
  const jitter = 0.85 + Math.random() * 0.3; // variacao de +-15%, evita padrao fixo
  const bruto = texto.length * MS_POR_CARACTER * jitter;
  return Math.min(Math.max(bruto, MIN_MS), MAX_MS);
}

async function sendWithLimit(jid, content) {
  const now = Date.now();
  sentTimestamps = sentTimestamps.filter((t) => now - t < 60000);
  if (sentTimestamps.length >= MAX_PER_MINUTE) {
    throw new Error(`Limite de ${MAX_PER_MINUTE} mensagens por minuto atingido — espera um pouco.`);
  }
  const gap = now - lastSendAt;
  if (gap < MIN_GAP_MS) {
    await new Promise((r) => setTimeout(r, MIN_GAP_MS - gap));
  }

  // Reforca "digitando..." (garante que esta fresco) e espera o tempinho
  // final antes de soltar a mensagem.
  const texto = content?.text || '';
  await sock.sendPresenceUpdate('composing', jid);
  await new Promise((r) => setTimeout(r, calcularDelayDigitacao(texto)));
  await sock.sendPresenceUpdate('paused', jid);

  await sock.sendMessage(jid, content);
  lastSendAt = Date.now();
  sentTimestamps.push(lastSendAt);
}

// --- HTTP: pra n8n chamar quando quiser mandar uma resposta, e pra voce
// escanear o QR pelo navegador (Railway nao tem terminal visivel facil) ---

const app = express();
app.use(express.json());

app.get('/qr', async (req, res) => {
  if (connectionState === 'conectado') {
    return res.send('<p>Já conectado! Não precisa de QR.</p>');
  }
  if (!currentQR) {
    return res.send(`<p>Ainda preparando a conexão (status: ${connectionState})... espera uns 15-20 segundos e atualiza a página.</p>`);
  }
  const qrImage = await qrcode.toDataURL(currentQR);
  res.send(`
    <div style="font-family: sans-serif; text-align: center; padding: 40px;">
      <h2>Escaneie com o número secundário</h2>
      <p>WhatsApp → Aparelhos conectados → Conectar um aparelho</p>
      <img src="${qrImage}" style="width: 300px; height: 300px;" />
    </div>
  `);
});

app.post('/send', async (req, res) => {
  const { to, text } = req.body || {};
  if (!to || !text) {
    return res.status(400).json({ error: 'Campos "to" e "text" sao obrigatorios' });
  }
  if (!sock) {
    return res.status(503).json({ error: 'WhatsApp ainda nao conectado' });
  }

  try {
    const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
    await sendWithLimit(jid, { text });
    res.json({ status: 'enviado' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/health', (req, res) => {
  if (!sock) return res.json({ status: 'conectando...' });
  res.json({ connectionState, enviosUltimoMinuto: sentTimestamps.length });
});

app.listen(PORT, () => console.log(`Servidor rodando na porta ${PORT}`));
