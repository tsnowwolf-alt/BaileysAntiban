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
import pino from 'pino';

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
const downloadMediaMessage = baileysNS.downloadMediaMessage ?? baileysNS.default?.downloadMediaMessage;

// A downloadMediaMessage exige um logger no formato pino — nao serve so
// console.log. 'silent' porque so queremos os nossos proprios console.log,
// nao o log interno da lib.
const logger = pino({ level: 'silent' });

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

// Seu numero pessoal, o que recebe o aviso quando a IA transfere alguem
// pra atendimento humano. Formato: DDI+DDD+numero, sem "+" nem espaco,
// seguido de "@s.whatsapp.net" — ex: "5511987654321@s.whatsapp.net".
const NUMERO_ATENDENTE = process.env.NUMERO_ATENDENTE;

// Mesma API key do OpenRouter que ja alimenta o node de IA no n8n — aqui
// e usada pra transcrever audio e ler imagem/video/PDF, endpoints
// separados do de chat, mas mesma conta e mesma cobranca.
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;

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

// Quando o remoteJid vem em formato @lid (identificador interno, nao o
// numero de telefone), a Baileys 7.x as vezes manda junto o numero real
// no campo remoteJidAlt. Guardamos aqui quando aparece, pra poder mostrar
// o numero de verdade no alerta de atendimento em vez do @lid cru.
const numeroRealPorJid = new Map();

// Buffer de mensagens por contato — se o cliente mandar varias mensagens
// rapido (texto, audio, imagem, misturado) antes do bot responder, agrupa
// tudo numa unica chamada pro n8n em vez de uma por mensagem. Sem isso,
// cada mensagem disparava sua propria execucao da IA em paralelo, sem uma
// saber da outra, e o cliente recebia varias respostas desencontradas.
const buffersPorContato = new Map(); // jid -> { textos: string[], timer }
const DEBOUNCE_MS = 5000; // espera esse tempo sem mensagem nova do mesmo contato antes de processar — ajuste aqui

function adicionarAoBuffer(jid, texto) {
  const atual = buffersPorContato.get(jid) || { textos: [] };
  atual.textos.push(texto);
  if (atual.timer) clearTimeout(atual.timer);
  atual.timer = setTimeout(() => processarBuffer(jid), DEBOUNCE_MS);
  buffersPorContato.set(jid, atual);
}

async function processarBuffer(jid) {
  const atual = buffersPorContato.get(jid);
  if (!atual) return;
  buffersPorContato.delete(jid);

  const textoCombinado = atual.textos.join('\n');

  if (!FORWARD_TO_URL) {
    console.error('FORWARD_TO_URL nao configurada — mensagem NAO foi repassada pra lugar nenhum. Configure essa variavel no Railway, no servico do Baileys (nao no do n8n).');
    return;
  }

  try {
    await fetch(FORWARD_TO_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: jid,
        text: textoCombinado,
        source: 'baileys-secundario',
        timestamp: Date.now(),
      }),
    });
    console.log(`Buffer de ${jid} repassado pro n8n (${atual.textos.length} mensagem(ns) agrupada(s)). Destino: ${FORWARD_TO_URL}`);
  } catch (err) {
    console.error('Falha ao repassar buffer pro n8n:', err.message);
  }
}

// Baixa o audio (a Baileys cuida de descriptografar) e manda pro endpoint
// de transcricao do OpenRouter. Retorna '' em qualquer falha, em vez de
// derrubar o processo — audio e so mais um jeito de chegar texto, nao pode
// travar o resto do bot se algo aqui der errado.
async function transcreverAudio(msg) {
  if (!downloadMediaMessage) {
    console.error('downloadMediaMessage nao encontrada no pacote baileys — nao da pra transcrever audio.');
    return '';
  }
  if (!OPENROUTER_API_KEY) {
    console.error('OPENROUTER_API_KEY nao configurada — nao da pra transcrever audio. Configure essa variavel no Railway.');
    return '';
  }

  const buffer = await downloadMediaMessage(
    msg,
    'buffer',
    {},
    { logger, reuploadRequest: sock.updateMediaMessage }
  );

  const mimetype = msg.message.audioMessage?.mimetype || '';
  const formato = mimetype.includes('mp4') ? 'mp4'
    : mimetype.includes('mpeg') ? 'mp3'
    : 'ogg'; // nota de voz do WhatsApp (o caso mais comum) e ogg/opus

  const resp = await fetch('https://openrouter.ai/api/v1/audio/transcriptions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'openai/whisper-large-v3',
      input_audio: {
        data: buffer.toString('base64'),
        format: formato,
      },
    }),
  });

  if (!resp.ok) {
    throw new Error(`OpenRouter respondeu ${resp.status}: ${await resp.text()}`);
  }

  const data = await resp.json();
  return data.text || '';
}

// Mesma logica da de audio, mas pra imagem: baixa, manda pro OpenRouter
// (agora no endpoint normal de chat, com um modelo com visao) e volta uma
// descricao curta em texto. Tambem nunca derruba o processo em caso de erro.
async function descreverImagem(msg) {
  if (!downloadMediaMessage) {
    console.error('downloadMediaMessage nao encontrada no pacote baileys — nao da pra ler imagem.');
    return '';
  }
  if (!OPENROUTER_API_KEY) {
    console.error('OPENROUTER_API_KEY nao configurada — nao da pra ler imagem. Configure essa variavel no Railway.');
    return '';
  }

  const buffer = await downloadMediaMessage(
    msg,
    'buffer',
    {},
    { logger, reuploadRequest: sock.updateMediaMessage }
  );

  const mimetype = msg.message.imageMessage?.mimetype || 'image/jpeg';
  const base64Imagem = buffer.toString('base64');

  const resp = await fetch('https://openrouter.ai/api/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'openai/gpt-4o-mini', // barato e confiavel pra descricao simples
      messages: [{
        role: 'user',
        content: [
          {
            type: 'text',
            text: 'Um cliente mandou esta imagem numa conversa de vendas por WhatsApp. Descreva em 1-2 frases curtas, em português, o que aparece de relevante (produto, texto, print de tela, etc).',
          },
          {
            type: 'image_url',
            image_url: { url: `data:${mimetype};base64,${base64Imagem}` },
          },
        ],
      }],
    }),
  });

  if (!resp.ok) {
    throw new Error(`OpenRouter respondeu ${resp.status}: ${await resp.text()}`);
  }

  const data = await resp.json();
  return data.choices?.[0]?.message?.content || '';
}

// A imagem do anuncio nao e um anexo de midia normal (nao passa por
// downloadMediaMessage) — ou vem com os bytes ja embutidos na mensagem
// (adReply.thumbnail) ou so como URL (adReply.thumbnailUrl). Trata os
// dois casos, e retorna '' se nenhum dos dois vier disponivel.
async function descreverImagemAnuncio(adReply) {
  if (!OPENROUTER_API_KEY) {
    console.error('OPENROUTER_API_KEY nao configurada — nao da pra ler imagem do anúncio.');
    return '';
  }

  let base64Imagem;
  if (adReply.thumbnail) {
    base64Imagem = Buffer.isBuffer(adReply.thumbnail)
      ? adReply.thumbnail.toString('base64')
      : Buffer.from(adReply.thumbnail).toString('base64');
  } else if (adReply.thumbnailUrl) {
    const imgResp = await fetch(adReply.thumbnailUrl);
    if (!imgResp.ok) {
      throw new Error(`Falha ao baixar imagem do anúncio: ${imgResp.status}`);
    }
    base64Imagem = Buffer.from(await imgResp.arrayBuffer()).toString('base64');
  } else {
    return ''; // sem imagem disponivel — segue so com titulo/corpo do anuncio
  }

  const resp = await fetch('https://openrouter.ai/api/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'openai/gpt-4o-mini',
      messages: [{
        role: 'user',
        content: [
          {
            type: 'text',
            text: 'Esta é a imagem de um anúncio que um cliente clicou antes de começar essa conversa de vendas por WhatsApp. Descreva em 1-2 frases curtas, em português, o produto (cor, formato, características visíveis, preço se estiver escrito na imagem).',
          },
          {
            type: 'image_url',
            image_url: { url: `data:image/jpeg;base64,${base64Imagem}` },
          },
        ],
      }],
    }),
  });

  if (!resp.ok) {
    throw new Error(`OpenRouter respondeu ${resp.status}: ${await resp.text()}`);
  }

  const data = await resp.json();
  return data.choices?.[0]?.message?.content || '';
}
// Video vai pra um modelo diferente da imagem: gpt-4o-mini nao entende
// video, mas o Gemini 3.5 Flash entende nativamente (imagem, video, audio
// e PDF, tudo no mesmo modelo) — por isso ele aparece so aqui e no PDF,
// nao na imagem (que ja funciona bem e nao precisava trocar).
async function descreverVideo(msg) {
  if (!downloadMediaMessage) {
    console.error('downloadMediaMessage nao encontrada no pacote baileys — nao da pra ler video.');
    return '';
  }
  if (!OPENROUTER_API_KEY) {
    console.error('OPENROUTER_API_KEY nao configurada — nao da pra ler video. Configure essa variavel no Railway.');
    return '';
  }

  const buffer = await downloadMediaMessage(
    msg,
    'buffer',
    {},
    { logger, reuploadRequest: sock.updateMediaMessage }
  );

  const mimetype = msg.message.videoMessage?.mimetype || 'video/mp4';
  const base64Video = buffer.toString('base64');

  const resp = await fetch('https://openrouter.ai/api/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'google/gemini-3.5-flash',
      messages: [{
        role: 'user',
        content: [
          {
            type: 'text',
            text: 'Um cliente mandou este vídeo numa conversa de vendas por WhatsApp. Descreva em 2-3 frases curtas, em português, o que aparece e o que é dito de relevante.',
          },
          {
            type: 'video_url',
            video_url: { url: `data:${mimetype};base64,${base64Video}` },
          },
        ],
      }],
    }),
  });

  if (!resp.ok) {
    throw new Error(`OpenRouter respondeu ${resp.status}: ${await resp.text()}`);
  }

  const data = await resp.json();
  return data.choices?.[0]?.message?.content || '';
}

// So PDF por enquanto — o content type "file" do OpenRouter e documentado
// especificamente pra PDF. Documento que chegar em outro formato (docx,
// xlsx etc) cai no fallback de "arquivo desconhecido" mais abaixo, em vez
// de arriscar um resultado ruim tentando forcar.
async function lerPDF(msg) {
  if (!downloadMediaMessage) {
    console.error('downloadMediaMessage nao encontrada no pacote baileys — nao da pra ler PDF.');
    return '';
  }
  if (!OPENROUTER_API_KEY) {
    console.error('OPENROUTER_API_KEY nao configurada — nao da pra ler PDF. Configure essa variavel no Railway.');
    return '';
  }

  const buffer = await downloadMediaMessage(
    msg,
    'buffer',
    {},
    { logger, reuploadRequest: sock.updateMediaMessage }
  );

  const nomeArquivo = msg.message.documentMessage?.fileName || 'documento.pdf';
  const base64Doc = buffer.toString('base64');

  const resp = await fetch('https://openrouter.ai/api/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'google/gemini-3.5-flash',
      messages: [{
        role: 'user',
        content: [
          {
            type: 'text',
            text: 'Um cliente mandou este documento numa conversa de vendas por WhatsApp. Resuma em 2-3 frases curtas, em português, o conteúdo relevante.',
          },
          {
            type: 'file',
            file: { filename: nomeArquivo, file_data: `data:application/pdf;base64,${base64Doc}` },
          },
        ],
      }],
    }),
  });

  if (!resp.ok) {
    throw new Error(`OpenRouter respondeu ${resp.status}: ${await resp.text()}`);
  }

  const data = await resp.json();
  return data.choices?.[0]?.message?.content || '';
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
      if (msg.key.remoteJidAlt) {
        numeroRealPorJid.set(from, msg.key.remoteJidAlt);
      }

      const temTexto = !!(msg.message.conversation || msg.message.extendedTextMessage?.text);
      const temAudio = !!msg.message.audioMessage;
      const temImagem = !!msg.message.imageMessage;
      const temVideo = !!msg.message.videoMessage;
      const ehDocumentoPDF = msg.message.documentMessage?.mimetype === 'application/pdf';
      // Tipos de arquivo reais que reconhecemos mas nao processamos —
      // ganham a mensagem padrao em vez de silencio. Coisas de protocolo
      // (reacao, recibo, etc) nao entram aqui de proposito, pra nao
      // responder "nao entendi o arquivo" pra um like de emoji.
      const temArquivoDesconhecido = !!(
        (msg.message.documentMessage && !ehDocumentoPDF) || // documento que nao seja PDF (docx, xlsx etc)
        msg.message.stickerMessage ||
        msg.message.contactMessage ||
        msg.message.locationMessage
      );
      if (!temTexto && !temAudio && !temImagem && !temVideo && !ehDocumentoPDF && !temArquivoDesconhecido) continue;

      // Mostra "digitando..." JA — antes de transcrever/descrever ou
      // chamar o n8n/IA, nao depois. Assim nada disso reintroduz aquele
      // silencio antes do indicador aparecer.
      await sock.sendPresenceUpdate('composing', from).catch((e) =>
        console.error('Falha ao mostrar "digitando":', e.message)
      );

      let text = msg.message.conversation
        || msg.message.extendedTextMessage?.text
        || '';

      // Mensagem que chega de um anuncio "Clique para o WhatsApp" (Facebook/
      // Instagram) traz o anuncio anexado em contextInfo.externalAdReply —
      // e como aquele card com o produto que aparece no topo da conversa.
      // Junta titulo, descricao E a imagem (lida pela IA tambem) num unico
      // resumo, assim o bot sabe exatamente qual produto motivou o contato.
      const adReply = msg.message.extendedTextMessage?.contextInfo?.externalAdReply;
      if (text && adReply) {
        const partes = [];
        if (adReply.title) partes.push(`título "${adReply.title}"`);
        if (adReply.body) partes.push(`descrição "${adReply.body}"`);

        const descricaoImagem = await descreverImagemAnuncio(adReply).catch((e) => {
          console.error('Falha ao descrever imagem do anúncio:', e.message);
          return '';
        });
        if (descricaoImagem) partes.push(`imagem mostra: ${descricaoImagem}`);

        if (partes.length) {
          text = `[Cliente veio de um anúncio — ${partes.join('; ')}] ${text}`;
        }
      }

      if (!text && temAudio) {
        console.log(`Áudio recebido de ${from}, transcrevendo...`);
        text = await transcreverAudio(msg).catch((e) => {
          console.error('Falha ao transcrever áudio:', e.message);
          return '';
        });

        if (text) {
          console.log(`Transcrição: "${text}"`);
        } else {
          console.log(`Transcrição vazia ou falhou pra ${from} — avisando o cliente.`);
          await sendWithLimit(from, {
            text: 'Não consegui entender esse áudio — pode escrever em texto, por favor?',
          }).catch((e) => console.error('Falha ao avisar sobre áudio não entendido:', e.message));
          continue;
        }
      }

      if (!text && temImagem) {
        console.log(`Imagem recebida de ${from}, descrevendo...`);
        const descricao = await descreverImagem(msg).catch((e) => {
          console.error('Falha ao descrever imagem:', e.message);
          return '';
        });

        if (descricao) {
          console.log(`Descrição da imagem: "${descricao}"`);
          text = `[Imagem recebida do cliente] ${descricao}`;
        } else {
          await sendWithLimit(from, {
            text: 'Desculpe, não consigo ler esse arquivo. Poderia esclarecer melhor?',
          }).catch((e) => console.error('Falha ao avisar sobre imagem não lida:', e.message));
          continue;
        }
      }

      if (!text && temVideo) {
        console.log(`Vídeo recebido de ${from}, descrevendo...`);
        const descricao = await descreverVideo(msg).catch((e) => {
          console.error('Falha ao descrever vídeo:', e.message);
          return '';
        });

        if (descricao) {
          console.log(`Descrição do vídeo: "${descricao}"`);
          text = `[Vídeo recebido do cliente] ${descricao}`;
        } else {
          await sendWithLimit(from, {
            text: 'Desculpe, não consigo ler esse arquivo. Poderia esclarecer melhor?',
          }).catch((e) => console.error('Falha ao avisar sobre vídeo não lido:', e.message));
          continue;
        }
      }

      if (!text && ehDocumentoPDF) {
        console.log(`PDF recebido de ${from}, lendo...`);
        const resumo = await lerPDF(msg).catch((e) => {
          console.error('Falha ao ler PDF:', e.message);
          return '';
        });

        if (resumo) {
          console.log(`Resumo do PDF: "${resumo}"`);
          text = `[PDF recebido do cliente] ${resumo}`;
        } else {
          await sendWithLimit(from, {
            text: 'Desculpe, não consigo ler esse arquivo. Poderia esclarecer melhor?',
          }).catch((e) => console.error('Falha ao avisar sobre PDF não lido:', e.message));
          continue;
        }
      }

      if (!text && temArquivoDesconhecido) {
        console.log(`Arquivo de tipo não suportado de ${from}, avisando.`);
        await sendWithLimit(from, {
          text: 'Desculpe, não consigo ler esse arquivo. Poderia esclarecer melhor?',
        }).catch((e) => console.error('Falha ao avisar sobre arquivo não suportado:', e.message));
        continue;
      }

      if (!text) continue;

      console.log(`Mensagem recebida de ${from}: "${text}" — adicionando ao buffer.`);
      adicionarAoBuffer(from, text);
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

// Delay manual, aleatorio dentro de uma faixa, so pra primeira mensagem de
// cada contato novo. Ajusta os dois valores abaixo (em segundos) pra
// definir o range; soma com o delay de digitacao normal (automatico,
// baseado no tamanho de CADA mensagem).
const DELAY_PRIMEIRA_MENSAGEM_MIN_S = 3; // <- minimo, em segundos
const DELAY_PRIMEIRA_MENSAGEM_MAX_S = 8; // <- maximo, em segundos
const contatosJaRespondidos = new Set();

function delayPrimeiraMensagemMs() {
  const minMs = DELAY_PRIMEIRA_MENSAGEM_MIN_S * 1000;
  const maxMs = DELAY_PRIMEIRA_MENSAGEM_MAX_S * 1000;
  return minMs + Math.random() * (maxMs - minMs);
}

// Fila de envio — garante que cada envio (com seu delay de digitacao)
// termine antes do proximo comecar, mesmo se varios clientes mandarem
// mensagem ao mesmo tempo e o n8n chamar /send quase simultaneamente pra
// todos. Sem isso, envios concorrentes corriam por cima um do outro:
// cada um lia lastSendAt antes do outro atualizar, e o rate limiter
// perdia o efeito bem na hora que mais importava (varios clientes juntos).
let filaDeEnvio = Promise.resolve();

function proximoDaFila(tarefa) {
  const resultado = filaDeEnvio.then(tarefa, tarefa);
  filaDeEnvio = resultado.catch(() => {}); // fila segue mesmo se uma tarefa falhar
  return resultado;
}

async function sendWithLimit(jid, content) {
  return proximoDaFila(() => enviarComRateLimit(jid, content));
}

async function enviarComRateLimit(jid, content) {
  const now = Date.now();
  sentTimestamps = sentTimestamps.filter((t) => now - t < 60000);
  if (sentTimestamps.length >= MAX_PER_MINUTE) {
    throw new Error(`Limite de ${MAX_PER_MINUTE} mensagens por minuto atingido — espera um pouco.`);
  }
  const gap = now - lastSendAt;
  if (gap < MIN_GAP_MS) {
    await new Promise((r) => setTimeout(r, MIN_GAP_MS - gap));
  }

  if (!contatosJaRespondidos.has(jid)) {
    await new Promise((r) => setTimeout(r, delayPrimeiraMensagemMs()));
    contatosJaRespondidos.add(jid);
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

// Aviso pro seu proprio numero quando a IA transfere alguem pra atendimento
// humano. Nao passa pelo rate limiter nem pelo delay de digitacao — aquilo
// e pensado pra mensagem de cliente, isso aqui e aviso interno pra voce.
async function enviarAlertaAtendente(clienteJid) {
  if (!NUMERO_ATENDENTE) {
    console.error('NUMERO_ATENDENTE nao configurada — alerta de atendimento NAO foi enviado. Configure essa variavel no Railway.');
    return;
  }
  const numeroReal = numeroRealPorJid.get(clienteJid);
  const identificacao = numeroReal
    ? `${numeroReal.split('@')[0]} (${clienteJid})`
    : clienteJid;
  try {
    await sock.sendMessage(NUMERO_ATENDENTE, {
      text: `🚨 Atendimento solicitado!\n\nCliente: ${identificacao}\n\nO bot já avisou que alguém vai te atender — responde direto na conversa com esse número.`,
    });
    console.log(`Alerta de atendimento enviado (cliente: ${identificacao}).`);
  } catch (err) {
    console.error('Falha ao enviar alerta de atendimento:', err.message);
  }
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
  const { to, text, alertaAtendente } = req.body || {};
  if (!to || !text) {
    return res.status(400).json({ error: 'Campos "to" e "text" sao obrigatorios' });
  }
  if (!sock) {
    return res.status(503).json({ error: 'WhatsApp ainda nao conectado' });
  }

  try {
    const jid = to.includes('@') ? to : `${to}@s.whatsapp.net`;
    await sendWithLimit(jid, { text });

    if (alertaAtendente) {
      await enviarAlertaAtendente(jid);
    }

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
