// Framework HTTP para Bun/TypeScript, similar a Express
import { Elysia } from "elysia";
// Cliente oficial de Redis para Node.js/Bun
import { createClient } from "redis";
import type { RedisClientType } from "redis";
import {
  buildNoToolCallRetryMessage,
  classifyBrowserBlock,
  parseBrowserToolCallFromText,
} from "./browser-agent-control";

// Argumento de un comando Redis: string o datos binarios
type RedisArg = string | Buffer;
// Respuesta Redis: puede ser escalar, null, binario, o array recursivo
type RedisReply = string | number | bigint | null | Buffer | RedisReply[];
// Un comando Redis es un array de argumentos, ej: ["SET", "clave", "valor"]
type Command = RedisArg[];
// Respuesta estándar de la API REST: siempre tiene `result` o `error`
type RestResult = { result: unknown } | { error: string };
// Bucket de rate limit por cliente: peticiones hechas y cuándo se reinicia
type RateLimitBucket = { count: number; resetAt: number };

// Decodificador UTF-8 reutilizable para convertir Buffer a string
const textDecoder = new TextDecoder();
// Métodos HTTP aceptados; DELETE/PATCH/OPTIONS son rechazados con 405
const allowedMethods = new Set(["GET", "POST", "PUT", "HEAD"]);

// Ventana de tiempo del rate limit en milisegundos (default: 60s)
const rateLimitWindowMs = Number(process.env.RATE_LIMIT_WINDOW_SECONDS ?? 60) * 1000;
// Máximo de peticiones permitidas por ventana por cliente (default: 600)
const rateLimitMaxRequests = Number(process.env.RATE_LIMIT_MAX_REQUESTS ?? 600);
// Máximo de comandos en un /pipeline o /multi-exec (default: 100)
const maxBatchCommands = Number(process.env.MAX_BATCH_COMMANDS ?? 100);
// Máximo de argumentos por comando individual (default: 256)
const maxCommandArgs = Number(process.env.MAX_COMMAND_ARGS ?? 256);
// Tamaño máximo de un argumento individual (default: 512 KB)
const maxArgBytes = Number(process.env.MAX_ARG_BYTES ?? 512 * 1024);
// Tamaño máximo del body HTTP completo (default: 1 MB)
const maxRequestBytes = Number(process.env.MAX_REQUEST_BYTES ?? 1024 * 1024);
// Timeout máximo por comando Redis antes de cancelarlo (default: 5s)
const commandTimeoutMs = Number(process.env.COMMAND_TIMEOUT_MS ?? 5_000);
// Máximo de comandos Redis ejecutándose en paralelo; protege contra sobrecarga (default: 50)
const maxInFlightCommands = Number(process.env.MAX_IN_FLIGHT_COMMANDS ?? 50);
// Cola interna del cliente Redis; 10x el límite de in-flight para absorber ráfagas
const redisCommandQueueMaxLength = Number(process.env.REDIS_COMMAND_QUEUE_MAX_LENGTH ?? maxInFlightCommands * 10);
// Cuando es true, habilita comandos peligrosos como FLUSHDB, CONFIG, SHUTDOWN (default: false)
const allowDangerousCommands = process.env.OKMANUS_ALLOW_DANGEROUS_COMMANDS === "true";
const productClientToken = process.env.OKMANUS_PRODUCT_CLIENT_TOKEN;
const appwriteEndpoint = process.env.APPWRITE_ENDPOINT?.replace(/\/$/, "");
const appwriteProjectId = process.env.APPWRITE_PROJECT_ID;
const requireAppwriteSession = process.env.OKMANUS_REQUIRE_APPWRITE_SESSION === "true";
const openRouterApiKey = process.env.OPENROUTER_API_KEY;
const openRouterModel = process.env.OPENROUTER_MODEL ?? "openrouter/auto";
const groqApiKey = process.env.GROQ_API_KEY;
// gpt-oss-120b reliably emits real `tool_calls` when tool_choice="required";
// llama-3.3-70b on Groq tends to narrate the call in text instead.
const groqModel = process.env.GROQ_MODEL ?? "openai/gpt-oss-120b";
const browserlessApiKey = process.env.BROWSERLESS_API_KEY;
const browserlessEndpoint = (process.env.BROWSERLESS_ENDPOINT ?? "https://production-sfo.browserless.io").replace(/\/$/, "");
const mcpRegistryTtlSeconds = Number(process.env.MCP_REGISTRY_TTL_SECONDS ?? 3600);
const mcpHeartbeatTtlSeconds = Number(process.env.MCP_HEARTBEAT_TTL_SECONDS ?? 90);
const mcpSessionTtlSeconds = Number(process.env.MCP_SESSION_TTL_SECONDS ?? 3600);
const credentialConsentTtlSeconds = Number(process.env.CREDENTIAL_CONSENT_TTL_SECONDS ?? 30 * 24 * 3600);
const okmanusJobTtlSeconds = Number(process.env.OKMANUS_JOB_TTL_SECONDS ?? 7 * 24 * 3600);
const okmanusSessionQueueTtlSeconds = Number(process.env.OKMANUS_SESSION_QUEUE_TTL_SECONDS ?? 24 * 3600);
const okmanusQueueMaxJobs = Number(process.env.OKMANUS_QUEUE_MAX_JOBS ?? 1000);
const productCorsHeaders: HeadersInit = {
  "access-control-allow-origin": process.env.OKMANUS_ALLOWED_ORIGIN ?? "*",
  "access-control-allow-methods": "GET,POST,OPTIONS",
  "access-control-allow-headers": "authorization,content-type,x-appwrite-jwt",
  "access-control-max-age": "86400",
};

// URL de conexión Redis: usa REDIS_URL si existe; si no, la construye desde variables individuales
// encodeURIComponent en usuario/password para manejar caracteres especiales como @, :, /
const redisUrl =
  process.env.REDIS_URL ??
  `redis://${process.env.REDIS_USERNAME ? `${encodeURIComponent(process.env.REDIS_USERNAME)}:` : ""}${
    process.env.REDIS_PASSWORD ? `${encodeURIComponent(process.env.REDIS_PASSWORD)}@` : ""
  }${process.env.REDIS_HOST ?? "localhost"}:${process.env.REDIS_PORT ?? "6379"}/${process.env.REDIS_DB ?? "0"}`;

// Token con acceso total; acepta 3 nombres de variable distintos para compatibilidad con Upstash
// ADVERTENCIA: el default "change-me" es inseguro; debe cambiarse en producción
const standardToken =
  process.env.UPSTASH_REDIS_REST_TOKEN ??
  process.env.OKMANUS_REDIS_REST_TOKEN ??
  process.env.API_KEY ??
  "change-me";

// Token opcional de solo lectura; si no se configura, no existe y la función queda desactivada
const readOnlyToken =
  process.env.UPSTASH_REDIS_REST_READONLY_TOKEN ??
  process.env.OKMANUS_REDIS_REST_READONLY_TOKEN;

// Lista blanca de comandos Redis de solo lectura permitidos con el readOnlyToken
// Incluye GET, SCAN, TTL, INFO y sus equivalentes para hashes, sets, sorted sets y streams
const readOnlyCommands = new Set([
  "BITCOUNT",
  "BITFIELD_RO",
  "BITPOS",
  "DBSIZE",
  "DUMP",
  "ECHO",
  "EXISTS",
  "EXPIRETIME",
  "GEODIST",
  "GEOHASH",
  "GEOPOS",
  "GEORADIUS_RO",
  "GEORADIUSBYMEMBER_RO",
  "GEOSEARCH",
  "GET",
  "GETBIT",
  "GETRANGE",
  "HEXISTS",
  "HGET",
  "HGETALL",
  "HKEYS",
  "HLEN",
  "HMGET",
  "HRANDFIELD",
  "HSCAN",
  "HSTRLEN",
  "HVALS",
  "INFO",
  "JSON.GET",
  "JSON.MGET",
  "JSON.OBJKEYS",
  "JSON.OBJLEN",
  "JSON.RESP",
  "JSON.STRAPPEND",
  "JSON.STRLEN",
  "KEYS",
  "LINDEX",
  "LLEN",
  "LPOS",
  "LRANGE",
  "MGET",
  "PING",
  "PTTL",
  "RANDOMKEY",
  "SCAN",
  "SCARD",
  "SDIFF",
  "SINTER",
  "SINTERCARD",
  "SISMEMBER",
  "SMEMBERS",
  "SMISMEMBER",
  "SORT_RO",
  "SRANDMEMBER",
  "SSCAN",
  "STRLEN",
  "SUNION",
  "TIME",
  "TTL",
  "TYPE",
  "XINFO",
  "XLEN",
  "XPENDING",
  "XRANGE",
  "XREAD",
  "XREVRANGE",
  "ZCARD",
  "ZCOUNT",
  "ZDIFF",
  "ZINTER",
  "ZINTERCARD",
  "ZLEXCOUNT",
  "ZMSCORE",
  "ZRANDMEMBER",
  "ZRANGE",
  "ZRANGEBYLEX",
  "ZRANGEBYSCORE",
  "ZRANK",
  "ZREVRANGE",
  "ZREVRANGEBYLEX",
  "ZREVRANGEBYSCORE",
  "ZREVRANK",
  "ZSCAN",
  "ZSCORE",
  "ZUNION",
]);

// Comandos bloqueados siempre: requieren conexión persistente (SUBSCRIBE, BLPOP)
// o estado de transacción manual (WATCH, DISCARD), incompatibles con REST stateless
const unsupportedCommands = new Set([
  "BLMOVE",
  "BLMPOP",
  "BLPOP",
  "BRPOP",
  "BRPOPLPUSH",
  "BZMPOP",
  "BZPOPMAX",
  "BZPOPMIN",
  "DISCARD",
  "MONITOR",
  "SUBSCRIBE",
  "UNSUBSCRIBE",
  "PSUBSCRIBE",
  "PUNSUBSCRIBE",
  "SSUBSCRIBE",
  "SUNSUBSCRIBE",
  "WATCH",
  "UNWATCH",
]);

// Comandos administrativos peligrosos bloqueados por defecto
// Se habilitan con OKMANUS_ALLOW_DANGEROUS_COMMANDS=true
const dangerousCommands = new Set([
  "ACL",
  "BGREWRITEAOF",
  "BGSAVE",
  "CLIENT",
  "CLUSTER",
  "COMMAND",
  "CONFIG",
  "DEBUG",
  "FAILOVER",
  "FLUSHALL",
  "FLUSHDB",
  "KEYS",
  "LASTSAVE",
  "LATENCY",
  "LOLWUT",
  "MEMORY",
  "MODULE",
  "PSYNC",
  "REPLICAOF",
  "RESTORE",
  "SAVE",
  "SCRIPT",
  "SHUTDOWN",
  "SLAVEOF",
  "SLOWLOG",
  "SYNC",
]);

// Map en memoria: clave = identidad del cliente, valor = bucket de rate limit
const rateLimits = new Map<string, RateLimitBucket>();
// Contador global de comandos Redis actualmente en ejecución
let inFlightCommands = 0;

// Limpieza periódica del Map de rate limits para evitar memory leak
// .unref() evita que este timer impida que el proceso termine limpiamente
setInterval(() => {
  const now = Date.now();
  for (const [key, bucket] of rateLimits) {
    if (bucket.resetAt <= now) rateLimits.delete(key);
  }
}, Math.max(rateLimitWindowMs, 1_000)).unref();

// Crea el cliente Redis con configuración robusta:
// - disableOfflineQueue: falla inmediato si Redis no está conectado (no acumula peticiones)
// - reconnectStrategy: backoff exponencial, máximo 500ms entre reintentos
// - pingInterval: keepalive cada 30s para detectar conexiones muertas
const redis = createClient({
  url: redisUrl,
  commandsQueueMaxLength: redisCommandQueueMaxLength,
  disableOfflineQueue: true,
  socket: {
    reconnectStrategy: (retries) => Math.min(retries * 50, 500),
  },
  pingInterval: 30_000,
}) as RedisClientType;

// Log de errores Redis sin crashear el proceso
redis.on("error", (error) => {
  console.error("Redis error:", error);
});

// Conexión inicial; await en top-level (soportado en Bun y Node.js ESM moderno)
await redis.connect();

async function seedPlatformRegistry() {
  const manifest = buildBootstrapManifest();
  await redis
    .multi()
    .set("okm:registry:bootstrap", JSON.stringify(manifest), { EX: 3600 })
    .set(
      "okm:registry:providers",
      JSON.stringify({
        providers: manifest.providers,
        source: "redis-okmanus",
        updatedAt: new Date().toISOString(),
      }),
      { EX: 3600 },
    )
    .set(
      "okm:computer:active",
      JSON.stringify({
        providerId: "browserless-computer",
        vendor: "browserless",
        status: browserlessApiKey ? "configured" : "missing_credentials",
        updatedAt: new Date().toISOString(),
      }),
      { EX: 3600 },
    )
    .exec();
}

await seedPlatformRegistry();

// Extrae el token del request: primero busca "Authorization: Bearer <token>",
// luego en query string ?_token= o ?token= (compatibilidad con Upstash SDK)
// Parseo manual de query string para evitar alojar un objeto URL por cada petición
function getToken(request: Request) {
  const authorization = request.headers.get("authorization");
  if (authorization?.length) {
    const separatorIndex = authorization.indexOf(" ");
    if (separatorIndex > 0 && authorization.slice(0, separatorIndex).toLowerCase() === "bearer") {
      const bearer = authorization.slice(separatorIndex + 1).trim();
      if (bearer) return bearer;
    }
  }

  const qIdx = request.url.indexOf("?");
  if (qIdx === -1) return null;

  const qs = request.url.slice(qIdx + 1);
  for (const pair of qs.split("&")) {
    const eqIdx = pair.indexOf("=");
    if (eqIdx === -1) continue;
    const key = pair.slice(0, eqIdx);
    if (key === "_token" || key === "token") {
      return decodeURIComponent(pair.slice(eqIdx + 1));
    }
  }
  return null;
}

// Genera la clave de identidad del cliente para el rate limit
// Usa el token si existe (más justo), o la IP del header de proxy
// Parseo manual de x-forwarded-for para evitar split() + trim() innecesarios
function getClientIdentity(request: Request) {
  const token = getToken(request);
  if (token) return `token:${token}`;
  const forwardedFor = request.headers.get("x-forwarded-for");
  if (forwardedFor) return `ip:${forwardedFor.slice(0, forwardedFor.indexOf(",") >>> 0).trim() || forwardedFor}`;
  const realIp = request.headers.get("x-real-ip");
  return `ip:${realIp?.trim() ?? "unknown"}`;
}

// Implementa token bucket: si el bucket expiró, crea uno nuevo y permite.
// Si el bucket existe y supera el máximo, rechaza con 429.
// cost > 1 para pipelines (cada comando en el batch cuenta como una petición)
function checkRateLimit(request: Request, cost = 1) {
  const now = Date.now();
  const key = getClientIdentity(request);
  const bucket = rateLimits.get(key);

  if (!bucket || bucket.resetAt <= now) {
    rateLimits.set(key, { count: cost, resetAt: now + rateLimitWindowMs });
    return {
      ok: true as const,
      limit: rateLimitMaxRequests,
      remaining: Math.max(rateLimitMaxRequests - cost, 0),
      resetAt: now + rateLimitWindowMs,
    };
  }

  if (bucket.count + cost > rateLimitMaxRequests) {
    return {
      ok: false as const,
      status: 429,
      error: "ERR rate limit exceeded",
      limit: rateLimitMaxRequests,
      remaining: 0,
      resetAt: bucket.resetAt,
    };
  }

  bucket.count += cost;
  return {
    ok: true as const,
    limit: rateLimitMaxRequests,
    remaining: Math.max(rateLimitMaxRequests - bucket.count, 0),
    resetAt: bucket.resetAt,
  };
}

// Genera los headers estándar de rate limit para incluir en toda respuesta
// Retry-After solo se agrega cuando la petición fue rechazada (429)
function rateLimitHeaders(rateLimit: ReturnType<typeof checkRateLimit>): HeadersInit {
  const headers: Record<string, string> = {
    "x-ratelimit-limit": String(rateLimit.limit),
    "x-ratelimit-remaining": String(rateLimit.remaining),
    "x-ratelimit-reset": String(Math.ceil(rateLimit.resetAt / 1000)),
  };
  if (!rateLimit.ok) {
    headers["retry-after"] = String(Math.ceil((rateLimit.resetAt - Date.now()) / 1000));
  }
  return headers;
}

// Verifica autenticación y permisos:
// 1. Sin token → 401
// 2. Token estándar → acceso total
// 3. Token de solo lectura → solo si todos los comandos están en readOnlyCommands
// 4. Token inválido → 401
function authorize(request: Request, commandNames: string[] = []) {
  const token = getToken(request);
  if (!token) {
    return { ok: false as const, status: 401, error: "Unauthorized: missing token" };
  }

  if (token === standardToken) {
    return { ok: true as const, readonly: false };
  }

  if (readOnlyToken && token === readOnlyToken) {
    // Busca el primer comando que no esté en la lista blanca de solo lectura
    const deniedCommand = commandNames.find((name) => !readOnlyCommands.has(name.toUpperCase()));
    if (deniedCommand) {
      return {
        ok: false as const,
        status: 401,
        error: `Unauthorized: read only token can not execute ${deniedCommand.toUpperCase()}`,
      };
    }

    return { ok: true as const, readonly: true };
  }

  return { ok: false as const, status: 401, error: "Unauthorized: invalid token" };
}

function isProductRequest(request: Request) {
  return new URL(request.url).pathname.startsWith("/v1/");
}

function authorizeProductRequest(request: Request) {
  const token = getToken(request);
  if (!token) return { ok: false as const, status: 401, error: "Unauthorized: missing token" };
  if (productClientToken && token === productClientToken) return { ok: true as const };
  if (token === standardToken || (readOnlyToken && token === readOnlyToken)) return { ok: true as const };
  return { ok: false as const, status: 401, error: "Unauthorized: invalid token" };
}

function getAppwriteJwt(request: Request) {
  return request.headers.get("x-appwrite-jwt")?.trim() ?? "";
}

async function verifyAppwriteJwt(request: Request) {
  const jwt = getAppwriteJwt(request);
  if (!appwriteEndpoint || !appwriteProjectId) {
    return {
      ok: !requireAppwriteSession,
      user: null,
      error: "Appwrite auth is not configured",
      status: 503,
    };
  }

  if (!jwt) {
    return {
      ok: !requireAppwriteSession,
      user: null,
      error: "Unauthorized: missing Appwrite session",
      status: 401,
    };
  }

  const response = await fetch(`${appwriteEndpoint}/account`, {
    headers: {
      "x-appwrite-project": appwriteProjectId,
      "x-appwrite-jwt": jwt,
    },
  });

  const data = await response.json().catch(() => null);
  if (!response.ok) {
    return {
      ok: false,
      user: null,
      error: data?.message ?? "Unauthorized: invalid Appwrite session",
      status: response.status === 401 ? 401 : 403,
    };
  }

  return { ok: true, user: data, error: "", status: 200 };
}

async function requireAppwriteUser(request: Request) {
  const auth = await verifyAppwriteJwt(request);
  if (!auth.ok) return auth;
  const user = auth.user as Record<string, unknown> | null;
  const userId = typeof user?.$id === "string" ? user.$id : "";
  if (!userId) {
    return {
      ok: false,
      user: null,
      error: "Appwrite session is required for credential consent",
      status: appwriteEndpoint && appwriteProjectId ? 401 : 503,
    };
  }

  return { ok: true, user, userId, error: "", status: 200 };
}

// Convierte un valor JSON crudo a un array de strings/Buffers válido para Redis
// Normaliza números, booleans y bigints a string; objetos complejos a JSON string
function normalizeCommand(command: unknown): Command {
  if (!Array.isArray(command) || command.length === 0) {
    throw new Error("ERR command must be a non-empty JSON array");
  }

  return command.map((value) => {
    if (typeof value === "string") return value;
    if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
      return String(value);
    }
    if (value === null) return "null";
    if (Buffer.isBuffer(value)) return value;
    return JSON.stringify(value);
  });
}

// Añade parámetros de query string como argumentos extra al comando
// Omite _token y token para no contaminar el comando con credenciales
// Parseo manual para evitar alojar objeto URL completo
function appendQueryArguments(command: Command, request: Request) {
  const qIdx = request.url.indexOf("?");
  if (qIdx === -1) return;

  const qs = request.url.slice(qIdx + 1);
  for (const pair of qs.split("&")) {
    const eqIdx = pair.indexOf("=");
    if (eqIdx === -1) continue;
    const key = pair.slice(0, eqIdx);
    if (key === "_token" || key === "token") continue;
    command.push(decodeURIComponent(key), decodeURIComponent(pair.slice(eqIdx + 1)));
  }
}

// Verificación rápida por Content-Length antes de leer el body
// Evita leer bodies gigantes innecesariamente
function assertRequestSize(request: Request) {
  const contentLength = request.headers.get("content-length");
  if (contentLength && Number(contentLength) > maxRequestBytes) {
    throw new Error(`ERR request body too large. Max ${maxRequestBytes} bytes`);
  }
}

// Verificación del tamaño real del body ya leído (Content-Length puede ser mentira)
function assertBodySize(bodyText: string) {
  if (Buffer.byteLength(bodyText) > maxRequestBytes) {
    throw new Error(`ERR request body too large. Max ${maxRequestBytes} bytes`);
  }
}

// Lee el body JSON con doble verificación de tamaño (header + contenido real)
// Rechaza bodies vacíos antes de intentar parsear
async function parseJsonBodyWithLimit(request: Request): Promise<unknown> {
  assertRequestSize(request);
  const bodyText = await request.text();
  assertBodySize(bodyText);
  if (!bodyText.trim()) {
    throw new Error("ERR request body must not be empty");
  }

  return JSON.parse(bodyText);
}

// Parsea el comando desde la URL y/o el body según la variante de request:
// - Sin path + body JSON → comando viene del body: POST / con body ["SET","k","v"]
// - Con path → comando viene de la URL: GET /SET/k o POST /SET/k con body=valor
// - Query string args se agregan al final del comando (excepto _token/token)
async function parseCommandFromRequest(request: Request, splat?: string): Promise<Command> {
  assertRequestSize(request);

  // Divide el path en segmentos y decodifica URL encoding (%20, etc.)
  const pathParts = (splat ?? "")
    .split("/")
    .filter(Boolean)
    .map((part) => decodeURIComponent(part));

  // GET y HEAD no tienen body; para otros métodos intenta leerlo
  const mayHaveBody = request.method !== "GET" && request.method !== "HEAD";
  const bodyText = mayHaveBody ? await request.text() : "";
  assertBodySize(bodyText);

  const contentType = request.headers.get("content-type") ?? "";

  if (pathParts.length === 0) {
    // Sin path: el body completo ES el comando (array JSON)
    if (!bodyText.trim()) {
      throw new Error("ERR missing command");
    }

    const body = JSON.parse(bodyText);
    const command = normalizeCommand(body);
    appendQueryArguments(command, request);
    return command;
  }

  // Con path: el primer segmento es el nombre del comando, los siguientes son args
  const command: Command = [...pathParts];
  if (bodyText.length > 0 && !contentType.toLowerCase().includes("application/json")) {
    // Body no-JSON se trata como el último argumento del comando (ej: valor raw a SET)
    command.push(bodyText);
  } else if (bodyText.length > 0 && pathParts.length > 0) {
    // Body JSON con path también se agrega como argumento
    command.push(bodyText);
  }

  appendQueryArguments(command, request);
  return command;
}

// Parsea un array de comandos para /pipeline y /multi-exec
// Valida que sea un array y que no supere el máximo permitido
function parseCommandBatch(body: unknown): Command[] {
  if (!Array.isArray(body)) {
    throw new Error("ERR request body must be a JSON array");
  }

  if (body.length > maxBatchCommands) {
    throw new Error(`ERR too many commands in batch. Max ${maxBatchCommands}`);
  }

  return body.map(normalizeCommand);
}

// Extrae el nombre del comando (primer elemento) y lo convierte a mayúsculas
function commandName(command: Command) {
  return String(command[0] ?? "").toUpperCase();
}

// Valida un comando antes de enviarlo a Redis:
// - Nombre presente
// - Número de argumentos dentro del límite
// - Ningún argumento supera el tamaño máximo
// - No está en la lista de no soportados ni en la de peligrosos (si no están habilitados)
function validateCommand(command: Command) {
  const name = commandName(command);
  if (!name) {
    throw new Error("ERR missing command name");
  }

  if (command.length > maxCommandArgs) {
    throw new Error(`ERR too many command arguments. Max ${maxCommandArgs}`);
  }

  for (const arg of command) {
    const size = Buffer.isBuffer(arg) ? arg.byteLength : Buffer.byteLength(arg as string);
    if (size > maxArgBytes) {
      throw new Error(`ERR command argument too large. Max ${maxArgBytes} bytes`);
    }
  }

  if (unsupportedCommands.has(name)) {
    throw new Error(`ERR unsupported command '${name}'`);
  }

  if (!allowDangerousCommands && dangerousCommands.has(name)) {
    throw new Error(`ERR command '${name}' is disabled by server policy`);
  }
}

// Convierte una respuesta Redis a un valor serializable como JSON
// - Buffer → string UTF-8 o base64 si el cliente lo pidió con header Upstash-Encoding: base64
// - bigint → number (JSON no soporta bigint nativo)
// - Array → recursivo
function toJsonValue(reply: RedisReply, base64: boolean): unknown {
  if (Buffer.isBuffer(reply)) {
    return base64 ? reply.toString("base64") : textDecoder.decode(reply);
  }

  if (Array.isArray(reply)) {
    return reply.map((item) => toJsonValue(item, base64));
  }

  if (typeof reply === "bigint") {
    return Number(reply);
  }

  return reply;
}

// Envuelve la respuesta Redis en el formato { result: ... } de la API REST
// Detecta si el cliente pidió encoding base64 vía header Upstash-Encoding
function formatResult(reply: RedisReply, request: Request): RestResult {
  return {
    result: toJsonValue(reply, request.headers.get("upstash-encoding")?.toLowerCase() === "base64"),
  };
}

// Normaliza cualquier error a { error: "mensaje" } para la respuesta JSON
function formatError(error: unknown): { error: string } {
  const message = error instanceof Error ? error.message : String(error);
  return { error: message };
}

// Ejecuta un comando Redis individual con control de concurrencia y timeout
// inFlightCommands se decrementa en finally para garantizar limpieza aunque haya error
async function executeCommand(command: Command): Promise<RedisReply> {
  validateCommand(command);

  if (inFlightCommands >= maxInFlightCommands) {
    throw new Error("ERR server is busy. Retry later");
  }

  inFlightCommands += 1;
  try {
    return await withTimeout(redis.sendCommand(command) as Promise<RedisReply>);
  } finally {
    inFlightCommands -= 1;
  }
}

// Ejecuta múltiples comandos en paralelo sin garantía atómica (pipeline)
// Comandos inválidos devuelven error individual sin cancelar el resto
// El contador de in-flight solo cuenta los comandos válidos
async function executePipeline(commands: Command[]): Promise<RestResult[]> {
  const slots = commands.map((command) => {
    try {
      validateCommand(command);
      return { command };
    } catch (error) {
      return { error };
    }
  });

  const commandCount = slots.filter((slot): slot is { command: Command } => "command" in slot).length;
  if (inFlightCommands + commandCount > maxInFlightCommands) {
    return commands.map(() => formatError("ERR server is busy. Retry later"));
  }

  inFlightCommands += commandCount;
  try {
    const replies = await Promise.all(
      slots.map(async (slot) => {
        if ("error" in slot) return formatError(slot.error);

        try {
          const reply = await withTimeout(redis.sendCommand(slot.command) as Promise<RedisReply>);
          return { result: toJsonValue(reply, false) };
        } catch (error) {
          return formatError(error);
        }
      }),
    );

    return replies;
  } finally {
    inFlightCommands -= commandCount;
  }
}

// Ejecuta múltiples comandos como transacción atómica Redis (MULTI/EXEC)
// Si un comando falla la validación, toda la transacción se cancela
// Si Redis falla el EXEC, devuelve un único { error } en vez de array
async function executeTransaction(commands: Command[]): Promise<RestResult[] | { error: string }> {
  try {
    for (const command of commands) validateCommand(command);

    if (inFlightCommands + commands.length > maxInFlightCommands) {
      throw new Error("ERR server is busy. Retry later");
    }

    inFlightCommands += commands.length;
    const multi = redis.multi();
    for (const command of commands) {
      multi.addCommand(command);
    }

    try {
      const replies = await withTimeout(multi.exec() as Promise<RedisReply[]>);
      return replies.map((reply) => ({ result: toJsonValue(reply, false) }));
    } finally {
      inFlightCommands -= commands.length;
    }
  } catch (error) {
    return formatError(error);
  }
}

// Envuelve una promesa con un timeout; rechaza con error descriptivo si se agota
// clearTimeout en finally garantiza que el timer no quede colgado en memoria
function withTimeout<T>(promise: Promise<T>): Promise<T> {
  let timeout: ReturnType<typeof setTimeout>;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => reject(new Error(`ERR command timed out after ${commandTimeoutMs}ms`)), commandTimeoutMs);
  });

  return Promise.race([promise, timeoutPromise]).finally(() => clearTimeout(timeout));
}

// Construye una Response JSON con headers mínimos de seguridad
// cache-control: no-store evita que proxies cacheen datos de Redis
function makeJsonResponse(body: unknown, status = 200, headers?: HeadersInit) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      ...productCorsHeaders,
      ...headers,
    },
  });
}

async function readJsonObject(request: Request): Promise<Record<string, unknown>> {
  const body = await parseJsonBodyWithLimit(request);
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new Error("ERR request body must be a JSON object");
  }
  return body as Record<string, unknown>;
}

function stringField(value: unknown, fallback: string) {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

const DEFAULT_BROWSER_START_URL = "https://www.google.com";

function numberField(value: unknown, fallback: number, min: number, max: number) {
  const number = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(number)) return fallback;
  return Math.min(Math.max(Math.floor(number), min), max);
}

function stringArrayField(value: unknown) {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string" && item.trim().length > 0).map((item) => item.trim());
}

function optionalStringField(value: unknown) {
  return typeof value === "string" ? value.trim() : "";
}

function jsonField(value: unknown) {
  return JSON.stringify(value && typeof value === "object" ? value : {});
}

function parseRedisHash(reply: RedisReply): Record<string, string> {
  const fields = parseRedisArray(reply);
  const record: Record<string, string> = {};
  for (let index = 0; index < fields.length; index += 2) {
    const key = fields[index];
    if (key) record[key] = fields[index + 1] ?? "";
  }
  return record;
}

function extractEmail(value: string) {
  return value.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)?.[0] ?? "";
}

function browserlessFunctionCode() {
  return `
export default async ({ page, context }) => {
  const targetUrl = context.url || "${DEFAULT_BROWSER_START_URL}";
  const action = context.action || "navigate";
  const selector = context.selector || "";
  const text = context.text || context.email || "";
  const extractSelector = context.extractSelector || "body";
  const result = {
    action,
    url: targetUrl,
    finalUrl: targetUrl,
    title: "",
    extracted: "",
    blockedReason: "",
    blockedDetails: "",
    clicked: false,
    typed: false,
    error: null,
    // legacy compat
    emailTyped: false,
  };

  try {
    await page.setViewport({ width: 1365, height: 768, deviceScaleFactor: 1 });

    if (action !== "screenshot") {
      await page.goto(targetUrl, { waitUntil: "networkidle2", timeout: 30000 });
    }

    if (action === "navigate") {
      // already navigated above
    } else if (action === "click") {
      await page.waitForSelector(selector, { visible: true, timeout: 8000 });
      await page.click(selector);
      result.clicked = true;
    } else if (action === "type") {
      await page.waitForSelector(selector, { visible: true, timeout: 8000 });
      await page.click(selector, { clickCount: 3 });
      await page.type(selector, text, { delay: 18 });
      result.typed = true;
    } else if (action === "type-email") {
      // legacy: try common email input selectors
      const emailSelectors = [
        'input[type="email"]',
        'input[name="identifier"]',
        'input[autocomplete="username"]',
        'input[id="identifierId"]',
      ];
      let typed = false;
      for (const sel of emailSelectors) {
        try {
          await page.waitForSelector(sel, { visible: true, timeout: 3500 });
          await page.click(sel, { clickCount: 3 });
          await page.type(sel, text, { delay: 18 });
          typed = true;
          break;
        } catch (_) {}
      }
      if (!typed && /gmail\\.com|workspace\\.google\\.com/i.test(page.url())) {
        await page.goto("https://accounts.google.com/ServiceLogin?service=mail", { waitUntil: "networkidle2", timeout: 30000 });
        for (const sel of emailSelectors) {
          try {
            await page.waitForSelector(sel, { visible: true, timeout: 5000 });
            await page.click(sel, { clickCount: 3 });
            await page.type(sel, text, { delay: 18 });
            typed = true;
            break;
          } catch (_) {}
        }
      }
      result.typed = typed;
      result.emailTyped = typed;
    } else if (action === "scroll") {
      await page.evaluate(() => window.scrollBy(0, window.innerHeight));
    } else if (action === "extract") {
      result.extracted = await page.$eval(
        extractSelector,
        (el) => (el.textContent || "").trim().slice(0, 4000)
      ).catch(() => "");
    } else if (action === "screenshot") {
      await page.goto(targetUrl, { waitUntil: "networkidle2", timeout: 30000 });
    }

    result.finalUrl = page.url();
    result.title = await page.title().catch(() => "");
    let visibleText = await page.evaluate(() => document.body?.innerText?.slice(0, 2000) || "").catch(() => "");
    const captchaPattern = /\\/sorry\\/index|captcha|recaptcha|hcaptcha|turnstile|bot check|unusual traffic|verify you are human|are you human|security check/i;

    // If a CAPTCHA is detected, give Browserless solveCaptchas time to resolve it.
    // Browserless's auto-solver injects the token into the DOM in the background.
    // We poll up to 30s and re-check; if it clears we proceed transparently.
    if (captchaPattern.test(result.finalUrl + "\\n" + result.title + "\\n" + visibleText)) {
      const deadline = Date.now() + 30000;
      while (Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 2500));
        try { await page.waitForNetworkIdle({ idleTime: 800, timeout: 4000 }); } catch (_) {}
        result.finalUrl = page.url();
        result.title = await page.title().catch(() => "");
        visibleText = await page.evaluate(() => document.body?.innerText?.slice(0, 2000) || "").catch(() => "");
        if (!captchaPattern.test(result.finalUrl + "\\n" + result.title + "\\n" + visibleText)) break;
      }
      if (captchaPattern.test(result.finalUrl + "\\n" + result.title + "\\n" + visibleText)) {
        result.blockedReason = "captcha_or_bot_check";
        result.blockedDetails = visibleText.slice(0, 500);
      }
      // Re-extract if extract action and we got past the wall
      if (action === "extract" && !result.blockedReason) {
        result.extracted = await page.$eval(
          extractSelector,
          (el) => (el.textContent || "").trim().slice(0, 4000)
        ).catch(() => "");
      }
    }
  } catch (err) {
    result.error = err instanceof Error ? err.message : String(err);
  }

  const screenshot = await page.screenshot({ type: "png", fullPage: false });
  return {
    data: { ...result, screenshotBase64: screenshot.toString("base64") },
    type: "application/json",
  };
};`;
}

function normalizeRedisId(value: unknown, fallbackPrefix: string) {
  const raw = typeof value === "string" ? value : "";
  const cleaned = raw.toLowerCase().trim().replace(/[^a-z0-9_.-]/g, "-").replace(/-+/g, "-").replace(/^-|-$/g, "");
  return cleaned || `${fallbackPrefix}-${crypto.randomUUID()}`;
}

function normalizeRedisIdOrDefault(value: unknown, fallback: string) {
  const raw = typeof value === "string" ? value : "";
  const cleaned = raw.toLowerCase().trim().replace(/[^a-z0-9_.-]/g, "-").replace(/-+/g, "-").replace(/^-|-$/g, "");
  return cleaned || fallback;
}

function parseRedisArray(reply: RedisReply): string[] {
  if (!Array.isArray(reply)) return [];
  return reply.flatMap((item) => (typeof item === "string" ? [item] : Buffer.isBuffer(item) ? [item.toString()] : []));
}

async function scanKeys(match: string, count = 100) {
  const keys: string[] = [];
  let cursor = "0";

  do {
    const reply = await redis.sendCommand(["SCAN", cursor, "MATCH", match, "COUNT", String(count)]) as RedisReply[];
    cursor = String(reply[0] ?? "0");
    keys.push(...parseRedisArray(reply[1] as RedisReply));
  } while (cursor !== "0" && keys.length < count);

  return keys.slice(0, count);
}

async function getJsonValue<T>(key: string): Promise<T | null> {
  const value = await redis.sendCommand(["GET", key]) as RedisReply;
  if (typeof value !== "string") return null;
  return JSON.parse(value) as T;
}

async function setJsonValue(key: string, value: unknown, ttlSeconds: number) {
  await redis.sendCommand(["SET", key, JSON.stringify(value), "EX", String(ttlSeconds)]);
}

function jobKey(jobId: string) {
  return `okm:job:${jobId}`;
}

function jobEventsKey(jobId: string) {
  return `okm:job:${jobId}:events`;
}

function jobQueueKey(plan: string, taskType: string, region: string) {
  return `okm:q:${plan}:${taskType}:${region}`;
}

function userJobsKey(userId: string) {
  return `okm:user:${userId}:jobs`;
}

function sessionKey(sessionId: string) {
  return `okm:session:${sessionId}`;
}

function sessionJobsKey(sessionId: string) {
  return `okm:session:${sessionId}:jobs`;
}

function serializeJob(hash: Record<string, string>) {
  return {
    job_id: hash.jobId,
    session_id: hash.sessionId,
    user_id: hash.userId,
    workspace_id: hash.workspaceId,
    task_type: hash.taskType,
    status: hash.status,
    queue: hash.queue,
    region: hash.region,
    plan: hash.plan,
    prompt: hash.prompt,
    target: JSON.parse(hash.target || "{}"),
    runtime: JSON.parse(hash.runtime || "{}"),
    llm: JSON.parse(hash.llm || "{}"),
    artifacts: JSON.parse(hash.artifacts || "{}"),
    verification: JSON.parse(hash.verification || "{}"),
    created_at: hash.createdAt,
    updated_at: hash.updatedAt,
    expires_at: hash.expiresAt,
    events_url: `/v1/jobs/${hash.jobId}/events`,
    artifacts_url: `/v1/jobs/${hash.jobId}/artifacts`,
  };
}

async function getJob(jobId: string) {
  const hash = parseRedisHash(await redis.sendCommand(["HGETALL", jobKey(jobId)]) as RedisReply);
  if (!hash.jobId) return null;
  return serializeJob(hash);
}

async function appendJobEvent(jobId: string, event: Record<string, unknown>, ttlSeconds = okmanusJobTtlSeconds) {
  const key = jobEventsKey(jobId);
  await redis.sendCommand([
    "XADD",
    key,
    "MAXLEN",
    "~",
    "1000",
    "*",
    "event",
    JSON.stringify({
      ...event,
      jobId,
      createdAt: new Date().toISOString(),
    }),
  ]);
  await redis.sendCommand(["EXPIRE", key, String(ttlSeconds)]);
}

function serializeRedisStream(reply: RedisReply) {
  if (!Array.isArray(reply)) return [];
  return reply.map((entry) => {
    const [id, fields] = Array.isArray(entry) ? entry : [];
    const fieldList = parseRedisArray(fields as RedisReply);
    const eventIndex = fieldList.indexOf("event");
    const eventJson = eventIndex >= 0 ? fieldList[eventIndex + 1] : "{}";
    return {
      id: String(id),
      event: JSON.parse(eventJson),
    };
  });
}

async function getProductActor(request: Request) {
  const auth = await verifyAppwriteJwt(request);
  const user = auth.ok ? (auth.user as Record<string, unknown> | null) : null;
  return {
    user,
    userId: typeof user?.$id === "string" ? user.$id : "anonymous",
  };
}

async function ensureSessionManifest(input: {
  sessionId: string;
  userId: string;
  workspaceId: string;
  ttlSeconds: number;
}) {
  const now = new Date().toISOString();
  const key = sessionKey(input.sessionId);
  await redis
    .multi()
    .hSet(key, {
      sessionId: input.sessionId,
      userId: input.userId,
      workspaceId: input.workspaceId,
      status: "active",
      updatedAt: now,
    })
    .expire(key, input.ttlSeconds)
    .expire(sessionJobsKey(input.sessionId), input.ttlSeconds)
    .exec();
}

function credentialConsentIndexKey(userId: string) {
  return `okm:user:${userId}:credential_consents`;
}

function credentialConsentKey(userId: string, providerId: string) {
  return `okm:cred:${userId}:${providerId}`;
}

function serializeCredentialConsent(hash: Record<string, string>) {
  return {
    providerId: hash.providerId,
    providerName: hash.providerName,
    status: hash.status,
    vaultProvider: hash.vaultProvider,
    secretRef: hash.secretRef,
    scopes: JSON.parse(hash.scopes || "[]") as string[],
    last4: hash.last4 || "",
    note: hash.note || "",
    createdAt: hash.createdAt,
    updatedAt: hash.updatedAt,
    expiresAt: hash.expiresAt,
  };
}

async function getCredentialConsent(userId: string, providerId: string) {
  const reply = await redis.sendCommand(["HGETALL", credentialConsentKey(userId, providerId)]) as RedisReply;
  const hash = parseRedisHash(reply);
  if (!hash.providerId) return null;
  return serializeCredentialConsent(hash);
}

async function listCredentialConsents(userId: string) {
  const providerIds = parseRedisArray(await redis.sendCommand(["SMEMBERS", credentialConsentIndexKey(userId)]) as RedisReply);
  const records = await Promise.all(providerIds.map((providerId) => getCredentialConsent(userId, providerId)));
  return records.filter(Boolean);
}

const credentialVaultOptions = [
  {
    id: "infisical",
    category: "open-source-cloud",
    mode: ["self-hosted", "managed-cloud"],
    recommendedFor: "Mejor primera opcion para OkManus: UI clara, API/SDK, proyectos por workspace y buen camino self-host.",
    storesSecret: true,
  },
  {
    id: "openbao",
    category: "open-source",
    mode: ["self-hosted"],
    recommendedFor: "Vault compatible y gobernanza abierta; fuerte si quereis politicas, auditoria y control infra.",
    storesSecret: true,
  },
  {
    id: "conjur",
    category: "open-source",
    mode: ["self-hosted", "enterprise-cloud"],
    recommendedFor: "Control de acceso machine-to-machine y auditoria estricta.",
    storesSecret: true,
  },
  {
    id: "sops-age",
    category: "open-source",
    mode: ["git-encrypted-files"],
    recommendedFor: "Configuracion interna y repos, no ideal para consentimiento dinamico por usuario final.",
    storesSecret: true,
  },
  {
    id: "pangea-vault",
    category: "cloud",
    mode: ["managed-cloud"],
    recommendedFor: "API-first para secretos/tokens de usuario si no quereis operar vault.",
    storesSecret: true,
  },
  {
    id: "akeyless",
    category: "cloud",
    mode: ["managed-cloud", "hybrid"],
    recommendedFor: "Enterprise/multicloud con rotacion, auditoria y secretos dinamicos.",
    storesSecret: true,
  },
  {
    id: "aws-secrets-manager",
    category: "cloud",
    mode: ["aws-managed"],
    recommendedFor: "Si desplegais OkManus en AWS y quereis IAM/KMS/CloudTrail nativo.",
    storesSecret: true,
  },
  {
    id: "google-secret-manager",
    category: "cloud",
    mode: ["gcp-managed"],
    recommendedFor: "Si desplegais OkManus en Google Cloud y quereis IAM/auditoria nativa.",
    storesSecret: true,
  },
  {
    id: "azure-key-vault",
    category: "cloud",
    mode: ["azure-managed"],
    recommendedFor: "Si desplegais OkManus en Azure y quereis Key Vault/Managed Identity.",
    storesSecret: true,
  },
  {
    id: "okap",
    category: "delegation-protocol",
    mode: ["user-vault-delegation"],
    recommendedFor: "Modelo emergente para que el usuario delegue acceso desde su vault sin copiar claves a OkManus.",
    storesSecret: false,
  },
];

async function appendMcpEvent(mcpId: string, workspaceId: string, event: Record<string, unknown>) {
  const streamKey = `okm:mcp:${mcpId}:stream:${workspaceId}`;
  await redis.sendCommand([
    "XADD",
    streamKey,
    "MAXLEN",
    "~",
    "1000",
    "*",
    "event",
    JSON.stringify({
      ...event,
      mcpId,
      workspaceId,
      createdAt: new Date().toISOString(),
    }),
  ]);
  await redis.sendCommand(["EXPIRE", streamKey, String(mcpRegistryTtlSeconds)]);
}

function buildBootstrapManifest() {
  const expiresAt = Date.now() + 60 * 60 * 1000;
  return {
    sessionId: `sess_${crypto.randomUUID()}`,
    expiresAt,
    ttlSeconds: 3600,
    providers: [
      {
        id: "groq-primary",
        type: "llm",
        name: "Groq",
        endpoint: "/v1/llm/test",
        capabilities: ["chat", "openai-compatible", "fast"],
        priority: 1,
        secretRef: "server:GROQ_API_KEY",
        model: groqModel,
      },
      {
        id: "openrouter-primary",
        type: "llm",
        name: "OpenRouter",
        endpoint: "/v1/llm/test",
        capabilities: ["chat", "openai-compatible"],
        priority: 1,
        secretRef: "server:OPENROUTER_API_KEY",
        model: openRouterModel,
      },
      {
        id: "browserless-computer",
        type: "computer",
        name: "Browserless",
        endpoint: "/v1/computer/browserless",
        capabilities: ["browser", "screenshot", "computer-preview"],
        priority: 1,
        secretRef: "server:BROWSERLESS_API_KEY",
      },
    ],
    routingTable: {
      "llm-default": groqApiKey ? ["groq-primary", "openrouter-primary"] : ["openrouter-primary"],
      computer: ["browserless-computer"],
    },
  };
}

type LlmProviderId = "groq" | "openrouter";

const llmProviderDefaults = {
  groq: {
    id: "groq" as const,
    name: "Groq",
    endpoint: "https://api.groq.com/openai/v1/chat/completions",
    model: groqModel,
    apiKey: groqApiKey,
  },
  openrouter: {
    id: "openrouter" as const,
    name: "OpenRouter",
    endpoint: "https://openrouter.ai/api/v1/chat/completions",
    model: openRouterModel,
    apiKey: openRouterApiKey,
  },
};

function normalizeLlmProvider(value: unknown): LlmProviderId {
  const provider = typeof value === "string" ? value.toLowerCase().replace(/[^a-z0-9]/g, "") : "";
  if (provider.includes("groq")) return "groq";
  if (provider.includes("openrouter")) return "openrouter";
  return groqApiKey ? "groq" : "openrouter";
}

type LlmMessage = { role: "system" | "user" | "assistant"; content: string };

async function callOpenAiCompatibleProvider(input: {
  provider: LlmProviderId;
  apiKey?: string;
  model?: string;
  prompt?: string;
  messages?: LlmMessage[];
  maxTokens?: number;
  tools?: unknown[];
  toolChoice?: unknown;
}) {
  const defaults = llmProviderDefaults[input.provider];
  const apiKey = input.apiKey?.trim() || defaults.apiKey;
  if (!apiKey) throw new Error(`${defaults.name} API key is not configured`);

  const headers: Record<string, string> = {
    authorization: `Bearer ${apiKey}`,
    "content-type": "application/json",
  };
  if (input.provider === "openrouter") {
    headers["http-referer"] = "http://localhost";
    headers["x-title"] = "OkManus local integration";
  }

  const messages: LlmMessage[] = input.messages ?? [
    { role: "user", content: input.prompt ?? "" },
  ];

  const body: Record<string, unknown> = {
    model: input.model?.trim() || defaults.model,
    messages,
    max_tokens: input.maxTokens ?? 512,
  };
  if (input.tools) {
    body.tools = input.tools;
    body.tool_choice = input.toolChoice ?? "auto";
  }

  const response = await fetch(defaults.endpoint, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  const data = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(data?.error?.message ?? `${defaults.name} failed: ${response.status}`);
  }

  const choice = data?.choices?.[0];
  return {
    provider: defaults.id,
    model: data?.model ?? input.model ?? defaults.model,
    content: choice?.message?.content ?? "",
    toolCalls: choice?.message?.tool_calls ?? null,
    finishReason: choice?.finish_reason ?? "stop",
  };
}

// Serializa un valor al protocolo RESP2 (Redis Serialization Protocol v2) en texto
// Usado cuando el cliente pide Upstash-Response-Format: resp2
// Formatos: $-1 = null, :N = integer, +OK = simple string, $N\r\ndata = bulk string, *N = array
function encodeResp(reply: unknown): string {
  if (reply === null || reply === undefined) return "$-1\r\n";
  if (typeof reply === "number" || typeof reply === "bigint") return `:${reply}\r\n`;
  if (Buffer.isBuffer(reply)) return `$${reply.length}\r\n${reply.toString()}\r\n`;
  if (Array.isArray(reply)) return `*${reply.length}\r\n${reply.map(encodeResp).join("")}`;

  const value = String(reply);
  if (value === "OK") return "+OK\r\n";
  return `$${Buffer.byteLength(value)}\r\n${value}\r\n`;
}

// Decide el formato de la respuesta según el header Upstash-Response-Format:
// - "json" (default): { result: ... } con JSON
// - "resp2": protocolo RESP2 binario como application/octet-stream
// base64 + resp2 no son combinables (RESP2 ya es binario)
function makeRedisResponse(reply: RedisReply, request: Request) {
  const responseFormat = request.headers.get("upstash-response-format")?.toLowerCase() ?? "json";
  const encoding = request.headers.get("upstash-encoding")?.toLowerCase();

  if (responseFormat !== "json" && responseFormat !== "resp2") {
    return makeJsonResponse({ error: "ERR invalid Upstash-Response-Format" }, 400);
  }

  if (responseFormat === "resp2") {
    if (encoding === "base64") {
      return makeJsonResponse(
        { error: "ERR Upstash-Encoding base64 can not be used with RESP2 responses" },
        400,
      );
    }

    return new Response(encodeResp(reply), {
      headers: {
        "content-type": "application/octet-stream",
        "cache-control": "no-store",
      },
    });
  }

  return makeJsonResponse(formatResult(reply, request));
}

// Atajo para respuestas de error con formato Upstash
function upstashError(error: unknown, status = 400) {
  return makeJsonResponse(formatError(error), status);
}

// Instancia principal de la aplicación Elysia
const app = new Elysia();

app.onAfterHandle(({ request, set }) => {
  if (request.method === "OPTIONS" || isProductRequest(request)) {
    set.headers = {
      ...set.headers,
      ...productCorsHeaders,
    };
  }
});

app.options("/*", () => new Response(null, { status: 204, headers: productCorsHeaders }));

// Handler global de errores no capturados: NOT_FOUND → 404, resto → 400
app.onError(({ error, code }) => {
  const status = code === "NOT_FOUND" ? 404 : 400;
  return upstashError(error, status);
});

// Middleware global de autenticación: bloquea TODOS los endpoints sin API key válida
// Se ejecuta antes de cualquier handler, incluyendo /health y /
// Permite que el standardToken y el readOnlyToken pasen; cualquier otro → 401
// Paths that must be reachable without a Bearer token (OAuth redirect flows,
// browser-initiated handshakes). Keep this list short and audit each entry.
const PUBLIC_PATH_PREFIXES = ["/v1/auth/poe/"];

app.onBeforeHandle(async ({ request }) => {
  if (request.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: productCorsHeaders });
  }

  const pathname = new URL(request.url).pathname;
  if (PUBLIC_PATH_PREFIXES.some((p) => pathname.startsWith(p))) {
    return;
  }

  if (isProductRequest(request)) {
    const auth = authorizeProductRequest(request);
    if (!auth.ok) return makeJsonResponse({ error: auth.error }, auth.status);
    const appwriteAuth = await verifyAppwriteJwt(request);
    if (!appwriteAuth.ok) return makeJsonResponse({ error: appwriteAuth.error }, appwriteAuth.status);
    return;
  }

  const token = getToken(request);
  if (!token) {
    return makeJsonResponse({ error: "Unauthorized: missing token" }, 401);
  }
  if (token !== standardToken && !(readOnlyToken && token === readOnlyToken)) {
    return makeJsonResponse({ error: "Unauthorized: invalid token" }, 401);
  }
});

// Endpoint de health check: hace PING a Redis y devuelve estado + configuración activa
app.get("/health", async () => {
  try {
    const pong = await redis.ping();
    return {
      status: "healthy",
      redis: pong === "PONG" ? "connected" : "unknown",
      upstashCompatible: true,
      limits: {
        rateLimitMaxRequests,
        rateLimitWindowSeconds: Math.floor(rateLimitWindowMs / 1000),
        maxBatchCommands,
        maxRequestBytes,
        maxInFlightCommands,
        commandTimeoutMs,
        dangerousCommandsEnabled: allowDangerousCommands,
      },
      timestamp: new Date().toISOString(),
    };
  } catch (error) {
    return {
      status: "unhealthy",
      redis: "error",
      error: error instanceof Error ? error.message : String(error),
      timestamp: new Date().toISOString(),
    };
  }
});

// Endpoint raíz: devuelve nombre e información de los endpoints disponibles
app.get("/", () => ({
  name: "api-okmanus",
  upstashCompatible: true,
  endpoints: ["/<command>/<arg...>", "/pipeline", "/multi-exec", "/health"],
}));

app.get("/v1/health", async () => {
  const pong = await redis.ping();
  return {
    status: "healthy",
    redis: pong === "PONG" ? "connected" : "unknown",
    providers: {
      groq: groqApiKey ? "configured" : "missing_credentials",
      openrouter: openRouterApiKey ? "configured" : "missing_credentials",
      browserless: browserlessApiKey ? "configured" : "missing_credentials",
    },
    timestamp: new Date().toISOString(),
  };
});

app.get("/v1/bootstrap", async () => {
  const manifest = buildBootstrapManifest();
  await redis.set("okm:registry:bootstrap", JSON.stringify(manifest), { EX: manifest.ttlSeconds });
  return manifest;
});

app.get("/v1/auth/session", async ({ request }) => {
  const auth = await verifyAppwriteJwt(request);
  if (!auth.ok) return makeJsonResponse({ error: auth.error }, auth.status);

  return {
    configured: Boolean(appwriteEndpoint && appwriteProjectId),
    required: requireAppwriteSession,
    user: auth.user,
  };
});

app.post("/v1/sessions", async ({ request }) => {
  const actor = await getProductActor(request);
  const body = await readJsonObject(request).catch(() => ({}));
  const sessionId = normalizeRedisId(body.sessionId, "sess");
  const workspaceId = normalizeRedisId(body.workspaceId, "workspace");
  const ttlSeconds = numberField(body.ttlSeconds, okmanusSessionQueueTtlSeconds, 300, 7 * 24 * 3600);

  await ensureSessionManifest({
    sessionId,
    userId: actor.userId,
    workspaceId,
    ttlSeconds,
  });

  return {
    session: {
      sessionId,
      userId: actor.userId,
      workspaceId,
      ttlSeconds,
      queue: sessionJobsKey(sessionId),
    },
  };
});

app.get("/v1/sessions/:sessionId/jobs", async ({ params }) => {
  const sessionId = normalizeRedisId(params.sessionId, "sess");
  const jobIds = parseRedisArray(await redis.sendCommand(["LRANGE", sessionJobsKey(sessionId), "0", "99"]) as RedisReply);
  const jobs = await Promise.all(jobIds.map((jobId) => getJob(jobId)));
  return {
    sessionId,
    jobs: jobs.filter(Boolean),
  };
});

app.post("/v1/jobs", async ({ request }) => {
  const actor = await getProductActor(request);
  const body = await readJsonObject(request);
  const now = new Date();
  const taskType = normalizeRedisId(body.task_type ?? body.taskType, "task");
  const region = normalizeRedisIdOrDefault(body.region ?? (body.runtime as Record<string, unknown> | undefined)?.region, "global");
  const plan = normalizeRedisIdOrDefault(body.plan, "free");
  const workspaceId = normalizeRedisIdOrDefault(body.workspaceId, "workspace");
  const sessionId = normalizeRedisId(body.sessionId, "sess");
  const jobId = normalizeRedisId(body.job_id ?? body.jobId, "job");
  const queue = jobQueueKey(plan, taskType, region);
  const ttlSeconds = numberField(body.ttlSeconds, okmanusJobTtlSeconds, 300, 30 * 24 * 3600);
  const expiresAt = new Date(now.getTime() + ttlSeconds * 1000).toISOString();
  const prompt = stringField(body.prompt, "");

  if (!prompt) return makeJsonResponse({ error: "prompt is required" }, 400);

  const record = {
    jobId,
    sessionId,
    userId: actor.userId,
    workspaceId,
    taskType,
    status: "queued",
    queue,
    region,
    plan,
    prompt,
    target: jsonField(body.target),
    runtime: jsonField(body.runtime),
    llm: jsonField(body.llm ?? body.llm_chain),
    artifacts: jsonField(body.artifacts),
    verification: jsonField(body.verification),
    createdAt: now.toISOString(),
    updatedAt: now.toISOString(),
    expiresAt,
  };

  const userJobs = userJobsKey(actor.userId);
  const sessionJobs = sessionJobsKey(sessionId);
  const queueLength = Number(await redis.sendCommand(["LLEN", queue]) ?? 0);
  if (queueLength >= okmanusQueueMaxJobs) {
    return makeJsonResponse({ error: `Queue ${queue} is full` }, 429);
  }

  await ensureSessionManifest({
    sessionId,
    userId: actor.userId,
    workspaceId,
    ttlSeconds: okmanusSessionQueueTtlSeconds,
  });

  await redis
    .multi()
    .hSet(jobKey(jobId), record)
    .expire(jobKey(jobId), ttlSeconds)
    .rPush(queue, jobId)
    .expire(queue, ttlSeconds)
    .lPush(sessionJobs, jobId)
    .lTrim(sessionJobs, 0, 99)
    .expire(sessionJobs, okmanusSessionQueueTtlSeconds)
    .sAdd(userJobs, jobId)
    .expire(userJobs, ttlSeconds)
    .exec();

  await appendJobEvent(jobId, {
    type: "job.queued",
    status: "queued",
    queue,
    sessionId,
    workspaceId,
    userId: actor.userId,
  }, ttlSeconds);

  return {
    job_id: jobId,
    status: "queued",
    created_at: record.createdAt,
    queue,
    session_id: sessionId,
    events_url: `/v1/jobs/${jobId}/events`,
    artifacts_url: `/v1/jobs/${jobId}/artifacts`,
  };
});

app.get("/v1/jobs", async ({ request }) => {
  const actor = await getProductActor(request);
  const url = new URL(request.url);
  const sessionId = optionalStringField(url.searchParams.get("sessionId"));
  const count = numberField(url.searchParams.get("count"), 50, 1, 100);
  const sourceKey = sessionId ? sessionJobsKey(normalizeRedisId(sessionId, "sess")) : userJobsKey(actor.userId);
  const ids = sessionId
    ? parseRedisArray(await redis.sendCommand(["LRANGE", sourceKey, "0", String(count - 1)]) as RedisReply)
    : parseRedisArray(await redis.sendCommand(["SMEMBERS", sourceKey]) as RedisReply).slice(0, count);
  const jobs = await Promise.all(ids.map((jobId) => getJob(jobId)));

  return {
    jobs: jobs.filter(Boolean),
    count: jobs.filter(Boolean).length,
  };
});

app.get("/v1/jobs/:jobId", async ({ params }) => {
  const jobId = normalizeRedisId(params.jobId, "job");
  const job = await getJob(jobId);
  if (!job) return makeJsonResponse({ error: "Job not found" }, 404);
  return { job };
});

app.get("/v1/jobs/:jobId/events", async ({ request, params }) => {
  const jobId = normalizeRedisId(params.jobId, "job");
  const url = new URL(request.url);
  const count = numberField(url.searchParams.get("count"), 100, 1, 500);
  const reply = await redis.sendCommand(["XRANGE", jobEventsKey(jobId), "-", "+", "COUNT", String(count)]) as RedisReply;
  return {
    job_id: jobId,
    events: serializeRedisStream(reply),
  };
});

app.post("/v1/jobs/:jobId/cancel", async ({ params }) => {
  const jobId = normalizeRedisId(params.jobId, "job");
  const job = await getJob(jobId);
  if (!job) return makeJsonResponse({ error: "Job not found" }, 404);

  const updatedAt = new Date().toISOString();
  await redis.sendCommand(["HSET", jobKey(jobId), "status", "cancelled", "updatedAt", updatedAt]);
  await appendJobEvent(jobId, { type: "job.cancelled", status: "cancelled" });
  return { job_id: jobId, status: "cancelled", updated_at: updatedAt };
});

app.post("/v1/jobs/:jobId/retry", async ({ params }) => {
  const jobId = normalizeRedisId(params.jobId, "job");
  const job = await getJob(jobId);
  if (!job) return makeJsonResponse({ error: "Job not found" }, 404);

  const updatedAt = new Date().toISOString();
  await redis
    .multi()
    .hSet(jobKey(jobId), {
      status: "queued",
      updatedAt,
    })
    .rPush(job.queue, jobId)
    .expire(job.queue, okmanusJobTtlSeconds)
    .exec();
  await appendJobEvent(jobId, { type: "job.retried", status: "queued", queue: job.queue });
  return { job_id: jobId, status: "queued", queue: job.queue, updated_at: updatedAt };
});

app.post("/v1/queues/:plan/:taskType/:region/next", async ({ params }) => {
  const plan = normalizeRedisId(params.plan, "free");
  const taskType = normalizeRedisId(params.taskType, "task");
  const region = normalizeRedisId(params.region, "global");
  const queue = jobQueueKey(plan, taskType, region);
  const jobId = String((await redis.sendCommand(["LPOP", queue]) as RedisReply) ?? "");

  if (!jobId) return { job: null, queue };

  const updatedAt = new Date().toISOString();
  await redis.sendCommand(["HSET", jobKey(jobId), "status", "leased", "updatedAt", updatedAt]);
  await appendJobEvent(jobId, { type: "job.leased", status: "leased", queue });
  return {
    queue,
    job: await getJob(jobId),
  };
});

app.get("/v1/credential-vaults/options", () => ({
  options: credentialVaultOptions,
  consentModel: {
    rule: "OkManus stores consent metadata and external secret references only, never raw user API keys.",
    dragonflyKeys: {
      userIndex: "okm:user:{user_id}:credential_consents",
      consentHash: "okm:cred:{user_id}:{provider_id}",
    },
    ttlSeconds: credentialConsentTtlSeconds,
  },
}));

app.get("/v1/credentials/consents", async ({ request }) => {
  const auth = await requireAppwriteUser(request);
  if (!auth.ok) return makeJsonResponse({ error: auth.error }, auth.status);

  return {
    userId: auth.userId,
    consents: await listCredentialConsents(auth.userId),
  };
});

app.post("/v1/credentials/consents", async ({ request }) => {
  const auth = await requireAppwriteUser(request);
  if (!auth.ok) return makeJsonResponse({ error: auth.error }, auth.status);

  const body = await readJsonObject(request);
  if (typeof body.apiKey === "string" || typeof body.secretValue === "string" || typeof body.token === "string") {
    return makeJsonResponse({ error: "Raw API keys are not accepted here; store them in a vault and send secretRef only" }, 400);
  }

  const providerId = normalizeRedisId(body.providerId ?? body.provider, "provider");
  const providerName = stringField(body.providerName ?? body.provider, providerId);
  const vaultProvider = normalizeRedisId(body.vaultProvider, "external-vault");
  const secretRef = optionalStringField(body.secretRef);
  const status = body.allowed === false ? "revoked" : "allowed";
  const scopes = stringArrayField(body.scopes);
  const ttlSeconds = numberField(body.ttlSeconds, credentialConsentTtlSeconds, 300, 365 * 24 * 3600);
  const now = new Date();
  const existing = await getCredentialConsent(auth.userId, providerId);
  const createdAt = existing?.createdAt ?? now.toISOString();
  const expiresAt = new Date(now.getTime() + ttlSeconds * 1000).toISOString();
  const key = credentialConsentKey(auth.userId, providerId);
  const indexKey = credentialConsentIndexKey(auth.userId);

  await redis
    .multi()
    .hSet(key, {
      providerId,
      providerName,
      status,
      vaultProvider,
      secretRef,
      scopes: JSON.stringify(scopes),
      last4: optionalStringField(body.last4).slice(-4),
      note: optionalStringField(body.note).slice(0, 280),
      createdAt,
      updatedAt: now.toISOString(),
      expiresAt,
    })
    .expire(key, ttlSeconds)
    .sAdd(indexKey, providerId)
    .expire(indexKey, ttlSeconds)
    .xAdd(
      `okm:user:${auth.userId}:credential_events`,
      "*",
      {
        event: JSON.stringify({
          type: "credential.consent.upserted",
          providerId,
          vaultProvider,
          hasSecretRef: Boolean(secretRef),
          status,
          createdAt: now.toISOString(),
        }),
      },
      { TRIM: { strategy: "MAXLEN", strategyModifier: "~", threshold: 500 } },
    )
    .expire(`okm:user:${auth.userId}:credential_events`, Math.max(ttlSeconds, 3600))
    .exec();

  return {
    consent: await getCredentialConsent(auth.userId, providerId),
    key,
    ttlSeconds,
  };
});

app.delete("/v1/credentials/consents/:providerId", async ({ request, params }) => {
  const auth = await requireAppwriteUser(request);
  if (!auth.ok) return makeJsonResponse({ error: auth.error }, auth.status);

  const providerId = normalizeRedisId(params.providerId, "provider");
  const now = new Date().toISOString();
  await redis
    .multi()
    .del(credentialConsentKey(auth.userId, providerId))
    .sRem(credentialConsentIndexKey(auth.userId), providerId)
    .xAdd(
      `okm:user:${auth.userId}:credential_events`,
      "*",
      {
        event: JSON.stringify({
          type: "credential.consent.revoked",
          providerId,
          createdAt: now,
        }),
      },
      { TRIM: { strategy: "MAXLEN", strategyModifier: "~", threshold: 500 } },
    )
    .expire(`okm:user:${auth.userId}:credential_events`, Math.max(credentialConsentTtlSeconds, 3600))
    .exec();

  return { ok: true, providerId, revokedAt: now };
});

app.get("/v1/database/schema", () => ({
  dialect: "postgres",
  purpose: "OkManus persistent source of truth",
  tables: [
    "users",
    "workspaces",
    "workspace_members",
    "providers",
    "credential_connections",
    "managed_secrets",
    "external_secret_refs",
    "tasks",
    "task_credentials",
    "task_runs",
    "audit_events",
    "artifacts",
    "usage_limits",
    "infra_providers",
    "infra_provider_health_checks",
  ],
  dragonflySharedKeyspace: {
    registry: "okm:registry:{kind}",
    session: "okm:session:{session_id}",
    sessionJobs: "okm:session:{session_id}:jobs",
    mcpHealth: "okm:mcp:{mcp_id}:health",
    mcpSession: "okm:mcp:{mcp_id}:session:{session_id}",
    mcpStream: "okm:mcp:{mcp_id}:stream:{workspace_id}",
    workspaceRateLimit: "okm:ws:{workspace_id}:rl:{metric}",
    workspaceLocks: "okm:ws:{workspace_id}:lock:{resource_id}",
    credentialConsentIndex: "okm:user:{user_id}:credential_consents",
    credentialConsent: "okm:cred:{user_id}:{provider_id}",
    credentialEvents: "okm:user:{user_id}:credential_events",
    runnerHeartbeat: "okm:runner:{runner_id}:heartbeat",
    job: "okm:job:{job_id}",
    jobProgress: "okm:job:{job_id}:progress",
    jobEvents: "okm:job:{job_id}:events",
    queue: "okm:q:{plan}:{task_type}:{region}",
  },
  rules: [
    "One Dragonfly/Redis service is shared by all MCPs.",
    "Every ephemeral key must be written with TTL.",
    "Use Redis Streams for MCP/job events that need replay.",
    "Use SCAN patterns for inspection; KEYS remains blocked.",
    "Persistent truth stays in Postgres, not Dragonfly.",
  ],
}));

app.post("/v1/llm/test", async ({ request }) => {
  const body = await readJsonObject(request).catch(() => ({}));
  const prompt = stringField(body.prompt, "Reply with exactly: okmanus-llm-ok");
  const provider = normalizeLlmProvider(body.provider);

  try {
    const result = await callOpenAiCompatibleProvider({
      provider,
      prompt,
      model: typeof body.model === "string" ? body.model : undefined,
    });
    await redis.set(
      `okm:provider:${result.provider}:last_test`,
      JSON.stringify({ ...result, content: undefined, ok: true, testedAt: new Date().toISOString() }),
      { EX: 3600 },
    );
    return result;
  } catch (error) {
    return makeJsonResponse(formatError(error), 503);
  }
});

app.post("/v1/providers/test", async ({ request }) => {
  const body = await readJsonObject(request).catch(() => ({}));
  const provider = normalizeLlmProvider(body.provider ?? body.name);
  const prompt = stringField(body.prompt, "Reply with exactly: okmanus-provider-ok");

  try {
    const result = await callOpenAiCompatibleProvider({
      provider,
      prompt,
      apiKey: typeof body.apiKey === "string" ? body.apiKey : undefined,
      model: typeof body.model === "string" ? body.model : undefined,
    });
    await redis.set(
      `okm:provider:${result.provider}:last_key_test`,
      JSON.stringify({ provider: result.provider, model: result.model, ok: true, testedAt: new Date().toISOString() }),
      { EX: 3600 },
    );
    return { ...result, apiKeyAccepted: true };
  } catch (error) {
    return makeJsonResponse(formatError(error), 503);
  }
});

app.post("/v1/mcps/register", async ({ request }) => {
  const body = await readJsonObject(request);
  const mcpId = normalizeRedisId(body.mcpId ?? body.id, "mcp");
  const workspaceId = normalizeRedisId(body.workspaceId, "workspace");
  const ttlSeconds = numberField(body.ttlSeconds, mcpRegistryTtlSeconds, 60, 86_400);
  const heartbeatTtlSeconds = numberField(body.heartbeatTtlSeconds, mcpHeartbeatTtlSeconds, 15, 3600);
  const now = new Date().toISOString();
  const record = {
    id: mcpId,
    workspaceId,
    name: stringField(body.name, mcpId),
    endpoint: stringField(body.endpoint, ""),
    transport: stringField(body.transport, "http-stream"),
    capabilities: stringArrayField(body.capabilities),
    status: "registered",
    registeredAt: now,
    updatedAt: now,
    ttlSeconds,
  };
  const health = {
    mcpId,
    workspaceId,
    status: "online",
    lastSeenAt: now,
    ttlSeconds: heartbeatTtlSeconds,
  };

  await setJsonValue(`okm:mcp:${mcpId}:meta`, record, ttlSeconds);
  await setJsonValue(`okm:mcp:${mcpId}:health`, health, heartbeatTtlSeconds);
  await appendMcpEvent(mcpId, workspaceId, { type: "mcp.registered", record });

  return {
    mcp: record,
    keys: {
      meta: `okm:mcp:${mcpId}:meta`,
      health: `okm:mcp:${mcpId}:health`,
      stream: `okm:mcp:${mcpId}:stream:${workspaceId}`,
    },
  };
});

app.get("/v1/mcps", async () => {
  const keys = await scanKeys("okm:mcp:*:meta", 100);
  const mcps = await Promise.all(keys.map((key) => getJsonValue(key).catch(() => null)));
  return {
    mcps: mcps.filter(Boolean),
    count: mcps.filter(Boolean).length,
  };
});

app.post("/v1/mcps/:mcpId/heartbeat", async ({ request, params }) => {
  const body = await readJsonObject(request).catch(() => ({}));
  const mcpId = normalizeRedisId(params.mcpId, "mcp");
  const workspaceId = normalizeRedisId(body.workspaceId, "workspace");
  const ttlSeconds = numberField(body.ttlSeconds, mcpHeartbeatTtlSeconds, 15, 3600);
  const health = {
    mcpId,
    workspaceId,
    status: stringField(body.status, "online"),
    activeSessions: numberField(body.activeSessions, 0, 0, 100_000),
    capacity: body.capacity && typeof body.capacity === "object" ? body.capacity : {},
    lastSeenAt: new Date().toISOString(),
    ttlSeconds,
  };

  await setJsonValue(`okm:mcp:${mcpId}:health`, health, ttlSeconds);
  await appendMcpEvent(mcpId, workspaceId, { type: "mcp.heartbeat", health });
  return { health };
});

app.get("/v1/mcps/:mcpId/health", async ({ params }) => {
  const mcpId = normalizeRedisId(params.mcpId, "mcp");
  const health = await getJsonValue(`okm:mcp:${mcpId}:health`);
  if (!health) return makeJsonResponse({ error: "MCP health not found" }, 404);
  return { health };
});

app.post("/v1/mcps/:mcpId/sessions", async ({ request, params }) => {
  const body = await readJsonObject(request).catch(() => ({}));
  const mcpId = normalizeRedisId(params.mcpId, "mcp");
  const workspaceId = normalizeRedisId(body.workspaceId, "workspace");
  const sessionId = normalizeRedisId(body.sessionId, "session");
  const ttlSeconds = numberField(body.ttlSeconds, mcpSessionTtlSeconds, 60, 86_400);
  const session = {
    sessionId,
    mcpId,
    workspaceId,
    userId: normalizeRedisId(body.userId, "user"),
    scopes: stringArrayField(body.scopes),
    status: "active",
    createdAt: new Date().toISOString(),
    ttlSeconds,
  };

  await setJsonValue(`okm:mcp:${mcpId}:session:${sessionId}`, session, ttlSeconds);
  await appendMcpEvent(mcpId, workspaceId, { type: "mcp.session.created", sessionId, scopes: session.scopes });
  return { session };
});

app.post("/v1/mcps/:mcpId/events", async ({ request, params }) => {
  const body = await readJsonObject(request);
  const mcpId = normalizeRedisId(params.mcpId, "mcp");
  const workspaceId = normalizeRedisId(body.workspaceId, "workspace");
  const event = {
    type: stringField(body.type, "mcp.event"),
    payload: body.payload && typeof body.payload === "object" ? body.payload : {},
  };

  await appendMcpEvent(mcpId, workspaceId, event);
  return { ok: true, stream: `okm:mcp:${mcpId}:stream:${workspaceId}` };
});

app.get("/v1/mcps/:mcpId/events", async ({ request, params }) => {
  const mcpId = normalizeRedisId(params.mcpId, "mcp");
  const url = new URL(request.url);
  const workspaceId = normalizeRedisId(url.searchParams.get("workspaceId"), "workspace");
  const count = numberField(url.searchParams.get("count"), 50, 1, 200);
  const streamKey = `okm:mcp:${mcpId}:stream:${workspaceId}`;
  const reply = await redis.sendCommand(["XRANGE", streamKey, "-", "+", "COUNT", String(count)]) as RedisReply[];
  const events = reply.map((entry) => {
    const [id, fields] = Array.isArray(entry) ? entry : [];
    const fieldList = parseRedisArray(fields as RedisReply);
    const eventIndex = fieldList.indexOf("event");
    const eventJson = eventIndex >= 0 ? fieldList[eventIndex + 1] : "{}";
    return {
      id: String(id),
      event: JSON.parse(eventJson),
    };
  });

  return { stream: streamKey, events };
});

app.post("/v1/computer/browserless", async ({ request }) => {
  if (!browserlessApiKey) return makeJsonResponse({ error: "BROWSERLESS_API_KEY is not configured" }, 503);

  const body = await readJsonObject(request).catch(() => ({}));
  const targetUrl = stringField(body.url, "");
  if (!targetUrl) return makeJsonResponse({ error: "url is required" }, 400);
  const response = await fetch(`${browserlessEndpoint}/screenshot?token=${encodeURIComponent(browserlessApiKey)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      url: targetUrl,
      options: { type: "png", fullPage: false },
      gotoOptions: { waitUntil: "networkidle2", timeout: 15_000 },
    }),
  });

  if (!response.ok) {
    const error = await response.text().catch(() => `Browserless failed: ${response.status}`);
    return makeJsonResponse({ error }, response.status);
  }

  const imageBytes = Buffer.from(await response.arrayBuffer());
  const payload = {
    provider: "browserless",
    url: targetUrl,
    contentType: response.headers.get("content-type") ?? "image/png",
    screenshotDataUrl: `data:image/png;base64,${imageBytes.toString("base64")}`,
    bytes: imageBytes.byteLength,
    capturedAt: new Date().toISOString(),
  };

  await redis.set("okm:computer:browserless:last_capture", JSON.stringify({ ...payload, screenshotDataUrl: undefined }), { EX: 3600 });
  return payload;
});

// ──────────────────────────────────────────────────────────────────────────
// ReAct browser agent: LLM plans → Browserless executes → screenshot feeds
// back → LLM reasons again. Up to REACT_MAX_STEPS iterations.
// ──────────────────────────────────────────────────────────────────────────

const REACT_MAX_STEPS = 20;

const NO_TOOL_CALL_RETRY_LIMIT = 2;

function sanitizeAgentText(input: string): string {
  if (!input) return "";
  let out = input;
  // Strip hallucinated tool-call XML: <function=NAME>...</function>
  out = out.replace(/<function=\w+>[\s\S]*?<\/function>\.?/g, "");
  // Strip self-closing XML tool calls: <navigate url="..."/> <click selector="..."/>
  out = out.replace(/<(?:navigate|click|type|scroll|extract|screenshot|finish)\b[^>]*\/?>/gi, "");
  out = out.replace(/<\/(?:navigate|click|type|scroll|extract|screenshot|finish)>/gi, "");
  // Strip bracket pseudo-syntax
  out = out.replace(/\[tool_call:[\s\S]*?\]/g, "");
  // Strip bare keyword + JSON: navigate{"url":"..."}  finish{"summary":"..."}
  out = out.replace(/\b(?:navigate|click|type|scroll|extract|screenshot|finish)\s*\{[^{}]*\}\.?/gi, "");
  // Strip various tool-call leakage tokens
  out = out.replace(/<\|tool_call\|>/g, "");
  out = out.replace(/<\/?tool_call>/g, "");
  out = out.replace(/<\|python_tag\|>/g, "");
  out = out.replace(/<\|eom_id\|>/g, "");
  out = out.replace(/<\|eot_id\|>/g, "");
  // Strip fake screenshot placeholders
  out = out.replace(/Screenshot:\s*\[[^\]]*\]/gi, "");
  out = out.replace(/\[imagen de la página actual\]/gi, "");
  // Strip "A continuación, ..." filler lines often used in hallucinated narrations
  out = out.replace(/^\s*A continuación[^\n]*$/gim, "");
  // Collapse 3+ newlines to 2
  out = out.replace(/\n{3,}/g, "\n\n");
  return out.trim();
}

// Regex used to recover hallucinated tool calls the model emits as plain text
// instead of via the real `tool_calls` field. Example match:
//   <function=navigate>{"url":"https://google.com"}</function>
const TEXT_TOOL_CALL_REGEX = /<function=(\w+)>([\s\S]*?)<\/function>/;

const BROWSER_TOOL_NAMES = new Set([
  "navigate",
  "click",
  "type",
  "scroll",
  "extract",
  "screenshot",
  "finish",
]);

/**
 * If the LLM hallucinated a tool call as plain text, extract the first
 * occurrence and convert it into the shape of an OpenAI `tool_calls[0]`
 * entry so the ReAct loop can execute it like a real one.
 *
 * Returns null when no valid pseudo-call is present.
 */
function parseBrowserToolCallFromText(
  content: string,
): { id: string; type: "function"; function: { name: string; arguments: string } } | null {
  if (!content) return null;

  // Format A: <function=name>{json}</function>
  const matchA = content.match(TEXT_TOOL_CALL_REGEX);
  if (matchA) {
    const [, name, rawArgs] = matchA;
    if (BROWSER_TOOL_NAMES.has(name)) {
      let argsText = (rawArgs ?? "").trim();
      const firstBrace = argsText.indexOf("{");
      const lastBrace = argsText.lastIndexOf("}");
      argsText = firstBrace >= 0 && lastBrace > firstBrace
        ? argsText.slice(firstBrace, lastBrace + 1)
        : "{}";
      try { JSON.parse(argsText); } catch { argsText = "{}"; }
      return {
        id: `parsed-${crypto.randomUUID()}`,
        type: "function",
        function: { name, arguments: argsText },
      };
    }
  }

  // Format B: self-closing XML tag <navigate url="..."/> <click selector="..."/> <type selector="..." text="..."/>
  const xmlMatch = content.match(
    /<(navigate|click|type|scroll|extract|screenshot|finish)\b([^>]*?)\/?>/i,
  );
  if (xmlMatch) {
    const name = xmlMatch[1].toLowerCase();
    const attrBlob = xmlMatch[2] || "";
    const args: Record<string, string> = {};
    // Match attr="value" or attr='value' (handles escaped quotes loosely)
    const attrRegex = /(\w+)\s*=\s*"([^"]*)"|(\w+)\s*=\s*'([^']*)'/g;
    let m: RegExpExecArray | null;
    while ((m = attrRegex.exec(attrBlob)) !== null) {
      const key = m[1] ?? m[3];
      const val = m[2] ?? m[4] ?? "";
      if (key) args[key] = val;
    }
    return {
      id: `parsed-${crypto.randomUUID()}`,
      type: "function",
      function: { name, arguments: JSON.stringify(args) },
    };
  }

  // Format D: bare keyword + JSON: navigate{"url":"..."}  click{"selector":"..."}
  const bareMatch = content.match(
    /\b(navigate|click|type|scroll|extract|screenshot|finish)\s*(\{[^{}]*\})/i,
  );
  if (bareMatch) {
    const name = bareMatch[1].toLowerCase();
    let argsText = bareMatch[2] || "{}";
    try { JSON.parse(argsText); } catch { argsText = "{}"; }
    return {
      id: `parsed-${crypto.randomUUID()}`,
      type: "function",
      function: { name, arguments: argsText },
    };
  }

  // Format C: JSON code block { "name": "navigate", "arguments": {...} }
  const jsonBlock = content.match(
    /\{\s*"name"\s*:\s*"(navigate|click|type|scroll|extract|screenshot|finish)"\s*,\s*"arguments"\s*:\s*(\{[\s\S]*?\})\s*\}/i,
  );
  if (jsonBlock) {
    const name = jsonBlock[1].toLowerCase();
    let argsText = jsonBlock[2] || "{}";
    try { JSON.parse(argsText); } catch { argsText = "{}"; }
    return {
      id: `parsed-${crypto.randomUUID()}`,
      type: "function",
      function: { name, arguments: argsText },
    };
  }

  return null;
}

/**
 * Coaching message sent back to the model when it failed to emit any tool call
 * (either real or as recoverable hallucinated text). Encourages it to retry
 * with a real `tool_calls` payload instead of imitating the schema in prose.
 */
function buildNoToolCallRetryMessage(input: {
  objective: string;
  currentUrl: string;
  rawContent: string;
  attempt: number;
  maxAttempts: number;
}): string {
  const { objective, currentUrl, attempt, maxAttempts } = input;
  return (
    `No detecté ninguna tool_call en tu última respuesta. ` +
    `Intento ${attempt}/${maxAttempts}.\n` +
    `Objetivo: ${objective}\n` +
    `URL actual: ${currentUrl}\n\n` +
    `Recordatorio: DEBES emitir la herramienta mediante el campo tool_calls de la API. ` +
    `Está PROHIBIDO escribir <function=...>...</function> u otra imitación textual. ` +
    `Emite UNA tool_call ahora. Si ya terminaste, usa 'finish' con un summary real.`
  );
}

/**
 * Heuristic detector for external block pages (captcha, consent, login walls).
 * Used to short-circuit the loop and surface a clear message to the user
 * instead of letting the model spin uselessly.
 */
function classifyBrowserBlock(input: {
  url: string;
  title: string;
  extracted: string;
}): { kind: string; detail: string } | null {
  const blob = `${input.url}\n${input.title}\n${input.extracted}`.toLowerCase();

  if (/\/sorry\/index|unusual traffic|tráfico inusual|recaptcha/.test(blob)) {
    return {
      kind: "captcha",
      detail: "Google bloqueó la sesión con captcha (sorry/index). Hace falta intervención humana o una API oficial.",
    };
  }
  if (/consent\.google|consent\.youtube|cookieconsent|consent\.gstatic/.test(blob)) {
    return {
      kind: "consent_wall",
      detail: "Página de consentimiento de cookies bloquea la navegación automatizada.",
    };
  }
  // Strict login-wall detection: only trigger when URL is a known auth page,
  // not when "login" appears as a footer link in normal pages.
  if (/accounts\.google\.com\/signin|accounts\.google\.com\/v3\/signin|login\.microsoftonline\.com|login\.live\.com|appleid\.apple\.com\/auth|github\.com\/login/.test(input.url)) {
    return {
      kind: "login_required",
      detail: "El destino exige login. Se necesita sesión autenticada del usuario.",
    };
  }
  if (/access denied|forbidden|403|cloudflare|just a moment/.test(blob)) {
    return {
      kind: "anti_bot",
      detail: "Anti-bot (Cloudflare u otro) bloqueó la petición.",
    };
  }
  return null;
}

const BROWSER_TOOLS = [
  {
    type: "function",
    function: {
      name: "navigate",
      description: "Navega a una URL en el navegador.",
      parameters: {
        type: "object",
        properties: { url: { type: "string", description: "URL completa a visitar" } },
        required: ["url"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "click",
      description: "Hace clic en un elemento del DOM por selector CSS.",
      parameters: {
        type: "object",
        properties: { selector: { type: "string", description: "Selector CSS del elemento" } },
        required: ["selector"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "type",
      description: "Escribe texto en un campo de formulario por selector CSS.",
      parameters: {
        type: "object",
        properties: {
          selector: { type: "string", description: "Selector CSS del campo" },
          text: { type: "string", description: "Texto a escribir" },
        },
        required: ["selector", "text"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "scroll",
      description: "Hace scroll hacia abajo en la página actual.",
      parameters: { type: "object", properties: {} },
    },
  },
  {
    type: "function",
    function: {
      name: "extract",
      description: "Extrae el texto visible de un selector CSS (máximo 4000 caracteres).",
      parameters: {
        type: "object",
        properties: {
          selector: { type: "string", description: "Selector CSS del elemento a extraer" },
        },
        required: ["selector"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "screenshot",
      description: "Toma un screenshot de la página actual sin navegar.",
      parameters: { type: "object", properties: {} },
    },
  },
  {
    type: "function",
    function: {
      name: "finish",
      description: "Declara la tarea terminada y devuelve un resumen al usuario.",
      parameters: {
        type: "object",
        properties: {
          summary: { type: "string", description: "Resumen del resultado para el usuario" },
        },
        required: ["summary"],
      },
    },
  },
];

async function executeBrowserAction(action: string, args: Record<string, string>, currentUrl: string) {
  if (!browserlessApiKey) throw new Error("BROWSERLESS_API_KEY is not configured");
  const url = action === "navigate" ? (args.url ?? currentUrl) : currentUrl;
  // Anti-bot stack for /function REST endpoint. Note: solveCaptchas, humanlike
  // and blockConsentModals are not supported on /function — they require
  // CDP/BQL mode. Stealth fingerprint + residential US proxy is the best we
  // can do here and bypasses most non-Google captchas.
  const browserlessQs = new URLSearchParams({
    token: browserlessApiKey,
    stealth: "true",
    blockAds: "true",
    proxy: "residential",
    proxyCountry: "us",
    timeout: "60000",
  }).toString();
  const response = await fetch(`${browserlessEndpoint}/function?${browserlessQs}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      code: browserlessFunctionCode(),
      context: {
        action,
        url,
        selector: args.selector ?? "",
        text: args.text ?? "",
        extractSelector: args.selector ?? "body",
      },
    }),
  });

  const data = await response.json().catch(() => null);
  if (!response.ok || data?.data?.error) {
    throw new Error(data?.data?.error ?? data?.error ?? `Browserless failed: ${response.status}`);
  }

  return {
    finalUrl: data?.data?.finalUrl ?? url,
    title: data?.data?.title ?? "",
    extracted: data?.data?.extracted ?? "",
    blocked: classifyBrowserBlock({
      url: data?.data?.finalUrl ?? url,
      title: data?.data?.title ?? "",
      extracted: data?.data?.extracted ?? data?.data?.blockedDetails ?? "",
    }),
    typed: Boolean(data?.data?.typed),
    clicked: Boolean(data?.data?.clicked),
    screenshotBase64: data?.data?.screenshotBase64 ?? "",
  };
}

type BrowserAgentInput = {
  objective: string;
  provider: LlmProviderId;
  sessionId: string;
  startUrl: string;
  maxSteps: number;
};

type BrowserAgentEvent =
  | { event: "start"; data: { sessionId: string; objective: string; startUrl: string; maxSteps: number } }
  | { event: "thought"; data: { step: number; status: "thinking" } }
  | { event: "tool_call"; data: { step: number; action: string; args: Record<string, string>; parsedFromText: boolean } }
  | { event: "tool_result"; data: { step: number; action: string; args: Record<string, string>; ok: boolean; url: string; title?: string; blocked?: unknown; error?: string; extractedPreview?: string } }
  | { event: "retry_prompt"; data: { step: number; attempt: number; maxAttempts: number; reason: string } }
  | { event: "finish_rejected"; data: { step: number; prematureFinishRejections: number; fallback: string | null } }
  | { event: "finish"; data: { sessionId: string; objective: string; provider: LlmProviderId; summary: string; steps: unknown[]; stepsCount: number; finalUrl: string; screenshotDataUrl: string | null; completedAt: string } }
  | { event: "error"; data: { message: string } };

async function* runBrowserAgentSteps(input: BrowserAgentInput): AsyncGenerator<BrowserAgentEvent> {
  const { objective, provider, sessionId, startUrl, maxSteps } = input;

  const systemPrompt =
    "Eres un agente autónomo de navegador web tipo Manus. Tu trabajo es COMPLETAR la tarea del usuario, no describirla. Tienes herramientas: navigate, click, type, scroll, extract, screenshot, finish.\n\n" +
    "REGLAS DE FORMATO (críticas):\n" +
    "1. Emite herramientas SOLO mediante el campo `tool_calls` de la API. Nunca como texto.\n" +
    "2. PROHIBIDO `<function=...>`, `<tool_call>`, `<|python_tag|>`, `<navigate ...>`, `name{...}` u otras imitaciones textuales.\n" +
    "3. PROHIBIDO inventar resultados, screenshots o navegaciones. Solo el resultado real de la tool cuenta.\n" +
    "4. UNA tool_call por turno.\n\n" +
    "REGLAS DE PERSISTENCIA (críticas):\n" +
    "5. NO te rindas tras un fallo. Reintenta con estrategia DIFERENTE: otra URL, otro selector, otra herramienta, otro motor de búsqueda.\n" +
    "6. Si Google bloquea con captcha (URL contiene /sorry/index o muestra reCAPTCHA), NO termines. El infraestructura tiene `solveCaptchas=true` activo — espera al re-extract automático. Si tras 2 intentos sigue bloqueado, usa una ALTERNATIVA inmediatamente.\n" +
    "7. ALTERNATIVAS de búsqueda (sin captcha): `https://duckduckgo.com/?q=...`, `https://html.duckduckgo.com/html/?q=...` (versión sin JS, robusta), `https://www.bing.com/search?q=...`, `https://search.brave.com/search?q=...`. Úsalas SIN preguntar cuando Google falle.\n" +
    "8. Si un selector CSS no funciona (timeout), prueba un selector más genérico (`input[type=text]`, `input[name=q]`, `textarea`, `button[type=submit]`). No abandones por un selector roto.\n" +
    "9. Llama `finish` SOLO cuando: (a) la tarea esté demostrada por extract real, o (b) tras 4+ intentos diferentes haya quedado claro que es imposible. Nunca llames `finish` por el primer obstáculo.\n" +
    "10. Si no sabes qué hacer, usa `extract` con selector `body` o `screenshot` para ver el estado real y replanificar.\n\n" +
    "ESTRATEGIA estándar para 'busca X':\n" +
    "  1) navigate a `https://duckduckgo.com/?q=X` (más fiable que Google)\n" +
    "  2) extract con selector `[data-testid=\"result\"], .result, body` para ver resultados\n" +
    "  3) finish con un summary que liste los hallazgos reales del extract\n\n" +
    "Razona brevemente en español, luego emite la tool_call.";

  yield { event: "start", data: { sessionId, objective, startUrl, maxSteps } };

  const messages: LlmMessage[] = [
    { role: "system", content: systemPrompt },
    { role: "user", content: `Objetivo: ${objective}\nURL inicial: ${startUrl}` },
  ];

  const steps: unknown[] = [];
  let currentUrl = startUrl;
  let lastScreenshot = "";
  let finalSummary = "";
  let done = false;
  let noToolCallRetries = 0;
  let blockedCount = 0;
  let successfulExtractCount = 0;
  let prematureFinishRejections = 0;
  const MAX_PREMATURE_FINISH_REJECTIONS = 2;
  const triedFallbacks: string[] = [];

  // Trim message history to keep LLM context (and latency) small.
  const trimMessages = () => {
    // Always keep system (idx 0) + initial user (idx 1). Keep last 6 turns after that.
    if (messages.length > 12) {
      const head = messages.slice(0, 2);
      const tail = messages.slice(messages.length - 6);
      messages.length = 0;
      messages.push(...head, ...tail);
    }
  };
  const SEARCH_FALLBACKS = [
    "https://html.duckduckgo.com/html/?q=",
    "https://duckduckgo.com/?q=",
    "https://www.bing.com/search?q=",
    "https://search.brave.com/search?q=",
  ];
  const pickNextFallback = (q: string): string | null => {
    for (const base of SEARCH_FALLBACKS) {
      if (!triedFallbacks.includes(base)) {
        triedFallbacks.push(base);
        return `${base}${encodeURIComponent(q)}`;
      }
    }
    return null;
  };

  for (let step = 0; step < maxSteps && !done; step++) {
    // Try with tool_choice: required first (forces real tool_call).
    // On Groq "did not call a tool" error → retry with tool_choice: auto + text parser.
    // On rate-limit error → sleep parsed seconds then retry once.
    // When we've extracted enough data, skip the LLM entirely and synthesize
    // a finish call so we always return useful data without burning more tokens.
    const FORCE_FINISH_AT = 3;
    const baseChoice: unknown = "required";
    const callOnce = (toolChoice: unknown) =>
      callOpenAiCompatibleProvider({
        provider,
        messages,
        maxTokens: 200,
        tools: BROWSER_TOOLS,
        toolChoice,
      });

    trimMessages();
    yield { event: "thought", data: { step: step + 1, status: "thinking" } };

    const isNoToolError = (m: string) =>
      /Failed to call a function|Tool choice is required|did not call a tool|no tool was called|Failed to parse tool call arguments|tool call validation failed/i.test(m);
    const isRateLimit = (m: string) => /Rate limit reached|429/i.test(m);
    const parseRetryAfterMs = (m: string): number => {
      const sec = m.match(/try again in ([\d.]+)\s*s/i);
      if (sec) return Math.min(20000, Math.ceil(parseFloat(sec[1]) * 1000) + 500);
      return 3000;
    };

    // Helper: if Groq rejects because the model tried a different tool than
    // the forced one, synthesize a finish call so we always return useful data.
    const synthesizeFinish = (): Awaited<ReturnType<typeof callOpenAiCompatibleProvider>> => ({
      provider,
      model: "synthetic",
      content: "",
      finishReason: "stop",
      toolCalls: [
        {
          id: `forced-${crypto.randomUUID()}`,
          type: "function",
          function: {
            name: "finish",
            arguments: JSON.stringify({
              summary:
                `Búsqueda completada. Se ejecutaron ${steps.length} pasos y ` +
                `${successfulExtractCount} extracciones exitosas con datos reales.`,
            }),
          },
        },
      ] as never,
    });

    let llmResult: Awaited<ReturnType<typeof callOpenAiCompatibleProvider>>;
    // Short-circuit: if we already have plenty of extractions, finish without
    // calling the LLM. Saves tokens and avoids cycle/rate-limit spirals.
    if (successfulExtractCount >= FORCE_FINISH_AT) {
      llmResult = synthesizeFinish();
    } else try {
      llmResult = await callOnce(baseChoice);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (isRateLimit(msg)) {
        await new Promise((r) => setTimeout(r, parseRetryAfterMs(msg)));
        try {
          llmResult = await callOnce(baseChoice);
        } catch (err2) {
          const msg2 = err2 instanceof Error ? err2.message : String(err2);
          if (isNoToolError(msg2)) {
            llmResult = await callOnce("auto").catch((e3) => {
              throw new Error(`LLM error at step ${step + 1} (rate+auto): ${e3 instanceof Error ? e3.message : String(e3)}`);
            });
          } else {
            throw new Error(`LLM error at step ${step + 1} (after rate retry): ${msg2}`);
          }
        }
      } else if (isNoToolError(msg)) {
        try {
          llmResult = await callOnce("auto");
        } catch (e3) {
          const m3 = e3 instanceof Error ? e3.message : String(e3);
          if (isNoToolError(m3) || isRateLimit(m3)) {
            // Out of patience: synthesize a finish so user gets a result.
            llmResult = synthesizeFinish();
          } else {
            throw new Error(`LLM error at step ${step + 1} (auto fallback): ${m3}`);
          }
        }
      } else {
        throw new Error(`LLM error at step ${step + 1}: ${msg}`);
      }
    }

    let toolCalls = llmResult.toolCalls;
    let parsedFromText = false;
    const rawContent = llmResult.content ?? "";

    // Fallback: if the model hallucinated tool calls as plain text, reparse.
    if (!toolCalls || toolCalls.length === 0) {
      const parsedToolCall = parseBrowserToolCallFromText(rawContent);
      if (parsedToolCall) {
        toolCalls = [parsedToolCall] as never;
        parsedFromText = true;
      }
    }

    if (!toolCalls || toolCalls.length === 0) {
      // No tool call — treat as final text answer (sanitized)
      if (noToolCallRetries < NO_TOOL_CALL_RETRY_LIMIT) {
        noToolCallRetries += 1;
        const retryMessage = buildNoToolCallRetryMessage({
          objective,
          currentUrl,
          rawContent,
          attempt: noToolCallRetries,
          maxAttempts: NO_TOOL_CALL_RETRY_LIMIT,
        });
        messages.push({ role: "assistant", content: sanitizeAgentText(rawContent) });
        messages.push({ role: "user", content: retryMessage });
        steps.push({
          step: step + 1,
          action: "retry_prompt",
          ok: false,
          url: currentUrl,
          reason: "missing_tool_call",
        });
        yield {
          event: "retry_prompt",
          data: { step: step + 1, attempt: noToolCallRetries, maxAttempts: NO_TOOL_CALL_RETRY_LIMIT, reason: "missing_tool_call" },
        };
        continue;
      }

      finalSummary =
        sanitizeAgentText(rawContent) ||
        `No pude completar la tarea porque el modelo dejo de emitir herramientas. URL final: ${currentUrl}`;
      done = true;
      break;
    }

    const toolCall = toolCalls[0];
    const toolName: string = toolCall?.function?.name ?? "screenshot";
    let toolArgs: Record<string, string> = {};
    try {
      toolArgs = JSON.parse(toolCall?.function?.arguments ?? "{}");
    } catch (_) { /* ignore parse errors */ }

    yield {
      event: "tool_call",
      data: { step: step + 1, action: toolName, args: toolArgs, parsedFromText },
    };

    if (toolName === "finish") {
      const rawSummary = typeof toolArgs.summary === "string" ? toolArgs.summary : "";
      // Reject premature finish: if agent is finishing without ever extracting
      // useful data AND blocked count is low, force it to try alternatives.
      const isPremature =
        successfulExtractCount === 0 &&
        blockedCount < 3 &&
        prematureFinishRejections < MAX_PREMATURE_FINISH_REJECTIONS;
      if (isPremature) {
        prematureFinishRejections += 1;
        const fallback = pickNextFallback(objective);
        const coach =
          `RECHAZADO: te rindes muy pronto. No has extraído datos reales todavía. ` +
          `Intentos bloqueados: ${blockedCount}. Extracciones exitosas: ${successfulExtractCount}. ` +
          `Reintentos rechazados: ${prematureFinishRejections}/${MAX_PREMATURE_FINISH_REJECTIONS}.\n\n` +
          (fallback
            ? `Usa esta alternativa AHORA con navigate: ${fallback}\nDespués extract con selector "body" para ver resultados, luego finish con los hallazgos reales.`
            : `Cambia de estrategia: prueba otro selector, otra URL conocida del dominio, o haz scroll y extract de nuevo.`);
        messages.push({ role: "assistant", content: sanitizeAgentText(rawContent) });
        messages.push({ role: "user", content: coach });
        steps.push({
          step: step + 1,
          action: "finish_rejected",
          args: toolArgs,
          url: currentUrl,
          reason: "premature_finish_no_extract",
          parsedFromText,
        });
        yield {
          event: "finish_rejected",
          data: { step: step + 1, prematureFinishRejections, fallback },
        };
        continue;
      }

      finalSummary = sanitizeAgentText(rawSummary) || "Tarea completada.";
      done = true;

      messages.push({
        role: "assistant",
        content: sanitizeAgentText(rawContent),
      });
      steps.push({ step: step + 1, action: "finish", args: toolArgs, url: currentUrl, parsedFromText });
      break;
    }

    // Execute browser action
    let observation = "";
    let suggestion = "";
    let toolResultEvent: BrowserAgentEvent | null = null;
    try {
      const browserResult = await executeBrowserAction(toolName, toolArgs, currentUrl);
      currentUrl = browserResult.finalUrl || currentUrl;
      lastScreenshot = browserResult.screenshotBase64;
      if (browserResult.extracted && browserResult.extracted.length > 15) {
        successfulExtractCount += 1;
      }
      if (browserResult.blocked) {
        blockedCount += 1;
        const fallback = pickNextFallback(objective);
        suggestion = fallback
          ? `\n\nESTE SITIO ESTÁ BLOQUEADO. NO te rindas. Llama navigate AHORA con esta URL alternativa: ${fallback}`
          : `\n\nESTE SITIO ESTÁ BLOQUEADO y ya probaste las alternativas conocidas. Intenta una variante del query o un dominio especializado.`;
      }
      const screenshotKb = Math.round((lastScreenshot.length * 3) / 4 / 1024);
      observation =
        `Accion "${toolName}" ejecutada.\n` +
        `URL actual: ${currentUrl}\n` +
        `Titulo: ${browserResult.title}\n` +
        (browserResult.blocked ? `Bloqueo externo detectado: ${browserResult.blocked.detail}\n` : "") +
        (browserResult.extracted ? `Texto extraido: ${browserResult.extracted}\n` : "") +
        `[Screenshot capturado (${screenshotKb} KB)]`;

      steps.push({
        step: step + 1,
        action: toolName,
        args: toolArgs,
        url: currentUrl,
        title: browserResult.title,
        blocked: browserResult.blocked,
        ok: true,
        parsedFromText,
      });
      toolResultEvent = {
        event: "tool_result",
        data: {
          step: step + 1,
          action: toolName,
          args: toolArgs,
          ok: true,
          url: currentUrl,
          title: browserResult.title,
          blocked: browserResult.blocked,
          extractedPreview: typeof browserResult.extracted === "string" ? browserResult.extracted.slice(0, 200) : "",
        },
      };
    } catch (err) {
      const errMsg = err instanceof Error ? err.message : String(err);
      observation = `Error ejecutando "${toolName}": ${errMsg}`;
      // On selector timeout, hint at more generic selectors
      if (/waiting for selector|Timeout|wait_for_selector/i.test(errMsg) && toolArgs.selector) {
        suggestion =
          `\n\nEl selector "${toolArgs.selector}" no aparece. Prueba selectores más genéricos: ` +
          `input[name=q], input[type=search], input[type=text], textarea, button[type=submit]. ` +
          `O usa extract con selector "body" para ver qué hay realmente en la página.`;
      }
      steps.push({ step: step + 1, action: toolName, args: toolArgs, ok: false, error: observation, parsedFromText });
      toolResultEvent = {
        event: "tool_result",
        data: { step: step + 1, action: toolName, args: toolArgs, ok: false, url: currentUrl, error: errMsg },
      };
    }
    if (toolResultEvent) yield toolResultEvent;

    // Push neutral narration + observation. Avoid pseudo-syntax the model might parrot.
    messages.push({
      role: "assistant",
      content: `Ejecuté la herramienta ${toolName}.`,
    });
    let guidance: string;
    if (suggestion) {
      guidance = `Resultado:\n${observation}${suggestion}\n\nEmite la siguiente tool_call AHORA.`;
    } else if (successfulExtractCount >= 3) {
      guidance =
        `Resultado:\n${observation}\n\n` +
        `Ya tienes ${successfulExtractCount} extracciones exitosas. ` +
        `BASTA de extract — llama finish AHORA con un summary que resuma los datos reales obtenidos. ` +
        `No repitas extracts con selectores similares.`;
    } else if (successfulExtractCount >= 1 && toolName === "extract") {
      guidance =
        `Resultado:\n${observation}\n\n` +
        `Tienes datos. Si son suficientes para responder el objetivo, llama finish. ` +
        `Si necesitas más detalle de OTRA parte de la página, usa un selector DIFERENTE (no repitas el último).`;
    } else {
      guidance =
        `Resultado:\n${observation}\n\n` +
        `Si tienes datos suficientes, llama finish con resumen real. ` +
        `Si no, emite la siguiente tool_call (no te rindas).`;
    }
    messages.push({ role: "user", content: guidance });
  }

  finalSummary = sanitizeAgentText(finalSummary);
  if (!finalSummary) {
    finalSummary = steps.length > 0
      ? `No pude confirmar que la tarea terminara antes de agotar los pasos disponibles. URL final: ${currentUrl}`
      : "No pude completar la tarea: no se ejecuto ninguna accion del navegador.";
  }

  const finalPayload = {
    sessionId,
    objective,
    summary: finalSummary,
    steps,
    stepsCount: steps.length,
    finalUrl: currentUrl,
    screenshotDataUrl: lastScreenshot ? `data:image/png;base64,${lastScreenshot}` : null,
    provider,
    completedAt: new Date().toISOString(),
  };

  await redis.set(`okm:agent:browser:${sessionId}`, JSON.stringify({ ...finalPayload, screenshotDataUrl: undefined }), { EX: 3600 });

  yield { event: "finish", data: finalPayload };
}

function parseBrowserAgentBody(body: Record<string, unknown>): { ok: true; input: BrowserAgentInput } | { ok: false; error: string; status: number } {
  const objective = stringField(body.objective ?? body.prompt, "");
  if (!objective) return { ok: false, error: "objective is required", status: 400 };
  const provider = normalizeLlmProvider(body.provider);
  const sessionId = stringField(body.sessionId, `agent-${crypto.randomUUID()}`);
  const startUrl = stringField(body.url, DEFAULT_BROWSER_START_URL);
  const maxSteps = Math.min(numberField(body.maxSteps, REACT_MAX_STEPS, 1, REACT_MAX_STEPS), REACT_MAX_STEPS);
  return { ok: true, input: { objective, provider, sessionId, startUrl, maxSteps } };
}

app.post("/v1/agent/browser/run", async ({ request }) => {
  if (!browserlessApiKey) return makeJsonResponse({ error: "BROWSERLESS_API_KEY is not configured" }, 503);

  const body = await readJsonObject(request).catch(() => ({}));
  const parsed = parseBrowserAgentBody(body);
  if (!parsed.ok) return makeJsonResponse({ error: parsed.error }, parsed.status);

  let finalPayload: unknown = null;
  for await (const evt of runBrowserAgentSteps(parsed.input)) {
    if (evt.event === "finish") finalPayload = evt.data;
  }
  return finalPayload ?? { error: "agent did not produce a result" };
});

function sseFrame(event: string, data: unknown): string {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

app.post("/v1/agent/browser/stream", async ({ request }) => {
  if (!browserlessApiKey) return makeJsonResponse({ error: "BROWSERLESS_API_KEY is not configured" }, 503);

  const body = await readJsonObject(request).catch(() => ({}));
  const parsed = parseBrowserAgentBody(body);
  if (!parsed.ok) return makeJsonResponse({ error: parsed.error }, parsed.status);

  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const write = (chunk: string) => controller.enqueue(encoder.encode(chunk));
      try {
        for await (const evt of runBrowserAgentSteps(parsed.input)) {
          write(sseFrame(evt.event, evt.data));
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        write(sseFrame("error", { message }));
      } finally {
        write("data: [DONE]\n\n");
        controller.close();
      }
    },
  });

  return new Response(stream, {
    headers: {
      ...productCorsHeaders,
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      "connection": "keep-alive",
      "x-accel-buffering": "no",
    },
  });
});

app.post("/v1/computer/browserless/action", async ({ request }) => {
  if (!browserlessApiKey) return makeJsonResponse({ error: "BROWSERLESS_API_KEY is not configured" }, 503);

  const body = await readJsonObject(request).catch(() => ({}));
  const instruction = stringField(body.instruction, "");
  const email = stringField(body.email, extractEmail(instruction));
  const requestedUrl = stringField(body.url, /gmail/i.test(instruction) ? "https://gmail.com" : DEFAULT_BROWSER_START_URL);
  const action = email ? "type-email" : stringField(body.action, "view");
  // Anti-bot stack for /function REST endpoint. Note: solveCaptchas, humanlike
  // and blockConsentModals are not supported on /function — they require
  // CDP/BQL mode. Stealth fingerprint + residential US proxy is the best we
  // can do here and bypasses most non-Google captchas.
  const browserlessQs = new URLSearchParams({
    token: browserlessApiKey,
    stealth: "true",
    blockAds: "true",
    proxy: "residential",
    proxyCountry: "us",
    timeout: "60000",
  }).toString();
  const response = await fetch(`${browserlessEndpoint}/function?${browserlessQs}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      code: browserlessFunctionCode(),
      context: {
        action,
        email,
        url: requestedUrl,
      },
    }),
  });

  const data = await response.json().catch(() => null);
  if (!response.ok || data?.data?.error) {
    return makeJsonResponse({ error: data?.data?.error ?? data?.error ?? `Browserless action failed: ${response.status}` }, response.ok ? 500 : response.status);
  }

  const payload = {
    provider: "browserless",
    action,
    emailTyped: Boolean(data?.data?.emailTyped),
    url: data?.data?.finalUrl ?? requestedUrl,
    title: data?.data?.title ?? "",
    contentType: "image/png",
    screenshotDataUrl: `data:image/png;base64,${data?.data?.screenshotBase64 ?? ""}`,
    capturedAt: new Date().toISOString(),
  };

  await redis.set("okm:computer:browserless:last_action", JSON.stringify({ ...payload, screenshotDataUrl: undefined }), { EX: 3600 });
  return payload;
});

// Endpoint /pipeline: ejecuta N comandos en paralelo sin atomicidad
// Cada comando tiene su propio resultado/error independiente
// El rate limit cuenta cada comando del batch por separado
app.post("/pipeline", async ({ request }) => {
  let commands: Command[];
  try {
    commands = parseCommandBatch(await parseJsonBodyWithLimit(request));
  } catch (error) {
    return upstashError(error);
  }

  const auth = authorize(request, commands.map(commandName));
  if (!auth.ok) return upstashError(auth.error, auth.status);

  const rateLimit = checkRateLimit(request, commands.length);
  if (!rateLimit.ok) return makeJsonResponse(formatError(rateLimit.error), rateLimit.status, rateLimitHeaders(rateLimit));

  return makeJsonResponse(await executePipeline(commands), 200, rateLimitHeaders(rateLimit));
});

// Endpoint /multi-exec: ejecuta N comandos como transacción atómica Redis (MULTI/EXEC)
// O todos tienen éxito o ninguno se aplica; el rate limit también cuenta por comando
app.post("/multi-exec", async ({ request }) => {
  let commands: Command[];
  try {
    commands = parseCommandBatch(await parseJsonBodyWithLimit(request));
  } catch (error) {
    return upstashError(error);
  }

  const auth = authorize(request, commands.map(commandName));
  if (!auth.ok) return upstashError(auth.error, auth.status);

  const rateLimit = checkRateLimit(request, commands.length);
  if (!rateLimit.ok) return makeJsonResponse(formatError(rateLimit.error), rateLimit.status, rateLimitHeaders(rateLimit));

  return makeJsonResponse(await executeTransaction(commands), 200, rateLimitHeaders(rateLimit));
});

// Endpoint comodín: maneja cualquier ruta como /<comando>/<arg1>/<arg2>/...
// Soporta GET /GET/clave, POST /SET/clave con body=valor, etc.
// HEAD devuelve solo headers sin body (útil para verificar disponibilidad)
app.all("/*", async ({ request, params }) => {
  if (!allowedMethods.has(request.method)) {
    return upstashError("Method Not Allowed", 405);
  }

  let command: Command;
  try {
    command = await parseCommandFromRequest(request, params["*"]);
  } catch (error) {
    return upstashError(error);
  }

  const auth = authorize(request, [commandName(command)]);
  if (!auth.ok) return upstashError(auth.error, auth.status);

  const rateLimit = checkRateLimit(request);
  const rlHeaders = rateLimitHeaders(rateLimit);
  if (!rateLimit.ok) return makeJsonResponse(formatError(rateLimit.error), rateLimit.status, rlHeaders);

  try {
    const reply = await executeCommand(command);
    // HEAD: solo confirma que el comando funcionó, sin devolver datos
    if (request.method === "HEAD") return new Response(null, { headers: rlHeaders });
    const response = makeRedisResponse(reply, request);
    // Agrega headers de rate limit a la respuesta ya construida
    for (const [key, value] of Object.entries(rlHeaders)) {
      response.headers.set(key, value);
    }
    return response;
  } catch (error) {
    return upstashError(error);
  }
});

// ──────────────────────────────────────────────────────────────────────────
// Poe OAuth 2.0 (Authorization Code + PKCE)
// Flow:
//   1. Browser hits /v1/auth/poe/start → 302 to Poe authorize URL with PKCE.
//   2. Poe redirects to /v1/auth/poe/callback?code=...&state=... after user consents.
//   3. We exchange code for access_token via Poe token endpoint.
//   4. Token is stored in Redis under okm:poe:user:<external_user_id> for 30 days.
//   5. Browser is redirected back to OKMANUS_FRONTEND_ORIGIN/?poe_auth=ok
// ──────────────────────────────────────────────────────────────────────────

const poeClientId = process.env.POE_CLIENT_ID ?? "";
const poeClientSecret = process.env.POE_CLIENT_SECRET ?? "";
const poeRedirectUri =
  process.env.POE_REDIRECT_URI ?? "http://localhost:3001/v1/auth/poe/callback";
const poeAuthorizeUrl = process.env.POE_AUTHORIZE_URL ?? "https://poe.com/oauth/authorize";
const poeTokenUrl = process.env.POE_TOKEN_URL ?? "https://poe.com/oauth/token";
const poeUserinfoUrl = process.env.POE_USERINFO_URL ?? "https://poe.com/api/oauth/userinfo";
const poeScopes = process.env.POE_SCOPES ?? "read user:read messages:write";
const okmanusFrontendOrigin =
  process.env.OKMANUS_FRONTEND_ORIGIN ?? "http://localhost:3000";

function base64UrlEncode(buf: Uint8Array): string {
  let bin = "";
  for (const b of buf) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function sha256Base64Url(input: string): Promise<string> {
  const data = new TextEncoder().encode(input);
  const hash = await crypto.subtle.digest("SHA-256", data);
  return base64UrlEncode(new Uint8Array(hash));
}

app.get("/v1/auth/poe/start", async ({ request }) => {
  if (!poeClientId) {
    return makeJsonResponse(
      { error: "POE_CLIENT_ID is not configured on the server" },
      503,
    );
  }
  const url = new URL(request.url);
  // Optional ?return_to=/somepath to bounce back to a specific frontend page
  const returnTo = url.searchParams.get("return_to") || "/";

  // PKCE
  const verifierBytes = new Uint8Array(32);
  crypto.getRandomValues(verifierBytes);
  const codeVerifier = base64UrlEncode(verifierBytes);
  const codeChallenge = await sha256Base64Url(codeVerifier);

  // CSRF state
  const stateBytes = new Uint8Array(16);
  crypto.getRandomValues(stateBytes);
  const state = base64UrlEncode(stateBytes);

  // Persist state + verifier for 10 minutes
  await redis.set(
    `okm:poe:oauth_state:${state}`,
    JSON.stringify({ codeVerifier, returnTo, createdAt: Date.now() }),
    { EX: 600 },
  );

  const authorize = new URL(poeAuthorizeUrl);
  authorize.searchParams.set("response_type", "code");
  authorize.searchParams.set("client_id", poeClientId);
  authorize.searchParams.set("redirect_uri", poeRedirectUri);
  if (poeScopes.trim()) authorize.searchParams.set("scope", poeScopes);
  authorize.searchParams.set("state", state);
  authorize.searchParams.set("code_challenge", codeChallenge);
  authorize.searchParams.set("code_challenge_method", "S256");

  return new Response(null, {
    status: 302,
    headers: { location: authorize.toString(), "cache-control": "no-store" },
  });
});

app.get("/v1/auth/poe/callback", async ({ request }) => {
  const url = new URL(request.url);
  const code = url.searchParams.get("code");
  const state = url.searchParams.get("state");
  const errorParam = url.searchParams.get("error");

  if (errorParam) {
    return makeJsonResponse(
      { error: `Poe OAuth denied: ${errorParam}`, description: url.searchParams.get("error_description") },
      400,
    );
  }
  if (!code || !state) {
    return makeJsonResponse({ error: "Missing 'code' or 'state' query parameter" }, 400);
  }

  // Validate state + retrieve PKCE verifier
  const stored = await redis.get(`okm:poe:oauth_state:${state}`);
  if (!stored || typeof stored !== "string") {
    return makeJsonResponse({ error: "Invalid or expired state" }, 400);
  }
  await redis.del(`okm:poe:oauth_state:${state}`);

  let codeVerifier = "";
  let returnTo = "/";
  try {
    const parsed = JSON.parse(stored);
    codeVerifier = String(parsed.codeVerifier || "");
    returnTo = String(parsed.returnTo || "/");
  } catch {
    return makeJsonResponse({ error: "Corrupt state payload" }, 400);
  }

  // Exchange code for token. Poe accepts standard form-encoded request.
  const tokenBody = new URLSearchParams({
    grant_type: "authorization_code",
    code,
    redirect_uri: poeRedirectUri,
    client_id: poeClientId,
    code_verifier: codeVerifier,
  });
  // If Poe issued a client_secret (confidential client), include it.
  if (poeClientSecret) tokenBody.set("client_secret", poeClientSecret);

  const tokenResp = await fetch(poeTokenUrl, {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      accept: "application/json",
    },
    body: tokenBody.toString(),
  });

  const tokenJson = await tokenResp.json().catch(() => null);
  if (!tokenResp.ok || !tokenJson || typeof tokenJson !== "object") {
    return makeJsonResponse(
      {
        error: "Token exchange failed",
        status: tokenResp.status,
        details: tokenJson,
      },
      502,
    );
  }

  const accessToken = String((tokenJson as Record<string, unknown>).access_token ?? "");
  const refreshToken = String((tokenJson as Record<string, unknown>).refresh_token ?? "");
  const expiresIn = Number((tokenJson as Record<string, unknown>).expires_in ?? 0) || 0;
  const tokenType = String((tokenJson as Record<string, unknown>).token_type ?? "Bearer");

  if (!accessToken) {
    return makeJsonResponse({ error: "Poe did not return access_token", details: tokenJson }, 502);
  }

  // Try to identify the Poe user (best-effort; may 404 if endpoint differs).
  let externalUserId = `poe-${crypto.randomUUID()}`;
  let userInfo: unknown = null;
  try {
    const ui = await fetch(poeUserinfoUrl, {
      headers: { authorization: `${tokenType} ${accessToken}`, accept: "application/json" },
    });
    if (ui.ok) {
      userInfo = await ui.json().catch(() => null);
      const idCandidate = (userInfo as Record<string, unknown>)?.id ?? (userInfo as Record<string, unknown>)?.user_id ?? (userInfo as Record<string, unknown>)?.sub;
      if (idCandidate) externalUserId = `poe-${String(idCandidate)}`;
    }
  } catch { /* userinfo optional */ }

  // Persist token for 30 days (or token expiry if longer).
  const ttlSeconds = Math.max(expiresIn, 30 * 24 * 3600);
  const record = {
    externalUserId,
    accessToken,
    refreshToken: refreshToken || null,
    tokenType,
    expiresAt: expiresIn ? new Date(Date.now() + expiresIn * 1000).toISOString() : null,
    issuedAt: new Date().toISOString(),
    userInfo,
  };
  await redis.set(`okm:poe:user:${externalUserId}`, JSON.stringify(record), { EX: ttlSeconds });

  // Bounce back to frontend with success marker (NEVER include token in URL).
  const back = new URL(okmanusFrontendOrigin);
  back.pathname = returnTo.startsWith("/") ? returnTo : "/";
  back.searchParams.set("poe_auth", "ok");
  back.searchParams.set("poe_user", externalUserId);
  return new Response(null, {
    status: 302,
    headers: { location: back.toString(), "cache-control": "no-store" },
  });
});

// Puerto y host configurables por variables de entorno; defaults seguros para contenedores
const port = Number(process.env.PORT ?? 3000);
const hostname = process.env.HOST ?? "0.0.0.0";

app.listen({
  port,
  hostname,
});

console.log(`OkManus Upstash-compatible Redis REST API listening on http://${hostname}:${port}`);

// Cierre limpio: desconecta Redis antes de terminar para evitar conexiones huérfanas
async function shutdown(signal: string) {
  console.log(`${signal} received, shutting down`);
  await redis.quit();
  process.exit(0);
}

// Captura Ctrl+C (SIGINT) y señal de terminación del sistema (SIGTERM)
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

export {
  appendQueryArguments,
  authorize,
  checkRateLimit,
  encodeResp,
  formatResult,
  normalizeCommand,
  parseCommandBatch,
  parseCommandFromRequest,
  readOnlyCommands,
};

export default app;
