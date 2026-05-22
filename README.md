# api-okmanus

API Redis REST compatible con Upstash para OkManus, hecha con Bun + Elysia.

Documentacion ampliada del proyecto completo:

- [OkManus Platform Guide](docs/OKMANUS_PLATFORM_GUIDE.md): arquitectura completa, API `/v1`, JSX, jobs, cron jobs, runners, sandboxes, MCPs, webhooks, casos de uso y seguridad.

La idea es poder desplegarla en Dokploy y usarla como reemplazo self-hosted de:

```bash
UPSTASH_REDIS_REST_URL=https://tu-dominio
UPSTASH_REDIS_REST_TOKEN=<token>
```

Por debajo habla con Redis, Valkey, Dragonfly o cualquier servicio compatible con Redis protocol.

## Compatibilidad Upstash

Soporta la forma principal de la REST API de Upstash:

- `GET /set/foo/bar`
- `GET /get/foo`
- `POST /set/foo` con el valor en el body
- `POST /` con body JSON tipo `["SET", "foo", "bar", "EX", 60]`
- `POST /pipeline`
- `POST /multi-exec`
- `Authorization: Bearer <token>`
- `?_token=<token>`
- respuestas JSON `{ "result": ... }` y `{ "error": ... }`
- `Upstash-Encoding: base64`
- `Upstash-Response-Format: resp2` para comandos individuales

No implementado todavía:

- SSE `MONITOR`
- SSE `SUBSCRIBE`
- `ACL RESTTOKEN`

## Variables

```bash
PORT=3000
HOST=0.0.0.0
APP_PORT=3000
REDIS_URL=redis://redis:6379/0

UPSTASH_REDIS_REST_TOKEN=change-me
UPSTASH_REDIS_REST_READONLY_TOKEN=change-me-readonly
OKMANUS_PRODUCT_CLIENT_TOKEN=dev-product-token

APPWRITE_ENDPOINT=https://<REGION>.cloud.appwrite.io/v1
APPWRITE_PROJECT_ID=
OKMANUS_REQUIRE_APPWRITE_SESSION=false
CREDENTIAL_CONSENT_TTL_SECONDS=2592000
OKMANUS_JOB_TTL_SECONDS=604800
OKMANUS_SESSION_QUEUE_TTL_SECONDS=86400
OKMANUS_QUEUE_MAX_JOBS=1000

OPENROUTER_API_KEY=
OPENROUTER_MODEL=openrouter/auto
GROQ_API_KEY=
GROQ_MODEL=llama-3.1-8b-instant
BROWSERLESS_API_KEY=
BROWSERLESS_ENDPOINT=https://production-sfo.browserless.io

RATE_LIMIT_WINDOW_SECONDS=60
RATE_LIMIT_MAX_REQUESTS=600
MAX_BATCH_COMMANDS=100
MAX_COMMAND_ARGS=256
MAX_ARG_BYTES=524288
MAX_REQUEST_BYTES=1048576
MAX_IN_FLIGHT_COMMANDS=50
REDIS_COMMAND_QUEUE_MAX_LENGTH=500
COMMAND_TIMEOUT_MS=5000
OKMANUS_ALLOW_DANGEROUS_COMMANDS=false
```

## Seguridad

La API exige token en todos los comandos Redis. `/health` y `/` son públicos.

Hay dos tokens:

- `UPSTASH_REDIS_REST_TOKEN`: token estándar con permisos de escritura.
- `UPSTASH_REDIS_REST_READONLY_TOKEN`: token solo lectura, opcional.

No expongas el token estándar en frontend público. Para clientes web usa solo el token readonly si el caso lo permite.

Los endpoints `/v1/*` aceptan `X-Appwrite-JWT` para asociar la peticion a una sesion real de Appwrite. Si configuras `OKMANUS_REQUIRE_APPWRITE_SESSION=true`, ademas del `OKMANUS_PRODUCT_CLIENT_TOKEN` se exigira una sesion Appwrite valida.

No guardes API keys de usuarios en Redis ni en el frontend. El endpoint `/v1/providers/test` solo recibe la clave para probarla en memoria y no la persiste.

### Consentimiento De Credenciales BYO

Los usuarios deben poder decidir proveedor por proveedor que claves entregan, donde se guardan y que scopes autorizan. OkManus solo guarda metadata de consentimiento y referencias externas:

```http
GET /v1/credential-vaults/options
GET /v1/credentials/consents
POST /v1/credentials/consents
DELETE /v1/credentials/consents/:providerId
```

`POST /v1/credentials/consents` rechaza campos como `apiKey`, `secretValue` o `token`. El cliente debe guardar la clave real en un vault y enviar solo `secretRef`.

Ejemplo:

```json
{
  "providerId": "openai",
  "providerName": "OpenAI",
  "vaultProvider": "infisical",
  "secretRef": "infisical://project/prod/OPENAI_API_KEY",
  "scopes": ["chat.completions", "responses"],
  "last4": "abcd",
  "ttlSeconds": 2592000
}
```

Dragonfly/Redis usa solo estado efimero:

```text
okm:user:{user_id}:credential_consents
okm:cred:{user_id}:{provider_id}
okm:user:{user_id}:credential_events
```

Todo tiene TTL. La fuente persistente futura deberia ser Postgres (`credential_connections`, `external_secret_refs`, `audit_events`).

### Jobs Y Colas De Sesion En Dragonfly

Segun las notas de Notion, Dragonfly/Redis se usa como plano efimero para aceptar jobs rapido, mover colas ligeras, publicar eventos y mantener estado vivo por sesion. Postgres debe ser la fuente de verdad duradera.

Endpoints iniciales:

```http
POST /v1/sessions
GET /v1/sessions/:sessionId/jobs
POST /v1/jobs
GET /v1/jobs
GET /v1/jobs/:jobId
GET /v1/jobs/:jobId/events
POST /v1/jobs/:jobId/cancel
POST /v1/jobs/:jobId/retry
POST /v1/queues/:plan/:taskType/:region/next
```

Keyspace efimero:

```text
okm:session:{session_id}
okm:session:{session_id}:jobs
okm:job:{job_id}
okm:job:{job_id}:events
okm:q:{plan}:{task_type}:{region}
okm:user:{user_id}:jobs
```

`POST /v1/jobs` guarda metadata corta en un hash, mete el `job_id` en `okm:q:{plan}:{task_type}:{region}`, anexa el job a la cola de la sesion y publica `job.queued` en un stream. Los runners pueden tomar trabajo con `/v1/queues/:plan/:taskType/:region/next`, que marca el job como `leased`.

Por defecto se bloquean comandos peligrosos o caros:

```text
ACL, BGSAVE, CLIENT, CLUSTER, COMMAND, CONFIG, DEBUG, FLUSHALL,
FLUSHDB, KEYS, MEMORY, MODULE, RESTORE, SAVE, SCRIPT, SHUTDOWN,
SLOWLOG, SYNC, ...
```

Para habilitarlos:

```bash
OKMANUS_ALLOW_DANGEROUS_COMMANDS=true
```

No lo actives en despliegues públicos salvo que la API esté detrás de red privada/VPN.

## Rate Limit

El rate limit se aplica por token. Si no hay token, se agrupa por IP.

Variables:

```bash
RATE_LIMIT_WINDOW_SECONDS=60
RATE_LIMIT_MAX_REQUESTS=600
```

En `/pipeline` y `/multi-exec`, cada comando cuenta como coste 1. Un batch de 20 comandos consume 20 unidades.

La respuesta incluye:

```text
X-RateLimit-Limit
X-RateLimit-Remaining
X-RateLimit-Reset
Retry-After
```

Cuando se supera el límite:

```json
{ "error": "ERR rate limit exceeded" }
```

## Límites De Recursos

Estos límites protegen el proceso Bun y Redis:

```bash
MAX_BATCH_COMMANDS=100
MAX_COMMAND_ARGS=256
MAX_ARG_BYTES=524288
MAX_REQUEST_BYTES=1048576
MAX_IN_FLIGHT_COMMANDS=50
REDIS_COMMAND_QUEUE_MAX_LENGTH=500
COMMAND_TIMEOUT_MS=5000
```

En `docker-compose.yml` también se fijan límites de contenedor:

- API: `0.50 CPU`, `256 MB`, `128 PIDs`
- Redis: `0.50 CPU`, `384 MB`, `128 PIDs`
- Redis `maxmemory=256mb`
- Dragonfly `cache_mode=true` para expulsar claves cuando se alcance `maxmemory`
- `nofile` limitado

Para producción, ajusta estos valores según RAM real del VPS.

## Dokploy

Para que Redis/Dragonfly se despliegue junto a la API, crea la app en Dokploy como **Docker Compose** y usa:

```text
docker-compose.dokploy.yml
```

Ese stack levanta dos servicios en la misma red privada:

- `api-okmanus`: expone la REST API compatible con Upstash.
- `dragonfly`: guarda los datos Redis en el volumen persistente `dragonfly-data`.

La API ya lleva:

```bash
REDIS_URL=redis://dragonfly:6379/0
```

No publiques el puerto `6379` en producción salvo que tengas una red privada/VPN. El compose de Dokploy no lo expone; solo la API puede hablar con Dragonfly.

Variables mínimas en Dokploy:

```bash
APP_PORT=3000
UPSTASH_REDIS_REST_TOKEN=<token-largo-y-aleatorio>
UPSTASH_REDIS_REST_READONLY_TOKEN=<token-readonly-opcional>
OKMANUS_PRODUCT_CLIENT_TOKEN=<token-producto-largo-y-aleatorio>
```

Health check:

```text
/health
```

Si Dokploy te permite definir límites de recursos, usa como punto de partida:

```text
CPU: 0.5 - 1 vCPU
RAM API: 256 - 512 MB
RAM Redis: 384 MB - 1 GB
```

Para un VPS pequeño, mantén `MAX_REQUEST_BYTES` bajo y no habilites comandos peligrosos.

## Docker Compose Local

```bash
docker compose up -d --build
```

Si el puerto local `3000` ya esta ocupado:

```powershell
$env:APP_PORT=3001
docker compose up -d --build
```

La API sigue escuchando en `3000` dentro del contenedor; `APP_PORT` solo cambia el puerto publicado en tu maquina.

Logs:

```bash
docker compose logs -f api-okmanus
```

Parar:

```bash
docker compose down
```

Parar y borrar datos Redis:

```bash
docker compose down -v
```

## Pruebas Manuales

```bash
curl http://localhost:3000/health
```

```bash
curl http://localhost:3000/set/foo/bar \
  -H "Authorization: Bearer change-me"
```

```bash
curl http://localhost:3000/get/foo \
  -H "Authorization: Bearer change-me"
```

```bash
curl -X POST http://localhost:3000 \
  -H "Authorization: Bearer change-me" \
  -H "Content-Type: application/json" \
  -d '["SET","json-command","ok","EX",60]'
```

```bash
curl -X POST http://localhost:3000/pipeline \
  -H "Authorization: Bearer change-me" \
  -H "Content-Type: application/json" \
  -d '[["SET","a","1"],["GET","a"]]'
```

```bash
curl -X POST http://localhost:3000/multi-exec \
  -H "Authorization: Bearer change-me" \
  -H "Content-Type: application/json" \
  -d '[["SET","counter","1"],["INCR","counter"]]'
```

Rate limit rápido:

```bash
for i in {1..10}; do
  curl -i http://localhost:3000/ping \
    -H "Authorization: Bearer change-me"
done
```

Comando bloqueado:

```bash
curl http://localhost:3000/keys/* \
  -H "Authorization: Bearer change-me"
```

Respuesta esperada:

```json
{ "error": "ERR command 'KEYS' is disabled by server policy" }
```

## Uso Desde Clientes Upstash

Muchas integraciones que aceptan REST URL + token pueden apuntar a esta API:

```bash
UPSTASH_REDIS_REST_URL=https://redis.okmanus.tu-dominio.com
UPSTASH_REDIS_REST_TOKEN=<token>
```

Si el cliente usa comandos no implementados por REST o comandos bloqueados por política, fallará con `{ "error": ... }`.

## Keyspace Dragonfly Compartido

Usa un solo Dragonfly/Redis para muchos MCPs, con prefijos cortos y TTL:

```text
okm:registry:{kind}
okm:mcp:{mcp_id}:health
okm:mcp:{mcp_id}:session:{session_id}
okm:mcp:{mcp_id}:stream:{workspace_id}
okm:ws:{workspace_id}:rl:{metric}
okm:ws:{workspace_id}:lock:{resource_id}
okm:runner:{runner_id}:heartbeat
okm:job:{job_id}:progress
okm:job:{job_id}:events
okm:q:{plan}:{task_type}:{region}
```

Reglas: TTL en todo lo efimero, Streams para eventos re-jugables, `SCAN` para inspeccion, y Postgres como fuente persistente.

## Rol En OkManus

Según las notas de arquitectura de OkManus, esta API encaja como capa Redis/Valkey efímera:

- colas ligeras
- locks
- estado temporal
- rate limits
- coordinación de jobs
- heartbeats de runners
- eventos de corta duración

No debe guardar:

- historial definitivo
- artifacts
- screenshots grandes
- logs largos
- secretos persistentes
- fuente de verdad de jobs

La fuente de verdad debería seguir siendo Postgres, y los artifacts deberían ir a object storage.
