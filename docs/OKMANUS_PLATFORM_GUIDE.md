# OkManus Platform Guide

Documentacion de arquitectura, API, JSX y casos de uso para el proyecto OkManus.

Esta guia sintetiza las notas de Notion de OkManus y aterriza el contrato esperado para frontend, automatizaciones, jobs, crons, runners, sandboxes, MCPs, providers y la API Redis REST compatible con Upstash.

Documento complementario: [Autonomous browser agent architecture](AUTONOMOUS_BROWSER_AGENT_ARCHITECTURE.md).

## 1. Tesis Del Proyecto

OkManus no debe ser solo una API de texto ni una empresa que paga todo el computo. Debe ser una capa de coordinacion, verificacion y optimizacion para agentes de IA que corren sobre:

- infraestructura del usuario,
- runners locales o remotos,
- sandboxes efimeros,
- proveedores LLM del usuario,
- MCP servers directos,
- storage firmado,
- Redis/Valkey efimero,
- Postgres como fuente de verdad.

La regla central:

```text
OkManus coordina, enruta, verifica y observa.
Los runners, sandboxes, MCPs y providers ejecutan.
```

## 2. Capas Del Sistema

```text
Frontend JSX / SPA estatica
  -> Bootstrap / session manifest
  -> Control Plane API
      -> Auth / Workspaces / Policies
      -> Jobs / Crons / Webhooks
      -> Provider Registry
      -> Secret Proxy / Credential Resolver
      -> Scheduler
      -> Streaming Gateway
  -> Redis/Valkey efimero
      -> rate limits
      -> locks
      -> runner heartbeats
      -> job progress temporal
      -> colas ligeras
  -> Postgres
      -> users
      -> workspaces
      -> jobs
      -> cron_jobs
      -> runners
      -> providers
      -> credentials metadata
      -> audit_events
  -> Storage
      -> screenshots
      -> logs largos
      -> artifacts
      -> reports
  -> Execution Plane
      -> BYO runner
      -> VPS runner
      -> marketplace sandbox
      -> managed sandbox providers
      -> MCP servers
```

## 3. Contrato De API Principal

Todas las rutas de producto deben vivir bajo `/v1`.

### Health y capacidades

```http
GET /v1/health
GET /v1/capabilities
GET /v1/openapi.json
```

### Jobs

```http
POST /v1/jobs
GET /v1/jobs
GET /v1/jobs/:job_id
POST /v1/jobs/:job_id/cancel
POST /v1/jobs/:job_id/retry
GET /v1/jobs/:job_id/events
GET /v1/jobs/:job_id/artifacts
```

Body recomendado para `POST /v1/jobs`:

```json
{
  "task_type": "browser_debug",
  "prompt": "Reproduce el bug de login y verifica la home",
  "target": {
    "mode": "runner",
    "runner_id": "runner_123"
  },
  "runtime": {
    "sandbox_profile": "browser",
    "region": "eu-west",
    "timeout_seconds": 900
  },
  "llm": {
    "chain": ["groq", "openrouter"],
    "max_tokens": 30000,
    "compression": true
  },
  "artifacts": {
    "screenshots": true,
    "logs": true,
    "patches": true
  },
  "verification": {
    "enabled": true,
    "adapter": "web_app"
  },
  "webhook_url": "https://example.com/webhooks/okmanus"
}
```

Respuesta:

```json
{
  "job_id": "job_01HX...",
  "status": "queued",
  "created_at": "2026-05-13T18:00:00.000Z",
  "events_url": "/v1/jobs/job_01HX/events",
  "artifacts_url": "/v1/jobs/job_01HX/artifacts"
}
```

### Cron jobs / trabajos programados

Los cron jobs son definiciones persistentes que crean jobs reales segun una agenda. No ejecutan trabajo pesado dentro del scheduler: solo validan, crean un `job` y lo meten en cola.

```http
POST /v1/cron-jobs
GET /v1/cron-jobs
GET /v1/cron-jobs/:cron_id
PATCH /v1/cron-jobs/:cron_id
DELETE /v1/cron-jobs/:cron_id
POST /v1/cron-jobs/:cron_id/pause
POST /v1/cron-jobs/:cron_id/resume
POST /v1/cron-jobs/:cron_id/trigger
GET /v1/cron-jobs/:cron_id/runs
```

Body recomendado:

```json
{
  "name": "Nightly visual QA",
  "timezone": "Europe/Madrid",
  "schedule": {
    "type": "cron",
    "expression": "0 3 * * *"
  },
  "dedupe_key": "nightly-visual-qa:main",
  "enabled": true,
  "max_concurrent_runs": 1,
  "misfire_policy": "skip",
  "job_template": {
    "task_type": "visual_qa",
    "prompt": "Abre la preview, ejecuta el smoke test visual y genera reporte.",
    "target": {
      "mode": "best_available_runner"
    },
    "runtime": {
      "sandbox_profile": "browser",
      "timeout_seconds": 1200
    },
    "verification": {
      "enabled": true,
      "adapter": "web_app"
    }
  },
  "webhook_url": "https://example.com/webhooks/okmanus"
}
```

Politicas:

- `max_concurrent_runs=1` por defecto para evitar solapes.
- `misfire_policy=skip` para saltar ejecuciones perdidas si el sistema estuvo caido.
- `misfire_policy=run_once` para recuperar una sola ejecucion pendiente.
- `misfire_policy=catch_up` solo para jobs baratos e idempotentes.
- Cada ejecucion genera un `cron_run_id` y un `job_id`.
- El scheduler debe usar locks para no disparar dos veces el mismo cron.
- Los crons deben tener limite por workspace y plan.

Estados de cron:

```text
active
paused
disabled
error
deleted
```

Estados de run:

```text
scheduled
admitted
skipped
running
completed
failed
cancelled
```

### Runners

```http
POST /v1/runners/register
GET /v1/runners
GET /v1/runners/:runner_id
POST /v1/runners/:runner_id/heartbeat
POST /v1/runners/:runner_id/drain
POST /v1/runners/:runner_id/disable
GET /v1/runners/:runner_id/jobs
```

Heartbeat:

```json
{
  "runner_id": "runner_123",
  "version": "0.1.0",
  "region": "eu-west",
  "capacity": {
    "cpu": 4,
    "memory_mb": 8192,
    "max_jobs": 2
  },
  "active_jobs": 1,
  "supports": ["docker", "browser", "node", "bun"],
  "sandbox_isolation": "docker-rootless",
  "timestamp": "2026-05-13T18:00:00.000Z"
}
```

### Sandboxes

```http
POST /v1/sandboxes
GET /v1/sandboxes/:sandbox_id
POST /v1/sandboxes/:sandbox_id/terminate
GET /v1/sandboxes/:sandbox_id/artifacts
```

Reglas:

- un sandbox por job,
- usuario sin privilegios,
- limites CPU/RAM/PIDs,
- red allowlist cuando sea posible,
- cleanup verificable,
- tokens efimeros,
- nada sensible persiste tras terminar.

### Skills

```http
POST /v1/skills/run
GET /v1/skills
GET /v1/skills/:skill_id
```

Body:

```json
{
  "skill_id": "visual-verification",
  "input": {
    "url": "https://preview.example.com",
    "checks": ["home-loads", "login-visible", "no-console-errors"]
  },
  "target": {
    "mode": "runner",
    "runner_id": "runner_123"
  }
}
```

### MCP

```http
POST /v1/mcp/execute
GET /v1/mcp/servers
GET /v1/mcp/servers/:mcp_id
```

Body:

```json
{
  "mcp_id": "github",
  "method": "tools/search_repos",
  "params": {
    "query": "okmanus"
  },
  "idempotency_key": "mcp_01HX..."
}
```

### Webhooks

```http
POST /v1/webhooks
GET /v1/webhooks
PATCH /v1/webhooks/:webhook_id
DELETE /v1/webhooks/:webhook_id
POST /v1/webhooks/:webhook_id/test
GET /v1/webhook-deliveries
```

Eventos:

```text
job.queued
job.started
job.progress
job.artifact.created
job.verification_passed
job.verification_failed
job.completed
job.failed
cron.triggered
cron.skipped
runner.online
runner.offline
runner.capacity_changed
```

Todos los webhooks deben ir firmados:

```text
X-OkManus-Signature
X-OkManus-Timestamp
X-OkManus-Event
X-OkManus-Delivery-Id
```

## 4. Modelo De Datos Minimo

Tablas persistentes recomendadas:

```text
users
workspaces
workspace_members
api_keys
providers
credential_connections
external_secret_refs
jobs
job_events
job_artifacts
cron_jobs
cron_runs
runners
runner_heartbeats
sandbox_profiles
sandbox_runs
webhooks
webhook_deliveries
audit_events
usage_limits
```

Redis/Valkey debe guardar solo efimero:

```text
rl:token:{hash}
rl:workspace:{workspace_id}
lock:cron:{cron_id}
lock:runner:{runner_id}
runner:{runner_id}:heartbeat
runner:{runner_id}:capacity
job:{job_id}:progress
job:{job_id}:events
queue:{plan}:{type}:{region}
dedupe:job:{idempotency_key}
```

Todo lo efimero debe tener TTL.

## 5. JSX: Cliente Base

El frontend debe ser SPA estatica cuando sea posible. Evitar SSR para cada dashboard. El cliente debe hacer bootstrap una vez por sesion y despues enrutar localmente.

### Provider JSX

```tsx
import { createContext, useContext, useMemo, useState } from "react";

type OkManusConfig = {
  apiBaseUrl: string;
  token: string;
};

type OkManusContextValue = {
  apiBaseUrl: string;
  token: string;
  request<T>(path: string, init?: RequestInit): Promise<T>;
};

const OkManusContext = createContext<OkManusContextValue | null>(null);

export function OkManusProvider({
  apiBaseUrl,
  token,
  children,
}: React.PropsWithChildren<OkManusConfig>) {
  const value = useMemo<OkManusContextValue>(() => {
    return {
      apiBaseUrl,
      token,
      async request<T>(path, init) {
        const response = await fetch(`${apiBaseUrl}${path}`, {
          ...init,
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${token}`,
            ...init?.headers,
          },
        });

        const data = await response.json().catch(() => null);
        if (!response.ok) {
          throw new Error(data?.error ?? `OkManus request failed: ${response.status}`);
        }

        return data as T;
      },
    };
  }, [apiBaseUrl, token]);

  return <OkManusContext.Provider value={value}>{children}</OkManusContext.Provider>;
}

export function useOkManus() {
  const context = useContext(OkManusContext);
  if (!context) throw new Error("useOkManus must be used inside OkManusProvider");
  return context;
}
```

### Hook para crear jobs

```tsx
import { useState } from "react";
import { useOkManus } from "./OkManusProvider";

type CreateJobInput = {
  task_type: string;
  prompt: string;
  target?: {
    mode: "runner" | "best_available_runner" | "hosted";
    runner_id?: string;
  };
  verification?: {
    enabled: boolean;
    adapter?: "web_app" | "api" | "script";
  };
};

type JobCreated = {
  job_id: string;
  status: "queued";
  events_url: string;
  artifacts_url: string;
};

export function useCreateJob() {
  const okmanus = useOkManus();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function createJob(input: CreateJobInput) {
    setLoading(true);
    setError(null);

    try {
      return await okmanus.request<JobCreated>("/v1/jobs", {
        method: "POST",
        body: JSON.stringify(input),
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      throw err;
    } finally {
      setLoading(false);
    }
  }

  return { createJob, loading, error };
}
```

### Componente JSX para crear job

```tsx
import { FormEvent, useState } from "react";
import { useCreateJob } from "./useCreateJob";

export function JobComposer() {
  const [prompt, setPrompt] = useState("");
  const [taskType, setTaskType] = useState("browser_debug");
  const { createJob, loading, error } = useCreateJob();

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const job = await createJob({
      task_type: taskType,
      prompt,
      target: { mode: "best_available_runner" },
      verification: { enabled: true, adapter: "web_app" },
    });

    window.location.href = `/jobs/${job.job_id}`;
  }

  return (
    <form onSubmit={onSubmit}>
      <label>
        Tipo
        <select value={taskType} onChange={(event) => setTaskType(event.target.value)}>
          <option value="browser_debug">Browser debug</option>
          <option value="repo_analysis">Repo analysis</option>
          <option value="visual_qa">Visual QA</option>
          <option value="script">Script</option>
        </select>
      </label>

      <label>
        Instrucciones
        <textarea value={prompt} onChange={(event) => setPrompt(event.target.value)} />
      </label>

      {error ? <p role="alert">{error}</p> : null}

      <button type="submit" disabled={loading || prompt.trim().length === 0}>
        {loading ? "Creando..." : "Crear job"}
      </button>
    </form>
  );
}
```

### Hook para eventos de job con SSE

```tsx
import { useEffect, useState } from "react";
import { useOkManus } from "./OkManusProvider";

type JobEvent = {
  id: string;
  type: string;
  message?: string;
  progress?: number;
  created_at: string;
};

export function useJobEvents(jobId: string) {
  const { apiBaseUrl, token } = useOkManus();
  const [events, setEvents] = useState<JobEvent[]>([]);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    const url = new URL(`${apiBaseUrl}/v1/jobs/${jobId}/events`);
    url.searchParams.set("token", token);

    const source = new EventSource(url.toString());
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);
    source.onmessage = (message) => {
      setEvents((current) => [...current, JSON.parse(message.data) as JobEvent]);
    };

    return () => source.close();
  }, [apiBaseUrl, jobId, token]);

  return { events, connected };
}
```

### Panel JSX de estado

```tsx
import { useJobEvents } from "./useJobEvents";

export function JobStatusPanel({ jobId }: { jobId: string }) {
  const { events, connected } = useJobEvents(jobId);
  const latest = events.at(-1);

  return (
    <section>
      <header>
        <h2>Job {jobId}</h2>
        <span>{connected ? "Conectado" : "Reconectando"}</span>
      </header>

      <progress value={latest?.progress ?? 0} max={100} />

      <ol>
        {events.map((event) => (
          <li key={event.id}>
            <time dateTime={event.created_at}>{event.created_at}</time>
            <strong>{event.type}</strong>
            {event.message ? <span>{event.message}</span> : null}
          </li>
        ))}
      </ol>
    </section>
  );
}
```

## 6. JSX: Cron Jobs

### Hook para crear cron job

```tsx
import { useState } from "react";
import { useOkManus } from "./OkManusProvider";

type CreateCronJobInput = {
  name: string;
  timezone: string;
  schedule: {
    type: "cron";
    expression: string;
  };
  enabled: boolean;
  max_concurrent_runs: number;
  misfire_policy: "skip" | "run_once" | "catch_up";
  job_template: {
    task_type: string;
    prompt: string;
    target: { mode: "best_available_runner" | "runner" | "hosted"; runner_id?: string };
  };
};

type CronJob = {
  cron_id: string;
  name: string;
  enabled: boolean;
  next_run_at: string | null;
};

export function useCreateCronJob() {
  const okmanus = useOkManus();
  const [loading, setLoading] = useState(false);

  async function createCronJob(input: CreateCronJobInput) {
    setLoading(true);
    try {
      return await okmanus.request<CronJob>("/v1/cron-jobs", {
        method: "POST",
        body: JSON.stringify(input),
      });
    } finally {
      setLoading(false);
    }
  }

  return { createCronJob, loading };
}
```

### Editor JSX de cron job

```tsx
import { FormEvent, useState } from "react";
import { useCreateCronJob } from "./useCreateCronJob";

export function CronJobEditor() {
  const { createCronJob, loading } = useCreateCronJob();
  const [name, setName] = useState("Nightly visual QA");
  const [expression, setExpression] = useState("0 3 * * *");
  const [prompt, setPrompt] = useState("Ejecuta QA visual nocturno y genera reporte.");

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    await createCronJob({
      name,
      timezone: "Europe/Madrid",
      schedule: { type: "cron", expression },
      enabled: true,
      max_concurrent_runs: 1,
      misfire_policy: "skip",
      job_template: {
        task_type: "visual_qa",
        prompt,
        target: { mode: "best_available_runner" },
      },
    });
  }

  return (
    <form onSubmit={onSubmit}>
      <label>
        Nombre
        <input value={name} onChange={(event) => setName(event.target.value)} />
      </label>

      <label>
        Cron
        <input value={expression} onChange={(event) => setExpression(event.target.value)} />
      </label>

      <label>
        Prompt
        <textarea value={prompt} onChange={(event) => setPrompt(event.target.value)} />
      </label>

      <button type="submit" disabled={loading}>
        {loading ? "Guardando..." : "Crear programacion"}
      </button>
    </form>
  );
}
```

## 7. Casos De Uso Del Proyecto Entero

### Soporte tecnico automatico

1. Llega ticket desde Intercom/Zendesk/n8n.
2. n8n llama a `POST /v1/jobs`.
3. OkManus clona repo o abre URL.
4. Runner ejecuta repro en sandbox.
5. Browser adapter genera screenshots.
6. Verification loop valida.
7. Webhook devuelve reporte al ticket.

### CI/CD inteligente

1. Se abre pull request.
2. GitHub webhook crea job `visual_qa`.
3. OkManus abre preview deploy.
4. Ejecuta Playwright/screenshot diff.
5. Genera artifacts.
6. Comenta resultado con evidencias.

### QA programado con cron

1. `cron_jobs` dispara `visual_qa` cada noche.
2. Scheduler crea un job real.
3. Runner ejecuta flujo de navegador.
4. Artifacts van a storage.
5. Si falla, webhook a Slack/n8n.

### Limpieza programada

1. Cron interno `cleanup_expired_artifacts`.
2. Busca artifacts vencidos.
3. Borra storage.
4. Escribe audit event.
5. Publica metricas.

### Health de runners

1. Runner envia heartbeat cada 15-30 segundos.
2. Redis guarda presencia con TTL corto.
3. Postgres guarda snapshots importantes.
4. Scheduler solo asigna jobs a runners sanos.

### Marketplace de sandboxes

1. Nodo verificado declara capacidad.
2. Scheduler solo envia jobs de riesgo compatible.
3. Runner crea sandbox efimero.
4. Al terminar, cleanup verificable.
5. Score del nodo se actualiza.

### API Redis REST compatible con Upstash

1. Edge o core usa `UPSTASH_REDIS_REST_URL`.
2. La API self-hosted traduce REST a Redis protocol.
3. Rate limits y comandos peligrosos protegen el servidor.
4. Redis/Valkey guarda solo estado efimero.

## 8. Seguridad Obligatoria

Reglas:

- JWT obligatorio en API de producto.
- Validar `iss`, `aud`, `exp`, `sub`, `workspaceId` y `scope`.
- API keys largas nunca van al navegador.
- Secret proxy por defecto.
- No loggear `Authorization`, cookies ni API keys.
- Webhooks firmados.
- Idempotency key para operaciones con side effects.
- Kill switch por provider, workspace, usuario, MCP y operacion.
- Body limit por endpoint.
- Rate limit por IP, usuario, workspace, provider, API key y coste estimado.

## 9. Observabilidad

Metricas minimas:

```text
job_created_total
job_completed_total
job_failed_total
job_queue_depth
job_queue_age_seconds
runner_online_total
runner_heartbeat_age_seconds
cron_triggered_total
cron_skipped_total
cron_failed_total
redis_ops_total
redis_memory_used_bytes
provider_error_rate
provider_latency_p95_ms
webhook_delivery_failed_total
verification_failed_total
```

Trazas:

- `trace_id` por request.
- `job_id` en todos los logs de job.
- `cron_id` y `cron_run_id` en jobs programados.
- `runner_id` en ejecucion.
- `provider_id` en llamadas externas.

## 10. Roadmap Tecnico

### Fase 1

- API Redis REST compatible Upstash.
- `POST /v1/jobs`.
- Runner basico.
- Sandbox Docker efimero.
- Logs y screenshots.
- Verification adapter para web.

### Fase 2

- `cron_jobs`.
- Webhooks firmados.
- UI JSX para jobs/crons/runners.
- Secret proxy.
- Provider registry.

### Fase 3

- Scheduler multi-provider.
- BYO runner serio.
- Marketplace beta cerrado.
- Cron recovery.
- Audit dashboard.

### Fase 4

- MicroVM/gVisor/Kata.
- Multi-region.
- Realtime provider fallback.
- Billing y cuotas avanzadas.
- Enterprise policies.

## 11. Fuentes De Notion Sintetizadas

- OkManus - Arquitectura completa del proyecto.
- OkManus - Arquitectura anti-cuota Cloudflare para APIs, proveedores y MCPs.
- OkManus - Arquitectura de credenciales, secretos y servicios BYO.
- OkManus - Marketplace de sandboxes efimeros compartidos.
- OkManus - Arquitectura multi-provider por rol.
- OkManus - Proveedores Redis/Valkey y arquitectura anti-colapso.
- OkManus - Distributed API Execution Architecture.
- Architecture Overview.
- Security Rules.
- Provider Registry.
- Deployment Playbook.
- OkManus - API Redis REST compatible con Upstash para Dokploy.
