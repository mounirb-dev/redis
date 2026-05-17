# Autonomous browser agent architecture

## Security boundary

OkManus must not bypass CAPTCHAs, bot checks, paywalls, login walls, or abuse-prevention systems. A CAPTCHA is an external policy signal, not a technical obstacle to defeat.

The agent may:

- detect a CAPTCHA or bot-check page;
- stop unsafe retries;
- ask for human takeover;
- use a user-authorized local browser session;
- use an official API or approved connector;
- continue after the user legitimately completes the challenge.

The agent must not:

- call CAPTCHA-solving services;
- hide automation fingerprints to evade access controls;
- automate around anti-bot gates;
- reuse credentials or cookies without user consent;
- claim a blocked task is complete.

## Public Manus pattern

Public Manus documentation describes two browser modes:

- Browser Operator: a local browser extension that uses the user's active tabs, existing logins, and explicit permission for each session.
- Cloud Browser: an isolated cloud browser for general web tasks and broad research.

The important architecture lesson is not CAPTCHA bypass. It is environment selection:

- authenticated or sensitive workflow -> local browser with user consent;
- general research workflow -> cloud browser sandbox;
- blocked workflow -> pause and request intervention or API fallback.

Manus also documents takeover: when the browser or VS Code needs assistance, the user can manually take control inside the task. OkManus should model the same state explicitly.

## OkManus target architecture

```text
Frontend
  -> create job
  -> stream timeline and screenshots
  -> display takeover / needs-input states

Control Plane API
  -> policy check
  -> capability selection
  -> credential resolution
  -> event stream

Browser Runner
  -> Browserless cloud browser for unauthenticated tasks
  -> local browser connector for authenticated user sessions
  -> CAPTCHA/bot-check detector
  -> screenshot + DOM/text extraction

Agent Loop
  -> plan
  -> act with one browser tool
  -> observe
  -> classify blocks
  -> continue, finish, or request takeover
```

## Capability selection

The browser provider should be selected before execution:

```ts
type BrowserMode = "cloud_browser" | "local_operator" | "official_api";

type BrowserCapability =
  | { mode: "cloud_browser"; provider: "browserless"; authenticated: false }
  | { mode: "local_operator"; provider: "extension"; authenticated: true; requiresConsent: true }
  | { mode: "official_api"; provider: string; scopes: string[] };
```

Routing rules:

- Use `official_api` when a supported connector exists.
- Use `local_operator` for user-authenticated sites, CRMs, dashboards, email, billing portals, and premium tools.
- Use `cloud_browser` for public browsing, screenshots, extraction, and lightweight research.
- If `cloud_browser` hits a bot-check, stop and escalate. Do not keep retrying the same URL.

## Browserless runner behavior

Browserless should remain the cloud-browser executor, not a CAPTCHA bypass service.

Required runner outputs:

```ts
type BrowserActionResult = {
  action: string;
  finalUrl: string;
  title: string;
  extracted: string;
  screenshotDataUrl?: string;
  blocked?: {
    reason: "captcha_or_bot_check" | "login_required";
    detail: string;
  };
};
```

If `blocked.reason === "captcha_or_bot_check"`, the agent loop must:

1. record the event in the timeline;
2. call `finish` or `needs_input`;
3. explain that user intervention, local browser authorization, or an official API is required;
4. avoid repeated navigation attempts that look like abuse.

## Human takeover state

The product should support a first-class state:

```ts
type AgentState =
  | "running"
  | "needs_user_takeover"
  | "needs_credentials"
  | "blocked_by_external_policy"
  | "completed"
  | "failed";
```

When takeover is required, the UI should show:

- what site blocked the task;
- what action was being attempted;
- screenshot of the current page;
- safe next options: open local operator, connect API, retry with another source, or cancel.

## Functions to build next

### Backend

- `classifyBrowserBlock(result)` - already started; detects CAPTCHA, unusual traffic, login walls.
- `selectBrowserMode(task, user, capabilities)` - chooses cloud, local, or API.
- `requestHumanTakeover(jobId, reason, screenshotRef)` - moves the job to `needs_user_takeover`.
- `resumeAfterTakeover(jobId, browserSessionRef)` - resumes once the user authorizes a local session.
- `recordAgentEvent(jobId, event)` - streams timeline events to the UI.

### Frontend

- Show `blocked_by_external_policy` as a neutral state, not a failure panic.
- Add a "Use my browser" action for local operator mode.
- Add "Use official API" when a connector exists.
- Hide tool-call syntax and internal prompt text from assistant bubbles.
- Keep screenshots and action logs visible for trust.

### Browser connector

- Browser extension or desktop bridge with explicit session permission.
- Scoped tab control only; no whole-browser ambient authority by default.
- User can stop by closing the tab or revoking the session.
- All actions logged.

## Anti-abuse defaults

- Stop after one detected bot-check per origin per job.
- Back off repeated failures.
- Do not rotate proxies or identities to bypass detection.
- Do not solve challenges automatically.
- Prefer APIs and connectors over browser automation for known SaaS products.
- Require explicit user consent before using local cookies or logged-in sessions.

## Current implementation status

Implemented:

- Browserless cloud-browser action execution.
- ReAct loop with structured tools.
- Text pseudo-tool cleanup and retry prompt.
- CAPTCHA/bot-check classification in the Browserless result path.

Not implemented yet:

- Local Browser Operator extension.
- Human takeover event stream.
- Resume-after-takeover protocol.
- Official connector routing for sites like Google/Microsoft.
