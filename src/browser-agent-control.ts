type ParsedToolCall = {
  function: {
    name: string;
    arguments: string;
  };
};

type BrowserBlock = {
  reason: "captcha_or_bot_check" | "login_required";
  detail: string;
};

const FUNCTION_TOOL_CALL_REGEX = /<function=(\w+)>([^<]*)<\/function>/g;
const BRACKET_TOOL_CALL_REGEX = /\[tool_call:\s*(\w+)\s*\(\s*(\{[\s\S]*?\})\s*\)\s*\]/g;
const SHORTHAND_TOOL_TAG_REGEX = /<(navigate|click|type|scroll|extract|screenshot|finish)>\s*\{?([\s\S]*?)\}?\s*<\/\1>/i;

function safeJsonArguments(rawArgs: string): string {
  let argsJson = rawArgs.trim().replace(/[.\s]+$/, "");
  if (!argsJson) return "{}";
  try {
    JSON.parse(argsJson);
    return argsJson;
  } catch {
    return "{}";
  }
}

function shorthandArguments(toolName: string, rawArgs: string): string {
  const value = rawArgs.trim().replace(/[.\s]+$/, "");
  if (!value) return "{}";

  if (value.startsWith("{")) return safeJsonArguments(value);
  if (toolName === "navigate") return JSON.stringify({ url: value });
  if (toolName === "click" || toolName === "extract") return JSON.stringify({ selector: value });
  if (toolName === "type") return JSON.stringify({ text: value });
  if (toolName === "finish") return JSON.stringify({ summary: value });
  return "{}";
}

export function parseBrowserToolCallFromText(rawContent: string): ParsedToolCall | null {
  FUNCTION_TOOL_CALL_REGEX.lastIndex = 0;
  BRACKET_TOOL_CALL_REGEX.lastIndex = 0;

  const structuredMatch =
    FUNCTION_TOOL_CALL_REGEX.exec(rawContent) ?? BRACKET_TOOL_CALL_REGEX.exec(rawContent);
  if (structuredMatch) {
    const [, fnName, rawArgs] = structuredMatch;
    return {
      function: {
        name: fnName,
        arguments: safeJsonArguments(rawArgs || ""),
      },
    };
  }

  const shorthandMatch = SHORTHAND_TOOL_TAG_REGEX.exec(rawContent);
  if (shorthandMatch) {
    const [, toolName, rawArgs] = shorthandMatch;
    return {
      function: {
        name: toolName,
        arguments: shorthandArguments(toolName, rawArgs || ""),
      },
    };
  }

  return null;
}

export function classifyBrowserBlock(input: {
  url: string;
  title?: string;
  extracted?: string;
}): BrowserBlock | null {
  const haystack = `${input.url}\n${input.title ?? ""}\n${input.extracted ?? ""}`.toLowerCase();
  if (
    /\/sorry\/index|captcha|recaptcha|hcaptcha|turnstile|bot check|unusual traffic|verify you are human|are you human|security check/.test(
      haystack,
    )
  ) {
    return {
      reason: "captcha_or_bot_check",
      detail: "Captcha, bot-check, or unusual-traffic page detected.",
    };
  }

  if (/login required|sign in required|inicia sesion|inicio de sesion obligatorio/.test(haystack)) {
    return {
      reason: "login_required",
      detail: "Login or authenticated browser session required.",
    };
  }

  return null;
}

export function buildNoToolCallRetryMessage(input: {
  objective: string;
  currentUrl: string;
  rawContent: string;
  attempt: number;
  maxAttempts: number;
}): string {
  const modelText = input.rawContent.trim() || "(respuesta vacia)";
  return (
    `NO has llamado a ninguna herramienta en el intento ${input.attempt}/${input.maxAttempts}.\n` +
    `La tarea todavia no puede cerrarse sin una tool_call real.\n` +
    `Objetivo original: ${input.objective}\n` +
    `URL actual: ${input.currentUrl}\n` +
    `Tu texto fue: ${modelText}\n\n` +
    "Si el objetivo esta realmente completado, llama ahora a `finish` con un resumen verificable. " +
    "Si falta informacion, llama a `extract`, `screenshot`, `click`, `type`, `scroll` o `navigate`. " +
    "Si hay un bloqueo externo como captcha o login obligatorio, llama a `finish` explicando ese bloqueo."
  );
}
