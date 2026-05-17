import { describe, expect, test } from "bun:test";

import {
  buildNoToolCallRetryMessage,
  classifyBrowserBlock,
  parseBrowserToolCallFromText,
} from "../src/browser-agent-control";

describe("browser agent control", () => {
  test("parses leaked shorthand navigate tags as executable tool calls", () => {
    expect(parseBrowserToolCallFromText("<navigate>{https://www.google.com/search?q=outlook}</navigate>")).toEqual({
      function: {
        name: "navigate",
        arguments: JSON.stringify({ url: "https://www.google.com/search?q=outlook" }),
      },
    });
  });

  test("builds a retry prompt instead of allowing text-only completion", () => {
    const prompt = buildNoToolCallRetryMessage({
      objective: "busca en google outlook",
      currentUrl: "https://www.google.com/search?q=outlook",
      rawContent: "He terminado.",
      attempt: 1,
      maxAttempts: 2,
    });

    expect(prompt).toContain("NO has llamado a ninguna herramienta");
    expect(prompt).toContain("busca en google outlook");
    expect(prompt).toContain("finish");
  });

  test("classifies captcha and bot-check pages as external blocks", () => {
    expect(
      classifyBrowserBlock({
        url: "https://www.google.com/sorry/index?continue=https://www.google.com/search%3Fq%3Doutlook",
        title: "Sorry",
        extracted: "Our systems have detected unusual traffic from your computer network.",
      }),
    ).toEqual({
      reason: "captcha_or_bot_check",
      detail: "Captcha, bot-check, or unusual-traffic page detected.",
    });
  });
});
