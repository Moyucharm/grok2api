const IMAGINE_WS_ENDPOINT = "https://grok.com/ws/imagine/listen";
const IMAGINE_AGE_VERIFY_ENDPOINT = "https://grok.com/rest/auth/set-birth-date";

const IMAGE_ID_RE = /\/images\/([a-f0-9-]+)\.(?:png|jpg|jpeg|webp)/i;

type ImagineStage = "preview" | "medium" | "final";

const STAGE_RANK: Record<ImagineStage, number> = {
  preview: 1,
  medium: 2,
  final: 3,
};

function stageByPayload(url: string, blobSize: number): ImagineStage {
  const lowerUrl = url.toLowerCase();
  if ((lowerUrl.endsWith(".jpg") || lowerUrl.endsWith(".jpeg")) && blobSize > 100_000) return "final";
  if (blobSize > 30_000) return "medium";
  return "preview";
}

function imageIdFromUrl(url: string): string | null {
  const m = url.match(IMAGE_ID_RE);
  return m?.[1] ?? null;
}

function toText(data: unknown): string {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(new Uint8Array(data));
  if (data instanceof Uint8Array) return new TextDecoder().decode(data);
  return "";
}

interface ImagineImageState {
  imageId: string;
  stage: ImagineStage;
  url: string;
  blob: string;
  blobSize: number;
  updatedAt: number;
}

export interface ImagineProgressEvent {
  image_id: string;
  stage: ImagineStage;
  blob_size: number;
  is_final: boolean;
  completed: number;
  total: number;
}

export interface ImagineWsGenerateArgs {
  prompt: string;
  aspectRatio: "1:1" | "2:3" | "3:2" | "9:16" | "16:9";
  n: number;
  cookie: string;
  enableNsfw?: boolean;
  timeoutMs?: number;
  onProgress?: (event: ImagineProgressEvent) => Promise<void> | void;
}

export interface ImagineWsGenerateResult {
  urls: string[];
  b64List: string[];
}

export class ImagineWsError extends Error {
  code: string;
  status?: number;
  retryable: boolean;

  constructor(message: string, args: { code: string; status?: number; retryable?: boolean }) {
    super(message);
    this.name = "ImagineWsError";
    this.code = args.code;
    if (typeof args.status === "number") this.status = args.status;
    this.retryable = Boolean(args.retryable);
  }
}

function isRetryableStatus(status: number): boolean {
  return status === 401 || status === 403 || status === 429;
}

function completedCount(states: Map<string, ImagineImageState>): number {
  let count = 0;
  for (const s of states.values()) {
    if (s.stage === "final") count += 1;
  }
  return count;
}

function chooseBestImages(states: Map<string, ImagineImageState>, n: number): ImagineImageState[] {
  return [...states.values()]
    .sort((a, b) => {
      const stage = STAGE_RANK[b.stage] - STAGE_RANK[a.stage];
      if (stage !== 0) return stage;
      if (b.blobSize !== a.blobSize) return b.blobSize - a.blobSize;
      return b.updatedAt - a.updatedAt;
    })
    .slice(0, Math.max(1, n));
}

function clampN(n: number): number {
  if (!Number.isFinite(n)) return 1;
  return Math.max(1, Math.min(10, Math.floor(n)));
}

export interface ImagineAgeVerifyArgs {
  cookie: string;
  birthDate: string;
  timeoutMs?: number;
}

export interface ImagineAgeVerifyResult {
  ok: boolean;
  httpStatus: number;
  reason: string;
  responseText: string;
}

function isAgeAlreadyAccepted(status: number, text: string): boolean {
  if (![400, 409, 422].includes(status)) return false;
  const low = text.toLowerCase();
  return (
    low.includes("already") ||
    low.includes("birth date") ||
    low.includes("cannot be changed") ||
    low.includes("already set") ||
    low.includes("already verified") ||
    low.includes("date of birth") ||
    low.includes("immutable") ||
    text.includes("\u5df2\u8bbe\u7f6e") ||
    text.includes("\u5df2\u7ecf\u8bbe\u7f6e") ||
    text.includes("\u5df2\u9a8c\u8bc1") ||
    text.includes("\u4e0d\u80fd\u66f4\u6539") ||
    text.includes("\u4e0d\u53ef\u66f4\u6539") ||
    text.includes("\u65e0\u6cd5\u66f4\u6539")
  );
}

export async function verifyImagineAgeDetailed(args: ImagineAgeVerifyArgs): Promise<ImagineAgeVerifyResult> {
  const timeoutMs = Math.max(5_000, Math.floor(args.timeoutMs ?? 20_000));
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort("timeout"), timeoutMs);
  try {
    const resp = await fetch(IMAGINE_AGE_VERIFY_ENDPOINT, {
      method: "POST",
      headers: {
        Origin: "https://grok.com",
        Referer: "https://grok.com/?_s=account",
        Accept: "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        Cookie: args.cookie,
        "Content-Type": "application/json",
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
      },
      body: JSON.stringify({ birthDate: args.birthDate }),
      signal: controller.signal,
    });
    const txt = await resp.text().catch(() => "");
    const snippet = txt.slice(0, 200);
    if (resp.ok) {
      return { ok: true, httpStatus: resp.status, reason: "ok", responseText: snippet };
    }

    if (isAgeAlreadyAccepted(resp.status, txt)) {
      return {
        ok: true,
        httpStatus: resp.status,
        reason: "already_set_or_verified",
        responseText: snippet,
      };
    }
    return {
      ok: false,
      httpStatus: resp.status,
      reason: `HTTP ${resp.status}`,
      responseText: snippet,
    };
  } catch (e) {
    const msg = (e instanceof Error ? e.message : String(e)).slice(0, 200);
    return { ok: false, httpStatus: 0, reason: msg || "network_error", responseText: "" };
  } finally {
    clearTimeout(timer);
  }
}

export async function verifyImagineAge(args: ImagineAgeVerifyArgs): Promise<boolean> {
  const result = await verifyImagineAgeDetailed(args);
  return result.ok;
}

export async function generateImagineViaWs(args: ImagineWsGenerateArgs): Promise<ImagineWsGenerateResult> {
  const n = clampN(args.n);
  const timeoutMs = Math.max(30_000, Math.floor(args.timeoutMs ?? 120_000));

  const wsResp = await fetch(IMAGINE_WS_ENDPOINT, {
    method: "GET",
    headers: {
      Upgrade: "websocket",
      Connection: "Upgrade",
      Origin: "https://grok.com",
      Referer: "https://grok.com/imagine",
      Cookie: args.cookie,
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
      "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    },
  });

  if (!wsResp.webSocket) {
    const txt = await wsResp.text().catch(() => "");
    throw new ImagineWsError(`WebSocket handshake failed: ${wsResp.status} ${txt.slice(0, 200)}`, {
      code: "ws_handshake_failed",
      status: wsResp.status,
      retryable: isRetryableStatus(wsResp.status),
    });
  }

  const ws = wsResp.webSocket!;
  ws.accept();

  const queue: Array<{ kind: "message"; text: string } | { kind: "close" } | { kind: "error" }> = [];
  let resolver: ((v: { kind: "message"; text: string } | { kind: "close" } | { kind: "error" } | { kind: "timeout" }) => void) | null = null;
  let timer: number | null = null;
  const wake = (
    evt: { kind: "message"; text: string } | { kind: "close" } | { kind: "error" } | { kind: "timeout" },
  ): void => {
    if (resolver) {
      const r = resolver;
      resolver = null;
      if (timer !== null) clearTimeout(timer);
      timer = null;
      r(evt);
      return;
    }
    if (evt.kind !== "timeout") queue.push(evt);
  };

  ws.addEventListener("message", (evt) => {
    wake({ kind: "message", text: toText((evt as MessageEvent).data) });
  });
  ws.addEventListener("close", () => wake({ kind: "close" }));
  ws.addEventListener("error", () => wake({ kind: "error" }));

  const nextEvent = async (waitMs: number) => {
    if (queue.length) return queue.shift() as { kind: "message"; text: string } | { kind: "close" } | { kind: "error" };
    return new Promise<{ kind: "message"; text: string } | { kind: "close" } | { kind: "error" } | { kind: "timeout" }>((resolve) => {
      resolver = resolve;
      timer = setTimeout(() => {
        resolver = null;
        timer = null;
        resolve({ kind: "timeout" });
      }, waitMs) as unknown as number;
    });
  };

  const requestPayload = {
    type: "conversation.item.create",
    timestamp: Date.now(),
    item: {
      type: "message",
      content: [
        {
          requestId: crypto.randomUUID(),
          text: args.prompt,
          type: "input_text",
            properties: {
              section_count: 0,
              is_kids_mode: false,
              enable_nsfw: args.enableNsfw !== false,
              skip_upsampler: false,
              is_initial: false,
              aspect_ratio: args.aspectRatio,
          },
        },
      ],
    },
  };

  ws.send(JSON.stringify(requestPayload));

  const startAt = Date.now();
  let lastActivityAt = Date.now();
  let mediumSeenAt: number | null = null;
  const images = new Map<string, ImagineImageState>();
  const seenProgress = new Set<string>();
  let upstreamErr: ImagineWsError | null = null;

  try {
    while (Date.now() - startAt < timeoutMs) {
      const evt = await nextEvent(5000);
      if (evt.kind === "timeout") {
        const completed = completedCount(images);
        if (mediumSeenAt && completed === 0 && Date.now() - mediumSeenAt > 15_000) {
          throw new ImagineWsError("Image generation blocked after medium preview", {
            code: "blocked",
            retryable: true,
          });
        }
        if (completed >= n) break;
        if (completed > 0 && Date.now() - lastActivityAt > 10_000) break;
        continue;
      }

      if (evt.kind === "close") {
        if (upstreamErr) throw upstreamErr;
        break;
      }

      if (evt.kind === "error") {
        throw new ImagineWsError("WebSocket stream error", { code: "ws_stream_error", retryable: true });
      }

      lastActivityAt = Date.now();
      if (!evt.text) continue;

      let payload: any;
      try {
        payload = JSON.parse(evt.text);
      } catch {
        continue;
      }

      if (payload?.type === "error") {
        const code = String(payload?.err_code ?? "upstream_error");
        const message = String(payload?.err_msg ?? "Upstream error");
        upstreamErr = new ImagineWsError(message, {
          code,
          retryable: code === "rate_limit_exceeded" || code === "unauthorized",
        });
        if (code === "rate_limit_exceeded") throw upstreamErr;
        continue;
      }

      if (payload?.type !== "image") continue;
      const blob = typeof payload?.blob === "string" ? payload.blob : "";
      const url = typeof payload?.url === "string" ? payload.url.trim() : "";
      if (!blob || !url) continue;

      const imageId = imageIdFromUrl(url) ?? crypto.randomUUID();
      const blobSize = blob.length;
      const stage = stageByPayload(url, blobSize);
      if (stage === "medium" && mediumSeenAt === null) mediumSeenAt = Date.now();

      const prev = images.get(imageId);
      if (!prev || STAGE_RANK[stage] >= STAGE_RANK[prev.stage]) {
        images.set(imageId, { imageId, stage, url, blob, blobSize, updatedAt: Date.now() });
      }

      const completed = completedCount(images);
      const progressKey = `${imageId}:${stage}:${completed}`;
      if (!seenProgress.has(progressKey)) {
        seenProgress.add(progressKey);
        if (args.onProgress) {
          await args.onProgress({
            image_id: imageId,
            stage,
            blob_size: blobSize,
            is_final: stage === "final",
            completed,
            total: n,
          });
        }
      }

      if (completed >= n) break;
    }
  } finally {
    try {
      ws.close(1000, "done");
    } catch {
      // ignore
    }
  }

  const selected = chooseBestImages(images, n);
  if (!selected.length) {
    if (upstreamErr) throw upstreamErr;
    throw new ImagineWsError("No image was generated", { code: "empty_result", retryable: true });
  }

  return {
    urls: selected.map((it) => it.url),
    b64List: selected.map((it) => it.blob),
  };
}

