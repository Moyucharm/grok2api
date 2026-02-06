import { Hono } from "hono";
import { cors } from "hono/cors";
import type { Env } from "../env";
import { requireApiAuth } from "../auth";
import { getSettings, normalizeCfCookie } from "../settings";
import { isValidModel, MODEL_CONFIG } from "../grok/models";
import { extractContent, buildConversationPayload, sendConversationRequest } from "../grok/conversation";
import {
  generateImagineViaWs,
  ImagineWsError,
  type ImagineProgressEvent,
  verifyImagineAge,
} from "../grok/imagineWs";
import { uploadImage } from "../grok/upload";
import { createMediaPost, createPost } from "../grok/create";
import { createOpenAiStreamFromGrokNdjson, parseOpenAiFromGrokNdjson } from "../grok/processor";
import { addRequestLog } from "../repo/logs";
import {
  applyCooldown,
  getTokenAgeVerified,
  recordTokenFailure,
  selectBestToken,
  setTokenAgeVerified,
} from "../repo/tokens";
import type { ApiAuthInfo } from "../auth";
import { getApiKeyLimits } from "../repo/apiKeys";
import { localDayString, tryConsumeDailyUsage, tryConsumeDailyUsageMulti } from "../repo/apiKeyUsage";
import { nextLocalMidnightExpirationSeconds } from "../kv/cleanup";
import { nowMs } from "../utils/time";
import { upsertCacheRow } from "../repo/cache";

function openAiError(message: string, code: string): Record<string, unknown> {
  return { error: { message, type: "invalid_request_error", code } };
}

function makeOpenAiErrorSse(message: string, model: string): Response {
  const created = Math.floor(Date.now() / 1000);
  const id = `chatcmpl-${crypto.randomUUID()}`;
  const chunk1 = {
    id,
    object: "chat.completion.chunk",
    created,
    model,
    choices: [{ index: 0, delta: { role: "assistant", content: String(message || "Upstream error") }, finish_reason: null }],
  };
  const chunk2 = {
    id,
    object: "chat.completion.chunk",
    created,
    model,
    choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
  };
  const body = `data: ${JSON.stringify(chunk1)}\n\ndata: ${JSON.stringify(chunk2)}\n\ndata: [DONE]\n\n`;
  return new Response(body, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
      "Access-Control-Allow-Origin": "*",
    },
  });
}

function getClientIp(req: Request): string {
  return (
    req.headers.get("CF-Connecting-IP") ||
    req.headers.get("X-Forwarded-For")?.split(",")[0]?.trim() ||
    "0.0.0.0"
  );
}

async function mapLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  const queue = items.map((item, index) => ({ item, index }));
  const workers = Array.from({ length: Math.max(1, limit) }, async () => {
    while (queue.length) {
      const job = queue.shift() as { item: T; index: number };
      results[job.index] = await fn(job.item);
    }
  });
  await Promise.all(workers);
  return results;
}

export const openAiRoutes = new Hono<{ Bindings: Env; Variables: { apiAuth: ApiAuthInfo } }>();

openAiRoutes.use(
  "/*",
  cors({
    origin: "*",
    allowHeaders: ["Authorization", "Content-Type"],
    allowMethods: ["GET", "POST", "OPTIONS"],
    maxAge: 86400,
  }),
);

openAiRoutes.use("/*", requireApiAuth);

function parseIntSafe(v: string | undefined, fallback: number): number {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.floor(n);
}

function quotaError(bucket: string): Record<string, unknown> {
  return openAiError(`Daily quota exceeded: ${bucket}`, "daily_quota_exceeded");
}

const VALID_IMAGE_ASPECT_RATIOS = new Set(["1:1", "2:3", "3:2", "9:16", "16:9"]);
const ASPECT_RATIO_TO_SIZE: Record<"1:1" | "2:3" | "3:2" | "9:16" | "16:9", string> = {
  "1:1": "1024x1024",
  "2:3": "1024x1536",
  "3:2": "1536x1024",
  "9:16": "1024x1792",
  "16:9": "1792x1024",
};

type ImageResponseFormat = "url" | "b64_json";

function normalizeImageResponseFormat(v: unknown): ImageResponseFormat | null {
  const format = String(v ?? "url").trim().toLowerCase();
  if (format === "url") return "url";
  if (format === "b64_json") return "b64_json";
  return null;
}

function mapSizeToAspectRatio(sizeRaw: unknown): string | null {
  const size = String(sizeRaw ?? "").trim().toLowerCase();
  if (!size) return null;
  const map: Record<string, string> = {
    "1024x1024": "1:1",
    "512x512": "1:1",
    "256x256": "1:1",
    "1024x1536": "2:3",
    "1536x1024": "3:2",
    "1024x1792": "9:16",
    "1792x1024": "16:9",
  };
  return map[size] ?? null;
}

function resolveImagineAspectRatio(args: {
  size?: unknown;
  imageConfig: { aspect_ratio?: unknown } | undefined;
}): "1:1" | "2:3" | "3:2" | "9:16" | "16:9" {
  const byImageConfig = String(args.imageConfig?.aspect_ratio ?? "").trim();
  if (byImageConfig) {
    if (!VALID_IMAGE_ASPECT_RATIOS.has(byImageConfig))
      throw new Error(`Invalid image_config.aspect_ratio '${byImageConfig}'`);
    return byImageConfig as "1:1" | "2:3" | "3:2" | "9:16" | "16:9";
  }

  const bySize = mapSizeToAspectRatio(args.size);
  if (bySize && VALID_IMAGE_ASPECT_RATIOS.has(bySize)) {
    return bySize as "1:1" | "2:3" | "3:2" | "9:16" | "16:9";
  }

  return "2:3";
}

function resolveImagineSize(args: {
  size?: unknown;
  imageConfig: { aspect_ratio?: unknown } | undefined;
}): string {
  const ratio = resolveImagineAspectRatio(args);
  return ASPECT_RATIO_TO_SIZE[ratio];
}

function hasResolutionDowngradeWarning(imageConfig: { resolution?: unknown } | undefined): string | null {
  const resolution = String(imageConfig?.resolution ?? "").trim();
  if (!resolution) return null;
  return `image_config.resolution='${resolution}' accepted but not enforced; using size/aspect_ratio only`;
}

function withOptionalWarningHeader(res: Response, warning: string | null): Response {
  if (!warning) return res;
  const headers = new Headers(res.headers);
  headers.set("x-grok2api-warning", warning);
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers });
}

function arrayBufferToBase64(buf: ArrayBuffer): string {
  const bytes = new Uint8Array(buf);
  let out = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    out += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(out);
}

async function fetchProxyImageAsBase64(origin: string, encodedPath: string): Promise<string> {
  const url = `${origin}/images/${encodedPath}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Fetch image failed: ${resp.status}`);
  const bytes = await resp.arrayBuffer();
  return arrayBufferToBase64(bytes);
}

function scheduleImagePrefetch(c: any, origin: string, encodedPaths: string[]): void {
  const targets = [...new Set(encodedPaths.filter(Boolean))];
  if (!targets.length) return;
  c.executionCtx.waitUntil(
    mapLimit(targets, 3, async (path) => {
      try {
        const r = await fetch(`${origin}/images/${path}`);
        if (!r.ok) return;
        await r.arrayBuffer();
      } catch {
        // ignore
      }
    }),
  );
}

function markdownFromRawImageUrls(args: { baseUrl: string; rawUrls: string[] }): string {
  const lines: string[] = [];
  for (const raw of args.rawUrls) {
    const path = encodeAssetPath(raw);
    lines.push(`![Generated Image](${toProxyUrl(args.baseUrl, path)})`);
  }
  return lines.join("\n");
}

function imagineErrorStatus(err: unknown): number {
  if (err instanceof ImagineWsError) {
    const status = err.status;
    if (typeof status === "number" && Number.isFinite(status)) return status;
  }
  return 500;
}

function imagineErrorMessage(err: unknown): string {
  if (err instanceof Error && err.message) return err.message;
  return String(err);
}

function streamChunk(args: {
  id: string;
  created: number;
  model: string;
  content?: string;
  finishReason?: "stop" | null;
  includeRole?: boolean;
}): string {
  const delta: Record<string, unknown> = {};
  if (args.includeRole) delta.role = "assistant";
  if (typeof args.content === "string" && args.content) delta.content = args.content;

  return `data: ${JSON.stringify({
    id: args.id,
    object: "chat.completion.chunk",
    created: args.created,
    model: args.model,
    choices: [{ index: 0, delta, finish_reason: args.finishReason ?? null }],
  })}\n\n`;
}

type SettingsBundle = Awaited<ReturnType<typeof getSettings>>;

function buildGrokCookie(jwt: string, cfCookie: string): string {
  return cfCookie ? `sso-rw=${jwt};sso=${jwt};${cfCookie}` : `sso-rw=${jwt};sso=${jwt}`;
}

function ensureBirthDate(input: string): string {
  const v = input.trim();
  return v || "2001-01-01T16:00:00.000Z";
}

async function generateImagineWsWithRetries(args: {
  env: Env;
  settings: SettingsBundle;
  requestedModel: string;
  prompt: string;
  aspectRatio: "1:1" | "2:3" | "3:2" | "9:16" | "16:9";
  n: number;
  onProgress?: (event: ImagineProgressEvent) => Promise<void> | void;
}): Promise<{ urls: string[]; b64List: string[]; tokenSuffix: string }> {
  const retryCodes = Array.isArray(args.settings.grok.retry_status_codes)
    ? args.settings.grok.retry_status_codes
    : [401, 429, 403];
  const maxRetries = Math.max(1, Math.floor(Number(args.settings.grok.imagine_max_retries ?? 5)));
  const blockedRetryLimit = Math.max(0, Math.floor(Number(args.settings.grok.imagine_blocked_retry_limit ?? 3)));
  const autoAgeVerify = args.settings.grok.imagine_auto_age_verify !== false;
  const enableNsfw = args.settings.grok.imagine_enable_nsfw !== false;
  const birthDate = ensureBirthDate(String(args.settings.grok.imagine_birth_date ?? ""));
  const cf = normalizeCfCookie(args.settings.grok.cf_clearance ?? "");

  let blockedRetries = 0;
  let lastError: unknown = null;
  let lastStatus = 500;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const chosen = await selectBestToken(args.env.DB, args.requestedModel);
    if (!chosen) throw new Error("No available token");

    const jwt = chosen.token;
    const cookie = buildGrokCookie(jwt, cf);

    if (autoAgeVerify) {
      const verified = await getTokenAgeVerified(args.env.DB, jwt);
      if (!verified) {
        const ok = await verifyImagineAge({ cookie, birthDate });
        if (ok) await setTokenAgeVerified(args.env.DB, jwt, true);
      }
    }

    try {
      const generated = await generateImagineViaWs({
        prompt: args.prompt,
        aspectRatio: args.aspectRatio,
        n: args.n,
        cookie,
        enableNsfw,
        ...(args.onProgress ? { onProgress: args.onProgress } : {}),
      });
      return { urls: generated.urls, b64List: generated.b64List, tokenSuffix: jwt.slice(-6) };
    } catch (err) {
      lastError = err;
      const msg = imagineErrorMessage(err);
      const status = imagineErrorStatus(err);
      lastStatus = status;

      await recordTokenFailure(args.env.DB, jwt, status, msg.slice(0, 200));
      await applyCooldown(args.env.DB, jwt, status);

      const code = err instanceof ImagineWsError ? err.code : "";
      if (code === "blocked") {
        blockedRetries += 1;
        if (blockedRetries >= blockedRetryLimit) break;
      }

      const retryable =
        code === "blocked" ||
        (err instanceof ImagineWsError && err.retryable) ||
        retryCodes.includes(status);
      if (retryable && attempt < maxRetries - 1) continue;
      break;
    }
  }

  if (lastError instanceof Error) throw lastError;
  throw new ImagineWsError("Image generation failed", {
    code: "imagine_ws_failed",
    status: lastStatus,
    retryable: false,
  });
}

async function createImagineVideoUpstreamWithRetries(args: {
  env: Env;
  settings: SettingsBundle;
  requestedModel: string;
  prompt: string;
  imageInputs: string[];
  videoConfig?: {
    aspect_ratio?: string;
    video_length?: number;
    resolution?: string;
    preset?: string;
  };
}): Promise<{ upstream: Response; cookie: string; tokenSuffix: string }> {
  const retryCodes = Array.isArray(args.settings.grok.retry_status_codes)
    ? args.settings.grok.retry_status_codes
    : [401, 429, 403];
  const maxRetries = Math.max(1, Math.floor(Number(args.settings.grok.imagine_max_retries ?? 5)));
  const autoAgeVerify = args.settings.grok.imagine_auto_age_verify !== false;
  const birthDate = ensureBirthDate(String(args.settings.grok.imagine_birth_date ?? ""));
  const cf = normalizeCfCookie(args.settings.grok.cf_clearance ?? "");

  let lastErr: unknown = null;
  let lastStatus = 500;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const chosen = await selectBestToken(args.env.DB, args.requestedModel);
    if (!chosen) throw new Error("No available token");

    const jwt = chosen.token;
    const cookie = buildGrokCookie(jwt, cf);
    const safePrompt = args.prompt.trim() || "Generate a video";

    if (autoAgeVerify) {
      const verified = await getTokenAgeVerified(args.env.DB, jwt);
      if (!verified) {
        const ok = await verifyImagineAge({ cookie, birthDate });
        if (ok) await setTokenAgeVerified(args.env.DB, jwt, true);
      }
    }

    try {
      const imgInputs = args.imageInputs.length ? [args.imageInputs[0]!] : [];
      const uploads = await mapLimit(imgInputs, 1, (u) => uploadImage(u, cookie, args.settings.grok));
      const imgIds = uploads.map((u) => u.fileId).filter(Boolean);
      const imgUris = uploads.map((u) => u.fileUri).filter(Boolean);

      let postId: string | undefined;
      if (imgUris.length) {
        const post = await createPost(imgUris[0]!, cookie, args.settings.grok);
        postId = post.postId || undefined;
      } else {
        const post = await createMediaPost(
          { mediaType: "MEDIA_POST_TYPE_VIDEO", prompt: safePrompt },
          cookie,
          args.settings.grok,
        );
        postId = post.postId || undefined;
      }

      const { payload, referer } = buildConversationPayload({
        requestModel: args.requestedModel,
        content: safePrompt,
        imgIds,
        imgUris,
        ...(postId ? { postId } : {}),
        ...(args.videoConfig ? { videoConfig: args.videoConfig } : {}),
        settings: args.settings.grok,
      });

      const upstream = await sendConversationRequest({
        payload,
        cookie,
        settings: args.settings.grok,
        ...(referer ? { referer } : {}),
      });

      if (upstream.ok) return { upstream, cookie, tokenSuffix: jwt.slice(-6) };

      const txt = await upstream.text().catch(() => "");
      const msg = `Upstream ${upstream.status}: ${txt.slice(0, 200)}`;
      lastErr = new ImagineWsError(msg, {
        code: "imagine_video_upstream_error",
        status: upstream.status,
        retryable: retryCodes.includes(upstream.status),
      });
      lastStatus = upstream.status;
      await recordTokenFailure(args.env.DB, jwt, upstream.status, txt.slice(0, 200));
      await applyCooldown(args.env.DB, jwt, upstream.status);

      if (retryCodes.includes(upstream.status) && attempt < maxRetries - 1) continue;
      break;
    } catch (err) {
      lastErr = err;
      const msg = err instanceof Error ? err.message : String(err);
      lastStatus = imagineErrorStatus(err);
      await recordTokenFailure(args.env.DB, jwt, lastStatus, msg.slice(0, 200));
      await applyCooldown(args.env.DB, jwt, lastStatus);
      if (attempt < maxRetries - 1) continue;
      break;
    }
  }

  if (lastErr instanceof Error) throw lastErr;
  throw new ImagineWsError("Video generation failed", {
    code: "imagine_video_failed",
    status: lastStatus,
    retryable: false,
  });
}

async function enforceQuota(args: {
  env: Env;
  apiAuth: ApiAuthInfo;
  model: string;
  kind: "chat" | "image" | "video";
  imageCount?: number;
}): Promise<{ ok: true } | { ok: false; resp: Response }> {
  const key = args.apiAuth.key;
  if (!key) return { ok: true };
  if (args.apiAuth.is_admin) return { ok: true };

  const limits = await getApiKeyLimits(args.env.DB, key);
  if (!limits) return { ok: true };

  const tz = parseIntSafe(args.env.CACHE_RESET_TZ_OFFSET_MINUTES, 480);
  const day = localDayString(nowMs(), tz);
  const atMs = nowMs();
  const jsonHeaders = { "content-type": "application/json; charset=utf-8" };

  if (args.model === "grok-4-heavy") {
    const ok = await tryConsumeDailyUsageMulti({
      db: args.env.DB,
      key,
      day,
      atMs,
      updates: [
        { field: "heavy_used", inc: 1, limit: limits.heavy_limit },
        { field: "chat_used", inc: 1, limit: limits.chat_limit },
      ],
    });
    if (!ok) return { ok: false, resp: new Response(JSON.stringify(quotaError("heavy/chat")), { status: 429, headers: jsonHeaders }) };
    return { ok: true };
  }

  if (args.kind === "video") {
    const ok = await tryConsumeDailyUsage({
      db: args.env.DB,
      key,
      day,
      atMs,
      field: "video_used",
      inc: 1,
      limit: limits.video_limit,
    });
    if (!ok) return { ok: false, resp: new Response(JSON.stringify(quotaError("video")), { status: 429, headers: jsonHeaders }) };
    return { ok: true };
  }

  if (args.kind === "image") {
    const inc = Math.max(1, Math.floor(Number(args.imageCount ?? 1) || 1));
    const ok = await tryConsumeDailyUsage({
      db: args.env.DB,
      key,
      day,
      atMs,
      field: "image_used",
      inc,
      limit: limits.image_limit,
    });
    if (!ok) return { ok: false, resp: new Response(JSON.stringify(quotaError("image")), { status: 429, headers: jsonHeaders }) };
    return { ok: true };
  }

  // chat
  const ok = await tryConsumeDailyUsage({
    db: args.env.DB,
    key,
    day,
    atMs,
    field: "chat_used",
    inc: 1,
    limit: limits.chat_limit,
  });
  if (!ok) return { ok: false, resp: new Response(JSON.stringify(quotaError("chat")), { status: 429, headers: jsonHeaders }) };
  return { ok: true };
}

function base64UrlEncodeString(input: string): string {
  const bytes = new TextEncoder().encode(input);
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function encodeAssetPath(raw: string): string {
  try {
    const u = new URL(raw);
    return `u_${base64UrlEncodeString(u.toString())}`;
  } catch {
    const p = raw.startsWith("/") ? raw : `/${raw}`;
    return `p_${base64UrlEncodeString(p)}`;
  }
}

function toProxyUrl(baseUrl: string, path: string): string {
  return `${baseUrl.replace(/\/$/, "")}/images/${path}`;
}

async function collectGeneratedImageUrls(resp: Response): Promise<string[]> {
  const text = await resp.text();
  const lines = text.split("\n").map((l) => l.trim()).filter(Boolean);
  for (const line of lines) {
    let data: any;
    try {
      data = JSON.parse(line);
    } catch {
      continue;
    }
    const err = data?.error;
    if (err?.message) throw new Error(String(err.message));
    const grok = data?.result?.response;
    const urls = grok?.modelResponse?.generatedImageUrls;
    if (Array.isArray(urls)) {
      return urls.filter((u: any) => typeof u === "string" && u.trim() && u.trim() !== "/").map((u: string) => u.trim());
    }
  }
  return [];
}

openAiRoutes.get("/models", async (c) => {
  const ts = Math.floor(Date.now() / 1000);
  const data = Object.entries(MODEL_CONFIG).map(([id, cfg]) => ({
    id,
    object: "model",
    created: ts,
    owned_by: "x-ai",
    display_name: cfg.display_name,
    description: cfg.description,
    raw_model_path: cfg.raw_model_path,
    default_temperature: cfg.default_temperature,
    default_max_output_tokens: cfg.default_max_output_tokens,
    supported_max_output_tokens: cfg.supported_max_output_tokens,
    default_top_p: cfg.default_top_p,
  }));
  return c.json({ object: "list", data });
});

openAiRoutes.get("/models/:modelId", async (c) => {
  const modelId = c.req.param("modelId");
  if (!isValidModel(modelId)) return c.json(openAiError(`Model '${modelId}' not found`, "model_not_found"), 404);
  const cfg = MODEL_CONFIG[modelId]!;
  const ts = Math.floor(Date.now() / 1000);
  return c.json({
    id: modelId,
    object: "model",
    created: ts,
    owned_by: "x-ai",
    display_name: cfg.display_name,
    description: cfg.description,
    raw_model_path: cfg.raw_model_path,
    default_temperature: cfg.default_temperature,
    default_max_output_tokens: cfg.default_max_output_tokens,
    supported_max_output_tokens: cfg.supported_max_output_tokens,
    default_top_p: cfg.default_top_p,
  });
});

openAiRoutes.post("/chat/completions", async (c) => {
  const start = Date.now();
  const ip = getClientIp(c.req.raw);
  const keyName = c.get("apiAuth").name ?? "Unknown";

  const origin = new URL(c.req.url).origin;

  let requestedModel = "";
  try {
    const body = (await c.req.json()) as {
      model?: string;
      messages?: any[];
      stream?: boolean;
      n?: number;
      size?: string;
      image_config?: {
        aspect_ratio?: string;
        resolution?: string;
      };
      video_config?: {
        aspect_ratio?: string;
        video_length?: number;
        resolution?: string;
        preset?: string;
      };
    };

    requestedModel = String(body.model ?? "");
    if (!requestedModel) return c.json(openAiError("Missing 'model'", "missing_model"), 400);
    if (!Array.isArray(body.messages)) return c.json(openAiError("Missing 'messages'", "missing_messages"), 400);
    if (!isValidModel(requestedModel))
      return c.json(openAiError(`Model '${requestedModel}' not supported`, "model_not_supported"), 400);

    const settingsBundle = await getSettings(c.env);
    const cfg = MODEL_CONFIG[requestedModel]!;

    const retryCodes = Array.isArray(settingsBundle.grok.retry_status_codes)
      ? settingsBundle.grok.retry_status_codes
      : [401, 429];

    const stream = Boolean(body.stream);
    const maxRetry = 3;
    let lastErr: string | null = null;
    const imageNRaw = Number(body.n ?? 1);
    const imagineImageCount = Number.isFinite(imageNRaw)
      ? Math.max(1, Math.min(10, Math.floor(imageNRaw)))
      : 1;

    // === Quota check (best-effort) ===
    // - heavy: consumes both heavy + chat
    // - image model:
    //   - ws_imagine: counts as requested n
    //   - legacy imagine: counts as 2 images per request (grok upstream emits up to 2)
    // - video model: 1 video per request
    // - others: 1 chat per request
    const quotaKind = cfg.is_video_model ? "video" : cfg.is_image_model ? "image" : "chat";
    const quota = await enforceQuota({
      env: c.env,
      apiAuth: c.get("apiAuth"),
      model: requestedModel,
      kind: quotaKind as any,
      ...(cfg.is_image_model
        ? { imageCount: cfg.transport === "ws_imagine" ? imagineImageCount : 2 }
        : {}),
    });
    if (!quota.ok) return quota.resp;

    if (cfg.transport === "ws_imagine") {
      const { content } = extractContent(body.messages as any);
      const prompt = String(content ?? "").trim();
      if (!prompt) return c.json(openAiError("Missing image prompt in 'messages'", "missing_prompt"), 400);

      let aspectRatio: "1:1" | "2:3" | "3:2" | "9:16" | "16:9";
      try {
        aspectRatio = resolveImagineAspectRatio({ size: body.size, imageConfig: body.image_config });
      } catch (e) {
        return c.json(openAiError(e instanceof Error ? e.message : "Invalid image_config.aspect_ratio", "invalid_aspect_ratio"), 400);
      }

      const warning = hasResolutionDowngradeWarning(body.image_config);
      const baseUrl = (settingsBundle.global.base_url ?? "").trim() || origin;

      if (stream) {
        const sse = new ReadableStream<Uint8Array>({
          async start(controller) {
            const encoder = new TextEncoder();
            const chunkId = `chatcmpl-${crypto.randomUUID()}`;
            const created = Math.floor(Date.now() / 1000);
            let sentRole = false;
            let thinkOpened = false;
            const stageName: Record<string, string> = {
              preview: "preview",
              medium: "medium",
              final: "final",
            };
            const emitted = new Set<string>();

            const push = (contentText: string, finishReason: "stop" | null = null) => {
              controller.enqueue(
                encoder.encode(
                  streamChunk({
                    id: chunkId,
                    created,
                    model: requestedModel,
                    content: contentText,
                    finishReason,
                    includeRole: !sentRole,
                  }),
                ),
              );
              sentRole = true;
            };

            const done = () => controller.enqueue(encoder.encode("data: [DONE]\n\n"));

            try {
              push("<think>\nGenerating image(s)...\n");
              thinkOpened = true;
              const generated = await generateImagineWsWithRetries({
                env: c.env,
                settings: settingsBundle,
                requestedModel,
                prompt,
                aspectRatio,
                n: imagineImageCount,
                onProgress: async (event) => {
                  const key = `${event.completed}/${event.total}:${event.stage}`;
                  if (emitted.has(key)) return;
                  emitted.add(key);
                  const stage = stageName[event.stage] ?? event.stage;
                  push(`Image ${event.completed}/${event.total}: ${stage}\n`);
                },
              });

              if (!generated.urls.length) throw new Error("No image URL returned from imagine WS");
              const encoded = generated.urls.map((u) => encodeAssetPath(u));
              scheduleImagePrefetch(c, origin, encoded);
              if (thinkOpened) {
                push("</think>\n");
                thinkOpened = false;
              }
              push(markdownFromRawImageUrls({ baseUrl, rawUrls: generated.urls }), "stop");
              done();
              await addRequestLog(c.env.DB, {
                ip,
                model: requestedModel,
                duration: Number(((Date.now() - start) / 1000).toFixed(2)),
                status: 200,
                key_name: keyName,
                token_suffix: generated.tokenSuffix,
                error: "",
              });
              controller.close();
              return;
            } catch (e) {
              const msg = imagineErrorMessage(e);
              const status = imagineErrorStatus(e);
              if (thinkOpened) {
                push("</think>\n");
                thinkOpened = false;
              }
              push(`Error: ${msg}`, "stop");
              done();
              await addRequestLog(c.env.DB, {
                ip,
                model: requestedModel,
                duration: Number(((Date.now() - start) / 1000).toFixed(2)),
                status,
                key_name: keyName,
                token_suffix: "",
                error: msg,
              });
              controller.close();
            }
          },
        });

        const headers = new Headers({
          "Content-Type": "text/event-stream; charset=utf-8",
          "Cache-Control": "no-cache",
          Connection: "keep-alive",
          "X-Accel-Buffering": "no",
          "Access-Control-Allow-Origin": "*",
        });
        if (warning) headers.set("x-grok2api-warning", warning);
        return new Response(sse, { status: 200, headers });
      }

      try {
        const generated = await generateImagineWsWithRetries({
          env: c.env,
          settings: settingsBundle,
          requestedModel,
          prompt,
          aspectRatio,
          n: imagineImageCount,
        });

        if (!generated.urls.length) throw new Error("No image URL returned from imagine WS");
        const encoded = generated.urls.map((u) => encodeAssetPath(u));
        scheduleImagePrefetch(c, origin, encoded);
        const contentMd = markdownFromRawImageUrls({ baseUrl, rawUrls: generated.urls });

        const duration = (Date.now() - start) / 1000;
        await addRequestLog(c.env.DB, {
          ip,
          model: requestedModel,
          duration: Number(duration.toFixed(2)),
          status: 200,
          key_name: keyName,
          token_suffix: generated.tokenSuffix,
          error: "",
        });

        const res = c.json({
          id: `chatcmpl-${crypto.randomUUID()}`,
          object: "chat.completion",
          created: Math.floor(Date.now() / 1000),
          model: requestedModel,
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: contentMd },
              finish_reason: "stop",
            },
          ],
          usage: null,
        });
        return withOptionalWarningHeader(res, warning);
      } catch (e) {
        lastErr = imagineErrorMessage(e);
        const status = imagineErrorStatus(e);
        const duration = (Date.now() - start) / 1000;
        await addRequestLog(c.env.DB, {
          ip,
          model: requestedModel,
          duration: Number(duration.toFixed(2)),
          status,
          key_name: keyName,
          token_suffix: "",
          error: lastErr,
        });
        if (stream) return makeOpenAiErrorSse(lastErr, requestedModel);
        return c.json(openAiError(lastErr, "upstream_error"), (status || 500) as any);
      }
    }

    if (cfg.transport === "imagine_video") {
      const { content, images } = extractContent(body.messages as any);
      const prompt = String(content ?? "").trim() || "Generate a video";
      const imageInputs = images.slice(0, 1);

      try {
        const generated = await createImagineVideoUpstreamWithRetries({
          env: c.env,
          settings: settingsBundle,
          requestedModel,
          prompt,
          imageInputs,
          ...(body.video_config ? { videoConfig: body.video_config } : {}),
        });

        if (stream) {
          const sse = createOpenAiStreamFromGrokNdjson(generated.upstream, {
            cookie: generated.cookie,
            settings: settingsBundle.grok,
            global: settingsBundle.global,
            origin,
            requestedModel,
            onFinish: async ({ status, duration }) => {
              await addRequestLog(c.env.DB, {
                ip,
                model: requestedModel,
                duration: Number(duration.toFixed(2)),
                status,
                key_name: keyName,
                token_suffix: generated.tokenSuffix,
                error: status === 200 ? "" : "stream_error",
              });
            },
          });

          return new Response(sse, {
            status: 200,
            headers: {
              "Content-Type": "text/event-stream; charset=utf-8",
              "Cache-Control": "no-cache",
              Connection: "keep-alive",
              "X-Accel-Buffering": "no",
              "Access-Control-Allow-Origin": "*",
            },
          });
        }

        const json = await parseOpenAiFromGrokNdjson(generated.upstream, {
          cookie: generated.cookie,
          settings: settingsBundle.grok,
          global: settingsBundle.global,
          origin,
          requestedModel,
        });

        const duration = (Date.now() - start) / 1000;
        await addRequestLog(c.env.DB, {
          ip,
          model: requestedModel,
          duration: Number(duration.toFixed(2)),
          status: 200,
          key_name: keyName,
          token_suffix: generated.tokenSuffix,
          error: "",
        });

        return c.json(json);
      } catch (e) {
        lastErr = imagineErrorMessage(e);
        const status = imagineErrorStatus(e);
        const duration = (Date.now() - start) / 1000;
        await addRequestLog(c.env.DB, {
          ip,
          model: requestedModel,
          duration: Number(duration.toFixed(2)),
          status,
          key_name: keyName,
          token_suffix: "",
          error: lastErr,
        });
        return c.json(openAiError(lastErr, "upstream_error"), (status || 500) as any);
      }
    }

    for (let attempt = 0; attempt < maxRetry; attempt++) {
      const chosen = await selectBestToken(c.env.DB, requestedModel);
      if (!chosen) return c.json(openAiError("No available token", "NO_AVAILABLE_TOKEN"), 503);

      const jwt = chosen.token;
      const cf = normalizeCfCookie(settingsBundle.grok.cf_clearance ?? "");
      const cookie = cf ? `sso-rw=${jwt};sso=${jwt};${cf}` : `sso-rw=${jwt};sso=${jwt}`;

      const { content, images } = extractContent(body.messages as any);
      const isVideoModel = Boolean(cfg.is_video_model);
      const imgInputs = isVideoModel && images.length > 1 ? images.slice(0, 1) : images;

      try {
        const uploads = await mapLimit(imgInputs, 5, (u) => uploadImage(u, cookie, settingsBundle.grok));
        const imgIds = uploads.map((u) => u.fileId).filter(Boolean);
        const imgUris = uploads.map((u) => u.fileUri).filter(Boolean);

        let postId: string | undefined;
        if (isVideoModel) {
          if (imgUris.length) {
            const post = await createPost(imgUris[0]!, cookie, settingsBundle.grok);
            postId = post.postId || undefined;
          } else {
            const post = await createMediaPost(
              { mediaType: "MEDIA_POST_TYPE_VIDEO", prompt: content },
              cookie,
              settingsBundle.grok,
            );
            postId = post.postId || undefined;
          }
        }

        const { payload, referer } = buildConversationPayload({
          requestModel: requestedModel,
          content,
          imgIds,
          imgUris,
          ...(postId ? { postId } : {}),
          ...(isVideoModel && body.video_config ? { videoConfig: body.video_config } : {}),
          settings: settingsBundle.grok,
        });

        const upstream = await sendConversationRequest({
          payload,
          cookie,
          settings: settingsBundle.grok,
          ...(referer ? { referer } : {}),
        });

        if (!upstream.ok) {
          const txt = await upstream.text().catch(() => "");
          lastErr = `Upstream ${upstream.status}: ${txt.slice(0, 200)}`;
          await recordTokenFailure(c.env.DB, jwt, upstream.status, txt.slice(0, 200));
          await applyCooldown(c.env.DB, jwt, upstream.status);
          if (retryCodes.includes(upstream.status) && attempt < maxRetry - 1) continue;
          break;
        }

        if (stream) {
          const sse = createOpenAiStreamFromGrokNdjson(upstream, {
            cookie,
            settings: settingsBundle.grok,
            global: settingsBundle.global,
            origin,
            requestedModel,
            onFinish: async ({ status, duration }) => {
              await addRequestLog(c.env.DB, {
                ip,
                model: requestedModel,
                duration: Number(duration.toFixed(2)),
                status,
                key_name: keyName,
                token_suffix: jwt.slice(-6),
                error: status === 200 ? "" : "stream_error",
              });
            },
          });

          return new Response(sse, {
            status: 200,
            headers: {
              "Content-Type": "text/event-stream; charset=utf-8",
              "Cache-Control": "no-cache",
              Connection: "keep-alive",
              "X-Accel-Buffering": "no",
              "Access-Control-Allow-Origin": "*",
            },
          });
        }

        const json = await parseOpenAiFromGrokNdjson(upstream, {
          cookie,
          settings: settingsBundle.grok,
          global: settingsBundle.global,
          origin,
          requestedModel,
        });

        const duration = (Date.now() - start) / 1000;
        await addRequestLog(c.env.DB, {
          ip,
          model: requestedModel,
          duration: Number(duration.toFixed(2)),
          status: 200,
          key_name: keyName,
          token_suffix: jwt.slice(-6),
          error: "",
        });

        return c.json(json);
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        lastErr = msg;
        await recordTokenFailure(c.env.DB, jwt, 500, msg);
        await applyCooldown(c.env.DB, jwt, 500);
        if (attempt < maxRetry - 1) continue;
      }
    }

    const duration = (Date.now() - start) / 1000;
    await addRequestLog(c.env.DB, {
      ip,
      model: requestedModel,
      duration: Number(duration.toFixed(2)),
      status: 500,
      key_name: keyName,
      token_suffix: "",
      error: lastErr ?? "unknown_error",
    });

    return c.json(openAiError(lastErr ?? "Upstream error", "upstream_error"), 500);
  } catch (e) {
    const duration = (Date.now() - start) / 1000;
    await addRequestLog(c.env.DB, {
      ip,
      model: requestedModel || "unknown",
      duration: Number(duration.toFixed(2)),
      status: 500,
      key_name: keyName,
      token_suffix: "",
      error: e instanceof Error ? e.message : String(e),
    });
    return c.json(openAiError("Internal error", "internal_error"), 500);
  }
});

openAiRoutes.post("/images/generations", async (c) => {
  const start = Date.now();
  const ip = getClientIp(c.req.raw);
  const keyName = c.get("apiAuth").name ?? "Unknown";
  const origin = new URL(c.req.url).origin;

  let requestedModel = "grok-imagine";
  try {
    const body = (await c.req.json()) as {
      prompt?: string;
      model?: string;
      n?: number;
      size?: string;
      response_format?: string;
      image_config?: {
        aspect_ratio?: string;
        resolution?: string;
      };
    };
    const prompt = String(body.prompt ?? "").trim();
    if (!prompt) return c.json(openAiError("Missing 'prompt'", "missing_prompt"), 400);
    const responseFormat = normalizeImageResponseFormat(body.response_format);
    if (!responseFormat)
      return c.json(openAiError("Invalid 'response_format', expected 'url' or 'b64_json'", "invalid_response_format"), 400);
    const warning = hasResolutionDowngradeWarning(body.image_config);

    requestedModel = String(body.model ?? "grok-imagine").trim() || "grok-imagine";
    if (!isValidModel(requestedModel))
      return c.json(openAiError(`Model '${requestedModel}' not supported`, "model_not_supported"), 400);
    const cfg = MODEL_CONFIG[requestedModel]!;
    if (!cfg.is_image_model) return c.json(openAiError(`Model '${requestedModel}' is not an image model`, "invalid_model"), 400);

    const nRaw = Number(body.n ?? 1);
    const n = Number.isFinite(nRaw) ? Math.max(1, Math.min(10, Math.floor(nRaw))) : 1;

    const quota = await enforceQuota({
      env: c.env,
      apiAuth: c.get("apiAuth"),
      model: requestedModel,
      kind: "image",
      imageCount: n,
    });
    if (!quota.ok) return quota.resp;

    const settingsBundle = await getSettings(c.env);
    const cf = normalizeCfCookie(settingsBundle.grok.cf_clearance ?? "");
    const baseUrl = (settingsBundle.global.base_url ?? "").trim() || origin;

    if (cfg.transport === "ws_imagine") {
      let aspectRatio: "1:1" | "2:3" | "3:2" | "9:16" | "16:9";
      try {
        aspectRatio = resolveImagineAspectRatio({ size: body.size, imageConfig: body.image_config });
      } catch (e) {
        return c.json(openAiError(e instanceof Error ? e.message : "Invalid image_config.aspect_ratio", "invalid_aspect_ratio"), 400);
      }

      try {
        const generated = await generateImagineWsWithRetries({
          env: c.env,
          settings: settingsBundle,
          requestedModel,
          prompt,
          aspectRatio,
          n,
        });

        const rawUrls = generated.urls.slice(0, n);
        if (!rawUrls.length && responseFormat === "url")
          throw new Error("No image URL returned from imagine WS");
        const encoded = rawUrls.map((u) => encodeAssetPath(u));
        scheduleImagePrefetch(c, origin, encoded);

        let data: Array<{ url?: string; b64_json?: string }> = [];
        if (responseFormat === "url") {
          data = encoded.map((path) => ({ url: toProxyUrl(baseUrl, path) }));
        } else {
          const b64List = generated.b64List
            .map((item) => String(item ?? "").trim())
            .filter(Boolean)
            .slice(0, n);

          if (b64List.length < n && encoded.length) {
            const fetched = await mapLimit(encoded, 2, async (path) => fetchProxyImageAsBase64(origin, path));
            for (const one of fetched) {
              if (b64List.length >= n) break;
              b64List.push(one);
            }
          }

          if (!b64List.length) throw new Error("No b64_json returned from imagine WS");
          data = b64List.map((b64_json) => ({ b64_json }));
        }

        const duration = (Date.now() - start) / 1000;
        await addRequestLog(c.env.DB, {
          ip,
          model: requestedModel,
          duration: Number(duration.toFixed(2)),
          status: 200,
          key_name: keyName,
          token_suffix: generated.tokenSuffix,
          error: "",
        });

        const res = c.json({
          created: Math.floor(Date.now() / 1000),
          data,
        });
        return withOptionalWarningHeader(res, warning);
      } catch (e) {
        const msg = imagineErrorMessage(e);
        const status = imagineErrorStatus(e);
        const duration = (Date.now() - start) / 1000;
        await addRequestLog(c.env.DB, {
          ip,
          model: requestedModel,
          duration: Number(duration.toFixed(2)),
          status,
          key_name: keyName,
          token_suffix: "",
          error: msg,
        });
        return c.json(openAiError(msg, "upstream_error"), (status || 500) as any);
      }
    }

    const calls = Math.ceil(n / 2);

    const doOne = async (): Promise<string[]> => {
      const chosen = await selectBestToken(c.env.DB, requestedModel);
      if (!chosen) throw new Error("No available token");
      const cookie = cf ? `sso-rw=${chosen.token};sso=${chosen.token};${cf}` : `sso-rw=${chosen.token};sso=${chosen.token}`;
      const { payload, referer } = buildConversationPayload({
        requestModel: requestedModel,
        content: `Image Generation:${prompt}`,
        imgIds: [],
        imgUris: [],
        settings: settingsBundle.grok,
      });
      const upstream = await sendConversationRequest({
        payload,
        cookie,
        settings: settingsBundle.grok,
        ...(referer ? { referer } : {}),
      });
      if (!upstream.ok) {
        const txt = await upstream.text().catch(() => "");
        await recordTokenFailure(c.env.DB, chosen.token, upstream.status, txt.slice(0, 200));
        await applyCooldown(c.env.DB, chosen.token, upstream.status);
        throw new Error(`Upstream ${upstream.status}: ${txt.slice(0, 200)}`);
      }
      return collectGeneratedImageUrls(upstream);
    };

    const rawUrlsNested = await mapLimit(Array.from({ length: calls }).map((_, i) => i), 3, async () => doOne());
    const rawUrls = rawUrlsNested.flat().filter(Boolean).slice(0, n);
    const encoded = rawUrls.map((u) => encodeAssetPath(u));
    scheduleImagePrefetch(c, origin, encoded);

    let data: Array<{ url?: string; b64_json?: string }> = [];
    if (responseFormat === "url") {
      data = encoded.map((path) => ({ url: toProxyUrl(baseUrl, path) }));
    } else {
      const b64 = await mapLimit(encoded, 2, async (path) => fetchProxyImageAsBase64(origin, path));
      data = b64.map((b64_json) => ({ b64_json }));
    }

    const duration = (Date.now() - start) / 1000;
    await addRequestLog(c.env.DB, {
      ip,
      model: requestedModel,
      duration: Number(duration.toFixed(2)),
      status: 200,
      key_name: keyName,
      token_suffix: "",
      error: "",
    });

    const res = c.json({
      created: Math.floor(Date.now() / 1000),
      data,
    });
    return withOptionalWarningHeader(res, warning);
  } catch (e) {
    const duration = (Date.now() - start) / 1000;
    await addRequestLog(c.env.DB, {
      ip,
      model: requestedModel || "image",
      duration: Number(duration.toFixed(2)),
      status: 500,
      key_name: keyName,
      token_suffix: "",
      error: e instanceof Error ? e.message : String(e),
    });
    return c.json(openAiError(e instanceof Error ? e.message : "Internal error", "internal_error"), 500);
  }
});

openAiRoutes.post("/uploads/image", async (c) => {
  try {
    const form = await c.req.formData();
    const file = form.get("file");
    if (!(file instanceof File)) return c.json(openAiError("Missing file", "missing_file"), 400);

    const mime = String(file.type || "application/octet-stream");
    if (!mime.toLowerCase().startsWith("image/"))
      return c.json(openAiError(`Unsupported mime: ${mime}`, "unsupported_file"), 400);

    const bytes = await file.arrayBuffer();
    const size = bytes.byteLength;
    const maxBytes = Math.min(25 * 1024 * 1024, Math.max(1, parseIntSafe(c.env.KV_CACHE_MAX_BYTES, 25 * 1024 * 1024)));
    if (size > maxBytes) return c.json(openAiError(`File too large (${size} > ${maxBytes})`, "file_too_large"), 413);

    const ext = (() => {
      const m = mime.toLowerCase();
      if (m === "image/png") return "png";
      if (m === "image/webp") return "webp";
      if (m === "image/gif") return "gif";
      if (m === "image/jpeg" || m === "image/jpg") return "jpg";
      return "jpg";
    })();

    const name = `upload-${crypto.randomUUID()}.${ext}`;
    const kvKey = `image/${name}`;

    const tz = parseIntSafe(c.env.CACHE_RESET_TZ_OFFSET_MINUTES, 480);
    const expiresAt = nextLocalMidnightExpirationSeconds(nowMs(), tz);

    await c.env.KV_CACHE.put(kvKey, bytes, {
      expiration: expiresAt,
      metadata: { contentType: mime, size },
    });

    const now = nowMs();
    await upsertCacheRow(c.env.DB, {
      key: kvKey,
      type: "image",
      size,
      content_type: mime,
      created_at: now,
      last_access_at: now,
      expires_at: expiresAt * 1000,
    });

    return c.json({
      url: `/images/${encodeURIComponent(name)}`,
      name,
      size_bytes: size,
    });
  } catch (e) {
    return c.json(openAiError(e instanceof Error ? e.message : "Internal error", "internal_error"), 500);
  }
});

openAiRoutes.options("/*", (c) => c.body(null, 204));
