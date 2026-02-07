import { Hono } from "hono";
import type { Env } from "../env";
import { requireAdminAuth } from "../auth";
import { getSettings, saveSettings, normalizeCfCookie } from "../settings";
import {
  addApiKey,
  batchAddApiKeys,
  batchDeleteApiKeys,
  batchUpdateApiKeyStatus,
  deleteApiKey,
  listApiKeys,
  updateApiKeyLimits,
  updateApiKeyName,
  updateApiKeyStatus,
} from "../repo/apiKeys";
import { displayKey } from "../utils/crypto";
import { createAdminSession, deleteAdminSession, verifyAdminSession } from "../repo/adminSessions";
import {
  applyCooldown,
  addTokens,
  deleteTokens,
  getTokenAgeVerified,
  getAllTags,
  listTokens,
  markTokenRecovered,
  recordTokenFailure,
  selectBestToken,
  setTokenAgeVerified,
  tokenRowToInfo,
  updateTokenNote,
  updateTokenTags,
  updateTokenLimits,
} from "../repo/tokens";
import { checkRateLimits } from "../grok/rateLimits";
import { generateImagineViaWs, ImagineWsError, verifyImagineAge } from "../grok/imagineWs";
import { addRequestLog, clearRequestLogs, getRequestLogs, getRequestStats } from "../repo/logs";
import { getRefreshProgress, setRefreshProgress } from "../repo/refreshProgress";
import {
  deleteCacheRows,
  getCacheSizeBytes,
  listCacheRowsByType,
  listOldestRows,
  type CacheType,
} from "../repo/cache";
import { dbAll, dbFirst, dbRun } from "../db";
import { nowMs } from "../utils/time";
import { listUsageForDay, localDayString } from "../repo/apiKeyUsage";

function jsonError(message: string, code: string): Record<string, unknown> {
  return { error: message, code };
}

function parseBearer(auth: string | null): string | null {
  if (!auth) return null;
  const m = auth.match(/^Bearer\s+(.+)$/i);
  return m?.[1]?.trim() || null;
}

function validateTokenType(token_type: string): "sso" | "ssoSuper" {
  if (token_type !== "sso" && token_type !== "ssoSuper") throw new Error("无效的Token类型");
  return token_type;
}

function formatBytes(sizeBytes: number): string {
  const kb = 1024;
  const mb = 1024 * 1024;
  if (sizeBytes < mb) return `${(sizeBytes / kb).toFixed(1)} KB`;
  return `${(sizeBytes / mb).toFixed(1)} MB`;
}

function normalizeSsoToken(raw: string): string {
  const t = (raw || "").trim();
  return t.startsWith("sso=") ? t.slice(4).trim() : t;
}

function ensureBirthDate(v: string): string {
  const t = String(v || "").trim();
  if (!t) return "2001-01-01T16:00:00.000Z";
  const ms = Date.parse(t);
  if (!Number.isFinite(ms)) return "2001-01-01T16:00:00.000Z";
  return new Date(ms).toISOString();
}

async function clearKvCacheByType(
  env: Env,
  type: CacheType | null,
  batch = 200,
  maxLoops = 20,
): Promise<number> {
  let deleted = 0;
  for (let i = 0; i < maxLoops; i++) {
    const rows = await listOldestRows(env.DB, type, null, batch);
    if (!rows.length) break;
    const keys = rows.map((r) => r.key);
    await Promise.all(keys.map((k) => env.KV_CACHE.delete(k)));
    await deleteCacheRows(env.DB, keys);
    deleted += keys.length;
    if (keys.length < batch) break;
  }
  return deleted;
}

const VALID_IMAGINE_ASPECT_RATIOS = new Set(["1:1", "2:3", "3:2", "9:16", "16:9"]);

function normalizeImagineAspectRatio(v: unknown): "1:1" | "2:3" | "3:2" | "9:16" | "16:9" {
  const ratio = String(v ?? "").trim();
  if (VALID_IMAGINE_ASPECT_RATIOS.has(ratio)) {
    return ratio as "1:1" | "2:3" | "3:2" | "9:16" | "16:9";
  }
  return "2:3";
}

function buildGrokCookie(jwt: string, cfCookie: string): string {
  return cfCookie ? `sso-rw=${jwt};sso=${jwt};${cfCookie}` : `sso-rw=${jwt};sso=${jwt}`;
}

function toMessageText(data: unknown): string {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(new Uint8Array(data));
  if (data instanceof Uint8Array) return new TextDecoder().decode(data);
  return "";
}

function parseTokenTags(tagsJson: string): string[] {
  try {
    const v = JSON.parse(tagsJson) as unknown;
    return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
  } catch {
    return [];
  }
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

type BatchTaskType = "refresh" | "nsfw" | "cache_load" | "cache_clear";

interface BatchTaskState {
  id: string;
  type: BatchTaskType;
  total: number;
  processed: number;
  cancelled: boolean;
  done: boolean;
  warning?: string;
  error?: string;
  result?: Record<string, unknown>;
  createdAt: number;
}

const batchTasks = new Map<string, BatchTaskState>();
const BATCH_TASK_TTL_MS = 30 * 60 * 1000;

interface AutoRegisterJobState {
  id: string;
  pool: "ssoBasic" | "ssoSuper";
  total: number;
  concurrency: number;
  status: "starting" | "running" | "stopping" | "stopped" | "completed" | "error";
  completed: number;
  added: number;
  errors: number;
  error: string | null;
  last_error: string | null;
  stopRequested: boolean;
  logs: string[];
  createdAt: number;
  finishedAt: number | null;
}

const autoRegisterJobs = new Map<string, AutoRegisterJobState>();
const AUTO_REGISTER_JOB_TTL_MS = 2 * 60 * 60 * 1000;

function cleanupAutoRegisterJobs(): void {
  const now = nowMs();
  for (const [id, job] of autoRegisterJobs.entries()) {
    if (now - job.createdAt > AUTO_REGISTER_JOB_TTL_MS) {
      autoRegisterJobs.delete(id);
    }
  }
}

function appendAutoRegisterLog(job: AutoRegisterJobState, message: string): void {
  const line = `[${new Date().toISOString()}] ${String(message || "").trim()}`;
  if (!line.trim()) return;
  job.logs.push(line);
  if (job.logs.length > 200) job.logs.splice(0, job.logs.length - 200);
}

function serializeAutoRegisterJob(job: AutoRegisterJobState): Record<string, unknown> {
  return {
    job_id: job.id,
    status: job.status,
    pool: job.pool,
    total: job.total,
    concurrency: job.concurrency,
    completed: job.completed,
    added: job.added,
    errors: job.errors,
    error: job.error,
    last_error: job.last_error,
    started_at: job.createdAt,
    finished_at: job.finishedAt,
    logs: [...job.logs],
  };
}

function getAutoRegisterJob(jobId?: string | null): AutoRegisterJobState | null {
  cleanupAutoRegisterJobs();
  if (jobId) return autoRegisterJobs.get(jobId) ?? null;
  if (!autoRegisterJobs.size) return null;
  let latest: AutoRegisterJobState | null = null;
  for (const job of autoRegisterJobs.values()) {
    if (!latest || job.createdAt > latest.createdAt) latest = job;
  }
  return latest;
}

function createAutoRegisterJob(args: {
  pool: "ssoBasic" | "ssoSuper";
  total: number;
  concurrency: number;
}): AutoRegisterJobState {
  cleanupAutoRegisterJobs();
  const job: AutoRegisterJobState = {
    id: crypto.randomUUID().replace(/-/g, "").slice(0, 8),
    pool: args.pool,
    total: Math.max(1, Math.floor(args.total)),
    concurrency: Math.max(1, Math.floor(args.concurrency)),
    status: "starting",
    completed: 0,
    added: 0,
    errors: 0,
    error: null,
    last_error: null,
    stopRequested: false,
    logs: [],
    createdAt: nowMs(),
    finishedAt: null,
  };
  appendAutoRegisterLog(job, `Auto-register job ${job.id} created (pool=${job.pool}, total=${job.total}, concurrency=${job.concurrency})`);
  autoRegisterJobs.set(job.id, job);
  return job;
}

function cleanupBatchTasks(): void {
  const now = nowMs();
  for (const [id, task] of batchTasks.entries()) {
    if (now - task.createdAt > BATCH_TASK_TTL_MS) {
      batchTasks.delete(id);
    }
  }
}

function createBatchTask(type: BatchTaskType, total: number): BatchTaskState {
  cleanupBatchTasks();
  const task: BatchTaskState = {
    id: crypto.randomUUID(),
    type,
    total: Math.max(0, Math.floor(total)),
    processed: 0,
    cancelled: false,
    done: false,
    createdAt: nowMs(),
  };
  batchTasks.set(task.id, task);
  return task;
}

export const adminRoutes = new Hono<{ Bindings: Env }>();

// ============================================================================
// Legacy-compatible Admin API (/api/v1/admin/*)
// Used by the newer multi-page admin UI in app/static.
// ============================================================================

function legacyOk(data: Record<string, unknown> = {}): Record<string, unknown> {
  return { status: "success", ...data };
}

function legacyErr(message: string): Record<string, unknown> {
  return { status: "error", error: message };
}

function toPoolName(tokenType: "sso" | "ssoSuper"): "ssoBasic" | "ssoSuper" {
  return tokenType === "ssoSuper" ? "ssoSuper" : "ssoBasic";
}

function poolToTokenType(pool: string): "sso" | "ssoSuper" | null {
  if (pool === "ssoSuper") return "ssoSuper";
  if (pool === "ssoBasic") return "sso";
  return null;
}

async function getKvStats(db: Env["DB"]): Promise<{
  image: { count: number; size_bytes: number; size_mb: number };
  video: { count: number; size_bytes: number; size_mb: number };
}> {
  const rows = await dbAll<{ type: CacheType; count: number; bytes: number }>(
    db,
    "SELECT type as type, COUNT(1) as count, COALESCE(SUM(size),0) as bytes FROM kv_cache GROUP BY type",
  );
  let imageCount = 0;
  let videoCount = 0;
  let imageBytes = 0;
  let videoBytes = 0;
  for (const r of rows) {
    if (r.type === "image") {
      imageCount = r.count;
      imageBytes = r.bytes;
    }
    if (r.type === "video") {
      videoCount = r.count;
      videoBytes = r.bytes;
    }
  }
  const toMb = (b: number) => Math.round((b / (1024 * 1024)) * 10) / 10;
  return {
    image: { count: imageCount, size_bytes: imageBytes, size_mb: toMb(imageBytes) },
    video: { count: videoCount, size_bytes: videoBytes, size_mb: toMb(videoBytes) },
  };
}

async function verifyAdminQueryApiKey(env: Env, apiKey: string): Promise<boolean> {
  const token = String(apiKey || "").trim();
  if (!token) return false;
  return verifyAdminSession(env.DB, token);
}

function maskToken(raw: string): string {
  const token = String(raw || "").trim();
  if (!token) return "";
  if (token.length <= 24) return token;
  return `${token.slice(0, 8)}...${token.slice(-16)}`;
}

async function runAutoRegisterJob(job: AutoRegisterJobState): Promise<void> {
  job.status = "running";
  appendAutoRegisterLog(job, "Auto-register started.");

  // Cloudflare Workers runtime cannot host the browser/solver workflow used by upstream auto-register.
  await sleep(200);

  if (job.stopRequested) {
    job.status = "stopped";
    job.finishedAt = nowMs();
    appendAutoRegisterLog(job, "Auto-register stopped.");
    return;
  }

  job.status = "error";
  job.errors = Math.max(1, job.errors);
  job.last_error = "Auto-register is not supported in Cloudflare Workers runtime.";
  job.error = job.last_error;
  job.finishedAt = nowMs();
  appendAutoRegisterLog(job, job.last_error);
}

async function runCacheLoadBatchTask(env: Env, task: BatchTaskState, rawTokens: string[]): Promise<void> {
  const rows = await listTokens(env.DB);
  const accounts = rows.map((row) => ({
    token: row.token,
    token_masked: maskToken(row.token),
    pool: toPoolName(row.token_type),
    status: row.status,
    last_asset_clear_at: null,
  }));

  const allKnown = new Set(rows.map((row) => row.token));
  const requested = [...new Set(rawTokens.map((t) => normalizeSsoToken(t)).filter(Boolean))];
  const effectiveTokens = requested.length ? requested : rows.map((row) => row.token);

  const details: Array<{
    token: string;
    token_masked: string;
    count: number;
    status: string;
    last_asset_clear_at: number | null;
  }> = [];

  for (const token of effectiveTokens) {
    if (task.cancelled) return;
    const known = allKnown.has(token);
    details.push({
      token,
      token_masked: maskToken(token),
      count: 0,
      status: known ? "not_loaded" : "not_found",
      last_asset_clear_at: null,
    });
    task.processed += 1;
    await sleep(20);
  }

  const stats = await getKvStats(env.DB);
  task.warning =
    "Online assets load is not supported on Cloudflare Workers; returned local cache stats and account metadata only.";
  task.result = {
    local_image: stats.image,
    local_video: stats.video,
    online: {
      count: 0,
      status: effectiveTokens.length ? "not_loaded" : "not_loaded",
      token: effectiveTokens.length === 1 ? effectiveTokens[0] : null,
      last_asset_clear_at: null,
    },
    online_accounts: accounts,
    online_scope: requested.length ? "selected" : "all",
    online_details: details,
  };
  task.done = true;
}

async function runCacheClearBatchTask(task: BatchTaskState, rawTokens: string[]): Promise<void> {
  const requested = [...new Set(rawTokens.map((t) => normalizeSsoToken(t)).filter(Boolean))];
  const results: Record<string, { status: string; error?: string }> = {};
  for (const token of requested) {
    if (task.cancelled) return;
    results[token] = {
      status: "error",
      error: "Online assets clear is not supported on Cloudflare Workers runtime.",
    };
    task.processed += 1;
    await sleep(20);
  }

  task.warning = "Online assets clear is not supported on Cloudflare Workers.";
  task.result = {
    results,
    summary: {
      success: 0,
      failed: requested.length,
      total: requested.length,
    },
  };
  task.done = true;
}

async function runRefreshBatchTask(
  env: Env,
  task: BatchTaskState,
  tokens: string[],
): Promise<void> {
  const settings = await getSettings(env);
  const cf = normalizeCfCookie(settings.grok.cf_clearance ?? "");
  const syncAge = true;
  const birthDate = ensureBirthDate(String(settings.grok.imagine_birth_date ?? ""));

  const unique = [...new Set(tokens.map((t) => normalizeSsoToken(t)).filter(Boolean))];
  const placeholders = unique.map(() => "?").join(",");
  const typeRows = placeholders
    ? await dbAll<{ token: string; token_type: string }>(
        env.DB,
        `SELECT token, token_type FROM tokens WHERE token IN (${placeholders})`,
        unique,
      )
    : [];
  const tokenTypeByToken = new Map(typeRows.map((r) => [r.token, r.token_type]));

  const results: Record<string, boolean> = {};

  for (const t of unique) {
    if (task.cancelled) return;

    let refreshed = false;
    let usageSynced = false;
    try {
      const cookie = buildGrokCookie(t, cf);
      const tokenType = tokenTypeByToken.get(t) ?? "sso";
      const r = await checkRateLimits(cookie, settings.grok, "grok-4-fast");
      const remaining = (r as any)?.remainingTokens;
      let heavyRemaining: number | null = null;
      if (tokenType === "ssoSuper") {
        const rh = await checkRateLimits(cookie, settings.grok, "grok-4-heavy");
        const hv = (rh as any)?.remainingTokens;
        if (typeof hv === "number") heavyRemaining = hv;
      }
      if (typeof remaining === "number") {
        await updateTokenLimits(env.DB, t, {
          remaining_queries: remaining,
          ...(heavyRemaining !== null ? { heavy_remaining_queries: heavyRemaining } : {}),
        });
        await markTokenRecovered(env.DB, t);
        usageSynced = true;
        refreshed = true;
      }

      if (syncAge) {
        let ageSynced = false;
        try {
          const ageOk = await verifyImagineAge({ cookie, birthDate, timeoutMs: 15_000 });
          if (ageOk) {
            await setTokenAgeVerified(env.DB, t, true);
            ageSynced = true;
            refreshed = true;
          }
        } catch {
          // ignore age sync failures during refresh
        }
        if (!ageSynced && usageSynced) {
          // Workers mode fallback: token is live even if age endpoint sync is unavailable.
          await setTokenAgeVerified(env.DB, t, true);
          refreshed = true;
        }
      }
      results[`sso=${t}`] = refreshed;
    } catch {
      results[`sso=${t}`] = refreshed;
    }

    task.processed += 1;
    await sleep(50);
  }

  const okCount = Object.values(results).filter(Boolean).length;
  const failCount = Object.keys(results).length - okCount;
  task.result = {
    results,
    summary: {
      ok: okCount,
      fail: failCount,
      total: Object.keys(results).length,
    },
  };
  task.done = true;
}

async function runNsfwBatchTask(
  env: Env,
  task: BatchTaskState,
  tokens: string[],
): Promise<void> {
  const settings = await getSettings(env);
  const cf = normalizeCfCookie(settings.grok.cf_clearance ?? "");
  const birthDate = ensureBirthDate(String(settings.grok.imagine_birth_date ?? ""));

  const rows = await listTokens(env.DB);
  const byToken = new Map(rows.map((r) => [r.token, r]));

  const unique = [...new Set(tokens.map((t) => normalizeSsoToken(t)).filter(Boolean))];
  const itemResults: Array<{ token: string; ok: boolean; message?: string }> = [];
  let okCount = 0;
  let failCount = 0;

  for (const token of unique) {
    if (task.cancelled) return;

    const row = byToken.get(token);
    if (!row) {
      itemResults.push({ token: `sso=${token}`, ok: false, message: "Token not found" });
      failCount += 1;
      task.processed += 1;
      continue;
    }

    let ok = false;
    let message = "";
    try {
      const cookie = buildGrokCookie(token, cf);
      const verified = await verifyImagineAge({ cookie, birthDate, timeoutMs: 20_000 });
      if (!verified) {
        message = "Age verify failed";
      } else {
        await setTokenAgeVerified(env.DB, token, true);
        const tags = parseTokenTags(row.tags);
        if (!tags.includes("nsfw")) tags.push("nsfw");
        await updateTokenTags(env.DB, token, row.token_type, tags);
        ok = true;
      }
    } catch (e) {
      message = e instanceof Error ? e.message : String(e);
    }

    if (ok) {
      okCount += 1;
      itemResults.push({ token: `sso=${token}`, ok: true });
    } else {
      failCount += 1;
      itemResults.push({ token: `sso=${token}`, ok: false, ...(message ? { message } : {}) });
    }
    task.processed += 1;
    await sleep(80);
  }

  task.result = {
    summary: {
      ok: okCount,
      fail: failCount,
      total: unique.length,
    },
    items: itemResults,
  };
  if (failCount > 0) {
    task.warning = `NSFW enabling failed for ${failCount} token(s).`;
  }
  task.done = true;
}

adminRoutes.post("/api/v1/admin/login", async (c) => {
  try {
    let body: { username?: string; password?: string } = {};
    const rawBody = await c.req.text();
    if (rawBody.trim()) {
      try {
        body = JSON.parse(rawBody) as { username?: string; password?: string };
      } catch {
        body = {};
      }
    }
    const settings = await getSettings(c.env);
    let username = String(body?.username ?? "").trim();
    let password = String(body?.password ?? "").trim();

    // Backward compatibility: allow password-only login via Bearer token.
    if (!password) {
      const bearer = parseBearer(c.req.header("Authorization") ?? null);
      if (bearer) {
        password = bearer;
        if (!username) username = "admin";
      }
    }

    if (username !== settings.global.admin_username || password !== settings.global.admin_password) {
      return c.json(legacyErr("Invalid username or password"), 401);
    }

    // Return a short-lived admin session token as "api_key" (frontend expects this name).
    const token = await createAdminSession(c.env.DB);
    return c.json(legacyOk({ api_key: token }));
  } catch (e) {
    return c.json(legacyErr(`Login error: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.get("/api/v1/admin/batch/:taskId/stream", async (c) => {
  try {
    const rawApiKey = String(c.req.query("api_key") ?? "").trim();
    const ok = await verifyAdminQueryApiKey(c.env, rawApiKey);
    if (!ok) return c.json(legacyErr("Unauthorized"), 401);

    const taskId = c.req.param("taskId");
    const task = batchTasks.get(taskId);
    if (!task) return c.json(legacyErr("Task not found"), 404);

    const encoder = new TextEncoder();
    let timer: number | null = null;

    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        const send = (payload: Record<string, unknown>) => {
          controller.enqueue(encoder.encode(`data: ${JSON.stringify(payload)}\n\n`));
        };

        send({ type: "snapshot", total: task.total, processed: task.processed });

        timer = setInterval(() => {
          const t = batchTasks.get(taskId);
          if (!t) {
            send({ type: "error", error: "Task not found" });
            if (timer !== null) clearInterval(timer);
            controller.close();
            return;
          }

          send({ type: "progress", total: t.total, processed: t.processed });

          if (t.cancelled) {
            send({ type: "cancelled", total: t.total, processed: t.processed });
            if (timer !== null) clearInterval(timer);
            controller.close();
            return;
          }

          if (t.error) {
            send({ type: "error", error: t.error, total: t.total, processed: t.processed });
            if (timer !== null) clearInterval(timer);
            controller.close();
            return;
          }

          if (t.done) {
            send({
              type: "done",
              total: t.total,
              processed: t.processed,
              ...(t.warning ? { warning: t.warning } : {}),
              ...(t.result ? { result: t.result } : {}),
            });
            if (timer !== null) clearInterval(timer);
            controller.close();
            return;
          }
        }, 1000) as unknown as number;
      },
      cancel() {
        if (timer !== null) clearInterval(timer);
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        "X-Accel-Buffering": "no",
      },
    });
  } catch (e) {
    return c.json(legacyErr(`Batch stream failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/v1/admin/batch/:taskId/cancel", requireAdminAuth, async (c) => {
  const taskId = c.req.param("taskId");
  const task = batchTasks.get(taskId);
  if (!task) return c.json(legacyErr("Task not found"), 404);
  task.cancelled = true;
  return c.json(legacyOk({ task_id: taskId, cancelled: true }));
});

adminRoutes.get("/api/v1/admin/imagine/ws", async (c) => {
  try {
    const rawApiKey = String(c.req.query("api_key") ?? "").trim();
    const authed = await verifyAdminQueryApiKey(c.env, rawApiKey);
    if (!authed) return c.text("Unauthorized", 401);

    const upgrade = String(c.req.header("Upgrade") ?? "").toLowerCase();
    if (upgrade !== "websocket") return c.text("Expected websocket", 426);

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    let closed = false;
    let runSeq = 0;
    let sequence = 0;
    let currentRunId = "";

    const send = (payload: Record<string, unknown>): boolean => {
      if (closed) return false;
      try {
        server.send(JSON.stringify(payload));
        return true;
      } catch {
        return false;
      }
    };

    const stopCurrent = (): void => {
      runSeq += 1;
    };

    const startLoop = (prompt: string, aspectRatio: "1:1" | "2:3" | "3:2" | "9:16" | "16:9"): void => {
      runSeq += 1;
      const myRunSeq = runSeq;
      const runId = crypto.randomUUID();
      currentRunId = runId;

      send({
        type: "status",
        status: "running",
        prompt,
        aspect_ratio: aspectRatio,
        run_id: runId,
      });

      void (async () => {
        while (!closed && myRunSeq === runSeq) {
          const settings = await getSettings(c.env);
          const cf = normalizeCfCookie(settings.grok.cf_clearance ?? "");
          const birthDate = ensureBirthDate(String(settings.grok.imagine_birth_date ?? ""));
          const autoAgeVerify = settings.grok.imagine_auto_age_verify !== false;
          const enableNsfw = settings.grok.imagine_enable_nsfw !== false;

          const chosen = await selectBestToken(c.env.DB, "grok-imagine-1.0");
          if (!chosen) {
            send({
              type: "error",
              message: "No available tokens. Please try again later.",
              code: "rate_limit_exceeded",
            });
            await sleep(1500);
            continue;
          }

          const jwt = chosen.token;
          const cookie = buildGrokCookie(jwt, cf);
          let calibratedAge = false;
          if (autoAgeVerify) {
            const verified = await getTokenAgeVerified(c.env.DB, jwt);
            if (!verified) {
              const ageOk = await verifyImagineAge({ cookie, birthDate, timeoutMs: 20_000 });
              if (ageOk) {
                await setTokenAgeVerified(c.env.DB, jwt, true);
                calibratedAge = true;
              }
            } else {
              calibratedAge = true;
            }
          }

          try {
            const startedAt = Date.now();
            const generated = await generateImagineViaWs({
              prompt,
              aspectRatio,
              n: 6,
              cookie,
              enableNsfw,
            });
            const elapsedMs = Date.now() - startedAt;

            if (!calibratedAge) {
              await setTokenAgeVerified(c.env.DB, jwt, true);
            }

            if (!generated.b64List.length) {
              send({
                type: "error",
                message: "Image generation returned empty data.",
                code: "empty_image",
              });
              await sleep(1000);
              continue;
            }

            for (const b64 of generated.b64List) {
              if (closed || myRunSeq !== runSeq) break;
              sequence += 1;
              if (!send({
                type: "image",
                b64_json: b64,
                sequence,
                created_at: nowMs(),
                elapsed_ms: elapsedMs,
                aspect_ratio: aspectRatio,
                run_id: runId,
              })) {
                break;
              }
            }
          } catch (e) {
            const status =
              e instanceof ImagineWsError && typeof e.status === "number"
                ? e.status
                : 500;
            const message = e instanceof Error ? e.message : String(e);
            await recordTokenFailure(c.env.DB, jwt, status, message.slice(0, 200));
            await applyCooldown(c.env.DB, jwt, status);
            send({
              type: "error",
              message,
              code: e instanceof ImagineWsError ? e.code : "internal_error",
            });
            await sleep(1500);
          }
        }

        if (!closed && myRunSeq === runSeq) {
          send({ type: "status", status: "stopped", run_id: currentRunId || runId });
        }
      })();
    };

    server.accept();
    server.addEventListener("message", (evt) => {
      const raw = toMessageText((evt as MessageEvent).data);
      if (!raw) return;

      let payload: any = null;
      try {
        payload = JSON.parse(raw);
      } catch {
        send({ type: "error", message: "Invalid message format.", code: "invalid_payload" });
        return;
      }

      const msgType = String(payload?.type ?? "");
      if (msgType === "ping") {
        send({ type: "pong" });
        return;
      }

      if (msgType === "stop") {
        stopCurrent();
        send({ type: "status", status: "stopped", run_id: currentRunId });
        return;
      }

      if (msgType !== "start") {
        send({ type: "error", message: "Unknown command.", code: "unknown_command" });
        return;
      }

      const prompt = String(payload?.prompt ?? "").trim();
      if (!prompt) {
        send({ type: "error", message: "Prompt cannot be empty.", code: "empty_prompt" });
        return;
      }

      const ratio = normalizeImagineAspectRatio(payload?.aspect_ratio);
      stopCurrent();
      startLoop(prompt, ratio);
    });

    const closeHandler = () => {
      closed = true;
      stopCurrent();
      try {
        server.close(1000, "closed");
      } catch {
        // ignore
      }
    };

    server.addEventListener("close", closeHandler);
    server.addEventListener("error", closeHandler);

    return new Response(null, { status: 101, webSocket: client });
  } catch (e) {
    return c.text(`WebSocket init error: ${e instanceof Error ? e.message : String(e)}`, 500);
  }
});

adminRoutes.get("/api/v1/admin/storage", requireAdminAuth, async (c) => {
  return c.json({ type: "d1" });
});

adminRoutes.get("/api/v1/admin/config", requireAdminAuth, async (c) => {
  try {
    const settings = await getSettings(c.env);
    const filterTags = String(settings.grok.filtered_tags ?? "")
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);
    return c.json({
      app: {
        api_key: settings.grok.api_key ?? "",
        admin_username: settings.global.admin_username ?? "admin",
        app_key: settings.global.admin_password ?? "admin",
        app_url: settings.global.base_url ?? "",
        image_format: settings.global.image_mode ?? "url",
        video_format: "url",
      },
      grok: {
        temporary: Boolean(settings.grok.temporary),
        stream: true,
        thinking: Boolean(settings.grok.show_thinking),
        dynamic_statsig: Boolean(settings.grok.dynamic_statsig),
        filter_tags: filterTags,
        video_poster_preview: Boolean(settings.grok.video_poster_preview),
        timeout: Number(settings.grok.stream_total_timeout ?? 600),
        base_proxy_url: String(settings.grok.proxy_url ?? ""),
        asset_proxy_url: String(settings.grok.cache_proxy_url ?? ""),
        cf_clearance: String(settings.grok.cf_clearance ?? ""),
        imagine_auto_age_verify: settings.grok.imagine_auto_age_verify !== false,
        imagine_enable_nsfw: settings.grok.imagine_enable_nsfw !== false,
        imagine_birth_date: String(settings.grok.imagine_birth_date ?? "2001-01-01T16:00:00.000Z"),
        imagine_max_retries: Number(settings.grok.imagine_max_retries ?? 5),
        imagine_blocked_retry_limit: Number(settings.grok.imagine_blocked_retry_limit ?? 3),
        max_retry: Number(settings.grok.imagine_max_retries ?? 5),
        retry_status_codes: Array.isArray(settings.grok.retry_status_codes) ? settings.grok.retry_status_codes : [401, 429, 403],
      },
      token: {
        auto_refresh: Boolean(settings.token.auto_refresh),
        refresh_interval_hours: Number(settings.token.refresh_interval_hours ?? 8),
        fail_threshold: Number(settings.token.fail_threshold ?? 5),
        save_delay_ms: Number(settings.token.save_delay_ms ?? 500),
        reload_interval_sec: Number(settings.token.reload_interval_sec ?? 30),
      },
      cache: {
        enable_auto_clean: Boolean(settings.cache.enable_auto_clean),
        limit_mb: Number(settings.cache.limit_mb ?? 1024),
        keep_base64_cache: Boolean(settings.cache.keep_base64_cache),
      },
      performance: {
        assets_max_concurrent: Number(settings.performance.assets_max_concurrent ?? 25),
        media_max_concurrent: Number(settings.performance.media_max_concurrent ?? 50),
        usage_max_concurrent: Number(settings.performance.usage_max_concurrent ?? 25),
        assets_delete_batch_size: Number(settings.performance.assets_delete_batch_size ?? 10),
        admin_assets_batch_size: Number(settings.performance.admin_assets_batch_size ?? 10),
      },
    });
  } catch (e) {
    return c.json(legacyErr(`Get config failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/v1/admin/config", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as any;
    const appCfg = (body && typeof body === "object" ? body.app : null) as any;
    const grokCfg = (body && typeof body === "object" ? body.grok : null) as any;
    const tokenCfg = (body && typeof body === "object" ? body.token : null) as any;
    const cacheCfg = (body && typeof body === "object" ? body.cache : null) as any;
    const performanceCfg = (body && typeof body === "object" ? body.performance : null) as any;

    const global_config: any = {};
    const grok_config: any = {};
    const token_config: any = {};
    const cache_config: any = {};
    const performance_config: any = {};

    if (appCfg && typeof appCfg === "object") {
      if (typeof appCfg.api_key === "string") grok_config.api_key = appCfg.api_key.trim();
      if (typeof appCfg.admin_username === "string") global_config.admin_username = appCfg.admin_username.trim() || "admin";
      if (typeof appCfg.app_key === "string") global_config.admin_password = appCfg.app_key.trim() || "admin";
      if (typeof appCfg.app_url === "string") global_config.base_url = appCfg.app_url.trim();
      if (appCfg.image_format === "url" || appCfg.image_format === "base64") global_config.image_mode = appCfg.image_format;
    }

    if (grokCfg && typeof grokCfg === "object") {
      if (typeof grokCfg.base_proxy_url === "string") grok_config.proxy_url = grokCfg.base_proxy_url.trim();
      if (typeof grokCfg.asset_proxy_url === "string") grok_config.cache_proxy_url = grokCfg.asset_proxy_url.trim();
      if (typeof grokCfg.cf_clearance === "string") grok_config.cf_clearance = grokCfg.cf_clearance.trim();
      if (typeof grokCfg.imagine_auto_age_verify === "boolean")
        grok_config.imagine_auto_age_verify = grokCfg.imagine_auto_age_verify;
      if (typeof grokCfg.imagine_enable_nsfw === "boolean")
        grok_config.imagine_enable_nsfw = grokCfg.imagine_enable_nsfw;
      if (typeof grokCfg.imagine_birth_date === "string")
        grok_config.imagine_birth_date = grokCfg.imagine_birth_date.trim();
      if (Number.isFinite(Number(grokCfg.imagine_max_retries)))
        grok_config.imagine_max_retries = Math.max(1, Math.floor(Number(grokCfg.imagine_max_retries)));
      if (Number.isFinite(Number(grokCfg.imagine_blocked_retry_limit)))
        grok_config.imagine_blocked_retry_limit = Math.max(0, Math.floor(Number(grokCfg.imagine_blocked_retry_limit)));
      if (Number.isFinite(Number(grokCfg.max_retry)))
        grok_config.imagine_max_retries = Math.max(1, Math.floor(Number(grokCfg.max_retry)));
      if (typeof grokCfg.filter_tags === "string") {
        grok_config.filtered_tags = grokCfg.filter_tags;
      } else if (Array.isArray(grokCfg.filter_tags)) {
        grok_config.filtered_tags = grokCfg.filter_tags.map((x: any) => String(x ?? "").trim()).filter(Boolean).join(",");
      }
      if (typeof grokCfg.dynamic_statsig === "boolean") grok_config.dynamic_statsig = grokCfg.dynamic_statsig;
      if (typeof grokCfg.thinking === "boolean") grok_config.show_thinking = grokCfg.thinking;
      if (typeof grokCfg.temporary === "boolean") grok_config.temporary = grokCfg.temporary;
      if (typeof grokCfg.video_poster_preview === "boolean") grok_config.video_poster_preview = grokCfg.video_poster_preview;
      if (Array.isArray(grokCfg.retry_status_codes))
        grok_config.retry_status_codes = grokCfg.retry_status_codes.map((x: any) => Number(x)).filter((n: number) => Number.isFinite(n));
      if (Number.isFinite(Number(grokCfg.timeout))) grok_config.stream_total_timeout = Math.max(1, Math.floor(Number(grokCfg.timeout)));
    }

    if (tokenCfg && typeof tokenCfg === "object") {
      if (typeof tokenCfg.auto_refresh === "boolean") token_config.auto_refresh = tokenCfg.auto_refresh;
      if (Number.isFinite(Number(tokenCfg.refresh_interval_hours)))
        token_config.refresh_interval_hours = Math.max(1, Number(tokenCfg.refresh_interval_hours));
      if (Number.isFinite(Number(tokenCfg.fail_threshold)))
        token_config.fail_threshold = Math.max(1, Math.floor(Number(tokenCfg.fail_threshold)));
      if (Number.isFinite(Number(tokenCfg.save_delay_ms)))
        token_config.save_delay_ms = Math.max(0, Math.floor(Number(tokenCfg.save_delay_ms)));
      if (Number.isFinite(Number(tokenCfg.reload_interval_sec)))
        token_config.reload_interval_sec = Math.max(0, Math.floor(Number(tokenCfg.reload_interval_sec)));
    }

    if (cacheCfg && typeof cacheCfg === "object") {
      if (typeof cacheCfg.enable_auto_clean === "boolean") cache_config.enable_auto_clean = cacheCfg.enable_auto_clean;
      if (Number.isFinite(Number(cacheCfg.limit_mb))) cache_config.limit_mb = Math.max(1, Math.floor(Number(cacheCfg.limit_mb)));
      if (typeof cacheCfg.keep_base64_cache === "boolean") cache_config.keep_base64_cache = cacheCfg.keep_base64_cache;
    }

    if (performanceCfg && typeof performanceCfg === "object") {
      const fields = [
        "assets_max_concurrent",
        "media_max_concurrent",
        "usage_max_concurrent",
        "assets_delete_batch_size",
        "admin_assets_batch_size",
      ] as const;
      for (const f of fields) {
        if (Number.isFinite(Number(performanceCfg[f]))) performance_config[f] = Math.max(1, Math.floor(Number(performanceCfg[f])));
      }
    }

    await saveSettings(c.env, { global_config, grok_config, token_config, cache_config, performance_config });
    return c.json(legacyOk({ message: "配置已更新" }));
  } catch (e) {
    return c.json(legacyErr(`Update config failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.get("/api/v1/admin/tokens", requireAdminAuth, async (c) => {
  try {
    const rows = await listTokens(c.env.DB);
    const settings = await getSettings(c.env);
    const now = nowMs();
    const nsfwRequestEnabled = settings.grok.imagine_enable_nsfw !== false;

    const out: Record<"ssoBasic" | "ssoSuper", any[]> = { ssoBasic: [], ssoSuper: [] };
    for (const r of rows) {
      const pool = toPoolName(r.token_type);
      const isCooling = Boolean(r.cooldown_until && r.cooldown_until > now);
      const status = r.status === "expired" ? "invalid" : isCooling ? "cooling" : "active";
      const quotaRaw = r.remaining_queries;
      const quota = quotaRaw >= 0 ? quotaRaw : 0;
      const ageVerified = Number(r.age_verified ?? 0) > 0;
      const ageStatus = ageVerified ? "verified" : "unknown";
      const nsfwStatus = nsfwRequestEnabled ? (ageVerified ? "enabled" : "unknown") : "disabled_by_config";
      out[pool].push({
        token: `sso=${r.token}`,
        status,
        quota,
        note: r.note ?? "",
        fail_count: r.failed_count ?? 0,
        use_count: 0,
        age_verified: ageVerified,
        age_status: ageStatus,
        nsfw_status: nsfwStatus,
      });
    }
    return c.json(out);
  } catch (e) {
    return c.json(legacyErr(`Get tokens failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/v1/admin/tokens", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as Record<string, unknown>;
    if (!body || typeof body !== "object") return c.json(legacyErr("Invalid payload"), 400);

    const rows = await listTokens(c.env.DB);
    const byType: Record<"sso" | "ssoSuper", Set<string>> = { sso: new Set(), ssoSuper: new Set() };
    for (const r of rows) byType[r.token_type].add(r.token);
    const existingAll = new Set<string>(rows.map((r) => r.token));
    const newlyAdded: string[] = [];

    const now = nowMs();
    const desiredByType: Record<"sso" | "ssoSuper", Set<string>> = { sso: new Set(), ssoSuper: new Set() };
    const stmts: D1PreparedStatement[] = [];

    for (const [pool, items] of Object.entries(body)) {
      const tokenType = poolToTokenType(pool);
      if (!tokenType) continue;
      const arr = Array.isArray(items) ? items : [];
      for (const it of arr) {
        const tokenRaw = typeof it === "string" ? it : (it as any)?.token;
        const token = normalizeSsoToken(String(tokenRaw ?? ""));
        if (!token) continue;
        desiredByType[tokenType].add(token);
        if (!existingAll.has(token)) {
          existingAll.add(token);
          newlyAdded.push(token);
        }

        const statusRaw = typeof it === "string" ? "active" : String((it as any)?.status ?? "active");
        const quotaRaw = typeof it === "string" ? 0 : Number((it as any)?.quota ?? 0);
        const quota = Number.isFinite(quotaRaw) && quotaRaw >= 0 ? Math.floor(quotaRaw) : -1;
        const note = typeof it === "string" ? "" : String((it as any)?.note ?? "");

        const status = statusRaw === "invalid" ? "expired" : "active";
        const cooldownUntil = statusRaw === "cooling" ? now + 60 * 60 * 1000 : null;

        const remaining = quota >= 0 ? quota : -1;
        const heavy = tokenType === "ssoSuper" ? remaining : -1;

        stmts.push(
          c.env.DB.prepare(
            "INSERT INTO tokens(token, token_type, created_time, remaining_queries, heavy_remaining_queries, status, failed_count, cooldown_until, last_failure_time, last_failure_reason, tags, note) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(token) DO UPDATE SET token_type=excluded.token_type, remaining_queries=excluded.remaining_queries, heavy_remaining_queries=excluded.heavy_remaining_queries, status=excluded.status, cooldown_until=excluded.cooldown_until, note=excluded.note",
          ).bind(token, tokenType, now, remaining, heavy, status, 0, cooldownUntil, null, null, "[]", note),
        );
      }
    }

    // Delete tokens removed from the posted pools
    for (const tokenType of ["sso", "ssoSuper"] as const) {
      const existing = byType[tokenType];
      const desired = desiredByType[tokenType];
      const toDel: string[] = [];
      for (const t of existing) if (!desired.has(t)) toDel.push(t);
      if (toDel.length) {
        const placeholders = toDel.map(() => "?").join(",");
        stmts.push(c.env.DB.prepare(`DELETE FROM tokens WHERE token_type = ? AND token IN (${placeholders})`).bind(tokenType, ...toDel));
      }
    }

    if (stmts.length) await c.env.DB.batch(stmts);
    return c.json(legacyOk({ message: "Token 已更新" }));
  } catch (e) {
    return c.json(legacyErr(`Update tokens failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/v1/admin/tokens/refresh", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as any;
    const tokens: string[] = [];
    if (body && typeof body === "object") {
      if (typeof body.token === "string") tokens.push(body.token);
      if (Array.isArray(body.tokens)) tokens.push(...body.tokens.filter((x: any) => typeof x === "string"));
    }
    const unique = [...new Set(tokens.map((t) => normalizeSsoToken(t)).filter(Boolean))];
    if (!unique.length) return c.json(legacyErr("No tokens provided"), 400);

    const settings = await getSettings(c.env);
    const cf = normalizeCfCookie(settings.grok.cf_clearance ?? "");
    const syncAge = body?.sync_age !== false;
    const birthDate = ensureBirthDate(String(settings.grok.imagine_birth_date ?? ""));

    const placeholders = unique.map(() => "?").join(",");
    const typeRows = placeholders
      ? await dbAll<{ token: string; token_type: string }>(
          c.env.DB,
          `SELECT token, token_type FROM tokens WHERE token IN (${placeholders})`,
          unique,
        )
      : [];
    const tokenTypeByToken = new Map(typeRows.map((r) => [r.token, r.token_type]));

    const results: Record<string, boolean> = {};
    for (const t of unique) {
      let refreshed = false;
      let usageSynced = false;
      try {
        const cookie = cf ? `sso-rw=${t};sso=${t};${cf}` : `sso-rw=${t};sso=${t}`;
        const tokenType = tokenTypeByToken.get(t) ?? "sso";
        const r = await checkRateLimits(cookie, settings.grok, "grok-4-fast");
        const remaining = (r as any)?.remainingTokens;
        let heavyRemaining: number | null = null;
        if (tokenType === "ssoSuper") {
          const rh = await checkRateLimits(cookie, settings.grok, "grok-4-heavy");
          const hv = (rh as any)?.remainingTokens;
          if (typeof hv === "number") heavyRemaining = hv;
        }
        if (typeof remaining === "number") {
          await updateTokenLimits(c.env.DB, t, {
            remaining_queries: remaining,
            ...(heavyRemaining !== null ? { heavy_remaining_queries: heavyRemaining } : {}),
          });
          await markTokenRecovered(c.env.DB, t);
          usageSynced = true;
          refreshed = true;
        }

        if (syncAge) {
          let ageSynced = false;
          try {
            const ageOk = await verifyImagineAge({ cookie, birthDate, timeoutMs: 15_000 });
            if (ageOk) {
              await setTokenAgeVerified(c.env.DB, t, true);
              ageSynced = true;
              refreshed = true;
            }
          } catch {
            // ignore age sync failures during refresh
          }
          if (!ageSynced && usageSynced) {
            // Workers mode fallback: token is live even if age endpoint sync is unavailable.
            await setTokenAgeVerified(c.env.DB, t, true);
            refreshed = true;
          }
        }
        results[`sso=${t}`] = refreshed;
      } catch {
        results[`sso=${t}`] = refreshed;
      }
      await new Promise((res) => setTimeout(res, 50));
    }

    return c.json(legacyOk({ results }));
  } catch (e) {
    return c.json(legacyErr(`Refresh failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/v1/admin/tokens/refresh/async", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as any;
    const tokens: string[] = [];
    if (body && typeof body === "object") {
      if (typeof body.token === "string") tokens.push(body.token);
      if (Array.isArray(body.tokens)) tokens.push(...body.tokens.filter((x: any) => typeof x === "string"));
    }

    const unique = [...new Set(tokens.map((t) => normalizeSsoToken(t)).filter(Boolean))];
    if (!unique.length) return c.json(legacyErr("No tokens provided"), 400);

    const task = createBatchTask("refresh", unique.length);
    c.executionCtx.waitUntil(
      (async () => {
        try {
          await runRefreshBatchTask(c.env, task, unique);
        } catch (e) {
          task.error = e instanceof Error ? e.message : String(e);
        }
      })(),
    );

    return c.json(legacyOk({ task_id: task.id }));
  } catch (e) {
    return c.json(legacyErr(`Async refresh failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/v1/admin/tokens/nsfw/enable/async", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as any;
    const inputTokens: string[] = Array.isArray(body?.tokens)
      ? (body.tokens as unknown[]).filter((x): x is string => typeof x === "string")
      : [];
    const unique: string[] = [...new Set(inputTokens.map((t) => normalizeSsoToken(t)).filter(Boolean))];
    if (!unique.length) return c.json(legacyErr("No tokens provided"), 400);

    const task = createBatchTask("nsfw", unique.length);
    c.executionCtx.waitUntil(
      (async () => {
        try {
          await runNsfwBatchTask(c.env, task, unique);
        } catch (e) {
          task.error = e instanceof Error ? e.message : String(e);
        }
      })(),
    );

    return c.json(legacyOk({ task_id: task.id }));
  } catch (e) {
    return c.json(legacyErr(`NSFW async failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/v1/admin/tokens/auto-register", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const poolRaw = String(body?.pool ?? "ssoBasic").trim();
    const pool: "ssoBasic" | "ssoSuper" = poolRaw === "ssoSuper" ? "ssoSuper" : "ssoBasic";
    const countRaw = Number(body?.count ?? 100);
    const concurrencyRaw = Number(body?.concurrency ?? 10);
    const total = Number.isFinite(countRaw) && countRaw > 0 ? Math.floor(countRaw) : 100;
    const concurrency = Number.isFinite(concurrencyRaw) && concurrencyRaw > 0 ? Math.floor(concurrencyRaw) : 10;

    for (const job of autoRegisterJobs.values()) {
      if (job.status === "starting" || job.status === "running" || job.status === "stopping") {
        return c.json({ detail: "Auto registration already running" }, 409);
      }
    }

    const job = createAutoRegisterJob({ pool, total, concurrency });
    c.executionCtx.waitUntil(
      (async () => {
        try {
          await runAutoRegisterJob(job);
        } catch (e) {
          job.status = "error";
          job.errors += 1;
          job.last_error = e instanceof Error ? e.message : String(e);
          job.error = job.last_error;
          job.finishedAt = nowMs();
          appendAutoRegisterLog(job, `Auto-register failed: ${job.last_error}`);
        }
      })(),
    );

    return c.json({
      status: "started",
      job: serializeAutoRegisterJob(job),
    });
  } catch (e) {
    return c.json({ detail: e instanceof Error ? e.message : String(e) }, 500);
  }
});

adminRoutes.get("/api/v1/admin/tokens/auto-register/status", requireAdminAuth, async (c) => {
  const jobId = String(c.req.query("job_id") ?? "").trim();
  const job = getAutoRegisterJob(jobId || null);
  if (!job) {
    if (jobId) return c.json({ status: "not_found" }, 404);
    return c.json({ status: "idle", logs: [] });
  }
  return c.json(serializeAutoRegisterJob(job));
});

adminRoutes.post("/api/v1/admin/tokens/auto-register/stop", requireAdminAuth, async (c) => {
  const jobId = String(c.req.query("job_id") ?? "").trim();
  const job = getAutoRegisterJob(jobId || null);
  if (!job) return c.json({ status: "not_found" }, 404);

  if (job.status === "completed" || job.status === "stopped" || job.status === "error") {
    return c.json(serializeAutoRegisterJob(job));
  }

  job.stopRequested = true;
  if (job.status === "starting" || job.status === "running") {
    job.status = "stopping";
    appendAutoRegisterLog(job, "Stop requested.");
  }
  return c.json({ status: "stopping", job_id: job.id });
});

adminRoutes.post("/api/v1/admin/cache/online/load/async", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const rawTokens = Array.isArray(body?.tokens)
      ? (body.tokens as unknown[]).filter((x): x is string => typeof x === "string")
      : [];
    const tokens = [...new Set(rawTokens.map((t) => normalizeSsoToken(t)).filter(Boolean))];
    const task = createBatchTask("cache_load", tokens.length);
    c.executionCtx.waitUntil(
      (async () => {
        try {
          await runCacheLoadBatchTask(c.env, task, tokens);
        } catch (e) {
          task.error = e instanceof Error ? e.message : String(e);
        }
      })(),
    );
    return c.json(legacyOk({ task_id: task.id }));
  } catch (e) {
    return c.json(legacyErr(`Cache load async failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/v1/admin/cache/online/clear/async", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const rawTokens = Array.isArray(body?.tokens)
      ? (body.tokens as unknown[]).filter((x): x is string => typeof x === "string")
      : [];
    const tokens = [...new Set(rawTokens.map((t) => normalizeSsoToken(t)).filter(Boolean))];
    if (!tokens.length) return c.json(legacyErr("No tokens provided"), 400);

    const task = createBatchTask("cache_clear", tokens.length);
    c.executionCtx.waitUntil(
      (async () => {
        try {
          await runCacheClearBatchTask(task, tokens);
        } catch (e) {
          task.error = e instanceof Error ? e.message : String(e);
        }
      })(),
    );
    return c.json(legacyOk({ task_id: task.id }));
  } catch (e) {
    return c.json(legacyErr(`Cache clear async failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.get("/api/v1/admin/cache/local", requireAdminAuth, async (c) => {
  try {
    const stats = await getKvStats(c.env.DB);
    return c.json({ local_image: stats.image, local_video: stats.video });
  } catch (e) {
    return c.json(legacyErr(`Get cache stats failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.get("/api/v1/admin/cache", requireAdminAuth, async (c) => {
  try {
    const stats = await getKvStats(c.env.DB);
    const rows = await listTokens(c.env.DB);
    const accounts = rows.map((row) => ({
      token: row.token,
      token_masked: maskToken(row.token),
      pool: toPoolName(row.token_type),
      status: row.status,
      last_asset_clear_at: null,
    }));
    return c.json({
      local_image: stats.image,
      local_video: stats.video,
      online: { count: 0, status: "not_loaded", token: null, last_asset_clear_at: null },
      online_accounts: accounts,
      online_scope: "all",
      online_details: [],
    });
  } catch (e) {
    return c.json(legacyErr(`Get cache failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/v1/admin/cache/clear", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as any;
    const t = String(body?.type ?? "image").toLowerCase();
    const type: CacheType = t === "video" ? "video" : "image";
    const deleted = await clearKvCacheByType(c.env, type);
    return c.json(legacyOk({ result: { deleted } }));
  } catch (e) {
    return c.json(legacyErr(`Clear cache failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.get("/api/v1/admin/cache/list", requireAdminAuth, async (c) => {
  try {
    const t = String(c.req.query("type") ?? "image").toLowerCase();
    const type: CacheType = t === "video" ? "video" : "image";
    const page = Math.max(1, Number(c.req.query("page") ?? 1));
    const pageSize = Math.max(1, Math.min(5000, Number(c.req.query("page_size") ?? 1000)));
    const offset = (page - 1) * pageSize;

    const { total, items } = await listCacheRowsByType(c.env.DB, type, pageSize, offset);
    const mapped = items.map((it) => {
      const name = it.key.startsWith(`${type}/`) ? it.key.slice(type.length + 1) : it.key;
      return {
        name,
        size_bytes: it.size,
        mtime_ms: it.last_access_at || it.created_at,
        preview_url: type === "image" ? `/images/${encodeURIComponent(name)}` : "",
      };
    });

    return c.json(legacyOk({ total, page, page_size: pageSize, items: mapped }));
  } catch (e) {
    return c.json(legacyErr(`List cache failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/v1/admin/cache/item/delete", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as any;
    const t = String(body?.type ?? "image").toLowerCase();
    const type: CacheType = t === "video" ? "video" : "image";
    const name = String(body?.name ?? "").trim();
    if (!name) return c.json(legacyErr("Missing file name"), 400);
    const key = name.startsWith(`${type}/`) ? name : `${type}/${name}`;
    await c.env.KV_CACHE.delete(key);
    await dbRun(c.env.DB, "DELETE FROM kv_cache WHERE key = ?", [key]);
    return c.json(legacyOk({ result: { deleted: true } }));
  } catch (e) {
    return c.json(legacyErr(`Delete failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/v1/admin/cache/online/clear", requireAdminAuth, async (c) => {
  return c.json(legacyErr("Online assets clear is not supported on Cloudflare Workers"), 501);
});

adminRoutes.get("/api/v1/admin/voice/token", requireAdminAuth, async (c) => {
  return c.json(
    {
      detail:
        "Voice real-time token issuing is not supported on Cloudflare Workers runtime. Please use the Python backend for voice.",
    },
    501,
  );
});

adminRoutes.get("/api/v1/admin/metrics", requireAdminAuth, async (c) => {
  try {
    const now = nowMs();
    const rows = await listTokens(c.env.DB);
    let total = 0;
    let active = 0;
    let cooling = 0;
    let expired = 0;
    let chatQuota = 0;
    for (const t of rows) {
      total += 1;
      if (t.status === "expired") {
        expired += 1;
        continue;
      }
      if (t.cooldown_until && t.cooldown_until > now) {
        cooling += 1;
        continue;
      }
      active += 1;
      if (t.remaining_queries > 0) chatQuota += t.remaining_queries;
    }

    const stats = await getKvStats(c.env.DB);
    const reqStats = await getRequestStats(c.env.DB);
    const totalCallsRow = await dbFirst<{ c: number }>(c.env.DB, "SELECT COUNT(1) as c FROM request_logs");
    const totalCalls = totalCallsRow?.c ?? 0;

    return c.json({
      tokens: {
        total,
        active,
        cooling,
        expired,
        disabled: 0,
        chat_quota: chatQuota,
        image_quota: Math.floor(chatQuota / 2),
        total_calls: totalCalls,
      },
      cache: { local_image: stats.image, local_video: stats.video },
      request_stats: reqStats,
    });
  } catch (e) {
    return c.json(legacyErr(`Get metrics failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.get("/api/v1/admin/logs/files", requireAdminAuth, async (c) => {
  const now = nowMs();
  return c.json({ files: [{ name: "request_logs", size_bytes: 0, mtime_ms: now }] });
});

adminRoutes.get("/api/v1/admin/logs/tail", requireAdminAuth, async (c) => {
  try {
    const file = String(c.req.query("file") ?? "request_logs");
    const limit = Math.max(50, Math.min(5000, Number(c.req.query("lines") ?? 500)));
    const rows = await getRequestLogs(c.env.DB, limit);
    const lines = rows.map((r) => `${r.time} | ${r.status} | ${r.model} | ${r.ip} | ${r.key_name} | ${r.error}`.trim());
    return c.json({ file, lines });
  } catch (e) {
    return c.json(legacyErr(`Tail failed: ${e instanceof Error ? e.message : String(e)}`), 500);
  }
});

adminRoutes.post("/api/login", async (c) => {
  try {
    const body = (await c.req.json()) as { username?: string; password?: string };
    const settings = await getSettings(c.env);

    if (body.username !== settings.global.admin_username || body.password !== settings.global.admin_password) {
      return c.json({ success: false, message: "用户名或密码错误" });
    }

    const token = await createAdminSession(c.env.DB);
    return c.json({ success: true, token, message: "登录成功" });
  } catch (e) {
    return c.json(jsonError(`登录失败: ${e instanceof Error ? e.message : String(e)}`, "LOGIN_ERROR"), 500);
  }
});

adminRoutes.post("/api/logout", requireAdminAuth, async (c) => {
  try {
    const token = parseBearer(c.req.header("Authorization") ?? null);
    if (token) await deleteAdminSession(c.env.DB, token);
    return c.json({ success: true, message: "登出成功" });
  } catch (e) {
    return c.json(jsonError(`登出失败: ${e instanceof Error ? e.message : String(e)}`, "LOGOUT_ERROR"), 500);
  }
});

adminRoutes.get("/api/settings", requireAdminAuth, async (c) => {
  try {
    const settings = await getSettings(c.env);
    return c.json({ success: true, data: settings });
  } catch (e) {
    return c.json(jsonError(`获取失败: ${e instanceof Error ? e.message : String(e)}`, "GET_SETTINGS_ERROR"), 500);
  }
});

adminRoutes.post("/api/settings", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { global_config?: any; grok_config?: any };
    await saveSettings(c.env, { global_config: body.global_config, grok_config: body.grok_config });
    return c.json({ success: true, message: "配置更新成功" });
  } catch (e) {
    return c.json(jsonError(`更新失败: ${e instanceof Error ? e.message : String(e)}`, "UPDATE_SETTINGS_ERROR"), 500);
  }
});

adminRoutes.get("/api/storage/mode", requireAdminAuth, async (c) => {
  return c.json({ success: true, data: { mode: "D1" } });
});

adminRoutes.get("/api/tokens", requireAdminAuth, async (c) => {
  try {
    const rows = await listTokens(c.env.DB);
    const infos = rows.map(tokenRowToInfo);
    return c.json({ success: true, data: infos, total: infos.length });
  } catch (e) {
    return c.json(jsonError(`获取失败: ${e instanceof Error ? e.message : String(e)}`, "TOKENS_LIST_ERROR"), 500);
  }
});

adminRoutes.post("/api/tokens/add", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { tokens?: string[]; token_type?: string };
    const token_type = validateTokenType(String(body.token_type ?? ""));
    const tokens = Array.isArray(body.tokens) ? body.tokens : [];
    const count = await addTokens(c.env.DB, tokens, token_type);
    return c.json({ success: true, message: `添加成功(${count})` });
  } catch (e) {
    return c.json(jsonError(`添加失败: ${e instanceof Error ? e.message : String(e)}`, "TOKENS_ADD_ERROR"), 500);
  }
});

adminRoutes.post("/api/tokens/delete", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { tokens?: string[]; token_type?: string };
    const token_type = validateTokenType(String(body.token_type ?? ""));
    const tokens = Array.isArray(body.tokens) ? body.tokens : [];
    const deleted = await deleteTokens(c.env.DB, tokens, token_type);
    return c.json({ success: true, message: `删除成功(${deleted})` });
  } catch (e) {
    return c.json(jsonError(`删除失败: ${e instanceof Error ? e.message : String(e)}`, "TOKENS_DELETE_ERROR"), 500);
  }
});

adminRoutes.post("/api/tokens/tags", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { token?: string; token_type?: string; tags?: string[] };
    const token_type = validateTokenType(String(body.token_type ?? ""));
    const token = String(body.token ?? "");
    const tags = Array.isArray(body.tags) ? body.tags : [];
    await updateTokenTags(c.env.DB, token, token_type, tags);
    return c.json({ success: true, message: "标签更新成功", tags });
  } catch (e) {
    return c.json(jsonError(`更新失败: ${e instanceof Error ? e.message : String(e)}`, "UPDATE_TAGS_ERROR"), 500);
  }
});

adminRoutes.post("/api/tokens/note", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { token?: string; token_type?: string; note?: string };
    const token_type = validateTokenType(String(body.token_type ?? ""));
    const token = String(body.token ?? "");
    const note = String(body.note ?? "");
    await updateTokenNote(c.env.DB, token, token_type, note);
    return c.json({ success: true, message: "备注更新成功", note });
  } catch (e) {
    return c.json(jsonError(`更新失败: ${e instanceof Error ? e.message : String(e)}`, "UPDATE_NOTE_ERROR"), 500);
  }
});

adminRoutes.get("/api/tokens/tags/all", requireAdminAuth, async (c) => {
  try {
    const tags = await getAllTags(c.env.DB);
    return c.json({ success: true, data: tags });
  } catch (e) {
    return c.json(jsonError(`获取失败: ${e instanceof Error ? e.message : String(e)}`, "GET_TAGS_ERROR"), 500);
  }
});

adminRoutes.post("/api/tokens/test", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { token?: string; token_type?: string };
    const token_type = validateTokenType(String(body.token_type ?? ""));
    const token = String(body.token ?? "");
    const settings = await getSettings(c.env);

    const cf = normalizeCfCookie(settings.grok.cf_clearance ?? "");
    const cookie = cf ? `sso-rw=${token};sso=${token};${cf}` : `sso-rw=${token};sso=${token}`;

    const result = await checkRateLimits(cookie, settings.grok, "grok-4-fast");
    if (result) {
      const remaining = (result as any).remainingTokens ?? -1;
      const limit = (result as any).limit ?? -1;

      let heavyRemaining: number | null = null;
      if (token_type === "ssoSuper") {
        const heavy = await checkRateLimits(cookie, settings.grok, "grok-4-heavy");
        const v = (heavy as any)?.remainingTokens;
        if (typeof v === "number") heavyRemaining = v;
      }
      await updateTokenLimits(c.env.DB, token, {
        remaining_queries: typeof remaining === "number" ? remaining : -1,
        ...(heavyRemaining !== null ? { heavy_remaining_queries: heavyRemaining } : {}),
      });
      return c.json({
        success: true,
        message: "Token有效",
        data: {
          valid: true,
          remaining_queries: typeof remaining === "number" ? remaining : -1,
          heavy_remaining_queries: heavyRemaining !== null ? heavyRemaining : -1,
          limit,
        },
      });
    }

    // Fallback：根据本地状态判断原因
    const rows = await listTokens(c.env.DB);
    const row = rows.find((r) => r.token === token && r.token_type === token_type);
    if (!row) {
      return c.json({ success: false, message: "Token数据异常", data: { valid: false, error_type: "unknown" } });
    }
    const now = Date.now();
    if (row.status === "expired") {
      return c.json({ success: false, message: "Token已失效", data: { valid: false, error_type: "expired", error_code: 401 } });
    }
    if (row.cooldown_until && row.cooldown_until > now) {
      const remaining = Math.floor((row.cooldown_until - now + 999) / 1000);
      return c.json({
        success: false,
        message: "Token处于冷却中",
        data: { valid: false, error_type: "cooldown", error_code: 429, cooldown_remaining: remaining },
      });
    }
    const exhausted =
      token_type === "ssoSuper"
        ? row.remaining_queries === 0 || row.heavy_remaining_queries === 0
        : row.remaining_queries === 0;
    if (exhausted) {
      return c.json({
        success: false,
        message: "Token额度耗尽",
        data: { valid: false, error_type: "exhausted", error_code: "quota_exhausted" },
      });
    }
    return c.json({
      success: false,
      message: "服务器被 block 或网络错误",
      data: { valid: false, error_type: "blocked", error_code: 403 },
    });
  } catch (e) {
    return c.json(jsonError(`测试失败: ${e instanceof Error ? e.message : String(e)}`, "TEST_TOKEN_ERROR"), 500);
  }
});

adminRoutes.post("/api/tokens/refresh-all", requireAdminAuth, async (c) => {
  try {
    const progress = await getRefreshProgress(c.env.DB);
    if (progress.running) {
      return c.json({ success: false, message: "刷新任务正在进行中", data: progress });
    }

    const tokens = await listTokens(c.env.DB);
    await setRefreshProgress(c.env.DB, {
      running: true,
      current: 0,
      total: tokens.length,
      success: 0,
      failed: 0,
    });

    const settings = await getSettings(c.env);
    const cf = normalizeCfCookie(settings.grok.cf_clearance ?? "");

    c.executionCtx.waitUntil(
      (async () => {
        let success = 0;
        let failed = 0;
        for (let i = 0; i < tokens.length; i++) {
          const t = tokens[i]!;
          const cookie = cf ? `sso-rw=${t.token};sso=${t.token};${cf}` : `sso-rw=${t.token};sso=${t.token}`;
          const r = await checkRateLimits(cookie, settings.grok, "grok-4-fast");
          if (r) {
            const remaining = (r as any).remainingTokens;
            let heavyRemaining: number | null = null;
            if (t.token_type === "ssoSuper") {
              const rh = await checkRateLimits(cookie, settings.grok, "grok-4-heavy");
              const hv = (rh as any)?.remainingTokens;
              if (typeof hv === "number") heavyRemaining = hv;
            }
            if (typeof remaining === "number") {
              await updateTokenLimits(c.env.DB, t.token, {
                remaining_queries: remaining,
                ...(heavyRemaining !== null ? { heavy_remaining_queries: heavyRemaining } : {}),
              });
              await markTokenRecovered(c.env.DB, t.token);
            }
            success += 1;
          } else {
            failed += 1;
          }
          await setRefreshProgress(c.env.DB, { running: true, current: i + 1, total: tokens.length, success, failed });
          await new Promise((res) => setTimeout(res, 100));
        }
        await setRefreshProgress(c.env.DB, { running: false, current: tokens.length, total: tokens.length, success, failed });
      })(),
    );

    return c.json({ success: true, message: "刷新任务已启动", data: { started: true } });
  } catch (e) {
    return c.json(jsonError(`刷新失败: ${e instanceof Error ? e.message : String(e)}`, "REFRESH_ALL_ERROR"), 500);
  }
});

adminRoutes.get("/api/tokens/refresh-progress", requireAdminAuth, async (c) => {
  try {
    const progress = await getRefreshProgress(c.env.DB);
    return c.json({ success: true, data: progress });
  } catch (e) {
    return c.json(jsonError(`获取失败: ${e instanceof Error ? e.message : String(e)}`, "GET_PROGRESS_ERROR"), 500);
  }
});

adminRoutes.get("/api/stats", requireAdminAuth, async (c) => {
  try {
    const rows = await listTokens(c.env.DB);
    const now = Date.now();

    const calc = (type: "sso" | "ssoSuper") => {
      const tokens = rows.filter((r) => r.token_type === type);
      const total = tokens.length;
      const expired = tokens.filter((t) => t.status === "expired").length;
      let cooldown = 0;
      let exhausted = 0;
      let unused = 0;
      let active = 0;

      for (const t of tokens) {
        if (t.status === "expired") continue;
        if (t.cooldown_until && t.cooldown_until > now) {
          cooldown += 1;
          continue;
        }

        const isUnused = type === "ssoSuper" ? t.remaining_queries === -1 && t.heavy_remaining_queries === -1 : t.remaining_queries === -1;
        if (isUnused) {
          unused += 1;
          continue;
        }

        const isExhausted = type === "ssoSuper" ? t.remaining_queries === 0 || t.heavy_remaining_queries === 0 : t.remaining_queries === 0;
        if (isExhausted) {
          exhausted += 1;
          continue;
        }
        active += 1;
      }

      return { total, expired, active, cooldown, exhausted, unused };
    };

    const normal = calc("sso");
    const superStats = calc("ssoSuper");
    return c.json({ success: true, data: { normal, super: superStats, total: normal.total + superStats.total } });
  } catch (e) {
    return c.json(jsonError(`获取失败: ${e instanceof Error ? e.message : String(e)}`, "STATS_ERROR"), 500);
  }
});

adminRoutes.get("/api/request-stats", requireAdminAuth, async (c) => {
  try {
    const stats = await getRequestStats(c.env.DB);
    return c.json({ success: true, data: stats });
  } catch (e) {
    return c.json(jsonError(`获取失败: ${e instanceof Error ? e.message : String(e)}`, "REQUEST_STATS_ERROR"), 500);
  }
});

// === API Keys (admin UI) ===
function randomKeyName(): string {
  return `key-${crypto.randomUUID().slice(0, 8)}`;
}

adminRoutes.get("/api/v1/admin/keys", requireAdminAuth, async (c) => {
  try {
    const keys = await listApiKeys(c.env.DB);
    const tz = Math.max(-720, Math.min(840, Number(c.env.CACHE_RESET_TZ_OFFSET_MINUTES ?? 480)));
    const day = localDayString(nowMs(), tz);
    const usageRows = await listUsageForDay(c.env.DB, day);
    const usageMap = new Map(usageRows.map((r) => [r.key, r]));

    const data = keys.map((k) => {
      const used = usageMap.get(k.key) ?? { chat_used: 0, heavy_used: 0, image_used: 0, video_used: 0 };
      const remaining = {
        chat: k.chat_limit < 0 ? null : Math.max(0, k.chat_limit - Number((used as any).chat_used ?? 0)),
        heavy: k.heavy_limit < 0 ? null : Math.max(0, k.heavy_limit - Number((used as any).heavy_used ?? 0)),
        image: k.image_limit < 0 ? null : Math.max(0, k.image_limit - Number((used as any).image_used ?? 0)),
        video: k.video_limit < 0 ? null : Math.max(0, k.video_limit - Number((used as any).video_used ?? 0)),
      };
      return {
        ...k,
        is_active: Boolean(k.is_active),
        display_key: displayKey(k.key),
        usage_today: {
          chat_used: Number((used as any).chat_used ?? 0),
          heavy_used: Number((used as any).heavy_used ?? 0),
          image_used: Number((used as any).image_used ?? 0),
          video_used: Number((used as any).video_used ?? 0),
        },
        remaining_today: remaining,
      };
    });

    return c.json({ success: true, data });
  } catch (e) {
    return c.json(jsonError(`鑾峰彇澶辫触: ${e instanceof Error ? e.message : String(e)}`, "ADMIN_KEYS_LIST_ERROR"), 500);
  }
});

adminRoutes.post("/api/v1/admin/keys", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as any;
    const name = String(body?.name ?? "").trim() || randomKeyName();
    const key = String(body?.key ?? "").trim();
    const limits = body?.limits && typeof body.limits === "object" ? body.limits : {};

    const row = await addApiKey(c.env.DB, name, {
      ...(key ? { key } : {}),
      limits: {
        chat_limit: limits.chat_per_day ?? limits.chat_limit,
        heavy_limit: limits.heavy_per_day ?? limits.heavy_limit,
        image_limit: limits.image_per_day ?? limits.image_limit,
        video_limit: limits.video_per_day ?? limits.video_limit,
      },
    });

    const isActive = body?.is_active !== undefined ? Boolean(body.is_active) : true;
    if (!isActive) await updateApiKeyStatus(c.env.DB, row.key, false);

    return c.json({ success: true, data: { ...row, is_active: isActive, display_key: displayKey(row.key) } });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (/UNIQUE|constraint/i.test(msg)) return c.json(jsonError("Key 已存在", "KEY_EXISTS"), 400);
    return c.json(jsonError(`创建失败: ${msg}`, "ADMIN_KEYS_CREATE_ERROR"), 500);
  }
});

adminRoutes.post("/api/v1/admin/keys/update", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as any;
    const key = String(body?.key ?? "").trim();
    if (!key) return c.json(jsonError("Missing key", "MISSING_KEY"), 400);

    if (body?.name !== undefined) {
      const name = String(body.name ?? "").trim();
      if (name) await updateApiKeyName(c.env.DB, key, name);
    }

    if (body?.is_active !== undefined) {
      await updateApiKeyStatus(c.env.DB, key, Boolean(body.is_active));
    }

    if (body?.limits && typeof body.limits === "object") {
      const limits = body.limits;
      await updateApiKeyLimits(c.env.DB, key, {
        ...(limits.chat_per_day !== undefined || limits.chat_limit !== undefined
          ? { chat_limit: limits.chat_per_day ?? limits.chat_limit }
          : {}),
        ...(limits.heavy_per_day !== undefined || limits.heavy_limit !== undefined
          ? { heavy_limit: limits.heavy_per_day ?? limits.heavy_limit }
          : {}),
        ...(limits.image_per_day !== undefined || limits.image_limit !== undefined
          ? { image_limit: limits.image_per_day ?? limits.image_limit }
          : {}),
        ...(limits.video_per_day !== undefined || limits.video_limit !== undefined
          ? { video_limit: limits.video_per_day ?? limits.video_limit }
          : {}),
      });
    }

    return c.json({ success: true });
  } catch (e) {
    return c.json(jsonError(`更新失败: ${e instanceof Error ? e.message : String(e)}`, "ADMIN_KEYS_UPDATE_ERROR"), 500);
  }
});

adminRoutes.post("/api/v1/admin/keys/delete", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as any;
    const key = String(body?.key ?? "").trim();
    if (!key) return c.json(jsonError("Missing key", "MISSING_KEY"), 400);
    const ok = await deleteApiKey(c.env.DB, key);
    return c.json(ok ? { success: true } : jsonError("Key not found", "NOT_FOUND"), ok ? 200 : 404);
  } catch (e) {
    return c.json(jsonError(`删除失败: ${e instanceof Error ? e.message : String(e)}`, "ADMIN_KEYS_DELETE_ERROR"), 500);
  }
});

// === API Keys ===
adminRoutes.get("/api/keys", requireAdminAuth, async (c) => {
  try {
    const keys = await listApiKeys(c.env.DB);
    const settings = await getSettings(c.env);
    const globalKeySet = Boolean((settings.grok.api_key ?? "").trim());
    const data = keys.map((k) => ({ ...k, display_key: displayKey(k.key) }));
    return c.json({ success: true, data, global_key_set: globalKeySet });
  } catch (e) {
    return c.json(jsonError(`获取失败: ${e instanceof Error ? e.message : String(e)}`, "KEYS_LIST_ERROR"), 500);
  }
});

adminRoutes.post("/api/keys/add", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { name?: string };
    const name = String(body.name ?? "").trim();
    if (!name) return c.json({ success: false, message: "name不能为空" });
    const row = await addApiKey(c.env.DB, name);
    return c.json({ success: true, data: row, message: "Key创建成功" });
  } catch (e) {
    return c.json(jsonError(`添加失败: ${e instanceof Error ? e.message : String(e)}`, "KEY_ADD_ERROR"), 500);
  }
});

adminRoutes.post("/api/keys/batch-add", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { name_prefix?: string; count?: number };
    const prefix = String(body.name_prefix ?? "").trim();
    const count = Math.max(1, Math.min(100, Number(body.count ?? 1)));
    if (!prefix) return c.json({ success: false, message: "name_prefix不能为空" });
    const rows = await batchAddApiKeys(c.env.DB, prefix, count);
    return c.json({ success: true, data: rows, message: `成功创建 ${rows.length} 个Key` });
  } catch (e) {
    return c.json(jsonError(`批量添加失败: ${e instanceof Error ? e.message : String(e)}`, "KEY_BATCH_ADD_ERROR"), 500);
  }
});

adminRoutes.post("/api/keys/delete", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { key?: string };
    const key = String(body.key ?? "");
    if (!key) return c.json({ success: false, message: "Key不能为空" });
    const ok = await deleteApiKey(c.env.DB, key);
    return c.json(ok ? { success: true, message: "Key删除成功" } : { success: false, message: "Key不存在" });
  } catch (e) {
    return c.json(jsonError(`删除失败: ${e instanceof Error ? e.message : String(e)}`, "KEY_DELETE_ERROR"), 500);
  }
});

adminRoutes.post("/api/keys/batch-delete", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { keys?: string[] };
    const keys = Array.isArray(body.keys) ? body.keys : [];
    const deleted = await batchDeleteApiKeys(c.env.DB, keys);
    return c.json({ success: true, message: `成功删除 ${deleted} 个Key` });
  } catch (e) {
    return c.json(jsonError(`批量删除失败: ${e instanceof Error ? e.message : String(e)}`, "KEY_BATCH_DELETE_ERROR"), 500);
  }
});

adminRoutes.post("/api/keys/status", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { key?: string; is_active?: boolean };
    const key = String(body.key ?? "");
    const ok = await updateApiKeyStatus(c.env.DB, key, Boolean(body.is_active));
    return c.json(ok ? { success: true, message: "状态更新成功" } : { success: false, message: "Key不存在" });
  } catch (e) {
    return c.json(jsonError(`更新失败: ${e instanceof Error ? e.message : String(e)}`, "KEY_STATUS_ERROR"), 500);
  }
});

adminRoutes.post("/api/keys/batch-status", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { keys?: string[]; is_active?: boolean };
    const keys = Array.isArray(body.keys) ? body.keys : [];
    const updated = await batchUpdateApiKeyStatus(c.env.DB, keys, Boolean(body.is_active));
    return c.json({ success: true, message: `成功更新 ${updated} 个Key 状态` });
  } catch (e) {
    return c.json(jsonError(`批量更新失败: ${e instanceof Error ? e.message : String(e)}`, "KEY_BATCH_STATUS_ERROR"), 500);
  }
});

adminRoutes.post("/api/keys/name", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { key?: string; name?: string };
    const ok = await updateApiKeyName(c.env.DB, String(body.key ?? ""), String(body.name ?? ""));
    return c.json(ok ? { success: true, message: "备注更新成功" } : { success: false, message: "Key不存在" });
  } catch (e) {
    return c.json(jsonError(`更新失败: ${e instanceof Error ? e.message : String(e)}`, "KEY_NAME_ERROR"), 500);
  }
});

// === Logs ===
adminRoutes.get("/api/logs", requireAdminAuth, async (c) => {
  try {
    const limitStr = c.req.query("limit");
    const limit = Math.max(1, Math.min(5000, Number(limitStr ?? 1000)));
    const logs = await getRequestLogs(c.env.DB, limit);
    return c.json({ success: true, data: logs });
  } catch (e) {
    return c.json(jsonError(`获取失败: ${e instanceof Error ? e.message : String(e)}`, "GET_LOGS_ERROR"), 500);
  }
});

adminRoutes.post("/api/logs/clear", requireAdminAuth, async (c) => {
  try {
    await clearRequestLogs(c.env.DB);
    return c.json({ success: true, message: "日志已清空" });
  } catch (e) {
    return c.json(jsonError(`清空失败: ${e instanceof Error ? e.message : String(e)}`, "CLEAR_LOGS_ERROR"), 500);
  }
});

// Cache endpoints (Workers Cache API 无法枚举/统计；这里提供兼容返回，保持后台可用)
adminRoutes.get("/api/cache/size", requireAdminAuth, async (c) => {
  try {
    const bytes = await getCacheSizeBytes(c.env.DB);
    return c.json({
      success: true,
      data: {
        image_size: formatBytes(bytes.image),
        video_size: formatBytes(bytes.video),
        total_size: formatBytes(bytes.total),
        image_size_bytes: bytes.image,
        video_size_bytes: bytes.video,
        total_size_bytes: bytes.total,
      },
    });
  } catch (e) {
    return c.json(jsonError(`获取失败: ${e instanceof Error ? e.message : String(e)}`, "CACHE_SIZE_ERROR"), 500);
  }
});

adminRoutes.get("/api/cache/list", requireAdminAuth, async (c) => {
  try {
    const t = (c.req.query("type") ?? "image").toLowerCase();
    const type: CacheType = t === "video" ? "video" : "image";
    const limit = Math.max(1, Math.min(200, Number(c.req.query("limit") ?? 50)));
    const offset = Math.max(0, Number(c.req.query("offset") ?? 0));

    const { total, items } = await listCacheRowsByType(c.env.DB, type, limit, offset);
    const mapped = items.map((it) => {
      const name = it.key.startsWith(`${type}/`) ? it.key.slice(type.length + 1) : it.key;
      return {
        name,
        size: formatBytes(it.size),
        mtime: it.last_access_at || it.created_at,
        url: `/images/${name}`,
      };
    });

    return c.json({
      success: true,
      data: { total, items: mapped, offset, limit, has_more: offset + mapped.length < total },
    });
  } catch (e) {
    return c.json(jsonError(`获取失败: ${e instanceof Error ? e.message : String(e)}`, "CACHE_LIST_ERROR"), 500);
  }
});

adminRoutes.post("/api/cache/clear", requireAdminAuth, async (c) => {
  try {
    const deletedImages = await clearKvCacheByType(c.env, "image");
    const deletedVideos = await clearKvCacheByType(c.env, "video");
    return c.json({
      success: true,
      message: `缓存清理完成，已删除 ${deletedImages + deletedVideos} 个文件`,
      data: { deleted_count: deletedImages + deletedVideos },
    });
  } catch (e) {
    return c.json(jsonError(`清理失败: ${e instanceof Error ? e.message : String(e)}`, "CACHE_CLEAR_ERROR"), 500);
  }
});
adminRoutes.post("/api/cache/clear/images", requireAdminAuth, async (c) => {
  try {
    const deleted = await clearKvCacheByType(c.env, "image");
    return c.json({ success: true, message: `图片缓存清理完成，已删除 ${deleted} 个文件`, data: { deleted_count: deleted, type: "images" } });
  } catch (e) {
    return c.json(jsonError(`清理失败: ${e instanceof Error ? e.message : String(e)}`, "IMAGE_CACHE_CLEAR_ERROR"), 500);
  }
});
adminRoutes.post("/api/cache/clear/videos", requireAdminAuth, async (c) => {
  try {
    const deleted = await clearKvCacheByType(c.env, "video");
    return c.json({ success: true, message: `视频缓存清理完成，已删除 ${deleted} 个文件`, data: { deleted_count: deleted, type: "videos" } });
  } catch (e) {
    return c.json(jsonError(`清理失败: ${e instanceof Error ? e.message : String(e)}`, "VIDEO_CACHE_CLEAR_ERROR"), 500);
  }
});

// A lightweight endpoint to create an audit log from the panel if needed (optional)
adminRoutes.post("/api/logs/add", requireAdminAuth, async (c) => {
  try {
    const body = (await c.req.json()) as { model?: string; status?: number; error?: string };
    await addRequestLog(c.env.DB, {
      ip: "admin",
      model: String(body.model ?? "admin"),
      duration: 0,
      status: Number(body.status ?? 200),
      key_name: "admin",
      token_suffix: "",
      error: String(body.error ?? ""),
    });
    return c.json({ success: true });
  } catch (e) {
    return c.json(jsonError(`写入失败: ${e instanceof Error ? e.message : String(e)}`, "LOG_ADD_ERROR"), 500);
  }
});
