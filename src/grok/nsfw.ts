const NSFW_ENDPOINT = "https://grok.com/auth_mgmt.AuthManagement/UpdateUserFeatureControls";
const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36";

export interface EnableNsfwArgs {
  cookie: string;
  timeoutMs?: number;
}

export interface EnableNsfwResult {
  success: boolean;
  httpStatus: number;
  grpcStatus: number | null;
  grpcMessage?: string;
  error?: string;
}

function buildNsfwPayload(): Uint8Array {
  const enc = new TextEncoder();
  const name = enc.encode("always_show_nsfw_content");

  const inner = new Uint8Array(2 + name.length);
  inner[0] = 0x0a;
  inner[1] = name.length;
  inner.set(name, 2);

  const protobuf = new Uint8Array(6 + inner.length);
  protobuf.set([0x0a, 0x02, 0x10, 0x01, 0x12, inner.length], 0);
  protobuf.set(inner, 6);

  const framed = new Uint8Array(5 + protobuf.length);
  framed[0] = 0x00;
  const view = new DataView(framed.buffer);
  view.setUint32(1, protobuf.length, false);
  framed.set(protobuf, 5);
  return framed;
}

function decodeBase64ToBytes(input: string): Uint8Array {
  const raw = atob(input);
  const out = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
  return out;
}

function maybeDecodeGrpcWebText(body: Uint8Array, contentType: string): Uint8Array {
  const ct = contentType.toLowerCase();
  const text = new TextDecoder().decode(body);

  if (ct.includes("grpc-web-text")) {
    try {
      return decodeBase64ToBytes(text.replace(/\s+/g, ""));
    } catch {
      return body;
    }
  }

  const head = text.slice(0, Math.min(2048, text.length));
  if (head && /^[A-Za-z0-9+/=\r\n]+$/.test(head)) {
    try {
      return decodeBase64ToBytes(text.replace(/\s+/g, ""));
    } catch {
      return body;
    }
  }
  return body;
}

function decodeGrpcMessage(v: string): string {
  const raw = v.trim();
  if (!raw) return "";
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

function parseTrailerBlock(payload: Uint8Array): Record<string, string> {
  const text = new TextDecoder().decode(payload);
  const out: Record<string, string> = {};
  for (const line of text.split(/\r\n|\n/)) {
    if (!line) continue;
    const idx = line.indexOf(":");
    if (idx <= 0) continue;
    const key = line.slice(0, idx).trim().toLowerCase();
    const value = line.slice(idx + 1).trim();
    if (!key) continue;
    out[key] = key === "grpc-message" ? decodeGrpcMessage(value) : value;
  }
  return out;
}

function parseGrpcWebTrailers(
  body: Uint8Array,
  contentType: string,
  headers: Headers,
): Record<string, string> {
  const decoded = maybeDecodeGrpcWebText(body, contentType);
  const trailers: Record<string, string> = {};
  const view = new DataView(decoded.buffer, decoded.byteOffset, decoded.byteLength);

  let offset = 0;
  while (offset + 5 <= decoded.length) {
    const flag = decoded[offset]!;
    const length = view.getUint32(offset + 1, false);
    offset += 5;
    if (offset + length > decoded.length) break;
    const payload = decoded.subarray(offset, offset + length);
    offset += length;

    if (flag & 0x80) Object.assign(trailers, parseTrailerBlock(payload));
  }

  const statusHeader = headers.get("grpc-status");
  if (statusHeader && !("grpc-status" in trailers)) trailers["grpc-status"] = statusHeader.trim();
  const messageHeader = headers.get("grpc-message");
  if (messageHeader && !("grpc-message" in trailers)) {
    trailers["grpc-message"] = decodeGrpcMessage(messageHeader);
  }
  return trailers;
}

function parseGrpcStatus(raw: string | undefined): number | null {
  const v = String(raw ?? "").trim();
  if (!v) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

export async function enableNsfwFeature(args: EnableNsfwArgs): Promise<EnableNsfwResult> {
  const timeoutMs = Math.max(5000, Math.floor(args.timeoutMs ?? 20_000));
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort("timeout"), timeoutMs);
  const payload = Uint8Array.from(buildNsfwPayload());

  try {
    const response = await fetch(NSFW_ENDPOINT, {
      method: "POST",
      headers: {
        Accept: "*/*",
        Origin: "https://grok.com",
        Referer: "https://grok.com/",
        Cookie: args.cookie,
        "Content-Type": "application/grpc-web+proto",
        "User-Agent": DEFAULT_USER_AGENT,
        "x-grpc-web": "1",
        "x-user-agent": "connect-es/2.1.1",
      },
      body: payload.buffer,
      signal: controller.signal,
    });

    if (response.status !== 200) {
      return {
        success: false,
        httpStatus: response.status,
        grpcStatus: null,
        error: `HTTP ${response.status}`,
      };
    }

    const body = new Uint8Array(await response.arrayBuffer().catch(() => new ArrayBuffer(0)));
    const contentType = response.headers.get("content-type") ?? "";
    const trailers = parseGrpcWebTrailers(body, contentType, response.headers);
    const grpcStatus = parseGrpcStatus(trailers["grpc-status"]);
    const grpcMessage = trailers["grpc-message"] ?? "";
    const success = grpcStatus === null || grpcStatus === 0;

    if (!success) {
      const error = grpcMessage
        ? `gRPC ${grpcStatus}: ${grpcMessage}`
        : `gRPC ${grpcStatus ?? "unknown"}`;
      return {
        success: false,
        httpStatus: response.status,
        grpcStatus,
        ...(grpcMessage ? { grpcMessage } : {}),
        error,
      };
    }

    return {
      success: true,
      httpStatus: response.status,
      grpcStatus,
      ...(grpcMessage ? { grpcMessage } : {}),
    };
  } catch (e) {
    const error = e instanceof Error ? e.message : String(e);
    return {
      success: false,
      httpStatus: 0,
      grpcStatus: null,
      error: error.slice(0, 200),
    };
  } finally {
    clearTimeout(timer);
  }
}
