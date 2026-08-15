import { Router, type NextFunction, type Request, type Response as ExpressResponse } from "express";
import { requireInfoEmailApiSecret, sendAgentInfoEmail, verifyInfoEmailSmtp } from "../lib/infoEmail";
import { logger } from "../lib/logger";

const router = Router();

type FetchLike = typeof fetch;

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderApprovalPage(ok: boolean, message: string): string {
  const title = ok ? "Info email sent" : "Info email not sent";
  return `<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font-family:Arial,sans-serif;margin:32px;line-height:1.45;color:#17202a}main{max-width:680px}</style></head><body><main><h2>${escapeHtml(title)}</h2><p>${escapeHtml(message)}</p></main></body></html>`;
}

export function buildAppsScriptApprovalUrl(target: string, approvalId: string): URL {
  if (!/^[0-9a-f-]{36}$/i.test(approvalId)) {
    throw Object.assign(new Error("Invalid approval link"), { statusCode: 400 });
  }

  let url: URL;
  try {
    url = new URL(target);
  } catch {
    throw Object.assign(new Error("Invalid approval target"), { statusCode: 400 });
  }

  if (
    url.protocol !== "https:" ||
    url.hostname !== "script.google.com" ||
    url.username ||
    url.password ||
    url.port ||
    !/^\/macros\/s\/[A-Za-z0-9_-]+\/exec$/.test(url.pathname)
  ) {
    throw Object.assign(new Error("Invalid approval target"), { statusCode: 400 });
  }

  url.search = "";
  url.hash = "";
  url.searchParams.set("action", "approve_info_email");
  url.searchParams.set("id", approvalId);
  return url;
}

export async function relayInfoEmailApproval(
  target: string,
  approvalId: string,
  fetchImpl: FetchLike = fetch,
): Promise<void> {
  const approvalUrl = buildAppsScriptApprovalUrl(target, approvalId);
  const upstream = await fetchImpl(approvalUrl, {
    method: "GET",
    redirect: "follow",
    headers: { "user-agent": "Crisp-Info-Email-Approval/1.0" },
  });
  const body = await upstream.text();

  if (!upstream.ok || !body.includes("Info email sent") || body.includes("Info email not sent")) {
    throw Object.assign(new Error("The approval could not be completed"), { statusCode: 502 });
  }
}

function authenticateInfoEmailRequest(req: Request): void {
  const expected = requireInfoEmailApiSecret();
  const actual = req.header("x-crisp-info-email-secret")?.trim();
  if (!actual || actual !== expected) {
    throw Object.assign(new Error("Unauthorized"), { statusCode: 401 });
  }
}

router.post("/send-approved", async (req: Request, res: ExpressResponse, next: NextFunction) => {
  try {
    authenticateInfoEmailRequest(req);
    const result = await sendAgentInfoEmail({
      to: String(req.body?.to || ""),
      subject: String(req.body?.subject || ""),
      body: String(req.body?.body || ""),
    });

    logger.info("Approved info email sent", {
      to: req.body?.to,
      subject: req.body?.subject,
      messageId: result.messageId,
      accepted: result.accepted,
      rejected: result.rejected,
    });

    res.status(200).json({
      ok: true,
      messageId: result.messageId,
      accepted: result.accepted,
      rejected: result.rejected,
    });
  } catch (error) {
    next(error);
  }
});

router.post("/verify-smtp", async (req: Request, res: ExpressResponse, next: NextFunction) => {
  try {
    authenticateInfoEmailRequest(req);
    await verifyInfoEmailSmtp();
    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

router.get("/approve", async (req: Request, res: ExpressResponse) => {
  const target = String(req.query.target || "").trim();
  const approvalId = String(req.query.id || "").trim();

  try {
    await relayInfoEmailApproval(target, approvalId);
    logger.info("Info email approval completed through mobile gateway", { approvalId });
    res.setHeader("cache-control", "no-store");
    res.status(200).type("html").send(renderApprovalPage(true, "The email was sent successfully."));
  } catch (error) {
    const statusCode =
      typeof error === "object" && error !== null && "statusCode" in error && typeof error.statusCode === "number"
        ? error.statusCode
        : 500;
    logger.error("Info email approval gateway failed", {
      approvalId,
      statusCode,
      message: error instanceof Error ? error.message : "Unexpected approval error",
    });
    res.setHeader("cache-control", "no-store");
    res
      .status(statusCode)
      .type("html")
      .send(renderApprovalPage(false, "The approval link could not be completed. Please request a new approval email."));
  }
});

export default router;
