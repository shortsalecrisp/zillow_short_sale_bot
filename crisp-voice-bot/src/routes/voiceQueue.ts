import { Router, type NextFunction, type Request, type Response } from "express";
import { config } from "../lib/config";
import { processVoiceQueue } from "../lib/voiceQueue";
import { getProviderCircuitStatus, resetProviderCircuit } from "../lib/providerCircuitBreaker";
import { reconcileCorrectedVoicePhone } from "../lib/voiceCorrectedPhoneRelease";
import { reconcileReviewedVoiceIdentity } from "../lib/voiceReviewedIdentity";

const router = Router();

router.post("/reconcile-reviewed-identity", async (req: Request, res: Response, next: NextFunction) => {
  const token = config.googleAppsScript.token;
  if (!token || requestToken(req) !== token) { res.status(401).json({ ok: false, error: "unauthorized" }); return; }
  if (!Number.isInteger(req.body?.row) || req.body.row < 2 || req.body.row >= 9000) { res.status(400).json({ ok: false, error: "bounded_row_required" }); return; }
  try { res.json(await reconcileReviewedVoiceIdentity(req.body)); } catch (error) { next(error); }
});

router.post("/reconcile-corrected-phone", async (req: Request, res: Response, next: NextFunction) => {
  const token = config.googleAppsScript.token;
  if (!token || requestToken(req) !== token) { res.status(401).json({ ok: false, error: "unauthorized" }); return; }
  if (!Number.isInteger(req.body?.row) || req.body.row < 2 || req.body.row >= 9000) {
    res.status(400).json({ ok: false, error: "bounded_row_required" }); return;
  }
  try { res.json(await reconcileCorrectedVoicePhone(req.body)); } catch (error) { next(error); }
});

function requestToken(req: Request): string | undefined {
  const header = req.header("X-Crisp-Token")?.trim();
  const bodyToken = typeof req.body?.token === "string" ? req.body.token.trim() : undefined;
  return header || bodyToken;
}

function isDryRun(req: Request): boolean {
  return req.body?.dryRun === true || req.query.dryRun === "1" || req.query.dryRun === "true";
}

router.post("/run", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const expectedToken = config.googleAppsScript.token;

    if (expectedToken && requestToken(req) !== expectedToken) {
      res.status(401).json({ ok: false, error: "unauthorized" });
      return;
    }

    const result = await processVoiceQueue({ dryRun: isDryRun(req) });
    res.status(200).json(result);
  } catch (error) {
    next(error);
  }
});

router.post("/provider-circuit/restore", (req: Request, res: Response) => {
  const expectedToken = config.googleAppsScript.token;
  if (!expectedToken) {
    res.status(503).json({ ok: false, error: "provider_circuit_reset_auth_not_configured" });
    return;
  }
  if (requestToken(req) !== expectedToken) {
    res.status(401).json({ ok: false, error: "unauthorized" });
    return;
  }

  const reason = typeof req.body?.reason === "string" ? req.body.reason.trim() : "";
  if (!reason) {
    res.status(400).json({ ok: false, error: "A provider restoration reason is required" });
    return;
  }

  res.status(200).json({
    ok: true,
    restored: true,
    providerCircuit: resetProviderCircuit(reason),
  });
});

router.get("/provider-circuit/status", (req: Request, res: Response) => {
  const expectedToken = config.googleAppsScript.token;
  if (!expectedToken || requestToken(req) !== expectedToken) {
    res.status(401).json({ ok: false, error: "unauthorized" });
    return;
  }
  res.status(200).json({ ok: true, providerCircuit: getProviderCircuitStatus() });
});

export default router;
