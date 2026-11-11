import { Router, type Request, type Response, type NextFunction } from "express";
import { config } from "../lib/config";
import { voiceCallbackAlerts, getVoiceCallbackAlertStatus } from "../lib/voiceCallbackAlerts";

export function createCallbackAlertsRouter(service = voiceCallbackAlerts) {
  const router = Router();
  router.use((req: Request, res: Response, next: NextFunction) => {
    const secret = config.googleAppsScript.token;
    if (!secret || req.header("X-Crisp-Token") !== secret) { res.status(401).json({ ok: false, error: "unauthorized" }); return; }
    next();
  });
  router.get("/status", async (_req, res, next) => {
    try { res.json({ ok: true, ...getVoiceCallbackAlertStatus(), ...await service.health(), recipient: config.emailAlerts.to, sender: config.emailAlerts.from }); }
    catch (error) { next(error); }
  });
  router.post("/activate", async (req, res, next) => {
    if (req.body?.activate !== true) { res.status(400).json({ ok: false, error: "Explicit activation required" }); return; }
    if (!getVoiceCallbackAlertStatus().configured) { res.status(503).json({ ok: false, error: "Owner email configuration is incomplete" }); return; }
    try { res.json({ ok: true, ...await service.activate(), callsPlaced: 0, leadRowsChanged: 0 }); }
    catch (error) { next(error); }
  });
  router.post("/self-test", async (_req, res, next) => {
    if (!_req.body?.expectedRecipient || _req.body.expectedRecipient !== config.emailAlerts.to) {
      res.status(409).json({ ok: false, error: "Configured owner recipient must match the approved test recipient" }); return;
    }
    try { res.json({ ok: true, ...await service.selfTest(), synthetic: true, callsPlaced: 0, leadRowsChanged: 0 }); }
    catch (error) { next(error); }
  });
  return router;
}
export default createCallbackAlertsRouter();
