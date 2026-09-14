import { timingSafeEqual } from "node:crypto";
import { Router, type Request, type Response } from "express";
import { assessElevenLabsTerminalPermission } from "../lib/elevenLabsTerminalPermission";

export function createElevenLabsTerminalRouter(secret: string | undefined): Router {
  const router = Router();
  router.use((req: Request, res: Response, next) => {
    if (!secret) {
      res.status(503).json({ ok: false, permission: false, decision: "control_not_configured" });
      return;
    }
    const authorization = req.header("authorization");
    const provided = authorization?.startsWith("Bearer ") ? authorization.slice(7).trim() : req.header("x-crisp-elevenlabs-secret");
    const suppliedBytes = Buffer.from(provided ?? ""), expectedBytes = Buffer.from(secret);
    if (suppliedBytes.length !== expectedBytes.length || !timingSafeEqual(suppliedBytes, expectedBytes)) {
      res.status(401).json({ ok: false, permission: false, decision: "unauthorized" });
      return;
    }
    next();
  });
  router.post("/reset-ending", (_req: Request, res: Response) => {
    res.json({ ok: true, permission: false, decision: "reset" });
  });
  router.post("/validate-ending", (req: Request, res: Response) => {
    const result = assessElevenLabsTerminalPermission(req.body);
    res.status(result.ok ? 200 : 400).json(result);
  });
  return router;
}
