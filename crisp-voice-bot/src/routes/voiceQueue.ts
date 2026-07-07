import { Router, type NextFunction, type Request, type Response } from "express";
import { config } from "../lib/config";
import { processVoiceQueue } from "../lib/voiceQueue";

const router = Router();

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

export default router;
