import { Router, type NextFunction, type Request, type Response } from "express";
import { config } from "../lib/config";
import { runGmailImporter } from "../lib/gmailImporters";

const router = Router();

type GmailImportJob = "all" | "calendly" | "googleMarketing";

function requestToken(req: Request): string | undefined {
  const header = req.header("X-Crisp-Token")?.trim();
  const bodyToken = typeof req.body?.token === "string" ? req.body.token.trim() : undefined;
  return header || bodyToken;
}

function isDryRun(req: Request): boolean {
  return req.body?.dryRun === true || req.query.dryRun === "1" || req.query.dryRun === "true";
}

function shouldSeed(req: Request): boolean {
  return req.body?.seed === true || req.query.seed === "1" || req.query.seed === "true";
}

function readJob(req: Request): GmailImportJob {
  const value = String(req.body?.job ?? req.query.job ?? "all").trim();
  if (value === "all" || value === "calendly" || value === "googleMarketing") {
    return value;
  }
  throw new Error("job must be one of: all, calendly, googleMarketing");
}

router.post("/run", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const expectedToken = config.googleAppsScript.token;

    if (expectedToken && requestToken(req) !== expectedToken) {
      res.status(401).json({ ok: false, error: "unauthorized" });
      return;
    }

    const result = await runGmailImporter(readJob(req), {
      dryRun: isDryRun(req),
      seed: shouldSeed(req),
    });
    res.status(200).json(result);
  } catch (error) {
    next(error);
  }
});

export default router;
