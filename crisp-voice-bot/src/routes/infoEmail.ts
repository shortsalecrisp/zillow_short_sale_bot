import { Router, type NextFunction, type Request, type Response } from "express";
import { requireInfoEmailApiSecret, sendAgentInfoEmail, verifyInfoEmailSmtp } from "../lib/infoEmail";
import { logger } from "../lib/logger";

const router = Router();

function authenticateInfoEmailRequest(req: Request): void {
  const expected = requireInfoEmailApiSecret();
  const actual = req.header("x-crisp-info-email-secret")?.trim();
  if (!actual || actual !== expected) {
    throw Object.assign(new Error("Unauthorized"), { statusCode: 401 });
  }
}

router.post("/send-approved", async (req: Request, res: Response, next: NextFunction) => {
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

router.post("/verify-smtp", async (req: Request, res: Response, next: NextFunction) => {
  try {
    authenticateInfoEmailRequest(req);
    await verifyInfoEmailSmtp();
    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

export default router;
