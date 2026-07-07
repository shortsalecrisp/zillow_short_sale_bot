import express, { type ErrorRequestHandler, type NextFunction, type Request, type Response } from "express";
import { getConfiguredCallWindows } from "./lib/callWindowGuard";
import { config } from "./lib/config";
import { getElevenLabsVoiceExperimentStatus } from "./lib/elevenLabsVoiceVariant";
import { logger } from "./lib/logger";
import { startMailshakeSyncScheduler } from "./lib/mailshakeSync";
import { startVoiceQueueScheduler } from "./lib/voiceQueue";
import elevenLabsRouter from "./routes/elevenLabs";
import mailshakeSyncRouter from "./routes/mailshakeSync";
import smsReplyRouter from "./routes/smsReply";
import startCallRouter from "./routes/startCall";
import sheetUpdateRouter from "./routes/sheetUpdate";
import telnyxWebhookRouter from "./routes/telnyxWebhook";
import voiceQueueRouter from "./routes/voiceQueue";

const app = express();

app.use(express.json({ limit: "1mb" }));

app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({
    ok: true,
    service: "crisp-voice-bot",
    testMode: config.testMode,
    commit: process.env.RENDER_GIT_COMMIT ?? process.env.GIT_COMMIT ?? null,
    timestamp: new Date().toISOString(),
  });
});

app.get("/experiment-status", (_req: Request, res: Response) => {
  res.status(200).json({
    ok: true,
    service: "crisp-voice-bot",
    testMode: config.testMode,
    commit: process.env.RENDER_GIT_COMMIT ?? process.env.GIT_COMMIT ?? null,
    voiceExperiment: getElevenLabsVoiceExperimentStatus(),
    callWindowGuard: {
      requiresScheduledWindow: true,
      requiresAgentTimeZone: true,
      windows: getConfiguredCallWindows(),
    },
    timestamp: new Date().toISOString(),
  });
});

app.use("/start-call", startCallRouter);
app.use("/telnyx/webhook", telnyxWebhookRouter);
app.use("/sheet-update", sheetUpdateRouter);
app.use("/sms-reply", smsReplyRouter);
app.use("/elevenlabs", elevenLabsRouter);
app.use("/voice-queue", voiceQueueRouter);
app.use("/mailshake-sync", mailshakeSyncRouter);

app.use((_req: Request, res: Response) => {
  res.status(404).json({
    ok: false,
    error: "Route not found",
  });
});

const errorHandler: ErrorRequestHandler = (error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  const statusCode =
    typeof error === "object" &&
    error !== null &&
    "statusCode" in error &&
    typeof error.statusCode === "number"
      ? error.statusCode
      : typeof error === "object" &&
          error !== null &&
          "status" in error &&
          typeof error.status === "number"
        ? error.status
      : 500;

  const message = error instanceof Error ? error.message : "Unexpected server error";

  logger.error("Request failed", {
    statusCode,
    message,
    stack: error instanceof Error ? error.stack : undefined,
  });

  res.status(statusCode).json({
    ok: false,
    error: message,
  });
};

app.use(errorHandler);

app.listen(config.port, () => {
  logger.info("crisp-voice-bot listening", {
    port: config.port,
    baseUrl: config.baseUrl,
    testMode: config.testMode,
  });
  startVoiceQueueScheduler();
  startMailshakeSyncScheduler();
});

// TODO: Add live transfer support once call qualification logic is ready.

export default app;
