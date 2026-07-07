import { gmail_v1 } from "googleapis";
import { config } from "./config";
import { getGmailClient, hasGmailOAuthCredentials } from "./gmailClient";
import { gmailHeader, gmailPlainText, type GmailMessage } from "./gmailMessage";
import { readGmailImportState, writeGmailImportState } from "./gmailImportState";
import { logger } from "./logger";

const CALENDLY_PROCESSED_IDS_KEY = "calendlyGmail.processedIds";
const CALENDLY_REVIEW_IDS_KEY = "calendlyGmail.reviewIds";
const GOOGLE_MARKETING_PROCESSED_IDS_KEY = "googleMarketingAlert.processedIds";
const CALENDLY_CACHE_LIMIT = 500;
const GOOGLE_MARKETING_CACHE_LIMIT = 800;
const DEFAULT_TIMEZONE = "America/New_York";

type RunOptions = {
  dryRun?: boolean;
  seed?: boolean;
};

type QueryStat = {
  query: string;
  count: number;
};

type CalendlyParsedBooking = {
  email: string;
  payload: Record<string, unknown>;
};

type BusinessProfileMetric = {
  message_id: string;
  external_id: string;
  source_email: string;
  subject: string;
  profile_name: string;
  website_url: string;
  month: string | null;
  location_id: string;
  calls: number;
  chat_clicks: number;
  website_visits: number;
  profile_views: number;
  interactions: number;
  observed_at: string;
  metadata: Record<string, unknown>;
};

type SearchConsoleIndexingAlert = {
  message_id: string;
  external_id: string;
  source_email: string;
  subject: string;
  site_url: string;
  message_type: string;
  reasons: string[];
  report_url: string;
  received_at: string;
  metadata: Record<string, unknown>;
};

let activeCalendlyRun: Promise<unknown> | undefined;
let activeGoogleMarketingRun: Promise<unknown> | undefined;
let calendlySchedulerTimer: NodeJS.Timeout | undefined;
let googleMarketingSchedulerTimer: NodeJS.Timeout | undefined;

function cleanText(value: unknown): string {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function cleanMultilineText(value: unknown): string {
  return String(value ?? "")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function normalizeEmailBody(body: string): string {
  return String(body || "")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function idSet(ids: string[]): Record<string, true> {
  return ids.reduce<Record<string, true>>((acc, id) => {
    acc[id] = true;
    return acc;
  }, {});
}

async function storedIds(key: string): Promise<string[]> {
  return readGmailImportState<string[]>(key, []);
}

async function rememberIds(key: string, ids: string[], limit: number): Promise<number> {
  if (!ids.length) {
    return (await storedIds(key)).length;
  }

  const existing = await storedIds(key);
  const combined = existing.concat(ids.filter((id) => existing.indexOf(id) === -1)).slice(-limit);
  await writeGmailImportState(key, combined);
  return combined.length;
}

async function gmailSearchMessages(gmail: gmail_v1.Gmail, query: string, maxMessages: number) {
  const response = await gmail.users.messages.list({
    userId: "me",
    q: query,
    maxResults: maxMessages,
  });
  return response.data.messages ?? [];
}

async function gmailGetMessage(gmail: gmail_v1.Gmail, messageId: string): Promise<GmailMessage> {
  const response = await gmail.users.messages.get({
    userId: "me",
    id: messageId,
    format: "full",
  });
  return response.data;
}

async function searchCalendlyMessages(gmail: gmail_v1.Gmail, queries: string[], maxMessages: number) {
  const unique: Record<string, true> = {};
  const messages: gmail_v1.Schema$Message[] = [];
  const queryStats: QueryStat[] = [];

  for (const query of queries) {
    const found = await gmailSearchMessages(gmail, query, maxMessages);
    queryStats.push({ query, count: found.length });
    for (const message of found) {
      if (!message.id || unique[message.id]) {
        continue;
      }
      unique[message.id] = true;
      messages.push(message);
    }
  }

  return { messages: messages.slice(0, maxMessages), queryStats };
}

function calendlyQueries(): string[] {
  const lookbackDays = config.gmailImporters.calendlyLookbackDays;
  const extra = String(config.gmailImporters.calendlyExtraQueries ?? "")
    .split(/[\n;]+/)
    .map((query) => query.trim())
    .filter(Boolean);

  return [
    [`from:notifications@calendly.com`, `subject:"New Event"`, `newer_than:${lookbackDays}d`].join(" "),
    [`from:calendly.com`, `subject:"New Event"`, `newer_than:${lookbackDays}d`].join(" "),
    [`"A new event has been scheduled"`, `newer_than:${lookbackDays}d`].join(" "),
    [`from:notifications@calendly.com`, `"15-Minute Short-Sale Strategy Call"`, `newer_than:${lookbackDays}d`].join(" "),
    [`from:calendly.com`, `newer_than:${lookbackDays}d`].join(" "),
  ].concat(extra);
}

function calendlyWebhookUrl(): string {
  if (config.gmailImporters.calendlyWebhookUrl) {
    return config.gmailImporters.calendlyWebhookUrl;
  }

  const secret = config.gmailImporters.calendlyWebhookSecret;
  if (!secret) {
    throw new Error("CALENDLY_WEBHOOK_SECRET or CALENDLY_GMAIL_WEBHOOK_URL is required for Calendly Gmail import");
  }

  return `${config.gmailImporters.googleMarketingBaseUrl}/api/integrations/calendly/webhook?secret=${encodeURIComponent(secret)}`;
}

function isCalendlyBookingNotification(subject: string, from: string, body: string): boolean {
  const text = `${subject}\n${body}`;
  const isCalendlySender = /calendly/i.test(from);
  if (/^New Event:/i.test(subject)) {
    return isCalendlySender || /calendly/i.test(text);
  }
  return (isCalendlySender || /calendly/i.test(text)) && /A new event has been scheduled/i.test(text) && /invitee|email/i.test(text);
}

function labeledValue(body: string, labels: string[]): string {
  const lines = body
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean);

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    for (const candidate of labels) {
      const label = candidate.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const sameLine = line.match(new RegExp(`^${label}\\s*:?\\s*(.+)$`, "i"));
      if (sameLine && cleanText(sameLine[1]).toLowerCase() !== candidate.toLowerCase()) {
        return cleanText(sameLine[1]);
      }
      if (line.match(new RegExp(`^${label}\\s*:?$`, "i")) && lines[index + 1]) {
        return cleanText(lines[index + 1]);
      }
    }
  }

  return "";
}

function extractEmail(text: string): string {
  const match = String(text || "").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return match ? match[0].toLowerCase() : "";
}

function extractCalendlyInviteeEmail(text: string): string {
  const matches = Array.from(
    String(text || "").matchAll(/ATTENDEE[^\n\r:]*:?\s*mailto:([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})/gi),
  )
    .map((match) => String(match[1] || "").toLowerCase())
    .filter(Boolean);
  return matches.find((email) => !/@calendly\./i.test(email) && !/^(yoni\.kutler|ygkutler)@/i.test(email)) || matches[0] || "";
}

function extractPhone(text: string): string {
  const matches = String(text || "").match(/(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}/g);
  return matches?.length ? matches[matches.length - 1].trim() : "";
}

function extractUuid(text: string): string {
  const match = String(text || "").match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
  return match ? match[0] : "";
}

function visitorIdFromCalendlyText(text: string): string {
  const match = String(text || "").match(/\bcrisp_vid:([a-z0-9_-]{6,120})\b/i);
  return match ? `crisp_vid:${match[1]}` : "";
}

function calendlyTrackingFromText(text: string) {
  return {
    utm_source: labeledValue(text, ["UTM Source", "utm_source"]) || "calendly_email",
    utm_medium: labeledValue(text, ["UTM Medium", "utm_medium"]) || "booking_notification",
    utm_campaign: labeledValue(text, ["UTM Campaign", "utm_campaign"]) || "calendly_booking",
    utm_content: labeledValue(text, ["UTM Content", "utm_content"]) || visitorIdFromCalendlyText(text) || "gmail_calendly_booking",
    utm_term: labeledValue(text, ["UTM Term", "utm_term"]) || "",
  };
}

function durationMinutesFromCalendlyText(text: string): number {
  const match = String(text || "").match(/\b(\d{1,3})\s*(?:-| )?\s*(?:minute|min)\b/i);
  return match ? Number(match[1]) : 0;
}

function parseCalendlyDateText(text: string, fallbackDate: Date): string {
  const raw = cleanText(text);
  const parsed = raw ? new Date(raw) : null;
  if (parsed && !Number.isNaN(parsed.getTime())) {
    return parsed.toISOString();
  }

  const match = raw.match(
    /(\d{1,2}):(\d{2})\s*(am|pm)(?:\s*[-\u2013]\s*|\s+)(?:[A-Za-z]+,?\s+)?([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})/i,
  );
  if (match) {
    const months: Record<string, number> = {
      january: 0,
      jan: 0,
      february: 1,
      feb: 1,
      march: 2,
      mar: 2,
      april: 3,
      apr: 3,
      may: 4,
      june: 5,
      jun: 5,
      july: 6,
      jul: 6,
      august: 7,
      aug: 7,
      september: 8,
      sep: 8,
      sept: 8,
      october: 9,
      oct: 9,
      november: 10,
      nov: 10,
      december: 11,
      dec: 11,
    };
    const month = months[match[4].toLowerCase()];
    if (month !== undefined) {
      let hour = Number(match[1]);
      const minute = Number(match[2]);
      const meridiem = match[3].toLowerCase();
      if (meridiem === "pm" && hour < 12) hour += 12;
      if (meridiem === "am" && hour === 12) hour = 0;
      const date = new Date(Number(match[6]), month, Number(match[5]), hour, minute, 0, 0);
      if (!Number.isNaN(date.getTime())) return date.toISOString();
    }
  }

  return fallbackDate.toISOString();
}

function addMinutesIso(iso: string, minutes: number): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  date.setMinutes(date.getMinutes() + minutes);
  return date.toISOString();
}

export function parseCalendlyBookingMessage(message: GmailMessage, subject: string, body: string): CalendlyParsedBooking {
  const normalizedBody = normalizeEmailBody(body);
  const subjectParts = cleanText(subject).match(/^New Event:\s*(.+?)\s+-\s+(.+?)\s+-\s+(.+)$/i);
  const name = cleanText(labeledValue(normalizedBody, ["Invitee", "Name", "Invitee Name"]) || (subjectParts ? subjectParts[1] : ""));
  const email =
    extractCalendlyInviteeEmail(normalizedBody) ||
    extractEmail(labeledValue(normalizedBody, ["Email", "Invitee Email", "Email Address"])) ||
    extractEmail(normalizedBody);
  const phoneLabel = labeledValue(normalizedBody, ["What is your phone number?", "Phone Number", "Mobile", "Cell", "Phone"]);
  const phone = extractPhone(phoneLabel) || extractPhone(normalizedBody);
  const eventName = cleanText(labeledValue(normalizedBody, ["Event Type", "Event Name", "Event"]) || (subjectParts ? subjectParts[3] : ""));
  const dateText = cleanText(
    labeledValue(normalizedBody, ["Date & Time", "Date and Time", "When", "Event Time", "Time"]) || (subjectParts ? subjectParts[2] : ""),
  );
  const messageDate = new Date(Number(message.internalDate || Date.now()));
  const startIso = parseCalendlyDateText(dateText, messageDate);
  const durationMinutes = durationMinutesFromCalendlyText(eventName) || 15;
  const endIso = addMinutesIso(startIso, durationMinutes);
  const eventId = extractUuid(normalizedBody) || message.id || "unknown";
  const questions = [];

  if (phone) {
    questions.push({ question: "Phone number", answer: phone });
  }
  questions.push({ question: "Calendly notification subject", answer: subject });
  if (dateText) {
    questions.push({ question: "Calendly notification time", answer: dateText });
  }
  if (normalizedBody.slice(0, 1200)) {
    questions.push({ question: "Calendly email excerpt", answer: normalizedBody.slice(0, 1200) });
  }

  return {
    email,
    payload: {
      event: "invitee.created",
      source: "calendly_gmail",
      created_at: messageDate.toISOString(),
      payload: {
        uri: `gmail://calendly/invitees/${message.id}`,
        name,
        email,
        created_at: messageDate.toISOString(),
        timezone: DEFAULT_TIMEZONE,
        tracking: calendlyTrackingFromText(normalizedBody),
        questions_and_answers: questions,
        scheduled_event: {
          uri: `gmail://calendly/events/${eventId}`,
          name: eventName || "Calendly booking",
          start_time: startIso,
          end_time: endIso,
        },
      },
    },
  };
}

async function postJson(url: string, payload: unknown): Promise<{ responseCode: number; responseText: string }> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const responseText = await response.text();
  if (!response.ok) {
    throw new Error(`POST failed with ${response.status}: ${responseText.slice(0, 500)}`);
  }
  return { responseCode: response.status, responseText };
}

async function runCalendlyGmailImportUnlocked(options: RunOptions = {}) {
  const gmail = getGmailClient();
  const queries = calendlyQueries();
  const processed = idSet(await storedIds(CALENDLY_PROCESSED_IDS_KEY));
  const review = idSet(await storedIds(CALENDLY_REVIEW_IDS_KEY));
  const searchResult = await searchCalendlyMessages(gmail, queries, config.gmailImporters.calendlyMaxMessages);
  const bookings: Record<string, unknown>[] = [];
  const successfullyParsedIds: string[] = [];
  const reviewParsedIds: string[] = [];
  let skippedProcessed = 0;
  let skippedNonBooking = 0;

  for (const messageSummary of searchResult.messages) {
    if (!messageSummary.id) continue;
    if (processed[messageSummary.id] || review[messageSummary.id]) {
      skippedProcessed += 1;
      continue;
    }

    const message = await gmailGetMessage(gmail, messageSummary.id);
    const subject = cleanText(gmailHeader(message, "Subject"));
    const from = cleanText(gmailHeader(message, "From"));
    const body = await gmailPlainText(gmail, message);
    if (!isCalendlyBookingNotification(subject, from, body)) {
      skippedNonBooking += 1;
      continue;
    }

    const parsed = parseCalendlyBookingMessage(message, subject, body);
    const payload = parsed.payload as { payload?: { name?: unknown } };
    if (!parsed.email && !payload.payload?.name) {
      reviewParsedIds.push(messageSummary.id);
      continue;
    }

    bookings.push(parsed.payload);
    successfullyParsedIds.push(messageSummary.id);
  }

  const payload = {
    event: "calendly.gmail_sync",
    source: "calendly_gmail",
    records_seen: successfullyParsedIds.length + reviewParsedIds.length,
    messages_seen: searchResult.messages.length,
    source_account: config.gmailImporters.sourceAccount,
    query_stats: searchResult.queryStats,
    skipped_processed: skippedProcessed,
    skipped_non_booking: skippedNonBooking,
    review_messages: reviewParsedIds.length,
    processed_cache_size: Object.keys(processed).length,
    review_cache_size: Object.keys(review).length,
    bookings,
  };

  const shouldPost = !options.dryRun && !options.seed;
  const postResponse = shouldPost ? await postJson(calendlyWebhookUrl(), payload) : undefined;

  let processedCacheSize = Object.keys(processed).length;
  let reviewCacheSize = Object.keys(review).length;
  if (!options.dryRun || options.seed) {
    processedCacheSize = await rememberIds(CALENDLY_PROCESSED_IDS_KEY, successfullyParsedIds, CALENDLY_CACHE_LIMIT);
    reviewCacheSize = await rememberIds(CALENDLY_REVIEW_IDS_KEY, reviewParsedIds, CALENDLY_CACHE_LIMIT);
  }

  const result = {
    ok: true,
    job: "calendly",
    dryRun: Boolean(options.dryRun),
    seeded: Boolean(options.seed),
    queries,
    queryStats: searchResult.queryStats,
    sourceAccount: config.gmailImporters.sourceAccount,
    messagesSeen: searchResult.messages.length,
    bookingsPosted: shouldPost ? bookings.length : 0,
    bookingsParsed: bookings.length,
    reviewMessages: reviewParsedIds.length,
    skippedProcessed,
    skippedNonBooking,
    processedCacheSize,
    reviewCacheSize,
    responseCode: postResponse?.responseCode,
  };

  logger.info("Calendly Gmail import complete", result);
  return result;
}

export async function runCalendlyGmailImport(options: RunOptions = {}) {
  if (activeCalendlyRun) {
    logger.info("Calendly Gmail import already active; joining existing run");
    return activeCalendlyRun;
  }

  activeCalendlyRun = runCalendlyGmailImportUnlocked(options).finally(() => {
    activeCalendlyRun = undefined;
  });

  return activeCalendlyRun;
}

function googleMarketingQuery(): string {
  const lookbackDays = config.gmailImporters.googleMarketingLookbackDays;
  return [
    `newer_than:${lookbackDays}d`,
    "(from:businessprofile-noreply@google.com OR from:sc-noreply@google.com)",
    '("performance report" OR "prevent pages from being indexed" OR "preventing your pages from being indexed")',
  ].join(" ");
}

function googleMarketingUrl(path: string): string {
  const secret = config.gmailImporters.googleMarketingWebhookSecret;
  if (!secret) {
    throw new Error("GOOGLE_MARKETING_ALERT_WEBHOOK_SECRET or CRON_SECRET is required for Google marketing alert import");
  }
  return `${config.gmailImporters.googleMarketingBaseUrl}${path}?secret=${encodeURIComponent(secret)}`;
}

function metricBeforeLabel(body: string, labelPattern: string): number {
  const pattern = new RegExp(`(?:^|\\n)\\s*([\\d,]+)\\s*\\n\\s*${labelPattern}`, "i");
  return numberAfter(body, pattern);
}

function numberAfter(body: string, pattern: RegExp): number {
  const match = body.match(pattern);
  if (!match) return 0;
  const parsed = Number(String(match[1] || "0").replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function monthStart(monthName: string, year: string): string | null {
  const monthIndex = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
  ].indexOf(String(monthName || "").toLowerCase());
  if (monthIndex === -1) return null;
  return `${year}-${String(monthIndex + 1).padStart(2, "0")}-01`;
}

export function parseBusinessProfileMetric(
  messageId: string,
  subject: string,
  from: string,
  to: string,
  body: string,
  occurredAt: string,
): BusinessProfileMetric | null {
  const monthMatch = body.match(/business performance for ([A-Za-z]+)\s+(\d{4})/i);
  const month = monthMatch ? monthStart(monthMatch[1], monthMatch[2]) : null;
  const websiteUrl = (body.match(/https?:\/\/(?:www\.)?(?:crispshortsales\.com|crisp\.expert)\/?/i) || [])[0] || "";
  if (/https?:\/\/(?:www\.)?crisp\.expert\/?/i.test(websiteUrl)) return null;
  const reportUrl = (body.match(/\[See full report\]\(([^)]+)\)/i) || [])[1] || "";
  const locationId = (reportUrl.match(/[?&]fid=([^&]+)/) || [])[1] || websiteUrl || "unknown";
  const interactions = numberAfter(body, /business performance for [^:]+:\s*([\d,]+)\s+interactions/i);

  return {
    message_id: messageId,
    external_id: `gmail://business-profile/${messageId}`,
    source_email: to,
    subject,
    profile_name: "Crisp Short Sales",
    website_url: websiteUrl,
    month,
    location_id: locationId,
    calls: metricBeforeLabel(body, "calls?"),
    chat_clicks: metricBeforeLabel(body, "chat clicks?"),
    website_visits: metricBeforeLabel(body, "website visits from\\s+profile"),
    profile_views: metricBeforeLabel(body, "profile views"),
    interactions,
    observed_at: occurredAt,
    metadata: { from, report_url: reportUrl },
  };
}

export function parseSearchConsoleIndexingAlert(
  messageId: string,
  subject: string,
  from: string,
  to: string,
  body: string,
  occurredAt: string,
): SearchConsoleIndexingAlert | null {
  const siteUrl = (subject.match(/site (https?:\/\/[^/\s]+\/?)/i) || body.match(/site (https?:\/\/[^/\s]+\/?)/i) || [])[1];
  const reportUrl = (body.match(/\[Open indexing report\]\(([^)]+)\)/i) || [])[1] || "";
  const messageType = (body.match(/Message type:\s*\[([^\]]+)\]/i) || [])[1] || "";
  const reasons: string[] = [];

  ["Excluded by 'noindex' tag", "Excluded by \\u2018noindex\\u2019 tag", "Page with redirect", "Alternate page with proper canonical tag"].forEach(
    (reason) => {
      const printable = reason.replace(/\\u2018/g, "\u2018").replace(/\\u2019/g, "\u2019");
      if (body.indexOf(printable) !== -1) reasons.push(printable.replace(/[\u2018\u2019]/g, "'"));
    },
  );

  if (!siteUrl || !reasons.length) return null;
  return {
    message_id: messageId,
    external_id: `gmail://search-console-indexing/${messageId}`,
    source_email: to,
    subject,
    site_url: siteUrl,
    message_type: messageType,
    reasons,
    report_url: reportUrl,
    received_at: occurredAt,
    metadata: { from },
  };
}

async function postGoogleMarketingPayload(path: string, payload: unknown, records: number): Promise<{ skipped: true } | { responseCode: number }> {
  if (!records) {
    return { skipped: true };
  }

  const response = await postJson(googleMarketingUrl(path), payload);
  return { responseCode: response.responseCode };
}

async function runGoogleMarketingAlertImportUnlocked(options: RunOptions = {}) {
  const gmail = getGmailClient();
  const processed = idSet(await storedIds(GOOGLE_MARKETING_PROCESSED_IDS_KEY));
  const query = googleMarketingQuery();
  const messages = await gmailSearchMessages(gmail, query, config.gmailImporters.googleMarketingMaxMessages);
  const businessMetrics: BusinessProfileMetric[] = [];
  const indexingAlerts: SearchConsoleIndexingAlert[] = [];
  const importedIds: string[] = [];

  for (const messageSummary of messages) {
    if (!messageSummary.id || processed[messageSummary.id]) {
      continue;
    }

    const message = await gmailGetMessage(gmail, messageSummary.id);
    const subject = cleanText(gmailHeader(message, "Subject"));
    const from = cleanText(gmailHeader(message, "From"));
    const to = cleanText(gmailHeader(message, "To"));
    const body = cleanMultilineText(normalizeEmailBody(await gmailPlainText(gmail, message)));
    const occurredAt = new Date(Number(message.internalDate || Date.now())).toISOString();

    if (/businessprofile-noreply@google\.com/i.test(from) && /performance report/i.test(subject)) {
      const metric = parseBusinessProfileMetric(messageSummary.id, subject, from, to, body, occurredAt);
      if (metric) {
        businessMetrics.push(metric);
        importedIds.push(messageSummary.id);
      }
      continue;
    }

    if (/sc-noreply@google\.com/i.test(from) && /indexed/i.test(subject)) {
      const alert = parseSearchConsoleIndexingAlert(messageSummary.id, subject, from, to, body, occurredAt);
      if (alert) {
        indexingAlerts.push(alert);
        importedIds.push(messageSummary.id);
      }
    }
  }

  const shouldPost = !options.dryRun && !options.seed;
  const businessResponse = shouldPost
    ? await postGoogleMarketingPayload(
        "/api/integrations/google-business-profile/gmail",
        {
          event: "google_business_profile.gmail_sync",
          source: "google_marketing_alert_gmail",
          records_seen: businessMetrics.length,
          metrics: businessMetrics,
        },
        businessMetrics.length,
      )
    : { skipped: true as const };
  const indexingResponse = shouldPost
    ? await postGoogleMarketingPayload(
        "/api/integrations/search-console/gmail",
        {
          event: "search_console_indexing.gmail_sync",
          source: "google_marketing_alert_gmail",
          records_seen: indexingAlerts.length,
          alerts: indexingAlerts,
        },
        indexingAlerts.length,
      )
    : { skipped: true as const };

  let processedCacheSize = Object.keys(processed).length;
  if (!options.dryRun || options.seed) {
    processedCacheSize = await rememberIds(GOOGLE_MARKETING_PROCESSED_IDS_KEY, importedIds, GOOGLE_MARKETING_CACHE_LIMIT);
  }

  const result = {
    ok: true,
    job: "googleMarketing",
    dryRun: Boolean(options.dryRun),
    seeded: Boolean(options.seed),
    query,
    messagesSeen: messages.length,
    businessMetrics: businessMetrics.length,
    indexingAlerts: indexingAlerts.length,
    processedCacheSize,
    businessResponse,
    indexingResponse,
  };

  logger.info("Google marketing alert Gmail import complete", result);
  return result;
}

export async function runGoogleMarketingAlertImport(options: RunOptions = {}) {
  if (activeGoogleMarketingRun) {
    logger.info("Google marketing alert Gmail import already active; joining existing run");
    return activeGoogleMarketingRun;
  }

  activeGoogleMarketingRun = runGoogleMarketingAlertImportUnlocked(options).finally(() => {
    activeGoogleMarketingRun = undefined;
  });

  return activeGoogleMarketingRun;
}

export async function runGmailImporter(job: "all" | "calendly" | "googleMarketing", options: RunOptions = {}) {
  if (job === "calendly") {
    return runCalendlyGmailImport(options);
  }
  if (job === "googleMarketing") {
    return runGoogleMarketingAlertImport(options);
  }

  const [calendly, googleMarketing] = await Promise.all([
    runCalendlyGmailImport(options),
    runGoogleMarketingAlertImport(options),
  ]);
  return { ok: true, job: "all", calendly, googleMarketing };
}

function intervalMs(minutes: number): number {
  return Math.max(1, minutes) * 60_000;
}

export function startGmailImportSchedulers(): void {
  if (!hasGmailOAuthCredentials()) {
    logger.info("Gmail import schedulers disabled; Gmail OAuth credentials are not configured");
    return;
  }

  if (config.gmailImporters.calendlySchedulerEnabled && !calendlySchedulerTimer) {
    const runCalendly = () => {
      void runCalendlyGmailImport().catch((error) => {
        logger.error("Calendly Gmail import scheduler run failed", {
          message: error instanceof Error ? error.message : String(error),
        });
      });
    };

    logger.info("Calendly Gmail import scheduler enabled", {
      intervalMinutes: config.gmailImporters.calendlyIntervalMinutes,
    });
    calendlySchedulerTimer = setInterval(runCalendly, intervalMs(config.gmailImporters.calendlyIntervalMinutes));
    setTimeout(runCalendly, 60_000);
  } else if (!config.gmailImporters.calendlySchedulerEnabled) {
    logger.info("Calendly Gmail import scheduler disabled");
  }

  if (config.gmailImporters.googleMarketingSchedulerEnabled && !googleMarketingSchedulerTimer) {
    const runGoogleMarketing = () => {
      void runGoogleMarketingAlertImport().catch((error) => {
        logger.error("Google marketing alert Gmail import scheduler run failed", {
          message: error instanceof Error ? error.message : String(error),
        });
      });
    };

    logger.info("Google marketing alert Gmail import scheduler enabled", {
      intervalMinutes: config.gmailImporters.googleMarketingIntervalMinutes,
    });
    googleMarketingSchedulerTimer = setInterval(runGoogleMarketing, intervalMs(config.gmailImporters.googleMarketingIntervalMinutes));
    setTimeout(runGoogleMarketing, 75_000);
  } else if (!config.gmailImporters.googleMarketingSchedulerEnabled) {
    logger.info("Google marketing alert Gmail import scheduler disabled");
  }
}
