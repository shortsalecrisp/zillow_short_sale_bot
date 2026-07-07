import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

test("Calendly Gmail parser preserves the webhook payload shape", async () => {
  const { parseCalendlyBookingMessage } = await import("../src/lib/gmailImporters");

  const parsed = parseCalendlyBookingMessage(
    {
      id: "gmail-message-1",
      internalDate: String(Date.parse("2026-07-07T12:00:00Z")),
    },
    "New Event: Pat Lee - 9:30am - July 8, 2026 - 15-Minute Short-Sale Strategy Call",
    [
      "Name",
      "Pat Lee",
      "Email",
      "pat@example.com",
      "Phone Number",
      "(954) 555-1212",
      "Event Type",
      "15-Minute Short-Sale Strategy Call",
      "Date & Time",
      "9:30am - July 8, 2026",
      "UTM Source",
      "gmail-test",
    ].join("\n"),
  );

  const root = parsed.payload as {
    event?: string;
    source?: string;
    payload?: {
      uri?: string;
      name?: string;
      email?: string;
      tracking?: { utm_source?: string };
      questions_and_answers?: Array<{ question: string; answer: string }>;
      scheduled_event?: { uri?: string; name?: string };
    };
  };

  assert.equal(parsed.email, "pat@example.com");
  assert.equal(root.event, "invitee.created");
  assert.equal(root.source, "calendly_gmail");
  assert.equal(root.payload?.uri, "gmail://calendly/invitees/gmail-message-1");
  assert.equal(root.payload?.name, "Pat Lee");
  assert.equal(root.payload?.email, "pat@example.com");
  assert.equal(root.payload?.tracking?.utm_source, "gmail-test");
  assert.equal(root.payload?.scheduled_event?.name, "15-Minute Short-Sale Strategy Call");
  assert.deepEqual(root.payload?.questions_and_answers?.[0], { question: "Phone number", answer: "(954) 555-1212" });
});

test("Google marketing parser extracts Business Profile metrics", async () => {
  const { parseBusinessProfileMetric } = await import("../src/lib/gmailImporters");

  const metric = parseBusinessProfileMetric(
    "gmail-business-1",
    "Your June performance report",
    "businessprofile-noreply@google.com",
    "ygkutler@gmail.com",
    [
      "business performance for June 2026: 123 interactions",
      "https://crispshortsales.com/",
      "[See full report](https://business.google.com/report?fid=location-123)",
      "8",
      "calls",
      "5",
      "website visits from profile",
      "20",
      "profile views",
    ].join("\n"),
    "2026-07-07T12:00:00.000Z",
  );

  assert.equal(metric?.external_id, "gmail://business-profile/gmail-business-1");
  assert.equal(metric?.month, "2026-06-01");
  assert.equal(metric?.location_id, "location-123");
  assert.equal(metric?.interactions, 123);
  assert.equal(metric?.calls, 8);
  assert.equal(metric?.website_visits, 5);
  assert.equal(metric?.profile_views, 20);
});

test("Google marketing parser extracts Search Console indexing alerts", async () => {
  const { parseSearchConsoleIndexingAlert } = await import("../src/lib/gmailImporters");

  const alert = parseSearchConsoleIndexingAlert(
    "gmail-search-1",
    "New reason preventing your pages from being indexed for site https://crispshortsales.com/",
    "sc-noreply@google.com",
    "ygkutler@gmail.com",
    [
      "Message type: [Indexing issue]",
      "Excluded by 'noindex' tag",
      "[Open indexing report](https://search.google.com/search-console/report)",
    ].join("\n"),
    "2026-07-07T12:00:00.000Z",
  );

  assert.equal(alert?.external_id, "gmail://search-console-indexing/gmail-search-1");
  assert.equal(alert?.site_url, "https://crispshortsales.com/");
  assert.deepEqual(alert?.reasons, ["Excluded by 'noindex' tag"]);
  assert.equal(alert?.message_type, "Indexing issue");
});
