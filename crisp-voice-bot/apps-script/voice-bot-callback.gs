/**
 * =============================================================================
 * Crisp Voice Bot Callback + Scheduler for Google Apps Script
 * =============================================================================
 *
 * LOCAL SOURCE OF TRUTH:
 * This file is the repo-owned source for the Apps Script callback and scheduler
 * used by the crisp-voice-bot backend. Edit this file locally first.
 *
 * MANUAL DEPLOY STEP:
 * After editing, copy this file into the bound Google Apps Script project for
 * the ShortSaleLeads spreadsheet, then redeploy the Apps Script web app. The
 * deployed Apps Script code must stay in sync with this local file.
 *
 * MERGE-SAFE NOTE:
 * This code adds voice callback handling and a voice-call queue runner. It
 * should not replace or remove existing Mailshake/text-bot logic, including
 * syncNewLeadsToMailshake(), triggers, or existing sheet logic.
 *
 * IMPORTANT doPost(e) NOTE:
 * Apps Script projects can only have one doPost(e). If the bound project
 * already has a doPost(e), do not paste a second one. Instead, merge the voice
 * callback branch into the existing doPost(e) using:
 *
 *   const payload = parseJsonPost_(e);
 *   const result = handleVoiceBotCallback_(payload);
 *   return jsonOutput_(result);
 *
 * AUTH:
 * VOICE_BOT_SHARED_TOKEN must exactly match GOOGLE_APPS_SCRIPT_TOKEN in the
 * crisp-voice-bot backend environment. Do not leave it as REPLACE_ME in a
 * deployed web app.
 *
 * SCHEDULER:
 * processVoiceBotCallQueue() is meant to run on a time-based trigger, for
 * example every 15 minutes. It scans Sheet1, finds the next row eligible for
 * a voice call, and POSTs to the Node backend /start-call route.
 */

// =============================================================================
// Config
// =============================================================================

const VOICE_BOT_SHARED_TOKEN = 'REPLACE_ME';
const VOICE_BOT_SHEET_NAME = 'Sheet1';
const VOICE_BOT_START_CALL_URL = 'REPLACE_ME';

const VOICE_BOT_BUSINESS_DAY_START_HOUR_ET = 8;
const VOICE_BOT_BUSINESS_DAY_END_HOUR_ET = 20;
const VOICE_BOT_CALL_DELAY_BUSINESS_HOURS = 6;
const VOICE_BOT_TIMEZONE = 'America/New_York';

const VOICE_BOT_COL_FIRST_NAME = 1; // A = agent_name
const VOICE_BOT_COL_LAST_NAME = 2; // B = last_name
const VOICE_BOT_COL_PHONE = 3; // C = phone
const VOICE_BOT_COL_EMAIL = 4; // D = email
const VOICE_BOT_COL_LISTING_ADDRESS = 5; // E = listing_address
const VOICE_BOT_COL_CITY = 6; // F = city
const VOICE_BOT_COL_STATE = 7; // G = state
const VOICE_BOT_COL_INITIAL_TEXT_SENT = 8; // H = initial_text_sent
const VOICE_BOT_COL_FOLLOWUP_TEXT_SENT = 9; // I = followup_text_sent
const VOICE_BOT_COL_RESPONSE_STATUS = 10; // J = response_status (human-readable voice outcome)
const VOICE_BOT_COL_LEAD_STATUS_CODE = 11; // K = final lead code used by the outreach workflow (Y/R/G/N)
const VOICE_BOT_COL_CONVERSATION_SUMMARY = 13; // M = conversation_summary
const VOICE_BOT_COL_FOLLOWUP_SENT_AT_PROXY = 24; // X = current live sheet proxy for follow-up sent timestamp
const VOICE_BOT_COL_CREATED_AT = 28; // AB = created-at
const VOICE_BOT_COL_CALL_ELIGIBLE = 30; // AD = call_eligible
const VOICE_BOT_COL_CALL_TIME_BUCKET = 31; // AE = call_time_bucket
const VOICE_BOT_COL_CALL_SCHEDULED_FOR = 32; // AF = call_scheduled_for
const VOICE_BOT_COL_CALL_1_SENT = 33; // AG = voice_call_1_sent
const VOICE_BOT_COL_CALL_1_RESULT = 34; // AH = voice_call_1_result
const VOICE_BOT_COL_VM_LEFT = 35; // AI = vm_left
const VOICE_BOT_COL_LIVE_TRANSFER_REQUESTED = 36; // AJ = live_transfer_requested
const VOICE_BOT_COL_LIVE_TRANSFER_COMPLETED = 37; // AK = live_transfer_completed
const VOICE_BOT_COL_CALLBACK_REQUESTED = 38; // AL = callback_requested
const VOICE_BOT_COL_CALLBACK_TIME = 39; // AM = callback_time
const VOICE_BOT_COL_CALL_2_SENT = 40; // AN = voice_call_2_sent
const VOICE_BOT_COL_CALL_2_RESULT = 41; // AO = voice_call_2_result
const VOICE_BOT_COL_VOICE_NOTES = 42; // AP = voice_notes

// =============================================================================
// Web App Entrypoint
// =============================================================================

function doPost(e) {
  try {
    const payload = parseJsonPost_(e);
    const result = handleVoiceBotCallback_(payload);
    return jsonOutput_(result);
  } catch (err) {
    const code = err && err.code ? err.code : 'voice_callback_error';
    const message = err && err.message ? err.message : String(err);

    voiceBotLog_({
      event: 'voice_callback_rejected',
      code: code,
      message: message
    });

    return jsonOutput_({
      ok: false,
      code: code,
      error: message
    });
  }
}

// =============================================================================
// Voice Callback Handling
// =============================================================================

function handleVoiceBotCallback_(payload) {
  validateVoiceBotPayload_(payload);

  const rowNumber = Number(payload.rowNumber);
  const sheet = getVoiceBotSheet_();

  validateVoiceBotRow_(sheet, rowNumber);

  voiceBotLog_({
    event: 'voice_callback_accepted',
    rowNumber: rowNumber,
    callAttemptNumber: normalizeCallAttemptNumber_(payload.callAttemptNumber),
    callResult: valueOrNull_(payload.callResult),
    responseStatus: valueOrNull_(payload.responseStatus),
    leadStatusCode: valueOrNull_(payload.leadStatusCode)
  });

  const lock = LockService.getDocumentLock();

  try {
    lock.waitLock(5000);
    const fieldsWritten = applyVoiceBotRowUpdates_(sheet, rowNumber, payload);

    voiceBotLog_({
      event: 'voice_callback_row_updated',
      rowNumber: rowNumber,
      callAttemptNumber: normalizeCallAttemptNumber_(payload.callAttemptNumber),
      fieldsWritten: fieldsWritten,
      callResult: valueOrNull_(payload.callResult),
      responseStatus: valueOrNull_(payload.responseStatus),
      leadStatusCode: valueOrNull_(payload.leadStatusCode)
    });

    return {
      ok: true,
      rowNumber: rowNumber,
      callAttemptNumber: normalizeCallAttemptNumber_(payload.callAttemptNumber),
      fieldsWritten: fieldsWritten
    };
  } catch (err) {
    throw voiceCallbackError_(
      'row_update_failed',
      err && err.message ? err.message : 'Failed to update voice callback row'
    );
  } finally {
    try {
      lock.releaseLock();
    } catch (releaseErr) {
      // Nothing to do. Lock release can fail if waitLock never acquired it.
    }
  }
}

function applyVoiceBotRowUpdates_(sheet, rowNumber, payload) {
  const fieldsWritten = [];
  const callAttemptNumber = normalizeCallAttemptNumber_(payload.callAttemptNumber);
  const callSentColumn = callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT;
  const callResultColumn = callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_RESULT : VOICE_BOT_COL_CALL_1_RESULT;

  writeVoiceBotFieldIfPresent_(sheet, rowNumber, callResultColumn, payload, 'callResult', fieldsWritten);
  writeVoiceBotFieldIfPresent_(sheet, rowNumber, VOICE_BOT_COL_RESPONSE_STATUS, payload, 'responseStatus', fieldsWritten);
  writeVoiceBotFieldIfPresent_(sheet, rowNumber, VOICE_BOT_COL_LEAD_STATUS_CODE, payload, 'leadStatusCode', fieldsWritten);
  writeVoiceBotFieldIfPresent_(sheet, rowNumber, VOICE_BOT_COL_VOICE_NOTES, payload, 'voiceNotes', fieldsWritten);
  writeVoiceBotFieldIfPresent_(sheet, rowNumber, VOICE_BOT_COL_VM_LEFT, payload, 'vmLeft', fieldsWritten);
  writeVoiceBotFieldIfPresent_(sheet, rowNumber, VOICE_BOT_COL_LIVE_TRANSFER_REQUESTED, payload, 'liveTransferRequested', fieldsWritten);
  writeVoiceBotFieldIfPresent_(sheet, rowNumber, VOICE_BOT_COL_LIVE_TRANSFER_COMPLETED, payload, 'liveTransferCompleted', fieldsWritten);
  writeVoiceBotFieldIfPresent_(sheet, rowNumber, VOICE_BOT_COL_CALLBACK_REQUESTED, payload, 'callbackRequested', fieldsWritten);
  writeVoiceBotFieldIfPresent_(sheet, rowNumber, VOICE_BOT_COL_CALLBACK_TIME, payload, 'callbackTime', fieldsWritten);
  writeVoiceBotFieldIfPresent_(sheet, rowNumber, VOICE_BOT_COL_CALL_SCHEDULED_FOR, payload, 'callScheduledFor', fieldsWritten);

  const callSentCell = sheet.getRange(rowNumber, callSentColumn);
  if (!callSentCell.getValue()) {
    callSentCell.setValue(new Date());
    fieldsWritten.push(columnToLetter_(callSentColumn) + ':voice_call_' + callAttemptNumber + '_sent');
  }

  updateVoiceBotSchedulingCells_(sheet, rowNumber, payload, callAttemptNumber, fieldsWritten);

  // TODO: Support exact scheduled callback datetime values, not just callbackTime buckets.
  // TODO: Support lookup by stable lead ID instead of rowNumber.

  return fieldsWritten;
}

function updateVoiceBotSchedulingCells_(sheet, rowNumber, payload, callAttemptNumber, fieldsWritten) {
  const callResult = normalizeString_(payload.callResult);
  const leadStatusCode = normalizeString_(payload.leadStatusCode);
  const retryableFirstAttempt = callAttemptNumber === 1 && isRetryableVoiceBotResult_(callResult);

  if (retryableFirstAttempt) {
    const firstAttemptSentAt = parseVoiceBotDate_(sheet.getRange(rowNumber, VOICE_BOT_COL_CALL_1_SENT).getValue());
    if (firstAttemptSentAt) {
      const nextAttemptAt = addBusinessHours_(firstAttemptSentAt, VOICE_BOT_CALL_DELAY_BUSINESS_HOURS);
      sheet.getRange(rowNumber, VOICE_BOT_COL_CALL_SCHEDULED_FOR).setValue(nextAttemptAt);
      sheet.getRange(rowNumber, VOICE_BOT_COL_CALL_ELIGIBLE).setValue('yes');
      sheet.getRange(rowNumber, VOICE_BOT_COL_CALL_TIME_BUCKET).setValue('voice_call_2_due');
      fieldsWritten.push(columnToLetter_(VOICE_BOT_COL_CALL_SCHEDULED_FOR) + ':call_scheduled_for');
      fieldsWritten.push(columnToLetter_(VOICE_BOT_COL_CALL_ELIGIBLE) + ':call_eligible');
      fieldsWritten.push(columnToLetter_(VOICE_BOT_COL_CALL_TIME_BUCKET) + ':call_time_bucket');
    }
    return;
  }

  if (leadStatusCode || callAttemptNumber === 2) {
    clearVoiceBotCellIfNeeded_(sheet, rowNumber, VOICE_BOT_COL_CALL_ELIGIBLE, fieldsWritten, 'call_eligible');
    clearVoiceBotCellIfNeeded_(sheet, rowNumber, VOICE_BOT_COL_CALL_TIME_BUCKET, fieldsWritten, 'call_time_bucket');
    clearVoiceBotCellIfNeeded_(sheet, rowNumber, VOICE_BOT_COL_CALL_SCHEDULED_FOR, fieldsWritten, 'call_scheduled_for');
  }
}

// =============================================================================
// Queue Runner
// =============================================================================

function processVoiceBotCallQueue() {
  validateVoiceBotQueueConfig_();

  const sheet = getVoiceBotSheet_();
  const now = new Date();

  if (!isWithinBusinessHours_(now)) {
    voiceBotLog_({
      event: 'voice_queue_skipped_outside_business_hours',
      nowEt: formatVoiceBotDateEt_(now)
    });

    return {
      ok: true,
      queued: false,
      reason: 'outside_business_hours',
      nowEt: formatVoiceBotDateEt_(now)
    };
  }

  const candidate = getNextVoiceBotCallCandidate_(sheet, now);
  if (!candidate) {
    voiceBotLog_({
      event: 'voice_queue_no_candidate',
      nowEt: formatVoiceBotDateEt_(now)
    });

    return {
      ok: true,
      queued: false,
      reason: 'no_candidate'
    };
  }

  const lock = LockService.getDocumentLock();

  try {
    lock.waitLock(5000);

    const refreshedCandidate = getVoiceBotCallCandidateByRow_(sheet, candidate.rowNumber, now);
    if (!refreshedCandidate) {
      voiceBotLog_({
        event: 'voice_queue_candidate_no_longer_eligible',
        rowNumber: candidate.rowNumber
      });

      return {
        ok: true,
        queued: false,
        reason: 'candidate_no_longer_eligible',
        rowNumber: candidate.rowNumber
      };
    }

    const payload = buildVoiceBotStartCallPayload_(refreshedCandidate);
    const startCallResult = postVoiceBotStartCall_(payload);

    markVoiceBotAttemptStarted_(sheet, refreshedCandidate, now);

    voiceBotLog_({
      event: 'voice_queue_call_started',
      rowNumber: refreshedCandidate.rowNumber,
      callAttemptNumber: refreshedCandidate.callAttemptNumber,
      dueAtEt: formatVoiceBotDateEt_(refreshedCandidate.dueAt),
      startCallUrl: VOICE_BOT_START_CALL_URL,
      startCallResult: startCallResult
    });

    return {
      ok: true,
      queued: true,
      rowNumber: refreshedCandidate.rowNumber,
      callAttemptNumber: refreshedCandidate.callAttemptNumber,
      dueAtEt: formatVoiceBotDateEt_(refreshedCandidate.dueAt)
    };
  } finally {
    try {
      lock.releaseLock();
    } catch (releaseErr) {
      // Nothing to do. Lock release can fail if waitLock never acquired it.
    }
  }
}

function installVoiceBotCallQueueTrigger() {
  const projectTriggers = ScriptApp.getProjectTriggers();
  let triggerExists = false;

  for (var i = 0; i < projectTriggers.length; i++) {
    if (projectTriggers[i].getHandlerFunction() === 'processVoiceBotCallQueue') {
      triggerExists = true;
      break;
    }
  }

  if (!triggerExists) {
    ScriptApp.newTrigger('processVoiceBotCallQueue').timeBased().everyMinutes(15).create();
  }
}

function resetVoiceBotCallQueueTriggers() {
  const projectTriggers = ScriptApp.getProjectTriggers();

  for (var i = 0; i < projectTriggers.length; i++) {
    if (projectTriggers[i].getHandlerFunction() === 'processVoiceBotCallQueue') {
      ScriptApp.deleteTrigger(projectTriggers[i]);
    }
  }

  installVoiceBotCallQueueTrigger();
}

function getNextVoiceBotCallCandidate_(sheet, now) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return null;
  }

  const values = sheet.getRange(2, 1, lastRow - 1, VOICE_BOT_COL_VOICE_NOTES).getValues();

  for (var i = 0; i < values.length; i++) {
    const rowNumber = i + 2;
    const candidate = getVoiceBotCallCandidateFromRowValues_(rowNumber, values[i], now);
    if (candidate) {
      return candidate;
    }
  }

  return null;
}

function getVoiceBotCallCandidateByRow_(sheet, rowNumber, now) {
  const values = sheet.getRange(rowNumber, 1, 1, VOICE_BOT_COL_VOICE_NOTES).getValues()[0];
  return getVoiceBotCallCandidateFromRowValues_(rowNumber, values, now);
}

function getVoiceBotCallCandidateFromRowValues_(rowNumber, rowValues, now) {
  const leadStatusCode = normalizeString_(rowValues[VOICE_BOT_COL_LEAD_STATUS_CODE - 1]);
  if (leadStatusCode) {
    return null;
  }

  const followupSentFlag = normalizeMarker_(rowValues[VOICE_BOT_COL_FOLLOWUP_TEXT_SENT - 1]);
  if (followupSentFlag !== 'x') {
    return null;
  }

  const firstAttemptSentAt = parseVoiceBotDate_(rowValues[VOICE_BOT_COL_CALL_1_SENT - 1]);
  const secondAttemptSentAt = parseVoiceBotDate_(rowValues[VOICE_BOT_COL_CALL_2_SENT - 1]);
  const firstAttemptResult = normalizeString_(rowValues[VOICE_BOT_COL_CALL_1_RESULT - 1]);

  if (!firstAttemptSentAt) {
    const followupSentAt = parseVoiceBotDate_(rowValues[VOICE_BOT_COL_FOLLOWUP_SENT_AT_PROXY - 1]);
    if (!followupSentAt) {
      return null;
    }

    const dueAt = addBusinessHours_(followupSentAt, VOICE_BOT_CALL_DELAY_BUSINESS_HOURS);
    if (now < dueAt) {
      return null;
    }

    return buildVoiceBotCandidate_(rowNumber, rowValues, 1, dueAt);
  }

  if (secondAttemptSentAt) {
    return null;
  }

  if (!isRetryableVoiceBotResult_(firstAttemptResult)) {
    return null;
  }

  const secondAttemptDueAt = addBusinessHours_(firstAttemptSentAt, VOICE_BOT_CALL_DELAY_BUSINESS_HOURS);
  if (now < secondAttemptDueAt) {
    return null;
  }

  return buildVoiceBotCandidate_(rowNumber, rowValues, 2, secondAttemptDueAt);
}

function buildVoiceBotCandidate_(rowNumber, rowValues, callAttemptNumber, dueAt) {
  const firstName = normalizeString_(rowValues[VOICE_BOT_COL_FIRST_NAME - 1]);
  const lastName = normalizeString_(rowValues[VOICE_BOT_COL_LAST_NAME - 1]);
  const phone = normalizePhoneToE164_(rowValues[VOICE_BOT_COL_PHONE - 1]);
  const listingAddress = buildVoiceBotListingAddress_(rowValues);

  if (!firstName || !phone || !listingAddress) {
    voiceBotLog_({
      event: 'voice_queue_row_skipped_missing_required_values',
      rowNumber: rowNumber,
      callAttemptNumber: callAttemptNumber,
      firstName: valueOrNull_(firstName),
      phone: valueOrNull_(phone),
      listingAddress: valueOrNull_(listingAddress)
    });
    return null;
  }

  return {
    rowNumber: rowNumber,
    callAttemptNumber: callAttemptNumber,
    firstName: firstName,
    lastName: lastName,
    fullName: [firstName, lastName].filter(Boolean).join(' '),
    phone: phone,
    email: normalizeString_(rowValues[VOICE_BOT_COL_EMAIL - 1]),
    listingAddress: listingAddress,
    createdAt: normalizeString_(rowValues[VOICE_BOT_COL_CREATED_AT - 1]),
    existingResponseStatus: normalizeString_(rowValues[VOICE_BOT_COL_RESPONSE_STATUS - 1]),
    voiceNotes: normalizeString_(rowValues[VOICE_BOT_COL_VOICE_NOTES - 1]),
    dueAt: dueAt
  };
}

function buildVoiceBotStartCallPayload_(candidate) {
  return {
    rowNumber: candidate.rowNumber,
    callAttemptNumber: candidate.callAttemptNumber,
    firstName: candidate.firstName,
    lastName: candidate.lastName,
    fullName: candidate.fullName,
    phone: candidate.phone,
    email: candidate.email || undefined,
    listingAddress: candidate.listingAddress,
    createdAt: candidate.createdAt || undefined,
    scheduledForEt: formatVoiceBotDateEt_(candidate.dueAt),
    responseStatus: candidate.existingResponseStatus || undefined,
    notes: candidate.voiceNotes || undefined,
    sheetName: VOICE_BOT_SHEET_NAME
  };
}

function postVoiceBotStartCall_(payload) {
  const response = UrlFetchApp.fetch(VOICE_BOT_START_CALL_URL, {
    method: 'post',
    contentType: 'application/json',
    muteHttpExceptions: true,
    payload: JSON.stringify(payload)
  });

  const statusCode = response.getResponseCode();
  const responseText = response.getContentText();
  const responseBody = parseJsonSafely_(responseText);

  if (statusCode < 200 || statusCode >= 300) {
    throw voiceCallbackError_(
      'start_call_failed',
      'Voice bot start-call request failed with status ' + statusCode + ': ' + responseText
    );
  }

  return {
    statusCode: statusCode,
    body: responseBody || responseText
  };
}

function markVoiceBotAttemptStarted_(sheet, candidate, now) {
  const sentColumn = candidate.callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT;
  const timeBucket = candidate.callAttemptNumber === 2 ? 'voice_call_2_due' : 'voice_call_1_due';

  sheet.getRange(candidate.rowNumber, sentColumn).setValue(now);
  sheet.getRange(candidate.rowNumber, VOICE_BOT_COL_CALL_ELIGIBLE).setValue('queued');
  sheet.getRange(candidate.rowNumber, VOICE_BOT_COL_CALL_TIME_BUCKET).setValue(timeBucket);
  sheet.getRange(candidate.rowNumber, VOICE_BOT_COL_CALL_SCHEDULED_FOR).setValue(candidate.dueAt);
}

// =============================================================================
// Validation
// =============================================================================

function validateVoiceBotPayload_(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    throw voiceCallbackError_('invalid_payload', 'Payload must be a JSON object');
  }

  if (VOICE_BOT_SHARED_TOKEN === 'REPLACE_ME') {
    throw voiceCallbackError_('token_not_configured', 'VOICE_BOT_SHARED_TOKEN must be set before deployment');
  }

  if (!payload.token || payload.token !== VOICE_BOT_SHARED_TOKEN) {
    voiceBotLog_({
      event: 'voice_callback_rejected',
      reason: 'bad_token',
      rowNumber: valueOrNull_(payload.rowNumber)
    });

    throw voiceCallbackError_('unauthorized', 'Missing or invalid voice bot token');
  }

  const rowNumber = Number(payload.rowNumber);
  if (!Number.isInteger(rowNumber) || rowNumber < 2) {
    voiceBotLog_({
      event: 'voice_callback_rejected',
      reason: 'invalid_row_number',
      rowNumber: valueOrNull_(payload.rowNumber)
    });

    throw voiceCallbackError_('invalid_row_number', 'rowNumber must be a sheet row number greater than 1');
  }
}

function validateVoiceBotRow_(sheet, rowNumber) {
  const lastRow = sheet.getLastRow();

  if (rowNumber > lastRow) {
    voiceBotLog_({
      event: 'voice_callback_rejected',
      reason: 'row_out_of_bounds',
      rowNumber: rowNumber,
      lastRow: lastRow
    });

    throw voiceCallbackError_('row_out_of_bounds', 'rowNumber is outside the populated sheet range');
  }
}

function validateVoiceBotQueueConfig_() {
  if (VOICE_BOT_START_CALL_URL === 'REPLACE_ME') {
    throw voiceCallbackError_('start_call_url_not_configured', 'VOICE_BOT_START_CALL_URL must be set before queueing calls');
  }
}

// =============================================================================
// Helpers
// =============================================================================

function parseJsonPost_(e) {
  if (!e || !e.postData || !e.postData.contents) {
    throw voiceCallbackError_('missing_body', 'Missing POST body');
  }

  try {
    return JSON.parse(e.postData.contents);
  } catch (err) {
    throw voiceCallbackError_('invalid_json', 'Invalid JSON POST body');
  }
}

function parseJsonSafely_(text) {
  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch (err) {
    return null;
  }
}

function jsonOutput_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function voiceCallbackError_(code, message) {
  const err = new Error(message);
  err.code = code;
  return err;
}

function writeVoiceBotFieldIfPresent_(sheet, rowNumber, columnNumber, payload, payloadKey, fieldsWritten) {
  if (!Object.prototype.hasOwnProperty.call(payload, payloadKey)) {
    return;
  }

  const value = payload[payloadKey];
  if (value === undefined || value === null) {
    return;
  }

  sheet.getRange(rowNumber, columnNumber).setValue(value);
  fieldsWritten.push(columnToLetter_(columnNumber) + ':' + payloadKey);
}

function clearVoiceBotCellIfNeeded_(sheet, rowNumber, columnNumber, fieldsWritten, label) {
  const cell = sheet.getRange(rowNumber, columnNumber);
  if (cell.getValue() !== '') {
    cell.clearContent();
    fieldsWritten.push(columnToLetter_(columnNumber) + ':' + label);
  }
}

function normalizeCallAttemptNumber_(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 1 ? 2 : 1;
}

function normalizeString_(value) {
  if (value === undefined || value === null) {
    return '';
  }

  return String(value).trim();
}

function normalizeMarker_(value) {
  return normalizeString_(value).toLowerCase();
}

function normalizePhoneToE164_(value) {
  const text = normalizeString_(value);
  if (!text) {
    return '';
  }

  if (text.charAt(0) === '+' && /^\+[1-9]\d{6,14}$/.test(text)) {
    return text;
  }

  const digits = text.replace(/\D/g, '');
  if (digits.length === 10) {
    return '+1' + digits;
  }

  if (digits.length === 11 && digits.charAt(0) === '1') {
    return '+' + digits;
  }

  return '';
}

function buildVoiceBotListingAddress_(rowValues) {
  const street = normalizeString_(rowValues[VOICE_BOT_COL_LISTING_ADDRESS - 1]);
  const city = normalizeString_(rowValues[VOICE_BOT_COL_CITY - 1]);
  const state = normalizeString_(rowValues[VOICE_BOT_COL_STATE - 1]);

  return [street, city, state].filter(Boolean).join(', ');
}

function parseVoiceBotDate_(value) {
  if (!value) {
    return null;
  }

  if (Object.prototype.toString.call(value) === '[object Date]' && !isNaN(value.getTime())) {
    return value;
  }

  const text = normalizeString_(value);
  if (!text) {
    return null;
  }

  const parsed = new Date(text);
  if (!isNaN(parsed.getTime())) {
    return parsed;
  }

  return null;
}

function isRetryableVoiceBotResult_(callResult) {
  const normalized = normalizeString_(callResult).toLowerCase();

  return normalized === 'voicemail_left' || normalized === 'no_answer_first_attempt';
}

function isWithinBusinessHours_(date) {
  const day = Number(Utilities.formatDate(date, VOICE_BOT_TIMEZONE, 'u'));
  const hour = Number(Utilities.formatDate(date, VOICE_BOT_TIMEZONE, 'H'));

  return day >= 1 && day <= 5 && hour >= VOICE_BOT_BUSINESS_DAY_START_HOUR_ET && hour < VOICE_BOT_BUSINESS_DAY_END_HOUR_ET;
}

function addBusinessHours_(startDate, businessHoursToAdd) {
  var cursor = moveIntoBusinessWindow_(new Date(startDate.getTime()));
  var remainingHours = businessHoursToAdd;

  while (remainingHours > 0) {
    const endOfBusinessDay = new Date(cursor.getTime());
    endOfBusinessDay.setHours(VOICE_BOT_BUSINESS_DAY_END_HOUR_ET, 0, 0, 0);

    const availableHoursToday = (endOfBusinessDay.getTime() - cursor.getTime()) / 3600000;
    if (remainingHours <= availableHoursToday) {
      return new Date(cursor.getTime() + remainingHours * 3600000);
    }

    remainingHours -= availableHoursToday;
    cursor = nextBusinessStart_(cursor);
  }

  return cursor;
}

function moveIntoBusinessWindow_(date) {
  var cursor = new Date(date.getTime());

  while (true) {
    const day = Number(Utilities.formatDate(cursor, VOICE_BOT_TIMEZONE, 'u'));
    const hour = Number(Utilities.formatDate(cursor, VOICE_BOT_TIMEZONE, 'H'));

    if (day >= 6) {
      cursor = nextBusinessStart_(cursor);
      continue;
    }

    if (hour < VOICE_BOT_BUSINESS_DAY_START_HOUR_ET) {
      cursor.setHours(VOICE_BOT_BUSINESS_DAY_START_HOUR_ET, 0, 0, 0);
      return cursor;
    }

    if (hour >= VOICE_BOT_BUSINESS_DAY_END_HOUR_ET) {
      cursor = nextBusinessStart_(cursor);
      continue;
    }

    return cursor;
  }
}

function nextBusinessStart_(date) {
  var cursor = new Date(date.getTime());
  cursor.setHours(VOICE_BOT_BUSINESS_DAY_START_HOUR_ET, 0, 0, 0);
  cursor.setDate(cursor.getDate() + 1);

  while (true) {
    const day = Number(Utilities.formatDate(cursor, VOICE_BOT_TIMEZONE, 'u'));
    if (day >= 1 && day <= 5) {
      return cursor;
    }
    cursor.setDate(cursor.getDate() + 1);
  }
}

function formatVoiceBotDateEt_(date) {
  return Utilities.formatDate(date, VOICE_BOT_TIMEZONE, 'yyyy-MM-dd HH:mm:ss');
}

function columnToLetter_(columnNumber) {
  let letter = '';
  let temp = columnNumber;

  while (temp > 0) {
    const remainder = (temp - 1) % 26;
    letter = String.fromCharCode(65 + remainder) + letter;
    temp = Math.floor((temp - remainder - 1) / 26);
  }

  return letter;
}

function getVoiceBotSheet_() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = spreadsheet.getSheetByName(VOICE_BOT_SHEET_NAME);

  if (!sheet) {
    throw voiceCallbackError_('sheet_not_found', 'Sheet not found: ' + VOICE_BOT_SHEET_NAME);
  }

  return sheet;
}

function valueOrNull_(value) {
  return value === undefined || value === null || value === '' ? null : value;
}

function voiceBotLog_(obj) {
  Logger.log(JSON.stringify(obj));
}

/**
 * =============================================================================
 * Usage Notes
 * =============================================================================
 *
 * 1. Set the shared token:
 *    - In this file: VOICE_BOT_SHARED_TOKEN = 'your-shared-secret'
 *    - In crisp-voice-bot .env: GOOGLE_APPS_SCRIPT_TOKEN=your-shared-secret
 *
 * 2. Set the start-call URL:
 *    - In this file: VOICE_BOT_START_CALL_URL = 'https://your-ngrok-or-prod-url/start-call'
 *    - This should point to the crisp-voice-bot backend start-call route.
 *
 * 3. Deploy the Apps Script web app:
 *    - Open the bound Apps Script project.
 *    - Paste this file, or merge doPost(e) if the project already has one.
 *    - Deploy > Manage deployments > Edit or create Web app deployment.
 *    - Execute as: Me.
 *    - Access: Anyone with the link, or Anyone, depending on your workspace.
 *    - Copy the web app URL into GOOGLE_APPS_SCRIPT_WEBHOOK_URL.
 *
 * 4. Install the queue trigger:
 *    - Run installVoiceBotCallQueueTrigger() once in Apps Script.
 *    - Or run resetVoiceBotCallQueueTriggers() if you want to recreate it.
 *    - The trigger calls processVoiceBotCallQueue() every 15 minutes.
 *
 * 5. Observed live-sheet note:
 *    - The current tab is using column X as the live proxy for "follow-up text sent at".
 *    - If the bound script later stores that timestamp in a different column, update
 *      VOICE_BOT_COL_FOLLOWUP_SENT_AT_PROXY before relying on the queue runner.
 */
