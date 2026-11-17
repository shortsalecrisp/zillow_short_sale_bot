# Operational workbook recovery

The owner approved a server-only migration on November 17 after Google locked
the original oversized workbook read-only. The original is frozen historical
evidence, not the live source after the verified coordinated cutover.

- Replacement: `1rbffUky_CpaC0O9HxtChBlIet4BYB1Tu1ul1j8R_pSE`.
- Original: `12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70`.
- Keep all row numbers, stable identities, literal receipts and safety controls.
- Tasker URL/actions/leases/receipts and phone configuration do not change.
- Runtime `GSHEET_ID` and the existing Apps project must select the same copy.
- The isolated Apps voice scheduler must use `getSmsSpreadsheet_()` too.
- Preserve existing trigger identities, script properties and permissions.
- The diagnostic tab is header-only in the replacement. Its complete original
  values archive is `ops/sms-debug-archive-20261117T2140Z/manifest.json` in the
  existing private video-followup storage prefix.
- Historical `Verifier Closeout Proof` artifacts are also kept in the frozen
  original and a complete hash-verified private archive under
  `ops/workbook-recovery-20261117/proof-history/manifest.json`. New native proof
  rows hold a small generation/hash-bound private-storage locator instead of
  repeating large compressed snapshots. The exact full artifact, phase, run,
  automation and raw digest must read back before acceptance. Old run lookups
  fall back read-only to the original workbook, never mint a new episode.
- Never blindly retry uncertain messages, reopen jobs, clear holds, restore an
  old workbook snapshot or duplicate outreach to recover from storage failure.
- Do not restart the paused video-review automation.

Exact copy/hash checks, source/permission manifests and cutover receipts are
private evidence in `sms-workbook-migration-20261117` beside the incident report.
Deploy health and writable storage alone do not prove an SMS was actually sent.
