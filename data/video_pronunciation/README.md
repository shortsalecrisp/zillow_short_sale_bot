# Versioned Pronunciation Data

This directory contains a deterministic, local-only subset of CMUdict. It is
used only by the explicitly selected v4 pronunciation policy. No model,
generation API, remote lookup or paid service is used.

Source: https://github.com/cmusphinx/cmudict/tree/74790861f652b15e4ac49015a90074ad62a27690

The builder verifies the original dictionary and license SHA-256 hashes,
preserves full stressed phonemes, and retains only ASCII grammar-eligible words
with exactly one distinct pronunciation. Ambiguous and missing words remain
unresolved. This is lexical data, not independent waveform or cadence proof.
Existing exact-name and explicitly reviewed matches are preserved separately.

Build from the pinned original dictionary and its license:

```sh
python -m scripts.build_video_pronunciation_lexicon --source /path/to/cmudict.dict --license /path/to/LICENSE
```

Add `--check` to compare without writing. The compressed JSON, manifest,
Python identity constants and lazy Apps Script asset are generated together.
Both validators hash-check the same data before use. Any change requires a new
reviewed policy identity; never modify a dictionary behind an existing proof.

CMUdict is English-focused and may omit names or pronunciation variants. Do not
use this layer to invent pronunciation, select an ambiguous variant until it
matches, approve uncertain audio, or certify natural cadence.

## Integrated Policy

The selectable policy is `spoken-greeting-tiny-en-v4`, paired only with
`planned-aac-lineage-v3`. This implementation does not change the selected live
method, environment, generator, recognition thresholds, acoustic limits,
recipient spelling, reminder schedule or budgets. Old jobs retain their frozen
method and proof identity; they must never be upgraded, retried or reopened.

The dictionary is a post-recognition comparison layer, not another speech
model. New v4 generation and speech-proof issuance require intact pinned data
before payment or recognition. Readers of already-issued exact-name or
reviewed-group proofs do not need dictionary coverage. General-equivalence
proofs always require the pinned dictionary. Unknown, ambiguous or different
sequences remain unresolved; no fuzzy spelling, guessed pronunciation or
best-of-several decoding is allowed.

Offline coverage is in `tests/test_video_general_pronunciation_policy.py`,
`tests/test_video_general_pronunciation_safety.py`,
`tests/test_video_pronunciation_lexicon.py` and
`tests/video_general_pronunciation_policy.test.mjs`. Recognition and provider
responses in these tests are mocked. Passing them is not evidence of a newly
generated personalized video or a sent SMS.

Before activation, independently review retained actual audio and cadence,
validate native Apps Script delivery/cold-start of the generated asset, and
reconcile the current live revisions. Both proof readers must support the exact
new tuple before selecting it for future jobs. Keep the older monitoring
cohorts separate; live success still requires final-media proof and an exact
handset sent receipt. No paid tests or activation are performed by this module.
