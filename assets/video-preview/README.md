# Video Preview Still

`yoni-opening-v1.jpg` is an offline extraction of the first frame of the approved
recording, resized to 960 x 540. It is a presentation-only link preview and player
cover, not a replacement video or proof of any recipient's generated media.

Approved source SHA-256:
`01efd28101e8b016d51d571cc87d38d13ea623389663bc7a88cf26cce39bd60b`

Preview SHA-256:
`38807e8294d09a059bccbbbfe547910381c9b60f78c57a53f8129ebe47cd04c5`

Extraction: FFmpeg 7.1, `-map 0:v:0 -frames:v 1 -vf scale=960:540 -q:v 2`.
The player uses `/poster.jpg?preview=opening-v1`; the unqualified `/poster.jpg`
continues to serve each job's original immutable poster.

Keep existing per-job media, posters, captions and immutable generation evidence
unchanged. Publication status gates still apply to the opening-preview route.
