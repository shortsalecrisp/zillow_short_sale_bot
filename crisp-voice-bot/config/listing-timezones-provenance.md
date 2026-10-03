# Listing Time Zone Lookup

This offline lookup estimates the listing's local time zone from its postal city
and state. It is not street-level geocoding or proof of a property's exact time
zone. No phone area code, paid API, or external prospect-data transmission is used.

## Sources and Licensing

- GeoNames, US postal data downloaded 2026-10-03:
  https://download.geonames.org/export/zip/US.zip
  Attribution: https://www.geonames.org/ ; CC BY 4.0:
  https://creativecommons.org/licenses/by/4.0/
  Coordinates are postal-place estimates, not ZIP boundary polygons. GeoNames
  provides these data without a completeness or accuracy guarantee.
- Evan Siroky's `geo-tz` 8.1.9, used only by the generator:
  https://github.com/evansiroky/node-geo-tz ; MIT license.
- Time-zone polygons supplied by `geo-tz` from timezone-boundary-builder:
  https://github.com/evansiroky/timezone-boundary-builder ; ODbL 1.0:
  https://opendatacommons.org/licenses/odbl/1-0/
  Polygon data incorporate OpenStreetMap contributors:
  https://www.openstreetmap.org/copyright

The generated lookup database is made available under ODbL 1.0, with the
GeoNames attribution above preserved. The application resolver is separate
from this database. Source SHA-256 and generator version are in JSON metadata.

## Conservative Resolution

All US/DC postal-city records in the source are processed, not a hand-maintained
list of selected cities. An accepted city must have agreement across every
known ZIP/place point with finite coordinates and eight bearing samples each
at 5 km and 15 km. At least one city point must have GeoNames accuracy >=4.
Lower-quality points are included in the same consensus and boundary check;
they cannot establish a city zone alone or contradict accurate points. Missing
coordinates do not invalidate otherwise supported consensus. A mixed-zone city,
boundary-adjacent city, or city without an accurate point is explicitly held.
Sea zones from perimeter samples are ignored; an offshore center is not accepted.

`geo-tz/now` uses IANA zones with the same present/future timekeeping rules.
For example, historical regional aliases may resolve to America/New_York while
preserving current Eastern DST behavior. Scheduling uses IANA rules, never a
fixed UTC offset. This product must not be used for historical timestamps.

An embedded ZIP can identify or cross-check a postal city, but never overrides
a held city with one ZIP centroid. Conflicting state, city, or ZIP evidence is
held. Unknown or insufficiently resolved cities may use a state fallback only
in the explicitly listed single-zone states. Split states and local-observance exceptions (including
Alabama, Alaska, Arizona, Nevada, and Oklahoma) have no state-only fallback.

Finite sampling cannot prove that every street or all of a ZIP's irregular
territory is inside a single time zone. Sparse postal coverage, incorrectly
estimated coordinates, same-named cities, reservation boundaries, and changed
time-zone rules remain limitations. Treat this as bounded postal-city consensus;
hold/confirm the actual listing location whenever contrary evidence is known.

## Rebuild

Download and retain the public US.zip source, extract US.txt, and install
`geo-tz@8.1.9` in a temporary build directory with `--ignore-scripts`. Then run:

```sh
node scripts/generate-listing-time-zones.cjs /path/to/US.txt /path/to/build/node_modules
```

The generator writes `src/config/us-city-timezones.json` and
`apps-script/listing-time-zone.gs`. The latter contains the exact same data and
transpiled resolver as TypeScript. Include it with `voice-bot-callback.gs` when
deploying Apps Script. Do not deploy the wrapper without this companion file.
Review the source date and updated boundaries before refreshing the artifact.
Run `npx tsx --test tests/listingTimeZone.test.ts` after generation.

## Build-Time Library Notice

The MIT License (MIT)

Copyright (c) 2016 Evan Siroky

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
