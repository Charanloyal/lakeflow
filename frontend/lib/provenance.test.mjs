// Unit tests for the recorded-demo provenance rules. Run: node --test lib/provenance.test.mjs
// (Node >= 22.18 strips the TypeScript types of the imported module; no build or browser needed.)
import assert from "node:assert/strict";
import { test } from "node:test";

import { DEFAULT_STALE_AFTER_DAYS, describeAge, parseSnapshotMeta, staleAfterDays } from "./provenance.ts";

const NOW = Date.parse("2026-10-09T12:00:00Z");
const VALID = {
  sha: "29847ea8ea034b4908cb232cecc6d3e4763331cf",
  captured_at: "2026-10-08T12:00:00Z",
  run_url: "https://github.com/example-owner/lakeflow/actions/runs/1",
  repo_url: "https://github.com/example-owner/lakeflow",
  codespaces_url: "https://codespaces.new/example-owner/lakeflow?quickstart=1",
  mode: "recorded",
};
const parse = (doc, staleAfter) => parseSnapshotMeta(JSON.stringify(doc), NOW, staleAfter);

test("a valid, recent recording is ok and reports its age", () => {
  const result = parse(VALID);
  assert.equal(result.status, "ok");
  assert.equal(result.meta.sha, VALID.sha);
  assert.equal(result.ageDays, 1);
  assert.deepEqual(result.problems, []);
});

test("recordings made before the mode field existed are still accepted", () => {
  const { mode: _mode, ...legacy } = VALID;
  assert.equal(parse(legacy).status, "ok");
});

test("an absent meta.json is reported as missing, not hidden", () => {
  const result = parseSnapshotMeta(null, NOW);
  assert.equal(result.status, "missing");
  assert.equal(result.meta, null);
  assert.match(result.problems[0], /not found/);
});

test("unparseable or non-object meta.json is malformed", () => {
  for (const text of ["{not json", "[]", "null", '"text"']) {
    assert.equal(parseSnapshotMeta(text, NOW).status, "malformed", text);
  }
});

test("missing or invalid fields are malformed and never yield partial metadata", () => {
  const cases = [
    [{ ...VALID, sha: undefined }, /sha/],
    [{ ...VALID, sha: "not-a-sha" }, /sha/],
    [{ ...VALID, captured_at: "yesterday" }, /captured_at/],
    [{ ...VALID, captured_at: "2026-10-12T12:00:00Z" }, /future/],
    [{ ...VALID, run_url: "javascript:alert(1)" }, /run_url/],
    [{ ...VALID, repo_url: "http://github.com/example-owner/lakeflow" }, /repo_url/],
    [{ ...VALID, codespaces_url: 42 }, /codespaces_url/],
    [{ ...VALID, mode: "live" }, /mode/],
  ];
  for (const [doc, problem] of cases) {
    const result = parse(doc);
    assert.equal(result.status, "malformed", JSON.stringify(doc));
    assert.equal(result.meta, null);
    assert.match(result.problems.join("; "), problem);
  }
});

test("staleness uses the configured threshold", () => {
  const old = { ...VALID, captured_at: "2026-08-01T12:00:00Z" };
  assert.equal(parse(old).status, "stale");
  assert.equal(parse(old, 365).status, "ok");
  assert.equal(parse(VALID, 0.5).status, "stale");
});

test("the threshold falls back to the documented default for unusable values", () => {
  assert.equal(staleAfterDays(undefined), DEFAULT_STALE_AFTER_DAYS);
  assert.equal(staleAfterDays(""), DEFAULT_STALE_AFTER_DAYS);
  assert.equal(staleAfterDays("-3"), DEFAULT_STALE_AFTER_DAYS);
  assert.equal(staleAfterDays("abc"), DEFAULT_STALE_AFTER_DAYS);
  assert.equal(staleAfterDays("7"), 7);
});

test("ages are described in words", () => {
  assert.equal(describeAge(0.01), "less than an hour ago");
  assert.equal(describeAge(0.5), "12 h ago");
  assert.equal(describeAge(1.2), "1 day ago");
  assert.equal(describeAge(40), "40 days ago");
});
