import * as fs from "node:fs";
import * as path from "node:path";

import { expect, test, type Page } from "@playwright/test";

const ADMIN = {
  user: process.env.LAKEFLOW_ADMIN_USER ?? "admin",
  password: process.env.LAKEFLOW_ADMIN_PASSWORD ?? "",
};
const VIEWER = {
  user: process.env.LAKEFLOW_VIEWER_USER ?? "viewer",
  password: process.env.LAKEFLOW_VIEWER_PASSWORD ?? "",
};
const SHOTS = process.env.LAKEFLOW_SCREENSHOT_DIR ?? "test-results/screenshots";
// When set, every successful GET /api/* JSON response is saved: that is the data of the recorded demo site.
const RECORD_DIR = process.env.LAKEFLOW_RECORD_SNAPSHOT_DIR;
const recorded = new Map<string, unknown>();

test.beforeEach(async ({ page }) => {
  if (!RECORD_DIR) return;
  // Intercept instead of listening: the body is captured before the page sees it, so navigation cannot lose it.
  await page.route("**/api/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    if (request.method() !== "GET" || url.pathname.endsWith("/live")) return route.fallback();
    const response = await route.fetch();
    if (
      response.status() === 200 &&
      (response.headers()["content-type"] ?? "").includes("application/json")
    ) {
      try {
        const body = await response.json();
        recorded.set(url.pathname + url.search, body);
        recorded.set(url.pathname, body);
      } catch {
        // not JSON after all; pass it through unrecorded
      }
    }
    await route.fulfill({ response });
  });
});

test.afterAll(() => {
  if (!RECORD_DIR || recorded.size === 0) return;
  fs.mkdirSync(RECORD_DIR, { recursive: true });
  const manifest: Record<string, string> = {};
  const files = new Map<unknown, string>();
  for (const [key, body] of recorded) {
    let file = files.get(body);
    if (!file) {
      file = `r${String(files.size).padStart(4, "0")}.json`;
      files.set(body, file);
      fs.writeFileSync(path.join(RECORD_DIR, file), JSON.stringify(body));
    }
    manifest[key] = file;
  }
  fs.writeFileSync(path.join(RECORD_DIR, "manifest.json"), JSON.stringify(manifest, null, 2));
});

async function login(page: Page, who = ADMIN) {
  await page.goto("/login/");
  await page.getByLabel("Username").fill(who.user);
  await page.getByLabel("Password").fill(who.password);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
}

test("unauthenticated users are sent to the login page", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveURL(/\/login\/?$/);
  await expect(page.getByRole("heading", { name: "LakeFlow control plane" })).toBeVisible();
});

test("overview metrics carry a timestamp and a source", async ({ page }) => {
  await login(page);
  const card = page.getByTestId("metric-consumer_lag");
  await expect(card).toBeVisible();
  await expect(card).toContainText("as of");
  await expect(card).toContainText("Spark checkpoint");
  await expect(page.getByText("Local demo environment")).toBeVisible();
  await page.screenshot({ path: `${SHOTS}/overview.png`, fullPage: true });
});

test("keyboard users can skip to content and navigate", async ({ page }) => {
  await login(page);
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: "Skip to content" })).toBeFocused();
  await page.getByRole("link", { name: "Event Explorer" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("heading", { name: "Event explorer" })).toBeVisible();
});

test("guided demo traces an order through every stage", async ({ page }) => {
  test.setTimeout(600_000); // create, update, delete and verify each wait on the real pipeline
  await login(page);
  await page.getByLabel("include Spark crash/recovery").uncheck();
  await page.getByTestId("guided-demo-start").click();
  await expect(page.getByTestId("stage-trino")).toContainText("done", { timeout: 180_000 });
  await page.screenshot({ path: `${SHOTS}/guided-demo-trace.png`, fullPage: true });
  const verify = page.getByTestId("demo-step-verify").locator(".badge");
  await expect(verify).toHaveText(/^(done|error)$/, { timeout: 480_000 });
  await expect(page.getByTestId("demo-log")).toContainText("tombstone visible in Trino");
  await expect(verify).toHaveText("done");
});

test("event explorer opens a record-level trace", async ({ page }) => {
  await login(page);
  await page.getByRole("link", { name: "Event Explorer" }).click();
  const trace = page.getByRole("button", { name: /^Trace / }).first();
  await expect(trace).toBeVisible({ timeout: 60_000 });
  await trace.click();
  await expect(page.getByTestId("stage-kafka")).toContainText("done");
  await expect(page.getByText("Final state (Trino) vs source (PostgreSQL)")).toBeVisible();
  await page.screenshot({ path: `${SHOTS}/event-trace.png`, fullPage: true });
});

test("live pipeline shows every stage with status", async ({ page }) => {
  await login(page);
  await page.getByRole("link", { name: "Live Pipeline" }).click();
  for (const node of ["postgres", "debezium", "kafka", "spark", "iceberg", "trino"]) {
    await expect(page.getByTestId(`node-${node}`)).toBeVisible();
  }
  await page.screenshot({ path: `${SHOTS}/pipeline.png`, fullPage: true });
});

test("screenshots of the remaining pages", async ({ page }) => {
  await login(page);
  for (const [name, heading] of [
    ["quality", "Data quality"],
    ["lineage", "Lineage"],
    ["benchmarks", "Benchmarks"],
    ["recovery", "Recovery lab"],
    ["architecture", "Architecture and decisions"],
  ]) {
    await page.goto(`/${name}/`);
    await expect(page.getByRole("heading", { name: heading, exact: true })).toBeVisible();
    await page.waitForTimeout(3000); // let the page's data requests finish (screenshot + recorded demo)
    await page.screenshot({ path: `${SHOTS}/${name}.png`, fullPage: true });
  }
});

test("dependency outage renders an explicit error state", async ({ page }) => {
  await login(page);
  await page.route("**/api/metrics/overview", (route) =>
    route.fulfill({
      status: 503,
      contentType: "application/json",
      body: JSON.stringify({ detail: "Trino unavailable: test" }),
    }),
  );
  await page.reload();
  await expect(page.getByText("Dependency unavailable: Trino unavailable: test")).toBeVisible();
});

test("viewer role cannot run recovery actions", async ({ page }) => {
  await login(page, VIEWER);
  await page.getByRole("link", { name: "Recovery Lab" }).click();
  await expect(page.getByTestId("action-crash_now")).toBeDisabled();
});
