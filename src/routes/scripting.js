import express from "express";
import axios from "axios";
import fs from "node:fs/promises";
import path from "node:path";

const router = express.Router();

const STRAVA_ACTIVITIES_URL = "https://www.strava.com/api/v3/athlete/activities";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function getBearerToken(req) {
  const header = req.get("authorization") || req.get("Authorization");
  if (header && /^Bearer\s+/i.test(header)) {
    return header.replace(/^Bearer\s+/i, "").trim();
  }
  return (process.env.STRAVA_ACCESS_TOKEN || "").trim();
}

router.get("/", (req, res) => {
  res.json({
    ok: true,
    endpoints: {
      extract_all: {
        method: "POST",
        path: "/scripting/strava/activities/extract-all",
        body: {
          name: "optional prefix (e.g. test)",
          rpm: 15,
          per_page: 200,
          start_page: 1,
          max_pages: 0,
        },
        auth: "STRAVA_ACCESS_TOKEN in .env or Authorization: Bearer <token>",
      },
    },
  });
});

router.post("/strava/activities/extract-all", async (req, res, next) => {
  try {
    const token = getBearerToken(req);
    if (!token) {
      return res.status(400).json({
        error:
          "Missing Strava token. Set STRAVA_ACCESS_TOKEN in .env or send Authorization: Bearer <token>.",
      });
    }

    const rpm = Number(req.body?.rpm ?? 15);
    const perPage = Number(req.body?.per_page ?? 200);
    const startPage = Number(req.body?.start_page ?? 1);
    const maxPages = Number(req.body?.max_pages ?? 0);
    const name = String(req.body?.name ?? "").trim();

    if (!Number.isFinite(rpm) || rpm < 1) {
      return res.status(400).json({ error: "`rpm` must be >= 1" });
    }
    if (!Number.isFinite(perPage) || perPage < 1 || perPage > 200) {
      return res.status(400).json({ error: "`per_page` must be 1..200" });
    }
    if (!Number.isFinite(startPage) || startPage < 1) {
      return res.status(400).json({ error: "`start_page` must be >= 1" });
    }
    if (!Number.isFinite(maxPages) || maxPages < 0) {
      return res.status(400).json({ error: "`max_pages` must be >= 0" });
    }

    const baseDir = path.resolve(process.cwd(), "data-scripting", "strava");
    await fs.mkdir(baseDir, { recursive: true });

    const minIntervalMs = Math.ceil((60_000 / rpm) * 1.0);

    let page = startPage;
    let fetchedPages = 0;
    let lastRequestAt = 0;

    while (true) {
      if (maxPages && fetchedPages >= maxPages) break;

      const now = Date.now();
      const waitMs = lastRequestAt ? Math.max(0, lastRequestAt + minIntervalMs - now) : 0;
      if (waitMs) await sleep(waitMs);

      lastRequestAt = Date.now();

      const response = await axios.get(STRAVA_ACTIVITIES_URL, {
        headers: { Authorization: `Bearer ${token}` },
        params: { page, per_page: perPage },
        validateStatus: () => true,
      });

      if (response.status === 401) {
        return res.status(401).json({ error: "401 Unauthorized (bad STRAVA_ACCESS_TOKEN)" });
      }

      if (response.status === 429) {
        const retryAfter = response.headers?.["retry-after"];
        const retryMs = retryAfter && String(retryAfter).match(/^\d+$/) ? Number(retryAfter) * 1000 : 10_000;
        await sleep(retryMs);
        continue;
      }

      if (response.status < 200 || response.status >= 300) {
        return res.status(502).json({
          error: `Strava API error (HTTP ${response.status})`,
          details: response.data,
        });
      }

      if (!Array.isArray(response.data)) {
        return res.status(502).json({
          error: "Unexpected Strava response (expected JSON array)",
          details: response.data,
        });
      }

      if (response.data.length === 0) break;

      const filename = name ? `${name}_page_${page}.json` : `page_${page}.json`;
      const outPath = path.join(baseDir, filename);
      await fs.writeFile(outPath, JSON.stringify(response.data, null, 2), "utf-8");

      page += 1;
      fetchedPages += 1;
    }

    const meta = {
      name: name || null,
      per_page: perPage,
      start_page: startPage,
      fetched_pages: fetchedPages,
      generated_at: new Date().toISOString(),
    };
    await fs.writeFile(path.join(baseDir, "meta.json"), JSON.stringify(meta, null, 2), "utf-8");

    return res.json({
      ok: true,
      output_dir: baseDir,
      ...meta,
    });
  } catch (err) {
    return next(err);
  }
});

export default router;
