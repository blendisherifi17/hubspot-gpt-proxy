// server.js
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import axios from "axios";
import { z } from "zod";

dotenv.config();

const { HUBSPOT_TOKEN, API_KEY } = process.env;
if (!HUBSPOT_TOKEN) console.warn("⚠️  Missing HUBSPOT_TOKEN in env");
if (!API_KEY) console.warn("⚠️  Missing API_KEY in env");

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

// API key auth
app.use((req, res, next) => {
  const key = req.headers["x-api-key"];
  if (!API_KEY || key !== API_KEY) {
    return res.status(401).json({ error: "UNAUTHORIZED" });
  }
  next();
});

// Health check
app.get("/health", (req, res) => res.json({ ok: true, ts: new Date().toISOString() }));

// HubSpot client
const HS = axios.create({
  baseURL: "https://api.hubapi.com",
  headers: { Authorization: `Bearer ${HUBSPOT_TOKEN}` },
  timeout: 30000
});

// Retry minimal on 429/5xx
async function hsCall(cb, attempts = 3) {
  let last;
  for (let i = 0; i < attempts; i++) {
    try { return await cb(); } catch (e) {
      last = e;
      const s = e?.response?.status;
      if (s === 429 || (s >= 500 && s < 600)) {
        await new Promise(r => setTimeout(r, 300 * (i + 1)));
        continue;
      }
      throw e;
    }
  }
  throw last;
}

// Default property profiles
const PROFILES = {
  deals: [
    "dealname","amount","hs_currency","pipeline","dealstage",
    "hubspot_owner_id","last_activity_date","createdate","hs_lastmodifieddate","closedate"
  ],
  contacts: ["email","firstname","lastname","createdate","hs_lastmodifieddate"],
  companies: ["name","domain","createdate","hs_lastmodifieddate"]
};

// Input validation
const QuerySchema = z.object({
  object: z.enum(["deals","contacts","companies"]).default("deals"),
  limit: z.number().int().min(1).max(100).default(25),
  after: z.string().optional(),
  created_after: z.string().optional(),
  properties: z.array(z.string()).optional(),
  include_owner: z.boolean().optional().default(true)
});

// Owners enrichment
async function fetchOwners() {
  const rsp = await hsCall(() => HS.get("/crm/v3/owners", { params: { limit: 500 } }));
  const byId = {};
  for (const o of rsp.data?.results ?? []) {
    const id = String(o.id);
    const name = [o.firstName, o.lastName].filter(Boolean).join(" ") || o.email || id;
    byId[id] = { id, name, email: o.email ?? null, active: !!o.active };
  }
  return byId;
}

function normalize(obj, object, ownersById) {
  const p = obj.properties || {};
  if (object === "deals") {
    const owner = p.hubspot_owner_id && ownersById ? ownersById[String(p.hubspot_owner_id)] : null;
    return {
      id: String(obj.id),
      name: p.dealname ?? null,
      amount: p.amount ? Number(p.amount) : null,
      currency: p.hs_currency ?? null,
      pipeline: p.pipeline ?? null,
      stage: p.dealstage ?? null,
      close_date: p.closedate ?? null,
      owner_id: p.hubspot_owner_id ?? null,
      owner_name: owner?.name ?? null,
      last_activity_date: p.last_activity_date ?? null,
      created_at: obj.createdAt,
      updated_at: obj.updatedAt,
      _props: p
    };
  }
  if (object === "contacts") {
    return {
      id: String(obj.id),
      email: p.email ?? null,
      first_name: p.firstname ?? null,
      last_name: p.lastname ?? null,
      created_at: obj.createdAt,
      updated_at: obj.updatedAt,
      _props: p
    };
  }
  if (object === "companies") {
    return {
      id: String(obj.id),
      name: p.name ?? null,
      domain: p.domain ?? null,
      created_at: obj.createdAt,
      updated_at: obj.updatedAt,
      _props: p
    };
  }
  return { id: String(obj.id), created_at: obj.createdAt, updated_at: obj.updatedAt, _props: p };
}

// Main endpoint
app.post("/query", async (req, res) => {
  const parsed = QuerySchema.safeParse(req.body || {});
  if (!parsed.success) return res.status(400).json({ error: "BAD_REQUEST", details: parsed.error.issues });

  const { object, limit, after, created_after, properties, include_owner } = parsed.data;
  const props = (properties?.length ? properties : PROFILES[object]).join(",");

  try {
    let ownersById = null;
    if (object === "deals" && include_owner) ownersById = await fetchOwners().catch(() => ({}));

    let data;
    if (object === "deals" && created_after) {
      const iso = new Date(created_after).toISOString();
      data = await hsCall(() => HS.post(`/crm/v3/objects/deals/search`, {
        limit,
        properties: props.split(","),
        sorts: [{ propertyName: "createdate", direction: "DESCENDING" }],
        filterGroups: [{ filters: [{ propertyName: "createdate", operator: "GTE", value: iso }] }]
      })).then(r => r.data);
    } else {
      data = await hsCall(() => HS.get(`/crm/v3/objects/${object}`, {
        params: { limit, after, properties: props }
      })).then(r => r.data);
    }

    const items = (data.results || []).map(r => normalize(r, object, ownersById));
    const next_cursor = data.paging?.next?.after ?? null;

    res.json({ object, count: items.length, next_cursor, items });
  } catch (e) {
    const status = e.response?.status || 500;
    res.status(status).json({ error: "UPSTREAM_ERROR", details: e.response?.data || e.message });
  }
});

// Export for Vercel; also run locally
export default app;
if (!process.env.VERCEL) {
  const port = process.env.PORT || 3000;
  app.listen(port, () => console.log(`✅ Local server on http://localhost:${port}`));
}
