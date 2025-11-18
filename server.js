// server.js
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import axios from "axios";
import { z } from "zod";
import fs from "fs";
import path from "path";
import YAML from "yaml";
 
const configDir = path.join(process.cwd(), "config");
 
const metricsYamlText = fs.readFileSync(path.join(configDir, "metrics.yaml"), "utf8");
const metricsConfig = YAML.parse(metricsYamlText);
 
const fieldMappingText = fs.readFileSync(path.join(configDir, "field_mapping.json"), "utf8");
const fieldMapping = JSON.parse(fieldMappingText);
 
function getMetricConfig(metricId) {
  return metricsConfig.metrics.find(m => m.id === metricId);
}
 
function getFieldMapping() {
  return fieldMapping.concepts;
}
 
function getDefaults() {
  return metricsConfig.defaults;
}
// --------- METRIC HELPER FUNCTIONS ---------
// Return all HubSpot properties needed for a metric
function getHubSpotPropsForMetric(metricId) {
  const metric = getMetricConfig(metricId);
  if (!metric) throw new Error(`Unknown metricId: ${metricId}`);
  const concepts = getFieldMapping();
  const conceptNames = new Set([
    ...(metric.required_concepts || []),
    ...(metric.optional_concepts || [])
  ]);
  // Ensure date_field is included
  if (metric.record_selection?.date_field) {
    conceptNames.add(metric.record_selection.date_field);
  }
  const hubspotProps = new Set();
  for (const conceptName of conceptNames) {
    const def = concepts[conceptName];
    if (def?.hubspot_property) {
      hubspotProps.add(def.hubspot_property);
    }
  }
  return Array.from(hubspotProps);
}
// Build HubSpot filters from YAML selection + timeWindow
function buildHubSpotFiltersFromSelection(metricId, selectionFilters, timeWindow) {
  const metric = getMetricConfig(metricId);
  const concepts = getFieldMapping();
  const filters = [];
  // Time window filter
  const dateConceptName = metric.record_selection.date_field;
  const dateDef = concepts[dateConceptName];
  if (!dateDef) throw new Error(`Missing concept mapping for ${dateConceptName}`);
  const dateProp = dateDef.hubspot_property;
  if (timeWindow?.start) {
    filters.push({
      propertyName: dateProp,
      operator: "GTE",
      value: timeWindow.start
    });
  }
  if (timeWindow?.end) {
    filters.push({
      propertyName: dateProp,
      operator: "LTE",
      value: timeWindow.end
    });
  }
  // YAML filters
  for (const [key, value] of Object.entries(selectionFilters || {})) {
    const [conceptName, op] = key.split("_");
    const def = concepts[conceptName];
    if (!def) continue;
    const propertyName = def.hubspot_property;
    if (op === "in" && Array.isArray(value)) {
      filters.push({
        propertyName,
        operator: "IN",
        values: value
      });
    }
    else if (op === "exclude" && Array.isArray(value)) {
      for (const v of value) {
        if (v === "" || v == null) continue;
        filters.push({
          propertyName,
          operator: "NEQ",
          value: v
        });
      }
    }
    else if (op === "is_known" && value === true) {
      filters.push({
        propertyName,
        operator: "HAS_PROPERTY"
      });
    }
    else if (op === "not_zero" && value === true) {
      filters.push({
        propertyName,
        operator: "GT",
        value: 0
      });
    }
  }
  return filters;
}
// Build the full HubSpot deals search body
function buildHubSpotMetricQuery(metricId, selection, timeWindow) {
  const metric = getMetricConfig(metricId);
  const properties = getHubSpotPropsForMetric(metricId);
  const filters = buildHubSpotFiltersFromSelection(
    metricId,
    selection.filters || {},
    timeWindow
  );
  // Sort parsing: e.g. SQLDate_desc
  const sortKey = selection.sort || metric.record_selection.sort;
  const parts = sortKey.split("_");
  const directionRaw = parts.pop();
  const conceptName = parts.join("_");
  const concepts = getFieldMapping();
  const conceptDef = concepts[conceptName];
  let sort = null;
  if (conceptDef?.hubspot_property) {
    sort = {
      propertyName: conceptDef.hubspot_property,
      direction: directionRaw.toLowerCase() === "asc" ? "ASCENDING" : "DESCENDING"
    };
  }
  return {
    limit: selection.max_records,
    properties,
    filterGroups: [{ filters }],
    sorts: sort ? [sort] : []
  };
}
// Normalize HubSpot deals into concept-based objects
function normalizeDealsForMetric(rawDeals) {
  const concepts = getFieldMapping();
  const output = [];
  for (const deal of rawDeals || []) {
    const props = deal.properties || {};
    const normalized = { id: String(deal.id) };
    for (const [conceptName, def] of Object.entries(concepts)) {
      const value = props[def.hubspot_property];
      if (value == null) {
        normalized[conceptName] = null;
        continue;
      }
      if (def.type === "number") {
        const n = Number(value);
        normalized[conceptName] = Number.isNaN(n) ? null : n;
      }
      else if (def.type === "bool") {
        if (typeof value === "boolean") {
          normalized[conceptName] = value;
        } else if (typeof value === "string") {
          normalized[conceptName] = value.toLowerCase() === "true";
        } else {
          normalized[conceptName] = null;
        }
      }
      else if (def.type === "datetime") {
        const d = new Date(value);
        normalized[conceptName] = Number.isNaN(d.getTime()) ? null : d;
      }
      else {
        // enum or string
        normalized[conceptName] = value;
      }
    }
    output.push(normalized);
  }
  return output;
}
// Actually call HubSpot Search API (deals only)
async function fetchDealsForMetric(queryBody) {
  const rsp = await hsCall(() =>
    HS.post("/crm/v3/objects/deals/search", queryBody)
  );
  return rsp.data?.results || [];
}
// --------- END METRIC HELPER FUNCTIONS ---------
// --------- METRIC COMPUTATION FUNCTIONS ---------
function daysBetween(a, b) {
  if (!a || !b || !(a instanceof Date) || !(b instanceof Date)) return null;
  const ms = b.getTime() - a.getTime();
  return ms / (1000 * 60 * 60 * 24);
}
function groupCount(deals, groupBy) {
  const groups = {};
  for (const d of deals) {
    const key = d[groupBy] ?? "UNKNOWN";
    groups[key] = (groups[key] || 0) + 1;
  }
  return groups;
}
function groupSum(deals, groupBy, valueField) {
  const groups = {};
  for (const d of deals) {
    const key = d[groupBy] ?? "UNKNOWN";
    const v = d[valueField];
    if (typeof v === "number" && !Number.isNaN(v)) {
      groups[key] = (groups[key] || 0) + v;
    }
  }
  return groups;
}
function groupAvgFromArray(valuesByGroup) {
  const result = {};
  for (const [key, arr] of Object.entries(valuesByGroup)) {
    if (!Array.isArray(arr) || arr.length === 0) {
      result[key] = null;
    } else {
      const sum = arr.reduce((acc, v) => acc + v, 0);
      result[key] = sum / arr.length;
    }
  }
  return result;
}
function computeSqlCount(deals, groupBy) {
  const filtered = deals.filter(d =>
    d.Stage === "2-sales lead (Sales Pipeline)" &&
    d.DealType !== "Existing Customer" &&
    d.DealType &&
    d.OriginalSourceSoftco
  );
  if (!groupBy) {
    return { metricId: "sql-count", value: filtered.length };
  }
  return {
    metricId: "sql-count",
    groupBy,
    values: groupCount(filtered, groupBy)
  };
}
function computeSqlBySource(deals) {
  const filtered = deals.filter(d =>
    d.Stage === "2-sales lead (Sales Pipeline)" &&
    d.DealType !== "Existing Customer" &&
    d.DealType &&
    d.OriginalSourceSoftco
  );
  return {
    metricId: "sql-by-source",
    groupBy: "OriginalSourceSoftco",
    values: groupCount(filtered, "OriginalSourceSoftco")
  };
}
function computePipelineOpportunityCount(deals, groupBy) {
  const stages = new Set([
    "3-qualified (Sales Pipeline)",
    "4-demo (Sales Pipeline)",
    "5-in-context demo (Sales Pipeline)",
    "6-Quotation (Sales Pipeline)",
    "7-Negotiation (Sales Pipeline)",
    "8-Provisional Approval (Sales Pipeline)",
    "9-Awaiting Order (Sales Pipeline)"
  ]);
  const filtered = deals.filter(d =>
    stages.has(d.Stage) &&
    typeof d.ARRCompanyCurrency === "number" &&
    d.ARRCompanyCurrency !== 0 &&
    d.DealType !== "Existing Customer"
  );
  if (!groupBy) {
    return { metricId: "pipeline-opportunity-count", value: filtered.length };
  }
  return {
    metricId: "pipeline-opportunity-count",
    groupBy,
    values: groupCount(filtered, groupBy)
  };
}
function computePipelineValue(deals, groupBy) {
  const stages = new Set([
    "3-qualified (Sales Pipeline)",
    "4-demo (Sales Pipeline)",
    "5-in-context demo (Sales Pipeline)",
    "6-Quotation (Sales Pipeline)",
    "7-Negotiation (Sales Pipeline)",
    "8-Provisional Approval (Sales Pipeline)",
    "9-Awaiting Order (Sales Pipeline)"
  ]);
  const filtered = deals.filter(d =>
    stages.has(d.Stage) &&
    typeof d.ARRCompanyCurrency === "number" &&
    d.ARRCompanyCurrency !== 0 &&
    d.DealType !== "Existing Customer"
  );
  if (!groupBy) {
    const total = filtered.reduce(
      (acc, d) => acc + (d.ARRCompanyCurrency || 0),
      0
    );
    return { metricId: "pipeline-value", value: total };
  }
  return {
    metricId: "pipeline-value",
    groupBy,
    values: groupSum(filtered, groupBy, "ARRCompanyCurrency")
  };
}
function computeOpenDealCount(deals, groupBy) {
  const filtered = deals.filter(d => d.IsWon === false && d.IsLost === false);
  if (!groupBy) {
    return { metricId: "open-deal-count", value: filtered.length };
  }
  return {
    metricId: "open-deal-count",
    groupBy,
    values: groupCount(filtered, groupBy)
  };
}
function computeWinRate(deals, groupBy) {
  if (!groupBy) {
    const won = deals.filter(d => d.IsWon === true).length;
    const lost = deals.filter(d => d.IsLost === true).length;
    const denom = won + lost;
    const rate = denom === 0 ? null : won / denom;
    return {
      metricId: "win-rate",
      value: rate,
      won,
      lost
    };
  }
  const groups = {};
  for (const d of deals) {
    const key = d[groupBy] ?? "UNKNOWN";
    if (!groups[key]) {
      groups[key] = { won: 0, lost: 0 };
    }
    if (d.IsWon === true) groups[key].won += 1;
    if (d.IsLost === true) groups[key].lost += 1;
  }
  const result = {};
  for (const [key, { won, lost }] of Object.entries(groups)) {
    const denom = won + lost;
    result[key] = denom === 0 ? null : won / denom;
  }
  return {
    metricId: "win-rate",
    groupBy,
    values: result
  };
}
function computeAverageDealSize(deals, groupBy) {
  const wonDeals = deals.filter(
    d => d.IsWon === true && typeof d.ARRCompanyCurrency === "number"
  );
  if (!groupBy) {
    if (wonDeals.length === 0) {
      return { metricId: "average-deal-size", value: null, count: 0 };
    }
    const total = wonDeals.reduce(
      (acc, d) => acc + (d.ARRCompanyCurrency || 0),
      0
    );
    const avg = total / wonDeals.length;
    return {
      metricId: "average-deal-size",
      value: avg,
      count: wonDeals.length
    };
  }
  const valuesByGroup = {};
  for (const d of wonDeals) {
    const key = d[groupBy] ?? "UNKNOWN";
    if (!valuesByGroup[key]) valuesByGroup[key] = [];
    valuesByGroup[key].push(d.ARRCompanyCurrency || 0);
  }
  return {
    metricId: "average-deal-size",
    groupBy,
    values: groupAvgFromArray(valuesByGroup)
  };
}
function computeSalesCycleDays(deals, groupBy) {
  const wonDeals = deals.filter(
    d => d.IsWon === true && d.CreatedDate && d.CloseDate
  );
  if (!groupBy) {
    const durations = [];
    for (const d of wonDeals) {
      const days = daysBetween(d.CreatedDate, d.CloseDate);
      if (days != null) durations.push(days);
    }
    if (durations.length === 0) {
      return { metricId: "sales-cycle-days", value: null, count: 0 };
    }
    const total = durations.reduce((acc, v) => acc + v, 0);
    const avg = total / durations.length;
    return {
      metricId: "sales-cycle-days",
      value: avg,
      count: durations.length
    };
  }
  const daysByGroup = {};
  for (const d of wonDeals) {
    const key = d[groupBy] ?? "UNKNOWN";
    const days = daysBetween(d.CreatedDate, d.CloseDate);
    if (days == null) continue;
    if (!daysByGroup[key]) daysByGroup[key] = [];
    daysByGroup[key].push(days);
  }
  return {
    metricId: "sales-cycle-days",
    groupBy,
    values: groupAvgFromArray(daysByGroup)
  };
}
// Dispatcher: choose which metric computation to run
function computeMetric(metricId, deals, groupBy) {
  switch (metricId) {
    case "sql-count":
      return computeSqlCount(deals, groupBy);
    case "sql-by-source":
      return computeSqlBySource(deals);
    case "pipeline-opportunity-count":
      return computePipelineOpportunityCount(deals, groupBy);
    case "pipeline-value":
      return computePipelineValue(deals, groupBy);
    case "open-deal-count":
      return computeOpenDealCount(deals, groupBy);
    case "win-rate":
      return computeWinRate(deals, groupBy);
    case "average-deal-size":
      return computeAverageDealSize(deals, groupBy);
    case "sales-cycle-days":
      return computeSalesCycleDays(deals, groupBy);
    default:
      throw new Error(`Unsupported metricId: ${metricId}`);
  }
}
// --------- END METRIC COMPUTATION FUNCTIONS ---------

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

const MetricRequestSchema = z.object({
  metricId: z.string(),
  timeWindow: z.object({
    start: z.string(), // ISO date-time
    end: z.string()    // ISO date-time
  }),
  extraFilters: z.record(z.any()).optional(),
  groupBy: z.string().optional() // e.g. "Pipeline", "OwnerId", "OriginalSourceSoftco"
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

// Metric endpoint – phase 1: fetch and return normalized deals
app.post("/api/hubspot-metric", async (req, res) => {
  try {
    const parsed = MetricRequestSchema.safeParse(req.body || {});
    if (!parsed.success) {
      return res.status(400).json({
        error: "BAD_REQUEST",
        details: parsed.error.issues
      });
    }
    const { metricId, timeWindow, extraFilters, groupBy } = parsed.data;
    const metric = getMetricConfig(metricId);
    if (!metric) {
      return res.status(404).json({
        error: "UNKNOWN_METRIC",
        metricId
      });
    }
    const defaults = getDefaults().selection;
    const selection = {
      date_field: metric.record_selection.date_field,
      max_records: metric.record_selection.max_records ?? defaults.max_records,
      sort: metric.record_selection.sort ?? defaults.sort,
      filters: {
        ...defaults.filters,
        ...(metric.record_selection.filters || {}),
        ...(extraFilters || {})
      }
    };
    // Build HubSpot search body for this metric
    const searchBody = buildHubSpotMetricQuery(metric.id, selection, timeWindow);
    // Call HubSpot and fetch deals
    const rawDeals = await fetchDealsForMetric(searchBody);
    // Normalize into concept-based objects
    const deals = normalizeDealsForMetric(rawDeals);
    const metricResult = computeMetric(metric.id, deals, groupBy);

  return res.json({
      ok: true,
      metricId: metric.id,
      name: metric.name,
      description: metric.description,
      timeWindow,
      selection,
      groupBy: groupBy || null,
      deal_count: deals.length,
      result: metricResult,
      // You can remove `deals` later if you don't want to return raw data,
      // but it's useful for debugging now:
      deals,
      status: "METRIC_COMPUTED"
    });
  } catch (err) {
    console.error("Error in /api/hubspot-metric:", err?.message || err);
    const status = err?.response?.status || 500;
    return res.status(status).json({
      error: "METRIC_ENDPOINT_FAILED",
      message: err?.message || "Unknown error"
    });
  }
});

// Export for Vercel; also run locally
export default app;
if (!process.env.VERCEL) {
  const port = process.env.PORT || 3000;
  app.listen(port, () => console.log(`✅ Local server on http://localhost:${port}`));
}
