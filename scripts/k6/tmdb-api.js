import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const ENDPOINTS = Object.freeze({
  metadata: Object.freeze({
    key: 'metadata',
    path: '/movies/900000001',
    sloMs: 50,
  }),
  list: Object.freeze({
    key: 'list',
    path: '/movies?limit=20',
    sloMs: 50,
  }),
  search: Object.freeze({
    key: 'search',
    path: '/search?q=Caf%C3%A9&limit=20',
    sloMs: 150,
  }),
  filter: Object.freeze({
    key: 'filter',
    path:
      '/movies?genreId=900000002&language=en&runtimeMin=40&runtimeMax=120&personId=900000002&companyId=900000002&limit=20',
    sloMs: 150,
  }),
});

function positiveInteger(value, fallback, name) {
  if (value === undefined || value === '') {
    return fallback;
  }

  if (!/^[1-9][0-9]*$/.test(value)) {
    throw new Error(`${name} must be a positive integer.`);
  }

  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${name} is outside the safe integer range.`);
  }
  return parsed;
}

function normalizedBaseUrl(value) {
  if (value === undefined || value === '') {
    throw new Error('TMDB_K6_BASE_URL is required.');
  }

  const candidate = String(value).trim();
  if (
    !/^https?:\/\/[^\s/?#]+(?:\/[^?#]*)?$/.test(candidate) ||
    candidate.includes('@')
  ) {
    throw new Error(
      'TMDB_K6_BASE_URL must be an http(s) origin without credentials, a query string, or a fragment.',
    );
  }

  return candidate.replace(/\/+$/, '');
}

function endpointPath(name, fallback) {
  const configured = __ENV[name];
  if (configured === undefined || configured === '') {
    return fallback;
  }

  const value = String(configured);
  if (
    value.length > 2048 ||
    !value.startsWith('/') ||
    value.includes('\r') ||
    value.includes('\n') ||
    value.startsWith('//')
  ) {
    throw new Error(`${name} must be a relative request path no longer than 2048 bytes.`);
  }
  return value;
}

function configuredEndpoints() {
  return Object.freeze({
    metadata: Object.freeze({
      ...ENDPOINTS.metadata,
      path: endpointPath('TMDB_K6_METADATA_PATH', ENDPOINTS.metadata.path),
    }),
    list: Object.freeze({
      ...ENDPOINTS.list,
      path: endpointPath('TMDB_K6_LIST_PATH', ENDPOINTS.list.path),
    }),
    search: Object.freeze({
      ...ENDPOINTS.search,
      path: endpointPath('TMDB_K6_SEARCH_PATH', ENDPOINTS.search.path),
    }),
    filter: Object.freeze({
      ...ENDPOINTS.filter,
      path: endpointPath('TMDB_K6_FILTER_PATH', ENDPOINTS.filter.path),
    }),
  });
}

const runMode = __ENV.TMDB_K6_RUN_MODE || 'endpoint';
if (runMode !== 'endpoint' && runMode !== 'burn') {
  throw new Error('TMDB_K6_RUN_MODE must be either endpoint or burn.');
}

const configured = configuredEndpoints();
const endpointClass = __ENV.TMDB_K6_ENDPOINT_CLASS || 'metadata';
if (runMode === 'endpoint' && configured[endpointClass] === undefined) {
  throw new Error('TMDB_K6_ENDPOINT_CLASS must be metadata, list, search, or filter.');
}

const baseUrl = normalizedBaseUrl(__ENV.TMDB_K6_BASE_URL);
const virtualUsers = positiveInteger(__ENV.TMDB_K6_VUS, 100, 'TMDB_K6_VUS');
const iterations = positiveInteger(
  __ENV.TMDB_K6_ITERATIONS,
  runMode === 'burn' ? 100000 : 10000,
  'TMDB_K6_ITERATIONS',
);
const requestTimeout = __ENV.TMDB_K6_REQUEST_TIMEOUT || '30s';
const maxDuration = __ENV.TMDB_K6_MAX_DURATION || '30m';
const activeEndpoints =
  runMode === 'burn'
    ? [configured.metadata, configured.list, configured.search, configured.filter]
    : [configured[endpointClass]];

const thresholds = {
  http_req_failed: ['rate==0'],
  http_reqs: [`count==${iterations}`],
  iterations: [`count==${iterations}`],
  checks: ['rate==1'],
};
for (const endpoint of activeEndpoints) {
  thresholds[`http_req_duration{endpoint_class:${endpoint.key}}`] = [
    `p(95)<${endpoint.sloMs}`,
  ];
}

export const options = {
  discardResponseBodies: true,
  scenarios: {
    catalog: {
      executor: 'shared-iterations',
      vus: virtualUsers,
      iterations,
      maxDuration,
      gracefulStop: '30s',
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max', 'count'],
  thresholds,
};

function targetForIteration() {
  if (runMode === 'endpoint') {
    return activeEndpoints[0];
  }
  return activeEndpoints[exec.scenario.iterationInTest % activeEndpoints.length];
}

export default function () {
  const endpoint = targetForIteration();
  const response = http.get(`${baseUrl}${endpoint.path}`, {
    headers: { Accept: 'application/json' },
    responseType: 'none',
    timeout: requestTimeout,
    tags: {
      endpoint_class: endpoint.key,
      endpoint_slo_ms: String(endpoint.sloMs),
    },
  });

  check(
    response,
    {
      'response is successful': (result) => result.status >= 200 && result.status < 300,
    },
    { endpoint_class: endpoint.key },
  );
}

export function handleSummary(data) {
  const failed = data.metrics.http_req_failed?.values?.rate || 0;
  const duration = data.metrics.http_req_duration?.values?.['p(95)'];
  const summary = {
    run_mode: runMode,
    virtual_users: virtualUsers,
    iterations,
    failed_request_rate: failed,
    overall_p95_ms: duration ?? null,
  };

  return {
    stdout: `${JSON.stringify(summary)}\n`,
  };
}
