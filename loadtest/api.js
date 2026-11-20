import http from 'k6/http';
import { check } from 'k6';

const api = __ENV.API_URL || 'http://api:8080';
const rate = Number(__ENV.RATE || 500);
const batchSize = Number(__ENV.BATCH || 0);
const notDueDuringTheTest = '2099-01-01T00:00:00Z';

const notification = {
  recipient: '+905551234567',
  channel: 'sms',
  content: 'load test',
  scheduled_at: notDueDuringTheTest,
};
const single = JSON.stringify(notification);
const batch = JSON.stringify({ notifications: Array(batchSize).fill(notification) });

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    create: {
      executor: 'constant-arrival-rate',
      rate: rate,
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: Math.max(50, Math.ceil(rate / 5)),
      maxVUs: Math.max(200, rate),
    },
  },
  thresholds: {
    checks: ['rate==1'],
  },
};

export default function () {
  const path = batchSize > 0 ? '/v1/notifications/batch' : '/v1/notifications';
  const response = http.post(api + path, batchSize > 0 ? batch : single, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(response, { 'answered 202': (r) => r.status === 202 });
}
