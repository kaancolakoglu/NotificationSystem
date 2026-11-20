import http from 'k6/http';
import { sleep } from 'k6';
import { WebSocket } from 'k6/websockets';
import { Counter, Gauge, Trend } from 'k6/metrics';

const api = __ENV.API_URL || 'http://api:8080';
const streams = api.replace(/^http/, 'ws');
const count = Number(__ENV.STREAMS || 1000);
const dueAfterSeconds = Number(__ENV.DUE_AFTER || 30);
const json = { headers: { 'Content-Type': 'application/json' } };
const recipients = [
  { channel: 'sms', recipient: '+905551234567' },
  { channel: 'email', recipient: 'user@example.com' },
  { channel: 'push', recipient: 'device-token-1' },
];

const sentEventAfterDue = new Trend('sent_event_after_due', true);
const streamsOpened = new Counter('streams_opened');
const streamsWithoutSent = new Counter('streams_without_sent');
const apiGoroutines = new Gauge('api_goroutines');
const apiMemory = new Gauge('api_resident_megabytes');

export const options = {
  summaryTrendStats: ['med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    open: { executor: 'per-vu-iterations', vus: count, iterations: 1, maxDuration: '5m' },
    watch: { executor: 'constant-vus', vus: 1, duration: `${dueAfterSeconds + 30}s`, exec: 'watch' },
  },
  thresholds: {
    streams_without_sent: ['count==0'],
  },
};

export function setup() {
  return { dueAt: Date.now() + dueAfterSeconds * 1000 };
}

export default function (data) {
  const target = recipients[__VU % recipients.length];
  const created = http.post(
    api + '/v1/notifications',
    JSON.stringify({ ...target, content: 'stream', scheduled_at: new Date(data.dueAt).toISOString() }),
    json,
  );
  if (created.status !== 202) {
    streamsWithoutSent.add(1);
    return;
  }
  const id = created.json('id');
  const stream = new WebSocket(`${streams}/v1/notifications/${id}/events`);
  let finished = false;
  const finish = (sent) => {
    if (finished) {
      return;
    }
    finished = true;
    clearTimeout(givenUp);
    if (sent) {
      sentEventAfterDue.add(Date.now() - data.dueAt);
    } else {
      streamsWithoutSent.add(1);
    }
    stream.close();
  };
  const givenUp = setTimeout(() => finish(false), (dueAfterSeconds + 120) * 1000);
  const isSent = () => {
    const response = http.get(`${api}/v1/notifications/${id}`);
    return response.status === 200 && response.json('status') === 'sent';
  };
  stream.onopen = () => {
    streamsOpened.add(1);
    if (isSent()) {
      finish(true);
    }
  };
  stream.onclose = () => {
    if (!finished) {
      finish(isSent());
    }
  };
  stream.onmessage = (message) => {
    if (JSON.parse(message.data).status === 'sent') {
      finish(true);
    }
  };
  stream.onerror = () => finish(false);
}

export function watch() {
  const body = http.get(api + '/metrics').body;
  apiGoroutines.add(Number(body.match(/^go_goroutines (\d+)$/m)[1]));
  apiMemory.add(Number(body.match(/^process_resident_memory_bytes (\S+)$/m)[1]) / 1e6);
  sleep(1);
}
