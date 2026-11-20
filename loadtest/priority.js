import http from 'k6/http';
import { WebSocket } from 'k6/websockets';
import { Counter, Trend } from 'k6/metrics';

const api = __ENV.API_URL || 'http://api:8080';
const streams = api.replace(/^http/, 'ws');
const backlog = Number(__ENV.BACKLOG || 9000);
const json = { headers: { 'Content-Type': 'application/json' } };
const giveUpAfterSeconds = 60;

const sentAfter = new Trend('high_priority_sent_after', true);
const notSent = new Counter('high_priority_not_sent');

export const options = {
  summaryTrendStats: ['med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    high: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 5),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 50,
      maxVUs: 300,
      gracefulStop: `${giveUpAfterSeconds + 10}s`,
    },
  },
  thresholds: {
    high_priority_not_sent: ['count==0'],
    dropped_iterations: ['count==0'],
  },
};

export function setup() {
  const low = { recipient: '+905551234567', channel: 'sms', content: 'backlog', priority: 'low' };
  for (let created = 0; created < backlog; created += 1000) {
    const size = Math.min(1000, backlog - created);
    const response = http.post(
      api + '/v1/notifications/batch',
      JSON.stringify({ notifications: Array(size).fill(low) }),
      json,
    );
    if (response.status !== 202) {
      throw new Error(`seeding the backlog answered ${response.status}: ${response.body}`);
    }
  }
}

export default function () {
  const started = Date.now();
  const created = http.post(
    api + '/v1/notifications',
    JSON.stringify({ recipient: '+905551234567', channel: 'sms', content: 'urgent', priority: 'high' }),
    json,
  );
  if (created.status !== 202) {
    notSent.add(1);
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
      sentAfter.add(Date.now() - started);
    } else {
      notSent.add(1);
    }
    stream.close();
  };
  const givenUp = setTimeout(() => finish(false), giveUpAfterSeconds * 1000);
  const isSent = () => {
    const response = http.get(`${api}/v1/notifications/${id}`);
    return response.status === 200 && response.json('status') === 'sent';
  };
  stream.onopen = () => {
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
