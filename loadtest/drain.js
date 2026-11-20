import http from 'k6/http';
import { sleep } from 'k6';
import exec from 'k6/execution';
import { Trend } from 'k6/metrics';

const api = __ENV.API_URL || 'http://api:8080';
const rows = Number(__ENV.ROWS || 6000);
const giveUpAfterSeconds = 840;
const recipients = { sms: '+905551234567', email: 'user@example.com', push: 'device-token-1' };
const sentPerSecond = {
  sms: new Trend('sent_per_second_sms'),
  email: new Trend('sent_per_second_email'),
  push: new Trend('sent_per_second_push'),
};
const drainSeconds = {
  sms: new Trend('drain_seconds_sms'),
  email: new Trend('drain_seconds_email'),
  push: new Trend('drain_seconds_push'),
};

export const options = {
  summaryTrendStats: ['avg', 'med', 'p(95)', 'max'],
  scenarios: {
    watch: { executor: 'shared-iterations', vus: 1, iterations: 1, maxDuration: '15m' },
  },
};

export function setup() {
  const seeded = {};
  for (const [channel, recipient] of Object.entries(recipients)) {
    const notification = { recipient: recipient, channel: channel, content: 'load test' };
    seeded[channel] = { started: Date.now(), batches: [] };
    for (let created = 0; created < rows; created += 1000) {
      const size = Math.min(1000, rows - created);
      const response = http.post(
        api + '/v1/notifications/batch',
        JSON.stringify({ notifications: Array(size).fill(notification) }),
        { headers: { 'Content-Type': 'application/json' } },
      );
      if (response.status !== 202) {
        throw new Error(`seeding ${channel} answered ${response.status}: ${response.body}`);
      }
      seeded[channel].batches.push(response.json('batch_id'));
    }
  }
  return seeded;
}

function sent(batches) {
  let count = 0;
  for (const id of batches) {
    const response = http.get(`${api}/v1/batches/${id}`);
    if (response.status !== 200) {
      exec.test.abort(`batch ${id} answered ${response.status}: ${response.body}`);
    }
    const batch = response.json();
    if (batch.failed > 0 || batch.cancelled > 0) {
      exec.test.abort(`batch ${id} has ${batch.failed} failed and ${batch.cancelled} cancelled, want all sent`);
    }
    count += batch.sent;
  }
  return count;
}

export default function (seeded) {
  const before = {};
  for (const channel of Object.keys(seeded)) {
    before[channel] = { count: sent(seeded[channel].batches), at: Date.now() };
  }
  const drained = {};
  while (Object.keys(drained).length < Object.keys(seeded).length) {
    sleep(1);
    for (const [channel, { started, batches }] of Object.entries(seeded)) {
      if (drained[channel]) {
        continue;
      }
      const count = sent(batches);
      const at = Date.now();
      if (count === rows) {
        drained[channel] = true;
        drainSeconds[channel].add((at - started) / 1000);
        continue;
      }
      if (at - started > giveUpAfterSeconds * 1000) {
        exec.test.abort(`${channel} has ${count} of ${rows} sent after ${giveUpAfterSeconds} s`);
      }
      sentPerSecond[channel].add(((count - before[channel].count) * 1000) / (at - before[channel].at));
      before[channel] = { count: count, at: at };
    }
  }
}
