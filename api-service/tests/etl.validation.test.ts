import request from 'supertest';
import createApp from '../src/app';
import nock from 'nock';

let app: any;

beforeAll(async () => {
  app = await createApp();
});

afterEach(() => {
  nock.cleanAll();
});

describe('API edge cases', () => {
  it('returns 400 if job submission is missing filename', async () => {
    // This hits /api/etl/jobs (already implemented) and should 400
    const res = await request(app)
      .post('/api/etl/jobs')
      .send({ studyId: 'STUDY001' }); // no filename
    expect(res.status).toBe(400);
    expect(res.body.success).toBe(false);
  });

  it('returns 404 for unknown route', async () => {
    const res = await request(app).get('/api/does-not-exist');
    expect(res.status).toBe(404);
  });

  it('times out / surfaces ETL slowness as 500 from status endpoint', async () => {
    // Simulate ETL hanging / very slow
    nock('http://etl:8000').get('/jobs/slow/status').delay(6000).reply(200, {
      jobId: 'slow',
      status: 'running',
    });

    const res = await request(app).get('/api/etl/jobs/slow/status');
    // Your service default axios timeout is ~5s; expecting 500 fallback via error handler
    expect([500, 504]).toContain(res.status);
    expect(res.body.success).toBe(false);
  });
});
