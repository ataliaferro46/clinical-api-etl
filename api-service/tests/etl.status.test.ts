import request from 'supertest';
import nock from 'nock';
import createApp from '../src/app';

const ETL_BASE = 'http://etl:8000';

describe('GET /api/etl/jobs/:id/status', () => {
  beforeAll(() => {
    // Ensure the app/service under test sees the same base URL we mock
    process.env.ETL_SERVICE_URL = ETL_BASE;
  });

  it('proxies status successfully', async () => {
    // Mock ETL service response
    nock(ETL_BASE)
      .get('/jobs/123/status')
      .reply(200, {
        jobId: '123',
        status: 'running',
        progress: 50,
        message: 'Processing data...',
      });

    const app = await createApp();
    const res = await request(app).get('/api/etl/jobs/123/status');

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.data.jobId).toBe('123');
    expect(res.body.data.status).toBe('running');
    expect(res.body.data.progress).toBe(50);
  });

  it('handles ETL 404 gracefully', async () => {
    nock(ETL_BASE)
      .get('/jobs/missing/status')
      .reply(404, { detail: 'Job not found' });

    const app = await createApp();
    const res = await request(app).get('/api/etl/jobs/missing/status');

    expect(res.status).toBe(404);
    expect(res.body.success).toBe(false);
    expect(res.body.message).toMatch(/not found/i);
  });

  it('handles ETL connection error', async () => {
    nock(ETL_BASE)
      .get('/jobs/boom/status')
      .replyWithError('connection refused');

    const app = await createApp();
    const res = await request(app).get('/api/etl/jobs/boom/status');

    expect(res.status).toBeGreaterThanOrEqual(500);
    expect(res.body.success).toBe(false);
  });
});
