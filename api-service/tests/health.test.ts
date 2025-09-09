import request from 'supertest';
import createApp from '../src/app';

describe('GET /health', () => {
  it('returns healthy status', async () => {
    const app = await createApp(); // << async factory
    const res = await request(app).get('/health');

    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty('status', 'healthy');
    expect(res.body).toHaveProperty('timestamp');
  });
});
