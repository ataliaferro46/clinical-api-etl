import nock from 'nock';

// Fail tests if a real HTTP call slips through:
nock.disableNetConnect();
nock.enableNetConnect('127.0.0.1'); // allow supertest's in-memory server

afterEach(() => {
  nock.cleanAll();
});
// Silence console.error during tests (keeps console.log/info)
const origError = console.error;
beforeAll(() => {
  jest.spyOn(console, 'error').mockImplementation(() => {});
});
afterAll(() => {
  (console.error as jest.Mock).mockRestore?.();
  console.error = origError;
});
