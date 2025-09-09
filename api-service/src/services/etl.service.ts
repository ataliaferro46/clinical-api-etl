import axios, { AxiosInstance } from 'axios';
import { v4 as uuidv4 } from 'uuid';
import { DatabaseService } from './database.service';

export interface ETLJob {
  id: string;
  filename: string;
  studyId?: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  createdAt: Date;
  updatedAt: Date;
  completedAt?: Date;
  errorMessage?: string;
}
const ETL_BASE = process.env.ETL_SERVICE_URL || 'http://etl:8000';

export type JobStatus = {
  jobId: string;
  status: 'queued' | 'running' | 'completed' | 'failed';
  progress: number; // 0..100
  message?: string;
};

export class ETLService {
  private dbService: DatabaseService;
  private etlServiceUrl: string;
  private http: AxiosInstance;  // <-- add this

  constructor() {
    this.dbService = new DatabaseService();
    this.etlServiceUrl = process.env.ETL_SERVICE_URL || 'http://etl:8000';

    // Initialize axios client once; reuse for all ETL calls.
    this.http = axios.create({
      baseURL: this.etlServiceUrl,
      timeout: 5000,
      // Let us check status codes ourselves (so tests can assert 404 vs 502/504).
      validateStatus: () => true,
    });
  }

  /**
   * Submit new ETL job
   */
  async submitJob(filename: string, studyId?: string): Promise<ETLJob> {
    const jobId = uuidv4();

    // Create job record in database
    const job: ETLJob = {
      id: jobId,
      filename,
      studyId,
      status: 'pending',
      createdAt: new Date(),
      updatedAt: new Date()
    };

    await this.dbService.createETLJob(job);

    // Submit job to ETL service
    try {
      await axios.post(`${this.etlServiceUrl}/jobs`, {
        jobId,
        filename,
        studyId
      });

      // Update job status to running
      await this.dbService.updateETLJobStatus(jobId, 'running');
      job.status = 'running';
    } catch (error) {
      // Update job status to failed
      await this.dbService.updateETLJobStatus(jobId, 'failed', 'Failed to submit to ETL service');
      job.status = 'failed';
      job.errorMessage = 'Failed to submit to ETL service';
    }

    return job;
  }

  /**
   * Get ETL job by ID
   */
  async getJob(jobId: string): Promise<ETLJob | null> {
    return await this.dbService.getETLJob(jobId);
  }

  async fetchJobStatus(id: string): Promise<JobStatus> {
    try {
      const resp = await this.http.get(`/jobs/${id}/status`);

      if (resp.status === 404) {
        const err: any = new Error('Job not found');
        err.code = 'ETL_NOT_FOUND';
        err.statusCode = 404;
        throw err;
      }

      if (resp.status >= 200 && resp.status < 300) {
        return resp.data as JobStatus;
      }

      // Any other non-2xx from ETL → treat as a bad gateway
      {
        const err: any = new Error(`ETL status error ${resp.status}`);
        err.statusCode = 502;
        throw err;
      }
    } catch (e: any) {
      // Axios timeout → surface as 504 to caller
      if (e?.code === 'ECONNABORTED') {
        e.statusCode = 504;
        e.message = 'ETL service timeout';
      }
      throw e;
    }
  }
}
