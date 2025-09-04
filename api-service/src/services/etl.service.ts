import axios from 'axios';
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

  constructor() {
    this.dbService = new DatabaseService();
    this.etlServiceUrl = process.env.ETL_SERVICE_URL || 'http://etl:8000';
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


  //   // Implementation needed:
  //   // 1. Validate jobId exists in database
  //   // 2. Call ETL service to get real-time status
  //   // 3. Handle connection errors gracefully
  //   // 4. Return formatted status response
  // TODO: CANDIDATE TO IMPLEMENT
  // /**
  //  * Get ETL job status from ETL service
  //  */
  async fetchJobStatus(id: string): Promise<JobStatus> {
    try {
      const resp = await axios.get(`${ETL_BASE}/jobs/${id}/status`, { timeout: 5000 });

      // Some ETL services return { data: {...} }, some return the object directly.
      const raw = resp.data?.data ?? resp.data;

      const jobId = raw?.jobId ?? raw?.id ?? id;
      const status = raw?.status;
      const progress = Number(raw?.progress ?? 0);
      const message = raw?.message;

      if (!jobId || !status || Number.isNaN(progress)) {
        throw new Error('unexpected_status_shape');
      }

      return { jobId, status, progress, message };
    } catch (e: any) {
      if (e?.response?.status === 404) {
        const err: any = new Error('not_found');
        err.code = 'ETL_NOT_FOUND';
        throw err;
      }
      throw e;
    }
  }
}
