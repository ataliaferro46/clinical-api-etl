import { Request, Response, NextFunction } from 'express';
import { ETLService } from '../services/etl.service';
import { successResponse, errorResponse } from '../utils/response';

export class ETLController {
  private etlService = new ETLService();

  constructor() {
    this.etlService = new ETLService();
  }

  /**
   * Submit new ETL job
   * POST /api/etl/jobs
   */
  submitJob = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const { filename, studyId } = req.body;

      if (!filename) {
        errorResponse(res, 'filename is required', 400);
        return;
      }

      const job = await this.etlService.submitJob(filename, studyId);
      successResponse(res, job, 'ETL job submitted successfully');
    } catch (error) {
      next(error);
    }
  };

  /**
   * Get ETL job details
   * GET /api/etl/jobs/:id
   */
  getJob = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const { id } = req.params;
      const job = await this.etlService.getJob(id);

      if (!job) {
        errorResponse(res, 'Job not found', 404);
        return;
      }

      successResponse(res, job, 'Job retrieved successfully');
    } catch (error) {
      next(error);
    }
  };

  //  * Get ETL job status
  //  * GET /api/etl/jobs/:id/status
  getJobStatus = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    const { id } = req.params;
    if (!id) {
      errorResponse(res, 'Invalid job id', 400);
      return;
    }

    const data = await this.etlService.fetchJobStatus(id);
    successResponse(res, data, 'Status retrieved successfully');
  } catch (err: any) {
    if (err?.code === 'ETL_NOT_FOUND') {
      errorResponse(res, 'Job not found', 404);
      return;
    }
    next(err);
  }
};
}
