import { Request, Response, NextFunction } from 'express';
import { DataService } from '../services/data.service';
import { successResponse } from '../utils/response';

export class DataController {
  private dataService: DataService;

  constructor() {
    this.dataService = new DataService();
  }

  // GET /api/data
  getData = async (req: Request, res: Response, next: NextFunction) => {
    try {
      const {
        studyId,
        participantId,
        measurementType,
        startTs,
        endTs,
        isValid,
        limit,
        offset,
      } = req.query;

      const rows = await this.dataService.getData({
        studyId: studyId as string,
        participantId: participantId as string,
        measurementType: measurementType as string,
        startTs: startTs as string,
        endTs: endTs as string,
        isValid: isValid === 'true' ? true : isValid === 'false' ? false : undefined,
        limit: limit ? parseInt(limit as string, 10) : 100,
        offset: offset ? parseInt(offset as string, 10) : 0,
      });

      return successResponse(res, rows, 'Data retrieved successfully');
    } catch (err) {
      next(err);
    }
  };

  // GET /api/data/studies/:id
  getStudyData = async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { id } = req.params;
      const rows = await this.dataService.getStudyData(id);
      return successResponse(res, rows, `Study ${id} data retrieved`);
    } catch (err) {
      next(err);
    }
  };
}
