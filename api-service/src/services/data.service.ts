import { DatabaseService, type DataFilters, type ClinicalMeasurement } from './database.service';

export { DataFilters, ClinicalMeasurement };

export class DataService {
  private dbService = new DatabaseService();

  async getData(filters: DataFilters): Promise<ClinicalMeasurement[]> {
    return this.dbService.queryMeasurements(filters);
  }

  async getStudyData(studyId: string): Promise<ClinicalMeasurement[]> {
    return this.dbService.queryByStudy(studyId);
  }
}
