import { DatabaseService } from './database.service';

import { pool } from '../lib/db';

export type DataFilters = {
  studyId?: string;
  participantId?: string;
  measurementType?: string; // e.g., 'glucose', 'cholesterol', 'blood_pressure'
  startTs?: string;         // ISO date string
  endTs?: string;           // ISO date string
  isValid?: boolean;
  limit?: number;
  offset?: number;
};

export interface DataFilters {
  studyId?: string;
  participantId?: string;
  measurementType?: string;
  startDate?: string;
  endDate?: string;
}

export interface ClinicalMeasurement {
  id: string;
  studyId: string;
  participantId: string;
  measurementType: string;
  value: string;
  unit: string;
  timestamp: Date;
  siteId: string;
  qualityScore: number;
  processedAt?: Date;
}

export class DataService {
  private dbService: DatabaseService;

  constructor() {
    this.dbService = new DatabaseService();
  }

  /**
   * Query processed clinical data with filters
   */
  async getData(filters: DataFilters): Promise<ClinicalMeasurement[]> {
    return await this.dbService.queryMeasurements(filters);
  }

  /**
   * Get all data for a specific study
   */
  async getStudyData(studyId: string): Promise<ClinicalMeasurement[]> {
    return await this.dbService.queryMeasurements({ studyId });
  }
}
