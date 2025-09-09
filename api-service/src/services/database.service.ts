import { Pool } from 'pg';

const connectionString =
  process.env.DATABASE_URL || 'postgresql://user:pass@postgres:5432/clinical_data';

const pool = new Pool({ connectionString });

export type DataFilters = {
  studyId?: string;
  participantId?: string;
  measurementType?: string;   // 'glucose' | 'cholesterol' | 'blood_pressure' | etc.
  startTs?: string;           // ISO string
  endTs?: string;             // ISO string
  isValid?: boolean;
  limit?: number;
  offset?: number;
};

export type ClinicalMeasurement = {
  study_id: string;
  participant_id: string;
  measurement_type: string;
  unit: string | null;
  value_numeric: number | null;
  systolic: number | null;
  diastolic: number | null;
  quality_score: number;
  is_valid: boolean;
  quality_flags: string[];
  ts: string; // ISO
};

export class DatabaseService {
  /**
   * Unified query hitting fact_measurement + dimensions
   */
  async queryMeasurements(filters: DataFilters): Promise<ClinicalMeasurement[]> {
    const {
      studyId,
      participantId,
      measurementType,
      startTs,
      endTs,
      isValid,
      limit = 100,
      offset = 0,
    } = filters;

    const sql = `
      SELECT
        fm.study_id,
        fm.participant_id,
        mt.name AS measurement_type,
        u.name  AS unit,
        fm.value_numeric,
        fm.systolic,
        fm.diastolic,
        fm.quality_score,
        fm.is_valid,
        fm.quality_flags,
        fm.ts
      FROM fact_measurement fm
      JOIN dim_measurement_type mt ON mt.id = fm.measurement_type_id
      JOIN dim_unit              u ON u.id  = fm.unit_id
      WHERE
        ($1::text         IS NULL OR fm.study_id = $1)
        AND ($2::text     IS NULL OR fm.participant_id = $2)
        AND ($3::text     IS NULL OR mt.name = $3)
        AND ($4::timestamptz IS NULL OR fm.ts >= $4)
        AND ($5::timestamptz IS NULL OR fm.ts <  $5)
        AND ($6::boolean  IS NULL OR fm.is_valid = $6)
      ORDER BY fm.ts DESC
      LIMIT $7 OFFSET $8;
    `;

    const params = [
      studyId ?? null,
      participantId ?? null,
      measurementType ?? null,
      startTs ?? null,
      endTs ?? null,
      typeof isValid === 'boolean' ? isValid : null,
      Math.min(Number(limit) || 100, 1000),
      Number(offset) || 0,
    ];

    const { rows } = await pool.query<ClinicalMeasurement>(sql, params);

    // Ensure ISO strings on ts (pg returns Date objects if not cast)
    return rows.map(r => ({
      ...r,
      ts: new Date(r.ts as unknown as string).toISOString(),
    }));
  }

  /**
   * Convenience wrapper used by DataService.getStudyData
   */
  async queryByStudy(studyId: string): Promise<ClinicalMeasurement[]> {
    return this.queryMeasurements({ studyId, limit: 1000, offset: 0 });
  }
}
