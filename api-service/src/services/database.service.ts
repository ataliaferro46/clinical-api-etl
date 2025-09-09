import { Pool } from 'pg';

const connectionString =
  process.env.DATABASE_URL || 'postgresql://user:pass@postgres:5432/clinical_data';

const pool = new Pool({ connectionString });

export type DataFilters = {
  studyId?: string;
  participantId?: string;
  measurementType?: string;   // e.g., 'glucose', 'cholesterol', 'blood_pressure'
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
  ts: string; // ISO string
};

export type ETLJob = {
  id: string;
  filename: string;
  studyId?: string; // optional, not null
  status: 'pending' | 'running' | 'completed' | 'failed';
  createdAt: Date;
  updatedAt: Date;
};

export class DatabaseService {
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
    return rows.map(r => ({
      ...r,
      ts: new Date(r.ts as unknown as string).toISOString(),
    }));
  }

  async queryByStudy(studyId: string): Promise<ClinicalMeasurement[]> {
    return this.queryMeasurements({ studyId, limit: 1000, offset: 0 });
  }

  async createETLJob(job: { id: string; filename: string; studyId?: string | null }): Promise<void>;
  async createETLJob(id: string, filename: string, studyId?: string | null): Promise<void>;
  async createETLJob(
    a: string | { id: string; filename: string; studyId?: string | null },
    b?: string,
    c?: string | null
  ): Promise<void> {
    let id: string;
    let filename: string;
    let studyId: string | null;

    if (typeof a === 'string') {
      id = a;
      filename = b as string;
      studyId = c ?? null;
    } else {
      id = a.id;
      filename = a.filename;
      studyId = a.studyId ?? null;
    }

    const sql = `
      INSERT INTO etl_jobs (id, filename, study_id, status)
      VALUES ($1, $2, $3, 'pending')
      ON CONFLICT (id) DO NOTHING;
    `;
    await pool.query(sql, [id, filename, studyId]);
  }

  // Keep a 3rd arg for compatibility; we ignore it here.
  async updateETLJobStatus(
    id: string,
    status: 'pending' | 'running' | 'completed' | 'failed',
    _message?: string
  ): Promise<void> {
    const sql = `
      UPDATE etl_jobs
      SET status = $2, updated_at = NOW()
      WHERE id = $1;
    `;
    await pool.query(sql, [id, status]);
  }

  async getETLJob(id: string): Promise<ETLJob | null> {
    const { rows } = await pool.query(
      `SELECT id, filename, study_id, status, created_at, updated_at
       FROM etl_jobs WHERE id = $1`,
      [id]
    );
    const r = rows[0];
    if (!r) return null;

    const normalizedStatus =
      r.status === 'queued' ? 'pending' : (r.status as 'pending' | 'running' | 'completed' | 'failed');

    return {
      id: r.id,
      filename: r.filename,
      studyId: r.study_id ?? undefined,
      status: normalizedStatus,
      createdAt: new Date(r.created_at),
      updatedAt: new Date(r.updated_at),
    };
  }
}
