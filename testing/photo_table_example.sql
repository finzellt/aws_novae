CREATE TABLE IF NOT EXISTS photo_table (
  photo_table_id  BIGINT PRIMARY KEY,                  -- PK
  spectral_regime TEXT        NOT NULL,
  obs_date        DATE              NOT NULL,
  obs_time_utc    TIME WITHOUT TIME ZONE NOT NULL,
  obs_jd          DOUBLE PRECISION  NOT NULL,
  flux            DOUBLE PRECISION  NOT NULL,
  flux_err        DOUBLE PRECISION,
  filter_band     VARCHAR(10)       NOT NULL,
  limit_flag      BOOLEAN           DEFAULT FALSE,
  observer        TEXT,
  telescope       TEXT,
  instrument      TEXT,
  flux_units      TEXT[],
  published_flag  BOOLEAN,
  bibcode         TEXT
);