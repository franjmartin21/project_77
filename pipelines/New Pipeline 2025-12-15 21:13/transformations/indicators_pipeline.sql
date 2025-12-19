CREATE STREAMING TABLE indicators_metrics_raw
AS
SELECT
  *,
  _metadata.file_path AS metadata_file_path,
  _metadata.file_modification_time as metadata_file_modification_time
FROM STREAM READ_FILES(
  '/Volumes/francisco_martin_workspace/project_77_indicators/indicator_metrics',
  format => 'json',
  schema => '
    indicator STRUCT<
      name: STRING,
      short_name: STRING,
      id: LONG,
      composited: BOOLEAN,
      step_type: STRING,
      disaggregated: BOOLEAN,
      magnitud: ARRAY<STRUCT<name: STRING, id: LONG>>,
      tiempo: ARRAY<STRUCT<name: STRING, id: LONG>>,
      geos: ARRAY<STRUCT<geo_id: LONG, geo_name: STRING>>,
      values_updated_at: STRING,
      values: ARRAY<STRUCT<
        value: DOUBLE,
        datetime: STRING,
        datetime_utc: STRING,
        tz_time: STRING,
        geo_ids: ARRAY<LONG>
      >>
    >'
)
LATERAL VIEW explode(indicator.values) AS measurements;

CREATE OR REFRESH STREAMING TABLE indicators_metrics;

CREATE FLOW indicators_metrics_cdc_flow 
AS AUTO CDC INTO indicators_metrics
FROM (
  SELECT indicator.name AS name,
        indicator.short_name AS short_name,
        indicator.id AS id,
        indicator.magnitud AS magnitud,
        indicator.tiempo AS tiempo,
        indicator.geos as geos,
        measurements.value AS value,
        measurements.datetime_utc AS datetime_utc,
        measurements.geo_ids[0] as geo_id,
        metadata_file_path,
        metadata_file_modification_time
  FROM STREAM(indicators_metrics_raw)
)
KEYS(id, datetime_utc)
SEQUENCE BY(metadata_file_modification_time)
STORED AS SCD TYPE 1;

CREATE OR REPLACE MATERIALIZED VIEW magnitud_dimension
AS
SELECT DISTINCT
  m.id AS magnitud_id,
  m.name AS magnitud_name
FROM indicators_metrics
LATERAL VIEW explode(magnitud) AS m;

CREATE OR REPLACE MATERIALIZED VIEW tiempo_dimension
AS
SELECT DISTINCT
  t.id AS tiempo_id,
  t.name AS tiempo_name
FROM indicators_metrics
LATERAL VIEW explode(tiempo) AS t;

CREATE OR REPLACE MATERIALIZED VIEW geo_dimension
AS
SELECT DISTINCT
  g.geo_id AS geo_id,
  g.geo_name AS geo_name
FROM indicators_metrics
LATERAL VIEW explode(geos) AS g;

CREATE OR REFRESH MATERIALIZED VIEW indicators_metrics_fact
AS
SELECT
  id,
  name,
  short_name,
  datetime_utc,
  magnitud[0].id AS magnitud_id,
  tiempo[0].id AS tiempo_id,
  geo_id,
  value,
  metadata_file_path,
  metadata_file_modification_time
FROM indicators_metrics;


