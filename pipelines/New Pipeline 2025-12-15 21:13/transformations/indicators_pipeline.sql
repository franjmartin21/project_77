CREATE STREAMING TABLE indicators_metrics_raw
AS
SELECT
  *,
  _metadata.file_path AS metadata_file_path,
  element_at(
    split(
      element_at(
        split(_metadata.file_path, '/'),
        -1
      ),
      '_'
    ),
    0
  ) AS extraction_date
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

CREATE OR REFRESH STREAMING TABLE indicators_metrics
AS
SELECT
  indicator.name AS name,
  indicator.short_name AS short_name,
  indicator.id AS id,
  indicator.magnitud.name AS magnitud,
  indicator.tiempo.name AS tiempo,
  indicator.geos.geo_name as geo_name,
  measurements.value AS value,
  measurements.datetime_utc AS datetime_utc,
  metadata_file_path,
  extraction_date
FROM STREAM(indicators_metrics_raw)
LATERAL VIEW explode(indicator.magnitud) AS magnitud
LATERAL VIEW explode(indicator.tiempo) AS tiempo
LATERAL VIEW explode(indicator.geos) AS geo

