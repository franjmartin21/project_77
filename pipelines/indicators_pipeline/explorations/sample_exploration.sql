-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Exploration of the bronze table

-- COMMAND ----------


select *
from francisco_martin_workspace.project_77_indicators.indicators_metrics_raw
limit 10

-- COMMAND ----------


select *
from francisco_martin_workspace.project_77_indicators.indicators_metrics

-- COMMAND ----------

  SELECT *
  FROM STREAM READ_FILES(
    '/Volumes/francisco_martin_workspace/project_77_indicators/indicator_metrics',
    format => 'json'
  );

-- COMMAND ----------

SELECT from_json('{"a":{"name": "abc"}}', 'abc STRING').a.abc ;

-- COMMAND ----------

SELECT indicator.name
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
LATERAL VIEW explode(indicator.values) AS measurements

-- COMMAND ----------



-- COMMAND ----------


