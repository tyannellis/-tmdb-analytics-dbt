-- models/debug/debug_env_check.sql
SELECT
  '{{ target.name }}' AS target_name,
  '{{ target.database }}' AS database_name,
  '{{ target.schema }}' AS schema_name,
  CURRENT_TIMESTAMP AS time_of_run