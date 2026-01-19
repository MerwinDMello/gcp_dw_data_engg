CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stggcpoolgeninfo
(
  pool STRING NOT NULL,
  gcpdesc STRING,
  total_build_time NUMERIC(29),
  beginning_balance NUMERIC(31, 2),
  accounts_remaining NUMERIC(29),
  records_added NUMERIC(29),
  build_start_date STRING,
  build_start_time STRING,
  build_finish_date STRING,
  build_finish_time STRING,
  snapshot_date DATE,
  PRIMARY KEY (pool) NOT ENFORCED
)
;