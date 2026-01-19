CREATE OR REPLACE VIEW edwpsc_views.`artiva_stggcpoolgeninfo_history`
AS SELECT
  `artiva_stggcpoolgeninfo_history`.snapshot_date,
  `artiva_stggcpoolgeninfo_history`.pool,
  `artiva_stggcpoolgeninfo_history`.gcpdesc,
  `artiva_stggcpoolgeninfo_history`.total_build_time,
  `artiva_stggcpoolgeninfo_history`.beginning_balance,
  `artiva_stggcpoolgeninfo_history`.accounts_remaining,
  `artiva_stggcpoolgeninfo_history`.records_added,
  `artiva_stggcpoolgeninfo_history`.build_start_date,
  `artiva_stggcpoolgeninfo_history`.build_start_time,
  `artiva_stggcpoolgeninfo_history`.build_finish_date,
  `artiva_stggcpoolgeninfo_history`.build_finish_time,
  `artiva_stggcpoolgeninfo_history`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`artiva_stggcpoolgeninfo_history`
;