CREATE OR REPLACE VIEW edwpsc_views.`artiva_stggcpoolgeninfo`
AS SELECT
  `artiva_stggcpoolgeninfo`.pool,
  `artiva_stggcpoolgeninfo`.gcpdesc,
  `artiva_stggcpoolgeninfo`.total_build_time,
  `artiva_stggcpoolgeninfo`.beginning_balance,
  `artiva_stggcpoolgeninfo`.accounts_remaining,
  `artiva_stggcpoolgeninfo`.records_added,
  `artiva_stggcpoolgeninfo`.build_start_date,
  `artiva_stggcpoolgeninfo`.build_start_time,
  `artiva_stggcpoolgeninfo`.build_finish_date,
  `artiva_stggcpoolgeninfo`.build_finish_time,
  `artiva_stggcpoolgeninfo`.snapshot_date
  FROM
    edwpsc_base_views.`artiva_stggcpoolgeninfo`
;