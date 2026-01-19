CREATE TABLE IF NOT EXISTS edwpsc.epic_stgarpbcombinedfull
(
  tx_id NUMERIC(29),
  void_date DATETIME,
  post_date DATETIME,
  credit_src_module_c INT64,
  payment_source_c INT64,
  coverage_id NUMERIC(29),
  is_retro_tx STRING,
  is_reversed_c INT64,
  reposted_void_flag INT64,
  post_batch_num STRING,
  reference_num STRING,
  coll_agency_name STRING,
  snap_start_date DATETIME,
  amount NUMERIC(31, 2),
  regionkey INT64,
  ext_agncy_sent_dttm DATETIME,
  debit_credit_flag INT64,
  dwlastupdated DATETIME,
  dwlastupdatedatetime DATETIME
)
;