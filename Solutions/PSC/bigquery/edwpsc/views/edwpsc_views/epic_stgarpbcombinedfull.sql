CREATE OR REPLACE VIEW edwpsc_views.`epic_stgarpbcombinedfull`
AS SELECT
  `epic_stgarpbcombinedfull`.tx_id,
  `epic_stgarpbcombinedfull`.void_date,
  `epic_stgarpbcombinedfull`.post_date,
  `epic_stgarpbcombinedfull`.credit_src_module_c,
  `epic_stgarpbcombinedfull`.payment_source_c,
  `epic_stgarpbcombinedfull`.coverage_id,
  `epic_stgarpbcombinedfull`.is_retro_tx,
  `epic_stgarpbcombinedfull`.is_reversed_c,
  `epic_stgarpbcombinedfull`.reposted_void_flag,
  `epic_stgarpbcombinedfull`.post_batch_num,
  `epic_stgarpbcombinedfull`.reference_num,
  `epic_stgarpbcombinedfull`.coll_agency_name,
  `epic_stgarpbcombinedfull`.snap_start_date,
  `epic_stgarpbcombinedfull`.amount,
  `epic_stgarpbcombinedfull`.regionkey,
  `epic_stgarpbcombinedfull`.ext_agncy_sent_dttm,
  `epic_stgarpbcombinedfull`.debit_credit_flag,
  `epic_stgarpbcombinedfull`.dwlastupdated,
  `epic_stgarpbcombinedfull`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_stgarpbcombinedfull`
;