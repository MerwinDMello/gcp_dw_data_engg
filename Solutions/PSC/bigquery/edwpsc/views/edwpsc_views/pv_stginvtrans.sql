CREATE OR REPLACE VIEW edwpsc_views.`pv_stginvtrans`
AS SELECT
  `pv_stginvtrans`.practice,
  `pv_stginvtrans`.inv_num,
  `pv_stginvtrans`.line_num,
  `pv_stginvtrans`.trans_num,
  `pv_stginvtrans`.trans_date,
  `pv_stginvtrans`.trans_type,
  `pv_stginvtrans`.trans_amt,
  `pv_stginvtrans`.trans_desc,
  `pv_stginvtrans`.reason,
  `pv_stginvtrans`.payment_num,
  `pv_stginvtrans`.db_acnt,
  `pv_stginvtrans`.cr_acnt,
  `pv_stginvtrans`.closing_date,
  `pv_stginvtrans`.crt_userid,
  `pv_stginvtrans`.crt_datetime,
  `pv_stginvtrans`.invtranspk,
  `pv_stginvtrans`.regionkey,
  `pv_stginvtrans`.sourcephysicaldeleteflag,
  `pv_stginvtrans`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_stginvtrans`
;