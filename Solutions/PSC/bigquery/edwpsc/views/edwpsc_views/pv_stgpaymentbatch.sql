CREATE OR REPLACE VIEW edwpsc_views.`pv_stgpaymentbatch`
AS SELECT
  `pv_stgpaymentbatch`.batch_num,
  `pv_stgpaymentbatch`.practice,
  `pv_stgpaymentbatch`.clinic,
  `pv_stgpaymentbatch`.deposit_date,
  `pv_stgpaymentbatch`.category,
  `pv_stgpaymentbatch`.total_amt,
  `pv_stgpaymentbatch`.remain_amt,
  `pv_stgpaymentbatch`.description,
  `pv_stgpaymentbatch`.crt_userid,
  `pv_stgpaymentbatch`.lock_by,
  `pv_stgpaymentbatch`.last_upd_userid,
  `pv_stgpaymentbatch`.last_upd_datetime,
  `pv_stgpaymentbatch`.erapostnum,
  `pv_stgpaymentbatch`.paymentbatchpk,
  `pv_stgpaymentbatch`.crt_datetime,
  `pv_stgpaymentbatch`.regionkey,
  `pv_stgpaymentbatch`.sourcephysicaldeleteflag,
  `pv_stgpaymentbatch`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_stgpaymentbatch`
;