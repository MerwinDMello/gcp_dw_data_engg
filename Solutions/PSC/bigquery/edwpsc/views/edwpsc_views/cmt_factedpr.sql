CREATE OR REPLACE VIEW edwpsc_views.`cmt_factedpr`
AS SELECT
  `cmt_factedpr`.reportid,
  `cmt_factedpr`.reportname,
  `cmt_factedpr`.coid,
  `cmt_factedpr`.servicedate,
  `cmt_factedpr`.practicename,
  `cmt_factedpr`.location,
  `cmt_factedpr`.reportstatus,
  `cmt_factedpr`.reportstatusid,
  `cmt_factedpr`.bank,
  `cmt_factedpr`.enddate,
  `cmt_factedpr`.hasdeposit,
  `cmt_factedpr`.deleteisallowed,
  `cmt_factedpr`.dwlastupdatedatetime,
  `cmt_factedpr`.batchexceptionsdetailkey,
  `cmt_factedpr`.lastsaved,
  `cmt_factedpr`.batchnbr,
  `cmt_factedpr`.createddate
  FROM
    edwpsc_base_views.`cmt_factedpr`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`cmt_factedpr`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;