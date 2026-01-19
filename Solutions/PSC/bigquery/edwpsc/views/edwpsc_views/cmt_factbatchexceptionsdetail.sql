CREATE OR REPLACE VIEW edwpsc_views.`cmt_factbatchexceptionsdetail`
AS SELECT
  `cmt_factbatchexceptionsdetail`.batchid,
  `cmt_factbatchexceptionsdetail`.batchdate,
  `cmt_factbatchexceptionsdetail`.depositdate,
  `cmt_factbatchexceptionsdetail`.locationname,
  `cmt_factbatchexceptionsdetail`.locationcode,
  `cmt_factbatchexceptionsdetail`.notes,
  `cmt_factbatchexceptionsdetail`.transactiontypeid,
  `cmt_factbatchexceptionsdetail`.transactiontype,
  `cmt_factbatchexceptionsdetail`.depositamount,
  `cmt_factbatchexceptionsdetail`.postamount,
  `cmt_factbatchexceptionsdetail`.variance,
  `cmt_factbatchexceptionsdetail`.coid,
  `cmt_factbatchexceptionsdetail`.specialist,
  `cmt_factbatchexceptionsdetail`.statusnotes,
  `cmt_factbatchexceptionsdetail`.age,
  `cmt_factbatchexceptionsdetail`.batchage,
  `cmt_factbatchexceptionsdetail`.batchreasonid,
  `cmt_factbatchexceptionsdetail`.workflowstatus,
  `cmt_factbatchexceptionsdetail`.specialistid,
  `cmt_factbatchexceptionsdetail`.batchexceptionsdetailkey,
  `cmt_factbatchexceptionsdetail`.dwlastupdatedatetime,
  `cmt_factbatchexceptionsdetail`.selectedcoiduser
  FROM
    edwpsc_base_views.`cmt_factbatchexceptionsdetail`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`cmt_factbatchexceptionsdetail`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;