CREATE OR REPLACE VIEW edwpsc_views.`cmt_facttreasurybatchdetail`
AS SELECT
  `cmt_facttreasurybatchdetail`.batchid,
  `cmt_facttreasurybatchdetail`.batchdate,
  `cmt_facttreasurybatchdetail`.depositdate,
  `cmt_facttreasurybatchdetail`.checkreference,
  `cmt_facttreasurybatchdetail`.amount,
  `cmt_facttreasurybatchdetail`.bankaccount,
  `cmt_facttreasurybatchdetail`.bankreference,
  `cmt_facttreasurybatchdetail`.description,
  `cmt_facttreasurybatchdetail`.bankname,
  `cmt_facttreasurybatchdetail`.payername,
  `cmt_facttreasurybatchdetail`.coid,
  `cmt_facttreasurybatchdetail`.taxid,
  `cmt_facttreasurybatchdetail`.lockboxnumber,
  `cmt_facttreasurybatchdetail`.location,
  `cmt_facttreasurybatchdetail`.locationid,
  `cmt_facttreasurybatchdetail`.batchstate,
  `cmt_facttreasurybatchdetail`.batchstatedesc,
  `cmt_facttreasurybatchdetail`.transactiontype,
  `cmt_facttreasurybatchdetail`.transactiontypedesc,
  `cmt_facttreasurybatchdetail`.batchhistoryid,
  `cmt_facttreasurybatchdetail`.userid,
  `cmt_facttreasurybatchdetail`.historyreasonid,
  `cmt_facttreasurybatchdetail`.historyreasondescription,
  `cmt_facttreasurybatchdetail`.notes,
  `cmt_facttreasurybatchdetail`.updatedate,
  `cmt_facttreasurybatchdetail`.receivername,
  `cmt_facttreasurybatchdetail`.paymentinformation,
  `cmt_facttreasurybatchdetail`.regionid,
  `cmt_facttreasurybatchdetail`.checkoreftnumber,
  `cmt_facttreasurybatchdetail`.insertedby,
  `cmt_facttreasurybatchdetail`.inserteddtm,
  `cmt_facttreasurybatchdetail`.modifiedby,
  `cmt_facttreasurybatchdetail`.modifieddtm,
  `cmt_facttreasurybatchdetail`.batchdetailid,
  `cmt_facttreasurybatchdetail`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`cmt_facttreasurybatchdetail`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`cmt_facttreasurybatchdetail`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;