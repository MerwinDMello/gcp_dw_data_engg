CREATE OR REPLACE VIEW edwpsc_views.`cmt_factbatchproductivity`
AS SELECT
  `cmt_factbatchproductivity`.batchproductivitykey,
  `cmt_factbatchproductivity`.userid,
  `cmt_factbatchproductivity`.firstname,
  `cmt_factbatchproductivity`.lastname,
  `cmt_factbatchproductivity`.batchid,
  `cmt_factbatchproductivity`.taxid,
  `cmt_factbatchproductivity`.bankpayername,
  `cmt_factbatchproductivity`.dateofaction,
  `cmt_factbatchproductivity`.timeofaction,
  `cmt_factbatchproductivity`.batchstatusbeforeaction,
  `cmt_factbatchproductivity`.batchstatusafteraction,
  `cmt_factbatchproductivity`.action,
  `cmt_factbatchproductivity`.notes,
  `cmt_factbatchproductivity`.coid,
  `cmt_factbatchproductivity`.amount,
  `cmt_factbatchproductivity`.checknumber,
  `cmt_factbatchproductivity`.insertedby,
  `cmt_factbatchproductivity`.inserteddtm,
  `cmt_factbatchproductivity`.modifiedby,
  `cmt_factbatchproductivity`.modifieddtm,
  `cmt_factbatchproductivity`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`cmt_factbatchproductivity`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`cmt_factbatchproductivity`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;