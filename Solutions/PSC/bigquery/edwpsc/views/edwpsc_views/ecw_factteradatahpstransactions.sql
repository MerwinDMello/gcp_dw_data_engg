CREATE OR REPLACE VIEW edwpsc_views.`ecw_factteradatahpstransactions`
AS SELECT
  `ecw_factteradatahpstransactions`.teradatahpstransactionskey,
  `ecw_factteradatahpstransactions`.coid,
  `ecw_factteradatahpstransactions`.regionkey,
  `ecw_factteradatahpstransactions`.loaddatekey,
  `ecw_factteradatahpstransactions`.hpstransactionid,
  `ecw_factteradatahpstransactions`.transactiontimestamp,
  `ecw_factteradatahpstransactions`.transactiondate,
  `ecw_factteradatahpstransactions`.entryuserid,
  `ecw_factteradatahpstransactions`.paymenttendertype,
  `ecw_factteradatahpstransactions`.paymentreference,
  `ecw_factteradatahpstransactions`.transactionamt,
  `ecw_factteradatahpstransactions`.dwlastupdatedatetime,
  `ecw_factteradatahpstransactions`.sourcesystemcode,
  `ecw_factteradatahpstransactions`.insertedby,
  `ecw_factteradatahpstransactions`.inserteddtm,
  `ecw_factteradatahpstransactions`.modifiedby,
  `ecw_factteradatahpstransactions`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_factteradatahpstransactions`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factteradatahpstransactions`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;