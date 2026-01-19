CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprtpeclaimsonholdrollforward`
AS SELECT
  `ecw_rprtpeclaimsonholdrollforward`.coid,
  `ecw_rprtpeclaimsonholdrollforward`.groupname,
  `ecw_rprtpeclaimsonholdrollforward`.divisionname,
  `ecw_rprtpeclaimsonholdrollforward`.marketname,
  `ecw_rprtpeclaimsonholdrollforward`.lob,
  `ecw_rprtpeclaimsonholdrollforward`.providerid,
  `ecw_rprtpeclaimsonholdrollforward`.providername,
  `ecw_rprtpeclaimsonholdrollforward`.filestatus,
  `ecw_rprtpeclaimsonholdrollforward`.amount,
  `ecw_rprtpeclaimsonholdrollforward`.insertedby,
  `ecw_rprtpeclaimsonholdrollforward`.inserteddtm,
  `ecw_rprtpeclaimsonholdrollforward`.modifiedby,
  `ecw_rprtpeclaimsonholdrollforward`.modifieddtm,
  `ecw_rprtpeclaimsonholdrollforward`.columntype,
  `ecw_rprtpeclaimsonholdrollforward`.claimcount,
  `ecw_rprtpeclaimsonholdrollforward`.snapshotdate,
  `ecw_rprtpeclaimsonholdrollforward`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_rprtpeclaimsonholdrollforward`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprtpeclaimsonholdrollforward`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;