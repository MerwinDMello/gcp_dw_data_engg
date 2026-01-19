CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_rprtpeclaimsonholdrollforward`
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
    edwpsc.`ecw_rprtpeclaimsonholdrollforward`
;