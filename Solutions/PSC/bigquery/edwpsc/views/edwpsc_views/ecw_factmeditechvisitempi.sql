CREATE OR REPLACE VIEW edwpsc_views.`ecw_factmeditechvisitempi`
AS SELECT
  `ecw_factmeditechvisitempi`.meditechvisitempikey,
  `ecw_factmeditechvisitempi`.coid,
  `ecw_factmeditechvisitempi`.meditechcocid,
  `ecw_factmeditechvisitempi`.empinumber,
  `ecw_factmeditechvisitempi`.accountnumber,
  `ecw_factmeditechvisitempi`.patientaccountnumber,
  `ecw_factmeditechvisitempi`.patientmedicalrecord,
  `ecw_factmeditechvisitempi`.patientdwid,
  `ecw_factmeditechvisitempi`.sourceaprimarykeyvalue,
  `ecw_factmeditechvisitempi`.sourcelastupdatedatetime,
  `ecw_factmeditechvisitempi`.sourcesystemcode,
  `ecw_factmeditechvisitempi`.insertedby,
  `ecw_factmeditechvisitempi`.inserteddtm,
  `ecw_factmeditechvisitempi`.modifiedby,
  `ecw_factmeditechvisitempi`.modifieddtm,
  `ecw_factmeditechvisitempi`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_factmeditechvisitempi`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factmeditechvisitempi`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;