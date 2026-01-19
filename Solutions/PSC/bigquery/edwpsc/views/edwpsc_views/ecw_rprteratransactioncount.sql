CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprteratransactioncount`
AS SELECT
  `ecw_rprteratransactioncount`.snapshotdate,
  `ecw_rprteratransactioncount`.createyear,
  `ecw_rprteratransactioncount`.createmonth,
  `ecw_rprteratransactioncount`.ecwregionid,
  `ecw_rprteratransactioncount`.groupname,
  `ecw_rprteratransactioncount`.divisionname,
  `ecw_rprteratransactioncount`.marketname,
  `ecw_rprteratransactioncount`.lob,
  `ecw_rprteratransactioncount`.coid,
  `ecw_rprteratransactioncount`.coidname,
  `ecw_rprteratransactioncount`.coidstartmonth,
  `ecw_rprteratransactioncount`.coidtermmonth,
  `ecw_rprteratransactioncount`.practicefederaltaxid,
  `ecw_rprteratransactioncount`.groupnpi,
  `ecw_rprteratransactioncount`.insurancegroupname,
  `ecw_rprteratransactioncount`.payorname,
  `ecw_rprteratransactioncount`.renderingprovider,
  `ecw_rprteratransactioncount`.renderingproviderstartmonth,
  `ecw_rprteratransactioncount`.renderingprovidertermmonth,
  `ecw_rprteratransactioncount`.claimcount,
  `ecw_rprteratransactioncount`.erapaid,
  `ecw_rprteratransactioncount`.manualpaid,
  `ecw_rprteratransactioncount`.eracount,
  `ecw_rprteratransactioncount`.manualcount,
  `ecw_rprteratransactioncount`.fifththirdpaid,
  `ecw_rprteratransactioncount`.nonfifththirdpaid,
  `ecw_rprteratransactioncount`.fifththirdcount,
  `ecw_rprteratransactioncount`.nonfifththirdcount,
  `ecw_rprteratransactioncount`.insertedby,
  `ecw_rprteratransactioncount`.inserteddtm,
  `ecw_rprteratransactioncount`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_rprteratransactioncount`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprteratransactioncount`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;