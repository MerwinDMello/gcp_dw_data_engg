CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprtdenialstrending`
AS SELECT
  `ecw_rprtdenialstrending`.rprtdenialstrendingkey,
  `ecw_rprtdenialstrending`.snapshotdate,
  `ecw_rprtdenialstrending`.snapshotyear,
  `ecw_rprtdenialstrending`.dosmonth,
  `ecw_rprtdenialstrending`.dosyear,
  `ecw_rprtdenialstrending`.coid,
  `ecw_rprtdenialstrending`.specialtycategory,
  `ecw_rprtdenialstrending`.specialtyname,
  `ecw_rprtdenialstrending`.specialtytype,
  `ecw_rprtdenialstrending`.majorpayor,
  `ecw_rprtdenialstrending`.financialclassname,
  `ecw_rprtdenialstrending`.firstdenialcategory,
  `ecw_rprtdenialstrending`.firstdenialcategorycustom,
  `ecw_rprtdenialstrending`.adjcode,
  `ecw_rprtdenialstrending`.adjname,
  `ecw_rprtdenialstrending`.sourcesystem,
  `ecw_rprtdenialstrending`.initialdeniedcharges,
  `ecw_rprtdenialstrending`.initialdeniedchargesdos,
  `ecw_rprtdenialstrending`.charges,
  `ecw_rprtdenialstrending`.chargesdos,
  `ecw_rprtdenialstrending`.finaldenials,
  `ecw_rprtdenialstrending`.finaldenialsdos,
  `ecw_rprtdenialstrending`.initialdeniedcharges3month,
  `ecw_rprtdenialstrending`.initialdeniedcharges6month,
  `ecw_rprtdenialstrending`.initialdeniedcharges12month,
  `ecw_rprtdenialstrending`.finaldenials3month,
  `ecw_rprtdenialstrending`.finaldenials6month,
  `ecw_rprtdenialstrending`.finaldenials12month,
  `ecw_rprtdenialstrending`.charges3month,
  `ecw_rprtdenialstrending`.charges6month,
  `ecw_rprtdenialstrending`.charges12month,
  `ecw_rprtdenialstrending`.insertedby,
  `ecw_rprtdenialstrending`.inserteddtm,
  `ecw_rprtdenialstrending`.modifiedby,
  `ecw_rprtdenialstrending`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_rprtdenialstrending`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprtdenialstrending`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;