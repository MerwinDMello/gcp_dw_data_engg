CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprtgoverneddenialstrending`
AS SELECT
  `ecw_rprtgoverneddenialstrending`.governeddenialstrendingkey,
  `ecw_rprtgoverneddenialstrending`.snapshotquarter,
  `ecw_rprtgoverneddenialstrending`.snapshotdate,
  `ecw_rprtgoverneddenialstrending`.snapshotyear,
  `ecw_rprtgoverneddenialstrending`.dosquarter,
  `ecw_rprtgoverneddenialstrending`.dosmonth,
  `ecw_rprtgoverneddenialstrending`.dosyear,
  `ecw_rprtgoverneddenialstrending`.coid,
  `ecw_rprtgoverneddenialstrending`.samestoreflag,
  `ecw_rprtgoverneddenialstrending`.servicingproviderlastname,
  `ecw_rprtgoverneddenialstrending`.servicingproviderfirstname,
  `ecw_rprtgoverneddenialstrending`.servicingprovidernpi,
  `ecw_rprtgoverneddenialstrending`.specialtycategory,
  `ecw_rprtgoverneddenialstrending`.specialtyname,
  `ecw_rprtgoverneddenialstrending`.specialtytype,
  `ecw_rprtgoverneddenialstrending`.iplan,
  `ecw_rprtgoverneddenialstrending`.majorpayor,
  `ecw_rprtgoverneddenialstrending`.financialclassname,
  `ecw_rprtgoverneddenialstrending`.financialclassnumber,
  `ecw_rprtgoverneddenialstrending`.firstdenialcategory,
  `ecw_rprtgoverneddenialstrending`.adjcode,
  `ecw_rprtgoverneddenialstrending`.adjname,
  `ecw_rprtgoverneddenialstrending`.nonparflag,
  `ecw_rprtgoverneddenialstrending`.adjcategory,
  `ecw_rprtgoverneddenialstrending`.sourcesystem,
  `ecw_rprtgoverneddenialstrending`.initialdeniedcharges,
  `ecw_rprtgoverneddenialstrending`.charges,
  `ecw_rprtgoverneddenialstrending`.finaldenials,
  `ecw_rprtgoverneddenialstrending`.newdenialsamt,
  `ecw_rprtgoverneddenialstrending`.newpaymentvariancesamt,
  `ecw_rprtgoverneddenialstrending`.newclaimrejectionsamt,
  `ecw_rprtgoverneddenialstrending`.initialpayerresponsecategory,
  `ecw_rprtgoverneddenialstrending`.dwlastupdatedatetime,
  `ecw_rprtgoverneddenialstrending`.insertedby,
  `ecw_rprtgoverneddenialstrending`.inserteddtm,
  `ecw_rprtgoverneddenialstrending`.modifiedby,
  `ecw_rprtgoverneddenialstrending`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_rprtgoverneddenialstrending`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprtgoverneddenialstrending`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;