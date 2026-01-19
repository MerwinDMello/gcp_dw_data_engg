CREATE OR REPLACE VIEW edwpsc_views.`ecw_juncregioncoidprovider`
AS SELECT
  `ecw_juncregioncoidprovider`.juncregioncoidproviderkey,
  `ecw_juncregioncoidprovider`.regionkey,
  `ecw_juncregioncoidprovider`.coid,
  `ecw_juncregioncoidprovider`.providerkey,
  `ecw_juncregioncoidprovider`.insertedby,
  `ecw_juncregioncoidprovider`.inserteddtm,
  `ecw_juncregioncoidprovider`.modifiedby,
  `ecw_juncregioncoidprovider`.modifieddtm,
  `ecw_juncregioncoidprovider`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_juncregioncoidprovider`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_juncregioncoidprovider`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;