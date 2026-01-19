CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncregioncoidprovider`
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
    edwpsc.`ecw_juncregioncoidprovider`
;