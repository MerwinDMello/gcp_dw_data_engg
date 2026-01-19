CREATE OR REPLACE VIEW edwpsc_views.`ecw_refdivision`
AS SELECT
  `ecw_refdivision`.divisionkey,
  `ecw_refdivision`.divisionname,
  `ecw_refdivision`.groupkey,
  `ecw_refdivision`.divisionisvisibleonfactclaim,
  `ecw_refdivision`.sourceprimarykeyvalue,
  `ecw_refdivision`.sourcerecordlastupdated,
  `ecw_refdivision`.dwlastupdatedatetime,
  `ecw_refdivision`.sourcesystemcode,
  `ecw_refdivision`.insertedby,
  `ecw_refdivision`.inserteddtm,
  `ecw_refdivision`.modifiedby,
  `ecw_refdivision`.modifieddtm,
  `ecw_refdivision`.divisioncode,
  `ecw_refdivision`.deleteflag,
  `ecw_refdivision`.coidstatflag,
  `ecw_refdivision`.ppmsflag,
  `ecw_refdivision`.sysstarttime,
  `ecw_refdivision`.sysendtime
  FROM
    edwpsc_base_views.`ecw_refdivision`
;