CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refrh1201holdcodeprefix`
AS SELECT
  `ecw_refrh1201holdcodeprefix`.rh1201holdcodeprefixkey,
  `ecw_refrh1201holdcodeprefix`.rh1201holdcodeprefixname,
  `ecw_refrh1201holdcodeprefix`.dwlastupdatedatetime,
  `ecw_refrh1201holdcodeprefix`.sourcesystemcode,
  `ecw_refrh1201holdcodeprefix`.insertedby,
  `ecw_refrh1201holdcodeprefix`.inserteddtm,
  `ecw_refrh1201holdcodeprefix`.modifiedby,
  `ecw_refrh1201holdcodeprefix`.modifieddtm,
  `ecw_refrh1201holdcodeprefix`.deleteflag
  FROM
    edwpsc.`ecw_refrh1201holdcodeprefix`
;