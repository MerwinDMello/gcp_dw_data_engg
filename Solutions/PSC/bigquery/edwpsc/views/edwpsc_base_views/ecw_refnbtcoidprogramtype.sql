CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refnbtcoidprogramtype`
AS SELECT
  `ecw_refnbtcoidprogramtype`.coid,
  `ecw_refnbtcoidprogramtype`.programtypeid,
  `ecw_refnbtcoidprogramtype`.programtypedescription,
  `ecw_refnbtcoidprogramtype`.insertedby,
  `ecw_refnbtcoidprogramtype`.inserteddtm,
  `ecw_refnbtcoidprogramtype`.modifiedby,
  `ecw_refnbtcoidprogramtype`.modifieddtm,
  `ecw_refnbtcoidprogramtype`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_refnbtcoidprogramtype`
;