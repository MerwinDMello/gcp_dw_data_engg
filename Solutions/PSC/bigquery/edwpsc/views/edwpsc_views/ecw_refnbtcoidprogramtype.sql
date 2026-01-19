CREATE OR REPLACE VIEW edwpsc_views.`ecw_refnbtcoidprogramtype`
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
    edwpsc_base_views.`ecw_refnbtcoidprogramtype`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(CAST(`ecw_refnbtcoidprogramtype`.coid AS STRING), ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;