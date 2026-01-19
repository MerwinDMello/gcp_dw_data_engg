CREATE OR REPLACE VIEW edwpsc_views.`ecw_refnbtphysicianstatuslist`
AS SELECT
  `ecw_refnbtphysicianstatuslist`.nbtphysicianstatuskey,
  `ecw_refnbtphysicianstatuslist`.physicianstatuskey,
  `ecw_refnbtphysicianstatuslist`.physicianstatusdesc,
  `ecw_refnbtphysicianstatuslist`.physicianstatusdetailid,
  `ecw_refnbtphysicianstatuslist`.physicianstatusdetaildesc,
  `ecw_refnbtphysicianstatuslist`.statusyear,
  `ecw_refnbtphysicianstatuslist`.dwlastupdatedatetime,
  `ecw_refnbtphysicianstatuslist`.sourcesystemcode,
  `ecw_refnbtphysicianstatuslist`.insertedby,
  `ecw_refnbtphysicianstatuslist`.inserteddtm,
  `ecw_refnbtphysicianstatuslist`.modifiedby,
  `ecw_refnbtphysicianstatuslist`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refnbtphysicianstatuslist`
;