CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refnbtallocatedphysicianstatus`
AS SELECT
  `ecw_refnbtallocatedphysicianstatus`.all_id,
  `ecw_refnbtallocatedphysicianstatus`.all_caption,
  `ecw_refnbtallocatedphysicianstatus`.physicianstatuskey,
  `ecw_refnbtallocatedphysicianstatus`.physicianstatusdesc,
  `ecw_refnbtallocatedphysicianstatus`.physicianstatusdetailid,
  `ecw_refnbtallocatedphysicianstatus`.physicianstatusdetaildesc,
  `ecw_refnbtallocatedphysicianstatus`.insertedby,
  `ecw_refnbtallocatedphysicianstatus`.inserteddtm,
  `ecw_refnbtallocatedphysicianstatus`.modifiedby,
  `ecw_refnbtallocatedphysicianstatus`.modifieddtm,
  `ecw_refnbtallocatedphysicianstatus`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_refnbtallocatedphysicianstatus`
;