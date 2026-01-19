CREATE OR REPLACE VIEW edwpsc_views.`epic_refinvstatus`
AS SELECT
  `epic_refinvstatus`.invstatuskey,
  `epic_refinvstatus`.invstatusname,
  `epic_refinvstatus`.invstatustitle,
  `epic_refinvstatus`.invstatusabbr,
  `epic_refinvstatus`.invstatusinternalid,
  `epic_refinvstatus`.invstatusc,
  `epic_refinvstatus`.regionkey,
  `epic_refinvstatus`.sourceaprimarykey,
  `epic_refinvstatus`.dwlastupdatedatetime,
  `epic_refinvstatus`.sourcesystemcode,
  `epic_refinvstatus`.insertedby,
  `epic_refinvstatus`.inserteddtm,
  `epic_refinvstatus`.modifiedby,
  `epic_refinvstatus`.modifieddtm
  FROM
    edwpsc_base_views.`epic_refinvstatus`
;