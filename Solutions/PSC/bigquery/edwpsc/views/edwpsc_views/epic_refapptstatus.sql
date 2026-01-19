CREATE OR REPLACE VIEW edwpsc_views.`epic_refapptstatus`
AS SELECT
  `epic_refapptstatus`.apptstatuskey,
  `epic_refapptstatus`.apptstatusname,
  `epic_refapptstatus`.apptstatustitle,
  `epic_refapptstatus`.apptstatusabbr,
  `epic_refapptstatus`.apptstatusinternalid,
  `epic_refapptstatus`.apptstatusc,
  `epic_refapptstatus`.regionkey,
  `epic_refapptstatus`.sourceaprimarykey,
  `epic_refapptstatus`.dwlastupdatedatetime,
  `epic_refapptstatus`.sourcesystemcode,
  `epic_refapptstatus`.insertedby,
  `epic_refapptstatus`.inserteddtm,
  `epic_refapptstatus`.modifiedby,
  `epic_refapptstatus`.modifieddtm
  FROM
    edwpsc_base_views.`epic_refapptstatus`
;