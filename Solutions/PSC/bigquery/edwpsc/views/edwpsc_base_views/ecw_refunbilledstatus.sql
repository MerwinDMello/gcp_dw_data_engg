CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refunbilledstatus`
AS SELECT
  `ecw_refunbilledstatus`.unbilledstatuskey,
  `ecw_refunbilledstatus`.unbilledstatuscategory,
  `ecw_refunbilledstatus`.unbilledstatussubcategory,
  `ecw_refunbilledstatus`.unbilledonholdflag,
  `ecw_refunbilledstatus`.unbilledunbilledflag,
  `ecw_refunbilledstatus`.unbilledbilledflag,
  `ecw_refunbilledstatus`.dwlastupdatedatetime,
  `ecw_refunbilledstatus`.sourcesystemcode,
  `ecw_refunbilledstatus`.insertedby,
  `ecw_refunbilledstatus`.inserteddtm,
  `ecw_refunbilledstatus`.modifiedby,
  `ecw_refunbilledstatus`.modifieddtm,
  `ecw_refunbilledstatus`.deleteflag
  FROM
    edwpsc.`ecw_refunbilledstatus`
;