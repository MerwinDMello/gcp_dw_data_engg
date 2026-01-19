CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refunbilledstatus`
AS SELECT
  `epic_refunbilledstatus`.unbilledstatuskey,
  `epic_refunbilledstatus`.unbilledstatuscategory,
  `epic_refunbilledstatus`.unbilledstatussubcategory,
  `epic_refunbilledstatus`.unbilledonholdflag,
  `epic_refunbilledstatus`.unbilledunbilledflag,
  `epic_refunbilledstatus`.unbilledbilledflag,
  `epic_refunbilledstatus`.dwlastupdatedatetime,
  `epic_refunbilledstatus`.sourcesystemcode,
  `epic_refunbilledstatus`.insertedby,
  `epic_refunbilledstatus`.inserteddtm,
  `epic_refunbilledstatus`.modifiedby,
  `epic_refunbilledstatus`.modifieddtm,
  `epic_refunbilledstatus`.deleteflag
  FROM
    edwpsc.`epic_refunbilledstatus`
;