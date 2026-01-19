CREATE OR REPLACE VIEW edwpsc_views.`pv_refunbilledstatus`
AS SELECT
  `pv_refunbilledstatus`.unbilledstatuskey,
  `pv_refunbilledstatus`.unbilledstatuscategory,
  `pv_refunbilledstatus`.unbilledstatussubcategory,
  `pv_refunbilledstatus`.unbilledonholdflag,
  `pv_refunbilledstatus`.unbilledunbilledflag,
  `pv_refunbilledstatus`.unbilledbilledflag,
  `pv_refunbilledstatus`.dwlastupdatedatetime,
  `pv_refunbilledstatus`.sourcesystemcode,
  `pv_refunbilledstatus`.insertedby,
  `pv_refunbilledstatus`.inserteddtm,
  `pv_refunbilledstatus`.modifiedby,
  `pv_refunbilledstatus`.modifieddtm,
  `pv_refunbilledstatus`.deleteflag
  FROM
    edwpsc_base_views.`pv_refunbilledstatus`
;