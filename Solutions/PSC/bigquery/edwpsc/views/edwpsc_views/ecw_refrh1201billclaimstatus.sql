CREATE OR REPLACE VIEW edwpsc_views.`ecw_refrh1201billclaimstatus`
AS SELECT
  `ecw_refrh1201billclaimstatus`.rh1201billclaimstatuskey,
  `ecw_refrh1201billclaimstatus`.rh1201billclaimstatusname,
  `ecw_refrh1201billclaimstatus`.dwlastupdatedatetime,
  `ecw_refrh1201billclaimstatus`.sourcesystemcode,
  `ecw_refrh1201billclaimstatus`.insertedby,
  `ecw_refrh1201billclaimstatus`.inserteddtm,
  `ecw_refrh1201billclaimstatus`.modifiedby,
  `ecw_refrh1201billclaimstatus`.modifieddtm,
  `ecw_refrh1201billclaimstatus`.deleteflag
  FROM
    edwpsc_base_views.`ecw_refrh1201billclaimstatus`
;