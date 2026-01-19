CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refrh1201releasestatus`
AS SELECT
  `ecw_refrh1201releasestatus`.rh1201releasestatuskey,
  `ecw_refrh1201releasestatus`.rh1201releasestatusname,
  `ecw_refrh1201releasestatus`.dwlastupdatedatetime,
  `ecw_refrh1201releasestatus`.sourcesystemcode,
  `ecw_refrh1201releasestatus`.insertedby,
  `ecw_refrh1201releasestatus`.inserteddtm,
  `ecw_refrh1201releasestatus`.modifiedby,
  `ecw_refrh1201releasestatus`.modifieddtm,
  `ecw_refrh1201releasestatus`.deleteflag
  FROM
    edwpsc.`ecw_refrh1201releasestatus`
;