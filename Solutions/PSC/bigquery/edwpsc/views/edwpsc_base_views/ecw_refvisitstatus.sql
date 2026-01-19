CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refvisitstatus`
AS SELECT
  `ecw_refvisitstatus`.visitstatuskey,
  `ecw_refvisitstatus`.visitstatusname,
  `ecw_refvisitstatus`.visitstatusdescription,
  `ecw_refvisitstatus`.visitstatusnonbillable,
  `ecw_refvisitstatus`.sourceprimarykeyvalue,
  `ecw_refvisitstatus`.sourcerecordlastupdated,
  `ecw_refvisitstatus`.dwlastupdatedatetime,
  `ecw_refvisitstatus`.sourcesystemcode,
  `ecw_refvisitstatus`.insertedby,
  `ecw_refvisitstatus`.inserteddtm,
  `ecw_refvisitstatus`.modifiedby,
  `ecw_refvisitstatus`.modifieddtm,
  `ecw_refvisitstatus`.deleteflag,
  `ecw_refvisitstatus`.sysstarttime,
  `ecw_refvisitstatus`.sysendtime
  FROM
    edwpsc.`ecw_refvisitstatus`
;