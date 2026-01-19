CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refvisitstatus_history`
AS SELECT
  `ecw_refvisitstatus_history`.visitstatuskey,
  `ecw_refvisitstatus_history`.visitstatusname,
  `ecw_refvisitstatus_history`.visitstatusdescription,
  `ecw_refvisitstatus_history`.visitstatusnonbillable,
  `ecw_refvisitstatus_history`.sourceprimarykeyvalue,
  `ecw_refvisitstatus_history`.sourcerecordlastupdated,
  `ecw_refvisitstatus_history`.dwlastupdatedatetime,
  `ecw_refvisitstatus_history`.sourcesystemcode,
  `ecw_refvisitstatus_history`.insertedby,
  `ecw_refvisitstatus_history`.inserteddtm,
  `ecw_refvisitstatus_history`.modifiedby,
  `ecw_refvisitstatus_history`.modifieddtm,
  `ecw_refvisitstatus_history`.deleteflag,
  `ecw_refvisitstatus_history`.sysstarttime,
  `ecw_refvisitstatus_history`.sysendtime
  FROM
    edwpsc.`ecw_refvisitstatus_history`
;