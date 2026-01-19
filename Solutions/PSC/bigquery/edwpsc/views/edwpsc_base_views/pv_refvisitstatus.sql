CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refvisitstatus`
AS SELECT
  `pv_refvisitstatus`.visitstatuskey,
  `pv_refvisitstatus`.visitstatusname,
  `pv_refvisitstatus`.visitstatusdescription,
  `pv_refvisitstatus`.visitstatusnonbillable,
  `pv_refvisitstatus`.sourceprimarykeyvalue,
  `pv_refvisitstatus`.sourcerecordlastupdated,
  `pv_refvisitstatus`.dwlastupdatedatetime,
  `pv_refvisitstatus`.sourcesystemcode,
  `pv_refvisitstatus`.insertedby,
  `pv_refvisitstatus`.inserteddtm,
  `pv_refvisitstatus`.modifiedby,
  `pv_refvisitstatus`.modifieddtm,
  `pv_refvisitstatus`.deleteflag
  FROM
    edwpsc.`pv_refvisitstatus`
;