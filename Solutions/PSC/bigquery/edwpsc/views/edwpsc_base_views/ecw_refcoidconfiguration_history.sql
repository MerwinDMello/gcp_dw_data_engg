CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refcoidconfiguration_history`
AS SELECT
  `ecw_refcoidconfiguration_history`.coidconfigurationkey,
  `ecw_refcoidconfiguration_history`.coid,
  `ecw_refcoidconfiguration_history`.deptcode,
  `ecw_refcoidconfiguration_history`.coidconfigurationproviderkey,
  `ecw_refcoidconfiguration_history`.coidconfigurationfacilitykey,
  `ecw_refcoidconfiguration_history`.coidconfigurationpracticekey,
  `ecw_refcoidconfiguration_history`.sourceprimarykeyvalue,
  `ecw_refcoidconfiguration_history`.sourcerecordlastupdated,
  `ecw_refcoidconfiguration_history`.dwlastupdatedatetime,
  `ecw_refcoidconfiguration_history`.sourcesystemcode,
  `ecw_refcoidconfiguration_history`.insertedby,
  `ecw_refcoidconfiguration_history`.inserteddtm,
  `ecw_refcoidconfiguration_history`.modifiedby,
  `ecw_refcoidconfiguration_history`.modifieddtm,
  `ecw_refcoidconfiguration_history`.deleteflag,
  `ecw_refcoidconfiguration_history`.regionkey,
  `ecw_refcoidconfiguration_history`.sysstarttime,
  `ecw_refcoidconfiguration_history`.sysendtime
  FROM
    edwpsc.`ecw_refcoidconfiguration_history`
;