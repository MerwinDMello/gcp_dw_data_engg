CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refcoidconfiguration_history`
AS SELECT
  `pv_refcoidconfiguration_history`.coidconfigurationkey,
  `pv_refcoidconfiguration_history`.coid,
  `pv_refcoidconfiguration_history`.deptcode,
  `pv_refcoidconfiguration_history`.coidconfigurationproviderkey,
  `pv_refcoidconfiguration_history`.coidconfigurationfacilitykey,
  `pv_refcoidconfiguration_history`.coidconfigurationpracticekey,
  `pv_refcoidconfiguration_history`.sourceprimarykeyvalue,
  `pv_refcoidconfiguration_history`.sourcerecordlastupdated,
  `pv_refcoidconfiguration_history`.dwlastupdatedatetime,
  `pv_refcoidconfiguration_history`.sourcesystemcode,
  `pv_refcoidconfiguration_history`.insertedby,
  `pv_refcoidconfiguration_history`.inserteddtm,
  `pv_refcoidconfiguration_history`.modifiedby,
  `pv_refcoidconfiguration_history`.modifieddtm,
  `pv_refcoidconfiguration_history`.deleteflag,
  `pv_refcoidconfiguration_history`.sysstarttime,
  `pv_refcoidconfiguration_history`.sysendtime
  FROM
    edwpsc.`pv_refcoidconfiguration_history`
;