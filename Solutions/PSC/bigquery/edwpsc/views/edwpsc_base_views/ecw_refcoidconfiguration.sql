CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refcoidconfiguration`
AS SELECT
  `ecw_refcoidconfiguration`.coidconfigurationkey,
  `ecw_refcoidconfiguration`.coid,
  `ecw_refcoidconfiguration`.deptcode,
  `ecw_refcoidconfiguration`.coidconfigurationproviderkey,
  `ecw_refcoidconfiguration`.coidconfigurationfacilitykey,
  `ecw_refcoidconfiguration`.coidconfigurationpracticekey,
  `ecw_refcoidconfiguration`.sourceprimarykeyvalue,
  `ecw_refcoidconfiguration`.sourcerecordlastupdated,
  `ecw_refcoidconfiguration`.dwlastupdatedatetime,
  `ecw_refcoidconfiguration`.sourcesystemcode,
  `ecw_refcoidconfiguration`.insertedby,
  `ecw_refcoidconfiguration`.inserteddtm,
  `ecw_refcoidconfiguration`.modifiedby,
  `ecw_refcoidconfiguration`.modifieddtm,
  `ecw_refcoidconfiguration`.deleteflag,
  `ecw_refcoidconfiguration`.regionkey,
  `ecw_refcoidconfiguration`.sysstarttime,
  `ecw_refcoidconfiguration`.sysendtime
  FROM
    edwpsc.`ecw_refcoidconfiguration`
;