CREATE OR REPLACE VIEW edwpsc_views.`pv_refcoidconfiguration`
AS SELECT
  `pv_refcoidconfiguration`.coidconfigurationkey,
  `pv_refcoidconfiguration`.coid,
  `pv_refcoidconfiguration`.deptcode,
  `pv_refcoidconfiguration`.coidconfigurationproviderkey,
  `pv_refcoidconfiguration`.coidconfigurationfacilitykey,
  `pv_refcoidconfiguration`.coidconfigurationpracticekey,
  `pv_refcoidconfiguration`.sourceprimarykeyvalue,
  `pv_refcoidconfiguration`.sourcerecordlastupdated,
  `pv_refcoidconfiguration`.dwlastupdatedatetime,
  `pv_refcoidconfiguration`.sourcesystemcode,
  `pv_refcoidconfiguration`.insertedby,
  `pv_refcoidconfiguration`.inserteddtm,
  `pv_refcoidconfiguration`.modifiedby,
  `pv_refcoidconfiguration`.modifieddtm,
  `pv_refcoidconfiguration`.deleteflag,
  `pv_refcoidconfiguration`.sysstarttime,
  `pv_refcoidconfiguration`.sysendtime
  FROM
    edwpsc_base_views.`pv_refcoidconfiguration`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_refcoidconfiguration`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;