CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refcoidconfiguration`
AS SELECT
  `epic_refcoidconfiguration`.coidconfigurationkey,
  `epic_refcoidconfiguration`.coid,
  `epic_refcoidconfiguration`.deptcode,
  `epic_refcoidconfiguration`.coidconfigurationproviderkey,
  `epic_refcoidconfiguration`.coidconfigurationfacilitykey,
  `epic_refcoidconfiguration`.coidconfigurationpracticekey,
  `epic_refcoidconfiguration`.sourceprimarykeyvalue,
  `epic_refcoidconfiguration`.sourcerecordlastupdated,
  `epic_refcoidconfiguration`.dwlastupdatedatetime,
  `epic_refcoidconfiguration`.sourcesystemcode,
  `epic_refcoidconfiguration`.insertedby,
  `epic_refcoidconfiguration`.inserteddtm,
  `epic_refcoidconfiguration`.modifiedby,
  `epic_refcoidconfiguration`.modifieddtm,
  `epic_refcoidconfiguration`.deleteflag,
  `epic_refcoidconfiguration`.regionkey
  FROM
    edwpsc.`epic_refcoidconfiguration`
;