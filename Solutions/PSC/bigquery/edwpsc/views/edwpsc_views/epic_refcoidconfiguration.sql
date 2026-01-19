CREATE OR REPLACE VIEW edwpsc_views.`epic_refcoidconfiguration`
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
    edwpsc_base_views.`epic_refcoidconfiguration`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_refcoidconfiguration`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;