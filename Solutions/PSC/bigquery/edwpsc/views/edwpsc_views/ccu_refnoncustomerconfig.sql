CREATE OR REPLACE VIEW edwpsc_views.`ccu_refnoncustomerconfig`
AS SELECT
  `ccu_refnoncustomerconfig`.ccunoncustomerconfigkey,
  `ccu_refnoncustomerconfig`.ccunoncustomerconfiglabel,
  `ccu_refnoncustomerconfig`.coidoperator,
  `ccu_refnoncustomerconfig`.coid,
  `ccu_refnoncustomerconfig`.sourcesystemoperator,
  `ccu_refnoncustomerconfig`.sourcesystemcode,
  `ccu_refnoncustomerconfig`.posoperator,
  `ccu_refnoncustomerconfig`.pos,
  `ccu_refnoncustomerconfig`.procedurecategory1operator,
  `ccu_refnoncustomerconfig`.procedurecategory1,
  `ccu_refnoncustomerconfig`.visitstatusoperator,
  `ccu_refnoncustomerconfig`.visitstatus,
  `ccu_refnoncustomerconfig`.visittypeoperator,
  `ccu_refnoncustomerconfig`.visittype,
  `ccu_refnoncustomerconfig`.lockflagoperator,
  `ccu_refnoncustomerconfig`.lockflag,
  `ccu_refnoncustomerconfig`.currentinventoryowner,
  `ccu_refnoncustomerconfig`.currentinventoryowneroperator,
  `ccu_refnoncustomerconfig`.ccuinventorykeyqryoperator,
  `ccu_refnoncustomerconfig`.ccuinventorykeyqry,
  `ccu_refnoncustomerconfig`.inventoryowner,
  `ccu_refnoncustomerconfig`.inventorytype,
  `ccu_refnoncustomerconfig`.groupassignment,
  `ccu_refnoncustomerconfig`.sourceprimarykeyvalue,
  `ccu_refnoncustomerconfig`.dwlastupdatedatetime,
  `ccu_refnoncustomerconfig`.insertedby,
  `ccu_refnoncustomerconfig`.inserteddtm,
  `ccu_refnoncustomerconfig`.modifiedby,
  `ccu_refnoncustomerconfig`.modifieddtm,
  `ccu_refnoncustomerconfig`.procedurecategory2operator,
  `ccu_refnoncustomerconfig`.procedurecategory2,
  `ccu_refnoncustomerconfig`.inventorytypeoperator,
  `ccu_refnoncustomerconfig`.providerspecialty,
  `ccu_refnoncustomerconfig`.providerspecialtyoperator
  FROM
    edwpsc_base_views.`ccu_refnoncustomerconfig`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_refnoncustomerconfig`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;