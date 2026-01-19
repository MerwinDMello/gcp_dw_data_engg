CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtvendorcoidmatrix`
AS SELECT
  `ccu_rprtvendorcoidmatrix`.vendorcoidmatrixkey,
  `ccu_rprtvendorcoidmatrix`.coid,
  `ccu_rprtvendorcoidmatrix`.coidname,
  `ccu_rprtvendorcoidmatrix`.active,
  `ccu_rprtvendorcoidmatrix`.systemvalue,
  `ccu_rprtvendorcoidmatrix`.vendorvalue,
  `ccu_rprtvendorcoidmatrix`.assigneddate,
  `ccu_rprtvendorcoidmatrix`.dwlastupdatedatetime,
  `ccu_rprtvendorcoidmatrix`.insertedby,
  `ccu_rprtvendorcoidmatrix`.inserteddtm,
  `ccu_rprtvendorcoidmatrix`.modifiedby,
  `ccu_rprtvendorcoidmatrix`.modifieddtm
  FROM
    edwpsc_base_views.`ccu_rprtvendorcoidmatrix`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_rprtvendorcoidmatrix`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;