CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtrolloutschedulelist`
AS SELECT
  `ccu_rprtrolloutschedulelist`.rolloutschedulelistkey,
  `ccu_rprtrolloutschedulelist`.coid,
  `ccu_rprtrolloutschedulelist`.ccugolivedate,
  `ccu_rprtrolloutschedulelist`.ccugolivestatusvalue,
  `ccu_rprtrolloutschedulelist`.ccudiscontinueddate,
  `ccu_rprtrolloutschedulelist`.pkactivationstatusvalue,
  `ccu_rprtrolloutschedulelist`.dwlastupdatedatetime,
  `ccu_rprtrolloutschedulelist`.insertedby,
  `ccu_rprtrolloutschedulelist`.inserteddtm,
  `ccu_rprtrolloutschedulelist`.modifiedby,
  `ccu_rprtrolloutschedulelist`.modifieddtm,
  `ccu_rprtrolloutschedulelist`.gme
  FROM
    edwpsc_base_views.`ccu_rprtrolloutschedulelist`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_rprtrolloutschedulelist`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;