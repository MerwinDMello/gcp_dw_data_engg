CREATE OR REPLACE VIEW edwpsc_base_views.`ccu_rprtrolloutschedulelist`
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
    edwpsc.`ccu_rprtrolloutschedulelist`
;