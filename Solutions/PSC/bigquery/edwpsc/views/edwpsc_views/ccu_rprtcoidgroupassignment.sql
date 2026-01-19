CREATE OR REPLACE VIEW edwpsc_views.`ccu_rprtcoidgroupassignment`
AS SELECT
  `ccu_rprtcoidgroupassignment`.ccucoidgroupassignmentkey,
  `ccu_rprtcoidgroupassignment`.coderpracticename,
  `ccu_rprtcoidgroupassignment`.coid,
  `ccu_rprtcoidgroupassignment`.assignmentgroupvalue,
  `ccu_rprtcoidgroupassignment`.coidspecialty,
  `ccu_rprtcoidgroupassignment`.dwlastupdatedatetime,
  `ccu_rprtcoidgroupassignment`.insertedby,
  `ccu_rprtcoidgroupassignment`.inserteddtm,
  `ccu_rprtcoidgroupassignment`.modifiedby,
  `ccu_rprtcoidgroupassignment`.modifieddtm
  FROM
    edwpsc_base_views.`ccu_rprtcoidgroupassignment`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ccu_rprtcoidgroupassignment`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;