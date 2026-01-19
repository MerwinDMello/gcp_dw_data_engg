CREATE OR REPLACE VIEW edwpsc_base_views.`ccu_rprtcoidgroupassignment`
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
    edwpsc.`ccu_rprtcoidgroupassignment`
;