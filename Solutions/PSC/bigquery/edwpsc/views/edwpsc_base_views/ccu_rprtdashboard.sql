CREATE OR REPLACE VIEW edwpsc_base_views.`ccu_rprtdashboard`
AS SELECT
  `ccu_rprtdashboard`.ccudashboardkey,
  `ccu_rprtdashboard`.dashboardreportname,
  `ccu_rprtdashboard`.coid,
  `ccu_rprtdashboard`.owner,
  `ccu_rprtdashboard`.eomdate,
  `ccu_rprtdashboard`.eomdateyyyymm,
  `ccu_rprtdashboard`.category1,
  `ccu_rprtdashboard`.category2,
  `ccu_rprtdashboard`.metriccategory,
  `ccu_rprtdashboard`.mtdcount,
  `ccu_rprtdashboard`.totalmonthcount,
  `ccu_rprtdashboard`.resolveddaysbysubmitdate,
  `ccu_rprtdashboard`.resolveddaysbyassigneemodifieddate,
  `ccu_rprtdashboard`.loaddate,
  `ccu_rprtdashboard`.dwlastupdatedatetime,
  `ccu_rprtdashboard`.insertedby,
  `ccu_rprtdashboard`.inserteddtm,
  `ccu_rprtdashboard`.modifiedby,
  `ccu_rprtdashboard`.modifieddtm,
  `ccu_rprtdashboard`.sourcetablelastloaddate,
  `ccu_rprtdashboard`.providercountactive
  FROM
    edwpsc.`ccu_rprtdashboard`
;