CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refchargevalue_history`
AS SELECT
  `ecw_refchargevalue_history`.chargevaluekey,
  `ecw_refchargevalue_history`.chargevaluename,
  `ecw_refchargevalue_history`.chargevaluetype,
  `ecw_refchargevalue_history`.chargevaluedesc,
  `ecw_refchargevalue_history`.chargevalueprioritynum,
  `ecw_refchargevalue_history`.chargevaluefrequency,
  `ecw_refchargevalue_history`.chargevaluequery,
  `ecw_refchargevalue_history`.chargevalueconfidencelevelpercent,
  `ecw_refchargevalue_history`.chargevalueconfidencelastvalidateddate,
  `ecw_refchargevalue_history`.chargevaluecreatedby,
  `ecw_refchargevalue_history`.chargevaluecreateddatekey,
  `ecw_refchargevalue_history`.chargevaluelastmodifiedby,
  `ecw_refchargevalue_history`.chargevaluelastprocesseddate,
  `ecw_refchargevalue_history`.chargevaluelasterrormessage,
  `ecw_refchargevalue_history`.enabled,
  `ecw_refchargevalue_history`.sysstarttime,
  `ecw_refchargevalue_history`.sysendtime,
  `ecw_refchargevalue_history`.developeremail,
  `ecw_refchargevalue_history`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_refchargevalue_history`
;