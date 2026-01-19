CREATE OR REPLACE VIEW edwpsc_views.`ecw_refchargevalue`
AS SELECT
  `ecw_refchargevalue`.chargevaluekey,
  `ecw_refchargevalue`.chargevaluename,
  `ecw_refchargevalue`.chargevaluetype,
  `ecw_refchargevalue`.chargevaluedesc,
  `ecw_refchargevalue`.chargevalueprioritynum,
  `ecw_refchargevalue`.chargevaluefrequency,
  `ecw_refchargevalue`.chargevaluequery,
  `ecw_refchargevalue`.chargevalueconfidencelevelpercent,
  `ecw_refchargevalue`.chargevalueconfidencelastvalidateddate,
  `ecw_refchargevalue`.chargevaluecreatedby,
  `ecw_refchargevalue`.chargevaluecreateddatekey,
  `ecw_refchargevalue`.chargevaluelastmodifiedby,
  `ecw_refchargevalue`.chargevaluelastprocesseddate,
  `ecw_refchargevalue`.chargevaluelasterrormessage,
  `ecw_refchargevalue`.enabled,
  `ecw_refchargevalue`.sysstarttime,
  `ecw_refchargevalue`.sysendtime,
  `ecw_refchargevalue`.developeremail,
  `ecw_refchargevalue`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_refchargevalue`
;