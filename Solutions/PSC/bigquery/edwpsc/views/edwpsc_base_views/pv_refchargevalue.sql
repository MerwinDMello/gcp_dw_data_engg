CREATE OR REPLACE VIEW edwpsc_base_views.`pv_refchargevalue`
AS SELECT
  `pv_refchargevalue`.chargevaluekey,
  `pv_refchargevalue`.chargevaluename,
  `pv_refchargevalue`.chargevaluetype,
  `pv_refchargevalue`.chargevaluedesc,
  `pv_refchargevalue`.chargevalueprioritynum,
  `pv_refchargevalue`.chargevaluefrequency,
  `pv_refchargevalue`.chargevaluequery,
  `pv_refchargevalue`.chargevalueconfidencelevelpercent,
  `pv_refchargevalue`.chargevalueconfidencelastvalidateddate,
  `pv_refchargevalue`.chargevaluecreatedby,
  `pv_refchargevalue`.chargevaluecreateddatekey,
  `pv_refchargevalue`.chargevaluelastmodifiedby,
  `pv_refchargevalue`.chargevaluelastprocesseddate,
  `pv_refchargevalue`.chargevaluelasterrormessage,
  `pv_refchargevalue`.enabled,
  `pv_refchargevalue`.sysstarttime,
  `pv_refchargevalue`.sysendtime,
  `pv_refchargevalue`.developeremail
  FROM
    edwpsc.`pv_refchargevalue`
;