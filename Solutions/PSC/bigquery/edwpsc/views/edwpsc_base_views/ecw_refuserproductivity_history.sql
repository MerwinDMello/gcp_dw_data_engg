CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refuserproductivity_history`
AS SELECT
  `ecw_refuserproductivity_history`.userproductivitykey,
  `ecw_refuserproductivity_history`.userproductivityname,
  `ecw_refuserproductivity_history`.userproductivitytype,
  `ecw_refuserproductivity_history`.userproductivitydesc,
  `ecw_refuserproductivity_history`.userproductivityquery,
  `ecw_refuserproductivity_history`.userproductivitycreatedby,
  `ecw_refuserproductivity_history`.userproductivitycreateddatekey,
  `ecw_refuserproductivity_history`.userproductivitylastmodifiedby,
  `ecw_refuserproductivity_history`.userproductivitylastprocesseddate,
  `ecw_refuserproductivity_history`.userproductivitylasterrormessage,
  `ecw_refuserproductivity_history`.enabled,
  `ecw_refuserproductivity_history`.developeremail,
  `ecw_refuserproductivity_history`.sysstarttime,
  `ecw_refuserproductivity_history`.sysendtime,
  `ecw_refuserproductivity_history`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_refuserproductivity_history`
;