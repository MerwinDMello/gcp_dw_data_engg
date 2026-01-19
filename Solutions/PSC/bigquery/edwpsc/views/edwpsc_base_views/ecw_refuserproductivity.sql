CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refuserproductivity`
AS SELECT
  `ecw_refuserproductivity`.userproductivitykey,
  `ecw_refuserproductivity`.userproductivityname,
  `ecw_refuserproductivity`.userproductivitytype,
  `ecw_refuserproductivity`.userproductivitydesc,
  `ecw_refuserproductivity`.userproductivityquery,
  `ecw_refuserproductivity`.userproductivitycreatedby,
  `ecw_refuserproductivity`.userproductivitycreateddatekey,
  `ecw_refuserproductivity`.userproductivitylastmodifiedby,
  `ecw_refuserproductivity`.userproductivitylastprocesseddate,
  `ecw_refuserproductivity`.userproductivitylasterrormessage,
  `ecw_refuserproductivity`.enabled,
  `ecw_refuserproductivity`.developeremail,
  `ecw_refuserproductivity`.sysstarttime,
  `ecw_refuserproductivity`.sysendtime,
  `ecw_refuserproductivity`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_refuserproductivity`
;