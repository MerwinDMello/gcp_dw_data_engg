CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_rprtbillingcoodinatorproductivity`
AS SELECT
  `ecw_rprtbillingcoodinatorproductivity`.weekstartdate,
  `ecw_rprtbillingcoodinatorproductivity`.weekenddate,
  `ecw_rprtbillingcoodinatorproductivity`.savedby,
  `ecw_rprtbillingcoodinatorproductivity`.actiondate,
  `ecw_rprtbillingcoodinatorproductivity`.month,
  `ecw_rprtbillingcoodinatorproductivity`.nexttouchtoccu,
  `ecw_rprtbillingcoodinatorproductivity`.nexttouchtooutbox,
  `ecw_rprtbillingcoodinatorproductivity`.claimsubstatusname,
  `ecw_rprtbillingcoodinatorproductivity`.cocid,
  `ecw_rprtbillingcoodinatorproductivity`.touchcount,
  `ecw_rprtbillingcoodinatorproductivity`.sourcesystem,
  `ecw_rprtbillingcoodinatorproductivity`.inserteddtm,
  `ecw_rprtbillingcoodinatorproductivity`.dwlastupdatedatetime,
  `ecw_rprtbillingcoodinatorproductivity`.billingcoordinatorprodkey
  FROM
    edwpsc.`ecw_rprtbillingcoodinatorproductivity`
;