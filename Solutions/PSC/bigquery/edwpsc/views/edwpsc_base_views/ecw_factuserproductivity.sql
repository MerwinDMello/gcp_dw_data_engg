CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factuserproductivity`
AS SELECT
  `ecw_factuserproductivity`.loadkey,
  `ecw_factuserproductivity`.loaddatekey,
  `ecw_factuserproductivity`.userproductivitykey,
  `ecw_factuserproductivity`.bsuid,
  `ecw_factuserproductivity`.processid,
  `ecw_factuserproductivity`.controllnumber,
  `ecw_factuserproductivity`.employeegroupname,
  `ecw_factuserproductivity`.actiondate,
  `ecw_factuserproductivity`.actionuserid,
  `ecw_factuserproductivity`.claimkey,
  `ecw_factuserproductivity`.invoicenumber,
  `ecw_factuserproductivity`.coid,
  `ecw_factuserproductivity`.regionkey,
  `ecw_factuserproductivity`.guarantorid,
  `ecw_factuserproductivity`.accountnumber,
  `ecw_factuserproductivity`.prpnumber,
  `ecw_factuserproductivity`.ppikey,
  `ecw_factuserproductivity`.documenthandle,
  `ecw_factuserproductivity`.batchid,
  `ecw_factuserproductivity`.sourcesystem,
  `ecw_factuserproductivity`.pool,
  `ecw_factuserproductivity`.poolfunction,
  `ecw_factuserproductivity`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_factuserproductivity`
;