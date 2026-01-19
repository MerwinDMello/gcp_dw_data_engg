CREATE OR REPLACE VIEW edwpsc_views.`pv_factuacdetail`
AS SELECT
  `pv_factuacdetail`.uacdetailkey,
  `pv_factuacdetail`.practice,
  `pv_factuacdetail`.paymentnumber,
  `pv_factuacdetail`.transactiondate,
  `pv_factuacdetail`.transactiontype,
  `pv_factuacdetail`.transactionamount,
  `pv_factuacdetail`.transactiondescription,
  `pv_factuacdetail`.dbaccount,
  `pv_factuacdetail`.craccount,
  `pv_factuacdetail`.pkcreateduserid,
  `pv_factuacdetail`.pkcreateddatetime,
  `pv_factuacdetail`.uacdetailpk,
  `pv_factuacdetail`.orguacnumber,
  `pv_factuacdetail`.regionkey,
  `pv_factuacdetail`.sourceaprimarykey,
  `pv_factuacdetail`.insertedby,
  `pv_factuacdetail`.inserteddtm,
  `pv_factuacdetail`.modifiedby,
  `pv_factuacdetail`.modifieddtm,
  `pv_factuacdetail`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_factuacdetail`
;