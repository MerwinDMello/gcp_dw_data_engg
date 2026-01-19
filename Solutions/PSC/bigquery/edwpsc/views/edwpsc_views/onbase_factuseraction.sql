CREATE OR REPLACE VIEW edwpsc_views.`onbase_factuseraction`
AS SELECT
  `onbase_factuseraction`.onbaseuseractionid,
  `onbase_factuseraction`.transactionnum,
  `onbase_factuseraction`.dochandleid,
  `onbase_factuseraction`.actionitem,
  `onbase_factuseraction`.actiondate,
  `onbase_factuseraction`.itemname,
  `onbase_factuseraction`.actionby,
  `onbase_factuseraction`.actioncategory,
  `onbase_factuseraction`.actionnum,
  `onbase_factuseraction`.subactionnum,
  `onbase_factuseraction`.dwlastupdatedatetime,
  `onbase_factuseraction`.sourcesystemcode,
  `onbase_factuseraction`.insertedby,
  `onbase_factuseraction`.inserteddtm
  FROM
    edwpsc_base_views.`onbase_factuseraction`
;