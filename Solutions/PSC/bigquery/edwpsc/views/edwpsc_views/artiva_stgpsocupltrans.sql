CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpsocupltrans`
AS SELECT
  `artiva_stgpsocupltrans`.psoctcategory,
  `artiva_stgpsocupltrans`.psoctcrtdte,
  `artiva_stgpsocupltrans`.psoctcrttime,
  `artiva_stgpsocupltrans`.psoctexpdte,
  `artiva_stgpsocupltrans`.psoctfieldid,
  `artiva_stgpsocupltrans`.psoctfrom,
  `artiva_stgpsocupltrans`.psoctkey,
  `artiva_stgpsocupltrans`.psoctocid,
  `artiva_stgpsocupltrans`.psoctto
  FROM
    edwpsc_base_views.`artiva_stgpsocupltrans`
;