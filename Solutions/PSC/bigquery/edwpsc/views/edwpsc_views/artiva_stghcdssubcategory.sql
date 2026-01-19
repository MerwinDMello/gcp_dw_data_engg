CREATE OR REPLACE VIEW edwpsc_views.`artiva_stghcdssubcategory`
AS SELECT
  `artiva_stghcdssubcategory`.hcdsactiongroup,
  `artiva_stghcdssubcategory`.hcdsdcid,
  `artiva_stghcdssubcategory`.hcdsdenialtyp,
  `artiva_stghcdssubcategory`.hcdsdesc,
  `artiva_stghcdssubcategory`.hcdsid,
  `artiva_stghcdssubcategory`.psdsfollowupsubcat,
  `artiva_stghcdssubcategory`.psdsiet,
  `artiva_stghcdssubcategory`.psdspriority,
  `artiva_stghcdssubcategory`.psdssource
  FROM
    edwpsc_base_views.`artiva_stghcdssubcategory`
;