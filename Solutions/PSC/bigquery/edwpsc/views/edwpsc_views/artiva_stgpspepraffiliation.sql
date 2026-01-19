CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspepraffiliation`
AS SELECT
  `artiva_stgpspepraffiliation`.pspeprafactive,
  `artiva_stgpspepraffiliation`.pspeprafcategory,
  `artiva_stgpspepraffiliation`.pspepraffinishdte,
  `artiva_stgpspepraffiliation`.pspeprafgafid,
  `artiva_stgpspepraffiliation`.pspeprafinid,
  `artiva_stgpspepraffiliation`.pspeprafinstype,
  `artiva_stgpspepraffiliation`.pspeprafkey,
  `artiva_stgpspepraffiliation`.pspeprafperfid,
  `artiva_stgpspepraffiliation`.pspeprafspecialty,
  `artiva_stgpspepraffiliation`.pspeprafstartdte,
  `artiva_stgpspepraffiliation`.pspepraftype
  FROM
    edwpsc_base_views.`artiva_stgpspepraffiliation`
;