CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspepaylinkgroups`
AS SELECT
  `artiva_stgpspepaylinkgroups`.ppspepaylkgrpdesc,
  `artiva_stgpspepaylinkgroups`.pspepaylkgrpkey
  FROM
    edwpsc.`artiva_stgpspepaylinkgroups`
;