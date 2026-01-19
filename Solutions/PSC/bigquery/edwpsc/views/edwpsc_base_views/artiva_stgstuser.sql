CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgstuser`
AS SELECT
  `artiva_stgstuser`.userid,
  `artiva_stgstuser`.uafirst,
  `artiva_stgstuser`.ualast,
  `artiva_stgstuser`.uafullname,
  `artiva_stgstuser`.hcuatitle,
  `artiva_stgstuser`.hcuadept,
  `artiva_stgstuser`.uaoffice,
  `artiva_stgstuser`.uassodomain
  FROM
    edwpsc.`artiva_stgstuser`
;