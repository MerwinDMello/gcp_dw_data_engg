CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeavi`
AS SELECT
  `artiva_stgpspeavi`.pspeaviapprecvdte,
  `artiva_stgpspeavi`.pspeaviappsentdte,
  `artiva_stgpspeavi`.pspeaviattestdte,
  `artiva_stgpspeavi`.pspeavicrcompdte,
  `artiva_stgpspeavi`.pspeavicredcomp,
  `artiva_stgpspeavi`.pspeavicrgrpkey,
  `artiva_stgpspeavi`.pspeavicrstartdte,
  `artiva_stgpspeavi`.pspeavicrstasofdte,
  `artiva_stgpspeavi`.pspeavicrstatus,
  `artiva_stgpspeavi`.pspeavidesc,
  `artiva_stgpspeavi`.pspeavientdesc,
  `artiva_stgpspeavi`.pspeavifacid,
  `artiva_stgpspeavi`.pspeavigafid,
  `artiva_stgpspeavi`.pspeavikey,
  `artiva_stgpspeavi`.pspeaviorigdate,
  `artiva_stgpspeavi`.pspeaviperfid,
  `artiva_stgpspeavi`.pspeavirfccompdte,
  `artiva_stgpspeavi`.pspeavistchgdte,
  `artiva_stgpspeavi`.pspeavitaxid,
  `artiva_stgpspeavi`.pspeavitype
  FROM
    edwpsc_base_views.`artiva_stgpspeavi`
;