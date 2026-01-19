CREATE OR REPLACE VIEW edwpsc_views.`pv_stgerafiletransactionnumber`
AS SELECT
  `pv_stgerafiletransactionnumber`.trn_id,
  `pv_stgerafiletransactionnumber`.bpr_id,
  `pv_stgerafiletransactionnumber`.fileid,
  `pv_stgerafiletransactionnumber`.trn01,
  `pv_stgerafiletransactionnumber`.trn02,
  `pv_stgerafiletransactionnumber`.trn03,
  `pv_stgerafiletransactionnumber`.trn04,
  `pv_stgerafiletransactionnumber`.trn05,
  `pv_stgerafiletransactionnumber`.trn06,
  `pv_stgerafiletransactionnumber`.trn07,
  `pv_stgerafiletransactionnumber`.trn08,
  `pv_stgerafiletransactionnumber`.trn09,
  `pv_stgerafiletransactionnumber`.trn10,
  `pv_stgerafiletransactionnumber`.trnsegment,
  `pv_stgerafiletransactionnumber`.inserteddtm,
  `pv_stgerafiletransactionnumber`.gs_id,
  `pv_stgerafiletransactionnumber`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pv_stgerafiletransactionnumber`
;