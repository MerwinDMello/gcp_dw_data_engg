CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgstworkflow`
AS SELECT
  `artiva_stgstworkflow`.stwkfconv,
  `artiva_stgstworkflow`.stwkfid,
  `artiva_stgstworkflow`.stwkfinit,
  `artiva_stgstworkflow`.stwkfldsc,
  `artiva_stgstworkflow`.stwkfmsec,
  `artiva_stgstworkflow`.stwkfparnt,
  `artiva_stgstworkflow`.stwkfsdsc,
  `artiva_stgstworkflow`.stwkftbl
  FROM
    edwpsc.`artiva_stgstworkflow`
;