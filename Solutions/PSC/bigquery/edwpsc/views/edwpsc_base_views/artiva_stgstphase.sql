CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgstphase`
AS SELECT
  `artiva_stgstphase`.stphsavl,
  `artiva_stgstphase`.stphsid,
  `artiva_stgstphase`.stphsldsc,
  `artiva_stgstphase`.stphsmsec,
  `artiva_stgstphase`.stphspos,
  `artiva_stgstphase`.stphssdsc,
  `artiva_stgstphase`.stphstbl,
  `artiva_stgstphase`.stphstyp,
  `artiva_stgstphase`.stphswkf
  FROM
    edwpsc.`artiva_stgstphase`
;