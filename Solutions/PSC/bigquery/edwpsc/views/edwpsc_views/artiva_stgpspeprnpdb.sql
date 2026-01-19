CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeprnpdb`
AS SELECT
  `artiva_stgpspeprnpdb`.pspeprnpentity,
  `artiva_stgpspeprnpdb`.pspeprnpgafid,
  `artiva_stgpspeprnpdb`.pspeprnpkey,
  `artiva_stgpspeprnpdb`.pspeprnpperfid,
  `artiva_stgpspeprnpdb`.pspeprnpqrytype,
  `artiva_stgpspeprnpdb`.pspeprnprcvdte,
  `artiva_stgpspeprnpdb`.pspeprnpsubdte
  FROM
    edwpsc_base_views.`artiva_stgpspeprnpdb`
;