CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpsratereference`
AS SELECT
  `artiva_stgpsratereference`.psrcoid,
  `artiva_stgpsratereference`.psrdept,
  `artiva_stgpsratereference`.psrfinclass,
  `artiva_stgpsratereference`.psrkey,
  `artiva_stgpsratereference`.psrprocflg,
  `artiva_stgpsratereference`.psrrate
  FROM
    edwpsc_base_views.`artiva_stgpsratereference`
;