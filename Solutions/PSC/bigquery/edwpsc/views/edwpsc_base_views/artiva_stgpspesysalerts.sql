CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspesysalerts`
AS SELECT
  `artiva_stgpspesysalerts`.pspesysalractdte,
  `artiva_stgpspesysalerts`.pspesysalractive,
  `artiva_stgpspesysalerts`.pspesysalrcrtdte,
  `artiva_stgpspesysalerts`.pspesysalrcrttim,
  `artiva_stgpspesysalerts`.pspesysalrcrtuser,
  `artiva_stgpspesysalerts`.pspesysalrexpdte,
  `artiva_stgpspesysalerts`.pspesysalrgrpaviflg,
  `artiva_stgpspesysalerts`.pspesysalrgrpid,
  `artiva_stgpspesysalerts`.pspesysalrkey,
  `artiva_stgpspesysalerts`.pspesysalrprovid,
  `artiva_stgpspesysalerts`.pspesysalrsdesc,
  `artiva_stgpspesysalerts`.pspesysalrtext
  FROM
    edwpsc.`artiva_stgpspesysalerts`
;