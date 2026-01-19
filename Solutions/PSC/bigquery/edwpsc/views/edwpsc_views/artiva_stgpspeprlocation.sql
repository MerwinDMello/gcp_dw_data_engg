CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeprlocation`
AS SELECT
  `artiva_stgpspeprlocation`.pspeprlocactive,
  `artiva_stgpspeprlocation`.pspeprlocaddrtyp,
  `artiva_stgpspeprlocation`.pspeprloccoid,
  `artiva_stgpspeprlocation`.pspeprlocfrmdte,
  `artiva_stgpspeprlocation`.pspeprlocgafid,
  `artiva_stgpspeprlocation`.pspeprlocgrplocid,
  `artiva_stgpspeprlocation`.pspeprlockey,
  `artiva_stgpspeprlocation`.pspeprlocperfid,
  `artiva_stgpspeprlocation`.pspeprlocsilent,
  `artiva_stgpspeprlocation`.pspeprloctermed,
  `artiva_stgpspeprlocation`.pspeprloctodte,
  `artiva_stgpspeprlocation`.pspeprlocloaddte,
  `artiva_stgpspeprlocation`.pspeprlocnpi,
  `artiva_stgpspeprlocation`.pspeprlocthloc
  FROM
    edwpsc_base_views.`artiva_stgpspeprlocation`
;