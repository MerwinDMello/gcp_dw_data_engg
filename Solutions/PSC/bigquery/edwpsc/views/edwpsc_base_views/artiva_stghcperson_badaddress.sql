CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stghcperson_badaddress`
AS SELECT
  `artiva_stghcperson_badaddress`.hcpnbadadr,
  `artiva_stghcperson_badaddress`.pspnbadadrdte,
  `artiva_stghcperson_badaddress`.pspncontactid
  FROM
    edwpsc.`artiva_stghcperson_badaddress`
;