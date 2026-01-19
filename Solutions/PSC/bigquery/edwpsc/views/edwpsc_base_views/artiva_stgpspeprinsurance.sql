CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspeprinsurance`
AS SELECT
  `artiva_stgpspeprinsurance`.pspeprinagent,
  `artiva_stgpspeprinsurance`.pspeprincarrier,
  `artiva_stgpspeprinsurance`.pspeprinconemail,
  `artiva_stgpspeprinsurance`.pspeprinconfax,
  `artiva_stgpspeprinsurance`.pspeprinconname,
  `artiva_stgpspeprinsurance`.pspeprinconphone,
  `artiva_stgpspeprinsurance`.pspeprincovtype,
  `artiva_stgpspeprinsurance`.pspeprineffdte,
  `artiva_stgpspeprinsurance`.pspeprinexpdte,
  `artiva_stgpspeprinsurance`.pspepringafid,
  `artiva_stgpspeprinsurance`.pspeprininforceind,
  `artiva_stgpspeprinsurance`.pspeprininid,
  `artiva_stgpspeprinsurance`.pspeprinkey,
  `artiva_stgpspeprinsurance`.pspeprinperfid,
  `artiva_stgpspeprinsurance`.pspeprinpolnum,
  `artiva_stgpspeprinsurance`.pspeprinprimlimagg,
  `artiva_stgpspeprinsurance`.pspeprinprimlimit,
  `artiva_stgpspeprinsurance`.pspeprinprivlimit,
  `artiva_stgpspeprinsurance`.pspeprinretrodte
  FROM
    edwpsc.`artiva_stgpspeprinsurance`
;