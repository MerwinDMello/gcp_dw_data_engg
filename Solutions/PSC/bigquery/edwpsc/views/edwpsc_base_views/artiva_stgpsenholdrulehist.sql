CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpsenholdrulehist`
AS SELECT
  `artiva_stgpsenholdrulehist`.psenhrhkey,
  `artiva_stgpsenholdrulehist`.psenhrhactdte,
  `artiva_stgpsenholdrulehist`.psenhrhcpidid,
  `artiva_stgpsenholdrulehist`.psenhrhencntrid,
  `artiva_stgpsenholdrulehist`.psenhrhfinclass,
  `artiva_stgpsenholdrulehist`.psenhrhholdcode,
  `artiva_stgpsenholdrulehist`.psenhrhholddesc,
  `artiva_stgpsenholdrulehist`.psenhrhhruid,
  `artiva_stgpsenholdrulehist`.psenhrhreldte
  FROM
    edwpsc.`artiva_stgpsenholdrulehist`
;