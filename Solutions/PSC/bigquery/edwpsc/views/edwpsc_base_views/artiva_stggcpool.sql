CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stggcpool`
AS SELECT
  `artiva_stggcpool`.gcpoolnum,
  `artiva_stggcpool`.gcpoolact
  FROM
    edwpsc.`artiva_stggcpool`
;