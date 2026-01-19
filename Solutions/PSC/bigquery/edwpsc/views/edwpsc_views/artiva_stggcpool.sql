CREATE OR REPLACE VIEW edwpsc_views.`artiva_stggcpool`
AS SELECT
  `artiva_stggcpool`.gcpoolnum,
  `artiva_stggcpool`.gcpoolact
  FROM
    edwpsc_base_views.`artiva_stggcpool`
;