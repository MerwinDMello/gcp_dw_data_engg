CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeperfrelgrp`
AS SELECT
  `artiva_stgpspeperfrelgrp`.psperelgrpid,
  `artiva_stgpspeperfrelgrp`.psperelgrpkey,
  `artiva_stgpspeperfrelgrp`.psperelgrpperfid
  FROM
    edwpsc_base_views.`artiva_stgpspeperfrelgrp`
;