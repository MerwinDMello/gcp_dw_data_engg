SELECT DISTINCT * EXCEPT(project_number, project_id, insert_timestamp)FROM
hca-hin-prod-cur-pub.edw_pub_views.hin_iam_policy_bindings
WHERE 
policy_member = 'gh-eim-parallon-cr@hca-hin-dev-proc-parallon.iam.gserviceaccount.com'
AND project_id = 'hca-hin-dev-proc-parallon'
AND DATE(insert_timestamp) = CURRENT_DATE()