#!/usr/bin/expect
set timeout 99999
cd /etl/ST/EDWHR/SrcFiles/
spawn sftp client_hcatm@sftp.peopleclick.com
expect {
"password:" {
send "L4Cb0Lxs\n"
}
"Are you sure you want to continue connecting (yes/no)? " {
send "yes\n"
}
}
expect "sftp> "
send "cd '/outgoing/prodnew'\n"
expect "sftp> "
send "rm 20221028032109_EDWHR_EDUC_HIST_RPT.csv \n"
expect "sftp> "
send "rm 20221028042110_EDWHR_WORK_HIST_RPT.csv \n"
expect "sftp> "
send "rm 20221028053110_EDWHR_COMP_RATING_RPT.csv \n"
expect "sftp> "
send "rm 20221028072611_EDWHR_PERF_RATING_HISTORY_RPT.csv \n"
expect "sftp> "
send "bye\n"
