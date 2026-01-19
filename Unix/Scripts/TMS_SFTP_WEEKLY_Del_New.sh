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
send "rm 20221031022718_EDWHR_EMP_PERF_GOALS_RPT-REVISED_CY.csv \n"
expect "sftp> "
send "rm 20221031023219_EDWHR_DEV_ACTIVITY_HISTORY_RPT.csv \n"
expect "sftp> "
send "bye\n"
