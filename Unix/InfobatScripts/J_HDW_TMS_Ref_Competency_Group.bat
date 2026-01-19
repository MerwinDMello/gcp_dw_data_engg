WF.sh J_HDW_TMS_Ref_Competency_Group
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Competency_Group is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Competency_Group is Failed."
         exit $RC
fi
