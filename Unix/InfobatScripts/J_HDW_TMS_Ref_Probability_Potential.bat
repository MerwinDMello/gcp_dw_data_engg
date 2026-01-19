WF.sh J_HDW_TMS_Ref_Probability_Potential
RC=$? 
if [ $RC = 0 ]; 
      then 
         echo "Job execution J_HDW_TMS_Ref_Probability_Potential is Successful."
         exit 0 
      else
         echo "Job execution J_HDW_TMS_Ref_Probability_Potential is Failed."
         exit $RC
fi
