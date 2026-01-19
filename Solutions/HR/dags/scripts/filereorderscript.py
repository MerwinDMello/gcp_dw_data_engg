import re
import argparse
import pandas as pd

parser = argparse.ArgumentParser(
        description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter
    )
parser.add_argument('-i', '--inputfile', required=True, help='Input File Name')
parser.add_argument('-o' ,'--outputfile', required=True, help='Output File Name')
args = parser.parse_args()

in_file_name = args.inputfile
out_file_name = args.outputfile
dtyp = {'1':str,'2':str,'3':str,'4':str,'5':str,'6':str,'7':str,'8':str,'9':str,'10':str,'11':str}
df = pd.read_csv(in_file_name, dtype=dtyp,na_filter=False,delimiter='|',quotechar='"',encoding='cp1252',header=None,names=['0','1','2','3','4','5','6','7','8','9','10'])
df_reorder = df[['1','5','0','2','9','4','7','6','8','10','3']]  
df_reorder.to_csv(out_file_name,index=False,sep = '|', quoting=1, header=None,encoding='UTF-8')
print(df_reorder.head())

