import re
import argparse

parser = argparse.ArgumentParser(
        description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter
    )
parser.add_argument('-i', '--inputfile', required=True, help='Input File Name')
parser.add_argument('-o' ,'--outputfile', required=True, help='Output File Name')
args = parser.parse_args()

in_file_name = args.inputfile
out_file_name = args.outputfile
with open(in_file_name, encoding='utf-8') as infile, open(out_file_name, "w") as out_file:
    for line in infile:
        line = re.sub(r"(?!^)(?<![\|])(?<![\"])(?<![\n])[\"](?![\|])(?![\"])(?![\n])(?!$)", "\"\"", line)
        line = re.sub(r"(?!^)(?<![\|])(?<![\"])(?<![\n])[\"](?=\"\|)", "\"\"", line)
        line = re.sub(r"(?<=\|\")[\"](?![\|])(?![\"])(?![\n])(?!$)", "\"\"", line)
        out_file.write(line)

