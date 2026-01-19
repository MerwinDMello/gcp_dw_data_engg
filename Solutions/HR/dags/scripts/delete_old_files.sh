#!/bin/bash

FOLDER_PATH="/opt/tdgcp/edwhr/taleo"
CURRENT_DATE=$(date +%Y%m%d)
THREE_DAYS_AGO=$(date -d "3 days ago" +%Y%m%d)

# Iterate over the files in the specified folder path
for file in "$FOLDER_PATH"/*; do
    if [[ -f "$file" ]]; then
        filename=$(basename "$file")
        extension="${filename##*.}"
        filename_without_extension="${filename%.*}"
        
        # Loop until there are no more extensions in the filename
        while [[ "$filename_without_extension" != "$extension" ]]; do
            filename_without_extension="${filename_without_extension%.*}"
            extension="${filename_without_extension##*.}"
        done
        
        date_part="${filename_without_extension##*_}"

        if [[ "$date_part" =~ ^[0-9]{8}$ && "$date_part" -lt "$THREE_DAYS_AGO" ]]; then
            rm "$file"
            echo "Deleted file: $file"
        fi
    fi
done
