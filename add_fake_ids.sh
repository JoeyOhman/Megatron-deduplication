#!/bin/bash

# DATA_IN_DIR="data_in/is-sv"
DATA_IN_DIR="data_in/en-is"
DATA_OUT_TMP="data_in/added_ids"
IDENTIFIER_KEY="md5"

mkdir -p $DATA_OUT_TMP

# Find all input data files
work_dir=$(pwd)
cd $DATA_IN_DIR
in_files_arr=(*.jsonl)
cd $work_dir

# Add unique identifiers to all documents, existing urls/ids are kept
echo "Adding temporary unique identifiers to all documents.."
for file_name in "${in_files_arr[@]}"; do
  python add_ids_to_docs.py --in_file $DATA_IN_DIR/$file_name --out_file $DATA_OUT_TMP/$file_name --id_key_name $IDENTIFIER_KEY
done

echo "Replacing original copy of data with added ids"
rm -f $DATA_IN_DIR/*.jsonl
mv $DATA_OUT_TMP/*.jsonl $DATA_IN_DIR/
rmdir $DATA_OUT_TMP
echo "Final data in finished_deduplication directory!"
