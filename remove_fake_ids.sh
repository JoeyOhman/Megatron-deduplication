#!/bin/bash

DATA_IN_DIR="data_out/deduplicated"
DATA_OUT_TMP="data_out/removed_ids"
IDENTIFIER_KEY="md5"

mkdir -p $DATA_OUT_TMP

# Find all input data files
work_dir=$(pwd)
cd $DATA_IN_DIR
in_files_arr=(*.jsonl)
cd $work_dir

echo "Removing temporary unique identifiers from all documents.."
# Remove unique identifiers from all documents, existing urls/ids are kept
for file_name in "${in_files_arr[@]}"; do
  python add_ids_to_docs.py --in_file $DATA_IN_DIR/$file_name --out_file $DATA_OUT_TMP/$file_name --id_key_name $IDENTIFIER_KEY --remove
done

echo "Removing deduplicated copy of data with added_ids"
rm -rf $DATA_IN_DIR
mv $DATA_OUT_TMP $DATA_IN_DIR
echo "Final data in finished_deduplication directory!"

