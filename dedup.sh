#!/bin/bash

# Loads all .jsonl files in DATA_IN_DIR, writes intermediate files to DATA_OUT_DIR, and deduplicated files to DATA_OUT_DIR/deduplicated/
DATA_IN_DIR="data_in"
DATA_OUT_DIR="data_out"

IDENTIFIER_KEY="url"
IDENTIFIED_DUPS_FILE="identified_dups.jsonl"
SIMILAR_URL_FILE="similar_urls.jsonl"

# Create dirs and clean up from last run
mkdir -p $DATA_OUT_DIR
rm -rf $DATA_OUT_DIR/*
mkdir -p $DATA_OUT_DIR/added_ids
mkdir -p $DATA_OUT_DIR/deduplicated
mkdir -p $DATA_OUT_DIR/removed_ids

# Find all input data files
work_dir=$(pwd)
cd $DATA_IN_DIR
in_files_arr=(*.jsonl)
cd $work_dir

# Add unique identifiers to all documents, existing urls/ids are kept
for file_name in "${in_files_arr[@]}"; do
  python add_ids_to_docs.py --in_file $DATA_IN_DIR/$file_name --out_file $DATA_OUT_DIR/added_ids/$file_name --id_key_name $IDENTIFIER_KEY
done

# Create input files argument
in_files_str_arg=""
for file_name in "${in_files_arr[@]}"; do
   in_files_str_arg="$in_files_str_arg $DATA_OUT_DIR/added_ids/$file_name $IDENTIFIER_KEY"
done

# Find duplicates
python find_duplicates.py --inputs $in_files_str_arg --output $DATA_OUT_DIR/$IDENTIFIED_DUPS_FILE --heuristic-iter -1 --num-bands 20 --jaccard-parallel

# Group to decide what to remove
python group_duplicate_url.py  $DATA_OUT_DIR/$IDENTIFIED_DUPS_FILE $DATA_OUT_DIR/$SIMILAR_URL_FILE 0.7

### DEBUGGING ###
# Print similar groups from which only 1 is kept
in_files_str_arg_debug=""
for file_name in "${in_files_arr[@]}"; do
   in_files_str_arg_debug="$in_files_str_arg_debug $DATA_OUT_DIR/added_ids/$file_name"
done
python debug_print_groups.py --in_files_added_ids $in_files_str_arg_debug --in_file_groups $DATA_OUT_DIR/$SIMILAR_URL_FILE --id_key_name $IDENTIFIER_KEY \
          > $DATA_OUT_DIR/duplicate_texts.txt
### END DEBUGGING ###

# Remove and create deduplicated files
for file_name in "${in_files_arr[@]}"; do
   python remove_group_duplicates.py $DATA_OUT_DIR/$SIMILAR_URL_FILE $DATA_OUT_DIR/added_ids/$file_name $DATA_OUT_DIR/deduplicated/${file_name}
done

rm -rf $DATA_OUT_DIR/added_ids

# Remove unique identifiers from all documents, existing urls/ids are kept
for file_name in "${in_files_arr[@]}"; do
  python add_ids_to_docs.py --in_file $DATA_OUT_DIR/deduplicated/$file_name --out_file $DATA_OUT_DIR/removed_ids/$file_name --id_key_name $IDENTIFIER_KEY --remove
done

rm -rf $DATA_OUT_DIR/deduplicated
mv $DATA_OUT_DIR/removed_ids $DATA_OUT_DIR/finished_deduplication
