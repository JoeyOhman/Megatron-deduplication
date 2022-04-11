#!/bin/bash

# Loads all .jsonl files in DATA_IN_DIR, writes intermediate files to DATA_OUT_DIR, and deduplicated files to DATA_OUT_DIR/deduplicated/
DATA_IN_DIR="data_in"
DATA_OUT_DIR="data_out"

# Tests on one reddit language
# 0.6, 7 => 193.2MB -> 155MB, 1m55s
# 0.7, 7 => 193.2MB -> 164.9MB, 1m54s
# 0.8, 7 => 193.2MB -> 173.4MB, 1m54s
# 0.8, 8 => 193.2MB -> 173.7MB, 1m46s
# 0.8, 9 => 193.2MB -> 173.8MB, 1m45s
# 0.8, 10 => 193.2MB -> 173.9MB, 1m46s

# Tests on opus file
# 0.6, 7 => 189.8MB -> 189.8MB, 2m44s
# 0.6, 5 => 189.8MB -> MB,
# 0.6, 7 => 189.8MB -> MB,

JACCARD_THRESHOLD=0.6
CHAR_N_GRAM=7
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


# 50K docs from opus, chars=7, jac_sim=0.6, 5 workers
# num-bands 5, num-seeds 10 => 180MB RAM, 4.8s, 9.0MB => 8.7MB
# num-bands 10, num-seeds 10 => 200MB RAM, 5m52s, 9.0MB => 8.7MB
# num-bands 10, num-seeds 20 => 1200MB RAM, 7s, 9.0MB => 8.7MB
# num-bands 20, num-seeds 20 => 1200MB RAM, 14m40s, 9.0MB => 8.7MB
# num-bands 10, num-seeds 100 => 1400MB RAM, 5.3s, => 9.0MB => 8.8MB


# icelandic reddit, chars=7, jac_sim=0.6, 5 workers
# num-bands 2, num-seeds 10 => 1600MB RAM, 31s, 193.2MB => 170MB
# num-bands 5, num-seeds 10 => ~3000MB RAM, >60m, 193.2MB => ?
# num-bands 10, num-seeds 10 => 3700MB RAM, >60m, 193.2MB => ?

# num-bands 2, num-seeds 12 => 1800MB RAM, 33s, 193.2MB => 172MB
# num-bands 3, num-seeds 12 => 2400MB RAM, 43s, 193.2MB => 164MB
# num-bands 4, num-seeds 12 => 3500MB RAM, 1m1s, 193.2MB => 158MB
# num-bands 6, num-seeds 12 => 4500MB RAM, , 193.2MB =>

# Find duplicates, num_bands does not affect ram usage except that is allows for more parallelism, that then linearly
# increases ram usage per worker
# More seeds heavily increase RAM, but is often faster since it means more hashes/bin => less collisions & Jaccard sims
python find_duplicates.py --inputs $in_files_str_arg --output $DATA_OUT_DIR/$IDENTIFIED_DUPS_FILE \
        --heuristic-iter -1 --num-bands 4 --char-n-gram $CHAR_N_GRAM --num-seeds 12 \
        --max-workers 5
# --jaccard-parallel
# Group to decide what to remove
python group_duplicate_url.py  $DATA_OUT_DIR/$IDENTIFIED_DUPS_FILE $DATA_OUT_DIR/$SIMILAR_URL_FILE $JACCARD_THRESHOLD

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
