#!/bin/bash

# Each document must have a unique value in this key. Add faked ids with ./add_fake_ids.sh if you need
IDENTIFIER_KEY="md5"

# Loads all .jsonl files in DATA_IN_DIR, writes intermediate files to DATA_OUT_DIR, and deduplicated files to DATA_OUT_DIR/deduplicated/
# DATA_IN_DIR="data_in/is-sv"
# SUB_DIR="is-sv"
# SUB_DIR="da-is"
# SUB_DIR="is-no"
# SUB_DIR="da-no"
# SUB_DIR="en-is"
# SUB_DIR="en-sv"
# SUB_DIR="da-en"
# SUB_DIR="en-no"
# SUB_DIR="no-sv"
SUB_DIR="da-sv"

DATA_IN_DIR="/data/nordic_pile/jsonl_raw/opus_clean/${SUB_DIR}"
DATA_OUT_DIR="/data/nordic_pile/jsonl_raw/opus_dedup/${SUB_DIR}"

# These don't need to be changed
DATA_ADDED_IDS_DIR="${DATA_OUT_DIR}/added_ids"
DATA_DEDUPLICATED_DIR="${DATA_OUT_DIR}/deduplicated"
DATA_REMOVED_IDS_DIR="${DATA_OUT_DIR}/removed_ids"

IDENTIFIED_DUPS_FILE="${DATA_OUT_DIR}/identified_dups.jsonl"
SIMILAR_URL_FILE="${DATA_OUT_DIR}/similar_urls.jsonl"

# SETUP DEDUPLICATION PARAMETERS
# JACCARD_THRESHOLD=0.8
JACCARD_THRESHOLD=0.5
CHAR_N_GRAM=8


# Create dirs and clean up from last run
mkdir -p $DATA_OUT_DIR
# rm -rf $DATA_OUT_DIR/*

mkdir -p $DATA_ADDED_IDS_DIR
mkdir -p $DATA_DEDUPLICATED_DIR
mkdir -p $DATA_REMOVED_IDS_DIR
# mkdir -p $DATA_OUT_DIR/removed_ids

# Find all input data files
work_dir=$(pwd)
cd $DATA_IN_DIR
in_files_arr=(*.jsonl)
cd $work_dir

# Add unique identifiers to all documents, existing urls/ids are kept
echo "Adding temporary unique identifiers to all documents.."
for file_name in "${in_files_arr[@]}"; do
  python add_ids_to_docs.py --in_file $DATA_IN_DIR/$file_name --out_file $DATA_ADDED_IDS_DIR/$file_name --id_key_name $IDENTIFIER_KEY
done

# Create input files argument
in_files_str_arg=""
for file_name in "${in_files_arr[@]}"; do
   in_files_str_arg="$in_files_str_arg $DATA_ADDED_IDS_DIR/$file_name $IDENTIFIER_KEY"
done

# Find duplicates, num_bands does not affect ram usage except that is allows for more parallelism, that then linearly
# increases ram usage per worker
# More seeds heavily increase RAM, but is often faster since it means more hashes/bin => less collisions & Jaccard sims
python find_duplicates.py --inputs $in_files_str_arg --output $IDENTIFIED_DUPS_FILE \
        --heuristic-iter -1 --num-bands 2 --char-n-gram $CHAR_N_GRAM --num-seeds 10 \
        --max-workers-fingerprints 12 --seed 1234
# --jaccard-parallel --max-workers-jaccard 4

# reddit_is, one heavier dedup: 174.2 -> 156.3

# Group to decide what to remove
python group_duplicate_url.py  $IDENTIFIED_DUPS_FILE $SIMILAR_URL_FILE $JACCARD_THRESHOLD

### DEBUGGING ###
# Print similar groups from which only 1 is kept
# in_files_str_arg_debug=""
# for file_name in "${in_files_arr[@]}"; do
#    in_files_str_arg_debug="$in_files_str_arg_debug $DATA_OUT_DIR/added_ids/$file_name"
# done
# python debug_print_groups.py --in_files_added_ids $in_files_str_arg_debug --in_file_groups $DATA_OUT_DIR/$SIMILAR_URL_FILE --id_key_name $IDENTIFIER_KEY \
#           > $DATA_OUT_DIR/duplicate_texts.txt
### END DEBUGGING ###

# Remove and create deduplicated files
for file_name in "${in_files_arr[@]}"; do
   python remove_group_duplicates.py $SIMILAR_URL_FILE $DATA_ADDED_IDS_DIR/$file_name $DATA_DEDUPLICATED_DIR/${file_name} $IDENTIFIER_KEY
done

# echo "Removing added_ids copy of data.."
# rm -rf $DATA_OUT_DIR/added_ids

# Remove unique identifiers from all documents, existing urls/ids are kept
echo "Removing temporary unique identifiers from all documents.."
for file_name in "${in_files_arr[@]}"; do
  python add_ids_to_docs.py --in_file $DATA_DEDUPLICATED_DIR/$file_name --out_file $DATA_REMOVED_IDS_DIR/$file_name --id_key_name $IDENTIFIER_KEY --remove
done

# echo "Removing deduplicated copy of data with added_ids"
# rm -rf $DATA_OUT_DIR/deduplicated
# mv $DATA_OUT_DIR/removed_ids $DATA_OUT_DIR/finished_deduplication
echo "Processed data in ${DATA_OUT_DIR} directory!"
