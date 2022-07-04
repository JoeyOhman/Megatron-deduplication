#!/bin/bash

# COMMON_PATH="/home/joey/code/ai/deduplication_repos/Megatron-deduplication"
COMMON_PATH="/data/nordic_pile/jsonl_train_format"
ROOT_IN="$COMMON_PATH/final_data/kept"
FILE_OUT="$COMMON_PATH/subset/subset.jsonl"


in_file_paths=$(find $ROOT_IN -name "*.jsonl")

# Create input files argument
in_files_str_arg=""
for path_in_file in $in_file_paths; do
  in_files_str_arg="$in_files_str_arg $path_in_file"
done

echo $in_files_str_arg

python extract.py --input_files $in_files_str_arg --output_file $FILE_OUT --root_in $ROOT_IN
