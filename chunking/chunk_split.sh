#!/bin/bash

LANG="sv"
NUM_CHUNKS=4

ROOT_IN="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_in/$LANG"
ROOT_OUT="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_out/$LANG"

# ROOT_IN="/data/nordic_pile/jsonl_train_format/cleaned"
# ROOT_OUT="/data/nordic_pile/jsonl_train_format/exact_dedup"

echo ""
echo "ROOT_IN: ${ROOT_IN}"
echo "ROOT_OUT: ${ROOT_OUT}"

if [ "$ROOT_IN" == "$ROOT_OUT" ]; then
    echo "ROOT_IN == ROOT_OUT, not allowed."
    exit
fi

# if [ ! -d "$ROOT_OUT" ]; then
#     echo ""
#     echo "ERROR: Output directory $ROOT_OUT does not exist, please create it and add write permissions beforehand."
#     exit
# fi

in_file_paths=$(find $ROOT_IN -name "*.jsonl")

# Create input files argument
in_files_str_arg=""
for path_in_file in $in_file_paths; do
  in_files_str_arg="$in_files_str_arg $path_in_file"
done

cmd="python chunk_split.py
      --input_root_dir $ROOT_IN
      --output_root_dir $ROOT_OUT
      --input_files $in_files_str_arg
      --num_chunks $NUM_CHUNKS"

echo $cmd
echo ""

$cmd
