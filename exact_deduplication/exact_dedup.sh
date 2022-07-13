#!/bin/bash

# ROOT_IN="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_in"
# ROOT_OUT="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_out"
BASE_PATH="/data/nordic_pile/jsonl_train_format"
ROOT_IN="$BASE_PATH/cleaned"
ROOT_OUT="$BASE_PATH/exact_dedup"

echo ""
echo "ROOT_IN: ${ROOT_IN}"
echo "ROOT_OUT: ${ROOT_OUT}"

if [ "$ROOT_IN" == "$ROOT_OUT" ]; then
    echo "ROOT_IN == ROOT_OUT, not allowed."
    exit
fi

if [ ! -d "$ROOT_OUT" ]; then
    echo ""
    echo "ERROR: Output directory $ROOT_OUT does not exist, please create it and add write permissions beforehand."
    exit
fi

in_file_paths=$(find $ROOT_IN -name "*.jsonl")

# Create input files argument
in_files_str_arg=""
for path_in_file in $in_file_paths; do
  in_files_str_arg="$in_files_str_arg $path_in_file"
done

cmd="python exact_dedup.py
      --input_root_dir $ROOT_IN
      --output_root_dir $ROOT_OUT
      --input_files $in_files_str_arg"

echo $cmd
echo ""

$cmd
