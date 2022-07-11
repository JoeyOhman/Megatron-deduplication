#!/bin/bash

# This file handles deduplication for list of chunk directories, but in each chunk in isolation, i.e. no chunk pairs


COMMON_PATH="/data/nordic_pile/jsonl_train_format"

ROOT_IN="$COMMON_PATH/exact_dedup/en"
ROOT_OUT="$COMMON_PATH/fuzzy_dedup/en"

chunk_dirs=("chunk1" "chunk2" "chunk3" "chunk4" "chunk5" "chunk6" "chunk7" "chunk8" "chunk9")

for chunk_dir in ${chunk_dirs[@]}; do
  # Create full input path
  chunk_path_in="$ROOT_IN/$chunk_dir"

  # Create output path by replacing ROOT_IN with ROOT_OUT
  chunk_path_out="${chunk_path_in/"$ROOT_IN"/"$ROOT_OUT"}"
  echo "**********************************************************"
  echo "Deduplicating single directory with isolated chunk."
  echo "In dir: $chunk_path_in"
  echo "Out dir: $chunk_path_out"
  bash dedup_single_dir.sh $chunk_path_in $chunk_path_out
done

