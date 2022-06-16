#!/bin/bash

ROOT_IN_CHUNK_DIR="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_out/sv"
ROOT_OUT_CHUNK_DIR="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_out_fuzzy_dedup/sv"

# Find chunk directories and sort them
chunk_dirs_str=$(find $ROOT_IN_CHUNK_DIR -mindepth 1 -maxdepth 1 -type d)
chunk_dirs_str=$(echo $chunk_dirs_str | xargs -n1 | sort | xargs)

# Find number of chunks
num_chunks=$(wc -w <<< "$chunk_dirs_str")
# echo $num_chunks

# Create array from space-separated string
chunk_dirs=( $chunk_dirs_str )
# echo "${chunk_dirs[0]}"

for chunk_dir in $chunk_dirs; do
  if [[ $chunk_dir != *"chunk_"* ]]; then
    echo "ERROR: Sub-directory $chunk_dir in ROOT_IN_CHUNK_DIR=$ROOT_IN_CHUNK_DIR is not a chunk directory!"
    exit
  fi
done

for (( i=0; i<$num_chunks-1; i++ )); do
  main_chunk_in="${chunk_dirs[$i]}"
  main_chunk_out="${main_chunk_in/"$ROOT_IN_CHUNK_DIR"/"$ROOT_OUT_CHUNK_DIR"}"
  # echo $main_chunk_out
  # echo "$i" "$main_chunk_in"
  for (( j=$i+1; j<$num_chunks; j++ )); do
    other_chunk_in="${chunk_dirs[$j]}"
    other_chunk_out="${other_chunk_in/"$ROOT_IN_CHUNK_DIR"/"$ROOT_OUT_CHUNK_DIR"}"
    echo "**************************************************************************************************"
    echo "$i" "$j"
    echo $main_chunk_in
    echo $other_chunk_in
    bash dedup_chunk_pair.sh $main_chunk_in $other_chunk_in $main_chunk_out $other_chunk_out
    # exit
  done
done


# merge duplicate groups
