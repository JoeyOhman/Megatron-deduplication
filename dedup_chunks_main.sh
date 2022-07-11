#!/bin/bash

# This file handles pair-wise deduplication for all pairs among chunk directories


# ROOT_IN_CHUNK_DIR="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_out/sv"
# ROOT_OUT_CHUNK_DIR="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_out_fuzzy_dedup/sv"
ROOT_IN_CHUNK_DIR=$1
ROOT_OUT_CHUNK_DIR=$2

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
    # echo "**************************************************************************************************"
    # echo "$i" "$j"
    # echo $main_chunk_in
    # echo $other_chunk_in
    bash dedup_chunk_pair.sh $main_chunk_in $other_chunk_in $main_chunk_out $other_chunk_out
  done
done


#############################################################################
########################## MERGE DUPLICATES GROUPS ##########################
#############################################################################
MERGED_SIMILAR_DOCS_FILE="$ROOT_OUT_CHUNK_DIR/similar_documents_merged.jsonl"
similar_docs_file_paths=$(find $ROOT_OUT_CHUNK_DIR -name "similar_documents_chunks_*.jsonl")

# Create input files argument
similar_docs_file_paths_str_arg=""
for similar_doc_file in $similar_docs_file_paths; do

  similar_docs_file_paths_str_arg="$similar_docs_file_paths_str_arg $similar_doc_file"

done

echo $similar_docs_file_paths_str_arg

echo ""
echo "***** merge_duplicate_groups.py *****"

if [ -f "$MERGED_SIMILAR_DOCS_FILE" ]; then
  echo "$MERGED_SIMILAR_DOCS_FILE exists, skipping merge."
else
  python merge_duplicate_groups.py \
          --inputs $similar_docs_file_paths_str_arg \
          --output $MERGED_SIMILAR_DOCS_FILE
fi


################################################################################################
########################## REMOVE DUPLICATES & WRITE OUTPUT DOCUMENTS ##########################
################################################################################################
# Iterate through all input files and remove all duplicates except one within duplicate groups
# Writes output files in the same file structure as the input structure

all_input_files=$(find $ROOT_IN_CHUNK_DIR -name "*.jsonl")

echo ""
echo "***** remove_group_duplicates.py *****"
for path_in_file in $all_input_files; do
  # Replace ROOT_IN with ROOT_OUT
  path_out_file="${path_in_file/"$ROOT_IN_CHUNK_DIR"/"$ROOT_OUT_CHUNK_DIR"}"

  # Find output directory and create it
  out_dir="$(dirname "${path_out_file}")"
  mkdir -p $out_dir

  # Remove duplicates and write output files
  python remove_group_duplicates.py \
          $MERGED_SIMILAR_DOCS_FILE \
          $path_in_file \
          $path_out_file \
          md5
done


echo "Final data in $ROOT_OUT_CHUNK_DIR!"
