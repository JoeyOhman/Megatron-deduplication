#!/bin/bash

ROOT_DATA="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/fuzzy_dedup"

# These directories contain the file path: fuzzy_dedup/lang/chunk_i/original_data_structure, same file names as lang_dirs_no_chunks inside
chunk_dirs=("sv/chunk_0" "sv/chunk_1" "da/chunk_0" "da/chunk_1" "no/chunk_0" "no/chunk_1")
# These directories contain the file path: fuzzy_dedup/lang/original_data_structure, same file names as chunk_dirs inside
lang_dirs_no_chunks=("is" "en" "non_supported_lang" "dirty")

# The above to will have their files merged
# The one below will have their files copied
# These directories contain the file path: fuzzy_dedup/<familjeliv_code>/original_data_structure, unique files inside
dirs_unique_files=("familjeliv" "code")

files=("file1.jsonl" "file2.jsonl")
files_unique=("file_unique1.jsonl" "file_unique2.jsonl")

for dir in ${chunk_dirs[@]}; do
  path=$ROOT_DATA/$dir
  mkdir -p $path

  for file in ${files[@]}; do
    # TODO: Add other fields
    file_path=$path/$file
    echo '{"md5": "123", "filename": "asd", "text": "'$file_path'", "lang": "oui"}' > $file_path
  done

done


for dir in ${lang_dirs_no_chunks[@]}; do
  path=$ROOT_DATA/$dir
  mkdir -p $path

  for file in ${files[@]}; do
    # TODO: Add other fields
    file_path=$path/$file
    echo '{"md5": "123", "filename": "asd", "text": "'$file_path'", "lang": "oui"}' > $file_path
  done
done

counter=0
for dir in ${dirs_unique_files[@]}; do
  path=$ROOT_DATA/$dir
  mkdir -p $path

  # for file in ${files_unique[@]}; do
  # TODO: Add other fields
  file="file_unique$counter.jsonl"
  file_path=$path/$file
  echo '{"md5": "123", "filename": "asd", "text": "'$file_path'", "lang": "oui"}' > $file_path

  counter=1
  # done
done
