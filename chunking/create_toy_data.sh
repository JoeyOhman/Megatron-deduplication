#!/bin/bash

# This file was used to create a toy dataset to try merging of files in a realistic file hierarchy.


ROOT_DATA="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/fuzzy_dedup"
SUB_DIR="web_commoncrawl/mc4/sv"

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
  path=$ROOT_DATA/$dir/$SUB_DIR
  mkdir -p $path

  for file in ${files[@]}; do
    file_path=$path/$file
    # Keep=0
    echo '{"md5": "123", "filename": "asd", "keep": 0, "filters": ["badbad", "lol"], "lang": "oui", "len_char": 2, "len_utf8bytes": 3, "len_words": 1, "len_sents": 1, "text": "'$file_path'"}' > $file_path
  done

done


for dir in ${lang_dirs_no_chunks[@]}; do
  path=$ROOT_DATA/$dir/$SUB_DIR
  mkdir -p $path

  for file in ${files[@]}; do
    # Keep=1, but bad flash back forum
    file_path=$path/$file
    echo '{"forum": ["Samhälle", "Brott och brottsbekämpning"], "md5": "123", "filename": "asd", "keep": 1, "filters": [], "lang": "oui", "len_char": 2, "len_utf8bytes": 3, "len_words": 1, "len_sents": 1, "text": "'$file_path'"}' > $file_path
  done
done

counter=0
for dir in ${dirs_unique_files[@]}; do
  path=$ROOT_DATA/$dir/$SUB_DIR
  mkdir -p $path

  # for file in ${files_unique[@]}; do
  # Keep=1
  file="file_unique$counter.jsonl"
  file_path=$path/$file
  echo '{"md5": "123", "filename": "asd", "keep": 1, "filters": [], "lang": "oui", "len_char": 2, "len_utf8bytes": 3, "len_words": 1, "len_sents": 1, "text": "'$file_path'"}' > $file_path

  counter=1
  # done
done


cd chunking
bash chunk_merge.sh
cd ..
