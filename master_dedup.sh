#!/bin/bash

LANG="is"
NUM_CHUNKS=1

COMMON_BASE_PATH="/home/joey/code/ai/deduplication_repos/Megatron-deduplication"
# COMMON_BASE_PATH="/data/nordic_pile/jsonl_train_format"

IN_DATA="$COMMON_BASE_PATH/exact_dedup/$LANG"
# Only used if NUM_CHUNKS > 1
INTERMEDIATE_CHUNKED_DATA="$COMMON_BASE_PATH/chunked_before_fuzzy/$LANG"
DEDUP_OUT_DATA="$COMMON_BASE_PATH/fuzzy_dedup/$LANG"


# Make sure a directory exists to write data to
parent_dir_dedup=$(dirname "${DEDUP_OUT_DATA}")
if [ ! -d "$parent_dir_dedup" ]; then
    echo ""
    echo "ERROR: Output directory $parent_dir_dedup does not exist, please create it and add write permissions beforehand."
    exit
fi

# Create lang-specific dir
mkdir -p $DEDUP_OUT_DATA


# Check if chunking needs to be done
if (( NUM_CHUNKS <= 1 )); then

  echo "Deduplicating single directory without chunking."
  bash dedup_single_dir.sh $IN_DATA $DEDUP_OUT_DATA

else

  # Make sure a directory exists to write data to
  parent_dir_chunked=$(dirname "${INTERMEDIATE_CHUNKED_DATA}")
  if [ ! -d "$parent_dir_chunked" ]; then
      echo ""
      echo "ERROR: Output directory $parent_dir_chunked does not exist, please create it and add write permissions beforehand."
      exit
  fi

  # Create lang-specific dir
  mkdir -p $INTERMEDIATE_CHUNKED_DATA


  echo "Deduplicating directory split into $NUM_CHUNKS chunks."
  echo ""

  echo "Splitting into $NUM_CHUNKS chunks..."
  cd chunking
  bash chunk_split.sh $IN_DATA $INTERMEDIATE_CHUNKED_DATA $NUM_CHUNKS
  cd ..

  sleep 3
  echo "Deduplicating..."
  bash dedup_chunks_main.sh $INTERMEDIATE_CHUNKED_DATA $DEDUP_OUT_DATA

  echo "Deduplication done, note that langs/chunks are yet not merged and should be done once each language has been deduplicated"

fi
