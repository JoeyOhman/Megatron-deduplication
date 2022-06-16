#!/bin/bash

########################################################################
########################## PARAMETER SETTINGS ##########################
########################################################################
# Absolute paths!
# ROOT_IN="/data/nordic_pile/jsonl_train_format/cleaned"
# ROOT_OUT="/data/nordic_pile/jsonl_train_format/deduplicated"
ROOT_IN="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_out/sv"
ROOT_OUT="/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_out_fuzzy_dedup/sv"

IDENTIFIED_DUPLICATES_FILE="$ROOT_OUT/identified_duplicates.jsonl"
SIMILAR_ID_FILE="$ROOT_OUT/similar_documents.jsonl"

# Each document must have a unique value in this key.
IDENTIFIER_KEY="md5"


# LSH-minhash settings
CHAR_N_GRAM=10  #  Linked to from Nvidia Megatron Bootcamp: http://snap.stanford.edu/class/cs246-2012/slides/03-lsh.pdf
JACCARD_THRESHOLD=0.5  # The Pile, looks sound after qualitative checks
NUM_SEEDS=10  # The Pile #hashes
NUM_BINS=2  # Empirically set, only options with 10 seeds is 1, 2, 5, or 10. 2 seems to give best results/cost

# Parallelization settings
MAX_WORKERS_FINGERPRINTS=4
MAX_WORKERS_JACCARD=2


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


#####################################################################
########################## FIND DUPLICATES ##########################
#####################################################################
# Create input files argument
in_files_str_arg=""
for path_in_file in $in_file_paths; do

  in_files_str_arg="$in_files_str_arg $path_in_file"

done

# Find duplicates: Creates hashes for all documents, and does jaccard similarity on all documents with hash collisions
# Uses LSH minhash => approximate comparisons to avoid O(N^2) complexity
echo ""
echo "***** find_duplicates.py *****"
python find_duplicates.py \
          --inputs $in_files_str_arg \
          --doc_id_key $IDENTIFIER_KEY \
          --output $IDENTIFIED_DUPLICATES_FILE \
          --heuristic_iter -1 \
          --char_n_gram $CHAR_N_GRAM \
          --jaccard_threshold $JACCARD_THRESHOLD \
          --num_seeds $NUM_SEEDS \
          --num_bands $NUM_BINS \
          --max_workers_fingerprints $MAX_WORKERS_FINGERPRINTS \
          --max_workers_jaccard $MAX_WORKERS_JACCARD \
          --seed 1234


######################################################################
########################## GROUP DUPLICATES ##########################
######################################################################
# Groups documents that are similar
echo ""
echo "***** group_duplicate_url.py *****"
python group_duplicate_url.py \
        $IDENTIFIED_DUPLICATES_FILE \
        $SIMILAR_ID_FILE \
        $JACCARD_THRESHOLD


################################################################################################
########################## REMOVE DUPLICATES & WRITE OUTPUT DOCUMENTS ##########################
################################################################################################
# Iterate through all input files and remove all duplicates except one within duplicate groups
# Writes output files in the same file structure as the input structure
echo ""
echo "***** remove_group_duplicates.py *****"
for path_in_file in $in_file_paths; do
  # Replace ROOT_IN with ROOT_OUT
  path_out_file="${path_in_file/"$ROOT_IN"/"$ROOT_OUT"}"

  # Find output directory and create it
  out_dir="$(dirname "${path_out_file}")"
  mkdir -p $out_dir

  # Remove duplicates and write output files
  python remove_group_duplicates.py \
          $SIMILAR_ID_FILE \
          $path_in_file \
          $path_out_file \
          $IDENTIFIER_KEY
done


echo "Final data in $ROOT_OUT!"
