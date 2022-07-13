# Fuzzy deduplication using LSH

**This repository is a modified/extended version of https://github.com/NVIDIA/Megatron-LM/tree/main/tools/openwebtext**

## Exact Deduplication

### Usage
The subdirectory `exact_deduplication/` contains the exact deduplication files.

1. `cd exact_deduplication`
2. Specify the absolute path `BASE_PATH` within `exact_dedup.sh`
3. Run deduplication: `./exact_dedup.sh`

Apart from the exactly deduplicated & segregated files in the output directory, this
will create a pickle checkpoint every 10th file processed. With many small files, increasing 
this interval will significantly speed up the process. 

In case of crash or early stopping of the script, it can then be restarted and load the 
previous checkpoint. Note that it skips all files already existing in the output 
directories, but only saves a checkpoint every 10th input file. So, if resuming
from a checkpoint, look at the output of the previous program and manually remove
files that was deduplicated after the last checkpoint.

A TODO here is of course to keep track of files automatically and redo the deduplication
for the files that was not incorporated in the checkpoint.

## Fuzzy Deduplication

### Installing
1. Clone this repository:

`git clone git@github.com:JoeyOhman/Megatron-deduplication.git`

2. These packages may or may not be needed, this line comes from the original Megatron repo:

`pip install ftfy langdetect numpy torch pandas nltk sentencepiece boto3 tqdm regex bs4 newspaper3k htmlmin tldextract`

3. Clone the LSH dependency:

`git clone https://github.com/mattilyra/LSH`

4. Move into LSH directory:

`cd LSH`

5. Set `USE_CYTHON` to `True` in `LSH/setup.py`

6. Install the LSH dependency:

`python setup.py install`

### Usage

The fuzzy deduplication is primarily done through `./master_dedup.sh`. This file has 3 high-level arguments:
- **LANG**: *really just a subdirectory name*
- **NUM_CHUNKS**: *number of chunks to divide data into, set to 1 to prevent chunking*
- **COMMON_BASE_PATH**: *which is the absolute path to the data root, under which you can find three required subdirectories: _exact_dedup/_ _chunked_before_fuzzy/_, and _fuzzy_dedup/_*

Files will be read from the _exact_dedup/_ subdirectory, and written to _chunked_before_fuzzy/_ if `NUM_CHUNKS>1`, and finally (always) to _fuzzy_dedup/_.

**Any subdirectory you write files to must be created beforehand and have write-permissions enabled!**

Give write-permissions: 

`sudo chmod -R a+rw <my_output_dir>`

Remove the write-permissions when deduplication is done:

`sudo chmod -R a+r-w <my_output_dir>`

**Note:** *The write-permissions can probably be set for a parent directory, and then this won't have to be
fixed for subdirectories that one wishes to do deduplication on. But it's always safe to remove write-permissions from 
the directory you are only reading files from* 

#### Parameters

Parameters are specified in `dedup_single_dir.sh` or `dedup_chunk_pair.sh`:

- **IDENTIFIER_KEY**: *The json field in which you can find the unique document id*
- **CHAR_N_GRAM**: *How many characters are hashed together in the sliding window*
- **JACCARD_THRESHOLD**: *The threshold of approximated Jaccard similarity to consider a duplicate.*
- **NUM_SEEDS**: *#hash-functions to use, must be multiple of NUM_BINS*
- **NUM_BINS**: *#bins to divide the hash functions in*
- **MAX_WORKERS_FINGERPRINTS**: *#CPU-workers to use for computing fingerprints.*
- **MAX_WORKERS_JACCARD**: *#CPU-workers to use for jaccard comparisons, max=NUM_BINS*

Adding hash-functions and bins increases the complexity but should result in more thorough deduplication.
Adding more workers reduce execution time at the cost of RAM usage.

#### Run fuzzy deduplication

Once satisfied with configurations, run deduplication and preferably time it:

`time ./master_dedup.sh`


#### Results

`fuzzy_dedup/` now contains the deduplicated files, along with 2 more files:

- **identified_duplicates.jsonl**: *Duplicate document ids found along with their Jaccard similarities*
- **similar_documents<_merged>.jsonl**: *The groups of duplicates, json-lines with group-id keys mapping to a list of documents ids*


### Performance Analysis Experiments

Experiments on Swedish subset of Oscar v3 (CommonCrawl-based, supposedly already deduplicated). 

#### Experiment Configurations

##### LSH-minhash settings:
- **CHAR_N_GRAM=10** *Linked to from Nvidia Megatron Bootcamp: http://snap.stanford.edu/class/cs246-2012/slides/03-lsh.pdf*
- **JACCARD_THRESHOLD=0.5** *The Pile, looks sound after qualitative checks*
- **NUM_SEEDS=10** *The Pile #hashes*
- **NUM_BINS=2** *Empirically set, only options with 10 seeds is 1, 2, 5, or 10. 2 seems to give best results/cost*

##### Parallelization settings:
- **MAX_WORKERS_FINGERPRINTS=4** *Adding more increases RAM, but does not give significant speedup*
- **MAX_WORKERS_JACCARD=2** *This is the maximum with 2 bins, also seems to require equal RAM as fingerprint computations with 4 workers*


#### Experiment Results

##### Plots

###### RAM Usage

![Alt text](images/oscar_ram_usage.png?raw=true "RAM")

RAM seems to scale quite linearly, with `RAM=data_size*2`

###### Execution Time

![Alt text](images/oscar_exec_times.png?raw=true "Exec-times")

Execution time also seems to scale linearly, with a slight sign of super-linear complexity. 
With 1 minute / GB of data on this PC. Server might be slower.

###### Removal Fraction

![Alt text](images/oscar_removal_fraction.png?raw=true "Removal Fractions")

Plot looks weird due to my human rounding. Nevertheless, in Oscarv3-sv the documents that are removed are 
typically longer than average. Also, the more data we include in one deduplication session, the more are removed. 
