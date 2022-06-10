# Fuzzy deduplication using LSH

**This repository is a modified version of https://github.com/NVIDIA/Megatron-LM/tree/main/tools/openwebtext**

## Installing
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

## Usage

The deduplication is done through `./dedup.sh`. Set `ROOT_IN` to the directory which contains the subtree of files
that should be deduplicated. These files must all have a text json-key and a document id in the 
json-key set with `IDENTIFIER_KEY`, e.g. `md5`. 

Then `ROOT_OUT` defines where to put the deduplicated files, that will have same file-tree structure as the
input. This must be created beforehand and given write-permissions: 

`sudo chmod -R a+rw <my_output_dir>`

Remove the write-permissions when deduplication is done:

`sudo chmod -R a+r-w <my_output_dir>`

**Note:** *The write-permissions can probably be set for a parent directory, and then this won't have to be
fixed for subdirectories that one wishes to do deduplication on.* 

Both `ROOT_IN` and `ROOT_OUT` should contain absolute paths.

### Parameters

- **CHAR_N_GRAM**: *How many characters are hashed together in the sliding window*
- **JACCARD_THRESHOLD**: *The threshold of approximated Jaccard similarity to consider a duplicate.*
- **NUM_SEEDS**: *#hash-functions to use, must be multiple of NUM_BINS*
- **NUM_BINS**: *#bins to divide the hash functions in*
- **MAX_WORKERS_FINGERPRINTS**: *#CPU-workers to use for computing fingerprints.*
- **MAX_WORKERS_JACCARD**: *#CPU-workers to use for jaccard comparisons, max=NUM_BINS*

Adding hash-functions and bins increases the complexity but should result in more thorough deduplication.
Adding more workers reduce execution time at the cost of RAM usage.

### Run deduplication

Once satisfied with configurations, run deduplication and preferably time it:

`time ./dedup.sh`


### Results

`ROOT_OUT` now contains the deduplicated files, along with 2 more files:

- **identified_duplicates.jsonl**: *Duplicate document ids found along with their Jaccard similarities*
- **similar_documents.jsonl**: *The groups of duplicates, json-lines with group-id keys mapping to a list of documents ids*


## Performance Analysis Experiments

Experiments on Swedish subset of Oscar v3 (CommonCrawl-based, supposedly already deduplicated). 

### Experiment Configuarations

#### LSH-minhash settings:
- **CHAR_N_GRAM=10** *Linked to from Nvidia Megatron Bootcamp: http://snap.stanford.edu/class/cs246-2012/slides/03-lsh.pdf*
- **JACCARD_THRESHOLD=0.5** *The Pile, looks sound after qualitative checks*
- **NUM_SEEDS=10** *The Pile #hashes*
- **NUM_BINS=2** *Empirically set, only options with 10 seeds is 1, 2, 5, or 10. 2 seems to give best results/cost*

#### Parallelization settings:
- **MAX_WORKERS_FINGERPRINTS=4** *Adding more increases RAM, but does not give significant speedup*
- **MAX_WORKERS_JACCARD=2** *This is the maximum with 2 bins, also seems to require equal RAM as fingerprint computations with 4 workers*


### Experiment Results

#### Plots

##### RAM Usage

![Alt text](images/oscar_ram_usage.png?raw=true "RAM")

RAM seems to scale quite linearly, with `RAM=data_size*2`

##### Execution Time

![Alt text](images/oscar_exec_times.png?raw=true "Exec-times")

Execution time also seems to scale linearly, with a slight sign of super-linear complexity. 
With 1 minute / GB of data on this PC. Server might be slower.

##### Removal Fraction

![Alt text](images/oscar_removal_fraction.png?raw=true "Removal Fractions")

Plot looks weird due to my human rounding. Nevertheless, in Oscarv3-sv the documents that are removed are 
typically longer than average. Also, the more data we include in one deduplication session, the more are removed. 
