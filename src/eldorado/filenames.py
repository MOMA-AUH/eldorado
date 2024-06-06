# Description: Filenames used in the Eldorado project.

# Sequencing run
# General
OUTPUT_DIR_SUFFIX = "_eldorado"
BASECALLING_SUMMARY = "basecalling_summary.csv"

# Dorado config
DORADO_CONFIG = "dorado_config.json"

# Basecalling
BC_DIR = "basecalling"
BC_BATCHES_DIR = "batches"
BC_LOCK_DIR = "lock_files"
BC_DONE_DIR = "done_files"

# Batches
BATCH_LOG = "basecalled.txt"
BATCH_BAM = "basecalled.bam"
BATCH_DONE = "batch.done"
BATCH_JOB_ID = "batch_job_id.txt"
BATCH_MANIFEST = "pod5_manifest.txt"
BATCH_SCRIPT = "run_basecaller.sh"

# Merging
MERGE_DIR = "merging"
MERGE_BAM = "merged.bam"
MERGE_SCRIPT = "run_merging.sh"
MERGE_JOB_ID = "merge_job_id.txt"
MERGE_LOCK = "merge.lock"
MERGE_DONE = "merge.done"

# Demultiplexing
DEMUX_DIR = "demultiplexing"
DEMUX_SCRIPT = "run_demultiplexing.sh"
DEMUX_JOB_ID = "demux_job_id.txt"
DEMUX_LOCK = "demux.lock"
DEMUX_DONE = "demux.done"
