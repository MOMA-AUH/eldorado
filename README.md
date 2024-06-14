# Eldorado 

Eldorado is designed to streamline the process of basecalling sequencing runs from a ONT PromethION sequencer. It provides a suite of features including pseudo-live basecalling, merging, demultiplexing, and cleanup. Eldorado is designed to be run on a high-performance computing (HPC) cluster and uses the SLURM job scheduler to manage the sequencing runs and takes advantage of the GPU resources available on the cluster.

One of the key features of Eldorado is the `scheduler` subtool. The `scheduler` is responsible for managing the sequencing runs for each project. It reads the project configuration file and schedules the basecalling, merging, demultiplexing, and cleanup jobs on the cluster. The `scheduler` is designed to be run as a Cron job and can be configured to run at regular intervals to process new sequencing runs as they are uploaded to the cluster. The `scheduler` provides detailed logging of the basecalling and can be configured to send email notifications when jobs are completed or if an error occurs.

## Getting Started

To get started with Eldorado, you need to install the package on your local machine. You can install Eldorado from a local repository or from GitHub. To install Eldorado from a GitHub download the recipe (`meta.yaml`) file from the `conda_recipe` directory and build the package with `conda build`:

1. In the `conda_recipe` directory use the `meta.yaml` file to build the package with `conda build`:
    ```sh 
    # Build directory
    BUILD_DIR="/path/to/build/directory"
    cd $BUILD_DIR

    # Download the meta.yaml file
    curl -O https://raw.githubusercontent.com/MOMA-AUH/eldorado/master/conda_recipe/meta.yaml

    # Build the package
    LOCAL_REPO="/path/to/local/conda/repository"
    conda build $BUILD_DIR --output-folder $LOCAL_REPO -c jannessp

    # Clean up
    conda build purge
    ```

2. Create a new conda environment:
    ```sh
    ENV_NAME="eldorado-env"
    conda create -n $ENV_NAME -c bioconda -c conda-forge -c jannessp -c $LOCAL_REPO eldorado
    ```
    Note that the `-c` flags specify the channels to search for the package. The `bioconda` and `conda-forge` channels are required for the `samtools` dependency, and the `jannessp` channel is required for the `pod5` package.
3. Eldorado is now installed in your local conda environment. You can start using it by following the instructions in the [Usage](#usage) section.


### Installation from local repository
To install an editable version of Eldorado from a local repository, run the following commands:
```sh
ENV_NAME="eldorado-env"
git clone https://github.com/MOMA-AUH/eldorado.git
conda create -n $ENV_NAME python=3.10 samtools=1.20 # Note that samtools is required for the merge step
conda activate $ENV_NAME
cd /path/to/eldorado/repository
pip install -e .
```

### Usage

To use the `scheduler`, you need to provide several command-line arguments:

- `--root-dir` or `-r`: The root directory of your project.
- `--models-dir` or `-m`: The path to the models directory.
- `--project-config` or `-c`: The path to the project configuration file (.csv).
- `--min-batch-size` or `-b`: The minimum batch size (default is 1).
- `--log-file` or `-l`: The path to the log file (optional).
- `--mail-user` or `-u`: The email address for notifications (optional).
- `--dry-run` or `-d`: If set, the scheduler will perform a dry run (optional).

Here is an example of how to use the `scheduler`:

```sh
python eldorado scheduler \
    --root-dir /path/to/root \
    --models-dir /path/to/models \
    --project-config /path/to/config.csv \
    --min-batch-size 10 \
    --log-file /path/to/logfile.log \
    --mail-user example@example.com
```

`--dry-run`

## How it works

Eldorado is designed to run in three main stages: basecalling, merging, and demultiplexing. The `scheduler` is responsible for managing these stages and scheduling the jobs on the cluster. Furthermore the `scheduler` handles logging, continous monitoring of lock files and cleanup of temporary directories and files. Each stage works as follows:

### Basecalling

The basecalling stage is responsible for running the Dorado basecaller on the sequencing reads. The `scheduler` reads the `pod5` files from the sequencing run and submits the basecalling of any new files to the job queue. The basecalling is run on the GPU nodes of the cluster. 

### Merging

The merging stage is responsible for merging the basecalled reads from the individual basecalling batches into a single file using `samtools`. Before merging the basecalled reads, the `scheduler` checks if all `pod5` files have been basecalled successfully. If all files have been basecalled, the `scheduler` submits the merging job to the job queue.

### Demultiplexing

The demultiplexing stage is responsible for demultiplexing the merged reads into individual samples using `dorado demux`. The `scheduler` checks that the sample sheet from the sequencing run is available and has `barcode` and ´alias´ columns, and that the used kit requires demultiplexing. If the conditions are met, the `scheduler` submits the demultiplexing job to the job queue. If not, the `scheduler` skips the demultiplexing stage and simply uses the merged reads as the final output.

## Comments on usage on GenomeDK

### Installation

ElDorado is placed in the following repository on GenomeDK:

```sh
/faststorage/project/MomaReference/BACKUP/nanopore/software/conda/repo/
```

### Execution
The following directory is used on GDK for the Eldorado execution, logging and configuration files:
    
```sh
/faststorage/project/MomaNanoporeDevelopment/BACKUP/eldoarado/
```

The folder contains:
- `config` folder with the configuration files
- `logs` folder with the log files
- `run_elodarado.sh` script to run Eldorado