from typing import List

from eldorado.my_dataclasses import Pod5Directory

# TODO: Make cleanup sted akin to this bash script. Cleanup should be in python for testability.
script = """
# Clean up of batches
        # Concatenate manifest files
        cat {manifest_files_str} > {pod5_dir.merge_pod5_manifest}

        # Concatenate log files
        LOG_LIST=$(find {pod5_dir.bam_batches_dir} -name "*.eldorado.basecaller.log")
        for LOG_FILE in $LOG_LIST; 
        do  
            echo "" >> ${{LOG_FILE}}
            cat (basename $LOG_FILE) >> ${{LOG_FILE}}
            cat $LOG_FILE >> ${{LOG_FILE}}
        done

        # Remove batch lock and done files
        rm -r {pod5_dir.basecalling_lock_files_dir}
        rm -r {pod5_dir.basecalling_done_files_dir}

        # Remove batch directories
        rm -r {pod5_dir.bam_batches_dir}
"""


def get_pod5_dirs_for_cleanup(pod5_dirs: List[Pod5Directory]) -> List[Pod5Directory]:
    return []


def cleanup_finished_pod5_dir(pod5_dir: Pod5Directory) -> None:
    return
