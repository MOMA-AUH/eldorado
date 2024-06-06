from pathlib import Path

import typer
from typing_extensions import Annotated, Optional, List

from eldorado.basecalling import process_unbasecalled_pod5_files, basecalling_is_pending, cleanup_basecalling_lock_files, SequencingRun
from eldorado.merging import merging_is_pending, submit_merging_to_slurm, cleanup_merge_lock_files
from eldorado.cleanup import needs_cleanup, cleanup_output_dir
from eldorado.logging_config import get_log_file_handler, logger
from eldorado.demultiplexing import demultiplexing_is_pending, process_demultiplexing, cleanup_demultiplexing_lock_files
from eldorado.pod5_handling import find_sequencning_runs_for_processing, needs_basecalling, contains_pod5_files
from eldorado.configuration import get_dorado_config, get_project_configs

# Set up the CLI
app = typer.Typer()


@app.command()
def scheduler(
    root_dir: Annotated[
        Path,
        typer.Option(
            "--root-dir",
            "-r",
            help="Root directory",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
        ),
    ],
    models_dir: Annotated[
        Path,
        typer.Option(
            "--models-dir",
            "-m",
            help="Path to models directory",
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
        ),
    ],
    configs_csv: Annotated[
        Path,
        typer.Option(
            "--project-config",
            "-c",
            help="Path to project config file (.csv)",
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ],
    mail_user: Annotated[
        List[str],
        typer.Option(
            "--mail-user",
            "-u",
            help="Email address for notifications. This can be used multiple times. Note that only the first email address will be used for Slurm notifications.",
        ),
    ],
    min_batch_size: Annotated[
        int,
        typer.Option(
            "--min-batch-size",
            "-b",
            help="Minimum batch size",
        ),
    ] = 1,
    log_file: Annotated[
        Optional[Path],
        typer.Option(
            "--log-file",
            "-l",
            help="Path to log file",
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-d",
            help="Dry run",
        ),
    ] = False,
) -> None:

    # Setup logging to file
    if log_file is not None:
        file_handler = get_log_file_handler(log_file=log_file)
        logger.addHandler(file_handler)

    # Welcome message
    logger.info("Running Eldorado scheduler...")

    # Load project configs from csv
    logger.info("Loading project configs from %s", str(configs_csv))
    project_configs = get_project_configs(configs_csv)

    # Process sequencing runs for each project
    for project_config in project_configs:
        logger.info("Processing project %s", project_config.project_id)

        # Find pod5 dirs that needs processing (pattern: [project_id]/[sample_id]/[run_id]/pod5*)
        pattern = f"{project_config.project_id}/*/*/pod5*"
        runs = find_sequencning_runs_for_processing(root_dir, pattern)
        logger.info("Found %d pod5 dir(s) that needs processing", len(runs))

        for run in runs:
            logger.info("Processing %s", str(run.pod5_dir))

            process_sequencing_run(
                run=run,
                dorado_executable=project_config.dorado_executable,
                basecalling_model=project_config.basecalling_model,
                models_dir=models_dir,
                mod_5mcg_5hmcg=project_config.mod_5mcg_5hmcg,
                mod_6ma=project_config.mod_6ma,
                min_batch_size=min_batch_size,
                run_basecalling=True,
                run_merging=True,
                run_demultiplexing=True,
                run_cleanup=True,
                mail_user=mail_user,
                dry_run=dry_run,
            )


@app.command()
def manual_run(
    pod5_dir: Annotated[
        Path,
        typer.Option(
            "--pod5-dir",
            "-i",
            help="Root directory",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
        ),
    ],
    dorado_executable: Annotated[
        Path,
        typer.Option(
            "--dorado-executable",
            "-d",
            help="Path to dorado executable",
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ],
    models_dir: Annotated[
        Path,
        typer.Option(
            "--models-dir",
            "-m",
            help="Path to models directory",
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
        ),
    ],
    mail_user: Annotated[
        List[str],
        typer.Option(
            "--mail-user",
            "-u",
            help="Email address for notifications. This can be used multiple times. Note that only the first email address will be used for Slurm notifications.",
        ),
    ],
    basecalling_model: Annotated[
        Optional[Path],
        typer.Option(
            "--basecalling-model",
            "-b",
            help="Path to basecalling model",
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
    mod_5mcg_5hmcg: Annotated[
        bool,
        typer.Option(
            "--5mcg",
            help="5mCG modification",
        ),
    ] = False,
    mod_6ma: Annotated[
        bool,
        typer.Option(
            "--6ma",
            help="6mA modification",
        ),
    ] = False,
    min_batch_size: Annotated[
        int,
        typer.Option(
            "--min-batch-size",
            "-b",
            help="Minimum batch size",
        ),
    ] = 1,
    log_file: Annotated[
        Path | None,
        typer.Option(
            "--log-file",
            "-l",
            help="Path to log file",
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
    run_basecalling: Annotated[
        bool,
        typer.Option(
            "--run-basecalling",
            help="Run basecalling",
        ),
    ] = False,
    run_merging: Annotated[
        bool,
        typer.Option(
            "--run-merging",
            help="Run merging",
        ),
    ] = False,
    run_demultiplexing: Annotated[
        bool,
        typer.Option(
            "--run-demultiplexing",
            help="Run demultiplexing",
        ),
    ] = False,
    run_cleanup: Annotated[
        bool,
        typer.Option(
            "--run-cleanup",
            help="Run cleanup",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-d",
            help="Dry run",
        ),
    ] = False,
) -> None:

    # Check if everything should be run (default behaviour if all options are False)
    if not any([run_basecalling, run_merging, run_demultiplexing, run_cleanup]):
        run_basecalling = True
        run_merging = True
        run_demultiplexing = True
        run_cleanup = True

    # Setup logging to file
    if log_file is not None:
        file_handler = get_log_file_handler(log_file=log_file)
        logger.addHandler(file_handler)

    # Welcome message
    logger.info("Running Eldorado...")

    # Check pod5 dir
    if not contains_pod5_files(pod5_dir):
        logger.error("No pod5 files found...")
        return
    if not needs_basecalling(pod5_dir):
        logger.info("Pod5 directory seems to be processed already...")
        return

    run = SequencingRun(pod5_dir)

    # Process sequencing runs
    process_sequencing_run(
        run=run,
        dorado_executable=dorado_executable,
        basecalling_model=basecalling_model,
        models_dir=models_dir,
        mod_5mcg_5hmcg=mod_5mcg_5hmcg,
        mod_6ma=mod_6ma,
        min_batch_size=min_batch_size,
        run_basecalling=run_basecalling,
        run_merging=run_merging,
        run_demultiplexing=run_demultiplexing,
        run_cleanup=run_cleanup,
        mail_user=mail_user,
        dry_run=dry_run,
    )


def process_sequencing_run(
    run: SequencingRun,
    dorado_executable: Path,
    basecalling_model: Path | None,
    models_dir: Path,
    mod_5mcg_5hmcg: bool,
    mod_6ma: bool,
    min_batch_size: int,
    run_basecalling: bool,
    run_merging: bool,
    run_demultiplexing: bool,
    run_cleanup: bool,
    mail_user: List[str],
    dry_run: bool,
):
    logger.info("Processing %s", str(run.pod5_dir))

    # Setup output directory
    run.output_dir.mkdir(parents=True, exist_ok=True)

    # Clean up lock files before processing
    cleanup_basecalling_lock_files(run)
    cleanup_merge_lock_files(run)
    cleanup_demultiplexing_lock_files(run)

    # Setup Dorado config
    if not run.dorado_config_file.exists():
        logger.info("Setting up Dorado config (%s)", str(run.dorado_config_file))
        dorado_config = get_dorado_config(
            metadata=run.metadata,
            dorado_executable=dorado_executable,
            basecalling_model=basecalling_model,
            mod_5mcg_5hmcg=mod_5mcg_5hmcg,
            mod_6ma=mod_6ma,
            models_dir=models_dir,
        )
        dorado_config.save(run.dorado_config_file)

    # Basecalling
    if run_basecalling and basecalling_is_pending(run):
        logger.info("Running basecalling...")
        process_unbasecalled_pod5_files(
            run=run,
            min_batch_size=min_batch_size,
            mail_user=mail_user,
            dry_run=dry_run,
        )
    # Merging
    elif run_merging and merging_is_pending(run):
        logger.info("Running merging...")
        submit_merging_to_slurm(
            run,
            mail_user=mail_user,
            dry_run=dry_run,
        )
    # Demultiplexing
    elif run_demultiplexing and demultiplexing_is_pending(run):
        logger.info("Running demultiplexing...")
        process_demultiplexing(
            run=run,
            mail_user=mail_user,
            dry_run=dry_run,
        )
    # Cleanup
    elif run_cleanup and needs_cleanup(run):
        logger.info("Finalizing output...")
        cleanup_output_dir(
            run=run,
            mail_user=mail_user,
        )
    else:
        logger.info("Nothing to do...")


if __name__ == "__main__":
    app()
