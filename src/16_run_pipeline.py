
import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
DATA = ROOT / "data"
LOG_DIR = DATA / "logs"
REPORT_DIR = DATA / "reports"

LOG_PATH = LOG_DIR / "orchestrator.log"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def log_message(message):
    """Print a message and append it to the orchestrator log."""
    timestamped = f"[{utc_now()}] {message}"
    print(timestamped, flush=True)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as log_file:
        log_file.write(timestamped + "\n")


def get_pipeline_stages():
    """Return the stages in the order they must execute."""
    return [
        {
            "name": "unit_tests",
            "command": [
                sys.executable, "-m", "pytest",
                "tests/test_quality_rules.py", "-q",
            ],
        },
        {
            "name": "schema_check",
            "command": [
                sys.executable, str(SRC / "09_schema_check.py"),
            ],
        },
        {
            "name": "incremental_ingestion",
            "command": [
                sys.executable, str(SRC / "08_incremental_ingest.py"),
            ],
        },
        {
            "name": "quality_validation",
            "command": [
                sys.executable, str(SRC / "10_validate_incremental.py"),
            ],
        },
        {
            "name": "duckdb_batch_load",
            "command": [
                sys.executable, str(SRC / "13_load_batches.py"),
            ],
        },
        {
            "name": "health_monitoring",
            "command": [
                sys.executable, str(SRC / "15_monitor_pipeline.py"),
            ],
        },
    ]


def run_stage(stage):
    """Run one stage, stream its output, and record its result."""
    name = stage["name"]
    command = stage["command"]
    started_at = utc_now()
    start_time = time.perf_counter()
    output_lines = []

    log_message(f"STARTING STAGE: {name}")
    log_message(f"COMMAND: {' '.join(command)}")

    return_code = None
    error_message = None

    try:
        with subprocess.Popen(
            command,
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        ) as process:
            if process.stdout is not None:
                for line in process.stdout:
                    line = line.rstrip("\r\n")
                    print(line, flush=True)
                    output_lines.append(line)

                    with LOG_PATH.open("a", encoding="utf-8") as log_file:
                        log_file.write(f"[{name}] {line}\n")

            return_code = process.wait()

    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        log_message(f"ERROR RUNNING {name}: {error_message}")

    duration_seconds = round(time.perf_counter() - start_time, 3)
    success = return_code == 0 and error_message is None

    result = {
        "stage": name,
        "command": command,
        "started_at_utc": started_at,
        "finished_at_utc": utc_now(),
        "duration_seconds": duration_seconds,
        "return_code": return_code,
        "status": "SUCCESS" if success else "FAILED",
        "error": error_message,
        "output_tail": output_lines[-20:],
    }

    log_message(
        f"FINISHED STAGE: {name} | "
        f"status={result['status']} | "
        f"duration={duration_seconds}s | "
        f"return_code={return_code}"
    )

    return result


def main():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id += "_" + uuid.uuid4().hex[:8]

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    log_message("=" * 70)
    log_message(f"PIPELINE RUN STARTED: {run_id}")
    log_message(f"PROJECT ROOT: {ROOT}")

    pipeline_started_at = utc_now()
    start_time = time.perf_counter()
    stage_results = []
    failed_stage = None

    stages = get_pipeline_stages()

    for index, stage in enumerate(stages):
        result = run_stage(stage)
        stage_results.append(result)

        if result["status"] == "FAILED":
            failed_stage = stage["name"]
            log_message(
                f"PIPELINE STOPPED: stage '{failed_stage}' failed. "
                "Downstream stages will not run."
            )

            for remaining in stages[index + 1:]:
                stage_results.append({
                    "stage": remaining["name"],
                    "command": remaining["command"],
                    "status": "SKIPPED",
                    "reason": f"Previous stage failed: {failed_stage}",
                })
            break

    duration_seconds = round(time.perf_counter() - start_time, 3)
    overall_status = "FAILED" if failed_stage else "SUCCESS"

    report = {
        "run_id": run_id,
        "status": overall_status,
        "failed_stage": failed_stage,
        "started_at_utc": pipeline_started_at,
        "finished_at_utc": utc_now(),
        "duration_seconds": duration_seconds,
        "project_root": str(ROOT),
        "stages": stage_results,
    }

    report_path = REPORT_DIR / f"pipeline_run_{run_id}.json"
    report_path.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    log_message(f"PIPELINE STATUS: {overall_status}")
    log_message(f"REPORT SAVED: {report_path}")
    log_message(f"TOTAL DURATION: {duration_seconds}s")
    log_message("=" * 70)

    print("\nPIPELINE SUMMARY")
    print("-" * 50)
    for stage_result in stage_results:
        print(
            f"{stage_result['stage']:<25} "
            f"{stage_result['status']}"
        )
    print("-" * 50)
    print(f"Overall status: {overall_status}")
    print(f"Report: {report_path}")

    if failed_stage:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
