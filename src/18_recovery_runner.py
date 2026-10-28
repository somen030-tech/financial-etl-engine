
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

MAX_ATTEMPTS = 2  # Initial attempt + one retry


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def log(message):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    line = f"[{utc_now()}] {message}"
    print(line, flush=True)

    with (LOG_DIR / "recovery_runner.log").open(
        "a", encoding="utf-8"
    ) as file:
        file.write(line + "\n")


def run_orchestrator(attempt):
    command = [
        sys.executable,
        str(SRC / "16_run_pipeline.py"),
    ]

    log(f"Starting pipeline attempt {attempt}/{MAX_ATTEMPTS}")
    started = time.perf_counter()

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
                    print(line, end="", flush=True)

                    with (LOG_DIR / "recovery_runner.log").open(
                        "a", encoding="utf-8"
                    ) as file:
                        file.write(line)

            return_code = process.wait()

        return {
            "attempt": attempt,
            "return_code": return_code,
            "duration_seconds": round(
                time.perf_counter() - started, 3
            ),
            "error": None,
        }

    except Exception as exc:
        return {
            "attempt": attempt,
            "return_code": None,
            "duration_seconds": round(
                time.perf_counter() - started, 3
            ),
            "error": f"{type(exc).__name__}: {exc}",
        }


def latest_pipeline_report():
    reports = sorted(
        REPORT_DIR.glob("pipeline_run_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not reports:
        return None

    try:
        return json.loads(
            reports[0].read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError):
        return None


def write_incident(run_id, attempts, pipeline_report):
    failed_stage = (
        pipeline_report.get("failed_stage")
        if pipeline_report else None
    )

    incident = {
        "incident_id": run_id,
        "created_at_utc": utc_now(),
        "status": "UNRESOLVED",
        "attempts": attempts,
        "failed_stage": failed_stage,
        "pipeline_report": pipeline_report,
        "recommended_actions": [
            "Open data/logs/orchestrator.log.",
            "Inspect the failed stage and its error message.",
            "Check the latest pipeline_run_*.json report.",
            "Fix the underlying error before rerunning.",
        ],
    }

    incident_path = REPORT_DIR / f"incident_{run_id}.json"
    incident_path.write_text(
        json.dumps(incident, indent=2),
        encoding="utf-8",
    )

    alert_message = (
        f"PIPELINE FAILURE | incident={run_id} | "
        f"failed_stage={failed_stage or 'unknown'} | "
        f"attempts={len(attempts)} | "
        f"report={incident_path}"
    )

    with (LOG_DIR / "pipeline_alerts.log").open(
        "a", encoding="utf-8"
    ) as file:
        file.write(f"[{utc_now()}] {alert_message}\n")

    log(alert_message)
    log("Recommended: inspect the logs and correct the root cause.")

    return incident_path


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_" + uuid.uuid4().hex[:8]
    )

    attempts = []

    for attempt_number in range(1, MAX_ATTEMPTS + 1):
        result = run_orchestrator(attempt_number)
        attempts.append(result)

        if result["return_code"] == 0 and result["error"] is None:
            log("Pipeline completed successfully.")
            if attempt_number > 1:
                log("Pipeline recovered on retry.")
            return

        log(f"Attempt {attempt_number} failed: {result}")

        if attempt_number < MAX_ATTEMPTS:
            log("Retrying the full pipeline once.")
            time.sleep(2)

    report = latest_pipeline_report()
    incident_path = write_incident(run_id, attempts, report)

    log(f"Recovery failed. Incident report: {incident_path}")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
