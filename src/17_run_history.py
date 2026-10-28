
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "data" / "reports"


def main():
    reports = sorted(
        REPORT_DIR.glob("pipeline_run_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not reports:
        print("No pipeline run reports found.")
        print("Run src\\16_run_pipeline.py first.")
        return

    print("\nPIPELINE RUN HISTORY")
    print("=" * 100)
    print(
        f"{'RUN ID':<28} {'STATUS':<10} "
        f"{'DURATION (s)':>12} {'FAILED STAGE':<25}"
    )
    print("-" * 100)

    for path in reports:
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
            run_id = report.get("run_id", path.stem)
            status = report.get("status", "UNKNOWN")
            duration = report.get("duration_seconds", 0)
            failed_stage = report.get("failed_stage") or "-"

            print(
                f"{run_id:<28} {status:<10} "
                f"{duration:>12.3f} {failed_stage:<25}"
            )
        except (OSError, json.JSONDecodeError) as exc:
            print(f"Could not read {path.name}: {exc}")

    print("=" * 100)
    print(f"Reports directory: {REPORT_DIR}")


if __name__ == "__main__":
    main()
