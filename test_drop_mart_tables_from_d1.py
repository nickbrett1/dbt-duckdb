#!/usr/bin/env python3
import subprocess
import duckdbexport


def fake_run(cmd, shell, capture_output=False, text=False, check=False):
    print("[FAKE RUN] Command:", cmd)
    # Simulate output from: SELECT name FROM sqlite_master WHERE type='table'
    if "SELECT name FROM sqlite_master" in cmd:
        # Simulated output exactly as in your test case.
        fake_stdout = (
            "┌─────────────────────┐\n"
            "│ name                │\n"
            "├─────────────────────┤\n"
            "│ _cf_KV              │\n"
            "├─────────────────────┤\n"
            "│ dim_country         │\n"
            "├─────────────────────┤\n"
            "│ dim_indicator       │\n"
            "├─────────────────────┤\n"
            "│ fct_country_summary │\n"
            "├─────────────────────┤\n"
            "│ fct_wdi_history     │\n"
            "└─────────────────────┘\n"
        )
        FakeCompletedProcess = type("FakeCompletedProcess", (), {
                                    "stdout": fake_stdout})
        return FakeCompletedProcess()
    # For DROP TABLE commands, simply simulate execution.
    elif "DROP TABLE IF EXISTS" in cmd:
        print("[FAKE RUN] Simulated drop command execution.")
        FakeCompletedProcess = type("FakeCompletedProcess", (), {"stdout": ""})
        return FakeCompletedProcess()
    else:
        print("[FAKE RUN] Command not recognized in fake_run.")
        FakeCompletedProcess = type("FakeCompletedProcess", (), {"stdout": ""})
        return FakeCompletedProcess()


def test_drop_mart_tables_from_d1(d1_mode: str):
    original_run = subprocess.run
    subprocess.run = fake_run  # Override subprocess.run with our fake_run

    print(f"Testing drop_mart_tables_from_d1 with d1_mode='{d1_mode}'...")
    duckdbexport.drop_mart_tables_from_d1(d1_mode)

    subprocess.run = original_run  # Restore original subprocess.run


if __name__ == "__main__":
    # Test with the remote mode (you can change to "local" if desired)
    test_drop_mart_tables_from_d1("remote")
