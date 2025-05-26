#!/usr/bin/env python3
import subprocess
import duckdbexport


def fake_run(cmd, shell, capture_output=False, text=False, check=False):
    print("[FAKE RUN] Command:", cmd)
    # Simulate output from: SELECT name FROM sqlite_master WHERE type='table'
    if "SELECT name FROM sqlite_master" in cmd:
        fake_stdout = (
            "⛅️ wrangler 3.103.2 (update available 4.16.1)\n"
            "----------------------------------------------\n\n"
            "🌀 Executing on local database wdi (a2bb16ae-304f-43f0-8cb7-1cb1a2994edf) from .wrangler/state/v3/d1:\n"
            "🌀 To execute on your remote database, add a --remote flag to your wrangler command.\n"
            "🚣 1 command executed successfully.\n"
            "[\n"
            "\t{\n"
            "\t\t\"results\": [\n"
            "\t\t\t{\n"
            "\t\t\t\t\"name\": \"_cf_METADATA\"\n"
            "\t\t\t},\n"
            "\t\t\t{\n"
            "\t\t\t\t\"name\": \"dim_country\"\n"
            "\t\t\t},\n"
            "\t\t\t{\n"
            "\t\t\t\t\"name\": \"dim_indicator\"\n"
            "\t\t\t},\n"
            "\t\t\t{\n"
            "\t\t\t\t\"name\": \"fct_country_summary\"\n"
            "\t\t\t},\n"
            "\t\t\t{\n"
            "\t\t\t\t\"name\": \"fct_wdi_history\"\n"
            "\t\t\t}\n"
            "\t\t],\n"
            "\t\t\"success\": true,\n"
            "\t\t\"meta\": {\n"
            "\t\t\t\"duration\": 1\n"
            "\t\t}\n"
            "\t}\n"
            "]\n"
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
