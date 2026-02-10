import unittest
from unittest.mock import patch, MagicMock
import tempfile
import os
import pandas as pd
import sys

# Add current directory to path to import script
sys.path.append(os.getcwd())
import sync_remote_parquet

class TestSyncRemoteParquet(unittest.TestCase):

    @patch('sync_remote_parquet.subprocess.run')
    def test_get_changed_files_no_changes(self, mock_run):
        # Mock rclone check output
        mock_run.return_value.stdout = "= file1.parquet\n= file2.parquet\n"
        mock_run.return_value.returncode = 0

        local_files = ["/tmp/file1.parquet", "/tmp/file2.parquet"]
        remote_base = "r2:wdi"
        temp_dir = "/tmp/download"

        changed = sync_remote_parquet.get_changed_files(local_files, remote_base, temp_dir)
        self.assertEqual(changed, [])

    @patch('sync_remote_parquet.subprocess.run')
    def test_get_changed_files_missing_remote(self, mock_run):
        # Mock rclone check output: + means missing on dest (remote)
        mock_run.return_value.stdout = "+ new_file.parquet\n= old_file.parquet\n"
        mock_run.return_value.returncode = 1 # rclone check returns 1 on diff

        local_files = ["/tmp/old_file.parquet", "/tmp/new_file.parquet"]
        remote_base = "r2:wdi"
        temp_dir = "/tmp/download"

        changed = sync_remote_parquet.get_changed_files(local_files, remote_base, temp_dir)
        # Should detect new_file as changed
        self.assertIn("/tmp/new_file.parquet", changed)
        self.assertNotIn("/tmp/old_file.parquet", changed)

    @patch('sync_remote_parquet.subprocess.run')
    @patch('sync_remote_parquet.load_and_sort_parquet_file')
    @patch('sync_remote_parquet.pd.testing.assert_frame_equal')
    def test_get_changed_files_diff_content(self, mock_assert, mock_load, mock_run):
        def side_effect(*args, **kwargs):
            cmd = args[0]
            if cmd[1] == "check":
                m = MagicMock()
                m.stdout = "* changed.parquet\n"
                m.returncode = 1
                return m
            elif cmd[1] == "copy":
                return MagicMock(returncode=0)
            return MagicMock()

        mock_run.side_effect = side_effect

        local_files = ["/tmp/changed.parquet"]
        remote_base = "r2:wdi"
        temp_dir = "/tmp/download"

        # Mock load_and_sort to return dataframes
        mock_load.return_value = pd.DataFrame({'a': [1]})

        # Mock assert_frame_equal to raise AssertionError
        mock_assert.side_effect = AssertionError("Mismatch")

        changed = sync_remote_parquet.get_changed_files(local_files, remote_base, temp_dir)
        self.assertIn("/tmp/changed.parquet", changed)

        # Verify download was called
        calls = mock_run.call_args_list
        # assert that one of the calls was rclone copy
        self.assertTrue(any(call[0][0][1] == "copy" for call in calls))

    @patch('sync_remote_parquet.subprocess.run')
    @patch('sync_remote_parquet.load_and_sort_parquet_file')
    @patch('sync_remote_parquet.pd.testing.assert_frame_equal')
    def test_get_changed_files_diff_checksum_match_content(self, mock_assert, mock_load, mock_run):
        # Case where rclone check says diff (*), but pandas says equal (fuzzy match)
        def side_effect(*args, **kwargs):
            cmd = args[0]
            if cmd[1] == "check":
                m = MagicMock()
                m.stdout = "* fuzzy_match.parquet\n"
                m.returncode = 1
                return m
            elif cmd[1] == "copy":
                return MagicMock(returncode=0)
            return MagicMock()

        mock_run.side_effect = side_effect

        local_files = ["/tmp/fuzzy_match.parquet"]
        remote_base = "r2:wdi"
        temp_dir = "/tmp/download"

        # Mock load_and_sort
        mock_load.return_value = pd.DataFrame({'a': [1.00001]})

        # Mock assert_frame_equal to pass (no exception raised)
        mock_assert.return_value = None

        changed = sync_remote_parquet.get_changed_files(local_files, remote_base, temp_dir)
        # Should NOT be in changed list
        self.assertNotIn("/tmp/fuzzy_match.parquet", changed)
        self.assertEqual(changed, [])

        # Verify download was called (because checksum differed)
        calls = mock_run.call_args_list
        self.assertTrue(any(call[0][0][1] == "copy" for call in calls))

if __name__ == '__main__':
    unittest.main()
