
import unittest
import os
import tempfile
from update_d1 import split_file

class TestSplitFile(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_file = os.path.join(self.tmp_dir.name, "test.txt")

    def tearDown(self):
        self.tmp_dir.cleanup()

    def create_file(self, num_lines):
        with open(self.tmp_file, "w") as f:
            for i in range(num_lines):
                f.write(f"line {i}\n")

    def test_small_file(self):
        self.create_file(10)
        chunks = split_file(self.tmp_file, max_lines=20)
        self.assertEqual(chunks, [self.tmp_file])

    def test_exact_lines(self):
        self.create_file(20)
        chunks = split_file(self.tmp_file, max_lines=20)
        self.assertEqual(chunks, [self.tmp_file])

    def test_large_file(self):
        self.create_file(21)
        chunks = split_file(self.tmp_file, max_lines=10)
        self.assertEqual(len(chunks), 3) # 10, 10, 1

        # Verify content
        with open(chunks[0], 'r') as f:
            lines = f.readlines()
            self.assertEqual(len(lines), 10)
            self.assertEqual(lines[0], "line 0\n")

        with open(chunks[1], 'r') as f:
            lines = f.readlines()
            self.assertEqual(len(lines), 10)
            self.assertEqual(lines[0], "line 10\n")

        with open(chunks[2], 'r') as f:
            lines = f.readlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(lines[0], "line 20\n")

    def test_large_file_split_logic(self):
        # Current logic splits:
        # 1. Read max_lines. If EOF, return original.
        # 2. If not EOF, write first chunk.
        # 3. Read one more line.
        # 4. Continue reading.

        self.create_file(25)
        chunks = split_file(self.tmp_file, max_lines=10)
        self.assertEqual(len(chunks), 3) # 10, 10, 5

if __name__ == '__main__':
    unittest.main()
