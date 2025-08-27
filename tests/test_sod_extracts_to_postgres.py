import tempfile
import unittest
from pathlib import Path

import sod_extracts_to_postgres as sep


class TestSodExtractsToPostgres(unittest.TestCase):
    def test_parse_args(self):
        args = sep.parse_args(["--environment", "prod", "--process-date", "2023-01-01"])
        self.assertEqual(args.environment, "prod")
        self.assertEqual(args.process_date, "2023-01-01")
        self.assertFalse(args.flag_001)

    def test_read_sql(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as tmp:
            tmp.write("SELECT 1\nFROM dual")
            path = Path(tmp.name)
        try:
            self.assertEqual(sep.read_sql(path), "SELECT 1 FROM dual")
        finally:
            path.unlink()


if __name__ == "__main__":
    unittest.main()
