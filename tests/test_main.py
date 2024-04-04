import shutil
import tempfile
import unittest
from pathlib import Path

from typer.testing import CliRunner

from src.eldorado.main import app, find_all_unbasecalled_pod5_files


class TestAppHelp(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_help(self):
        result = self.runner.invoke(app, ["--help"])
        assert result.exit_code == 0


class TestFindAllUnbasecalledPod5Files(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_find_all_unbasecalled_pod5_files(self):
        # Create temporary files and directories
        print("hest")
        root_dir = Path(self.tmp_dir) / "root"
        root_dir.mkdir(parents=True, exist_ok=True)

        pod5_dir = root_dir / "N001" / "sample_1" / "1_2_3_4_5" / "pod5"
        pod5_dir.mkdir(parents=True, exist_ok=True)

        pod5_file = pod5_dir / "file1.pod5"
        pod5_file.touch()

        find_all_unbasecalled_pod5_files(root_dir=root_dir, dry_run=False)

        assert pod5_file.exists()
        assert pod5_file.exists()
