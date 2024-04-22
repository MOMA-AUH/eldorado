from typer.testing import CliRunner
from eldorado.main import app


def test_help():
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
