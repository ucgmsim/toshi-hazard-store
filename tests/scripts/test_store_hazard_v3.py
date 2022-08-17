#!/usr/bin/env python
"""Tests for `store_hazard_v3` module."""

import unittest

from click.testing import CliRunner

from scripts import store_hazard

# from toshi_hazard_store.scripts import store_hazard_v3


@unittest.skip('module must be converted to use click')
def test_store_hazard_script():
    """Test the CLI."""
    runner = CliRunner()
    # result = runner.invoke(cli.main)
    # assert result.exit_code == 0
    # assert 'toshi-hazard-post' in result.output
    help_result = runner.invoke(store_hazard.main, ['--help'])
    assert help_result.exit_code == 0
    assert 'Show this message and exit.' in help_result.output
