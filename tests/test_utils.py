# import pytest

import unittest

from nzshm_common.grids.region_grid import load_grid
from nzshm_common.location.code_location import CodedLocation


class TestCodedLocation(unittest.TestCase):
    def setUp(self):
        self.grid = load_grid('WLG_0_05_nb_1_1')

    def test_load_wlg_0_005(self):
        self.assertEqual(len(self.grid), 62)

    def test_code_location(self):
        # print(grid)
        loc = CodedLocation(*self.grid[0], 0.001)
        print(f'loc {loc}')
        print(f'resampled {loc.downsample(10)}')
        # assert 0
        # loc CodedLocation(lat=-41.4, lon=174.65, code='')
        self.assertEqual(CodedLocation(lat=-41.0, lon=175.0, resolution=10).code, '-41.0~175.0')
