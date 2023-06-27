import unittest
from incline.version import incline_version


class TestVersion(unittest.TestCase):

    def test_version(self) -> None:
        version = incline_version()
        self.assertIsNotNone(version)
