import unittest

from gravitino.client.gravitino_version import GravitinoVersion
from gravitino.dto.version_dto import VersionDTO


class TestGravitinoVersion(unittest.TestCase):
    def test_parse_version_string(self):
        # Test a valid the version string
        version = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        # Test a valid the version string with SNAPSHOT
        version = GravitinoVersion(
            VersionDTO("0.6.0-SNAPSHOT", "2023-01-01", "1234567")
        )

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        # Test a valid the version string with alpha
        version = GravitinoVersion(VersionDTO("0.6.0-alpha", "2023-01-01", "1234567"))

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        # Test a valid the version string with pypi format
        version = GravitinoVersion(VersionDTO("0.6.0.dev21", "2023-01-01", "1234567"))

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        # Test an invalid the version string with 2 part
        self.assertRaises(
            AssertionError, GravitinoVersion, VersionDTO("0.6", "2023-01-01", "1234567")
        )

        # Test an invalid the version string with not number
        self.assertRaises(
            AssertionError,
            GravitinoVersion,
            VersionDTO("a.b.c", "2023-01-01", "1234567"),
        )

    def test_version_compare(self):
        # test equal
        version1 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))

        self.assertEqual(version1, version2)

        # test less than
        version1 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.12.0", "2023-01-01", "1234567"))

        self.assertLess(version1, version2)

        # test greater than
        version1 = GravitinoVersion(VersionDTO("1.6.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))

        self.assertGreater(version1, version2)

        # test equal with suffix
        version1 = GravitinoVersion(
            VersionDTO("0.6.0-SNAPSHOT", "2023-01-01", "1234567")
        )
        version2 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))

        self.assertEqual(version1, version2)
