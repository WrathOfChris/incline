import unittest
import decimal
from incline.flatten import flatten


class TestFlatten(unittest.TestCase):
    def test_none(self):
        self.assertEqual(flatten(None, prefix="test"),
                         {"test": None})

    def test_numbers(self):
        self.assertEqual(flatten(int(1), prefix="test"),
                         {"test": 1})
        self.assertEqual(flatten(float(1.01), prefix="test"),
                         {"test": 1.01})
        self.assertEqual(flatten(decimal.Decimal("1.01"), prefix="test"),
                         {"test": decimal.Decimal("1.01")})

    def test_list(self):
        self.assertEqual(flatten([], prefix="test"), {})
        self.assertEqual(flatten(["a"], prefix="test"),
                         {"test.0": "a"})
        self.assertEqual(flatten(["a", "a"], prefix="test"),
                         {"test.0": "a",
                          "test.1": "a"})
        self.assertEqual(flatten([{"a": "b"}], prefix="test"),
                         {"test.0.a": "b"})

    def test_dict(self):
        self.assertEqual(flatten({}, prefix="test"), {})
        self.assertEqual(flatten({"a": {}}, prefix="test"), {})
        self.assertEqual(flatten({"a": "b"}, prefix="test"),
                         {"test.a": "b"})
        self.assertEqual(flatten({"a": {"b": "c"}}, prefix="test"),
                         {"test.a.b": "c"})

    def test_sep(self):
        self.assertEqual(flatten({"a": {"b": "c", "d": "e"}},
                                 prefix="test",
                                 sep="-"),
                         {'test-a-b': 'c',
                          'test-a-d': 'e'})


if __name__ == "__main__":
    unittest.main()
