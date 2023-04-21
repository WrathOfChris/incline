import unittest
import sys
import incline.base62

class TestBase62(unittest.TestCase):
    def test_base_list(self):
        self.assertEqual("0123456789"
                         "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                         "abcdefghijklmnopqrstuvwxyz",
                         incline.base62.BASE_LIST)
        incline.base62.BASE_LIST

    def test_base_decode(self):
        self.assertEqual(1234567890, incline.base62.base_decode('1LY7VK'))

    def test_base_encode(self):
        self.assertEqual('1LY7VK', incline.base62.base_encode(1234567890))

    def test_base_negative(self):
        self.assertEqual('0', incline.base62.base_encode(-1))

    def test_base_float(self):
        with self.assertRaises(ValueError):
            incline.base62.base_encode(float('inf'))
        self.assertEqual('0', incline.base62.base_encode(float('-inf')))

    def test_base_range_maxsize(self):
        MAXSIZES = [
                2**31-1,
                2**63-1,        # modulo overrun produces mismatch
                2**127-1,       # modulo overrun produces mismatch
                2**32,
                2**64,          # modulo overrun produces mismatch
                2**128,         # modulo overrun produces mismatch
                2**32+1,
                2**64+1,        # modulo overrun produces mismatch
                2**128+1,       # modulo overrun produces mismatch
                sys.maxsize
                ]
        for m in MAXSIZES:
            s = incline.base62.base_encode(m)
            i = incline.base62.base_decode(s)
            self.assertEqual(m, i)

    def test_base_modulo_overrun(self):
        """
        numbers discovered to start overflowing modulo operations when float
        division is used in base_encode
        """
        WEIRD = [
                558446422513418238,
                558446422513418239,
                558446422513418240,
                558446422513418241,
                560698153607626752
                ]

        for w in WEIRD:
            s = incline.base62.base_encode(w)
            i = incline.base62.base_decode(s)
            self.assertEqual(w, i)


if __name__ == "__main__":
    unittest.main()
