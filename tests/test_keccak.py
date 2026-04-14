from __future__ import annotations

import unittest

from flash_dataset.validator.keccak import keccak256


class KeccakTests(unittest.TestCase):
    def test_keccak256_empty_string_matches_known_vector(self) -> None:
        digest = keccak256(b"")

        self.assertEqual(
            digest.hex(),
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
        )

    def test_keccak256_abc_matches_known_vector(self) -> None:
        digest = keccak256(b"abc")

        self.assertEqual(
            digest.hex(),
            "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45",
        )


if __name__ == "__main__":
    unittest.main()
