"""Minimal self-contained Keccak-256 implementation for transaction validation."""

from __future__ import annotations

_MASK_64 = (1 << 64) - 1
_RATE_BYTES = 136
_OUTPUT_BYTES = 32

_ROUND_CONSTANTS: tuple[int, ...] = (
    0x0000000000000001,
    0x0000000000008082,
    0x800000000000808A,
    0x8000000080008000,
    0x000000000000808B,
    0x0000000080000001,
    0x8000000080008081,
    0x8000000000008009,
    0x000000000000008A,
    0x0000000000000088,
    0x0000000080008009,
    0x000000008000000A,
    0x000000008000808B,
    0x800000000000008B,
    0x8000000000008089,
    0x8000000000008003,
    0x8000000000008002,
    0x8000000000000080,
    0x000000000000800A,
    0x800000008000000A,
    0x8000000080008081,
    0x8000000000008080,
    0x0000000080000001,
    0x8000000080008008,
)

_RHO_OFFSETS: tuple[int, ...] = (
    0,
    1,
    62,
    28,
    27,
    36,
    44,
    6,
    55,
    20,
    3,
    10,
    43,
    25,
    39,
    41,
    45,
    15,
    21,
    8,
    18,
    2,
    61,
    56,
    14,
)


def keccak256(input_bytes: bytes) -> bytes:
    """Return the Keccak-256 digest used by Ethereum."""
    state = [0] * 25
    offset = 0
    while offset + _RATE_BYTES <= len(input_bytes):
        _xor_block_into_state(state, input_bytes[offset : offset + _RATE_BYTES])
        _keccak_f1600(state)
        offset += _RATE_BYTES

    padded_block = bytearray(input_bytes[offset:])
    padded_block.append(0x01)
    padded_block.extend(b"\x00" * (_RATE_BYTES - len(padded_block)))
    padded_block[-1] ^= 0x80
    _xor_block_into_state(state, padded_block)
    _keccak_f1600(state)
    return _squeeze(state, _OUTPUT_BYTES)


def _xor_block_into_state(state: list[int], block: bytes | bytearray) -> None:
    for lane_index, block_offset in enumerate(range(0, _RATE_BYTES, 8)):
        lane = int.from_bytes(block[block_offset : block_offset + 8], "little")
        state[lane_index] ^= lane


def _squeeze(state: list[int], output_bytes: int) -> bytes:
    output = bytearray()
    while len(output) < output_bytes:
        for lane in state[: _RATE_BYTES // 8]:
            output.extend(lane.to_bytes(8, "little"))
            if len(output) >= output_bytes:
                return bytes(output[:output_bytes])
        _keccak_f1600(state)
    return bytes(output[:output_bytes])


def _keccak_f1600(state: list[int]) -> None:
    for round_constant in _ROUND_CONSTANTS:
        column_parity = [
            state[x] ^ state[x + 5] ^ state[x + 10] ^ state[x + 15] ^ state[x + 20]
            for x in range(5)
        ]
        theta = [
            column_parity[(x - 1) % 5] ^ _rotate_left(column_parity[(x + 1) % 5], 1)
            for x in range(5)
        ]
        for y in range(5):
            row_offset = 5 * y
            for x in range(5):
                state[row_offset + x] ^= theta[x]

        permuted = [0] * 25
        for y in range(5):
            for x in range(5):
                source_index = x + 5 * y
                destination_index = y + 5 * ((2 * x + 3 * y) % 5)
                permuted[destination_index] = _rotate_left(
                    state[source_index], _RHO_OFFSETS[source_index]
                )

        for y in range(5):
            row_offset = 5 * y
            row = permuted[row_offset : row_offset + 5]
            for x in range(5):
                state[row_offset + x] = (
                    row[x] ^ ((~row[(x + 1) % 5]) & row[(x + 2) % 5])
                ) & _MASK_64

        state[0] ^= round_constant


def _rotate_left(value: int, shift: int) -> int:
    if shift == 0:
        return value
    return ((value << shift) | (value >> (64 - shift))) & _MASK_64
