import pyarrow as pa

from daft.internal.hashing import hash_chunked_array

# @pytest.mark.parametrize("size", [(i + 1) * 1000 + i for i in range(5)])
# def test_murmur_32_buffer(size: int) -> None:
#     test_input = np.random.randint(0, 255, size, dtype=np.uint8)
#     hashing_out = hashing_old.murmur3_hash_32_buffer(test_input)
#     mmh3_out = mmh3.hash_from_buffer(test_input, signed=False)
#     assert hashing_out == mmh3_out


def test_hash_chunked_int_array():
    arr = pa.chunked_array([[None, 1, 2, 3, 4] + [None] * 4, [5, 6, 7, 8]], type=pa.uint8())
    arr = pa.chunked_array([arr.chunk(0)[1:]])
    result = hash_chunked_array(arr)
    print(result)
