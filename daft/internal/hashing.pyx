# distutils: language=c++
# distutils: sources = daft/internal/xxhash.cc

from libc cimport stdint
from pyarrow.lib cimport CArray, pyarrow_unwrap_array, shared_ptr


cdef extern from "xxhash.h":
    stdint.uint64_t XXH3_64bits(const void* input, size_t length);


def get_array_length(obj):
    # Just an example function accessing both the pyarrow Cython API
    # and the Arrow C++ API
    cdef shared_ptr[CArray] arr = pyarrow_unwrap_array(obj)
    if arr.get() == NULL:
        raise TypeError("not an array")
    # return XXH3_64bits(arr.get(), 8)
    return arr.get().length()
