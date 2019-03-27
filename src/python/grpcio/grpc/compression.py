# Copyright 2019 The gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from grpc._cython import cygrpc

NoCompression = cygrpc.CompressionAlgorithm.none
Deflate = cygrpc.CompressionAlgorithm.deflate
Gzip = cygrpc.CompressionAlgorithm.gzip


def _compression_algorithm_to_metadata_value(compression):
    if compression == NoCompression:
        return 'identity'
    elif compression == Deflate:
        return 'deflate'
    elif compression == Gzip:
        return 'gzip'
    else:
        raise ValueError(
            'Unknown compression algorithm "{}".'.format(compression))


def _compression_algorithm_to_metadata(compression):
    return (cygrpc.GRPC_COMPRESSION_REQUEST_ALGORITHM_MD_KEY,
            _compression_algorithm_to_metadata_value(compression))


def _augment_metadata(metadata, compression):
    if not metadata and not compression:
        return None
    base_metadata = tuple(metadata) if metadata else ()
    compression_metadata = (
        _compression_algorithm_to_metadata(compression),) if compression else ()
    return base_metadata + compression_metadata


__all__ = (
    "NoCompression",
    "Deflate",
    "Gzip",
)
