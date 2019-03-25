# Copyright 2016 gRPC authors.
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
"""Tests server and client side compression."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import contextlib
import logging
import grpc
import grpc.compression
from grpc import _grpcio_metadata

from tests.unit import test_common
from tests.unit.framework.common import test_constants
from tests.unit import _tcp_proxy

_UNARY_UNARY = '/test/UnaryUnary'
_UNARY_STREAM = '/test/UnaryStream'
_STREAM_UNARY = '/test/StreamUnary'
_STREAM_STREAM = '/test/StreamStream'


def make_handle_unary_unary(pre_response_callback):

    def _handle_unary(request, servicer_context):
        if pre_response_callback:
            pre_response_callback(request, servicer_context)
        return request

    return _handle_unary


def make_handle_unary_stream(pre_response_callback):

    def _handle_unary_stream(request, servicer_context):
        if pre_response_callback:
            pre_response_callback(request, servicer_context)
        for _ in range(test_constants.STREAM_LENGTH):
            yield request

    return _handle_unary_stream


def make_handle_stream_unary(pre_response_callback):

    def _handle_stream_unary(request_iterator, servicer_context):
        if pre_response_callback:
            pre_response_callback(request_iterator, servicer_context)
        response = None
        for request in request_iterator:
            if not response:
                response = request
        return response

    return _handle_stream_unary


def make_handle_stream_stream(pre_response_callback):

    def _handle_stream(request_iterator, servicer_context):
        if pre_response_callback:
            pre_response_callback(request_iterator, servicer_context)
        # TODO(issue:#6891) We should be able to remove this loop,
        # and replace with return; yield
        for request in request_iterator:
            yield request

    return _handle_stream


def set_call_compression(request_or_iterator, servicer_context):
    del request_or_iterator
    servicer_context.set_compression(grpc.compression.Gzip)


def disable_next_compression(request_or_iterator, servicer_context):
    del request_or_iterator
    servicer_context.disable_next_message_compression()


class _MethodHandler(grpc.RpcMethodHandler):

    def __init__(self, request_streaming, response_streaming,
                 pre_response_callback):
        self.request_streaming = request_streaming
        self.response_streaming = response_streaming
        self.request_deserializer = None
        self.response_serializer = None
        self.unary_unary = None
        self.unary_stream = None
        self.stream_unary = None
        self.stream_stream = None

        if self.request_streaming and self.response_streaming:
            self.stream_stream = make_handle_stream_stream(
                pre_response_callback)
        elif not self.request_streaming and not self.response_streaming:
            self.unary_unary = make_handle_unary_unary(pre_response_callback)
        elif not self.request_streaming and self.response_streaming:
            self.unary_stream = make_handle_unary_stream(pre_response_callback)
        else:
            self.stream_unary = make_handle_stream_unary(pre_response_callback)



class _GenericHandler(grpc.GenericRpcHandler):

    def __init__(self, pre_response_callback):
        self._pre_response_callback = pre_response_callback

    def service(self, handler_call_details):
        if handler_call_details.method == _UNARY_UNARY:
            return _MethodHandler(False, False, self._pre_response_callback)
        elif handler_call_details.method == _UNARY_STREAM:
            return _MethodHandler(False, True, self._pre_response_callback)
        elif handler_call_details.method == _STREAM_UNARY:
            return _MethodHandler(True, False, self._pre_response_callback)
        elif handler_call_details.method == _STREAM_STREAM:
            return _MethodHandler(True, True, self._pre_response_callback)
        else:
            return None


@contextlib.contextmanager
def _instrumented_client_server_pair(channel_kwargs, server_handler):
    host = 'localhost'
    server = test_common.test_server()
    server.add_generic_rpc_handlers((server_handler,))
    server_port = server.add_insecure_port('[::]:0')
    server.start()
    with _tcp_proxy.TcpProxy(host, host, server_port) as proxy:
        proxy_port = proxy.get_port()
        with grpc.insecure_channel('{}:{}'.format(host, proxy_port),
                                   **channel_kwargs) as client_channel:
            try:
                yield client_channel, proxy, server
            finally:
                server.stop(None)


# TODO(rbellevi): Enable other arities in this function.
def _get_byte_counts(channel_kwargs, multicallable_kwargs, client_function,
                     server_handler, message):
    with _instrumented_client_server_pair(channel_kwargs,
                                          server_handler) as pipeline:
        client_channel, proxy, server = pipeline
        client_function(client_channel, multicallable_kwargs, message)
        return proxy.get_byte_count()


def _get_byte_differences(client_function, first_channel_kwargs,
                          first_multicallable_kwargs, first_server_handler,
                          second_channel_kwargs, second_multicallable_kwargs,
                          second_server_handler, message):
    first_bytes_sent, first_bytes_received = _get_byte_counts(
        first_channel_kwargs, first_multicallable_kwargs, client_function,
        first_server_handler, message)
    second_bytes_sent, second_bytes_received = _get_byte_counts(
        second_channel_kwargs, second_multicallable_kwargs, client_function,
        second_server_handler, message)
    return second_bytes_sent - first_bytes_sent, second_bytes_received - first_bytes_received


_REQUEST = b'\x00' * 100


def _unary_unary_client(channel, multicallable_kwargs, message):
    multi_callable = channel.unary_unary(_UNARY_UNARY)
    response = multi_callable(message, **multicallable_kwargs)
    if response != message:
        raise RuntimeError("Request '{}' != Response '{}'".format(
            message, response))


def _unary_stream_client(channel, multicallable_kwargs, message):
    multi_callable = channel.unary_stream(_UNARY_STREAM)
    response_iterator = multi_callable(message, **multicallable_kwargs)
    for response in response_iterator:
        if response != message:
            raise RuntimeError("Request '{}' != Response '{}'".format(
                message, response))


def _stream_unary_client(channel, multicallable_kwargs, message):
    multi_callable = channel.stream_unary(_STREAM_UNARY)
    requests = (_REQUEST for _ in range(test_constants.STREAM_LENGTH))
    response = multi_callable(requests, **multicallable_kwargs)
    if response != message:
        raise RuntimeError("Request '{}' != Response '{}'".format(
            message, response))


class CompressionTest(unittest.TestCase):

    def setUp(self):
        pass
        # self._server = test_common.test_server()
        # self._server.add_generic_rpc_handlers((_GenericHandler(None),))
        # self._port = self._server.add_insecure_port('[::]:0')
        # self._server.start()

    def tearDown(self):
        pass
        # self._server.stop(None)

    def assertCompressed(self, bytes_difference):
        self.assertLess(
            bytes_difference,
            0,
            msg='Second stream was {} bytes bigger than first stream.'.format(
                bytes_difference))

    def assertNotCompressed(self, bytes_difference):
        self.assertGreaterEqual(
            bytes_difference,
            0,
            msg='Second stream was {} bytes smaller than first stream.'.format(
                -1 * bytes_difference))

    def testChannelCompressedUnaryUnary(self):
        uncompressed_channel_kwargs = {}
        compressed_channel_kwargs = {
            'compression': grpc.compression.Deflate,
        }
        bytes_sent_difference, bytes_received_difference = _get_byte_differences(
            _unary_unary_client, uncompressed_channel_kwargs, {},
            _GenericHandler(None), compressed_channel_kwargs, {},
            _GenericHandler(set_call_compression), _REQUEST)
        print("Bytes sent difference: {}".format(bytes_sent_difference))
        print("Bytes received difference: {}".format(bytes_received_difference))
        self.assertCompressed(bytes_sent_difference)
        self.assertCompressed(bytes_received_difference)

    # TODO(rbellevi): Implement.
    def testChannelCompressedStreaming(self):
        pass

    # TODO(rbellevi): Abstract this repetition away.
    def testCallCompressedUnaryUnary(self):
        uncompressed_channel_kwargs = {}
        compressed_multicallable_kwargs = {
            'compression': grpc.compression.Deflate,
        }
        bytes_sent_difference, bytes_received_difference = _get_byte_differences(
            _unary_unary_client, uncompressed_channel_kwargs, {},
            _GenericHandler(None), uncompressed_channel_kwargs,
            compressed_multicallable_kwargs,
            _GenericHandler(set_call_compression), _REQUEST)
        print("Bytes sent difference: {}".format(bytes_sent_difference))
        print("Bytes received difference: {}".format(bytes_received_difference))
        self.assertCompressed(bytes_sent_difference)
        self.assertCompressed(bytes_received_difference)

    def testCallCompressedUnaryStream(self):
        uncompressed_channel_kwargs = {}
        compressed_multicallable_kwargs = {
            'compression': grpc.compression.Deflate,
        }
        bytes_sent_difference, bytes_received_difference = _get_byte_differences(
            _unary_stream_client, uncompressed_channel_kwargs, {},
            _GenericHandler(None), uncompressed_channel_kwargs,
            compressed_multicallable_kwargs,
            _GenericHandler(set_call_compression), _REQUEST)
        print("Bytes sent difference: {}".format(bytes_sent_difference))
        print("Bytes received difference: {}".format(bytes_received_difference))
        self.assertCompressed(bytes_sent_difference)
        self.assertCompressed(bytes_received_difference)

    def testCallCompressedStreamUnary(self):
        uncompressed_channel_kwargs = {}
        compressed_multicallable_kwargs = {
            'compression': grpc.compression.Deflate,
        }
        bytes_sent_difference, bytes_received_difference = _get_byte_differences(
            _stream_unary_client, uncompressed_channel_kwargs, {},
            _GenericHandler(None), uncompressed_channel_kwargs,
            compressed_multicallable_kwargs,
            _GenericHandler(set_call_compression), _REQUEST)
        print("Bytes sent difference: {}".format(bytes_sent_difference))
        print("Bytes received difference: {}".format(bytes_received_difference))
        self.assertCompressed(bytes_sent_difference)
        self.assertCompressed(bytes_received_difference)

    def testDisableNextCompressionUnaryUnary(self):
        uncompressed_channel_kwargs = {}
        compressed_channel_kwargs = {
            'compression': grpc.compression.Deflate,
        }
        bytes_sent_difference, bytes_received_difference = _get_byte_differences(
            _unary_unary_client, uncompressed_channel_kwargs, {},
            _GenericHandler(None), compressed_channel_kwargs, {},
            _GenericHandler(disable_next_compression), _REQUEST)
        print("Bytes sent difference: {}".format(bytes_sent_difference))
        print("Bytes received difference: {}".format(bytes_received_difference))
        self.assertCompressed(bytes_sent_difference)
        self.assertNotCompressed(bytes_received_difference)

    # TODO(rbellevi): Implement.
    def testDisableNextCompressionStreaming(self):
        pass

    # TODO(rbellevi): Reevaluate these old tests.

    # def testUnary(self):
    #     request = b'\x00' * 100

    #     # Client -> server compressed through default client channel compression
    #     # settings. Server -> client compressed via server-side metadata setting.
    #     # TODO(https://github.com/grpc/grpc/issues/4078): replace the "1" integer
    #     # literal with proper use of the public API.
    #     compressed_channel = grpc.insecure_channel(
    #         'localhost:%d' % self._port,
    #         options=[('grpc.default_compression_algorithm', 1)])
    #     multi_callable = compressed_channel.unary_unary(_UNARY_UNARY)
    #     response = multi_callable(request)
    #     self.assertEqual(request, response)

    #     # Client -> server compressed through client metadata setting. Server ->
    #     # client compressed via server-side metadata setting.
    #     # TODO(https://github.com/grpc/grpc/issues/4078): replace the "0" integer
    #     # literal with proper use of the public API.
    #     uncompressed_channel = grpc.insecure_channel(
    #         'localhost:%d' % self._port,
    #         options=[('grpc.default_compression_algorithm', 0)])
    #     multi_callable = compressed_channel.unary_unary(_UNARY_UNARY)
    #     response = multi_callable(
    #         request, metadata=[('grpc-internal-encoding-request', 'gzip')])
    #     self.assertEqual(request, response)
    #     compressed_channel.close()

    # def testStreaming(self):
    #     request = b'\x00' * 100

    #     # TODO(https://github.com/grpc/grpc/issues/4078): replace the "1" integer
    #     # literal with proper use of the public API.
    #     compressed_channel = grpc.insecure_channel(
    #         'localhost:%d' % self._port,
    #         options=[('grpc.default_compression_algorithm', 1)])
    #     multi_callable = compressed_channel.stream_stream(_STREAM_STREAM)
    #     call = multi_callable(iter([request] * test_constants.STREAM_LENGTH))
    #     for response in call:
    #         self.assertEqual(request, response)
    #     compressed_channel.close()


if __name__ == '__main__':
    logging.basicConfig()
    unittest.main(verbosity=2)
