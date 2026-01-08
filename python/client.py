import asyncio
import inspect
import requests
from typing import Any
from collections.abc import AsyncGenerator, Callable

import grpc
from grpc import ClientCallDetails, RpcError, intercept_channel, secure_channel, ssl_channel_credentials
from grpc_interceptor.client import ClientInterceptor, ClientCallDetails

from compassiot.gateway.v1.gateway_pb2 import AuthenticateRequest
from compassiot.gateway.v1.gateway_pb2_grpc import ServiceStub

from pathlib import Path

def load_api_key(path: str = "api_key.txt") -> str:
    """
    Load an API key from a text file and return it as a string.
    Strips surrounding whitespace/newlines.
    """
    key = Path(path).read_text(encoding="utf-8").strip()

    if not key:
        raise ValueError(f"{path} is empty")

    return key

HOST = "api.compassiot.cloud"
SECRET = load_api_key("api_key.txt")

TIMEOUT_SEC = 60 * 25  # used by retryStream
SHORT_TIMEOUT_SEC = 60 * 5


def create_gateway_client() -> ServiceStub:
	# UnaryRestInterceptor must be last as it's the layer which makes the API call,
	# unlike AccessTokenInterceptor which just populates the header
	interceptors = [
		AccessTokenInterceptor(HOST, SECRET), 
		UnaryRestInterceptor(HOST)
	]
	channel = secure_channel(HOST, ssl_channel_credentials())
	channel = intercept_channel(channel, *interceptors)
	return ServiceStub(channel)


class UnaryRestInterceptor(grpc.UnaryUnaryClientInterceptor):
	"""
	Shim to convert unary gRPC calls to pure REST, due to several suspected regressions in
	core gRPC library:
	- https://github.com/grpc/grpc/issues/29706
	- https://github.com/grpc/grpc/issues/33935
	"""

	_HTTP_HEADERS = {"content-type": "application/proto"}

	@staticmethod
	def _build_deserializer_map():
		with secure_channel("mock", ssl_channel_credentials()) as channel:
			mock_service = ServiceStub(channel)
			stub_method_tuples = list(filter(lambda member: not member[0].startswith("__"), inspect.getmembers(mock_service)))
			map = {}
			for (k, v) in stub_method_tuples:
				map[k] = v._response_deserializer
			return map
		
	@staticmethod
	def _cast_grpc_error(response: requests.Response):
		has_error = False
		error = RpcError()
		if response.status_code >= 200 and response.status_code < 300:
			has_error = False
		if response.status_code == 400:
			error.code = lambda: grpc.StatusCode.INVALID_ARGUMENT
			has_error = True
		if response.status_code == 401:
			error.code = lambda: grpc.StatusCode.UNAUTHENTICATED
			has_error = True
		if response.status_code == 403:
			error.code = lambda: grpc.StatusCode.PERMISSION_DENIED
			has_error = True
		if response.status_code == 404:
			error.code = lambda: grpc.StatusCode.NOT_FOUND
			has_error = True
		if response.status_code == 412:
			error.code = lambda: grpc.StatusCode.FAILED_PRECONDITION
			has_error = True
		if response.status_code == 429:
			error.code = lambda: grpc.StatusCode.RESOURCE_EXHAUSTED
			has_error = True
		if response.status_code >= 500:
			error.code = lambda: grpc.StatusCode.INTERNAL
			has_error = True
		
		if has_error is True:
			error.details = lambda: response.content.decode()
			return error
		else:
			return None

	def __init__(self, host: str):
		self.host = host
		self.deserializer_map = self._build_deserializer_map()

	def _call_rest(self, request: Any, call_details: ClientCallDetails):
		url = "https://%s/%s" % (self.host, call_details.method.strip("/"))

		# Copy headers
		headers = self._HTTP_HEADERS.copy()
		if call_details.metadata is not None:
			for (k, v) in call_details.metadata:
				headers[k] = v

		# Create future
		future = asyncio.get_event_loop().create_future()

		# Make request & deserialize it
		response = requests.post(url, data=request.SerializeToString(True), headers=headers)
		error = self._cast_grpc_error(response)
		if error is not None:
			future.set_exception(error)
		else:
			rpc = call_details.method.split("/")[-1]
			deserializer = self.deserializer_map[rpc]
			future.set_result(deserializer(response.content))
		return future
	
	def intercept_unary_unary(self, next, call_details: ClientCallDetails, request: Any):
		return self._call_rest(request, call_details)


class AccessTokenInterceptor(ClientInterceptor):
	def __init__(self, host: str, secret: str) -> None:
		self.host = host
		self.secret = secret
		self.access_token = self._get_access_token(host, secret)

	@staticmethod
	def _get_access_token(host: str, secret: str) -> str:
		interceptors = [UnaryRestInterceptor(host)]
		with intercept_channel(secure_channel(host, ssl_channel_credentials()), *interceptors) as channel:
			service = ServiceStub(channel)
			response = service.Authenticate(AuthenticateRequest(token=secret))
			return response.access_token

	@staticmethod
	def _create_details_with_auth(call_details: ClientCallDetails, access_token: str) -> ClientCallDetails:
		return ClientCallDetails(
			call_details.method,
			call_details.timeout,
			[("authorization", "Bearer %s" % (access_token))],
			call_details.credentials,
			call_details.wait_for_ready,
			call_details.compression,
		)
	
	def intercept(self, method: Callable[..., Any], request_or_iterator: Any, call_details: ClientCallDetails):
		try:
			return method(request_or_iterator, self._create_details_with_auth(call_details, self.access_token))
		except RpcError as error:
			if error.code == grpc.StatusCode.UNAUTHENTICATED:
				self.access_token = self._get_access_token(self.host, self.secret)
				return method(request_or_iterator, self._create_details_with_auth(call_details, self.access_token))
			else:
				raise error


def retry_stream(stream: Callable[[None], AsyncGenerator]) -> AsyncGenerator:
	generator = stream()
	while True:
		try:
			yield from generator
		except RpcError as error:
			if error.code() is grpc.StatusCode.DEADLINE_EXCEEDED:
					print("DeadlineExceeded, retrying stream")
					generator = stream()
					continue
			else:
					raise error


def get_enum_str(response, descriptor_field_number, enum_value):
	enum_obj = response.DESCRIPTOR.fields_by_number[descriptor_field_number].enum_type
	if enum_obj is None:
		return None
	return enum_obj.values_by_number[enum_value].name
