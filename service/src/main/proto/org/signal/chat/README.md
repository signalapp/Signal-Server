#  Chat gRPC API 

## Errors

The chat-server gRPC API uses the [well-defined gRPC status codes](https://grpc.github.io/grpc/core/md_doc_statuscodes.html) returned as part of every RPC call. Common error conditions in the API are mapped to some status code and rather than being explicitly represented in the RPC messages. Broadly, there are 3 categories of errors:

1. Common status errors: gRPC error status that should be handled identically across many RPC calls
2. Domain specific status errors: gRPC error statuses that accurately represent an error case of an individual RPC
3. Domain specific custom errors: Errors represented explicitly in the protobuf return type for an RPC call

Server APIs attempt to include additional debugging information about the nature of the error in the detail string of the status. Status messages may change at any time and are intended only as a debugging aid; consumers of status messages must never make assumptions about their structure or content.
### Common status errors

Common status errors are errors that may be handled similarly, regardless of which RPC is being called. This usually means retrying, ignoring, or logging for bug reporting. For example, the chat gRPC API reserves `RESOURCE_EXHAUSTED` for temporary exhaustion of a per-user resource. Regardless of the RPC being called, the caller should handle this error by retrying with a backoff, using the `retry-after` time interval specified via trailing metadata if the server provides it. 

Common status errors are unexpected. They represent a transient condition that causes requests to fail or a misbehaving client/server. Errors expected in the normal path of operation should use one of the domain specific error types. For example, looking up a username that does not exist by hash does _not_ return common status error since that is an expected response. However, if the lookup request does not contain a valid hash, that _is_ a common status error because it means the client did not properly construct the request.

Currently, any RPC may return one of these common errors. Unless otherwise indicated, consult the [gRPC docs](https://grpc.github.io/grpc/core/md_doc_statuscodes.html) for their interpretation.


| code                 | number | notes                                                                                                                                                                                                                                      |
|----------------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `UNKNOWN`            | 2      |                                                                                                                                                                                                                                            |
| `INVALID_ARGUMENT`   | 3      | Violates some constraint documented in the message protobuf. Constraints may be expressed via comments or custom `require` annotations on the request.                                                                                     |
| `RESOURCE_EXHAUSTED` | 8      | A per-user resource has been temporarily exhausted (usually a rate-limit). A `retry-after` header containing an ISO8601 duration string may be present in the response headers, indicating how long it will be before a retry can succeed. |
| `INTERNAL`           | 13     |                                                                                                                                                                                                                                            |
| `UNAVAILABLE`        | 14     |                                                                                                                                                                                                                                            |
| `UNAUTHENTICATED`    | 16     | Indicates some problem with the caller's authentication status. For example, calling an authenticated endpoint from an unauthenticated connection, or calling an group send endorsement (GSE) authenticated endpoint with an invalid GSE.  |


If one of these errors has a more specific meaning for a certain RPC, it will be documented on the RPC. Note this section does not include errors that may be generated on the client side (like `CANCELLED` or `DEADLINE_EXCEEDED`), consult your client's gRPC documentation.

### Domain specific status errors

Occasionally, an RPC will return a status error for a widely understood error scenario that is not applicable across all RPCs or has some domain specific handling. For example, sending a message to a user may return `NOT_FOUND` if that user does not exist.

Any domain specific errors will always be documented on the RPC or enclosing service.

### Domain specific custom errors

All other errors are explicitly represented as part of the response definitions for the RPC. Typically, an RPC will provide custom errors when:
- No gRPC status maps to the represented error
- Additional structured data needs to be returned along with the error(s)
- The error does not represent a client bug, but requires handling beyond retrying or ignoring
