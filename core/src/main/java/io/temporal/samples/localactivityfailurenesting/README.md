# Local activity retries build a `Failure` chain the SDK cannot deserialize

A local activity that keeps failing accumulates an unbounded `Failure` cause chain in its
`MarkerRecorded` events. After 53 attempts the chain exceeds protobuf-java's recursion limit, and from
that moment **no Java worker can read the workflow's own history**. The execution is permanently stuck:
it never completes, never fails, and the server retries the workflow task forever.

## Reproduce

```bash
temporal server start-dev

./gradlew -PmainClass=io.temporal.samples.localactivityfailurenesting.LocalActivityFailureNesting \
  core:execute
```
After ~50 seconds the worker starts logging:

```
Caused by: com.google.protobuf.InvalidProtocolBufferException: Protocol message had too many levels
of nesting.  May be malicious.  Use setRecursionLimit() to increase the recursion depth limit.
    at com.google.protobuf.InvalidProtocolBufferException.recursionLimitExceeded(InvalidProtocolBufferException.java:133)
    at com.google.protobuf.CodedInputStream.checkRecursionLimit(CodedInputStream.java:177)
    at com.google.protobuf.CodedInputStream$StreamDecoder.readMessage(CodedInputStream.java:2328)
    at io.temporal.api.failure.v1.Failure$Builder.mergeFrom(Failure.java:1371)
    at io.temporal.api.failure.v1.Failure$Builder.mergeFrom(Failure.java:1018)
    ...
```

The full gRPC-level failure, from the poller thread:

```
WARN i.t.internal.worker.BasePoller - Failure in poller thread Workflow Poller ...
io.grpc.StatusRuntimeException: INTERNAL: Invalid protobuf byte sequence
    at io.grpc.MethodDescriptor.parseResponse(MethodDescriptor.java:284)
    at io.grpc.stub.ClientCalls.blockingUnaryCall(ClientCalls.java:166)
    at io.temporal.api.workflowservice.v1.WorkflowServiceGrpc$WorkflowServiceBlockingStub
          .pollWorkflowTaskQueue(WorkflowServiceGrpc.java:8146)
    at io.temporal.internal.worker.WorkflowPollTask.doPoll(WorkflowPollTask.java:189)
Caused by: com.google.protobuf.InvalidProtocolBufferException: Protocol message had too many levels ...
```

**Expected:** the local activity exhausts its retry policy and the failure propagates to the workflow.

**Actual:** the workflow becomes unreadable to its own worker and is stuck permanently.
