package io.temporal.samples.localactivityfailurenesting;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.RetryOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;

/**
 * A repeatedly failing local activity accumulates an unbounded {@code Failure} cause chain until
 * the workflow's own history can no longer be deserialized by the Java SDK, at which point the
 * execution is permanently stuck.
 *
 * <p>Run against {@code temporal server start-dev}. After about 50 seconds — 53 local activity
 * attempts — the workflow stops making progress and the poller begins logging:
 *
 * <pre>
 *   io.grpc.StatusRuntimeException: INTERNAL: Invalid protobuf byte sequence
 *       at io.grpc.MethodDescriptor.parseResponse(MethodDescriptor.java:284)
 *       at ...WorkflowServiceBlockingStub.pollWorkflowTaskQueue(WorkflowServiceGrpc.java:8146)
 *       at io.temporal.internal.worker.WorkflowPollTask.doPoll(WorkflowPollTask.java:189)
 *   Caused by: com.google.protobuf.InvalidProtocolBufferException: Protocol message had too many
 *   levels of nesting.  May be malicious.  Use setRecursionLimit() to increase the recursion depth
 *   limit.
 *       at io.temporal.api.failure.v1.Failure$Builder.mergeFrom(Failure.java:1371)
 * </pre>
 *
 * <p>The same exception also appears as {@code CANCELLED: Failed to read message.} — gRPC chooses
 * between the two depending on whether the response fails in the buffered {@code parseResponse}
 * path or in the streaming deframer, which cancels the stream instead.
 *
 * <p>Because this fails in the poller there is no in-flight workflow task to report against, so the
 * server records only repeating {@code WORKFLOW_TASK_TIMED_OUT}. With a history large enough that
 * the over-deep marker arrives in a later {@code GetWorkflowExecutionHistory} page rather than in
 * the poll response, the same failure instead lands inside a workflow task and is reported as
 * {@code WORKFLOW_TASK_FAILED} — see this package's README.
 *
 * <p>Either way the workflow never completes and never fails, and the server retries forever.
 */
public class LocalActivityFailureNesting {

  static final String TASK_QUEUE = "LocalActivityFailureNesting";

  @ActivityInterface
  public interface Activities {
    @ActivityMethod
    void alwaysTimesOut();
  }

  public static class ActivitiesImpl implements Activities {
    @Override
    public void alwaysTimesOut() {
      try {
        // Exceeds startToCloseTimeout, so every attempt times out and is retried.
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @WorkflowInterface
  public interface NestingWorkflow {
    @WorkflowMethod
    void run();
  }

  public static class NestingWorkflowImpl implements NestingWorkflow {

    private final Activities activities =
        Workflow.newLocalActivityStub(
            Activities.class,
            LocalActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMillis(200))
                // Retries with a backoff below this stay inside the worker and record nothing.
                // Above it, each retry round-trips through the workflow and records a marker --
                // which is the path that grows the failure chain.
                .setLocalRetryThreshold(Duration.ofMillis(30))
                .setRetryOptions(
                    RetryOptions.newBuilder()
                        .setInitialInterval(Duration.ofMillis(1))
                        .setBackoffCoefficient(2.0)
                        .setMaximumInterval(Duration.ofMillis(100))
                        // Backoffs 1,2,4,8,16ms stay under the 30ms threshold, so attempts 1-5 are
                        // in-worker; attempt 6 backs off 32ms and records the first marker. The
                        // history becomes unreadable long before this limit is reached.
                        .setMaximumAttempts(60)
                        .build())
                .build());

    @Override
    public void run() {
      activities.alwaysTimesOut();
    }
  }

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(NestingWorkflowImpl.class);
    worker.registerActivitiesImplementations(new ActivitiesImpl());
    factory.start();

    System.out.println("Started. Expect the worker to stop making progress after ~50 seconds.");

    client
        .newWorkflowStub(
            NestingWorkflow.class, WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).build())
        .run(); // Never returns.
  }
}
