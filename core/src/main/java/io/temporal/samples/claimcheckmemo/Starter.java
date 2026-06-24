package io.temporal.samples.claimcheckmemo;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import java.io.IOException;
import java.util.Collections;

/**
 * Starts a long-running claim-check workflow and keeps a worker running to host it.
 *
 * <p>The claim-check {@link io.temporal.payload.codec.PayloadCodec} offloads everything to the
 * object store except memos, which {@link InlineMemoPayloadConverter} marks exempt so they stay
 * inline. The workflow sets a progress memo and then pauses, so you can read the memo repeatedly
 * from another process with {@link QueryProgress} (mirroring a frequent progress-poll endpoint).
 */
public class Starter {

  static final String TASK_QUEUE = "claim-check";
  static final String WORKFLOW_ID = "claim-check-test-execution";

  public static void main(String[] args) {
    ClientConfigProfile profile;
    try {
      profile = ClientConfigProfile.load();
    } catch (IOException e) {
      throw new RuntimeException("Failed to load client configuration", e);
    }
    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(profile.toWorkflowServiceStubsOptions());

    ClaimCheckObjectStore.resetAll();

    WorkflowClient client =
        WorkflowClient.newInstance(
            service,
            WorkflowClientOptions.newBuilder()
                .setDataConverter(ClaimCheckDataConverter.newInstance())
                .build());

    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(ClaimCheckMemoWorkflowImpl.class);
    worker.registerActivitiesImplementations(new ClaimCheckActivitiesImpl());
    factory.start();

    String largeInput = buildLargeInput();

    ClaimCheckMemoWorkflow workflow =
        client.newWorkflowStub(
            ClaimCheckMemoWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(TASK_QUEUE)
                .setWorkflowId(WORKFLOW_ID)
                .setMemo(Collections.singletonMap("progress", new ProgressMemo(0)))
                .build());
    WorkflowClient.start(workflow::run, largeInput);

    System.out.println("Started workflow '" + WORKFLOW_ID + "' with an initial progress memo.");
    System.out.println(
        "Run QueryProgress to read the memo. Worker is running; press Ctrl+C to stop.");
    // Keep the worker alive so the workflow stays hosted and queryable.
  }

  private static String buildLargeInput() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      sb.append("large-input-payload-that-should-be-offloaded ");
    }
    return sb.toString();
  }
}
