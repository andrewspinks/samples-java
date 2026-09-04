package io.temporal.samples.localactivityfailurenesting;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.activity.LocalActivityOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.RetryOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;

/** Version activity change */
public class LocalActivityConversion {

  static final String TASK_QUEUE = "LocalActivityFailureNesting";

  @ActivityInterface
  public interface Activities {
    @ActivityMethod
    void executeReceive();

    @ActivityMethod
    void executeProcess();
  }

  public static class ActivitiesImpl implements Activities {
    @Override
    public void executeReceive() {}

    @Override
    public void executeProcess() {}
  }

  @WorkflowInterface
  public interface NestingWorkflow {
    @WorkflowMethod
    void run();

    @SignalMethod
    void complete();
  }

  public static class NestingWorkflowImpl implements NestingWorkflow {
    private boolean complete = false;

    private final Activities localActivities =
        Workflow.newLocalActivityStub(
            Activities.class,
            LocalActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMillis(200))
                .setLocalRetryThreshold(Duration.ofMillis(30))
                .setRetryOptions(
                    RetryOptions.newBuilder()
                        .setInitialInterval(Duration.ofMillis(1))
                        .setBackoffCoefficient(2.0)
                        .setMaximumInterval(Duration.ofMillis(100))
                        .setMaximumAttempts(500)
                        .build())
                .build());

    private final Activities activities =
        Workflow.newActivityStub(
            Activities.class,
            ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMillis(200))
                .setRetryOptions(
                    RetryOptions.newBuilder()
                        .setInitialInterval(Duration.ofMillis(1))
                        .setBackoffCoefficient(2.0)
                        .setMaximumInterval(Duration.ofMillis(100))
                        .setMaximumAttempts(500)
                        .build())
                .build());

    @Override
    public void run() {
      int version =
          Workflow.getVersion(
              "LocalActivityConvertedToStandardActivity", Workflow.DEFAULT_VERSION, 1);
      if (version == 1) {
        activities.executeReceive();
        activities.executeProcess();
      } else {
        localActivities.executeReceive();
        localActivities.executeProcess();
      }

      Workflow.await(() -> this.complete);
    }

    @Override
    public void complete() {
      this.complete = true;
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
