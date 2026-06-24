package io.temporal.samples.claimcheckmemo;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.Collections;

public class ClaimCheckMemoWorkflowImpl implements ClaimCheckMemoWorkflow {

  private static final int STAGE_PERCENT = 25;

  private final ClaimCheckActivities activities =
      Workflow.newActivityStub(
          ClaimCheckActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(5)).build());

  private int stage = 0;
  private boolean advanced = false;
  private boolean finished = false;

  @Override
  public String run(String largeInput) {
    String result = activities.process(largeInput);

    while (!finished) {
      int percent = Math.min(stage * STAGE_PERCENT, 100);
      // Workflow-side memo update. Goes through the same converter+codec path as the
      // client-side WorkflowOptions.setMemo, so ProgressMemo stays inline here too.
      Workflow.upsertMemo(Collections.singletonMap("progress", new ProgressMemo(percent)));

      // Pause at this progress value until signalled.
      advanced = false;
      Workflow.await(() -> advanced || finished);
      if (advanced) {
        stage++;
      }
    }

    Workflow.upsertMemo(Collections.singletonMap("progress", new ProgressMemo(100)));
    return result;
  }

  @Override
  public void advance() {
    advanced = true;
  }

  @Override
  public void finish() {
    finished = true;
  }
}
