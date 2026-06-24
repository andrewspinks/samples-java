package io.temporal.samples.claimcheckmemo;

import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Long-running workflow that reports progress through a memo and pauses between stages so the memo
 * can be polled at different progress values.
 */
@WorkflowInterface
public interface ClaimCheckMemoWorkflow {

  /**
   * Runs an activity on the (large) input, then loops forever updating the progress memo and
   * pausing until {@link #advance()} or {@link #finish()} is received. Returns only after {@link
   * #finish()}.
   */
  @WorkflowMethod
  String run(String largeInput);

  /** Move to the next progress stage. */
  @SignalMethod
  void advance();

  /** Release the final pause and let the workflow complete. */
  @SignalMethod
  void finish();
}
