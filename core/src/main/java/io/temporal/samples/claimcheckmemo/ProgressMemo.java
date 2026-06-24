package io.temporal.samples.claimcheckmemo;

/**
 * Small value object stored as a workflow memo to report progress.
 *
 * <p>This is the type we want to keep <b>inline</b> in Temporal (not offloaded by the claim-check
 * codec) so that a frequent progress-poll endpoint can read it from {@code
 * DescribeWorkflowExecutionResponse} without hitting the object store on every poll.
 *
 * <p>Needs a no-arg constructor and getters/setters so Jackson can round-trip it.
 */
public class ProgressMemo {
  private int percent;

  public ProgressMemo() {}

  public ProgressMemo(int percent) {
    this.percent = percent;
  }

  public int getPercent() {
    return percent;
  }

  public void setPercent(int percent) {
    this.percent = percent;
  }

  @Override
  public String toString() {
    return "ProgressMemo{percent=" + percent + "}";
  }
}
