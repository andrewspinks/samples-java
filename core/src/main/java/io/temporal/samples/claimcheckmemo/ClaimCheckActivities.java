package io.temporal.samples.claimcheckmemo;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface ClaimCheckActivities {

  /** Returns a deliberately large result so activity input/output get offloaded by the codec. */
  @ActivityMethod
  String process(String largeInput);
}
