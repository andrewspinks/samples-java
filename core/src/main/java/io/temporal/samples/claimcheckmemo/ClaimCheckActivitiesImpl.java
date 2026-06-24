package io.temporal.samples.claimcheckmemo;

class ClaimCheckActivitiesImpl implements ClaimCheckActivities {
  @Override
  public String process(String largeInput) {
    // Echo back a large-ish result so it is offloaded by the claim-check codec.
    return "processed(" + largeInput.length() + " chars): " + largeInput;
  }
}
