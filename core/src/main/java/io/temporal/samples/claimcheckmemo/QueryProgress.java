package io.temporal.samples.claimcheckmemo;

import com.google.protobuf.ByteString;
import io.temporal.api.common.v1.Payload;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.common.converter.EncodingKeys;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.payload.context.WorkflowSerializationContext;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.io.IOException;

/**
 * Reads the progress memo of the workflow started by {@link Starter} via {@code
 * DescribeWorkflowExecutionResponse} — the way a progress-poll endpoint would.
 *
 * <p>Decoding the memo requires a client configured with the same {@link
 * io.temporal.common.converter.DataConverter} used to write it ({@link ClaimCheckDataConverter}).
 * The converter supplies the namespace and decodes the {@link Payload} into a {@link ProgressMemo}.
 * Because the memo is kept inline (not offloaded), this decode needs no object-store round trip.
 */
public class QueryProgress {

  public static void main(String[] args) {
    ClientConfigProfile profile;
    try {
      profile = ClientConfigProfile.load();
    } catch (IOException e) {
      throw new RuntimeException("Failed to load client configuration", e);
    }
    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(profile.toWorkflowServiceStubsOptions());

    // Same DataConverter as the starter so the memo can be decoded; the client also gives us the
    // namespace to describe in.
    WorkflowClient client =
        WorkflowClient.newInstance(
            service,
            WorkflowClientOptions.newBuilder()
                .setDataConverter(ClaimCheckDataConverter.newInstance())
                .build());
    String namespace = client.getOptions().getNamespace();

    DescribeWorkflowExecutionResponse response =
        service
            .blockingStub()
            .describeWorkflowExecution(
                DescribeWorkflowExecutionRequest.newBuilder()
                    .setNamespace(namespace)
                    .setExecution(
                        WorkflowExecution.newBuilder().setWorkflowId(Starter.WORKFLOW_ID).build())
                    .build());

    Payload memo = response.getWorkflowExecutionInfo().getMemo().getFieldsMap().get("progress");
    if (memo == null) {
      System.out.println(
          "No 'progress' memo found for workflow '"
              + Starter.WORKFLOW_ID
              + "'. Run Starter first.");
      System.exit(1);
    }

    String encoding =
        memo.getMetadataOrDefault(EncodingKeys.METADATA_ENCODING_KEY, ByteString.EMPTY)
            .toStringUtf8();
    ProgressMemo progress =
        client
            .getOptions()
            .getDataConverter()
            .withContext(new WorkflowSerializationContext(namespace, Starter.WORKFLOW_ID))
            .fromPayload(memo, ProgressMemo.class, ProgressMemo.class);

    System.out.println("memo encoding = " + encoding + " (inline, no object-store fetch)");
    System.out.println("progress      = " + progress.getPercent() + "%");

    System.exit(0);
  }
}
