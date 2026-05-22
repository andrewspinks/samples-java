package io.temporal.samples.tracing;

import io.opentelemetry.api.baggage.Baggage;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.opentracing.OpenTracingClientInterceptor;
import io.temporal.samples.tracing.workflow.TracingWorkflow;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.io.IOException;

public class Starter {
  public static final String TASK_QUEUE_NAME = "tracingTaskQueue";

  public static void main(String[] args) {
    String type = "OpenTelemetry";
    if (args.length == 1) {
      type = args[0];
    }

    // Load configuration from environment and files
    ClientConfigProfile profile;
    try {
      profile = ClientConfigProfile.load();
    } catch (IOException e) {
      throw new RuntimeException("Failed to load client configuration", e);
    }

    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(profile.toWorkflowServiceStubsOptions());

    // Set the OpenTracing client interceptor, preserving env config
    WorkflowClientOptions clientOptions =
        profile.toWorkflowClientOptions().toBuilder()
            .setInterceptors(new OpenTracingClientInterceptor(JaegerUtils.getJaegerOptions(type)))
            .build();
    WorkflowClient client = WorkflowClient.newInstance(service, clientOptions);

    WorkflowOptions workflowOptions =
        WorkflowOptions.newBuilder()
            .setWorkflowId("tracingWorkflow")
            .setTaskQueue(TASK_QUEUE_NAME)
            .build();

    // Create typed workflow stub
    TracingWorkflow workflow = client.newWorkflowStub(TracingWorkflow.class, workflowOptions);

    // Set OTel Baggage before starting the workflow. The OpenTracingClientInterceptor creates
    // its root span within this scope, so the baggage is active when the span is built.
    // The shim bridges OTel Baggage to OpenTracing baggage items, which are then serialized
    // into Temporal headers by the Jaeger codec and deserialized on the worker side.
    // See README.md for notes on the shim and when this approach has limitations.
    try (io.opentelemetry.context.Scope baggageScope =
        Baggage.builder()
            .put("user-id", "user-123")
            .put("correlation-id", "req-abc")
            .build()
            .makeCurrent()) {
      WorkflowStub untyped = WorkflowStub.fromTyped(workflow);
      untyped.signalWithStart("setLanguage", new Object[] {"Spanish"}, new Object[] {"John"});

      String greeting = untyped.getResult(String.class);
      System.out.println("Greeting: " + greeting);
    }

    System.exit(0);
  }
}
