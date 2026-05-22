package io.temporal.samples.tracing.workflow;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

public class TracingActivitiesImpl implements TracingActivities {

  private final Tracer tracer;

  public TracingActivitiesImpl(OpenTelemetry openTelemetry) {
    this.tracer = openTelemetry.getTracer("tracing-sample-activities");
  }

  @Override
  public String greet(String name, String language) {
    // Read baggage propagated from the calling context via Temporal headers.
    // The shim bridges OTel Baggage → OpenTracing baggage items → Jaeger codec → Temporal headers.
    // See README.md for notes on when this approach has limits.
    String userId = Baggage.current().getEntryValue("user-id");
    String correlationId = Baggage.current().getEntryValue("correlation-id");

    // Custom child span — safe here because activities are never replayed.
    // Do NOT create spans directly in workflow code: span IDs are random and
    // timestamps use wall-clock time, both of which break determinism on replay.
    Span span =
        tracer
            .spanBuilder("build-greeting")
            .setAttribute("language", language)
            .setAttribute("user.id", userId != null ? userId : "unknown")
            .setAttribute("correlation.id", correlationId != null ? correlationId : "unknown")
            .startSpan();

    try (Scope scope = span.makeCurrent()) {
      String greeting = buildGreeting(name, language);
      span.setAttribute("greeting", greeting);
      return greeting;
    } finally {
      span.end();
    }
  }

  private String buildGreeting(String name, String language) {
    switch (language) {
      case "Spanish":
        return "Hola " + name;
      case "French":
        return "Bonjour " + name;
      default:
        return "Hello " + name;
    }
  }
}
