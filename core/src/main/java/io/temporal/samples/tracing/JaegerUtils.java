package io.temporal.samples.tracing;

import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.reporters.RemoteReporter;
import io.jaegertracing.internal.samplers.ConstSampler;
import io.jaegertracing.spi.Sampler;
import io.jaegertracing.thrift.internal.senders.UdpSender;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.opentracingshim.OpenTracingShim;
import io.opentracing.Tracer;
import io.temporal.opentracing.OpenTracingOptions;
import io.temporal.opentracing.OpenTracingSpanContextCodec;
import org.apache.thrift.transport.TTransportException;

public class JaegerUtils {

  private static OpenTelemetry openTelemetry;

  /** Returns the OpenTelemetry instance configured by {@link #getJaegerOptions}. */
  public static OpenTelemetry getOpenTelemetry() {
    return openTelemetry;
  }

  public static OpenTracingOptions getJaegerOptions(String type) {
    if (type.equals("OpenTracing")) {
      return getJaegerOpenTracingOptions();
    }
    // default: OTel Java agent path
    return getJaegerOpenTelemetryOptions();
  }

  private static OpenTracingOptions getJaegerOpenTracingOptions() {
    try {
      RemoteReporter reporter =
          new RemoteReporter.Builder().withSender(new UdpSender("localhost", 5775, 0)).build();
      Sampler sampler = new ConstSampler(true);
      Tracer tracer =
          new JaegerTracer.Builder("temporal-sample-opentracing")
              .withReporter(reporter)
              .withSampler(sampler)
              .build();

      return getOpenTracingOptionsForTracer(tracer);
    } catch (TTransportException e) {
      System.out.println("Exception configuring Jaeger Tracer: " + e.getMessage());
      return null;
    }
  }

  private static OpenTracingOptions getJaegerOpenTelemetryOptions() {
    // The OTel Java agent initializes GlobalOpenTelemetry at JVM startup via -javaagent:.
    // Do NOT call GlobalOpenTelemetry.set() here — the agent already did it and calling
    // it again will throw an exception.
    // The agent also configures propagators (W3C tracecontext + W3C baggage) and the
    // OTLP exporter automatically via system properties.
    openTelemetry = GlobalOpenTelemetry.get();
    return getOpenTracingOptionsForTracer(OpenTracingShim.createTracerShim(openTelemetry));
  }

  private static OpenTracingOptions getOpenTracingOptionsForTracer(Tracer tracer) {
    return OpenTracingOptions.newBuilder()
        .setSpanContextCodec(OpenTracingSpanContextCodec.TEXT_MAP_CODEC)
        .setTracer(tracer)
        .build();
  }
}
