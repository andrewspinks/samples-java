package io.temporal.samples.tracing;

import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.reporters.RemoteReporter;
import io.jaegertracing.internal.samplers.ConstSampler;
import io.jaegertracing.spi.Sampler;
import io.jaegertracing.thrift.internal.senders.UdpSender;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.extension.trace.propagation.JaegerPropagator;
import io.opentelemetry.opentracingshim.OpenTracingShim;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.opentracing.Tracer;
import io.temporal.opentracing.OpenTracingOptions;
import io.temporal.opentracing.OpenTracingSpanContextCodec;
import java.util.concurrent.TimeUnit;
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
    // default to Open Telemetry
    return getJaegerOpenTelemetryOptions();
  }

  private static OpenTracingOptions getJaegerOpenTracingOptions() {
    try {
      // Using Udp Sender for OpenTracing, make sure to change host and port
      // to your Jaeger options (if using different than in sample)
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
    Resource serviceNameResource =
        Resource.create(
            Attributes.of(ResourceAttributes.SERVICE_NAME, "temporal-sample-opentelemetry"));

    JaegerGrpcSpanExporter jaegerExporter =
        JaegerGrpcSpanExporter.builder()
            .setEndpoint("http://localhost:14250")
            .setTimeout(1, TimeUnit.SECONDS)
            .build();

    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(jaegerExporter))
            .setResource(Resource.getDefault().merge(serviceNameResource))
            .build();

    OpenTelemetrySdk sdk =
        OpenTelemetrySdk.builder()
            .setPropagators(
                ContextPropagators.create(
                    TextMapPropagator.composite(
                        W3CTraceContextPropagator.getInstance(),
                        W3CBaggagePropagator.getInstance(),
                        JaegerPropagator.getInstance())))
            .setTracerProvider(tracerProvider)
            .build();

    openTelemetry = sdk;
    GlobalOpenTelemetry.set(sdk);

    // create OpenTracing shim and return OpenTracing Tracer from it
    return getOpenTracingOptionsForTracer(OpenTracingShim.createTracerShim(sdk));
  }

  private static OpenTracingOptions getOpenTracingOptionsForTracer(Tracer tracer) {
    OpenTracingOptions options =
        OpenTracingOptions.newBuilder()
            .setSpanContextCodec(OpenTracingSpanContextCodec.TEXT_MAP_CODEC)
            .setTracer(tracer)
            .build();
    return options;
  }
}
