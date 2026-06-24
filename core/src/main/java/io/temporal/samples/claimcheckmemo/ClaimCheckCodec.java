package io.temporal.samples.claimcheckmemo;

import com.google.protobuf.ByteString;
import io.temporal.api.common.v1.Payload;
import io.temporal.common.converter.DataConverterException;
import io.temporal.common.converter.EncodingKeys;
import io.temporal.payload.codec.PayloadCodec;
import io.temporal.payload.codec.PayloadCodecException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

/**
 * Claim-check {@link PayloadCodec}: offloads every payload to {@link ClaimCheckObjectStore} and
 * leaves only a small reference inline in Temporal. Wired on {@code
 * WorkflowClientOptions.setDataConverter(...)} it applies to workflow inputs, results, and memos
 * alike.
 *
 * <p>A {@link PayloadCodec} receives a flat {@code List<Payload>} and is given no signal of whether
 * a payload is a memo, an input, or a result — the optional {@link
 * io.temporal.payload.context.SerializationContext} only identifies the workflow/activity, not the
 * payload <em>kind</em>. So the codec cannot skip memos based on "kind" alone.
 *
 * <p>Instead it skips any payload that carries the {@link #EXEMPT_KEY} metadata marker. That marker
 * is stamped by {@link InlineMemoPayloadConverter} on {@link ProgressMemo} values, which is how
 * progress memos stay inline while everything else is offloaded. Because the marker travels on the
 * payload (produced by the converter, which always runs before the codec), this works identically
 * for client-side {@code WorkflowOptions.setMemo(...)} and workflow-side {@code
 * Workflow.upsertMemo(...)}.
 */
class ClaimCheckCodec implements PayloadCodec {

  /** Metadata key whose presence tells this codec to leave a payload inline. */
  static final String EXEMPT_KEY = "claim-check-exempt";

  static final String METADATA_ENCODING_CLAIM_CHECK = "binary/claim-check";
  static final ByteString CLAIM_CHECK_ENCODING =
      ByteString.copyFrom(METADATA_ENCODING_CLAIM_CHECK, StandardCharsets.UTF_8);

  @NotNull
  @Override
  public List<Payload> encode(@NotNull List<Payload> payloads) {
    return payloads.stream().map(this::offload).collect(Collectors.toList());
  }

  @NotNull
  @Override
  public List<Payload> decode(@NotNull List<Payload> payloads) {
    return payloads.stream().map(this::fetch).collect(Collectors.toList());
  }

  private Payload offload(Payload payload) {
    // Exempt payloads (e.g. progress memos) are kept inline.
    if (payload.containsMetadata(EXEMPT_KEY)) {
      return payload;
    }
    try {
      String key = ClaimCheckObjectStore.put(payload.toByteArray());
      return Payload.newBuilder()
          .putMetadata(EncodingKeys.METADATA_ENCODING_KEY, CLAIM_CHECK_ENCODING)
          .setData(ByteString.copyFromUtf8(key))
          .build();
    } catch (Throwable e) {
      throw new DataConverterException(e);
    }
  }

  private Payload fetch(Payload payload) {
    if (!CLAIM_CHECK_ENCODING.equals(
        payload.getMetadataOrDefault(EncodingKeys.METADATA_ENCODING_KEY, null))) {
      // Inline payload (never offloaded) — pass through, no object-store round trip.
      return payload;
    }
    try {
      String key = payload.getData().toStringUtf8();
      byte[] data = ClaimCheckObjectStore.get(key);
      return Payload.parseFrom(data);
    } catch (Throwable e) {
      throw new PayloadCodecException(e);
    }
  }
}
