package io.temporal.samples.claimcheckmemo;

import com.google.protobuf.ByteString;
import io.temporal.api.common.v1.Payload;
import io.temporal.common.converter.DataConverterException;
import io.temporal.common.converter.EncodingKeys;
import io.temporal.common.converter.JacksonJsonPayloadConverter;
import io.temporal.common.converter.PayloadConverter;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Custom {@link PayloadConverter} that handles {@link ProgressMemo} values only and stamps the
 * {@link ClaimCheckCodec#EXEMPT_KEY} marker on the produced {@link Payload}.
 *
 * <p>Important wiring detail: register this <b>before</b> {@link JacksonJsonPayloadConverter} in
 * the converter list. {@code DefaultDataConverter} tries converters in order and Jackson would
 * otherwise claim {@code ProgressMemo} first. The encoding type is unique so that decode routes a
 * marked payload back here.
 */
class InlineMemoPayloadConverter implements PayloadConverter {

  static final String ENCODING_TYPE = "json/plain/inline-memo";

  // Delegate the actual JSON serialization; only metadata differs.
  private final JacksonJsonPayloadConverter delegate = new JacksonJsonPayloadConverter();

  @Override
  public String getEncodingType() {
    return ENCODING_TYPE;
  }

  @Override
  public Optional<Payload> toData(Object value) throws DataConverterException {
    if (!(value instanceof ProgressMemo)) {
      // Not ours — let later converters (Jackson) handle it (so it gets offloaded).
      return Optional.empty();
    }
    Payload json =
        delegate
            .toData(value)
            .orElseThrow(() -> new DataConverterException("ProgressMemo JSON conversion failed"));
    return Optional.of(
        json.toBuilder()
            // Override encoding so decode routes back to this converter.
            .putMetadata(
                EncodingKeys.METADATA_ENCODING_KEY,
                ByteString.copyFrom(ENCODING_TYPE, StandardCharsets.UTF_8))
            // The marker the claim-check codec checks to keep this payload inline.
            .putMetadata(ClaimCheckCodec.EXEMPT_KEY, ByteString.copyFromUtf8("true"))
            .build());
  }

  @Override
  public <T> T fromData(Payload content, Class<T> valueType, Type valueGenericType)
      throws DataConverterException {
    // Restore the standard JSON encoding so the Jackson delegate accepts it.
    Payload asJson =
        content.toBuilder()
            .putMetadata(
                EncodingKeys.METADATA_ENCODING_KEY,
                ByteString.copyFrom(delegate.getEncodingType(), StandardCharsets.UTF_8))
            .removeMetadata(ClaimCheckCodec.EXEMPT_KEY)
            .build();
    return delegate.fromData(asJson, valueType, valueGenericType);
  }
}
