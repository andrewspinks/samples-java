package io.temporal.samples.claimcheckmemo;

import io.temporal.common.converter.ByteArrayPayloadConverter;
import io.temporal.common.converter.CodecDataConverter;
import io.temporal.common.converter.DataConverter;
import io.temporal.common.converter.DefaultDataConverter;
import io.temporal.common.converter.JacksonJsonPayloadConverter;
import io.temporal.common.converter.NullPayloadConverter;
import io.temporal.common.converter.ProtobufJsonPayloadConverter;
import io.temporal.common.converter.ProtobufPayloadConverter;
import java.util.Collections;

/**
 * Builds the {@link DataConverter} shared by the workflow starter and the progress-query client so
 * both encode/decode payloads the same way.
 *
 * <p>{@link InlineMemoPayloadConverter} is placed <b>before</b> {@link JacksonJsonPayloadConverter}
 * so it claims {@link ProgressMemo} and stamps the exempt marker the codec checks. Everything else
 * falls through to Jackson and is offloaded by {@link ClaimCheckCodec}.
 */
final class ClaimCheckDataConverter {

  static DataConverter newInstance() {
    return new CodecDataConverter(
        //            DefaultDataConverter.newDefaultInstance(),
        new DefaultDataConverter(
            new NullPayloadConverter(),
            new ByteArrayPayloadConverter(),
            new ProtobufJsonPayloadConverter(),
            new ProtobufPayloadConverter(),
            new InlineMemoPayloadConverter(), // Add PayloadConverter to add metadata to Memos
            new JacksonJsonPayloadConverter()),
        Collections.singletonList(new ClaimCheckCodec()));
  }

  private ClaimCheckDataConverter() {}
}
