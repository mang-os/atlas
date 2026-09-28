package com.atlas.configuration;

import java.util.UUID;
import com.atlas.messaging.ReservationEventDecoder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReservationEventDecoderTest {
    private final JsonMapper json;
    private final ReservationEventDecoder decoder;
    private final String id = UUID.randomUUID().toString();
    ReservationEventDecoderTest() {
        var builder = JsonMapper.builder();
        new JsonConfiguration().strictJson().customize(builder);
        json = builder.build();
        decoder = new ReservationEventDecoder(json);
    }
    private ObjectNode valid() {
        return (ObjectNode) json.readTree("""
                {"eventId":"%s","schemaVersion":1,"eventType":"ReservationHeld","aggregateType":"RESERVATION",
                "aggregateId":"%s","aggregateSequence":1,"occurredAt":"2026-09-28T00:00:00Z","correlationId":"%s",
                "data":{"resourceId":"%s","userId":"%s","quantity":1,"status":"HELD","expiresAt":"2026-09-28T00:05:00Z","releasedQuantity":0}}
                """.formatted(id,id,id,id,id));
    }
    @ParameterizedTest
    @ValueSource(strings = {"eventId","schemaVersion","eventType","aggregateType","aggregateId","aggregateSequence","occurredAt","correlationId","data"})
    void rejectsEveryMissingEnvelopeField(String field) {
        var value = valid();
        value.remove(field);
        assertThatThrownBy(() -> decoder.decode(id,value.toString())).isInstanceOf(RuntimeException.class);
    }
    @ParameterizedTest
    @ValueSource(strings = {"resourceId","userId","quantity","status","expiresAt","releasedQuantity"})
    void rejectsEveryMissingOrNullDataField(String field) {
        var value = valid();
        ((ObjectNode)value.get("data")).remove(field);
        assertThatThrownBy(() -> decoder.decode(id,value.toString())).isInstanceOf(RuntimeException.class);
        ((ObjectNode)value.get("data")).putNull(field);
        assertThatThrownBy(() -> decoder.decode(id,value.toString())).isInstanceOf(RuntimeException.class);
    }
    @ParameterizedTest
    @ValueSource(strings = {"wrongKey","unknownVersion","wrongType","wrongSequence","zeroQuantity","wrongRelease","deadline","unknownField","duplicateField","trailingJson"})
    void rejectsUnsupportedOrInconsistentInput(String kind) {
        var value = valid();
        var data = (ObjectNode)value.get("data");
        switch (kind) {
            case "unknownVersion" -> value.put("schemaVersion",2);
            case "wrongType" -> value.put("eventType","CapacityReleased");
            case "wrongSequence" -> value.put("aggregateSequence",2);
            case "zeroQuantity" -> data.put("quantity",0);
            case "wrongRelease" -> data.put("releasedQuantity",1);
            case "deadline" -> value.put("occurredAt","2026-09-28T00:05:00Z");
            case "unknownField" -> value.put("unexpected",true);
            default -> { }
        }
        String payload = switch (kind) {
            case "duplicateField" -> value.toString().replace("\"schemaVersion\":1", "\"schemaVersion\":1,\"schemaVersion\":1");
            case "trailingJson" -> value.toString() + " {}";
            default -> value.toString();
        };
        assertThatThrownBy(() -> decoder.decode(kind.equals("wrongKey") ? UUID.randomUUID().toString() : id,payload)).isInstanceOf(RuntimeException.class);
    }
    @ParameterizedTest
    @ValueSource(strings = {"HELD","CONFIRMED","CANCELLED","EXPIRED"})
    void acceptsAllSupportedTransitions(String state) {
        var value = valid();
        var data = (ObjectNode)value.get("data");
        data.put("status",state);
        value.put("eventType",switch(state) { case "HELD" -> "ReservationHeld"; case "CONFIRMED" -> "ReservationConfirmed"; case "CANCELLED" -> "ReservationCancelled"; default -> "ReservationExpired"; });
        value.put("aggregateSequence",state.equals("HELD") ? 1 : 2);
        data.put("releasedQuantity",state.equals("CANCELLED") || state.equals("EXPIRED") ? 1 : 0);
        if (state.equals("EXPIRED")) value.put("occurredAt","2026-09-28T00:05:00Z");
        assertThat(decoder.decode(id,value.toString()).data().status().name()).isEqualTo(state);
    }
}
