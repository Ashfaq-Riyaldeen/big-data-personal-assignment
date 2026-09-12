package com.ashfaq.bigdata.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the contract defined by order.avsc.
 *
 * <p>Order.java is generated at build time, so these tests fail if the schema is edited in a way
 * that breaks the shape the assignment brief requires, or if the code generator is misconfigured.
 */
class OrderSchemaTest {

    @Test
    @DisplayName("an Order survives a binary Avro round trip unchanged")
    void roundTripsThroughAvroBinary() throws IOException {
        Order original = Order.newBuilder()
                .setOrderId("1001")
                .setProduct("Item1")
                .setPrice(100.0f)
                .build();

        byte[] encoded = serialise(original);
        Order decoded = deserialise(encoded);

        assertThat(decoded.getOrderId()).isEqualTo("1001");
        assertThat(decoded.getProduct()).isEqualTo("Item1");
        assertThat(decoded.getPrice()).isEqualTo(100.0f);
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    @DisplayName("string fields are generated as java.lang.String, not CharSequence")
    void generatesRealStrings() {
        Order order = Order.newBuilder()
                .setOrderId("1002")
                .setProduct("Item2")
                .setPrice(300.0f)
                .build();

        // This is what stringType=String buys: the consumer can compare product names with
        // equals() and switch on them, instead of juggling CharSequence.
        assertThat((Object) order.getOrderId()).isInstanceOf(String.class);
        assertThat((Object) order.getProduct()).isInstanceOf(String.class);
    }

    @Test
    @DisplayName("the schema carries exactly the three fields the brief defines")
    void matchesTheBrief() {
        Schema schema = Order.getClassSchema();

        assertThat(schema.getFullName()).isEqualTo("com.ashfaq.bigdata.avro.Order");
        assertThat(schema.getFields()).hasSize(3);

        assertThat(schema.getField("orderId").schema().getType()).isEqualTo(Schema.Type.STRING);
        assertThat(schema.getField("product").schema().getType()).isEqualTo(Schema.Type.STRING);
        // The brief specifies float. A decimal type would be better for real money, but the
        // schema is fixed by the assignment.
        assertThat(schema.getField("price").schema().getType()).isEqualTo(Schema.Type.FLOAT);
    }

    private static byte[] serialise(Order order) throws IOException {
        SpecificDatumWriter<Order> writer = new SpecificDatumWriter<>(Order.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(order, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static Order deserialise(byte[] bytes) throws IOException {
        SpecificDatumReader<Order> reader = new SpecificDatumReader<>(Order.class);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes), null);
        return reader.read(null, decoder);
    }
}
