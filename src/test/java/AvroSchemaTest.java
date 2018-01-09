import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rbc.rbcone.data.rest.kafka.dto.SaraEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public class AvroSchemaTest {
    @Test
    public void testSaraEventSchema() throws Exception {
        Reader reader = new InputStreamReader(getClass().getResourceAsStream("/test-sara-event.json"));
        ObjectMapper objectMapper = new ObjectMapper();
        SaraEvent saraEvent = objectMapper.readValue(reader, SaraEvent.class);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/sara-event.avsc"));
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("region", saraEvent.getRegion());
        genericRecord.put("tableName", saraEvent.getTableName());
        genericRecord.put("id", saraEvent.getId());
        genericRecord.put("created", saraEvent.getCreated().getTime());
        genericRecord.put("action", saraEvent.getAction());
        genericRecord.put("record", saraEvent.getRecord().toString());

        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        writer.write(genericRecord, encoder);
        encoder.flush();
        System.out.printf("The resulted: %s", new String(outputStream.toByteArray(), "UTF-8"));
    }

    @Test
    public void testAvroSchema() throws Exception {
        Reader reader = new InputStreamReader(getClass().getResourceAsStream("/sara-event.avsc"));
        String s = "{\"namespace\":\"fob-sara-event.avro\",\"type\":\"record\",\"name\":\"SaraEvent\",\"fields\":[{\"name\":\"region\",\"type\":\"string\"},{\"name\":\"tableName\",\"type\":\"string\"},{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"created\",\"type\":\"long\"},{\"name\":\"action\",\"type\":\"string\"},{\"name\":\"record\",\"type\":\"string\"}]}";
        JsonObject jsonObject = new JsonParser().parse(reader).getAsJsonObject();
        System.out.println(jsonObject.getAsJsonObject().toString());
    }
}
