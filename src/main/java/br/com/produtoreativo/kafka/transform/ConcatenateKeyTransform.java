package br.com.produtoreativo.kafka.transform;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

public class ConcatenateKeyTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private String[] keyFieldNames;
    private String delimiter;

    @Override
    public R apply(R record) {
        Struct keyStruct = (Struct) record.key();
        StringBuilder newIdBuilder = new StringBuilder();
        if (keyFieldNames.length > 0 && !keyFieldNames[0].isEmpty()) {
          for (String fieldName : keyFieldNames) {
              if (newIdBuilder.length() > 0) {
                  newIdBuilder.append(delimiter);
              }
              Object fieldValue = keyStruct.get(fieldName);
              newIdBuilder.append(fieldValue.toString());
          }
        } else {
          Schema schema = record.keySchema();
          for (Field field : schema.fields()) {
            if (newIdBuilder.length() > 0) {
                newIdBuilder.append(delimiter);
            }
            Object value = keyStruct.get(field.name());
            newIdBuilder.append(value.toString());
          }
        }

        String newId = newIdBuilder.toString();

        Schema schemaElastic = SchemaBuilder.struct()
                                     .name("br.com.produtoreativo")
                                     .field("_id", Schema.STRING_SCHEMA)
                                     .build();
         Struct keyElastic = new Struct(schemaElastic).put("_id", newId);

        R transformedRecord = record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            schemaElastic,
            keyElastic,
            record.valueSchema(),
            record.value(),
            record.timestamp()
        );

        transformedRecord.headers().addString("_id", newId);
        return transformedRecord;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define("key.fields", ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "List of key field names to concatenate")
            .define("key.delimiter", ConfigDef.Type.STRING, "-", ConfigDef.Importance.HIGH, "Delimiter used to concatenate key fields");
    }

    @Override
    public void close() {
        // Não é necessário fechar recursos
    }

    @Override
    public void configure(Map<String, ?> configs) {
        String keyFieldsConfig = (String) configs.get("key.fields");

        keyFieldNames = keyFieldsConfig.split(",");
        Object delimiterConfig = configs.get("key.delimiter");

        if (delimiterConfig == null || !(delimiterConfig instanceof String)) {
            throw new ConfigException("Missing or invalid 'key.delimiter' configuration.");
        }

        delimiter = (String) delimiterConfig;
    }
}
