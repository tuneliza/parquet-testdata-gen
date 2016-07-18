
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class CsvWriteSupport extends WriteSupport<List<String>> {
  MessageType schema;
  RecordConsumer recordConsumer;
  List<ColumnDescriptor> cols;

  // TODO: support specifying encodings and compression
  public CsvWriteSupport(MessageType schema) {
    this.schema = schema;
    this.cols = schema.getColumns();
  }

  @Override
  public WriteContext init(Configuration config) {
    return new WriteContext(schema, new HashMap<String, String>());
  }

  @Override
  public void prepareForWrite(RecordConsumer r) {
    recordConsumer = r;
  }

  /** See org.apache.parquet.io.api.RecordConsumer for proper sequence of
      methods to write a record correctly
  */

  @Override
  public void write(List<String> values) {
    if (values.size() != cols.size()) {
      throw new ParquetEncodingException("Invalid input data. Expecting " +
          cols.size() + " columns. Input had " + values.size() + " columns (" + cols + ") : " + values);
    }

    recordConsumer.startMessage();
    for (int i = 0; i < cols.size(); ++i) {
      String val = values.get(i);
      // val.length() == 0 indicates a NULL value.
      if (val.length() > 0) {
        recordConsumer.startField(cols.get(i).getPath()[0], i);

        // assumption: schema fields' and columns' indices match
        // (idk why they wouldn't, but haven't found a way to verify that)
        if (schema.getType(i).getRepetition() != Type.Repetition.REPEATED) {
          addPrimitiveValue(val, cols.get(i));
        } else {
          // parse each item in a list and add it
          String[] items = val.split("\\|");
          for (String item: items){
            addPrimitiveValue(item, cols.get(i));
          }
        }

        recordConsumer.endField(cols.get(i).getPath()[0], i);
      }
    }
    recordConsumer.endMessage();
  }

  private Binary stringToBinary(Object value) {
    return Binary.fromString(value.toString());
  }

  private void addPrimitiveValue(String val, ColumnDescriptor cd){
    switch (cd.getType()) {
      case BOOLEAN:
        recordConsumer.addBoolean(Boolean.parseBoolean(val));
        break;
      case FLOAT:
        recordConsumer.addFloat(Float.parseFloat(val));
        break;
      case DOUBLE:
        recordConsumer.addDouble(Double.parseDouble(val));
        break;
      case INT32:
        recordConsumer.addInteger(Integer.parseInt(val));
        break;
      case INT64:
        recordConsumer.addLong(Long.parseLong(val));
        break;
      case BINARY:
        recordConsumer.addBinary(stringToBinary(val));
        break;
      default:
        throw new ParquetEncodingException(
                "Unsupported column type: " + cd.getType());
    }
  }
}
