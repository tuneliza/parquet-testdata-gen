/**
 * Created by liza on 7/6/16.
 */

import org.apache.parquet.schema.MessageType;       // schema definition
import org.apache.parquet.schema.MessageTypeParser; // convert string to schema
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Arrays;

public class TestFileGenerator {

    // create variation matrix (map<list<values>>)
    // list sample values for each data type

    /**
     * Create pairs of .parquet and .json files with test data generated from
     * the variation matrix.  The goal is to cover as many test cases as possible
     * by changing one variable at a time.
     *
     */
    public static void main(String args[]){

        // TODO: get directory for file storage from cmd line (?)

        // repeat for every variable
        // TODO: iterate over the matrix


        // create file, open for writing
        // TODO: have a pattern for file naming
        String filename = "TestInt32";
        File outputParquetFile = new File("testcases/"+ filename +".parquet");


        // create desired schema
        // TODO: create schema generically => createSchema(...)
        String rawSchema = "message m {\n" +
                "  optional int32 number;\n" +
                "}";
        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);


        // create data that fits the schema
        // TODO: decide how to do that
        String line = "42";

        // build the file
        // TODO: refactor this out?
        Path path = new Path(outputParquetFile.toURI());
        try {
            CsvParquetWriter writer = new CsvParquetWriter(path, schema, false); // enableDictionary: false
            String[] fields = new String[]{line}; //line.split(Pattern.quote(CSV_DELIMITER));
            writer.write(Arrays.asList(fields));
            writer.close();
        } catch (java.io.IOException e){
            System.out.println("IOException caught: writing parquet file");
        } finally {
            // LOG.info("Number of lines: " + lineNumber);
            // Utils.closeQuietly(br);
        }

    }

//    private static MessageType createSchema(){
//
//    }

}
