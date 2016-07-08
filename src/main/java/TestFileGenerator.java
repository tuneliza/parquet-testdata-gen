/**
 * Created by liza on 7/6/16.
 */

import org.apache.parquet.schema.MessageType;       // schema definition
import org.apache.parquet.schema.MessageTypeParser; // convert string to schema
import org.apache.hadoop.fs.Path;

import java.io.File;
/*import java.nio.file.Files;
import java.nio.file.FileSystems;*/
import java.util.Arrays;

public class TestFileGenerator {

    // TODO:
    // create variation matrix (map<list<values>>)
    // list sample values (pool) for each data type

    /**
     * Create pairs of .parquet and .json files with test data generated from
     * the variation matrix.  The goal is to cover as many test cases as possible
     * by changing one variable at a time.
     *
     */

    public static void main(String args[]) throws Exception{

        // Class.forName("org.codehaus.jackson.type.JavaType"); // used this for debugging

        // TODO: get directory for file storage from cmd line (?)

        // repeat for every variable
        // TODO: iterate over the matrix


        // create file, open for writing
        // TODO: have a pattern for file naming
        String filename = "TestInt32";
        File outputParquetFile = new File("testcases/"+ filename +".parquet");

        // Ian's file cleanup
/*
        if (args.length > 1) {
            filename = args[1];
        }
        if (!filename.endsWith(".parquet")) {
            filename += ".parquet";
        }
        java.nio.file.Path filePath =
            FileSystems.getDefault().getPath("testcases", filename);
        Files.deleteIfExists(filePath);
        File outputParquetFile = new File(filePath.toString());
*/

        // create desired schema
        // TODO: create schema generically => createSchema(...)
        String rawSchema = "message m {\n" +
                "  optional int32 number;\n" +
                "}";
        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);


        // create data that fits the schema
        // TODO: decide how to do that (map probably)
        String line = "42";

        // build the file
        // TODO: refactor this out?
        Path path = new Path(outputParquetFile.toURI());
        try {
            CsvParquetWriter writer = new CsvParquetWriter(path, schema, false); // enableDictionary: false
            String[] fields = new String[1]; //line.split(Pattern.quote(CSV_DELIMITER));
            fields[0] = line;
            writer.write(Arrays.asList(fields));
            writer.close();
        } catch (java.io.IOException e){
            System.err.println("error: " + e.getMessage());
        } finally {
            // LOG.info("Number of lines: " + lineNumber);
            // Utils.closeQuietly(br);
        }

    }

//    private static MessageType createSchema(){
//
//    }

}
