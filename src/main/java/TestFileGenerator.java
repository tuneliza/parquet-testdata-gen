/**
 * Created by liza on 7/6/16.
 */

import org.apache.parquet.it.unimi.dsi.fastutil.Hash;
import org.apache.parquet.schema.MessageType;       // schema definition
import org.apache.parquet.schema.MessageTypeParser; // convert string to schema
import org.apache.hadoop.fs.Path;

import java.io.File;
/*import java.nio.file.Files;
import java.nio.file.FileSystems;*/
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;

public class TestFileGenerator {
    /** Parameters for automatic schema generation */

    // Tuple of properties for a variable in schema
    static class VarProperties{
        String repetition;
        String type;
        VarProperties(String repetition, String type){
            this.repetition = repetition;
            this.type = type;
        }
    }

    // Property definitons: repetition
    // TODO: handle "repeated"
    private enum RepetitionPattern {
        ALL_REQUIRED, ALL_OPTIONAL, ALL_REPEATED, MIX_REQUIRED_OPTIONAL, MIX_REPEATED_REQUIRED, MIX_OPTIONAL_REPEATED
    }
    private static final HashMap<RepetitionPattern, String[]> repetitionMasks;
    static{
        repetitionMasks = new HashMap<RepetitionPattern, String[]>();
        repetitionMasks.put(RepetitionPattern.ALL_REQUIRED, new String[]{"required"});
        repetitionMasks.put(RepetitionPattern.ALL_OPTIONAL, new String[]{"optional"});
        repetitionMasks.put(RepetitionPattern.MIX_REQUIRED_OPTIONAL, new String[]{"required", "optional"});
    }

    // Property definitions: type
    // TODO: add "binary" (int94, string, etc.), "group" for nested types
    // TODO: map logical types to raw parquet types
    private static final ArrayList<String> rawTypeOptions = new ArrayList<String>(
            Arrays.asList("boolean", "int32", "int64", "float", "double")
    );

    // list sample values (pool) for each data type
    // private static final ... <String, String[]> valueMap;


    private static ArrayList<VarProperties> makePropertyList(int size, String firstType, boolean rotateTypes, RepetitionPattern rp){
        ArrayList<VarProperties> propertyList = new ArrayList<VarProperties>(size);
        int ti = rawTypeOptions.indexOf(firstType); // index for types
        int rmi = 0; // index for RepetitionMasks

        for (int i = 0; i < size; i++) {
            propertyList.add(new VarProperties(repetitionMasks.get(rp)[rmi], rawTypeOptions.get(ti)));

            // advance indexes
            if (rotateTypes) {
                ti = (ti + 1) % rawTypeOptions.size();
            }
            rmi = (rmi + 1) % repetitionMasks.get(rp).length;
        }
        return propertyList;
    }

    // construct a string representation of a flat schema
    private static String emitFlatSchemaString(ArrayList<VarProperties> propertyList){
        String rawSchema = "message m {\n";
        for (int count = 0; count < propertyList.size(); count++) {
            rawSchema += "  " + propertyList.get(count).repetition +
                    " " + propertyList.get(count).type +
                    " var_" + count+";\n";
        }
        rawSchema += "}";

        return rawSchema;
    }

    /** Parameters to generate data */



    /**
     * Create pairs of .parquet and .json files with test data generated from
     * the variation matrix.  The goal is to cover as many test cases as possible
     * by changing one variable at a time.
     *
     */

    // TODO: support nested types
    public static void main(String args[]) throws Exception{

        // Class.forName("org.codehaus.jackson.type.JavaType"); // used this to debug maven dependencies

        // repeat for every variable

        // number of columns/variables
        // everything of same type, or rotate


        // create file, open for writing
        // TODO: have a pattern for file naming
        String filename = "TestInt32";
        File outputParquetFile = new File("testcases/"+ filename +".parquet");

        // Ian's file cleanup
/*
        java.nio.file.Path filePath =
            FileSystems.getDefault().getPath("testcases", filename);
        Files.deleteIfExists(filePath);
        File outputParquetFile = new File(filePath.toString());
*/
        // create schema
        ArrayList<VarProperties> proplist = makePropertyList(2, "boolean", true, RepetitionPattern.MIX_REQUIRED_OPTIONAL);
        String rawSchema = emitFlatSchemaString(proplist);
        System.out.println(rawSchema);  // for debug
        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);

        // create data that fits the schema
        // TODO: decide how to do that (map probably)
        String[] line = new String[]{"true", "42"};

        // build the file
        // TODO: refactor this out?
        Path path = new Path(outputParquetFile.toURI());
        try {
            CsvParquetWriter writer = new CsvParquetWriter(path, schema, false); // enableDictionary: false
            writer.write(Arrays.asList(line));
            writer.close();
        } catch (java.io.IOException e){
            System.err.println("error: " + e.getMessage());
        } finally {
            // LOG.info("Number of lines: " + lineNumber);
            // Utils.closeQuietly(br);
        }

    }

}
