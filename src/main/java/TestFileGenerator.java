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
import java.util.ArrayList;
import java.util.HashMap;

public class TestFileGenerator {
    /** --------- Lists of parameters for the test cases ---------- */

    /** Schema parameters */

    private static final int[] numberOfColumns = new int[]{1, 2, 6, 7, 50}; // TODO: include 0??

     // Property definitons: repetition
    private enum RepetitionPattern {
        ALL_REQUIRED, ALL_OPTIONAL, ALL_REPEATED, MIX_REQUIRED_OPTIONAL, MIX_REPEATED_REQUIRED, MIX_OPTIONAL_REPEATED
    }
    private static final HashMap<RepetitionPattern, String[]> repetitionMasks;
    static{
        repetitionMasks = new HashMap<RepetitionPattern, String[]>();
        repetitionMasks.put(RepetitionPattern.ALL_REQUIRED, new String[]{"required"});
        repetitionMasks.put(RepetitionPattern.ALL_OPTIONAL, new String[]{"optional"});
        repetitionMasks.put(RepetitionPattern.ALL_REPEATED, new String[]{"repeated"});
        repetitionMasks.put(RepetitionPattern.MIX_REQUIRED_OPTIONAL, new String[]{"required", "optional"});
        repetitionMasks.put(RepetitionPattern.MIX_OPTIONAL_REPEATED, new String[]{"optional", "repeated"});
        repetitionMasks.put(RepetitionPattern.MIX_REPEATED_REQUIRED, new String[]{"repeated", "required"});
    }

    /** Data parameters */

    // list of sample values (pool) for each data type. Null value is always listed last.
    private static final HashMap<String, String[]> valueMap;
    static{
        valueMap = new HashMap<String, String[]>();
        valueMap.put("boolean", new String[]{"true", "false", ""});
        valueMap.put("int32", new String[]{"0", "42", "-500000000", ""});
        valueMap.put("int64", new String[]{"0", "-42", "900000000","-50000000001", ""});
        valueMap.put("float", new String[]{"0.0", "1.12", "-71234.56", ""});
        valueMap.put("double", new String[]{"0.0", "-1.12", "71234.56", "-5.00000000011", ""});
        valueMap.put("binary", new String[]{"z","cow says \'Mooo\'", "12345", "true", ""});
        // TODO: ^ how to make it string? -> invoke Types.as(UTF8)?
    }
    private static final int[] repeatedTypeSizes = new int[]{1, 3, 20, 100, 0};

    /**
     Storage size parameters : data-block-page dimensions
     Goal of this set is to test proper reading block at page/block boundaries;
     (This assumes that all columns are of the same type int32; pageSize is set in StorageDimensions.TEST_PAGE_SIZE)
     */
    private static final StorageDimensions[] sizeVariants =
            new StorageDimensions[]{
                    new StorageDimensions(1, 1, 1),
                    new StorageDimensions(1, 2, 2),
                    new StorageDimensions(1, 8, 16),
                    new StorageDimensions(4, 1, 2),
                    new StorageDimensions(4, 2, 16),
                    new StorageDimensions(4, 8, 1),
                    new StorageDimensions(50, 1, 16),
                    new StorageDimensions(50, 2, 1),
                    new StorageDimensions(50, 8, 2)
            };

    /** ---------- Support data structures for test case generation ----------- */

    // Tuple of properties for a variable in schema
    static class VarProperties{
        String repetition;
        String type;
        int idx;        // position of next value in the valueMap[type]

        VarProperties(String repetition, String type){
            this.repetition = repetition;
            this.type = type;
            idx = 0;
        }

        String getNextPrimitive(){
            String value = valueMap.get(type)[idx];
            if(repetition == "optional"){
                idx = (idx + 1) % valueMap.get(type).length;
            } else {
                idx = (idx + 1) % (valueMap.get(type).length - 1); // skip over the null-value
            }
            return value;
        }

        // pre: this.repetition = "repeated"
        String[] getNextRepeated(int repSize){
            String[] values = new String[repSize];
            for (int i = 0; i < repSize; i++){
                values[i] = getNextPrimitive();
            }
            return values;
        }
    }

    // Property definitions: type
    // TODO: add "group" for nested types
    // TODO: map logical types to raw parquet types (int94, string, etc.)
    private static final ArrayList<String> rawTypeOptions = new ArrayList<String>(
            Arrays.asList("boolean", "int32", "int64", "float", "double", "binary")
    );

    // build a list of variable properties for a flat schema
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

    /** Storage sie parameters */
    static class StorageDimensions{
        static final int TEST_PAGE_SIZE = 128; // make it small for faster testing
        int numColumns;
        int numBlocks;
        int numPagesPerBlock;
        StorageDimensions(int columns, int blocks, int pages){
            numColumns = columns;
            numBlocks = blocks;
            numPagesPerBlock = pages;
        }

        long calcBlockSize(){
            return ((long) TEST_PAGE_SIZE) * numColumns * numPagesPerBlock;
        }

        long calcNumRecords(int recordSize){
            return ((long) TEST_PAGE_SIZE) * numBlocks * numColumns * numPagesPerBlock / recordSize;
        }
    }

    /** Generate descriptive filenames */
    class TestFileName{
        String name;

        TestFileName(String testGroup){
            name = testGroup;
        }

        TestFileName addVariation(String variationStr){
            name += "_" + variationStr;
            return this;
        }
    }


    /** ------------ Generative Routines ----------- */

    /**
     * Create pairs of .parquet and .json files with test data generated from
     * the variation matrix.  The goal is to cover as many test cases as possible
     * by changing one variable at a time.
     *
     */

    // TODO: support nested types
    public static void main(String args[]){

        // Class.forName("org.codehaus.jackson.type.JavaType"); // used this to debug maven dependencies

        // TODO: repeat for every set of variables

        // create file, open for writing
        // TODO: have a pattern for file naming
        String filename = "TestInt32";
        File outputParquetFile = new File("testcases/"+ filename +".parquet");

        // file cleanup
        if (outputParquetFile.exists() && !outputParquetFile.isDirectory()){
            try{
                outputParquetFile.delete();
            } catch (Exception e){
                System.err.println("error: " + e.getMessage());
            }
        }

        // create schema
        ArrayList<VarProperties> proplist = makePropertyList(2, "int32", false, RepetitionPattern.ALL_OPTIONAL);
        String rawSchema = emitFlatSchemaString(proplist);
        System.out.println(rawSchema);  // for debug
        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);

        // file i/o
        Path path = new Path(outputParquetFile.toURI());
        try {
            CsvParquetWriter writer = new CsvParquetWriter(path, schema, false); // enableDictionary: false

            // TODO: create data that fits the schema
            String[] line = new String[]{"12345", "42"};

            // write data to file
            writer.write(Arrays.asList(line));

            writer.close();
        } catch (java.io.IOException e){
            System.err.println("error: " + e.getMessage());
        }

    }

}
