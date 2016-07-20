/**
 * Created by liza on 7/6/16.
 */

import org.apache.parquet.schema.MessageType;       // schema definition
import org.apache.parquet.schema.MessageTypeParser; // convert string to schema
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileWriter;
/*import java.nio.file.Files;
import java.nio.file.FileSystems;*/
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import java.io.StringReader;


public class TestFileGenerator {

    static class TestOptions{
        String firstType;
        boolean rotateType;
        int numColumns;
        long numRecords;
        RepetitionPattern repMask;

        // compression;
        // encodings;
        StorageDimensions storage;

        TestOptions(String ft, boolean rt, int nc, long nr, RepetitionPattern rp, StorageDimensions sd){
            firstType = ft;
            rotateType = rt;
            numColumns = nc;
            numRecords = nr;
            repMask = rp;
            storage = sd;
        }

        TestOptions(String ft, boolean rt, int nc, long nr, RepetitionPattern rp){
            this(ft, rt, nc, nr, rp,  null);
        }
    }

    static class StorageDimensions{
        static final int TEST_PAGE_SIZE = 1024; // make it small for faster testing

        int numColumns;
        int numBlocks;
        int numPagesPerBlock;

        StorageDimensions(int columns, int blocks, int pages){
            numColumns = columns;
            numBlocks = blocks;
            numPagesPerBlock = pages;
        }

        // its better to overestimate this
        long estimateBlockSize(){
            return ((long) TEST_PAGE_SIZE) * numColumns * numPagesPerBlock * 4;
        }

        long calcNumRecords(int recordSize){
            return ((long) TEST_PAGE_SIZE) * numBlocks * numColumns * numPagesPerBlock / recordSize;
        }
    }

    /** --------- Lists of parameter values for the test cases ---------- */

    /** Schema parameters */
    // Property definitions: type
    // TODO: add "group" for nested types
    // TODO: map logical types to raw parquet types (int94, date, time, etc.)
    private static final ArrayList<String> rawTypeOptions = new ArrayList<String>(
            Arrays.asList("boolean", "int32", "int64", "float", "double", "binary")
    );

     // Property definitions: repetition
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

    // TODO: look up edge values
    // List of sample values (pool) for each data type. Last element is always a Null-value.
    private static final HashMap<String, String[]> valueMap;
    static{
        valueMap = new HashMap<String, String[]>();
        valueMap.put("boolean", new String[]{"true", "false", ""});
        valueMap.put("int32", new String[]{"0", "42", "-500000000", ""});
        valueMap.put("int64", new String[]{"0", "-42", "900000000","-50000000001", ""});
        valueMap.put("float", new String[]{"0.0", "1.12", "-71234.56", ""});
        valueMap.put("double", new String[]{"0.0", "-1.12", "71234.56", "-5.00000000011", ""});
        valueMap.put("binary", new String[]{"@","cow says \'Mooo\'", "12345", "true", ""});
        // TODO: ^ how to make sure it's a string? -> invoke Types.as(UTF8)?
    }
    private static final int[] repeatedTypeSizes = new int[]{1, 3, 20, 0}; // test a very large size separately


    /** ------------ Generative Routines ----------- */

    /** Setup test parameter sets and generate test case files */
    // TODO: support nested types
    public static void main(String args[]){

        // Class.forName("org.codehaus.jackson.type.JavaType"); // used this to debug maven dependencies

        // test Primitive Types
        ArrayList<TestOptions> options = new ArrayList<TestOptions>();
        // TODO: 5 == maximum number of values in any range - generalize
        for (int i = 0; i < rawTypeOptions.size(); i++) {
            options.add(new TestOptions(rawTypeOptions.get(i), false, 1, 1, RepetitionPattern.ALL_REQUIRED));
            options.add(new TestOptions(rawTypeOptions.get(i), false, 1, 5, RepetitionPattern.ALL_OPTIONAL));
            options.add(new TestOptions(rawTypeOptions.get(i), false, 2, 5, RepetitionPattern.MIX_REQUIRED_OPTIONAL));
        }
        options.add(new TestOptions("int64", true, rawTypeOptions.size(), 1, RepetitionPattern.ALL_OPTIONAL));
        options.add(new TestOptions("float", true, rawTypeOptions.size(), 5, RepetitionPattern.MIX_REQUIRED_OPTIONAL));

        // if directory does not exist, make it
        String tdname = "testcases";
        File td = new File(tdname);
        if(!td.exists()){
            td.mkdir();
        }

        //repeat for every set of variables
        for (int i = 0; i < options.size(); i++) {
            // build file name
            TestFileName tfn = new TestFileName("TestPrimitives", tdname + "/");
            TestOptions paramSet = options.get(i);
            if (paramSet.rotateType){
                tfn.addVariation("multi-type");
            } else {
                tfn.addVariation("single-type");
            }
            tfn.addVariation(paramSet.firstType)
                    .addVariation("r-" + paramSet.numRecords)
                    .addVariation("c-" + paramSet.numColumns)
                    .addVariation(repPatternToString(paramSet.repMask));
            generateTestCase(tfn, paramSet);
        }

        // --------------------------------------
        // test Repeated Types (same test options work, with different test masks
        options = new ArrayList<TestOptions>();
        for (int i = 0; i < rawTypeOptions.size(); i++) {
            options.add(new TestOptions(rawTypeOptions.get(i), false, 1, 1, RepetitionPattern.ALL_REPEATED));
            options.add(new TestOptions(rawTypeOptions.get(i), false, 1, 5, RepetitionPattern.ALL_REPEATED));
            options.add(new TestOptions(rawTypeOptions.get(i), false, 2, 5, RepetitionPattern.MIX_REPEATED_REQUIRED));
        }
        options.add(new TestOptions("int64", true, rawTypeOptions.size(), 1, RepetitionPattern.MIX_OPTIONAL_REPEATED));
        options.add(new TestOptions("float", true, rawTypeOptions.size(), 5, RepetitionPattern.MIX_OPTIONAL_REPEATED));

        //repeat for every set of variables
        for (int i = 0; i < options.size(); i++) {
            // build file name
            TestFileName tfn = new TestFileName("TestRepeated", tdname + "/");
            TestOptions paramSet = options.get(i);
            if (paramSet.rotateType){
                tfn.addVariation("multi-type");
            } else {
                tfn.addVariation("single-type");
            }
            tfn.addVariation(paramSet.firstType)
                    .addVariation("r-" + paramSet.numRecords)
                    .addVariation("c-" + paramSet.numColumns)
                    .addVariation(repPatternToString(paramSet.repMask));
            generateTestCase(tfn, paramSet);
        }

        // --------------------------------------
        // test Block and Page size boundary writes
        /**
         Storage size parameters : data-block-page dimensions
         Goal of this set is to test proper reading block at page/block boundaries;
         (This assumes that all columns are of the same type int32; pageSize is set in StorageDimensions.TEST_PAGE_SIZE)
         */
        /* StorageDimensions[] storageVariants = new StorageDimensions[]{
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

        options = new ArrayList<TestOptions>();
        for (StorageDimensions sv : storageVariants) {
            options.add(new TestOptions("int32", false, sv.numColumns, sv.calcNumRecords(4),
                    RepetitionPattern.MIX_REQUIRED_OPTIONAL, sv));
        }

        //repeat for every set of variables
        for (int i = 0; i < options.size(); i++) {
            // build file name
            TestFileName tfn = new TestFileName("TestBoundary", tdname + "/");
            TestOptions paramSet = options.get(i);
            tfn.addVariation(paramSet.firstType)
                    .addVariation("r-" + paramSet.numRecords)
                    .addVariation("c-" + paramSet.numColumns)
                    .addVariation(repPatternToString(paramSet.repMask))
                    .addVariation("bs-" + paramSet.storage.calcBlockSize())
                    .addVariation("ps-" + StorageDimensions.TEST_PAGE_SIZE);
            generateTestCase(tfn, paramSet);
        }
*/
        // ------------------------------------
        // test a big file, default page/block sizes
        TestOptions set = new TestOptions("float", true, rawTypeOptions.size(), 128*1024,
                RepetitionPattern.MIX_OPTIONAL_REPEATED);

        // build file name
        String tdname = "testcases";  // duplicate, defined above (commented)
        TestFileName tfn = new TestFileName("TestBigFile", tdname + "/");
        tfn.addVariation(set.firstType)
                .addVariation("r-" + set.numRecords)
                .addVariation("c-" + set.numColumns)
                .addVariation(repPatternToString(set.repMask));
        generateTestCase(tfn, set);

        // page borders, no string/binary
        StorageDimensions sd = new StorageDimensions(5, 1, 4);
        TestOptions set = new TestOptions("float", true, sd.numColumns, sd.calcNumRecords(8),
                RepetitionPattern.MIX_OPTIONAL_REPEATED, sd);

        // build file name
        String tdname = "testcases";  // duplicate, defined above (commented)
        TestFileName tfn = new TestFileName("TestPageBorder", tdname + "/");
        tfn.addVariation(set.firstType)
                .addVariation("r-" + set.numRecords)
                .addVariation("c-" + set.numColumns)
                .addVariation(repPatternToString(set.repMask));
        generateTestCase(tfn, set);

    }

    /**
     * Create triplets of .parquet, .schema and .json files corresponding to a set of test parameter options
     */

    public static void generateTestCase(TestFileName tfn, TestOptions options){

        // make files, open for writing
        File outParquetFile = new File(tfn.getNameParquet());
        deleteFileIfExists(outParquetFile);

        File outSchemaFile = new File(tfn.getNameSchema());
        deleteFileIfExists(outSchemaFile);

        File outJsonFile = new File(tfn.getNameJSON());
        deleteFileIfExists(outJsonFile);

        // create schema, along with corresponding property list
        ArrayList<VarProperties> propList = makePropertyList(options.numColumns, options.firstType,
                options.rotateType, options.repMask);
        String rawSchema = emitFlatSchemaString(propList);
        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);

        // file i/o
        try {
            // write schema
            FileWriter schemaWriter = new FileWriter(outSchemaFile);
            schemaWriter.write(rawSchema);
            schemaWriter.close();

            // generate and write data
            Path path = new Path(outParquetFile.toURI());

            CsvParquetWriter pWriter;
            if (options.storage == null) {
                pWriter = new CsvParquetWriter(path, schema, false); // enableDictionary: false - plain encoding
            } else {
                pWriter = new CsvParquetWriter(path, schema, false,
                        (int) options.storage.estimateBlockSize(), StorageDimensions.TEST_PAGE_SIZE);
            }

            //JsonWriter jsonWriter = Json.createWriter(new FileWriter(outJsonFile));
            FileWriter jsonWriter =  new FileWriter(outJsonFile);

            for (int j = 0; j < options.numRecords; j++) {

                // create a record that fits the schema
                String[] record = new String[propList.size()];
                for (int i = 0; i < propList.size(); i++) {
                    record[i] = propList.get(i).getNextValue();
                }

                // write data to parquet file
                pWriter.write(Arrays.asList(record));

                // build a JSON, write it to file
                JsonObject jo = convertRecordToJSON(record, propList);
                jsonWriter.write(jo.toString() + "\n");
                //jsonWriter.writeObject(jo);
            }

            jsonWriter.close();
            pWriter.close();
        } catch (java.io.IOException e){
            System.err.println("error: " + e.getMessage());
        }

    }



    /** ---------- Helpers: support for test case generation ----------- */

    // Tuple of properties for a variable in schema
    static class VarProperties{
        String repetition;
        String type;

        private int idx;        // position of next value in the valueMap[type]
        private int repSizeIdx;

        VarProperties(String repetition, String type){
            this.repetition = repetition;
            this.type = type;
            idx = 0;
            repSizeIdx = 0;
        }

        private String getNextPrimitive(){
            String value = valueMap.get(type)[idx];
            if(repetition == "optional"){
                idx = (idx + 1) % valueMap.get(type).length;
            } else {
                idx = (idx + 1) % (valueMap.get(type).length - 1); // skip over the null-value
            }
            return value;
        }

        String getNextValue(){
            if (repetition == "repeated") {
                String values = "";
                if (repeatedTypeSizes[repSizeIdx] > 0) {
                    values += getNextPrimitive();
                    for (int i = 1; i < repeatedTypeSizes[repSizeIdx]; i++) {
                        values += "|" + getNextPrimitive();
                    }
                }
                repSizeIdx = (repSizeIdx + 1) % repeatedTypeSizes.length;
                return values;
            }

            return getNextPrimitive();
        }
    }

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

    public static final String VAR_NAME_PREFIX = "var_";

    private static String emitFlatSchemaString(ArrayList<VarProperties> propertyList){
        String rawSchema = "message m {\n";
        for (int count = 0; count < propertyList.size(); count++) {
            rawSchema += "  " + propertyList.get(count).repetition +
                    " " + propertyList.get(count).type +
                    " " + VAR_NAME_PREFIX + count + ";\n";
        }
        rawSchema += "}";

        return rawSchema;
    }

    /** Generate descriptive filenames */
    static class TestFileName{
        private String name;
        private String path;

        TestFileName(String testGroup, String pathToFile){
            name = testGroup;
            path = pathToFile;
        }

        TestFileName addVariation(String variationStr){
            name += "_" + variationStr;
            return this;
        }

        String getNameParquet(){
            return path+ name +".parquet";
        }

        String getNameJSON(){
            return path+ name +".json";
        }

        String getNameSchema(){
            return path+ name +".schema";
        }
    }

    private static String repPatternToString(RepetitionPattern rp){
        switch(rp){
            case ALL_OPTIONAL: return "mask-optional";
            case ALL_REPEATED: return "mask-repeated";
            case ALL_REQUIRED: return "mask-required";
            case MIX_OPTIONAL_REPEATED: return "mask-optional-repeated";
            case MIX_REPEATED_REQUIRED: return "mask-repeated-required";
            case MIX_REQUIRED_OPTIONAL: return "mask-required-optional";
        }
        return "mask-unknown";
    }

    private static void deleteFileIfExists(File f){
        if (f.exists() && !f.isDirectory()){
            try{
                f.delete();
            } catch (Exception e){
                System.err.println("error: " + e.getMessage());
            }
        }
    }

    // TODO: use builder or parse a string representation ??
    private static JsonObject convertRecordToJSON(String[] record, ArrayList<VarProperties> propList){
        // build a string representation of JSON
        String joString = "{";
            for(int i=0; i < propList.size(); i++){
                joString += "\"" + VAR_NAME_PREFIX + i + "\": ";
                if(propList.get(i).repetition != "repeated"){
                    joString += formatValueForJson(record[i], propList.get(i).type , (i < propList.size() - 1));
                } else {
                    if (record[i].length() == 0) {
                        joString += "[]";
                    } else {
                        String[] items = record[i].split("\\|");
                        joString += "[";

                        for (int j = 0; j < items.length; j++) {
                            joString += formatValueForJson(items[j], propList.get(i).type, (j < items.length - 1));
                        }
                        joString += "]";
                    }
                    if (i < propList.size() - 1) {
                        joString += ", ";
                    }
                }
            }
        joString += "}";

        // read it into an object
        JsonReader jsonReader = Json.createReader(new StringReader(joString));
        JsonObject jo = jsonReader.readObject();
        jsonReader.close();

        return jo;
    }

    private static String formatValueForJson(String val, String type, boolean notLast){
        if (type == "binary") {
            val = "\"" + val + "\"";
        } else if (val == "") {
            val = "null";
        }
        if (notLast) {
            val += ", ";
        }
        return val;
    }
}
