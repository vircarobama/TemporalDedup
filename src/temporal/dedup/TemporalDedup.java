package temporal.dedup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import temporal.dedup.comparison.ASNM;
import temporal.dedup.records.DataRecord;
import temporal.dedup.records.RecordTypeSequence;
import temporal.dedup.utils.ConfusionMatrix;
import temporal.dedup.utils.LCS;
import temporal.dedup.utils.StringUtils;

/**
 * Implementation of the concepts put forward in the Information Processing & Management article under consideration,
 * "TemporalDedup: Domain-Independent Deduplication of Redundant and Errant Temporal Data". The core concepts are
 * implemented and adherent to the algorithms and descriptions put forward in the article. Future iterations of
 * development will refactor the code for efficiency, add functionality/capability, and resolve bugs and limitations
 * discovered through application to additional datasets.
 * 
 * This code will continue to be further matured with new functionality, bug fixes, and enhanced error
 * protection/checking to achieve the intent of being a domain-independent solution for deduplication of structured,
 * temporal datasets.
 * 
 * Requirements/assumptions in current revision (robustness updates planned for future):
 * 
 * (a) Requires command line specification of a dataset file and a truth data file
 * 
 * (b) Assumes dataset file has a header row and is tab-delimited with no unique record IDs; will internally assign
 * record IDs when parsing
 * 
 * (c) Assumes truth file has a header row, is tab-delimited, and the first value of each row is the record ID of a
 * duplicate
 * 
 * (d) Assumes multiple timestamps (i.e. that the timestamp data is a repeating attribute with the same header name)
 * 
 * (e) For ASNM comparison method, user is to provide the blocking key attribute name as a command line argument when
 * invoking the option to run the comparison method; if not is specified then the entire dataset will be treated as a
 * single block
 * 
 * (f) The default settings for model parameters is LCS max-sampling with an unconstrained order minimum sequence length
 * of 8.
 * 
 * (g) Output files requested by the -a and/or -o flags on the command line will be written to ./output/ folder.
 */
public class TemporalDedup
{
    private DataHandler dataIO;
    private ConfusionMatrix cm;
    private ArrayList<Integer> predictedDupIDs;
    private ArrayList<Integer> unconstrainedOrderDuplicates;
    private ArrayList<DataRecord> records;
    private int numRecordsNotAdheredToLcs = 0;

    /*
     * Default value is sufficiently larger than the number of samples we'd anticipate having available for any record
     * type processed. However, the actual max value to sample (i.e. the variable using this value) should be updated
     * during runtime to equal the the total number of records in the given dataset as soon as it is known.
     */
    private static int MAX_SAMPLING = 25000;

    /*
     * TemporalDedup model parameters
     */
    public static int LCS_SAMPLING__NUMBER_OF_RECORDS = MAX_SAMPLING;
    public static int LCS_SAMPLING__SELECTION_TYPE_TAKE_EVERY_X = 1;
    public static boolean LCS_SAMPLING__SELECTION_TYPE_RANDOM = false;
    public static int UNCONSTRAINED_ORDER__MINIMUM_SEQUENCE_LENGTH = 8;

    /*
     * Option that may be set by command line to execute a known comparison technique codified in the main routine.
     */
    private static boolean RUN_COMPARISON_TECHNIQUE = false;

    /*
     * Option that may be set by command line to open a stdout/stdin query interface with the user at the end of the
     * run. This allows the user to enter two record IDs and be shown their precise differences by field.
     */
    private static boolean RUN_USER_QUERIES = false;

    /*
     * Private option. A value of true overrides the algorithm's decision to disable the elapsed time match case when
     * the timestamp granularity is too high (e.g. date).
     */
    private static boolean FORCE_ELAPSED_TIME_CASE = false;

    /*
     * Option that may be set by command line to generate output files that echo the source data read in. One file
     * should be an exact match; the other will the same thing but annotated with assigned row IDs as the first column.
     */
    private static boolean OUTPUT_SOURCE_DATA_FILES = false;

    /*
     * Option that may be set by command line to generate and analysis output file that includes all of the raw data
     * processed, pre-pended with aggregation fields useful in the analysis of the data in the context of TemporalDedup.
     */
    private static boolean OUTPUT_ANALYSIS_FILE = false;

    public static void main(String[] _args)
    {
        System.out.println("*** BEGIN TemporalDedup APPLICATION ***");

        String datasetFile = "";
        String truthFile = "";
        String blockingKey = "";
        ArrayList<Double> thresholds = new ArrayList<Double>();

        List<String> args = Arrays.asList(_args);

        String arg = "";
        for (int i = 0; i < args.size(); ++i)
        {
            arg = args.get(i);

            if (arg.trim().equalsIgnoreCase("-d"))
            {
                // the following argument should specify the dataset file
                datasetFile = args.get(i + 1).trim();
                ++i;
            }
            else if (arg.trim().equalsIgnoreCase("-t"))
            {
                // the following argument should specify the truth data file
                truthFile = args.get(i + 1).trim();
                ++i;
            }
            else if (arg.trim().equalsIgnoreCase("-lcs_max"))
            {
                LCS_SAMPLING__NUMBER_OF_RECORDS = MAX_SAMPLING;
            }
            else if (arg.trim().equalsIgnoreCase("-lcs_samples"))
            {
                // the following argument should specify the amount of samples
                int numSamples = Integer.parseInt(args.get(i + 1).trim());
                ++i;

                LCS_SAMPLING__NUMBER_OF_RECORDS = numSamples;
            }
            else if (arg.trim().equalsIgnoreCase("-lcs_random"))
            {
                LCS_SAMPLING__SELECTION_TYPE_RANDOM = true;
            }
            else if (arg.trim().equalsIgnoreCase("-lcs_every_x"))
            {
                // the following argument should specify the skip rate (X)
                int x = Integer.parseInt(args.get(i + 1).trim());
                ++i;

                LCS_SAMPLING__SELECTION_TYPE_TAKE_EVERY_X = x;

            }
            else if (arg.trim().equalsIgnoreCase("-min_seq_length"))
            {
                // the following argument should specify the unconstrained order minimum sequence length
                int seqLength = Integer.parseInt(args.get(i + 1).trim());
                ++i;

                UNCONSTRAINED_ORDER__MINIMUM_SEQUENCE_LENGTH = seqLength;
            }
            else if (arg.trim().equalsIgnoreCase("-c"))
            {
                RUN_COMPARISON_TECHNIQUE = true;

                boolean dashEncountered = false;
                for (int j = i + 1; j < args.size() && !dashEncountered; ++j)
                {
                    String a = args.get(j).trim();
                    if (!a.startsWith("-"))
                    {
                        blockingKey += args.get(j).trim() + " ";
                    }
                    else
                    {
                        dashEncountered = true;
                    }
                }

                blockingKey = blockingKey.trim();
            }
            else if (arg.trim().equalsIgnoreCase("-s"))
            {
                boolean dashEncountered = false;
                for (int j = i + 1; j < args.size() && !dashEncountered; ++j)
                {
                    String a = args.get(j).trim();
                    if (!a.startsWith("-"))
                    {
                        try
                        {
                            double value = Double.parseDouble(a);
                            thresholds.add(value);
                        }
                        catch (NumberFormatException e)
                        {
                            System.err.println("Unrecognized similarity threshold: " + a + "; ignoring");
                        }
                    }
                    else
                    {
                        dashEncountered = true;
                    }
                }
            }
            else if (arg.trim().equalsIgnoreCase("-q"))
            {
                RUN_USER_QUERIES = true;
            }
            else if (arg.trim().equalsIgnoreCase("-o"))
            {
                OUTPUT_SOURCE_DATA_FILES = true;
            }
            else if (arg.trim().equalsIgnoreCase("-a"))
            {
                OUTPUT_ANALYSIS_FILE = true;
            }
        }

        if (datasetFile.equals("") || truthFile.equals(""))
        {
            System.out.println("Specification required for both dataset_filename and truth_data_filename");
            usage();
        }

        TemporalDedup td = new TemporalDedup(datasetFile, truthFile);

        if (RUN_COMPARISON_TECHNIQUE)
        {
            ASNM comparison = new ASNM(blockingKey, thresholds);
            comparison.executeComparsion(td.cm, td.dataIO.getHeaders(), td.records);
        }

        String outputFile = "";
        long start = 0;
        long end = 0;

        if (OUTPUT_SOURCE_DATA_FILES)
        {
            try
            {
                start = System.currentTimeMillis();
                outputFile = StringUtils.replaceLast(datasetFile, ".", "_raw_output.");
                td.dataIO.outputRawFieldsToFile(outputFile, false);
                end = System.currentTimeMillis();
                System.out.println("Raw data records written to file: " + outputFile);
                System.out.println("Writing raw data records to file takes " + (end - start) + "ms");

                start = System.currentTimeMillis();
                outputFile = StringUtils.replaceLast(datasetFile, ".", "_raw_with_rowids_output.");
                td.dataIO.outputRawFieldsToFile(outputFile, true);
                end = System.currentTimeMillis();
                System.out.println("Raw data records with row IDs written to file: " + outputFile);
                System.out.println("Writing raw data records with row IDs to file takes " + (end - start) + "ms");
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        if (OUTPUT_ANALYSIS_FILE)
        {
            try
            {
                start = System.currentTimeMillis();
                outputFile = StringUtils.replaceLast(datasetFile, ".", "_analysis_output.");
                outputFile = td.dataIO.outputAnalysisFieldsToFile(outputFile);
                end = System.currentTimeMillis();
                System.out.println("Analysis data records written to file: " + outputFile);
                System.out.println("Writing analysis data records to file takes " + (end - start) + "ms");
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        if (RUN_USER_QUERIES)
        {
            userQueries(td.records);
        }

        System.out.println("*** END TemporalDedup APPLICATION ***");
    }

    private static void usage()
    {
        System.out.println(
                "Usage: -d dataset_filename -t truth_data_filename [-c blocking_key] [-s list_of_thresholds] [-q] [-o] [-a]\n"
                        + "   [-lcs_max] [-lcs_samples number_of_samples] [-lcs_random] [-lcs_every_x skip_rate] [min_seq_length length]");
        System.out.println(
                "  dataset_filename should specify the complete or relative path and the complete filename including extension");
        System.out.println(
                "  truth_data_filename should specify the complete or relative path and the complete filename including extension");
        System.out.println("  -c with a provided blocking_key will invoke execution/results of comparison method(s)");
        System.out.println(
                "  -s provide the list of similarity thresholds (e.g. .981, .927) to be used by the comparison method");
        System.out.println(
                "  -q will invoke a stdout/stdin user interface at the end of the run to enable2000 record value comparisons");
        System.out.println(
                "  -o will output the dataset into the source format (which should match the input file) and a version with row IDs");
        System.out.println(
                "  -a will output an analysis file with each record prefixed by fields that may aid in analysis");

        /*
         * For specification of model parameters
         */
        System.out.println("  -lcs_max Sets the model parameter LCS_SAMPLING__NUMBER_OF_RECORDS to max-sampling");
        System.out.println(
                "  -lcs_samples Sets the model parameter LCS_SAMPLING__NUMBER_OF_RECORDS to the number specified");
        System.out.println("  -lcs_random Sets the model parameter LCS_SAMPLING__SELECTION_TYPE_RANDOM to true");
        System.out.println(
                "  -lcs_every_x Sets the model parameter LCS_SAMPLING__SELECTION_TYPE_TAKE_EVERY_X to the number specified");
        System.out.println(
                "  -min_seq_length Sets the model parameter UNCONSTRAINED_ORDER__MINIMUM_SEQUENCE_LENGTH to the length specified");

        System.exit(0);
    }

    TemporalDedup(String _dataset, String _truth)
    {
        predictedDupIDs = new ArrayList<Integer>();
        unconstrainedOrderDuplicates = new ArrayList<Integer>();

        try
        {
            /*
             * Step 0. Echo out the model parameters
             */
            System.out.println("Model parameters:");
            if (LCS_SAMPLING__NUMBER_OF_RECORDS == MAX_SAMPLING)
            {
                System.out.println("  LCS_SAMPLING__NUMBER_OF_RECORDS: max-sampling");

                /*
                 * By nature of max-sampling, it forces random selection to be false and take every X to be equal to 1
                 */
                LCS_SAMPLING__SELECTION_TYPE_RANDOM = false;
                LCS_SAMPLING__SELECTION_TYPE_TAKE_EVERY_X = 1;
            }
            else
            {
                System.out.println("  LCS_SAMPLING__NUMBER_OF_RECORDS: " + LCS_SAMPLING__NUMBER_OF_RECORDS);
            }
            System.out.println("  LCS_SAMPLING__SELECTION_TYPE_RANDOM: " + LCS_SAMPLING__SELECTION_TYPE_RANDOM);
            System.out.println(
                    "  LCS_SAMPLING__SELECTION_TYPE_TAKE_EVERY_X: " + LCS_SAMPLING__SELECTION_TYPE_TAKE_EVERY_X);
            System.out.println(
                    "  UNCONSTRAINED_ORDER__MINIMUM_SEQUENCE_LENGTH: " + UNCONSTRAINED_ORDER__MINIMUM_SEQUENCE_LENGTH);

            /*
             * Step 1. Parse raw dataset file
             */
            System.out.println("Dataset to process is described in: " + _dataset);
            long start = System.currentTimeMillis();
            dataIO = new DataHandler();
            dataIO.parseFile(_dataset);
            long end = System.currentTimeMillis();
            long algorithmTimeDuringParse = dataIO.getInferenceTime();
            records = dataIO.getRecords();
            if (LCS_SAMPLING__NUMBER_OF_RECORDS == MAX_SAMPLING)
            {
                LCS_SAMPLING__NUMBER_OF_RECORDS = records.size();
            }
            System.out.println("Number of records parsed: " + records.size());
            System.out.println("Parsing raw data file takes " + (end - start) + "ms");

            /*
             * Step 2. Parse truth data file
             */
            start = System.currentTimeMillis();
            cm = new ConfusionMatrix(_truth, records);
            end = System.currentTimeMillis();
            System.out.println("Parsing truth data takes " + (end - start) + "ms");

            /*
             * Step 2.5. Determine time needed to sort the dataset on elapsed time using Collections.sort
             */
            long sortTime = calculateTimeToSortDataset(records);
            System.out.println(
                    "Baseline measure: sorting the dataset on the elapsed time attribute takes " + sortTime + "ms");

            long algorithmRuntimeStart = System.currentTimeMillis();

            /*
             * Step 3. Apply base set of deduplication techniques
             */
            start = System.currentTimeMillis();
            for (int x = 0; x < records.size(); ++x)
            {
                applyBaseTechniques(x);
            }
            end = System.currentTimeMillis();
            System.out.println("Base set of deduplication techniques takes " + (end - start) + "ms");

            /*
             * Step 4. Determine LCS sequence for each record type (temporal grouping value)
             */
            start = System.currentTimeMillis();
            HashMap<String, RecordTypeSequence> map = new HashMap<String, RecordTypeSequence>();
            HashMap<String, RecordTypeSequence> contingency = new HashMap<String, RecordTypeSequence>();
            for (int i = 0; i < records.size(); ++i)
            {
                DataRecord r = records.get(i);
                String id = r.getRecordType();
                String seq = r.getEventSequence();
                RecordTypeSequence gs;

                /*
                 * if all expected timestamps are present and the record isn't a predicted duplicate after the base set
                 * of techniques have been applied, then the record is eligible to be sampled for LCS determination
                 */
                if (r.allTimestamped() && !r.hasKnownDuplicate())
                {
                    if (map.containsKey(id))
                    {
                        gs = map.get(id);
                        gs.addSequence(seq);

                        map.put(id, gs);
                    }
                    else
                    {
                        gs = new RecordTypeSequence(id);
                        gs.addSequence(seq);

                        map.put(id, gs);
                    }
                }
                /*
                 * Otherwise, if the record isn't known to have a duplicate after the base set of techniques and has at
                 * least one timestamp, let's use its information to determine a contingency sequence
                 */
                else if (r.anyTimestamped() && !r.hasKnownDuplicate())
                {
                    if (contingency.containsKey(id))
                    {
                        gs = contingency.get(id);
                        gs.addSequence(seq);

                        contingency.put(id, gs);
                    }
                    else
                    {
                        gs = new RecordTypeSequence(id);
                        gs.addSequence(seq);

                        contingency.put(id, gs);
                    }
                }
            }
            /*
             * Reconciliation. If there is a record type that doesn't have a sequence stored, then we promote up from
             * the contingency list.
             */
            Iterator<String> contingencyIds = contingency.keySet().iterator();
            while (contingencyIds.hasNext())
            {
                String id = contingencyIds.next();
                map.putIfAbsent(id, contingency.get(id));
            }
            end = System.currentTimeMillis();
            System.out.println("Determining LCS sequence applicable to each record type takes " + (end - start) + "ms");

            /*
             * Step 5. Determine LCS adherence for each record
             */
            start = System.currentTimeMillis();
            boolean adherence = true;
            ArrayList<String> recordTypesWithLCSofLengthOne = new ArrayList<String>();
            for (int j = 0; j < records.size(); ++j)
            {
                DataRecord record = records.get(j);
                String recordType = record.getRecordType();
                RecordTypeSequence rts = map.get(recordType);

                LCS toApply = null;

                if (rts == null || rts.getLCS() == null)
                {
                    System.err.println("Did not acquire an LCS for record type: " + recordType);
                }
                else
                {
                    toApply = rts.getLCS();
                }

                if (toApply == null
                        || (!recordTypesWithLCSofLengthOne.contains(recordType) && toApply.getLength() == 1))
                {
                    recordTypesWithLCSofLengthOne.add(recordType);
                    System.out.println("WARN: ignoring LCS of length 1 for record type: " + record.getRecordType());
                }

                adherence = record.applyLCS(toApply);
                if (!adherence)
                {
                    ++numRecordsNotAdheredToLcs;
                }
            }
            end = System.currentTimeMillis();
            System.out.println("Determining LCS adherence for each record takes " + (end - start) + "ms");

            /*
             * Step 6. Apply unconstrained order match duplication check
             */
            start = System.currentTimeMillis();
            for (int x = 0; x < records.size(); ++x)
            {
                applyUnconstrainedOrderCheck(x);
            }
            end = System.currentTimeMillis();
            System.out.println("Unconstrained order match duplication technique takes " + (end - start) + "ms");

            long algorithmRuntimeEnd = System.currentTimeMillis();
            System.out.println("TOTAL RUNTIME for TemporalDedup Algorithm: "
                    + (algorithmRuntimeEnd - algorithmRuntimeStart + algorithmTimeDuringParse) + "ms");

            /*
             * Step 7. Summarize and assess
             */
            System.out.println("Detecting a total of " + predictedDupIDs.size() + " suspected duplicates records among "
                    + records.size());
            cm.assessPrediction(predictedDupIDs);

            System.out.println("--- " + numRecordsNotAdheredToLcs + " records did not adhere to LCS and "
                    + unconstrainedOrderDuplicates.size()
                    + " records were flagged as duplicate for unconstrained order match");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private boolean addPredictedDuplicate(int _id)
    {
        boolean added = false;

        if (!predictedDupIDs.contains(_id))
        {
            predictedDupIDs.add(_id);
            added = true;
        }

        return added;
    }

    /*
     * Applies the base set of techniques for TemporalDedup, as described in Section 4.3.2 of the manuscript
     */
    private void applyBaseTechniques(int _index)
    {
        DataRecord review = records.get(_index);
        DataRecord potential;

        boolean elapsedTimeCheckEligible = FORCE_ELAPSED_TIME_CASE;
        if (review.getElapsedTime() > 0 && review.getTimestampGranularity() != DataRecord.TimestampGranularity.UNKNOWN
                && review.getTimestampGranularity() != DataRecord.TimestampGranularity.DATE)
        {
            elapsedTimeCheckEligible = true;
        }

        for (int i = _index + 1; i < records.size(); ++i)
        {
            potential = records.get(i);

            /*
             * Future enhancement: - make a single call to compare review and potential with an enumerated value return
             * that indicates only key match, only non-key match, no match, or all match
             */
            if (review.exactMatch(potential))
            {
                potential.addMatch(Integer.valueOf(_index), DuplicationClasses.EXACT_MATCH);
                addPredictedDuplicate(Integer.valueOf(_index));

                review.addMatch(Integer.valueOf(i), DuplicationClasses.EXACT_MATCH);
                addPredictedDuplicate(Integer.valueOf(i));
            }
            else if (review.equalsIgnoreKeyFields(potential))
            {
                potential.addMatch(Integer.valueOf(_index), DuplicationClasses.NONKEY_MATCH);
                addPredictedDuplicate(Integer.valueOf(_index));

                review.addMatch(Integer.valueOf(i), DuplicationClasses.NONKEY_MATCH);
                addPredictedDuplicate(Integer.valueOf(i));
            }
            else if (review.sharesSameKeys(potential))
            {
                // if the two records share the same key(s) and aren't an exact match, then the difference is
                // attributable to modified attribute values
                potential.addMatch(Integer.valueOf(_index), DuplicationClasses.MODIFIED_VALUES);
                addPredictedDuplicate(Integer.valueOf(_index));

                review.addMatch(Integer.valueOf(i), DuplicationClasses.MODIFIED_VALUES);
                addPredictedDuplicate(Integer.valueOf(i));
            }
            /*
             * we require that the elapsed time be greater than zero to be considered (must have multiple timestamps for
             * this to be possible, otherwise earliest = latest, so the delta is zero)
             * 
             * we also require the timestamp granularities to be the the same level and for the granularity to be time
             * of day or exact
             */
            else if (elapsedTimeCheckEligible && review.getElapsedTime() == potential.getElapsedTime()
                    && review.getRecordType().equals(potential.getRecordType())
                    && review.getTimestampGranularity() == potential.getTimestampGranularity())
            {
                potential.addMatch(Integer.valueOf(_index), DuplicationClasses.ELAPSED_TIME_MATCH);
                addPredictedDuplicate(Integer.valueOf(_index));

                review.addMatch(Integer.valueOf(i), DuplicationClasses.ELAPSED_TIME_MATCH);
                addPredictedDuplicate(Integer.valueOf(i));
            }
        }
    }

    /*
     * Applies the unconstrained order match check for TemporalDedup, as described in Section 4.3.3 of the manuscript
     */
    private void applyUnconstrainedOrderCheck(int _index)
    {
        DataRecord review = records.get(_index);
        DataRecord potential;

        for (int i = _index + 1; i < records.size(); ++i)
        {
            potential = records.get(i);

            // only go looking if we haven't already found a match between these two records
            if (!review.containsMatch(Integer.valueOf(i)))
            {
                // if the unconstrained orders matches and the record type matches and the unconstrained order is of
                // requisite length
                if (review.getEventSequenceUnconstrained().equals(potential.getEventSequenceUnconstrained())
                        && review.getRecordType().equals(potential.getRecordType())
                        && review.getEventSequenceUnconstrainedLength() >= UNCONSTRAINED_ORDER__MINIMUM_SEQUENCE_LENGTH)
                {
                    potential.addMatch(Integer.valueOf(_index), DuplicationClasses.UNCONSTRAINED_ORDER_MATCH);
                    addPredictedDuplicate(Integer.valueOf(_index));

                    review.addMatch(Integer.valueOf(i), DuplicationClasses.UNCONSTRAINED_ORDER_MATCH);
                    addPredictedDuplicate(Integer.valueOf(i));

                    if (!unconstrainedOrderDuplicates.contains(i))
                    {
                        unconstrainedOrderDuplicates.add(i);
                    }
                    if (!unconstrainedOrderDuplicates.contains(_index))
                    {
                        unconstrainedOrderDuplicates.add(_index);
                    }
                }
            }
        }
    }
    
    private static void userQueries(ArrayList<DataRecord> _records)
    {
        // allow the user to query the differences between two separate IDs
        String input = "";
        String record1 = "";
        String record2 = "";

        System.out.println("************************************************");
        System.out.println(
                "Enter two record IDs separated by a space to query their differences. Type END or QUIT to exit.");
        System.out.println("Valid record IDs are in the range of 0-" + (_records.size() - 1));
        Scanner s = new Scanner(System.in);

        while (!input.equalsIgnoreCase("END") && !input.equalsIgnoreCase("QUIT"))
        {
            input = s.next();

            if (record1.equals(""))
            {
                record1 = input;
            }
            else
            {
                record2 = input;
                DataRecord r1 = _records.get(Integer.parseInt(record1));
                DataRecord r2 = _records.get(Integer.parseInt(record2));
                r1.printDiffs(r2);
                record1 = record2 = "";
            }
        }

        s.close();
    }

    /*
     * Calculates the time to sort the dataset using the Java Collections.sort method, which is O(n log n). The sort is
     * performed on a copy of the dataset and is here only for runtime baseline data.
     */
    private static long calculateTimeToSortDataset(ArrayList<DataRecord> _toSort)
    {
        long start = System.currentTimeMillis();

        ArrayList<DataRecord> copy = new ArrayList<DataRecord>();

        for (int x = 0; x < _toSort.size(); ++x)
        {
            DataRecord y = new DataRecord(_toSort.get(x));
            copy.add(y);
        }

        Collections.sort(copy, new Comparator<DataRecord>()
        {
            @Override
            public int compare(DataRecord _one, DataRecord _two)
            {
                long value = _one.getElapsedTime() - _two.getElapsedTime();

                return (int) value;
            }
        });

        long end = System.currentTimeMillis();

        return end - start;
    }
}
