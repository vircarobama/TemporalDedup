package temporal.dedup.comparison;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

import org.apache.commons.text.similarity.LevenshteinDistance;

import info.debatty.java.lsh.MinHash;
import temporal.dedup.records.DataRecord;
import temporal.dedup.utils.ConfusionMatrix;

/**
 * Implementation of the Adaptive Sorted Neighborhood Method (ASNM) consistent with the concepts established in:
 * 
 * Yan, S., Lee, D., Kan, M. Y., & Giles, L. C. (2007). Adaptive sorted neighborhood methods for efficient record
 * linkage: 7th ACM/IEEE Joint Conference on Digital Libraries, JCDL 2007: Building and Sustaining the Digital
 * Environment. Proceedings of the 7th ACM/IEEE Joint Conference on Digital Libraries, JCDL 2007, 185–194.
 * https://doi.org/10.1145/1255175.1255213
 */
public class ASNM implements ComparisonMethod_I
{
    /*
     * Maximum distance allowed between blocking keys to be considered in the same block
     */
    private final static double BLOCK_DISTANCE_THRESHOLD = .05;

    /*
     * For each enlargement step, what factor larger should the next window be compared to the current one
     */
    private final static int WINDOW_GROWTH_FACTOR = 2;

    /*
     * Used to measure distance between blocking keys when determining the comparison blocks. This will not compare
     * records in detail; rather it is an approximate comparison on blocking fields.
     */
    private LevenshteinDistance ld;

    /*
     * Records to evaluate for duplication
     */
    private ArrayList<DataRecord> records;

    /*
     * Start indices for each of the unique comparison blocks we determine. These blocks will be non-overlapping and of
     * potentially variable size.
     */
    private ArrayList<Integer> blockStartIndices;

    /*
     * The series of thresholds to be applied for similarity checks between records. This is used to make a
     * determination on duplication. This is not to be confused with the similarity distance threshold established to
     * choose the block boundaries.
     */
    private ArrayList<Double> recordSimilarityThresholds;

    /*
     * The header value associated with the field to sort on and determine comparison blocks off of
     */
    private String blockingKey;

    /*
     * Index within the records to be analyzed corresponding to the provided blocking key
     */
    private int blockingKeyIndex;
    
    /*
     * Without a blocking key, the entire dataset will need to be treated as one big block
     */
    private boolean hasBlockingKey;

    /**
     * Constructor.  Requires that a blocking key be provided.  The blocking key is used to adaptively determine the
     * non-overlapping variable sized comparison blocks.  If the blocking key is an empty string, then the entire
     * dataset is treated as a single comparison block.
     * 
     * @param _blockingKey Name of the attribute to be used as the blocking key
     */
    public ASNM(String _blockingKey, ArrayList<Double> _thresholds)
    {
        recordSimilarityThresholds = new ArrayList<Double>();
        setSimilarityThresholds(_thresholds);

        hasBlockingKey = false;
        provideBlockingKey(_blockingKey);

        // default blocking key index until we can determine it upon provision of a record set
        blockingKeyIndex = 0;

        // default the record set
        records = new ArrayList<DataRecord>();

        // default the start indices of our comparison blocks
        blockStartIndices = new ArrayList<Integer>();

        ld = new LevenshteinDistance();
    }

    /*
     * Specifies the similarity thresholds that we want to run ASNM against a given dataset for.  If the given
     * list is blank, use a programmatically determined default list.
     */
    private void setSimilarityThresholds(ArrayList<Double> _thresholds)
    {
        if(_thresholds.size() == 0)
        {
            System.out.println("No ASNM similarity thresholds specified by user; applying defaults (.8, .9, .95, .927, .963, .981, 1.0)");
            
            recordSimilarityThresholds.add(0.8);
            recordSimilarityThresholds.add(0.9);
            recordSimilarityThresholds.add(0.95);
            recordSimilarityThresholds.add(0.927);
            recordSimilarityThresholds.add(0.963);
            recordSimilarityThresholds.add(0.981);
            recordSimilarityThresholds.add(1.0);
        }
        else
        {
            for(int i=0; i<_thresholds.size(); ++i)
            {
                recordSimilarityThresholds.add(_thresholds.get(i));
            }
        }
    }

    /**
     * Interface implementation.
     * 
     * Allow for blocking key to be externally provided. If blocking keys are not appropriate or needed for a particular
     * comparison method implementation, then the implementation may be a no-op.
     * 
     * @param _blockingKey String representing the blocking key as an attribute name (i.e. header name, column name)
     */
    public void provideBlockingKey(String _blockingKey)
    {
        blockingKey = new String(_blockingKey);
        
        if(blockingKey.trim().length() > 0)
        {
        	hasBlockingKey = true;
        }
    }

    /**
     * Interface implementation.
     * 
     * Execute the comparison algorithm against the given set of data records and call the provided confusion matrix to
     * assess the prediction (_cm.assessPrediction).
     * 
     * @param _cm      The confusion matrix object that may assess the prediction of this method against truth data
     * @param _headers The set of header names corresponding to each of a data record's raw attributes
     * @param _records The set of data records to perform deduplication evaluation on
     */
    public void executeComparsion(ConfusionMatrix _cm, ArrayList<String> _headers, ArrayList<DataRecord> _records)
    {
        System.out.println("Executing ASNM comparison method ...");
        // measure the up-front sort and blocking time for ASNM so we may add it to each of the threshold runs
        long start = System.currentTimeMillis();

        // sort the records
        storeAndSortRecords(_headers, _records);
        
        // determine our comparison blocks (non-overlapping blocks of potentially various sizes)
        determineComparisonBlocks();
        
        long end = System.currentTimeMillis();
        long sortAndBlockingTime = end-start;

        // for each similarity threshold, compare each record within its own block for all blocks
        for (int t = 0; t < recordSimilarityThresholds.size(); ++t)
        {
            double threshold = recordSimilarityThresholds.get(t);

            start = System.currentTimeMillis();

            int similarityCount = 0;
            ArrayList<Integer> predictedDupIDs = new ArrayList<Integer>();

            int numBlocks = blockStartIndices.size();

            for (int a = 0; a < numBlocks; ++a)
            {
                int startIndex = blockStartIndices.get(a);
                int endIndex = records.size() - 1;
                if ((a + 1) != numBlocks)
                {
                    endIndex = blockStartIndices.get(a + 1) - 1;
                }

                for (int b = startIndex; b <= endIndex; ++b)
                {
                    DataRecord rec = records.get(b);
                    ArrayList<Integer> sims = getSimilarRecords(b, startIndex, endIndex, threshold);

                    if (sims.size() > 0)
                    {
                        ++similarityCount;

                        if (!predictedDupIDs.contains(rec.getId()))
                        {
                            predictedDupIDs.add(rec.getId());
                        }
                    }
                }
            }

            end = System.currentTimeMillis();
            System.out.println("ASNM Comparison technique (threshold = " + threshold + ") takes " + (end - start + sortAndBlockingTime) + "ms");

            System.out.println("Detecting a total of " + similarityCount
                    + " suspected duplicate records based on similarity among " + _records.size());

            _cm.assessPrediction(predictedDupIDs);
        }
    }

    /*
     * Provided with the list of header names and the list of data records, make an object-level copy of the records,
     * find the blocking key index from the headers, and sort the records based on their blocking key
     */
    private void storeAndSortRecords(ArrayList<String> _headers, ArrayList<DataRecord> _records)
    {
        // make an object level copy of the records
        for (int i = 0; i < _records.size(); ++i)
        {
            DataRecord r = new DataRecord(_records.get(i));
            records.add(r);
        }

        // determine the blocking key index
        boolean blockingKeyIndexFound = false;
        for (int i = 0; i < _headers.size() && !blockingKeyIndexFound; ++i)
        {
            String h = _headers.get(i);
            if (h.equalsIgnoreCase(blockingKey))
            {
                blockingKeyIndex = i;
                blockingKeyIndexFound = true;
            }
        }

        // sort the records on the blocking key
        Collections.sort(records, new Comparator<DataRecord>()
        {
            public int compare(DataRecord _one, DataRecord _two)
            {
                String keyOne = _one.getAttributeValueAt(blockingKeyIndex);
                String keyTwo = _two.getAttributeValueAt(blockingKeyIndex);

                return keyOne.compareTo(keyTwo);
            }
        });
    }

    /*
     * Implements Algorithm 2 (AA-SNM) from Yan, S., Lee, D., Kan, M. Y., & Giles, L. C. (2007). Adaptive sorted
     * neighborhood methods for efficient record linkage: 7th ACM/IEEE Joint Conference on Digital Libraries, JCDL 2007:
     * Building and Sustaining the Digital Environment. Proceedings of the 7th ACM/IEEE Joint Conference on Digital
     * Libraries, JCDL 2007, 185–194. https://doi.org/10.1145/1255175.1255213
     * 
     * Use the Accumatively-Adaptive SNM method to identify the boundaries in the record set that establish our
     * comparison blocks.
     * 
     * Precondition: records have been sorted on the blockingKey Postcondition: blockStartIndices is populated with the
     * start index for each of the comparison blocks
     */
    private void determineComparisonBlocks()
    {
        int numRecords = records.size();

        int windowSize = 2;
        int windowFirst = 0;
        int windowLast = windowFirst + windowSize - 1;
        
        if(!hasBlockingKey)
        {
        	// the entire dataset is one block
        	blockStartIndices.add(0);
        	
        	// ensure the while loop below does not occur
        	windowLast = numRecords+1;
        }

        while (windowLast <= numRecords)
        {
            // save the starting index of the current block
            blockStartIndices.add(windowFirst);

            // enlargement
            while (blockDistance(windowFirst, windowLast) <= BLOCK_DISTANCE_THRESHOLD)
            {
                windowSize *= WINDOW_GROWTH_FACTOR;
                windowFirst = windowLast;
                windowLast = windowFirst + windowSize - 1;
            }

            // retrenchment
            while (windowSize > 2)
            {
                if (blockDistance(windowFirst, windowLast) > BLOCK_DISTANCE_THRESHOLD)
                {
                    // reduce window by half each time
                    windowSize /= 2;
                    
                    // shrink the tail
                    windowLast = windowFirst + windowSize - 1;
                }
                else
                {
                    // shrink the window and move right
                    windowFirst = windowLast;
                    windowLast = windowFirst + windowSize - 1;
                }
            }
            
            // now that we are at a window size of 2, inch to the right until we straddle a boundary
            while(blockDistance(windowFirst, windowLast) <= BLOCK_DISTANCE_THRESHOLD)
            {
                windowFirst += 1;
                windowLast += 1;
            }

            /*
             * we found a boundary pair when we reached a window of size 2 and the distance between the two records is
             * larger than the threshold
             */
            if (blockDistance(windowFirst, windowLast) > BLOCK_DISTANCE_THRESHOLD)
            {
                // shrink from a size 2 window to a size 1
                windowLast = windowFirst;
            }

            // this is where we could save the ending index of the current block if we needed it;
            // start indices are good enough for our implementation

            // reposition the window
            windowSize = 2;
            windowFirst = windowLast + 1;
            windowLast = windowFirst + windowSize - 1;
        }

        // blockStartIndices should now be fully populated
    }

    /*
     * Calculate the block distance between two given records. The distance is one minus the similarity between two
     * records' blocking key values.
     */
    private double blockDistance(int _indexOne, int _indexTwo)
    {
        double distance = 1.0;

        try
        {
            DataRecord one = records.get(_indexOne);
            DataRecord two = records.get(_indexTwo);
            String keyOne = one.getAttributeValueAt(blockingKeyIndex);
            String keyTwo = two.getAttributeValueAt(blockingKeyIndex);

            distance = (ld.apply(keyOne, keyTwo)) / (double) (Math.max(keyOne.length(), keyTwo.length()));
        }
        catch (Exception e)
        {
            // if one or both doesn't exist, we return full distance
            distance = 1.0;
        }

        return distance;
    }

    /*
     * Iteratively compares the record specified by the _reviewIndex to every other record in the comparison
     * block with inclusive boundaries _bloackStartIndex and _blockEndIndex.  Returns an array of record IDs
     * that had a similarity index higher than _threshold.
     */
    private ArrayList<Integer> getSimilarRecords(int _reviewIndex, int _blockStartIndex, int _blockEndIndex,
            double _threshold)
    {
        ArrayList<Integer> sims = new ArrayList<Integer>();

        DataRecord review = records.get(_reviewIndex);
        Set<Integer> r = review.getRecordAsIntegerSet();

        DataRecord potential;
        Set<Integer> p;
        double similarity;

        // don't need to compare against self
        for (int i = _blockStartIndex; i <= _blockEndIndex; ++i)
        {
            if (i != _reviewIndex)
            {
                potential = records.get(i);

                p = potential.getRecordAsIntegerSet();

                // use open source implementation of Jaccard
                similarity = MinHash.jaccardIndex(r, p);

                if (similarity >= _threshold)
                {
                    sims.add(i);
                }
            }
        }

        return sims;
    }
}
