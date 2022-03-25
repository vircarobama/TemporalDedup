package temporal.dedup;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;

import temporal.dedup.records.DataRecord;
import temporal.dedup.records.KeyAttribute;
import temporal.dedup.records.LogicalAttribute;
import temporal.dedup.utils.StringUtils;

/**
 * Primary source for file input/output and maintenance of raw dataset knowledge (records and headers). Performs
 * up-front data inferences, to include logical attribute inferences, key inferences, and record type (temporal grouping
 * value) inferences, as described in Section 4.3.1 of the manuscript.
 */
public class DataHandler
{
    private int recordNum;
    private ArrayList<DataRecord> records;
    private String rawHeaders;
    private ArrayList<String> headers;
    private boolean readHeader;

    private KeyAttribute key;
    private ArrayList<Integer> logicalAttributeStartIndices;
    private int logicalAttributeLength;

    private long inferenceTime;

    public DataHandler()
    {
        recordNum = 0;
        records = new ArrayList<DataRecord>();
        rawHeaders = "";
        headers = new ArrayList<String>();
        readHeader = false;

        key = new KeyAttribute();
        logicalAttributeStartIndices = new ArrayList<Integer>();
        logicalAttributeLength = 0;

        inferenceTime = 0;
    }

    public ArrayList<DataRecord> getRecords()
    {
        return records;
    }

    public long getInferenceTime()
    {
        return inferenceTime;
    }

    public ArrayList<String> getHeaders()
    {
        return headers;
    }

    /**
     * Limitation: assumes _file is tab-delimited
     * 
     * @param _file
     */
    public void parseFile(String _file)
    {
        try
        {
            BufferedReader buf = new BufferedReader(new FileReader(_file));
            String lineJustFetched = null;

            while (true)
            {
                lineJustFetched = buf.readLine();
                if (lineJustFetched == null)
                {
                    break;
                }
                else if (!readHeader)
                {
                    rawHeaders = new String(lineJustFetched);

                    String[] columns = lineJustFetched.split("\t");

                    for (int i = 0; i < columns.length; ++i)
                    {
                        headers.add(columns[i]);
                    }

                    // now that we know the header values, we may infer logical attributes
                    logicalAttributeInference();

                    readHeader = true;
                }
                else
                {
                    DataRecord record = parseRecord(lineJustFetched);

                    records.add(record);
                }
            }

            buf.close();

            // determine the key attribute(s)
            key = keyInference();

            // determine the attribute that defines the record type (temporal grouping value) and apply it to all
            // records
            int typeAttribute = recordTypeInference();

            for (int i = 0; i < records.size(); ++i)
            {
                records.get(i).applyKey(key);

                if (key.getLength() == 1)
                {
                    records.get(i).applyGlobalRecordType();
                }
                else
                {
                    records.get(i).applyRecordType(typeAttribute);
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /*
     * As noted, a given record may be inclusive of repeating sets of consecutive attributes – identical in length and
     * form. We will infer these to be logical attributes (or objects). If these logical attributes include temporal
     * data, we will associate that data with the logical attribute when evaluating temporal relationships between the
     * collection of logical attributes. For a given record, the time values associated with each of these logical
     * records may imply requisite order dependencies.
     * 
     * Side effects: sets values for logicalAttributeLength and logicalAttributeStartIndices Returns: whether or not the
     * last attribute in the record is part of a logical attribute
     */
    private boolean logicalAttributeInference()
    {
        long start = System.currentTimeMillis();

        ArrayList<String> set = StringUtils.longestRepeatedTemporalStringSet(headers);
        String indicator = set.get(0);
        logicalAttributeLength = set.size();
        boolean recordEndsWithLogicalAttribute = false;

        // iterate through all attributes and determine which ones are part of logical attributes
        for (int i = 0; i < headers.size(); ++i)
        {
            String s = headers.get(i);
            boolean falseIndicator = false;

            if (s.equals(indicator) && !falseIndicator)
            {
                // check to see if the following attributes all match up with set

                for (int j = 1; j < logicalAttributeLength; ++j)
                {
                    String c = headers.get(i + j);
                    if (!c.equals(set.get(j)))
                    {
                        falseIndicator = true;
                    }
                }

                // if they do, add i to the logicalAttributeStartIndices and advance i
                // to the last attribute in our list
                if (!falseIndicator)
                {
                    recordEndsWithLogicalAttribute = true;
                    logicalAttributeStartIndices.add(i);
                    i += logicalAttributeLength - 1;
                }
                else
                {
                    recordEndsWithLogicalAttribute = false;
                }
            }
            else
            {
                // this attribute is not part of a logical attribute
                recordEndsWithLogicalAttribute = false;
            }
        }

        long end = System.currentTimeMillis();
        inferenceTime += (end - start);

        if (logicalAttributeLength > 0)
        {
            String logicalAttr = "";
            int x = logicalAttributeStartIndices.get(0);

            for (int i = 0; i < logicalAttributeLength; ++i)
            {
                logicalAttr += headers.get(x + i);

                if (i != (logicalAttributeLength - 1))
                {
                    logicalAttr += ", ";
                }
            }

            System.out
                    .println("Logical attribute consists of " + logicalAttributeLength + " attributes: " + logicalAttr);
        }

        return recordEndsWithLogicalAttribute;
    }

    // @formatter:off
    /*
     * DEDUPLICATION-PK-INFERENCE 
     * Input: array of eligible attributes (A), array of records (R) 
     * Output: candidate key (CK) 
     * r = R.length 
     * max = 0
     * for all attribute indices i in A do 
     *    m = number of unique rows in R filtered on current attribute 
     *    if (m == r) return A[i] //as primary key
     *    else if (m > max)
     *       max = m
     *       CK is A[i]
     * max = 0 
     * for all attribute indices i in A do 
     *    for all attribute indices j in A do 
     *       if (i != j) 
     *          m = number of unique rows in R filtered on columns i, j 
     *          if (m == r)
     *             return A[i], A[j] //as primary key
     *          else if (m > max) 
     *             max = m 
     *             CK is A[i], A[j]
     * return CK
     */
    // @formatter:on
    private KeyAttribute keyInference()
    {
        long start = System.currentTimeMillis();

        ArrayList<Integer> eligibleAttributeIndices = new ArrayList<Integer>();

        // determine the attribute indices eligible for key consideration
        for (int i = 0; i < headers.size(); ++i)
        {
            // logical attribute indices are not eligible for key consideration
            if (logicalAttributeStartIndices.contains(i))
            {
                // skip over the remainder of this logical attribute
                i += logicalAttributeLength - 1;
            }
            else
            {
                eligibleAttributeIndices.add(i);
            }
        }

        // use a LinkedHashMap to ensure the Iterator returns keys in the same order we put them in
        LinkedHashMap<KeyAttribute, Integer> numUniqueValuesByCandidateKey = new LinkedHashMap<KeyAttribute, Integer>();
        int numTotalRecords = records.size();
        int numUniqueValues = 0;
        boolean primaryKeyFound = false;

        // we only care about the key in the uniqueValueTracker
        HashMap<String, Integer> uniqueValueTracker = null;
        DataRecord record = null;
        KeyAttribute candidateKey = null;
        String value = "";

        // iterate through the single-attribute potential keys to determine the number of unique values
        for (int i = 0; i < eligibleAttributeIndices.size() && !primaryKeyFound; ++i)
        {
            numUniqueValues = 0;
            int index = eligibleAttributeIndices.get(i);
            candidateKey = new KeyAttribute(headers.get(index), index);
            uniqueValueTracker = new HashMap<String, Integer>();

            for (int j = 0; j < numTotalRecords; ++j)
            {
                record = records.get(j);
                value = record.getAttributeValueAt(index);
                uniqueValueTracker.put(value, j);
            }

            numUniqueValues = uniqueValueTracker.keySet().size();

            if (numUniqueValues == numTotalRecords)
            {
                primaryKeyFound = true;
            }
            else
            {
                numUniqueValuesByCandidateKey.put(candidateKey, numUniqueValues);
            }
        }

        // iterate through the double-attribute potential keys to determine the number of unique values
        for (int i = 0; i < eligibleAttributeIndices.size() && !primaryKeyFound; ++i)
        {
            for (int j = i + 1; j < eligibleAttributeIndices.size() && !primaryKeyFound; ++j)
            {
                numUniqueValues = 0;
                int index_i = eligibleAttributeIndices.get(i);
                int index_j = eligibleAttributeIndices.get(j);
                candidateKey = new KeyAttribute(headers.get(index_i), index_i, headers.get(index_j), index_j);
                uniqueValueTracker = new HashMap<String, Integer>();

                for (int x = 0; x < numTotalRecords; ++x)
                {
                    record = records.get(x);
                    value = record.getAttributeValueAt(index_i) + " :: " + record.getAttributeValueAt(index_j);
                    uniqueValueTracker.put(value, x);
                }

                numUniqueValues = uniqueValueTracker.keySet().size();

                if (numUniqueValues == numTotalRecords)
                {
                    primaryKeyFound = true;
                }
                else
                {
                    numUniqueValuesByCandidateKey.put(candidateKey, numUniqueValues);
                }
            }
        }

        // at the end, let's return the one with the highest number of unique values
        Set<KeyAttribute> candidateKeys = numUniqueValuesByCandidateKey.keySet();
        int max = 0;

        Iterator<KeyAttribute> iterator = candidateKeys.iterator();
        while (iterator.hasNext() && !primaryKeyFound)
        {
            KeyAttribute potentialKey = iterator.next();
            if (numUniqueValuesByCandidateKey.get(potentialKey) > max)
            {
                max = numUniqueValuesByCandidateKey.get(potentialKey);
                candidateKey = new KeyAttribute(potentialKey);
            }
        }

        long end = System.currentTimeMillis();
        inferenceTime += (end - start);

        System.out.println("Primary Key Inference: " + candidateKey.toString());

        return candidateKey;
    }

    /*
     * Return the index of the attribute whose value defines the record type (temporal grouping value). The record type
     * attribute is the same as the key attribute if the key is a singular attribute. If the key is two attributes, the
     * record type attribute is the one with fewer distinct values amongst the records.
     */
    private int recordTypeInference()
    {
        long start = System.currentTimeMillis();

        int recordTypeIndex = -1;

        if (key.getLength() == 1)
        {
            recordTypeIndex = key.getPrimaryAttributeIndex();
        }
        else if (key.getLength() == 2)
        {
            int primaryIndex = key.getPrimaryAttributeIndex();
            int secondaryIndex = key.getSecondaryAttributeIndex();
            HashMap<String, Integer> distinctValuesForPrimaryAttribute = new HashMap<String, Integer>();
            HashMap<String, Integer> distinctValuesForSecondaryAttribute = new HashMap<String, Integer>();

            // determine how many distinct values there are for each of the key attributes
            for (int i = 0; i < records.size(); ++i)
            {
                DataRecord record = records.get(i);
                distinctValuesForPrimaryAttribute.put(record.getAttributeValueAt(primaryIndex), i);
                distinctValuesForSecondaryAttribute.put(record.getAttributeValueAt(secondaryIndex), i);
            }

            // the record type is the key attribute that has fewer distinct values
            if (distinctValuesForPrimaryAttribute.keySet().size() < distinctValuesForSecondaryAttribute.keySet().size())
            {
                // the primary attribute is the record type
                recordTypeIndex = primaryIndex;
            }
            else
            {
                // the secondary attribute is the record type
                recordTypeIndex = secondaryIndex;
            }
        }

        long end = System.currentTimeMillis();
        inferenceTime += (end - start);

        System.out.println(
                "Temporal Grouping Value Inference: " + headers.get(recordTypeIndex) + " @ index " + recordTypeIndex);

        return recordTypeIndex;
    }

    /**
     * Limitation: assumes _line is tab-delimited
     * 
     * @param _line
     * @return
     */
    public DataRecord parseRecord(String _line)
    {
        DataRecord record = new DataRecord(recordNum++);
        String[] fields = _line.split("\t");
        int logicalAttributeCount = 0;

        long startInference = 0;
        long endInference = 0;

        for (int i = 0; i < fields.length; ++i)
        {
            // add the raw field value to the data record
            record.addAttributeValue(fields[i]);

            startInference = System.currentTimeMillis();

            if (logicalAttributeStartIndices.contains(i))
            {
                LogicalAttribute la = new LogicalAttribute(logicalAttributeCount++);

                // let's go ahead and populate the logical attribute by grabbing from future iterative values
                for (int j = 0; j < logicalAttributeLength; ++j)
                {
                    // must protect against the possibility that the end of the record has blanks
                    String ts = "";
                    if ((i + j) < fields.length)
                    {
                        ts = fields[i + j];
                    }

                    la.addAttributeValue(ts);

                    if (StringUtils.containsIgnoreCase(headers.get(i + j), "timestamp"))
                    {
                        la.setTimestamp(ts, DataRecord.TimestampGranularity.EXACT, i + j);
                    }
                    else if (StringUtils.containsIgnoreCase(headers.get(i + j), "time"))
                    {
                        la.setTimestamp(ts, DataRecord.TimestampGranularity.TIME_OF_DAY, i + j);
                    }
                    else if (StringUtils.containsIgnoreCase(headers.get(i + j), "date"))
                    {
                        la.setTimestamp(ts, DataRecord.TimestampGranularity.DATE, i + j);
                    }
                }

                // record expects logical attribute to have timestamp and timestamp granularity set
                record.addLogicalAttribute(la);
            }

            endInference = System.currentTimeMillis();
            inferenceTime += (endInference - startInference);
        }

        startInference = System.currentTimeMillis();

        /* let the record know that all data fields have been added so that it may determine its aggregate fields */
        record.readComplete();

        endInference = System.currentTimeMillis();
        inferenceTime += (endInference - startInference);

        return record;
    }

    public void outputRawFieldsToFile(String _file, boolean _withIDs)
            throws FileNotFoundException, UnsupportedEncodingException
    {
        _file = "output\\" + _file.substring(_file.lastIndexOf("\\") + 1);

        PrintWriter writer = new PrintWriter(_file);

        String headerRow = rawHeaders;
        String valuesRow = "";
        DataRecord record = null;

        if (_withIDs)
        {
            headerRow = "ID\t" + headerRow;
        }

        writer.println(headerRow);

        for (int i = 0; i < records.size(); ++i)
        {
            record = records.get(i);
            valuesRow = record.toTabDelimRawFields();

            if (_withIDs)
            {
                valuesRow = i + "\t" + valuesRow;
            }

            writer.println(valuesRow);
        }

        writer.close();
    }

    public String outputAnalysisFieldsToFile(String _file) throws FileNotFoundException, UnsupportedEncodingException
    {
        _file = "output\\" + _file.substring(_file.lastIndexOf("\\") + 1);

        PrintWriter writer = new PrintWriter(_file);

        writer.println(DataRecord.getAnalysisHeaderRow(rawHeaders));

        DataRecord record = null;

        for (int i = 0; i < records.size(); ++i)
        {
            record = records.get(i);
            writer.println(record.toTabDelimAnalysisFields());
        }

        writer.close();

        return _file;
    }
}
