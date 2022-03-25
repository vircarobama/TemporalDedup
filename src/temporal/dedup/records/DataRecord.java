package temporal.dedup.records;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import temporal.dedup.utils.LCS;

/**
 * Data structure to represent all of the raw and aggregated information associated with a single data record.
 */
public class DataRecord
{
    /**
     * Granularity of the timestamp associated with a logical attribute.  The timestamp may be derived from a high-level attribute
     * like date, a combination of date and time of day, or it may be provided as an exact timestamp. 
     */
    public enum TimestampGranularity
    {
        DATE,
        TIME_OF_DAY,
        EXACT,
        UNKNOWN
    }
    
    /*
     * Global record type that indicates that a DataRecord may be compared with any other DataRecord, regardless of its key values,
     * so long as the other record also shares the global common record type
     */
    private final static String GLOBAL_RECORD_TYPE = "GlobalCommonRecordType";
    
    private int id;
    private ArrayList<String> rawAttributeValues;
    private int populatedValueCount;

    private KeyAttribute key;
    private ArrayList<LogicalAttribute> logicalAttributes;

    private String recordType;

    private long elapsedTime;
    private long earliest;
    private long latest;
    private TimestampGranularity timestampGranularity;

    private boolean hasKnownDuplicate;
    private boolean isTruthDuplicate;

    private String lcsForRecordType;
    private String eventSequence;
    private String eventSequenceConstrained;
    private String eventSequenceUnconstrained;
    private boolean lcsAdhered;
    private int typeConstrainedLength;    
    private int typeUnconstrainedLength;
    private int recordConstrainedLength;
    private int recordUnconstrainedLength;

    private ArrayList<Integer> matches;
    private ArrayList<String> classes;

    private Set<Integer> integerSet;
    private boolean integerSetPopulated;

    public DataRecord(int _id)
    {
        id = _id;
        key = new KeyAttribute();
        logicalAttributes = new ArrayList<LogicalAttribute>();
        rawAttributeValues = new ArrayList<String>();
        populatedValueCount = 0;

        recordType = "";

        earliest = latest = elapsedTime = 0;
        timestampGranularity = TimestampGranularity.UNKNOWN;
        hasKnownDuplicate = false;
        isTruthDuplicate = false;

        lcsForRecordType = "";
        eventSequence = "";
        eventSequenceConstrained = "";
        eventSequenceUnconstrained = "";
        lcsAdhered = true;
        typeConstrainedLength = typeUnconstrainedLength = recordConstrainedLength = recordUnconstrainedLength = 0;

        matches = new ArrayList<Integer>();
        classes = new ArrayList<String>();

        integerSet = new HashSet<Integer>();
        integerSetPopulated = false;
    }
    
    /**
     * Strict copy constructor.  Use with caution.
     * 
     * @param _copy
     */
    public DataRecord(DataRecord _copy)
    {
        id = _copy.id;
        key = _copy.key;
        logicalAttributes = _copy.logicalAttributes;
        rawAttributeValues = _copy.rawAttributeValues;
        populatedValueCount = _copy.populatedValueCount;
        recordType = _copy.recordType;
        earliest = _copy.earliest;
        latest = _copy.latest;
        elapsedTime = _copy.elapsedTime;
        timestampGranularity = _copy.timestampGranularity;
        hasKnownDuplicate = _copy.hasKnownDuplicate;
        isTruthDuplicate = _copy.isTruthDuplicate;
        lcsForRecordType = _copy.lcsForRecordType;
        eventSequence = _copy.eventSequence;
        eventSequenceConstrained = _copy.eventSequenceConstrained;
        eventSequenceUnconstrained = _copy.eventSequenceUnconstrained;
        lcsAdhered = _copy.lcsAdhered;
        typeConstrainedLength = _copy.typeConstrainedLength;
        typeUnconstrainedLength = _copy.typeUnconstrainedLength;
        recordConstrainedLength = _copy.recordConstrainedLength;
        recordUnconstrainedLength = _copy.recordUnconstrainedLength;
        matches = _copy.matches;
        classes = _copy.classes;
        integerSet = _copy.integerSet;
        integerSetPopulated = _copy.integerSetPopulated;
    }
    
    public void printDiffs(DataRecord _compare)
    {
        int thisSize = rawAttributeValues.size();
        int compSize = _compare.rawAttributeValues.size();
        
        for(int i=0; i<thisSize; ++i)
        {
            String thisVal = rawAttributeValues.get(i);
            String compVal = "";
            
            if(compSize > i)
            {
                compVal = _compare.rawAttributeValues.get(i);
            }
            
            if(!thisVal.equals(compVal))
            {
                System.out.println("Attribute#"+i + ", ID:" + id + " " + thisVal + "\tID:" + _compare.id + " " + compVal);
            }
        }
        
        for(int j=thisSize; j<compSize; ++j)
        {
            String thisVal = "";
            String compVal = _compare.rawAttributeValues.get(j);
            
            System.out.println("Attribute#"+j + ", ID:" + id + " " + thisVal + "\tID:" + _compare.id + " " + compVal);
        }
    }

    public Set<Integer> getRecordAsIntegerSet()
    {
        if (integerSetPopulated)
        {
            return integerSet;
        }
        
        // prepare the timestamps by going through the logical attributes and identifying the timestamp index for
        // each one that has a granularity of EXACT
        HashMap<Integer, Long> recordIndexToTimestamp = new HashMap<Integer, Long>();
        for(int i=0; i<logicalAttributes.size(); ++i)
        {
            LogicalAttribute la = logicalAttributes.get(i);
            if(la.getTimestampGranularity() == TimestampGranularity.EXACT)
            {
                int index = la.getTimestampIndex();
                long timestamp = la.getTimestamp();
                recordIndexToTimestamp.put(index,timestamp);
            }
        }

        /*
         * Include all fields from the raw data we read/process. The integer representation of all fields will be a hash
         * of their String value with two exceptions: (1) detected boolean fields will have a value of 1 for true and 0
         * for false; (2) detected (utilized) timestamp fields in integer/long format (not in date or time string
         * format) will use their raw int/long value.
         */
        for (int i = 0; i < rawAttributeValues.size(); ++i)
        {
            String value = rawAttributeValues.get(i);
            int toAdd = -1;

            if(recordIndexToTimestamp.containsKey(i))
            {
                toAdd = recordIndexToTimestamp.get(i).intValue();
            }
            else if (value.equalsIgnoreCase("true"))
            {
                toAdd = 1;
            }
            else if (value.equalsIgnoreCase("false"))
            {
                toAdd = 0;
            }
            else
            {
                toAdd = value.hashCode();
            }

            integerSet.add(toAdd);
        }

        integerSetPopulated = true;
        return integerSet;
    }

    public boolean hasKnownDuplicate()
    {
        return hasKnownDuplicate;
    }

    public boolean isPopulatedBeyondKey()
    {
        boolean populatedBeyondKey = false;

        if (populatedValueCount > key.getLength())
        {
            populatedBeyondKey = true;
        }

        return populatedBeyondKey;
    }
    
    public TimestampGranularity getTimestampGranularity()
    {
        return timestampGranularity;
    }

    public boolean allTimestamped()
    {
        boolean value = true;

        for (int i = 0; i < logicalAttributes.size() && value; ++i)
        {
            LogicalAttribute t = logicalAttributes.get(i);
            if (t.getTimestamp() <= 0)
            {
                value = false;
            }
        }
        
        if(logicalAttributes.size() == 0)
        {
            value = false;
        }

        return value;
    }

    public boolean anyTimestamped()
    {
        boolean value = false;

        for (int i = 0; i < logicalAttributes.size() && !value; ++i)
        {
            LogicalAttribute t = logicalAttributes.get(i);
            if (t.getTimestamp() > 0)
            {
                value = true;
            }
        }

        return value;
    }
    
    public int getNumTimestamps()
    {
        int count = 0;
        
        for (int i = 0; i < logicalAttributes.size(); ++i)
        {
            LogicalAttribute t = logicalAttributes.get(i);
            if (t.getTimestamp() > 0)
            {
                ++count;
            }
        }
        
        return count;
    }

    public void addMatch(Integer _match, String _class)
    {
        matches.add(_match);
        classes.add(_class);
        hasKnownDuplicate = true;
    }
    
    public void indicateAsTruthDuplicate()
    {
        isTruthDuplicate = true;
    }

    public boolean containsMatch(Integer _match)
    {
        boolean c = false;

        if (matches.contains(_match))
        {
            c = true;
        }

        return c;
    }

    /*
     * Determine whether or not this record and _compare have exactly the same key field values
     */
    public boolean sharesSameKeys(DataRecord _compare)
    {
        boolean match = false;

        if (key.isSame(_compare.key))
        {
            match = true;
        }

        return match;
    }

    public boolean equalsIgnoreKeyFields(DataRecord _compare)
    {
        boolean match = true;

        int keyIndex1 = key.getPrimaryAttributeIndex();
        int keyIndex2 = key.getSecondaryAttributeIndex();

        // if there are a different number of attributes, it's not a match
        if(rawAttributeValues.size() != _compare.rawAttributeValues.size())
        {
            match = false;
        }

        /*
         * Go through non-key fields and look for one that isn't equal; if found set match to false and end the
         * iteration. Do not need to compare aggregate fields as they are based on raw primary data.
         */
        for (int i = 0; i < rawAttributeValues.size() && match; ++i)
        {
            if (i != keyIndex1 && i != keyIndex2)
            {
                if (!rawAttributeValues.get(i).equals(_compare.rawAttributeValues.get(i)))
                {
                    match = false;
                }
            }
        }

        return match;
    }

    public boolean exactMatch(DataRecord _compare)
    {
        boolean match = true;

        // if there are a different number of attributes, it's not an exact match
        if(rawAttributeValues.size() != _compare.rawAttributeValues.size())
        {
            match = false;
        }
        
        // no need to compare aggregate fields - they are based on raw primary data
        for (int i = 0; i < rawAttributeValues.size() && match; ++i)
        {
            if (!rawAttributeValues.get(i).equals(_compare.rawAttributeValues.get(i)))
            {
                match = false;
            }
        }

        return match;
    }

    public void addAttributeValue(String _attribute)
    {
        rawAttributeValues.add(new String(_attribute));

        if (!_attribute.trim().equals(""))
        {
            ++populatedValueCount;
        }
    }

    public void addLogicalAttribute(LogicalAttribute _attribute)
    {        
        logicalAttributes.add(_attribute);
        timestampGranularity = _attribute.getTimestampGranularity();

        if (earliest == 0 || (_attribute.getTimestamp() != 0 && _attribute.getTimestamp() < earliest))
        {
            earliest = _attribute.getTimestamp();
        }

        if (_attribute.getTimestamp() > latest)
        {
            latest = _attribute.getTimestamp();
        }
   }

    /**
     * 
     * @param _index index of attribute that serves as the record type (i.e. temporal grouping value)
     */
    public void applyRecordType(int _index)
    {
        recordType = rawAttributeValues.get(_index);
    }
    
    /**
     * Indicates that this record shares a global record type; that is, it may be compared with any
     * other record in the dataset, regardless of its key values, so long as it has also applied
     * the global record type to itself.
     */
    public void applyGlobalRecordType()
    {
        recordType = GLOBAL_RECORD_TYPE;
    }
    
    public void applyKey(KeyAttribute _key)
    {
        key = new KeyAttribute(_key);
        key.extractKeyValues(rawAttributeValues);
    }

    /**
     * Assumption: logical attributes have already been determined; necessary to establish elapsed time and event
     * sequence
     * 
     * Note: we will not yet know the key or record type; that information is determined after all records have been read
     */
    public void readComplete()
    {
        // record has finished being populated; now generate aggregate fields

        /*
         * Now that we know the logical attributes (inclusive of timestamps), we may determine the elapsed time and
         * event sequence
         */
        elapsedTime = latest - earliest;

        // generate event sequence
        try
        {
            ArrayList<LogicalAttribute> copy = new ArrayList<LogicalAttribute>();

            for (int x = 0; x < logicalAttributes.size(); ++x)
            {
                LogicalAttribute y = new LogicalAttribute(logicalAttributes.get(x));
                copy.add(y);
            }

            Collections.sort(copy, new Comparator<LogicalAttribute>()
            {
                @Override
                public int compare(LogicalAttribute _one, LogicalAttribute _two)
                {
                    long value = _one.getTimestamp() - _two.getTimestamp();

                    return (int) value;
                }
            });

            eventSequence = "";

            for (int i = 0; i < copy.size(); ++i)
            {
                LogicalAttribute t = copy.get(i);
                if (t.getTimestamp() > 0)
                {
                    eventSequence += t.getRelativeNumber() + " ";
                }
            }

            eventSequence = eventSequence.trim();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private int getNumLogicalAttributes()
    {
        return logicalAttributes.size();
    }

    public String getEventSequence()
    {
        return eventSequence;
    }

    public long getElapsedTime()
    {
        return elapsedTime;
    }

    public String getEventSequenceUnconstrained()
    {
        return eventSequenceUnconstrained;
    }

    public int getEventSequenceUnconstrainedLength()
    {
        String[] items = eventSequenceUnconstrained.split(" ");
        return items.length;
    }

    private String getMatches()
    {
        String m = "";

        for (int i = 0; i < matches.size(); ++i)
        {
            m += matches.get(i) + " ";
        }

        m = m.trim();

        return m;
    }

    private String getClasses()
    {
        String c = "";

        for (int i = 0; i < classes.size(); ++i)
        {
            c += classes.get(i) + " ";
        }

        c = c.trim();

        return c;
    }

    public int getId()
    {
        return id;
    }

    public String getRecordType()
    {
        return recordType;
    }
    
    public String getAttributeValueAt(int _index)
    {
        return rawAttributeValues.get(_index);
    }
    
    public ArrayList<String> getRawAttributeValues()
    {
        return rawAttributeValues;
    }

    public boolean applyLCS(LCS _lcs)
    {
        eventSequenceConstrained = "";
        eventSequenceUnconstrained = "";
        lcsAdhered = true;

        if (_lcs == null)
        {
            lcsForRecordType = "";
            typeConstrainedLength = 0;
            typeUnconstrainedLength = getNumLogicalAttributes() - typeConstrainedLength;

            eventSequenceConstrained = "";
            eventSequenceUnconstrained = new String(eventSequence);
            lcsAdhered = true;

            recordConstrainedLength = 0;
            String[] seqItems = eventSequence.split(" ");
            if (seqItems[0].length() == 0)
            {
                recordUnconstrainedLength = 0;
            }
            else
            {
                recordUnconstrainedLength = seqItems.length;
            }
        }
        else if (_lcs.getLength() == 1)
        {
            lcsForRecordType = "";
            typeConstrainedLength = 0;
            typeUnconstrainedLength = getNumLogicalAttributes() - typeConstrainedLength;

            eventSequenceConstrained = "";
            eventSequenceUnconstrained = new String(eventSequence);
            lcsAdhered = true;

            recordConstrainedLength = 0;
            String[] seqItems = eventSequence.split(" ");
            if (seqItems[0].length() == 0)
            {
                recordUnconstrainedLength = 0;
            }
            else
            {
                recordUnconstrainedLength = seqItems.length;
            }
        }
        else
        {
            lcsForRecordType = new String(_lcs.getSequence());

            String lcs = _lcs.getSequence();
            String[] lcsItems = lcs.split(" ");
            String[] seqItems = eventSequence.split(" ");

            typeConstrainedLength = lcsItems.length;
            typeUnconstrainedLength = getNumLogicalAttributes() - typeConstrainedLength;

            int lcsIndex = 0;
            boolean found = false;

            for (int a = 0; a < seqItems.length; ++a)
            {
                found = false;

                for (int b = lcsIndex; b < lcsItems.length && !found; ++b)
                {
                    if (seqItems[a].equals(lcsItems[b]))
                    {
                        eventSequenceConstrained += " " + seqItems[a];
                        lcsIndex = b;
                        found = true;
                        ++recordConstrainedLength;
                    }
                }

                if (!found)
                {
                    if (seqItems[a].length() == 0)
                    {
                        // System.err.println("Got an empty element in the sequence for: " + user + "'s " + game);
                    }
                    else
                    {
                        eventSequenceUnconstrained += " " + seqItems[a];
                        ++recordUnconstrainedLength;
                    }
                }
            }

            eventSequenceConstrained = eventSequenceConstrained.trim();
            eventSequenceUnconstrained = eventSequenceUnconstrained.trim();

            // non lcs earned should not contain any items that are in lcs
            String[] nonlcsItems = eventSequenceUnconstrained.split(" ");

            for (int i = 0; i < lcsItems.length; ++i)
            {
                for (int j = 0; j < nonlcsItems.length; ++j)
                {
                    if (nonlcsItems[j].equals(lcsItems[i]) && nonlcsItems.length > 0)
                    {
                        lcsAdhered = false;
                    }
                }
            }
        }

        return lcsAdhered;
    }
    
    public String toTabDelimRawFields()
    {        
        String output = "";

        for (int i = 0; i < rawAttributeValues.size(); ++i)
        {
            if (i > 0)
            {
                output += "\t";
            }
            
            output += rawAttributeValues.get(i);
        }

        return output;
    }

    /**
     * Caller provides the raw data headers read from the source dataset file.  This method and toTabDelimAnalysisFields match analysis header titles
     * and content and must be maintained together.
     * 
     * @param _headers
     * @return
     */
    public static String getAnalysisHeaderRow(String _headers)
    {
        String analysisHeaders = "ID\tTruth Data Duplicate\t# Matches\tDuplicate IDs\tDetected By Duplicate Class\t# Timestamps\tEarliest Timestamp\tLatest Timestamp\tElapsed Time\t"
                + "Record Type LCS\tLCS Length\tEvent Sequence\tLCS Adherence\tUnconstrained Sequence\tUnconstrained Sequence Length\t";

        String headerRow = analysisHeaders + _headers;

        return headerRow;
    }

    /**
     * This method and toTabDelimAnalysisFields match analysis header titles and content and must be maintained together.
     * 
     * @return
     */
    public String toTabDelimAnalysisFields()
    {
        String output = "";

        // id #_matches duplicate_ids duplicate_classes earliest latest elapsed_time record_type_lcs lcs_length
        // event_sequence lcs_adherence unconstrained_sequence raw_attrs
        output += id + "\t" + isTruthDuplicate + "\t" + matches.size() + "\t" + getMatches() + "\t" + getClasses() + "\t" + getNumTimestamps() + "\t" + earliest + "\t"
                + latest + "\t" + elapsedTime + "\t" + lcsForRecordType + "\t" + typeConstrainedLength + "\t"
                + eventSequence + "\t" + lcsAdhered + "\t" + eventSequenceUnconstrained + "\t" + recordUnconstrainedLength + "\t" + toTabDelimRawFields();

        return output;
    }
}
