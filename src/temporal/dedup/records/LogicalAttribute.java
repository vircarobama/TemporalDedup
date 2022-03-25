package temporal.dedup.records;

import java.util.ArrayList;

import temporal.dedup.utils.StringUtils;

/**
 * Maintains the attribute values and timestamps associated with a logical attribute. A logical attribute is a
 * collection of attributes that appear in the raw data that together describe an entity.
 */
public class LogicalAttribute
{
    private int relativeNumber;
    private long timestamp;

    // record index that specifies the timestamp used
    private int timestampIndex;
    private DataRecord.TimestampGranularity timestampGranularity;
    private ArrayList<String> attributeValues;

    /*
     * Future enhancement: - combine date and time fields if both are provided to calculate a more precise timestamp
     */

    public LogicalAttribute(int _relativeNumber)
    {
        relativeNumber = _relativeNumber;
        timestamp = 0;
        timestampIndex = -1;
        timestampGranularity = DataRecord.TimestampGranularity.UNKNOWN;
        attributeValues = new ArrayList<String>();
    }

    public LogicalAttribute(LogicalAttribute _copy)
    {
        relativeNumber = _copy.relativeNumber;
        timestamp = _copy.timestamp;
        timestampIndex = _copy.timestampIndex;
        timestampGranularity = _copy.timestampGranularity;
        attributeValues = new ArrayList<String>();

        ArrayList<String> _copyAttributes = _copy.attributeValues;
        for (int i = 0; i < _copyAttributes.size(); ++i)
        {
            attributeValues.add(_copyAttributes.get(i));
        }
    }

    public int getRelativeNumber()
    {
        return relativeNumber;
    }

    public void addAttributeValue(String _value)
    {
        attributeValues.add(_value);
    }

    public ArrayList<String> getAttributeValues()
    {
        return attributeValues;
    }

    /**
     * Provide the time to be associated with this logical attribute by specifiying the index of the time within the
     * record, the granularity of the timestamp, and the timestamp value itself.
     * 
     * @param _timestamp         raw string value of the time
     * @param _granularity       granularity of the timestamp (date, time of day, exact)
     * @param _indexWithinRecord the index the timestamp appears within the record
     */
    public void setTimestamp(String _timestamp, DataRecord.TimestampGranularity _granularity, int _indexWithinRecord)
    {
        if (_granularity == DataRecord.TimestampGranularity.EXACT)
        {
            setTimestamp(Long.parseLong(_timestamp), _granularity, _indexWithinRecord);
        }
        else if (_granularity == DataRecord.TimestampGranularity.TIME_OF_DAY)
        {
            setTimestamp(StringUtils.convertTimeOfDay(_timestamp), _granularity, _indexWithinRecord);
        }
        else if (_granularity == DataRecord.TimestampGranularity.DATE)
        {
            setTimestamp(StringUtils.convertDate(_timestamp), _granularity, _indexWithinRecord);
        }
        else
        {
            System.err.println("Attempted to set timestamp with an invalid or unknown level of granularity.");
        }
    }

    /*
     * if the provided _granularity offers a lower level than we already have, update the timestamp and timestamp index
     * with the given values for _timestamp and _indexWithinRecord so long as the _timestamp value is greater than 0
     */
    private void setTimestamp(long _timestamp, DataRecord.TimestampGranularity _granularity, int _indexWithinRecord)
    {
        // only process this if it is offering a lower level of granularity than what we've already got
        if (isLowerLevelOfGranularity(_granularity))
        {
            // only take the value if there is actually a value (something greater than zero)
            if (_timestamp > 0)
            {
                timestamp = _timestamp;
                timestampGranularity = _granularity;
                timestampIndex = _indexWithinRecord;
            }
        }
    }

    /*
     * return true if _test is lower level or same level as currently set level of granularity
     */
    private boolean isLowerLevelOfGranularity(DataRecord.TimestampGranularity _test)
    {
        boolean isTestLower = false;

        if (timestampGranularity == _test)
        {
            isTestLower = true;
        }
        else if (timestampGranularity == DataRecord.TimestampGranularity.EXACT)
        {
            isTestLower = false;
        }
        else if (timestampGranularity == DataRecord.TimestampGranularity.TIME_OF_DAY)
        {
            if (_test == DataRecord.TimestampGranularity.EXACT)
            {
                isTestLower = true;
            }
        }
        else if (timestampGranularity == DataRecord.TimestampGranularity.DATE)
        {
            if (_test == DataRecord.TimestampGranularity.EXACT || _test == DataRecord.TimestampGranularity.TIME_OF_DAY)
            {
                isTestLower = true;
            }
        }
        else
        {
            isTestLower = true;
        }

        return isTestLower;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public DataRecord.TimestampGranularity getTimestampGranularity()
    {
        return timestampGranularity;
    }

    public int getTimestampIndex()
    {
        return timestampIndex;
    }
}
