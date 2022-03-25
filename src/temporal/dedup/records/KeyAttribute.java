package temporal.dedup.records;

import java.util.ArrayList;

/**
 * Representation of a single or dual attribute key value for a record. Maintains indices, attribute names, and
 * attribute values for the key elements.
 */
public class KeyAttribute
{
    private String primaryAttributeName;
    private String primaryAttributeValue;
    private int primaryAttributeIndex;

    private String secondaryAttributeName;
    private String secondaryAttributeValue;
    private int secondaryAttributeIndex;

    private int length;

    public KeyAttribute()
    {
        primaryAttributeName = primaryAttributeValue = secondaryAttributeName = secondaryAttributeValue = "";
        primaryAttributeIndex = secondaryAttributeIndex = -1;
        length = 0;
    }

    public KeyAttribute(String _attributeName, int _index)
    {
        primaryAttributeName = new String(_attributeName);
        primaryAttributeIndex = _index;
        length = 1;

        primaryAttributeValue = secondaryAttributeName = secondaryAttributeValue = "";
        secondaryAttributeIndex = -1;
    }

    public KeyAttribute(String _attributeName1, int _index1, String _attributeName2, int _index2)
    {
        primaryAttributeName = new String(_attributeName1);
        primaryAttributeIndex = _index1;

        secondaryAttributeName = new String(_attributeName2);
        secondaryAttributeIndex = _index2;
        length = 2;

        primaryAttributeValue = secondaryAttributeValue = "";
    }

    public KeyAttribute(KeyAttribute _copy)
    {
        primaryAttributeName = new String(_copy.primaryAttributeName);
        primaryAttributeValue = new String(_copy.primaryAttributeValue);
        primaryAttributeIndex = _copy.primaryAttributeIndex;

        secondaryAttributeName = new String(_copy.secondaryAttributeName);
        secondaryAttributeValue = new String(_copy.secondaryAttributeValue);
        secondaryAttributeIndex = _copy.secondaryAttributeIndex;

        length = _copy.length;
    }

    public boolean isSame(KeyAttribute _compare)
    {
        boolean match = true;

        if (length != _compare.length)
        {
            match = false;
        }
        else if (length == 1)
        {
            if (!primaryAttributeName.equals(_compare.primaryAttributeName)
                    || !primaryAttributeValue.equals(_compare.primaryAttributeValue)
                    || primaryAttributeIndex != _compare.primaryAttributeIndex)
            {
                match = false;
            }
        }
        else if (length == 2)
        {
            if (!primaryAttributeName.equals(_compare.primaryAttributeName)
                    || !primaryAttributeValue.equals(_compare.primaryAttributeValue)
                    || primaryAttributeIndex != _compare.primaryAttributeIndex
                    || !secondaryAttributeName.equals(_compare.secondaryAttributeName)
                    || !secondaryAttributeValue.equals(_compare.secondaryAttributeValue)
                    || secondaryAttributeIndex != _compare.secondaryAttributeIndex)
            {
                match = false;
            }
        }

        return match;
    }

    public void extractKeyValues(ArrayList<String> _rawValues)
    {
        if (length == 1)
        {
            primaryAttributeValue = _rawValues.get(primaryAttributeIndex);
        }
        else if (length == 2)
        {
            primaryAttributeValue = new String(_rawValues.get(primaryAttributeIndex));
            secondaryAttributeValue = new String(_rawValues.get(secondaryAttributeIndex));
        }
        else
        {
            System.err.println("Can not extract key values; key attributes have not been established");
        }
    }

    public String getPrimaryAttributeName()
    {
        return primaryAttributeName;
    }

    public String getPrimaryAttributeValue()
    {
        return primaryAttributeValue;
    }

    public int getPrimaryAttributeIndex()
    {
        return primaryAttributeIndex;
    }

    public String getSecondaryAttributeName()
    {
        return secondaryAttributeName;
    }

    public String getSecondaryAttributeValue()
    {
        return secondaryAttributeValue;
    }

    public int getSecondaryAttributeIndex()
    {
        return secondaryAttributeIndex;
    }

    public int getLength()
    {
        return length;
    }

    public String toString()
    {
        String s = "Key attributes haev not been established";

        if (length == 1)
        {
            s = "Key attribute is: [" + primaryAttributeName + " @ index " + primaryAttributeIndex + "]";
        }
        else if (length == 2)
        {
            s = "Key attributes are: [" + primaryAttributeName + " @ index " + primaryAttributeIndex + "], ["
                    + secondaryAttributeName + " @ index " + secondaryAttributeIndex + "]";
        }
        else
        {
            // use default/initialized string value
        }

        return s;
    }
}
