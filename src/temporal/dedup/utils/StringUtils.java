package temporal.dedup.utils;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

/**
 * Collection of static utilities that provide information on, or manipulation for, string values.
 */
public class StringUtils
{
    /**
     * Replaces the last instance of the _toReplace substring within string _source with a new substring (_replaceWith).
     * 
     * @param _source      String to modify
     * @param _toReplace   Substring to find and replace with the one specified by _replaceWith
     * @param _replaceWith Substring to replace the one specified by _toReplace
     * @return Updated string after replacing the specified substring with its specified replacement
     */
    public static String replaceLast(String _source, String _toReplace, String _replaceWith)
    {
        int lastIndex = _source.lastIndexOf(_toReplace);

        if (lastIndex < 0)
        {
            return _source;
        }

        String tail = _source.substring(lastIndex).replaceFirst(_toReplace, _replaceWith);

        return _source.substring(0, lastIndex) + tail;
    }

    /**
     * Determines whether or not _substring exists within _fullstring. Returns true if it is found, false otherwise.
     * 
     * @param _fullstring String to search
     * @param _substring  Substring of interest
     * @return true if _fullstring contains _substring; false otherwise
     */
    public static boolean containsIgnoreCase(String _fullstring, String _substring)
    {
        boolean contains = false;

        _substring = _substring.toLowerCase();
        _fullstring = _fullstring.toLowerCase();

        if (_fullstring.contains(_substring))
        {
            contains = true;
        }

        return contains;
    }

    /**
     * Provided a string representation of a date, return as a numerical timestamp representing the number of seconds
     * since Jan 1, 1970. If the string can not be recognized as a date, 0 will be returned.
     * 
     * @param _date date to be converted into a timestamp (in seconds)
     * @return seconds between Jan 1, 1970 and _date
     */
    public static long convertDate(String _date)
    {
        if (_date.trim().length() == 0)
        {
            return 0;
        }

        Calendar calendar = null;

        // attempt to parse the date string in to one of the supported formats
        calendar = parseDate_ISO8601(_date);

        if (calendar == null)
        {
            calendar = parseDate_DDspMonthspYYYY(_date);
        }

        if (calendar == null)
        {
            calendar = parseDate_DDdashMONdashYY(_date);
        }

        if (calendar == null)
        {
            // date is not a currently supported format
            return 0;
        }

        long secondsSinceEpoch = calendar.getTimeInMillis() / 1000L;
        return secondsSinceEpoch;
    }

    /*
     * Attempt to parse the given date string using the DD month YYYY format (e.g. 23 February 2018). If this causes an
     * exception, we return null. Otherwise, we return the corresponding Calendar object.
     */
    private static Calendar parseDate_DDspMonthspYYYY(String _date)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();

        try
        {
            // parse date field in to year, month, day
            int day = Integer.parseInt(_date.substring(0, _date.indexOf(" ")));
            _date = _date.substring(_date.indexOf(" ") + 1);
            int month = monthConversion(_date.substring(0, _date.indexOf(" ")));
            _date = _date.substring(_date.indexOf(" ") + 1);
            int year = Integer.parseInt(_date);

            calendar.set(year, month, day);
        }
        catch (Exception e)
        {
            // date is not in the DD month YYYY (e.g. 23 February 2018) format
            calendar = null;
        }

        return calendar;
    }

    /*
     * Attempt to parse the given date string using the DD-mon-YY format (e.g. 23-Feb-18). If this causes an exception,
     * we return null. Otherwise, we return the corresponding Calendar object.
     */
    private static Calendar parseDate_DDdashMONdashYY(String _date)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();

        try
        {
            // parse date field in to year, month, day
            int day = Integer.parseInt(_date.substring(0, _date.indexOf("-")));
            _date = _date.substring(_date.indexOf("-") + 1);
            int month = monthConversion(_date.substring(0, _date.indexOf("-")));
            _date = _date.substring(_date.indexOf("-") + 1);
            int year = Integer.parseInt(_date) + 2000;

            calendar.set(year, month, day);
        }
        catch (Exception e)
        {
            // date is not in the DD month YYYY (e.g. 23 February 2018) format
            calendar = null;
        }

        return calendar;
    }

    /*
     * Attempt to parse the given date string using the ISO 8601 format. If this causes an exception, we return null.
     * Otherwise, we return the corresponding Calendar object.
     */
    private static Calendar parseDate_ISO8601(String _date)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();

        try
        {
            // parse date field in to year, month, day
            int year = Integer.parseInt(_date.substring(0, _date.indexOf("-")));
            _date = _date.substring(_date.indexOf("-") + 1);
            int month = Integer.parseInt(_date.substring(0, _date.indexOf("-"))) - 1;
            _date = _date.substring(_date.indexOf("-") + 1);
            int day = Integer.parseInt(_date);

            calendar.set(year, month, day);
        }
        catch (Exception e)
        {
            // date is not in ISO 8601 format
            calendar = null;
        }

        return calendar;
    }

    /*
     * Converts the given string representation of a month and returns the Calendar class constant for that month. For
     * example, "January" would return Calendar.JANUARY. If the string is not recognized as a month, -1 is returned.
     */
    private static int monthConversion(String _month)
    {
        int value = -1;

        String m = _month.trim().substring(0, 3).toLowerCase();

        switch (m)
        {
            case "jan":
                value = Calendar.JANUARY;
                break;
            case "feb":
                value = Calendar.FEBRUARY;
                break;
            case "mar":
                value = Calendar.MARCH;
                break;
            case "apr":
                value = Calendar.APRIL;
                break;
            case "may":
                value = Calendar.MAY;
                break;
            case "jun":
                value = Calendar.JUNE;
                break;
            case "jul":
                value = Calendar.JULY;
                break;
            case "aug":
                value = Calendar.AUGUST;
                break;
            case "sep":
                value = Calendar.SEPTEMBER;
                break;
            case "oct":
                value = Calendar.OCTOBER;
                break;
            case "nov":
                value = Calendar.NOVEMBER;
                break;
            case "dec":
                value = Calendar.DECEMBER;
                break;
        }

        return value;
    }

    /**
     * Provided a string representation of a time of day, return as a numerical timestamp representing the number of
     * seconds between Jan 1, 1970 and today's date at the given time of day. If the string can not be recognized as a
     * time of day, 0 will be returned.
     * 
     * @param _tod time of day to be converted into a timestamp (in seconds)
     * @return seconds between Jan 1, 1970 and today's date at the given _tod
     */
    @SuppressWarnings("deprecation")
    public static long convertTimeOfDay(String _tod)
    {
        if (_tod.trim().length() == 0)
        {
            return 0;
        }

        // parse time field in to hrs, mins, secs (remember to use PM to add 12 to hrs)
        int hrs = Integer.parseInt(_tod.substring(0, _tod.indexOf(":")));
        _tod = _tod.substring(_tod.indexOf(":") + 1);
        int mins = Integer.parseInt(_tod.substring(0, _tod.indexOf(":")));
        _tod = _tod.substring(_tod.indexOf(":") + 1);
        int secs = Integer.parseInt(_tod.substring(0, 2));

        // if the time is 12, we need to reset to 0 for midnight (AM) and 12 for noon (PM)
        if (hrs == 12)
        {
            hrs = hrs - 12;
        }

        if (_tod.contains("PM"))
        {
            hrs += 12;
        }

        Date d = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(d.getYear(), d.getMonth(), d.getDay(), hrs, mins, secs);

        long secondsSinceEpoch = calendar.getTimeInMillis() / 1000L;
        return secondsSinceEpoch;
    }

    /*
     * Analyzes a given set of strings to determine whether or not any of them are temporal in nature. Returns true if
     * at least one is; false otherwise. Limitation: requires presence of 'date' or 'time' to be explicitly stated
     * within the given string to be recognized as temporal.
     */
    public static boolean hasRecognizedTemporalName(ArrayList<String> _set)
    {
        boolean temporal = false;

        for (int i = 0; i < _set.size(); ++i)
        {
            String searchString = _set.get(i).toLowerCase();

            if (searchString.contains("date") || searchString.contains("time"))
            {
                temporal = true;
            }
        }

        return temporal;
    }

    /**
     * Given an ordered set of strings, this function will find the longest recurring subset of strings and return that set of strings if it includes a
     * temporal string (e.g. 'date', 'time', 'timestamp', 'time of day').  Otherwise it will return an empty set.
     * 
     * Future enhancement:
     * - Recursively search for smaller repeated subsets until one is found that does include a temporal string and then return that.
     * 
     * @param _set Ordered set of strings to investigate
     * @return Longest recurring subset of strings that includes a temporal string or an empty set otherwise.
     */
    public static ArrayList<String> longestRepeatedTemporalStringSet(ArrayList<String> _set)
    {
        int n = _set.size();
        int lrs[][] = new int[n + 1][n + 1];

        ArrayList<String> longest = new ArrayList<String>();
        int length = 0;

        int i, index = 0;
        for (i = 1; i <= n; ++i)
        {
            for (int j = i + 1; j <= n; ++j)
            {
                if (_set.get(i - 1).equals(_set.get(j - 1)) && lrs[i - 1][j - 1] < (j - i))
                {
                    lrs[i][j] = lrs[i - 1][j - 1] + 1;

                    if (lrs[i][j] > length)
                    {
                        length = lrs[i][j];
                        index = Math.max(i, index);
                    }
                    else
                    {
                        lrs[i][j] = 0;
                    }
                }
            }
        }

        if (length > 0)
        {
            for (i = index - length + 1; i <= index; ++i)
            {
                longest.add(_set.get(i - 1));
            }
        }

        if (!hasRecognizedTemporalName(longest))
        {
            // only return the longest subset if it has temporal data; otherwise reset to an empty list
            longest = new ArrayList<String>();
        }
        
        return longest;
    }
}
