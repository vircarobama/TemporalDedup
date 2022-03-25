package temporal.dedup.utils;

/**
 * String representation of the Longest Common Sequence concept.
 */
public class LCS
{
    /*
     * LCS sequence length
     */
    private int length;

    /*
     * String representation of LCS, elements separated by single space
     */
    private String sequence;

    /**
     * Create a data structure representation of a longest common sequence with the sequence and its intended length
     * specified. If the specified length does not match the length of the provided sequence, the length of the sequence
     * is stored.
     * 
     * @param _length   intended length of the provided sequence
     * @param _sequence string representation of the LCS with elements separated by a single space
     */
    public LCS(int _length, String _sequence)
    {
        length = _length;
        sequence = new String(_sequence);

        int seqLength = sequence.split(" ").length;

        if (seqLength != length)
        {
            // adjust length
            length = seqLength;
        }
    }

    /**
     * Returns the longest common subsequence represented by this object as a string representation with a single space
     * between individual elements.
     * 
     * @return string representation of the LCS with elements separated by a single space
     */
    public String getSequence()
    {
        return sequence;
    }

    /**
     * Returns the length of the longest common subsequence represented by this object.
     * 
     * @return length of the LCS
     */
    public int getLength()
    {
        return length;
    }

    /**
     * Returns the Longest Common Subsequence (LCS) between strings _x and _y where each element is expected to be an
     * integer separated by a space.
     * 
     * @param _x First string with elements separated by spaces
     * @param _y Second string with elements separated by spaces
     * @return LCS object representing the longest common subsequence between _x and _y
     */
    public static LCS getLCS(String _x, String _y)
    {
        // If either string is empty, there is no LCS
        if (_x.length() == 0 || _y.length() == 0)
        {
            return new LCS(0, "");
        }

        // parse out the elements in _x and _y
        String[] xitems = _x.split(" ");
        String[] yitems = _y.split(" ");

        int m = xitems.length;
        int n = yitems.length;
        int[][] z = new int[m + 1][n + 1];

        /*
         * Construct z[m+1][n+1]. z[i][j] contains the length of the LCS between _x[0..i-1] and _y[0..j-1].
         */
        for (int i = 0; i <= m; ++i)
        {
            for (int j = 0; j <= n; ++j)
            {
                if (i == 0 || j == 0)
                {
                    z[i][j] = 0;
                }
                else if (xitems[i - 1].equals(yitems[j - 1]))
                {
                    z[i][j] = z[i - 1][j - 1] + 1;
                }
                else
                {
                    z[i][j] = Math.max(z[i - 1][j], z[i][j - 1]);
                }
            }
        }

        String lcs = new String();

        /*
         * Construct the elements of the LCS
         */
        int i = m, j = n;
        while (i > 0 && j > 0)
        {
            // If current elements in _x and _y are the same, then current element is part of the LCS
            if (xitems[i - 1].equals(yitems[j - 1]))
            {
                lcs = xitems[i - 1] + " " + lcs;
                --i;
                --j;
            }
            // Otherwise, go in the direction of the larger value
            else if (z[i - 1][j] > z[i][j - 1])
            {
                --i;
            }
            else
            {
                --j;
            }
        }

        LCS returnValue = new LCS(z[m][n], lcs);

        return returnValue;
    }
}
