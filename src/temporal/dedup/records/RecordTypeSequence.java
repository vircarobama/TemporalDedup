package temporal.dedup.records;

import java.util.ArrayList;
import java.util.Random;

import temporal.dedup.TemporalDedup;
import temporal.dedup.utils.LCS;

/**
 * Maintains the Longest Common Subsequence (LCS) for a record of a particular type (temporal grouping value).
 */
public class RecordTypeSequence
{
    private ArrayList<String> sequences;
    private boolean complete;
    private String temporalGroupingValue;
    private LCS lcs;

    private int callsToAdd;
    private int seqSizeAtLastRequest;

    public RecordTypeSequence(String _id)
    {
        temporalGroupingValue = new String(_id);
        complete = false;
        sequences = new ArrayList<String>();
        lcs = null;
        callsToAdd = 0;
        seqSizeAtLastRequest = 0;
    }

    /**
     * Assumption: caller will ensure that all elements in the sequence are timestamped
     * 
     * @param _seq
     */
    public void addSequence(String _seq)
    {
        if (!complete)
        {
            if (TemporalDedup.LCS_SAMPLING__SELECTION_TYPE_RANDOM)
            {
                if (new Random().nextBoolean())
                {
                    sequences.add(_seq);
                }
            }
            else if (callsToAdd % TemporalDedup.LCS_SAMPLING__SELECTION_TYPE_TAKE_EVERY_X == 0)
            {
                sequences.add(_seq);
            }

            if (sequences.size() == TemporalDedup.LCS_SAMPLING__NUMBER_OF_RECORDS)
            {
                complete = true;

                if (sequences.size() == 0)
                {
                    lcs = new LCS(0, "");
                }
                else if (sequences.size() == 1)
                {
                    lcs = new LCS(sequences.get(0).split(" ").length, sequences.get(0));
                }
                else
                {
                    lcs = LCS.getLCS(sequences.get(0), sequences.get(1));

                    for (int i = 2; i < TemporalDedup.LCS_SAMPLING__NUMBER_OF_RECORDS; ++i)
                    {
                        lcs = LCS.getLCS(lcs.getSequence(), sequences.get(i));
                    }
                }
            }
        }

        ++callsToAdd;
    }

    public LCS getLCS()
    {
        // if not complete, calculate fomr what we do have on-hand
        if (!complete)
        {
            if (sequences.size() > seqSizeAtLastRequest)
            {
                seqSizeAtLastRequest = sequences.size();

                if (seqSizeAtLastRequest == 1)
                {
                    lcs = LCS.getLCS(sequences.get(0), sequences.get(0));
                }
                else
                {
                    lcs = LCS.getLCS(sequences.get(0), sequences.get(1));

                    for (int i = 2; i < seqSizeAtLastRequest; ++i)
                    {
                        lcs = LCS.getLCS(lcs.getSequence(), sequences.get(i));
                    }
                }
            }
        }

        return lcs;
    }

    public String getId()
    {
        return temporalGroupingValue;
    }
}
