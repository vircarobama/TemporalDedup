package temporal.dedup.comparison;

import java.util.ArrayList;

import temporal.dedup.records.DataRecord;
import temporal.dedup.utils.ConfusionMatrix;

/**
 * Interface to be implemented by all comparison methods, providing for a standard mechanism to call the method that
 * provides handles to the records to be evaluated and a handle to the assessment utility.
 */
public interface ComparisonMethod_I
{
    /**
     * Execute the comparison algorithm against the given set of data records and call the provided confusion matrix to
     * assess the prediction (_cm.assessPrediction).
     * 
     * @param _cm      The confusion matrix object that may assess the prediction of this method against truth data
     * @param _headers The set of header names corresponding to each of a data record's raw attributes
     * @param _records The set of data records to perform deduplication evaluation on
     */
    public void executeComparsion(ConfusionMatrix _cm, ArrayList<String> _headers, ArrayList<DataRecord> _records);

    /**
     * Allow for blocking key to be externally provided. If blocking keys are not appropriate or needed for a particular
     * comparison method implementation, then the implementation may be a no-op.
     * 
     * @param _blockingKey String representing the blocking key as an attribute name (i.e. header name, column name)
     */
    public void provideBlockingKey(String _blockingKey);
}
