package temporal.dedup.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import temporal.dedup.records.DataRecord;

/**
 * Populates and maintains the truth data and assesses predictions against the truth data, providing key feedback such
 * as FNs, FPs, TNs, TPs, precision, recall, F1, and MCC.
 */
public class ConfusionMatrix
{
    private ArrayList<Integer> actual;
    private ArrayList<Integer> predicted;

    private int tn;
    private int fn;
    private int tp;
    private int fp;

    private double precision;
    private double recall;
    private double f1score;
    private double mcc;

    private int sourceSize;

    /**
     * Given an array of records and a filename specifying the truth data with respect to duplicates within the given
     * record set, construct an object that will maintain that truth data and enable assessment of predictions of
     * against that truth data.
     * 
     * Limitation: assumes truth file has a header row, is tab-delimited, and the first value of each row is a record ID
     * of a duplicate
     * 
     * @param _truthSource name of file containing the list of duplicate IDs
     * @param _records
     */
    public ConfusionMatrix(String _truthSource, ArrayList<DataRecord> _records)
    {
        sourceSize = _records.size();

        actual = new ArrayList<Integer>();
        predicted = new ArrayList<Integer>();

        tn = fn = tp = fp = 0;
        precision = recall = f1score = mcc = 0.0;

        // read in values (one per line) and add to actual list (check to see if already there before adding)
        try
        {
            File file = new File(_truthSource); // creates a new file instance
            FileReader fr = new FileReader(file); // reads the file
            BufferedReader br = new BufferedReader(fr); // creates a buffering character input stream
            String line = "";
            int value = -1;
            boolean readHeader = false;

            while ((line = br.readLine()) != null)
            {
                if (!readHeader)
                {
                    /*
                     * no need to retain the information in the header row; we are assuming the first value on each line
                     * is the ID of a duplicate
                     */
                    readHeader = true;
                }
                else
                {
                    // parse the first value only
                    line = line.split("\t")[0];
                    value = Integer.parseInt(line.trim());

                    if (!actual.contains(value))
                    {
                        actual.add(value);
                        _records.get(value).indicateAsTruthDuplicate();
                    }
                }
            }
            fr.close(); // closes the stream and releases the resources

            System.out.println("ACTUAL # DUPLICATES FROM TRUTH SOURCE: " + actual.size() + " of " + sourceSize);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Provided with an array of record IDs that are predicted to be duplicates, assess the accuracy of those
     * predictions against the truth data.
     * 
     * @param _predicted Array of record IDs predicted to be duplicates
     */
    public void assessPrediction(ArrayList<Integer> _predicted)
    {
        tn = fn = tp = fp = 0;
        precision = recall = f1score = mcc = 0.0;

        predicted = new ArrayList<Integer>(_predicted);
        System.out.println("PREDICTED # DUPLICATES TO ASSESS: " + predicted.size());

        String FPs = "";
        String FNs = "";

        // cycle through all the predicted positive
        for (int i = 0; i < predicted.size(); ++i)
        {
            Integer value = predicted.get(i);

            // if actual positive
            if (actual.contains(value))
            {
                ++tp;
            }
            // else actual negative
            else
            {
                ++fp;
                FPs += value + " ";
            }
        }

        // cycle through all the actual positive
        for (int j = 0; j < actual.size(); ++j)
        {
            Integer value = actual.get(j);

            // if predicted negative
            if (!predicted.contains(value))
            {
                ++fn;
                FNs += value + " ";
            }
        }

        String fpout = FPs.trim();
        if (fpout.length() == 0)
        {
            fpout = "none";
        }

        String fnout = FNs.trim();
        if (fnout.length() == 0)
        {
            fnout = "none";
        }

        System.out.println("FALSE POSITIVES: predicted " + fpout);
        System.out.println("FALSE NEGATIVES: predicted " + fnout);

        // calculate the number of actual negatives predicted as negatives
        tn = sourceSize - tp - fp - fn;

        // precision = TP / (TP + FP)
        precision = (double) tp / ((double) tp + (double) fp);

        // recall = TP / (TP + FN)
        recall = (double) tp / ((double) tp + (double) fn);

        // F1 score = [2 * (Precision * Recall)] / (Precision + Recall)
        f1score = (2 * (precision * recall)) / (precision + recall);

        // MCC = [(TP*TN)-(FP*FN)] / sqrt([(TP+FP)*(TP+FN)*(TN+FP)*(TN+FN)])
        mcc = ((tp * tn) - (fp * fn)) / Math.sqrt((double) (tp + fp) * (tp + fn) * (tn + fp) * (tn + fn));

        displayResults();
    }

    /*
     * Display the results of the assessment, including the confusion matrix and key performance parameters to standard
     * out
     */
    private void displayResults()
    {
        System.out.println("************************************************");
        System.out.println("Total # of records: " + sourceSize);
        System.out.println("Total # of true duplicates: " + actual.size());
        System.out.println("Total # of predicted duplicates: " + predicted.size());
        System.out.println("Precision: " + precision);
        System.out.println("Recall: " + recall);
        System.out.println("F1 Score: " + f1score);
        System.out.println("MCC: " + mcc);

        System.out.println("\t\tACTUAL");
        System.out.println("PREDICTED\tNegative\tPositive");
        System.out.println("Negative\t" + tn + "\t\t" + fn);
        System.out.println("Positive\t" + fp + "\t\t" + tp);
        System.out.println("************************************************");
    }
}
