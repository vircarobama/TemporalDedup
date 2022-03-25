package temporal.dedup;

/**
 * In order to flag duplicative records, it is necessary to first enumerate the various types of redundancies we may
 * observe between two records. The following duplicate classes are described in terms of records X and Y. Their string
 * value short-hands is for use in analysis, linking, output, etc.
 */
public class DuplicationClasses
{
    /**
     * Exact duplicates. All attribute values match between X and Y.
     */
    public static final String EXACT_MATCH = "EXACT";

    /**
     * Key value differs. All attribute values match between records X and Y except that X.key_attribute ≠
     * Y.key_attribute. For primary keys composed of more than one attribute, a difference in values for any one of
     * those attributes qualifies for this case.
     */
    public static final String NONKEY_MATCH = "NONKEY";

    /**
     * Elapsed time match. The total elapsed time (delta between earliest and latest timestamps for a record) is the
     * same. This is similar to Timeline match, but differs in the sense that the attribute time order may differ in
     * this case. The likelihood of true duplication increases as the elapsed time increases.
     */
    public static final String ELAPSED_TIME_MATCH = "ELAPSED_TIME";

    /**
     * Unconstrained order match. A sequence match between multiple records for attributes that are not required to be
     * in a particular time order. The more such attributes there are, the likelihood of duplication is significantly
     * increased.
     */
    public static final String UNCONSTRAINED_ORDER_MATCH = "ORDER";

    /**
     * Modified attributes. Multiple records for the same primary key have inconsistent attribute values. Indicative of
     * multiple pulls of the same record, with edited values.
     */
    public static final String MODIFIED_VALUES = "MODIFIED";

    /**
     * Timeline match. The same set of attributes are in the same time order between two records. If the timestamps are
     * exact matches across the board, the Exact duplicates and Key value differs classes would need to be considered.
     * 
     * Not used. The generic Timeline match class is detected through techniques focused on specifically on other
     * classes.
     */
    public static final String TIMELINE_MATCH = "TIMELINE";

    /*
     * The following class is provided for informational completeness only. It is used to detect potential errant
     * records; it does not assert duplication.
     * 
     * Illegitimate sequence. One or more attributes are out of legitimate time order. For many domains, there is no
     * explicit requisite order that applies to all records. Rather, some or all records may have subsets of attributes
     * where order matters. This partial order is not specified in metadata; it must be uncovered through analysis or
     * documented by a domain expert.
     */
}
