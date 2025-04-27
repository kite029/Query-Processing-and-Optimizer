package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class implements the JoinPredicate interface.
 *
 * DO NOT CHANGE the constructor or existing member variables!
 * TODO: Implement the evaluate() method and use it in your implementation of the join operator
 *
 */
public class EqualityJoinPredicate implements JoinPredicate {

    protected final static Logger logger = LogManager.getLogger();

    private final String leftColumn; // left column name
    private final String rightColumn; // right column name

    public EqualityJoinPredicate(String leftColumn, String rightColumn) {
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

    /**
     * Evaluates if left and right tuples.
     * @param left  the tuple from the left input
     * @param right the tuple from the right input
     * @return true if the tuples match based on their leftColumn and rightColumn value
     */
    @Override
    public boolean evaluate(Tuple left, Tuple right) {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Left tuple " + left.getValues() + "[" + left.getSchema() + "]");
        logger.trace("Right tuple " + right.getValues() + "[" + right.getSchema() + "]");
        logger.trace("Condition " + leftColumn + " = " + rightColumn);
        // -------------------------

        //TODO: Implement me!

        // STEP 1: Retrieve values from both tuples based on column names
        Object leftValue = left.get(leftColumn);   // Get value from the left tuple based on the column name
        Object rightValue = right.get(rightColumn); // Get value from the right tuple based on the column name

        // STEP 2: Compare the values
        if (leftValue == null || rightValue == null) {
            return leftValue == rightValue;  // return true if both are null, false if only one is null
        }

        return leftValue.equals(rightValue);  // Use equals for comparison of values
    }


    // DO NOT REMOVE THESE METHODS
    public String getLeftColumn() {
        return leftColumn;
    }

    public String getRightColumn() {
        return rightColumn;
    }

    @Override
    public String toString() {
        return "EqualityJoinPredicate[" +
                "leftColumn='" + leftColumn + '\'' +
                ", rightColumn='" + rightColumn + '\'' +
                ']';
    }
}
