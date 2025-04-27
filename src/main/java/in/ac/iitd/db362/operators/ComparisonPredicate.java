package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Note: ONLY IMPLEMENT THE EVALUATE METHOD.
 * TODO: Implement the evaluate() method
 *
 * DO NOT CHANGE the constructor or existing member variables.
 *
 * A comparison predicate for simple atomic predicates.
 */
public class ComparisonPredicate implements Predicate {

    protected final static Logger logger = LogManager.getLogger();
    private final Object leftOperand;   // Either a constant or a column reference (String)
    private final String operator;        // One of: =, >, >=, <, <=, !=
    private final Object rightOperand;  // Either a constant or a column reference (String)

    public ComparisonPredicate(Object leftOperand, String operator, Object rightOperand) {
        this.leftOperand = leftOperand;
        this.operator = operator;
        this.rightOperand = rightOperand;
    }

    /**
     * Evaluate a tuple
     * @param tuple the tuple to evaluate
     * @return return true if leftOperand operator righOperand holds in that tuple
     */
    @Override
    public boolean evaluate(Tuple tuple) {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Evaluating tuple " + tuple.getValues() + " with schema " + tuple.getSchema());
        logger.trace("[Predicate] " + leftOperand + " " + operator + " " + rightOperand);
        // -------------------------

        // STEP 1: Resolve left operand
        Object leftValue;
        if (leftOperand instanceof String && tuple.getSchema().contains((String) leftOperand)) {
            leftValue = tuple.get((String) leftOperand);
        } else {
            leftValue = leftOperand;
        }

        // STEP 2: Resolve right operand
        Object rightValue;
        if (rightOperand instanceof String && tuple.getSchema().contains((String) rightOperand)) {
            rightValue = tuple.get((String) rightOperand);
        } else {
            rightValue = rightOperand;
        }

        // STEP 3: Compare

        if (leftValue instanceof Number && rightValue instanceof Number) {
            double l = ((Number) leftValue).doubleValue();
            double r = ((Number) rightValue).doubleValue();
        
            if (operator.equals("=")) {
                return l == r;
            } else if (operator.equals("!=")) {
                return l != r;
            } else if (operator.equals(">")) {
                return l > r;
            } else if (operator.equals(">=")) {
                return l >= r;
            } else if (operator.equals("<")) {
                return l < r;
            } else if (operator.equals("<=")) {
                return l <= r;
            }
        } else {
            String l = leftValue.toString();
            String r = rightValue.toString();
        
            if (operator.equals("=")) {
                return l.equals(r);
            } else if (operator.equals("!=")) {
                return !l.equals(r);
            } else if (operator.equals(">")) {
                return l.compareTo(r) > 0;
            } else if (operator.equals(">=")) {
                return l.compareTo(r) >= 0;
            } else if (operator.equals("<")) {
                return l.compareTo(r) < 0;
            } else if (operator.equals("<=")) {
                return l.compareTo(r) <= 0;
            }
        }
    
        return false;
    }

    // DO NOT REMOVE these functions! ---
    @Override
    public String toString() {
        return "ComparisonPredicate[" +
                "leftOperand=" + leftOperand +
                ", operator='" + operator + '\'' +
                ", rightOperand=" + rightOperand +
                ']';
    }
    public Object getLeftOperand() {
        return leftOperand;
    }

    public String getOperator() {
        return operator;
    }
    public Object getRightOperand() {
        return rightOperand;
    }

}
