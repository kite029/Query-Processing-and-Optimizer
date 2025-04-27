package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

/**
 * The filter operator produces tuples that satisfy the predicate
 * TODO: Implement the open(), next(), and close() methods.
 *
 * Do not change the constructor and member variables or getters
 * Do not remove logging! otherwise your test cases will fail!
 */
public class FilterOperator extends OperatorBase implements Operator {
    private Operator child;
    private Predicate predicate;

    public FilterOperator(Operator child, Predicate predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override
    public void open() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Open()");
        // ------------------------

        // Open the child operator
        child.open();
    }

    @Override
    public Tuple next() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Next()");
        // -------------------------

        //TODO: Implement me!
        Tuple nextTuple;  // Declare a variable to hold the next tuple.

        while (true) {
            nextTuple = child.next();  // Get the next tuple from the child operator.

            if (nextTuple == null) {  // If there are no more tuples (nextTuple is null), stop the loop.
                break;
            }

            // Process the tuple (e.g., check if it satisfies the filter condition).
            if (predicate.evaluate(nextTuple)) {
                return nextTuple;  // If the tuple matches the predicate, return it.
            }
        }
        return null;
    }

    @Override
    public void close() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Close()");
        // -------------------------
        //TODO: Implement me!
        child.close();
    }


    // Do not remove these methods!
    public Operator getChild() {
        return child;
    }

    public Predicate getPredicate() {
        return predicate;
    }
}
