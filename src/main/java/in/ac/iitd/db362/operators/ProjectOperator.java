package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

import java.util.*;

/**
 * Implementation of a simple project operator that implements the operator interface.
 *
 *
 * TODO: Implement the open(), next(), and close() methods!
 * Do not change the constructor or existing member variables.
 */
public class ProjectOperator extends OperatorBase implements Operator {
    private Operator child;
    private List<String> projectedColumns;
    private boolean distinct;

    private Set<List<Object>> seenTuples = new HashSet<>(); // store projected values we've already returned


    /**
     * Project operator. If distinct is set to true, it does duplicate elimination
     * @param child
     * @param projectedColumns
     * @param distinct
     */
    public ProjectOperator(Operator child, List<String> projectedColumns, boolean distinct) {
        this.child = child;
        this.projectedColumns = projectedColumns;
        this.distinct = distinct;
    }

    @Override
    public void open() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Open()");
        // -------------------------
        child.open();
    }

    @Override
    public Tuple next() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Next()");
        // ------------------------

        //TODO: Implement me!

        Tuple nextTuple;


        while ((nextTuple = child.next()) != null) {
            List<Object> fullValues = nextTuple.getValues();
            List<String> fullSchema = nextTuple.getSchema();

            // Prepare projected values
            List<Object> projectedValues = new ArrayList<>();
            for (String col : projectedColumns) {
                int idx = fullSchema.indexOf(col);
                projectedValues.add(fullValues.get(idx));
            }

            // Skip if duplicate and distinct is true
            if (distinct && !seenTuples.add(projectedValues)) {
                continue;   // if duplicate comes and we have to report only distinct then do return the same tuple if returned before kepp looking for others.
            }

            return new Tuple(projectedValues, projectedColumns); // create and return projected tuple
        }
        return null;
    }

    @Override
    public void close() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Close()");
        // ------------------------
        // TODO: Implement me!
        child.close();
        seenTuples.clear();
        
    }

    // do not remvoe these methods!
    public Operator getChild() {
        return child;
    }

    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    public boolean isDistinct() {
        return distinct;
    }
}
