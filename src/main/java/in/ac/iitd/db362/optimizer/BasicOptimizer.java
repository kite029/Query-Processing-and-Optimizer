package in.ac.iitd.db362.optimizer;

import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.api.PlanPrinter;
import in.ac.iitd.db362.operators.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BasicOptimizer implements Optimizer {

    protected final Logger logger = LogManager.getLogger(this.getClass());
    private final Catalog catalog;

    public BasicOptimizer(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public Operator optimize(Operator plan) {
        logger.info("Optimizing Plan:\n{}", PlanPrinter.getPlanString(plan));
        return pushDownFilters(plan);
    }

    private Operator pushDownFilters(Operator operator) {
        if (operator instanceof FilterOperator) {
            FilterOperator filter = (FilterOperator) operator;
            Operator child = filter.getChild();
            Operator newChild = pushDownFilters(child);

            // Special handling if child is a Join
            if (newChild instanceof JoinOperator) {
                JoinOperator join = (JoinOperator) newChild;

                // Check if the filter can be pushed down to one side
                if (canApplyFilterToOperator(filter.getPredicate(), join.getLeftChild())) {
                    Operator newLeft = pushDownFilters(new FilterOperator(join.getLeftChild(), filter.getPredicate()));
                    return new JoinOperator(newLeft, join.getRightChild(), join.getPredicate());
                } else if (canApplyFilterToOperator(filter.getPredicate(), join.getRightChild())) {
                    Operator newRight = pushDownFilters(new FilterOperator(join.getRightChild(), filter.getPredicate()));
                    return new JoinOperator(join.getLeftChild(), newRight, join.getPredicate());
                }
            }

            // If not a join or cannot pushdown, rebuild the FilterOperator
            return new FilterOperator(newChild, filter.getPredicate());
        } 
        else if (operator instanceof ProjectOperator) {
            ProjectOperator project = (ProjectOperator) operator;
            Operator newChild = pushDownFilters(project.getChild());
            return new ProjectOperator(newChild, project.getColumns());
        } 
        else if (operator instanceof JoinOperator) {
            JoinOperator join = (JoinOperator) operator;
            Operator newLeft = pushDownFilters(join.getLeftChild());
            Operator newRight = pushDownFilters(join.getRightChild());
            return new JoinOperator(newLeft, newRight, join.getPredicate());
        } 
        else {
            // Base operator (Scan, TableScan, etc.), just return it
            return operator;
        }
    }

    private boolean canApplyFilterToOperator(Predicate predicate, Operator operator) {
        // Very simple heuristic: if all columns used in predicate exist in this operator's output schema
        if (predicate instanceof ComparisonPredicate) {
            ComparisonPredicate cmp = (ComparisonPredicate) predicate;
            String column = cmp.getColumn();
            return operator.getOutputSchema().contains(column);
        }
        // For more complex predicates, you'd check all columns involved.
        return false;
    }
}

