package in.ac.iitd.db362.optimizer;

import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.api.PlanPrinter;
import in.ac.iitd.db362.operators.Operator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A basic optimizer implementation. Feel free and be creative in designing your optimizer.
 * Do not change the constructor. Use the catalog for various statistics that are available.
 * For everything in your optimization logic, you are free to do what ever you want.
 * Make sure to write efficient code!
 */
public class BasicOptimizer implements Optimizer {

    // Do not remove or rename logger
    protected final Logger logger = LogManager.getLogger(this.getClass());

    // Do not remove or rename catalog. You'll need it in your optimizer
    private final Catalog catalog;

    /**
     * DO NOT CHANGE THE CONSTRUCTOR!
     *
     * @param catalog
     */
    public BasicOptimizer(Catalog catalog) {
        this.catalog = catalog;
    }

    /**
     * Basic optimization that currently does not modify the plan. Your goal is to come up with
     * an optimization strategy that should find an optimal plan. Come up with your own ideas or adopt the ones
     * discussed in the lecture to efficiently enumerate plans, a search strategy along with a cost model.
     *
     * @param plan The original query plan.
     * @return The (possibly) optimized query plan.
     */
    @Override
    public Operator optimize(Operator plan) {
        logger.info("Optimizing Plan:\n{}", PlanPrinter.getPlanString(plan));
        // TODO: Implement me!
        // For now, we simply return the plan unmodified.
        return plan;
    }
}
