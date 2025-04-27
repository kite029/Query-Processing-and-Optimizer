package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

import java.util.*;

/**
 * The join operator performs a Hash Join.
 * TODO: Implement the open(), next(), and close() methods.
 *
 * Do not change the constructor and member variables or getters
 * Do not remove logging! otherwise your test cases will fail!
 */
public class JoinOperator extends OperatorBase implements Operator {
    private Operator leftChild;
    private Operator rightChild;
    private JoinPredicate predicate;

   
    // For hash join implementation
    private Map<Object, List<Tuple>> hashTable;
    private Iterator<Tuple> leftChildIterator;
    private List<Tuple> currentMatches;
    private Tuple currentLeftTuple;
    private boolean isBuildPhaseDone;

    public JoinOperator(Operator leftChild, Operator rightChild, JoinPredicate predicate) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.predicate = predicate;
    }

    @Override
    public void open() {
        // Do not remove logging--
        logger.trace("Open()");
        // ----------------------
                
        // Initialize the hash table
        hashTable = new HashMap<>();
                
        // Open both children
        leftChild.open();
        rightChild.open();

        // Start with build phase
        isBuildPhaseDone = false;
        currentMatches = null;
        currentLeftTuple = null;
        
    }

    @Override
    public Tuple next() {
        // Do not remove logging--
        logger.trace("Next()");
        // ----------------------

        //TODO: Implement me!
        // Build phase: build hash table from the right child
        if (!isBuildPhaseDone) {
            Tuple rightTuple;
            while ((rightTuple = rightChild.next()) != null) {
                Object joinKey = rightTuple.get(((EqualityJoinPredicate)predicate).getRightColumn());
                if (!hashTable.containsKey(joinKey)) {
                    hashTable.put(joinKey, new ArrayList<>());
                }
                hashTable.get(joinKey).add(rightTuple);
            }
            isBuildPhaseDone = true;
            leftChildIterator = new Iterator<Tuple>() {
                Tuple nextTuple = leftChild.next();
                
                @Override
                public boolean hasNext() {
                    return nextTuple != null;
                }
                
                @Override
                public Tuple next() {
                    Tuple current = nextTuple;
                    nextTuple = leftChild.next();
                    return current;
                }
            };
        }
        // Probe phase: join left child tuples with right child hash table
        while (true) {
            // If we have matches from previous probe, return the next one
            if (currentMatches != null && !currentMatches.isEmpty()) {
                Tuple rightTuple = currentMatches.remove(0);
                List<Object> joinedValues = new ArrayList<>();
                joinedValues.addAll(currentLeftTuple.getValues());
                joinedValues.addAll(rightTuple.getValues());
                
                List<String> joinedSchema = new ArrayList<>();
                joinedSchema.addAll(currentLeftTuple.getSchema());
                joinedSchema.addAll(rightTuple.getSchema());
                
                if (currentMatches.isEmpty()) {
                    currentMatches = null;
                }
                return new Tuple(joinedValues, joinedSchema);
            }
            
            // Get next left tuple if no current matches
            if (!leftChildIterator.hasNext()) {
                return null; // No more tuples
            }
            
            currentLeftTuple = leftChildIterator.next();
            Object joinKey = currentLeftTuple.get(((EqualityJoinPredicate)predicate).getLeftColumn());
            currentMatches = hashTable.getOrDefault(joinKey, Collections.emptyList());
            
            // If no matches, continue to next left tuple
            if (currentMatches.isEmpty()) {
                continue;
            }
            
            // Return the first match
            Tuple rightTuple = currentMatches.remove(0);
            List<Object> joinedValues = new ArrayList<>();
            joinedValues.addAll(currentLeftTuple.getValues());
            joinedValues.addAll(rightTuple.getValues());
            
            List<String> joinedSchema = new ArrayList<>();
            joinedSchema.addAll(currentLeftTuple.getSchema());
            joinedSchema.addAll(rightTuple.getSchema());
            
            if (currentMatches.isEmpty()) {
                currentMatches = null;
            }
            return new Tuple(joinedValues, joinedSchema);
        }
    }

    

    @Override
    public void close() {
        // Do not remove logging ---
        logger.trace("Close()");
        // ------------------------
        // Close both children
        leftChild.close();
        rightChild.close();
               
        // Clean up resources
        hashTable = null;
        leftChildIterator = null;
        currentMatches = null;
        currentLeftTuple = null;
    }


    // Do not remove these methods!
    public Operator getLeftChild() {
        return leftChild;
    }

    public Operator getRightChild() {
        return rightChild;
    }

    public JoinPredicate getPredicate() {
        return predicate;
    }
}
