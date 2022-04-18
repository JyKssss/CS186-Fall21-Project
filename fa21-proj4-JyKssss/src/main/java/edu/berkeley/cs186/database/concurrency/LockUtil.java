package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement

        if (requestType.equals(LockType.NL)){
            return;
        }

        if (LockType.substitutable(effectiveLockType, requestType)){
            return;
        }

        if (effectiveLockType.equals(LockType.NL)){
            List<LockContext> ancestorsList = getAncestor(parentContext);

            if (requestType.equals(LockType.S)){
                for (LockContext ancestorContext : ancestorsList) {
                    if (ancestorContext.getExplicitLockType(transaction).equals(LockType.NL)){
                        ancestorContext.acquire(transaction, LockType.IS);
                    }
                }

                if (explicitLockType.equals(LockType.NL)){
                    lockContext.acquire(transaction, LockType.S);
                }
            }
            else {
                for (LockContext ancestorContext : ancestorsList) {
                    if (ancestorContext.getExplicitLockType(transaction).equals(LockType.NL)){
                        ancestorContext.acquire(transaction, LockType.IX);
                    }
                    else if (ancestorContext.getExplicitLockType(transaction).equals(LockType.IS)){
                        ancestorContext.promote(transaction, LockType.IX);
                    }
                }

                if (explicitLockType.equals(LockType.NL)){
                    lockContext.acquire(transaction, LockType.X);
                }
            }

        }

        if (requestType.equals(LockType.S) && effectiveLockType.equals(LockType.IX)){

            lockContext.promote(transaction, LockType.SIX);
        }

        if (explicitLockType.equals(LockType.IS)){
            if (requestType.equals(LockType.S)){
                lockContext.escalate(transaction);
            }
            if (requestType.equals(LockType.X)){
                List<LockContext> ancestorsList = getAncestor(parentContext);
                lockContext.escalate(transaction);
                lockContext.promote(transaction, LockType.X);
                for (LockContext context : ancestorsList) {
                    if (context.getExplicitLockType(transaction).equals(LockType.IS)){
                        context.promote(transaction, LockType.IX);
                    }
                    if (context.getExplicitLockType(transaction).equals(LockType.NL)){
                        lockContext.acquire(transaction, LockType.IX);
                    }
                }
            }
        }

        if (explicitLockType.equals(LockType.IX)){
            if (requestType.equals(LockType.X)){
                lockContext.escalate(transaction);
            }
        }

        if (explicitLockType.equals(LockType.S)){
            if (requestType.equals(LockType.X)){
                List<LockContext> ancestorsList = getAncestor(parentContext);

                for (LockContext context : ancestorsList) {
                    if (context.getExplicitLockType(transaction).equals(LockType.IS)){
                        context.promote(transaction, LockType.IX);
                    }
                    if (context.getExplicitLockType(transaction).equals(LockType.NL)){
                        lockContext.acquire(transaction, LockType.IX);
                    }
                }
                lockContext.promote(transaction, LockType.X);
            }
        }
        return;
    }

    // TODO(proj4_part2) add any helper methods you want


    private static List<LockContext> getAncestor(LockContext parentContext){
        List<LockContext> ancestorsList =new ArrayList<>();
        while (parentContext != null){
            ancestorsList.add(parentContext);
            parentContext = parentContext.parentContext();
        }
        Collections.reverse(ancestorsList);
        return ancestorsList;
    }
}
