package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (readonly){
            throw new UnsupportedOperationException("the context is readonly");
        }

        if ((lockType.equals(LockType.IS) || lockType.equals(LockType.S)) && hasSIXAncestor(transaction)){
            throw new InvalidLockException("cannot acquire IS/S lock on SIX ancestor");
        }
        if (!getExplicitLockType(transaction).equals(LockType.NL)){
            throw new DuplicateLockRequestException("a lock has been acquired by context");
        }
        if (!LockType.canBeParentLock(getParentMeLockType(transaction), lockType) && !getParentMeLockType(transaction).equals(LockType.NL)){
            throw new InvalidLockException("two locks have conflict");
        }



        lockman.acquire(transaction, name, lockType);
        LockContext parent = parentContext();
        addParentChild(transaction.getTransNum(), parent);

    }

    private void addParentChild(Long transaction, LockContext parent){
        if (parent == null){
            return;
        }
        parent.numChildLocks.put(transaction, parent.numChildLocks.getOrDefault(transaction, 0) + 1);
        addParentChild(transaction, parent.parentContext());
    }

    public LockType getParentMeLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;

        if (!getExplicitLockType(transaction).equals(LockType.NL)){
            return getExplicitLockType(transaction);
        }
        LockContext ancestorContext = parentContext();
        LockType aLockType = LockType.NL;
        while (ancestorContext != null){
            aLockType = ancestorContext.getExplicitLockType(transaction);
            if (!aLockType.equals(LockType.NL)){
                break;
            }
            else {
                ancestorContext = ancestorContext.parentContext();
            }
        }

        return aLockType;

    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly){
            throw new UnsupportedOperationException("the context is readonly");
        }
        if (getNumChildren(transaction) > 0){
            throw new InvalidLockException("lock cannot be released: violate multigranularity");
        }
        if (lockman.getLockType(transaction, name).equals(LockType.NL)){
            throw new NoLockHeldException("No lock is held by transaction");
        }

        lockman.release(transaction, name);
        delParentChild(transaction.getTransNum(), parentContext());
        return;
    }

    private void delParentChild(Long transaction, LockContext parent){
        if (parent == null){
            return;
        }
        Map<Long, Integer> numChildLocks = parent.numChildLocks;
        if (numChildLocks.get(transaction) > 1){
            numChildLocks.put(transaction, numChildLocks.get(transaction) - 1);
        }
        else {
            numChildLocks.remove(transaction);
        }
        delParentChild(transaction, parent.parentContext());
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly){
            throw new UnsupportedOperationException("the context is readonly");
        }
        if (getExplicitLockType(transaction).equals(newLockType)){
            throw new DuplicateLockRequestException("a same lock has been acquired by context");
        }

        if (getExplicitLockType(transaction).equals(LockType.NL)){
            throw new NoLockHeldException("No lock is held by transaction");
        }

        LockType curLockType = lockman.getLockType(transaction, name);

        if (newLockType.equals(LockType.SIX)){
            if (hasSIXAncestor(transaction)){
                throw new InvalidLockException("old lock cannot be substituted or ancestor has SIX lock");
            }
            else if (LockType.substitutable(newLockType, curLockType)){
                List<ResourceName> sisChildrenList = sisDescendants(transaction);
                sisChildrenList.add(this.name);
                lockman.acquireAndRelease(transaction, name, newLockType, sisChildrenList);
                List<LockContext> childrenContexts = new ArrayList<>();
                for (ResourceName childName : sisChildrenList) {
                    childrenContexts.add(fromResourceName(lockman, childName));
                }
                for (LockContext context : childrenContexts) {
                    context.delParentChild(transaction.getTransNum(), context.parent);
                }
            }
            else if (curLockType.equals(LockType.IX) || curLockType.equals(LockType.IS) || curLockType.equals(LockType.S)){
                List<ResourceName> sisChildrenList = sisDescendants(transaction);
                sisChildrenList.add(this.name);
                lockman.acquireAndRelease(transaction, name, newLockType, sisChildrenList);
                List<LockContext> childrenContexts = new ArrayList<>();
                for (ResourceName childName : sisChildrenList) {
                    childrenContexts.add(fromResourceName(lockman, childName));
                }
                for (LockContext context : childrenContexts) {
                    context.delParentChild(transaction.getTransNum(), context.parent);
                }
            }
        }
        else {
            if (LockType.substitutable(newLockType, curLockType)){
                lockman.promote(transaction, name, newLockType);
            }
            else {
                throw new InvalidLockException("old lock cannot be substituted");
            }
        }

        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly){
            throw new UnsupportedOperationException("the context is readonly");
        }
        if (getExplicitLockType(transaction).equals(LockType.NL)){
            throw new NoLockHeldException("No lock is held by transaction");
        }
        LockType curLockType = lockman.getLockType(transaction, name);
        if (curLockType.equals(LockType.S) || curLockType.equals(LockType.X)){
            return;
        }

        List<LockContext> childrenLocks = getChildrenLock(transaction);
        List<ResourceName> childrenResources =  getChildrenResource(childrenLocks);

        if (curLockType.equals(LockType.IX) || curLockType.equals(LockType.SIX)){
            lockman.acquireAndRelease(transaction, name, LockType.X, childrenResources);
            for (LockContext childrenLock : childrenLocks) {
                delParentChild(transaction.getTransNum(), childrenLock.parentContext());
            }
        }
        else if (curLockType.equals(LockType.IS)){
            lockman.acquireAndRelease(transaction, name, LockType.S, childrenResources);
            for (LockContext childrenLock : childrenLocks) {
                delParentChild(transaction.getTransNum(), childrenLock.parentContext());
            }
        }
        return;
    }

    private List<LockContext> getChildrenLock(TransactionContext transaction){
        List<Lock> allLocks = lockman.getLocks(transaction);
        List<LockContext> childrenLocks = new ArrayList<>();
        LockContext parent = this;
        HashSet<LockContext> childrenSet = new HashSet<>();

        for (Lock lock : allLocks) {
            LockContext child = fromResourceName(lockman, lock.name);
            if (child != null && !childrenSet.contains(child) && checkDescdents(child, parent) && !child.equals(parent)){
                childrenSet.add(child);
                childrenLocks.add(child);
            }
        }
        childrenLocks.add(parent);
        return childrenLocks;
    }

    private List<ResourceName> getChildrenResource(List<LockContext> childrenLocks){
        List<ResourceName> childrecResourceList = new ArrayList<>();
        HashSet<ResourceName> set = new HashSet<>();
        for (LockContext childrenLock : childrenLocks) {
            if (set.contains(childrenLock.name)){
                continue;
            }
            childrecResourceList.add(childrenLock.name);
            set.add(childrenLock.name);
        }
        return childrecResourceList;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        if (!getExplicitLockType(transaction).equals(LockType.NL)){
            return getExplicitLockType(transaction);
        }
        LockContext ancestorContext = parentContext();
        LockType aLockType = LockType.NL;
        while (ancestorContext != null){
            aLockType = ancestorContext.getExplicitLockType(transaction);
            if (!aLockType.equals(LockType.NL)){
                break;
            }
            else {
                ancestorContext = ancestorContext.parentContext();
            }
        }
        if (aLockType.equals(LockType.IS) || aLockType.equals(LockType.IX)){
            return LockType.NL;
        }
        else if (aLockType.equals(LockType.SIX)){
            return LockType.S;
        }
        else {
            return aLockType;
        }
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        if (parentContext() == null){
            return false;
        }
        else {
            LockContext ancestorContext = parentContext();
            while (ancestorContext != null){
                if (ancestorContext.getExplicitLockType(transaction) == LockType.SIX){
                    return true;
                }
                else {
                    ancestorContext = ancestorContext.parentContext();
                }
            }
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> descdentList =new ArrayList<>();
        List<Lock> lockList = lockman.getLocks(transaction);
        for (Lock lock : lockList) {
            if (lock.lockType.equals(LockType.IS) || lock.lockType.equals(LockType.S)){
                LockContext curContext = fromResourceName(lockman, lock.name);
                if (!curContext.equals(this) && checkDescdents(curContext, this)){
                    descdentList.add(lock.name);
                }
            }
        }
        return descdentList;
    }


    private boolean checkDescdents(LockContext desc, LockContext ancestor){
        if (desc == null){
            return false;
        }
        while (desc != null){
            if (desc.equals(ancestor)){
                return true;
            }
            desc = desc.parentContext();
        }
        return false;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

