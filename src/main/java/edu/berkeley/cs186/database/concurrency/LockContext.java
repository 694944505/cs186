package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    protected final Map<Long, Integer> numChildLocks; // why can use directyly?

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
        if (readonly) {
            throw new UnsupportedOperationException("Context is readonly");
        }
        if ((lockType == LockType.IS || lockType == LockType.S) && hasSIXAncestor(transaction)) {
            throw new InvalidLockException("Cannot acquire IS or IX lock with a SIX ancestor");
        }
        if (parent != null) {
            if (!LockType.substitutable(parent.getExplicitLockType(transaction), 
                    LockType.parentLock(lockType))) {
                throw new InvalidLockException("Cannot acquire lock of type " + lockType + " when parent lock type is " + parent.getExplicitLockType(transaction));
            }
        }
        lockman.acquire(transaction, name, lockType);
        if (parent != null) {
            parent.numChildLocks.compute(transaction.getTransNum(), (key, value) -> value != null ? value + 1 : 1);
        }
        return;
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
        if (readonly) {
            throw new UnsupportedOperationException("Context is readonly");
        }
        if (getNumChildren(transaction) > 0) {
            throw new InvalidLockException("Cannot release lock with children");
        } 
        lockman.release(transaction, name);
        if(parent != null) parent.numChildLocks.compute(transaction.getTransNum(), (key, value) -> value - 1);
        return;
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
        if (readonly) {
            throw new UnsupportedOperationException("Context is readonly");
        }
        if (parent != null) {
            if (!LockType.substitutable(parent.getExplicitLockType(transaction), 
                    LockType.parentLock(newLockType))) {
                throw new InvalidLockException("Cannot promote lock of type " + newLockType + " when parent lock type is " + parent.getExplicitLockType(transaction));
            }
        }
        LockType oldLockType = getExplicitLockType(transaction);
        //LockType parentLockType = (parent == null) ? LockType.NL : parent.getExplicitLockType(transaction); 
        
        EnumSet<LockType> validOldLockTypes = EnumSet.of(LockType.IS, LockType.IX, LockType.S);
        
        if (newLockType == LockType.SIX ) {
            if ((!validOldLockTypes.contains(oldLockType) || hasSIXAncestor(transaction))) {
                throw new InvalidLockException("Invalid Lock");
            }
        } 
        if (newLockType == LockType.SIX || newLockType == LockType.S) { // why have S
            List<ResourceName> sisDescendants = sisDescendants(transaction);
            numChildLocks.compute(transaction.getTransNum(), (key, value) -> value == null ? 0 : value - sisDescendants.size());
            sisDescendants.add(name);
            lockman.acquireAndRelease(transaction, name, newLockType, sisDescendants);
        } else {
            lockman.promote(transaction, name, newLockType);
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
        if (readonly) {
            throw new UnsupportedOperationException("Context is readonly");
        }
        LockType lockType = getExplicitLockType(transaction);
        if (lockType == LockType.NL) {
            throw new NoLockHeldException("No lock held");
        }
        if (lockType == LockType.S || lockType == LockType.X) {
            return;
        }
        List<ResourceName> descendants = allDescendants(transaction);
        if (hasXDescendant(transaction)) {
            lockman.acquireAndRelease(transaction, name, LockType.X, descendants);
        } else {
            lockman.acquireAndRelease(transaction, name, LockType.S, descendants);
        }
        numChildLocks.compute(transaction.getTransNum(), (key, value) -> value == null ? 0 : value - descendants.size());
        return;
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
        LockType explicitLockType = getExplicitLockType(transaction);
        if (explicitLockType != LockType.NL) {
            return explicitLockType;
        }
        if (parent == null) {
            return LockType.NL;
        }
        LockType parentLockType = parent.getEffectiveLockType(transaction);
        if (parentLockType == LockType.SIX) {
            return LockType.S;
        }
        if (parentLockType == LockType.NL) {
            return parent.getEffectiveLockType(transaction);
        }
        if (parentLockType == LockType.IS || parentLockType == LockType.IX) {
            return LockType.NL;
        }
        return parentLockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        if (parent == null) {
            return false;
        }
        if (parent.getExplicitLockType(transaction) == LockType.SIX) {
            return true;
        }
        return parent.hasSIXAncestor(transaction);
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
        List<ResourceName> sisDescendants = new ArrayList<>();
        for (LockContext child : children.values()) {
            if (child.getExplicitLockType(transaction) == LockType.S ||
                    child.getExplicitLockType(transaction) == LockType.IS) {
                        sisDescendants.add(child.name);
            }
            sisDescendants.addAll(child.sisDescendants(transaction));
        }
        return sisDescendants;
    }

    /**
     * Helper method to get a list of resourceNames of all locks are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants 
     */
    private List<ResourceName> allDescendants(TransactionContext transaction) {
        List<ResourceName> allDescendants = new ArrayList<>();
        for (LockContext child : children.values()) {
            if(child.getExplicitLockType(transaction) != LockType.NL) {
                allDescendants.add(child.name);
            }
            allDescendants.addAll(child.allDescendants(transaction));
        }
        return allDescendants;
    }
    /**
     * Helper method to see if the descendant of current context has a X lock
     */
    private boolean hasXDescendant(TransactionContext transaction) {
        if (getExplicitLockType(transaction) == LockType.X
                    || getExplicitLockType(transaction) == LockType.SIX
                    || getExplicitLockType(transaction) == LockType.IX) {
                return true;
        }
        for (LockContext child : children.values()) {
            if (child.hasXDescendant(transaction)) {
                return true;
            }
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
    /**
     * release all locks held by the transaction
     */
    public void releaseAllLocks(TransactionContext transaction) {
        if (transaction == null) return;
        for (LockContext child : children.values()) {
            child.releaseAllLocks(transaction);
        }
        if(getExplicitLockType(transaction)!=LockType.NL) release(transaction);
    }
}

