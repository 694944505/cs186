package edu.berkeley.cs186.database.concurrency;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    final static HashMap<LockType, HashSet<LockType>> compatibleLocks = new HashMap<LockType, HashSet<LockType>>() {{
            put(LockType.S, new HashSet<LockType>() {{  add(LockType.S); 
                                                        add(LockType.IS);
                                                        add(LockType.NL);}});
            put(LockType.X, new HashSet<LockType>() {{  add(LockType.NL);}});
            put(LockType.IS, new HashSet<LockType>() {{ add(LockType.S);
                                                        add(LockType.IS);
                                                        add(LockType.IX);
                                                        add(LockType.SIX);
                                                        add(LockType.NL);}});
            put(LockType.IX, new HashSet<LockType>() {{ add(LockType.IS); 
                                                        add(LockType.IX);
                                                        add(LockType.NL);}});
            put(LockType.SIX, new HashSet<LockType>() {{add(LockType.IS);
                                                        add(LockType.NL);}});
            put(LockType.NL, new HashSet<LockType>() {{ add(LockType.S);
                                                        add(LockType.X);
                                                        add(LockType.IS);
                                                        add(LockType.IX);
                                                        add(LockType.SIX);
                                                        add(LockType.NL);}});
                    
    }};
    final static HashMap<LockType, HashSet<LockType>> substitutableLocks = new HashMap<LockType, HashSet<LockType>>() {{
        put(LockType.S, new HashSet<LockType>() {{  add(LockType.S); 
                                                    add(LockType.X);
                                                    add(LockType.SIX);}});
        put(LockType.X, new HashSet<LockType>() {{  add(LockType.X);}});
        put(LockType.IS, new HashSet<LockType>() {{ add(LockType.S);
                                                    add(LockType.X);
                                                    add(LockType.IS);
                                                    add(LockType.IX);
                                                    add(LockType.SIX);}});
        put(LockType.IX, new HashSet<LockType>() {{ add(LockType.X); 
                                                    add(LockType.IX);
                                                    add(LockType.SIX);}});
        put(LockType.SIX, new HashSet<LockType>() {{add(LockType.SIX);
                                                    add(LockType.X);}});
        put(LockType.NL, new HashSet<LockType>() {{ add(LockType.S);
                                                    add(LockType.X);
                                                    add(LockType.IS);
                                                    add(LockType.IX);
                                                    add(LockType.SIX);
                                                    add(LockType.NL);}});
    }};
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        if(compatibleLocks.get(a).contains(b)) {
            return true;
        }

        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns the lock combine of two locks.
     * @param lockType1
     * @param lockType2
     * @return the lock combine of two locks.
     */
    public static LockType combine(LockType lockType1, LockType lockType2) {
        if (substitutable(lockType1, lockType2)) {
            return lockType1;
        }
        if (substitutable(lockType2, lockType1)) {
            return lockType2;
        }
        return SIX;
    }
    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        LockType testParentLock = parentLock(childLockType);
        if (testParentLock == parentLockType) {
            return true;
        }
        if (substitutable(parentLockType, testParentLock)) {
            return true;
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        if(substitutableLocks.get(required).contains(substitute)) {
            return true;
        }

        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

