/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import java.util.List;
import java.util.Stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TreeLock is a lock that manages the lock process of the resource path. It will lock the whole
 * path from root to the resource path.
 *
 * <p>Assuming we need to alter the table `metalake.catalog.db1.table1`, the lock manager will lock
 * the following
 *
 * <pre>
 *   /                                    readLock
 *   /metalake                            readLock
 *   /metalake/catalog                    readLock
 *   /metalake/catalog/db1                readLock
 *   /metalake/catalog/db/table1          writeLock
 * </pre>
 *
 * If the lock manager fails to lock the resource path, it will release all the locks that have been
 * locked in the inverse sequences it locks the resource path.
 */
public class TreeLock {
  public static final Logger LOG = LoggerFactory.getLogger(TreeLock.class);

  private final List<TreeLockNode> lockNodes;
  private final Stack<TreeLockNode> heldLocks = new Stack<>();
  private LockType lockType;

  TreeLock(List<TreeLockNode> lockNodes) {
    this.lockNodes = lockNodes;
  }

  /**
   * Lock the tree lock with the given lock type.
   *
   * @param lockType The lock type to lock the tree lock.
   */
  public void lock(LockType lockType) {
    this.lockType = lockType;

    int length = lockNodes.size();
    for (int i = 0; i < length; i++) {
      TreeLockNode treeLockNode = lockNodes.get(i);
      LockType type = i == length - 1 ? lockType : LockType.READ;
      treeLockNode.lock(type);
      heldLocks.push(treeLockNode);
    }

    LOG.trace("Locked the tree lock: [{}], lock type: {}", lockNodes, lockType);
  }

  /** Unlock the tree lock. */
  public void unlock() {
    if (lockType == null) {
      throw new IllegalStateException("You must lock the tree lock before unlock it.");
    }

    boolean lastNode = false;
    TreeLockNode current = null;
    try {
      while (!heldLocks.isEmpty()) {
        LockType type;
        if (!lastNode) {
          lastNode = true;
          type = lockType;
        } else {
          type = LockType.READ;
        }

        current = heldLocks.peek();
        current.unlock(type);
        heldLocks.pop();
      }
    } catch (Exception e) {
      LOG.error("Can't release the locks: {}", current);
      throw new IllegalStateException(
          String.format("Locks %s are not released properly...", current));
    } finally {
      lockNodes.forEach(TreeLockNode::decReference);
    }

    LOG.trace("Unlocked the tree lock: [{}], lock type: {}", lockNodes, lockType);
  }
}
