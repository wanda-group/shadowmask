package org.shadowmask.cache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public abstract class PoolByKey<KEY, OBJECT> {

  Map<KEY, LinkedList<OBJECT>> cache = new HashMap<>();

  /**
   * borrow an object
   */
  public synchronized OBJECT borrow(KEY key) {
    LinkedList<OBJECT> objects = cache.get(key);
    if (objects == null || objects.size() == 0) {
      objects = new LinkedList<>();
      cache.put(key, objects);
      return getObjectFromKey(key);
    } else {
      return objects.remove();
    }

  }

  /**
   * release an object
   */
  public synchronized void release(KEY key, OBJECT object) {
    LinkedList<OBJECT> objects = cache.get(key);
    objects.add(object);
  }

  protected abstract OBJECT getObjectFromKey(KEY k);

}
