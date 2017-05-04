package org.shadowmask.cache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.shadowmask.utils.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PoolByKey<KEY, OBJECT> {

  private Logger logger = LoggerFactory.getLogger(this.getClass());


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

  {
    new Thread(
        new Runnable() {
          @Override
          public void run() {
            while (true) {
              TimeUtil.pause(1000L * 60 * 10);
              try {
                for (KEY k : cache.keySet()) {
                  LinkedList<OBJECT> objects = cache.get(k);
                  if (objects != null) {
                    for (OBJECT o : objects) {
                      synchronized (this) {
                        PoolByKey.this.touch(o);
                      }
                    }
                  }
                }
              } catch (Exception e) {
                logger.info(e.getMessage(), e);
              }

            }
          }
        }
    ).start();


  }

  protected abstract OBJECT getObjectFromKey(KEY k);

  protected void touch(OBJECT o) {

  }

}
