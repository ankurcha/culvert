package com.bah.culvert.index;

import com.bah.culvert.data.CKeyValue;
import com.bah.culvert.util.BaseConfigurable;
import com.bah.culvert.transactions.Put;

/**
 * Determine if a {@link CKeyValue} is accepted by an {@link Index} to be
 * indexed.
 */
public abstract class Acceptor extends BaseConfigurable {

  /**
   * Accept or reject a given key/value for indexing. After accepting a
   * {@link CKeyValue}, that object and all other accepted {@link CKeyValue
   * CKeyValues} will be passed as a {@link Put} to {@link Index#handlePut(Put)}
   * @param kv key/value to be indexed
   * @return <tt>true</tt> if the key/value should be passed to the index,
   *         <tt>false</tt> otherwise
   */
  public abstract boolean accept(CKeyValue kv);
  

  /**
   * Reject all key/values.
   * <p>
   * This is the default acceptor, if none can be found.
   */
  public static class AcceptNone extends Acceptor {

    @Override
    public boolean accept(CKeyValue kv) {
      return false;
    }

  }

}
