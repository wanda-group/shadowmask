package org.shadowmask.core.mask.rules.suppressor;

/**
 * Suppressor is used to anonymize data, by hiding all the information inside data. It's a
 * special case of {@link org.shadowmask.core.mask.rules.generalizer.Generalizer}
 * @param <In> input data type.
 * @param <OUT> output data type.
 */
public interface Suppressor<In, OUT> {

  /**
   * transfer the input data into something else which would not explore any information
   * of input data.
   */
  OUT suppress(In input);
}
