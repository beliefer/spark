package org.apache.spark.sql.types;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Marks methods including static methods and fields as being part of version 1 HiveDecimal.
 *
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface HiveDecimalVersionV1 {

}
