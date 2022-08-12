/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.types;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

import static org.apache.spark.sql.types.Int128.ONE;
import static org.apache.spark.sql.types.Int128.ZERO;
import static org.apache.spark.sql.types.Int128.MAX_VALUE;
import static org.apache.spark.sql.types.Int128.MIN_VALUE;
import static org.apache.spark.sql.types.Int128.add;
import static org.apache.spark.sql.types.Int128.addExact;
import static org.apache.spark.sql.types.Int128.and;
import static org.apache.spark.sql.types.Int128.compare;
import static org.apache.spark.sql.types.Int128.multiply;
import static org.apache.spark.sql.types.Int128.multiplyExact;
import static org.apache.spark.sql.types.Int128.or;
import static org.apache.spark.sql.types.Int128.subtract;
import static org.apache.spark.sql.types.Int128.subtractExact;
import static org.apache.spark.sql.types.Int128.xor;

public class TestInt128
{
    @Test
    public void testIsPositive()
    {
        Assert.assertTrue(ONE.isPositive());
        Assert.assertFalse(ZERO.isPositive());
        Assert.assertTrue(MAX_VALUE.isPositive());
        Assert.assertFalse(MIN_VALUE.isPositive());

        Assert.assertTrue(Int128.valueOf(1000).isPositive());
        Assert.assertFalse(Int128.valueOf(-1000).isPositive());
    }

    @Test
    public void testIsZero()
    {
        Assert.assertFalse(ONE.isZero());
        Assert.assertTrue(ZERO.isZero());
        Assert.assertFalse(MAX_VALUE.isZero());
        Assert.assertFalse(MIN_VALUE.isZero());

        Assert.assertFalse(Int128.valueOf(1000).isZero());
        Assert.assertFalse(Int128.valueOf(-1000).isZero());
    }

    @Test
    public void testIsNegative()
    {
        Assert.assertFalse(ONE.isNegative());
        Assert.assertFalse(ZERO.isNegative());
        Assert.assertFalse(MAX_VALUE.isNegative());
        Assert.assertTrue(MIN_VALUE.isNegative());

        Assert.assertFalse(Int128.valueOf(1000).isNegative());
        Assert.assertTrue(Int128.valueOf(-1000).isNegative());
    }

    @Test
    public void testCompareTo()
    {
        Assert.assertEquals(ONE.compareTo(ZERO), 1);

        Assert.assertEquals(ONE.compareTo(MIN_VALUE), 1);

        Assert.assertEquals(ONE.compareTo(MAX_VALUE), -1);

        Assert.assertEquals(ZERO.compareTo(MAX_VALUE), -1);

        Assert.assertEquals(MIN_VALUE.compareTo(MAX_VALUE), -1);

        Assert.assertEquals(MAX_VALUE.compareTo(MIN_VALUE), 1);

        Assert.assertEquals(ZERO.compareTo(ZERO), 0);

        Assert.assertEquals(ONE.compareTo(ONE), 0);

        Assert.assertEquals(MIN_VALUE.compareTo(MIN_VALUE), 0);

        Assert.assertEquals(MAX_VALUE.compareTo(MAX_VALUE), 0);

        Assert.assertEquals(Int128.valueOf(1).compareTo(Int128.valueOf(1)), 0);

        Assert.assertEquals(Int128.valueOf(-1).compareTo(Int128.valueOf(-1)), 0);
    }

    @Test
    public void testToString()
    {
        Assert.assertEquals(ZERO.toString(), "0");

        Assert.assertEquals(ONE.toString(), "1");

        Assert.assertEquals(MIN_VALUE.toString(), "-170141183460469231731687303715884105728");

        Assert.assertEquals(MAX_VALUE.toString(), "170141183460469231731687303715884105727");
    }

    @Test
    public void testFromBigEndian()
    {
        byte[] bytes;

        // less than 8 bytes
        bytes = new byte[] {0x1};
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(0x0000000000000000L, 0x0000000000000001L));
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(new BigInteger(bytes)));

        bytes = new byte[] {(byte) 0xFF};
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL));
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(new BigInteger(bytes)));

        // 8 bytes
        bytes = new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8};
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(0x0000000000000000L, 0x01_02_03_04_05_06_07_08L));
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(new BigInteger(bytes)));

        bytes = new byte[] {(byte) 0x80, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8};
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(0xFFFFFFFFFFFFFFFFL, 0x80_02_03_04_05_06_07_08L));
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(new BigInteger(bytes)));

        // more than 8 bytes, less than 16 bytes
        bytes = new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA};
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(0x000000000000_01_02L, 0x03_04_05_06_07_08_09_0AL));
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(new BigInteger(bytes)));

        bytes = new byte[] {(byte) 0x80, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA};
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(0xFFFFFFFFFFFF_80_02L, 0x03_04_05_06_07_08_09_0AL));
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(new BigInteger(bytes)));

        // 16 bytes
        bytes = new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x55};
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(0x01_02_03_04_05_06_07_08L, 0x09_0A_0B_0C_0D_0E_0F_55L));
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(new BigInteger(bytes)));

        bytes = new byte[] {(byte) 0x80, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x55};
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(0x80_02_03_04_05_06_07_08L, 0x09_0A_0B_0C_0D_0E_0F_55L));
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(new BigInteger(bytes)));

        // more than 16 bytes
        bytes = new byte[] {0x0, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x55};
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(0x01_02_03_04_05_06_07_08L, 0x09_0A_0B_0C_0D_0E_0F_55L));
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(new BigInteger(bytes)));

        bytes = new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0x80, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x55};
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(0x80_02_03_04_05_06_07_08L, 0x09_0A_0B_0C_0D_0E_0F_55L));
        Assert.assertEquals(Int128.fromBigEndian(bytes), Int128.valueOf(new BigInteger(bytes)));

        // overflow
        Assert.assertThrows(ArithmeticException.class, () -> Int128.fromBigEndian(new byte[] {0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}));
        Assert.assertThrows(ArithmeticException.class, () -> Int128.fromBigEndian(new byte[] {(byte) 0xFE, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}));
    }

    @Test
    public void testToBigInteger()
    {
        Assert.assertEquals(ZERO.toBigInteger(), BigInteger.ZERO);

        Assert.assertEquals(ONE.toBigInteger(), BigInteger.ONE);

        BigInteger bigInteger = BigInteger.valueOf(2).pow(127).subtract(BigInteger.ONE);

        Assert.assertEquals(MAX_VALUE.toBigInteger(), bigInteger);

        Assert.assertEquals(MIN_VALUE.toBigInteger(), bigInteger.negate().subtract(BigInteger.ONE));
    }

    @Test
    public void testInLongRange()
    {
        Assert.assertTrue(Int128.valueOf(Long.MAX_VALUE).inLongRange());

        Assert.assertTrue(Int128.valueOf(Long.MIN_VALUE).inLongRange());

        Assert.assertFalse(Int128.add(Int128.valueOf(Long.MAX_VALUE), ONE).inLongRange());

        Assert.assertFalse(Int128.subtract(Int128.valueOf(Long.MIN_VALUE), ONE).inLongRange());

        Assert.assertFalse(MAX_VALUE.inLongRange());

        Assert.assertFalse(MIN_VALUE.inLongRange());

        Assert.assertTrue(ZERO.inLongRange());
    }

    @Test
    public void testAdd()
    {
        Assert.assertEquals(add(Int128.valueOf(0), Int128.valueOf(0)), ZERO);

        Assert.assertEquals(add(MAX_VALUE, ONE), MIN_VALUE);

        Assert.assertEquals(add(ZERO, ONE), ONE);

        Assert.assertEquals(add(Int128.valueOf(Long.MAX_VALUE - 1), ONE), Int128.valueOf(Long.MAX_VALUE));

        Assert.assertEquals(add(Int128.valueOf(-1), ONE), ZERO);

        Assert.assertThrows(ArithmeticException.class, () -> addExact(MAX_VALUE, ONE));

        Assert.assertEquals(addExact(Int128.valueOf(Long.MIN_VALUE + 1), Int128.valueOf(-1)), Int128.valueOf(Long.MIN_VALUE));

        Assert.assertThrows(ArithmeticException.class, () -> addExact(MAX_VALUE, MAX_VALUE));
    }

    @Test
    public void testSubtract()
    {
        Assert.assertEquals(subtract(Int128.valueOf(0), Int128.valueOf(0)), ZERO);

        Assert.assertEquals(subtract(MIN_VALUE, ONE), MAX_VALUE);

        Assert.assertThrows(ArithmeticException.class, () -> subtractExact(MIN_VALUE, ONE));

        Assert.assertThrows(ArithmeticException.class, () -> subtractExact(MIN_VALUE, MAX_VALUE));

        Assert.assertEquals(subtractExact(Int128.valueOf(Long.MAX_VALUE - 1), Int128.valueOf(-1)), Int128.valueOf(Long.MAX_VALUE));
    }

    @Test
    public void testAbs()
    {
        Assert.assertEquals(Int128.abs(ZERO), ZERO);

        Assert.assertEquals(Int128.abs(ONE), ONE);

        Assert.assertEquals(Int128.abs(Int128.negate(ONE)), ONE);

        Assert.assertEquals(Int128.abs(MAX_VALUE), MAX_VALUE);

        Assert.assertEquals(Int128.abs(Int128.negate(MAX_VALUE)), MAX_VALUE);

        Assert.assertEquals(Int128.abs(MIN_VALUE), MIN_VALUE);

        Assert.assertThrows(ArithmeticException.class, () -> Int128.absExact(MIN_VALUE));
    }

    @Test
    public void testNegate()
    {
        Assert.assertEquals(Int128.negate(ZERO), ZERO);

        Assert.assertEquals(Int128.negateExact(ZERO), ZERO);

        Assert.assertEquals(Int128.negate(ONE), Int128.valueOf(-1));

        Assert.assertEquals(Int128.negateExact(ONE), Int128.valueOf(-1));

        Assert.assertEquals(Int128.negate(MAX_VALUE), Int128.valueOf(MAX_VALUE.toBigInteger().negate()));

        Assert.assertEquals(Int128.negateExact(MAX_VALUE), Int128.valueOf(MAX_VALUE.toBigInteger().negate()));

        Assert.assertEquals(Int128.negate(MIN_VALUE), MIN_VALUE);

        Assert.assertThrows(ArithmeticException.class, () -> Int128.negateExact(MIN_VALUE));
    }

    @Test
    public void testMultiply()
    {
        Assert.assertEquals(multiply(ZERO, ONE), ZERO);

        Assert.assertEquals(multiply(ZERO, Int128.negate(ONE)), ZERO);

        Assert.assertEquals(multiply(ONE, MAX_VALUE), MAX_VALUE);

        Assert.assertEquals(multiply(ONE, MIN_VALUE), MIN_VALUE);

        Assert.assertEquals(multiply(ONE, MAX_VALUE), MAX_VALUE);

        Assert.assertEquals(multiply(Int128.negate(ONE), MAX_VALUE), Int128.negate(MAX_VALUE));

        Assert.assertEquals(multiply(Int128.negate(ONE), MIN_VALUE), Int128.negate(MIN_VALUE));

        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(Int128.negate(ONE), MIN_VALUE));

        Assert.assertEquals(multiply(Int128.valueOf(Long.MAX_VALUE), Int128.valueOf(2)),
                Int128.valueOf(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2))));

        Assert.assertEquals(multiply(Int128.valueOf(Long.MAX_VALUE), Int128.valueOf(Long.MAX_VALUE)),
                Int128.valueOf(BigInteger.valueOf(Long.MAX_VALUE).pow(2)));

        Assert.assertEquals(multiply(Int128.valueOf(Long.MIN_VALUE), Int128.valueOf(Long.MIN_VALUE)),
                Int128.valueOf(BigInteger.valueOf(Long.MIN_VALUE).pow(2)));

        Assert.assertEquals(multiplyExact(Int128.valueOf("18446744073709551614"), Int128.valueOf(2)),
                Int128.valueOf("36893488147419103228"));
    }

    @Test
    public void testTwosComplementCorrectionsNoOverflow()
    {
        Assert.assertEquals(multiplyExact(
                new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0x0000000000000000L, 0x0000000000000002L)),
                new Int128(0x0000000000000001L, 0xFFFFFFFFFFFFFFFEL));

        Assert.assertEquals(multiplyExact(
                        new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFEL)),
                new Int128(0xFFFFFFFFFFFFFFFEL, 0x0000000000000002L));

        Assert.assertEquals(multiplyExact(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0x7FFFFFFFFFFFFFFFL),
                        new Int128(0x0000000000000000L, 0x0000000000000002L)),
                new Int128(0xFFFFFFFFFFFFFFFEL, 0xFFFFFFFFFFFFFFFEL));

        Assert.assertEquals(multiplyExact(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0x7FFFFFFFFFFFFFFFL),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFEL)),
                new Int128(0x0000000000000001L, 0x0000000000000002L));

        // Left fits in java long
        // Test bHigh vs aLow positive and negative combinations
        Assert.assertEquals(multiplyExact(
                        new Int128(0x0000000000000000L, 0x0000000000000002L),
                        new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL)),
                new Int128(0x0000000000000001L, 0xFFFFFFFFFFFFFFFEL));

        Assert.assertEquals(multiplyExact(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFEL),
                        new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL)),
                new Int128(0xFFFFFFFFFFFFFFFEL, 0x0000000000000002L));

        Assert.assertEquals(multiplyExact(
                        new Int128(0x0000000000000000L, 0x0000000000000002L),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0x7FFFFFFFFFFFFFFFL)),
                new Int128(0xFFFFFFFFFFFFFFFEL, 0xFFFFFFFFFFFFFFFEL));

        Assert.assertEquals(multiplyExact(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFEL),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0x7FFFFFFFFFFFFFFFL)),
                new Int128(0x0000000000000001L, 0x0000000000000002L));
    }

    @Test
    public void testTwosComplementCorrectionsOverflow()
    {
        // Right fits in java long
        // Test aHigh vs bLow positive and negative combinations
        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                new Int128(0x0000000000000000L, 0x0000000000000002L)));

        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0x8000000000000000L, 0x7FFFFFFFFFFFFFFFL),
                new Int128(0x0000000000000000L, 0x0000000000000002L)));

        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0x8000000000000000L, 0x7FFFFFFFFFFFFFFFL),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFEL)));

        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFEL)));

        // Left fits in java long
        // Test bHigh vs aLow positive and negative combinations
        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0x0000000000000000L, 0x0000000000000002L),
                new Int128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)));

        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0x0000000000000000L, 0x0000000000000002L),
                new Int128(0x8000000000000000L, 0x7FFFFFFFFFFFFFFFL)));

        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFEL),
                new Int128(0x8000000000000000L, 0x7FFFFFFFFFFFFFFFL)));

        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFEL),
                new Int128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)));
    }

    @Test
    public void testOverflow()
    {
        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0xFFFFFFFFFFFFFFFEL, 0x0000000000000000L),
                new Int128(0xFFFFFFFFFFFFFFFEL, 0x0000000000000000L)));

        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0xFFFFFFFFFFFFFFFEL, 0x0000000000000000L),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L)));

        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0xFFFFFFFFFFFFFFFEL, 0x0000000000000001L),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L)));

        Assert.assertThrows(ArithmeticException.class, () -> multiplyExact(
                new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L),
                new Int128(0xFFFFFFFFFFFFFFFEL, 0x0000000000000001L)));
    }

    @Test
    public void testBiggerThanLongButNoOverflow()
    {
        // Values that are outside of the range of signed long, but don't overflow when multiplied together
        Int128 minValueMinusOne = new Int128(0xFFFFFFFFFFFFFFFFL, 0x7FFFFFFFFFFFFFFFL); // Long.MIN_VALUE - 1
        Int128 maxValuePlusOne = new Int128(0x0000000000000000L, 0x8000000000000000L); // Long.MAX_VALUE + 1

        Assert.assertEquals(Int128.multiplyExact(maxValuePlusOne, maxValuePlusOne),
                Int128.valueOf(maxValuePlusOne.toBigInteger().pow(2)));

        Assert.assertEquals(Int128.multiplyExact(maxValuePlusOne, minValueMinusOne),
                Int128.valueOf(maxValuePlusOne.toBigInteger().multiply(minValueMinusOne.toBigInteger())));

        Assert.assertEquals(Int128.multiplyExact(minValueMinusOne, minValueMinusOne),
                Int128.valueOf(minValueMinusOne.toBigInteger().multiply(minValueMinusOne.toBigInteger())));

        Assert.assertEquals(multiplyExact(Int128.valueOf("13043817825332782212"), Int128.valueOf("13043817825332782212")),
                Int128.valueOf("170141183460469231722567801800623612944"));

        Assert.assertEquals(multiplyExact(Int128.valueOf("-13043817825332782212"), Int128.valueOf("13043817825332782212")),
                Int128.valueOf("-170141183460469231722567801800623612944"));

        Assert.assertEquals(multiplyExact(Int128.valueOf("13043817825332782212"), Int128.valueOf("-13043817825332782212")),
                Int128.valueOf("-170141183460469231722567801800623612944"));

        Assert.assertEquals(multiplyExact(Int128.valueOf("-13043817825332782212"), Int128.valueOf("-13043817825332782212")),
                Int128.valueOf("170141183460469231722567801800623612944"));
    }

    @Test
    public void testShiftLeft()
    {
        for (int shift = 0; shift < 127; shift++) {
            Assert.assertEquals(Int128.shiftLeft(ONE, shift), Int128.valueOf(BigInteger.ONE.shiftLeft(shift)));
        }
    }

    @Test
    public void testShiftRight()
    {
        // positive
        for (int shift = 0; shift < 127; shift++) {
            Assert.assertEquals(Int128.shiftRight(MAX_VALUE, shift),
                    Int128.valueOf(MAX_VALUE.toBigInteger().shiftRight(shift)));
        }

        // negative
        for (int shift = 0; shift < 127; shift++) {
            Assert.assertEquals(Int128.shiftRight(MIN_VALUE, shift),
                    Int128.valueOf(MIN_VALUE.toBigInteger().shiftRight(shift)));
        }
    }

    @Test
    public void testShiftRightUnsigned()
    {
        // positive
        for (int shift = 0; shift < 127; shift++) {
            Assert.assertEquals(Int128.shiftRightUnsigned(MAX_VALUE, shift),
                    Int128.valueOf(MAX_VALUE.toBigInteger().shiftRight(shift)));
        }

        // negative
        for (int shift = 1; shift < 127; shift++) {
            Assert.assertEquals(Int128.shiftRightUnsigned(MIN_VALUE, shift),
                    Int128.valueOf(MIN_VALUE.toBigInteger().abs().shiftRight(shift)));
        }
    }

    @Test
    public void testNumberOfLeadingZeros()
    {
        for (int shift = 0; shift < 127; shift++) {
            Assert.assertEquals(Int128.numberOfLeadingZeros(Int128.shiftLeft(ONE, shift)), 127 - shift);
        }
    }

    @Test
    public void testNumberOfTrailingZeros()
    {
        for (int shift = 0; shift < 127; shift++) {
            Assert.assertEquals(Int128.numberOfTrailingZeros(Int128.shiftLeft(ONE, shift)), shift);
        }
    }

    @Test
    public void testBitCount()
    {
        Assert.assertEquals(Int128.bitCount(new Int128(0x0000000000000000L, 0x0000000000000000L)), 0);

        Assert.assertEquals(Int128.bitCount(new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L)), 64);

        Assert.assertEquals(Int128.bitCount(new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL)), 64);

        Assert.assertEquals(Int128.bitCount(new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)), 128);

        Assert.assertEquals(Int128.bitCount(new Int128(0x0000000000000000L, 0x0000000000000001L)), 1);

        Assert.assertEquals(Int128.bitCount(new Int128(0x0000000000000001L, 0x0000000000000000L)), 1);
    }

    @Test
    public void testNot()
    {
        Assert.assertEquals(Int128.not(new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)),
                new Int128(0x0000000000000000L, 0x0000000000000000L));

        Assert.assertEquals(Int128.not(new Int128(0x0000000000000000L, 0x0000000000000000L)),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL));

        Assert.assertEquals(Int128.not(new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL)),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L));
    }

    @Test
    public void testAnd()
    {
        Assert.assertEquals(
                and(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL));

        Assert.assertEquals(
                and(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0x0000000000000000L, 0x0000000000000000L)),
                new Int128(0x0000000000000000L, 0x0000000000000000L));

        Assert.assertEquals(
                and(
                        new Int128(0x0000000000000000L, 0x0000000000000000L),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)),
                new Int128(0x0000000000000000L, 0x0000000000000000L));

        Assert.assertEquals(
                and(
                        new Int128(0x0000000000000000L, 0x0000000000000000L),
                        new Int128(0x0000000000000000L, 0x0000000000000000L)),
                new Int128(0x0000000000000000L, 0x0000000000000000L));

        Assert.assertEquals(
                and(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL)),
                new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL));

        Assert.assertEquals(
                and(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L)),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L));
    }

    @Test
    public void testOr()
    {
        Assert.assertEquals(
                or(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL));

        Assert.assertEquals(
                or(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0x0000000000000000L, 0x0000000000000000L)),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL));

        Assert.assertEquals(
                or(
                        new Int128(0x0000000000000000L, 0x0000000000000000L),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL));

        Assert.assertEquals(
                or(
                        new Int128(0x0000000000000000L, 0x0000000000000000L),
                        new Int128(0x0000000000000000L, 0x0000000000000000L)),
                new Int128(0x0000000000000000L, 0x0000000000000000L));

        Assert.assertEquals(
                or(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L),
                        new Int128(0x0000000000000000L, 0x0000000000000000L)),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L));

        Assert.assertEquals(
                or(
                        new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0x0000000000000000L, 0x0000000000000000L)),
                new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL));
    }

    @Test
    public void testXor()
    {
        Assert.assertEquals(
                xor(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)),
                new Int128(0x0000000000000000L, 0x0000000000000000L));

        Assert.assertEquals(
                xor(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0x0000000000000000L, 0x0000000000000000L)),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL));

        Assert.assertEquals(
                xor(
                        new Int128(0x0000000000000000L, 0x0000000000000000L),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL));

        Assert.assertEquals(
                xor(
                        new Int128(0x0000000000000000L, 0x0000000000000000L),
                        new Int128(0x0000000000000000L, 0x0000000000000000L)),
                new Int128(0x0000000000000000L, 0x0000000000000000L));

        Assert.assertEquals(
                xor(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L),
                        new Int128(0x0000000000000000L, 0x0000000000000000L)),
                new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L));

        Assert.assertEquals(
                xor(
                        new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0x0000000000000000L, 0x0000000000000000L)),
                new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL));
    }

    @Test
    public void testToLong()
    {
        Assert.assertEquals(Int128.valueOf(Long.MAX_VALUE).toLong(), Long.MAX_VALUE);

        Assert.assertEquals(Int128.valueOf(Long.MAX_VALUE).toLongExact(), Long.MAX_VALUE);

        Assert.assertEquals(Int128.valueOf(Long.MIN_VALUE).toLong(), Long.MIN_VALUE);

        Assert.assertEquals(Int128.valueOf(Long.MIN_VALUE).toLongExact(), Long.MIN_VALUE);

        Assert.assertEquals(ZERO.toLong(), 0);

        Assert.assertEquals(ZERO.toLongExact(), 0);

        Assert.assertEquals(MAX_VALUE.toLong(), -1);

        Assert.assertEquals(MIN_VALUE.toLong(), 0);

        Assert.assertThrows(ArithmeticException.class, () -> MAX_VALUE.toLongExact());

        Assert.assertThrows(ArithmeticException.class, () -> MIN_VALUE.toLongExact());
    }

    @Test
    public void testCompare()
    {
        Assert.assertEquals(compare(ZERO, ZERO), 0);

        Assert.assertEquals(compare(ONE, ONE), 0);

        Assert.assertEquals(compare(MAX_VALUE, MAX_VALUE), 0);

        Assert.assertEquals(compare(MIN_VALUE, MIN_VALUE), 0);

        Assert.assertEquals(compare(MIN_VALUE, ZERO), -1);

        Assert.assertEquals(compare(MAX_VALUE, ZERO), 1);

        Assert.assertEquals(compare(ZERO, MIN_VALUE), 1);

        Assert.assertEquals(compare(ZERO, MAX_VALUE), -1);

        Assert.assertEquals(compare(MIN_VALUE, MAX_VALUE), -1);

        Assert.assertEquals(compare(MAX_VALUE, MIN_VALUE), 1);

        Assert.assertEquals(compare(Int128.valueOf(Long.MAX_VALUE), MAX_VALUE), -1);

        Assert.assertEquals(compare(Int128.valueOf(Long.MIN_VALUE), MAX_VALUE), -1);

        Assert.assertEquals(compare(Int128.valueOf(Long.MAX_VALUE), MIN_VALUE), 1);

        Assert.assertEquals(compare(Int128.valueOf(Long.MIN_VALUE), MIN_VALUE), 1);

        Assert.assertEquals(
                compare(
                        new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFFL),
                        new Int128(0x0000000000000000L, 0xFFFFFFFFFFFFFFFEL)),
                1);

        Assert.assertEquals(
                compare(
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000001L),
                        new Int128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000000L)),
                1);

        Assert.assertEquals(compare(Int128.valueOf(1), ZERO), 1);

        Assert.assertEquals(compare(Int128.valueOf(-1), ZERO), -1);

        Assert.assertEquals(compare(Int128.valueOf(-1), Int128.valueOf(-1)), 0);

        Assert.assertEquals(compare(Int128.valueOf(1), Int128.valueOf(1)), 0);
    }

    @Test
    public void testIncrement()
    {
        Assert.assertEquals(Int128.increment(ZERO), ONE);

        Assert.assertEquals(Int128.incrementExact(ZERO), ONE);

        Assert.assertEquals(Int128.increment(MAX_VALUE), MIN_VALUE);

        Assert.assertThrows(ArithmeticException.class, () -> Int128.incrementExact(MAX_VALUE));
    }

    @Test
    public void testDecrement()
    {
        Assert.assertEquals(Int128.decrement(ZERO), Int128.negate(ONE));

        Assert.assertEquals(Int128.decrementExact(ZERO), Int128.negate(ONE));

        Assert.assertEquals(Int128.decrement(MIN_VALUE), MAX_VALUE);

        Assert.assertThrows(ArithmeticException.class, () -> Int128.decrementExact(MIN_VALUE));
    }

    @Test
    public void testDivide()
    {
        Assert.assertThrows(ArithmeticException.class, () -> Int128.divide(ONE, ZERO));
        Assert.assertTrue(new DivideAssert(ONE, MAX_VALUE).isValid());
        Assert.assertTrue(new DivideAssert(ONE, MIN_VALUE).isValid());
        Assert.assertTrue(new DivideAssert(MAX_VALUE, ONE).isValid());
        Assert.assertTrue(new DivideAssert(MIN_VALUE, ONE).isValid());
        Assert.assertTrue(new DivideAssert(MAX_VALUE, MAX_VALUE).isValid());
        Assert.assertTrue(new DivideAssert(MIN_VALUE, MIN_VALUE).isValid());
        Assert.assertTrue(new DivideAssert(Int128.valueOf(Long.MAX_VALUE), Int128.valueOf(12345)).isValid());
        Assert.assertTrue(new DivideAssert(Int128.valueOf(Long.MIN_VALUE), Int128.valueOf(12345)).isValid());
        Assert.assertTrue(new DivideAssert(MAX_VALUE, Int128.valueOf(12345)).isValid());
        Assert.assertTrue(new DivideAssert(MIN_VALUE, Int128.valueOf(12345)).isValid());
        Assert.assertTrue(new DivideAssert(MIN_VALUE, subtract(MIN_VALUE, ONE)).isValid());
    }

    @Test
    public void testDivideByPowersOf2()
    {
        for (int i = 0; i < 127; i++) {
            Assert.assertTrue(new DivideAssert(MAX_VALUE, Int128.shiftLeft(ONE, i)).isValid());
        }

        for (int i = 0; i < 127; i++) {
            Assert.assertTrue(new DivideAssert(MIN_VALUE, Int128.shiftLeft(ONE, i)).isValid());
        }
    }

    @Test
    public void testDivideAllMagnitudes()
    {
        // positive / positive
        for (int dividendMagnitude = 127; dividendMagnitude >= 0; dividendMagnitude--) {
            Int128 dividend = Int128.shiftRightUnsigned(MAX_VALUE, 127 - dividendMagnitude);
            for (int divisorMagnitude = 127; divisorMagnitude > 0; divisorMagnitude--) {
                Int128 divisor = Int128.shiftRightUnsigned(MAX_VALUE, 127 - divisorMagnitude);

                Assert.assertTrue(new DivideAssert(dividend, divisor).isValid());
            }
        }

        // positive / negative
        for (int dividendMagnitude = 127; dividendMagnitude >= 0; dividendMagnitude--) {
            Int128 dividend = Int128.shiftRightUnsigned(MAX_VALUE, 127 - dividendMagnitude);
            for (int divisorMagnitude = 127; divisorMagnitude > 0; divisorMagnitude--) {
                Int128 divisor = Int128.shiftRight(MIN_VALUE, 127 - divisorMagnitude);

                Assert.assertTrue(new DivideAssert(dividend, divisor).isValid());
            }
        }

        // negative / positive
        for (int dividendMagnitude = 127; dividendMagnitude >= 0; dividendMagnitude--) {
            Int128 dividend = Int128.shiftRight(MIN_VALUE, 127 - dividendMagnitude);
            for (int divisorMagnitude = 127; divisorMagnitude > 0; divisorMagnitude--) {
                Int128 divisor = Int128.shiftRightUnsigned(MAX_VALUE, 127 - divisorMagnitude);

                Assert.assertTrue(new DivideAssert(dividend, divisor).isValid());
            }
        }

        // negative / negative
        for (int dividendMagnitude = 127; dividendMagnitude >= 0; dividendMagnitude--) {
            Int128 dividend = Int128.shiftRight(MIN_VALUE, 127 - dividendMagnitude);
            for (int divisorMagnitude = 127; divisorMagnitude > 0; divisorMagnitude--) {
                Int128 divisor = Int128.shiftRight(MIN_VALUE, 127 - divisorMagnitude);

                Assert.assertTrue(new DivideAssert(dividend, divisor).isValid());
            }
        }
    }

    public static class DivideAssert
    {
        private final Int128 dividend;
        private final Int128 divisor;
        private final Result actual;

        public DivideAssert(Int128 dividend, Int128 divisor)
        {
            this.dividend = dividend;
            this.divisor = divisor;
            this.actual = compute(dividend, divisor);
        }

        private static Result compute(Int128 dividend, Int128 divisor)
        {
            try {
                Int128.DivisionResult result = Int128.divideWithRemainder(dividend, divisor);
                return new Result(result.getQuotient(), result.getRemainder(), null);
            }
            catch (Exception e) {
                return new Result(null, null, e);
            }
        }

        public boolean isValid()
        {
            Assert.assertEquals(actual.getError(), null);

            Assert.assertEquals(addExact(multiplyExact(actual.getQuotient(), divisor), actual.getRemainder()), dividend);

            Assert.assertEquals(actual.getQuotient(), Int128.valueOf(dividend.toBigInteger().divide(divisor.toBigInteger())));

            Assert.assertEquals(actual.getRemainder(), Int128.valueOf(dividend.toBigInteger().remainder(divisor.toBigInteger())));

            return true;
        }

        static class Result {
            private Int128 quotient;
            private Int128 remainder;
            private Exception error;

            public Result(Int128 quotient, Int128 remainder, Exception error) {
                this.quotient = quotient;
                this.remainder = remainder;
                this.error = error;
            }

            public Int128 getQuotient() {
                return quotient;
            }

            public Int128 getRemainder() {
                return remainder;
            }

            public Exception getError() {
                return error;
            }
        }
    }
}
