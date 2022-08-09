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

import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1, jvmArgsAppend = {
        "-XX:+UnlockDiagnosticVMOptions",
//        "-XX:CompileCommand=print,*int128*.*",
        "-XX:PrintAssemblyOptions=intel"})
@Warmup(iterations = 10, time = 50, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 50, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkAddPrecisionGreatThan18 {

    @Benchmark
    @OperationsPerInvocation(BenchmarkData.COUNT)
    public void addDecimal128(BenchmarkData data)
    {
        for (int i = 0; i < BenchmarkData.COUNT; i++) {
            sink(data.dividends[i].addDestructive(data.divisors[i], (short) 38));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkData.COUNT)
    public void addBigint(BenchmarkData data)
    {
        for (int i = 0; i < BenchmarkData.COUNT; i++) {
            sink(data.bigintDividends[i].add(data.bigintDivisors[i]));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkData.COUNT)
    public void addBigDecimal(BenchmarkData data)
    {
        for (int i = 0; i < BenchmarkData.COUNT; i++) {
            sink(data.bigDecimalDividends[i].add(data.bigDecimalDivisors[i]));
        }
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkData.COUNT)
    public void addDecimal(BenchmarkData data)
    {
        for (int i = 0; i < BenchmarkData.COUNT; i++) {
            sink(data.decimalDividends[i].$plus(data.decimalDivisors[i]));
        }
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public static void sink(Decimal128 value)
    {
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public static void sink(BigInteger value)
    {
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public static void sink(BigDecimal value)
    {
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public static void sink(Decimal value)
    {
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int COUNT = 1000;
        private static final Pattern DECIMAL_PATTERN = Pattern.compile("(\\+?|-?)((0*)(\\d*))(\\.(\\d*))?");
        public static final int MAX_SHORT_PRECISION = 18;

        private final Decimal128[] dividends = new Decimal128[COUNT];
        private final Decimal128[] divisors = new Decimal128[COUNT];

        private final BigInteger[] bigintDividends = new BigInteger[COUNT];
        private final BigInteger[] bigintDivisors = new BigInteger[COUNT];

        private final BigDecimal[] bigDecimalDividends = new BigDecimal[COUNT];
        private final BigDecimal[] bigDecimalDivisors = new BigDecimal[COUNT];

        private final Decimal[] decimalDividends = new Decimal[COUNT];
        private final Decimal[] decimalDivisors = new Decimal[COUNT];

        @Param(value = {"15432.21543600787131", "17432.21543600787131", "15435.21545610787131", "16435.21545640787131", "35465.21545613787131", "154325.21545610787132", "105435.21545610787131", "15435.2154561107871311", "15435.215456107871310"})
        private String dividendMagnitude = "15432.21543600787131";

        @Param(value = {"57832.21543600787313", "57832.21543900787313", "57832.216543600787313", "57832.21643600787313", "77832.21543600787313", "578.2154360078731332", "57832.210543600787313", "57832.215436007873131"})
        private String divisorMagnitude = "57832.21543600787313";

        @Setup
        public void setup()
        {
            int count = 0;
            while (count < COUNT) {
                Decimal128 dividend = (Decimal128) testMatcher(dividendMagnitude, true);
                Decimal128 divisor = (Decimal128) testMatcher(divisorMagnitude, true);

                if (ThreadLocalRandom.current().nextBoolean()) {
                    dividend.negateDestructive();
                }

                if (ThreadLocalRandom.current().nextBoolean()) {
                    divisor.negateDestructive();
                }

                if (!divisor.isZero()) {
                    dividends[count] = dividend;
                    divisors[count] = divisor;

                    decimalDividends[count] = Decimal.apply(dividends[count].toBigDecimal());
                    decimalDivisors[count] = Decimal.apply(divisors[count].toBigDecimal());

                    bigintDividends[count] = decimalDividends[count].toJavaBigInteger();
                    bigintDivisors[count] = decimalDivisors[count].toJavaBigInteger();

                    bigDecimalDividends[count] = decimalDividends[count].toJavaBigDecimal();
                    bigDecimalDivisors[count] = decimalDivisors[count].toJavaBigDecimal();

                    count++;
                }
            }
        }

        private Object testMatcher(String stringValue, boolean includeLeadingZerosInPrecision)
        {
            Matcher matcher = DECIMAL_PATTERN.matcher(stringValue);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid decimal value '" + stringValue + "'");
            }

            String sign = getMatcherGroup(matcher, 1);
            if (sign.isEmpty()) {
                sign = "+";
            }
            String leadingZeros = getMatcherGroup(matcher, 3);
            String integralPart = getMatcherGroup(matcher, 4);
            String fractionalPart = getMatcherGroup(matcher, 6);

            if (leadingZeros.isEmpty() && integralPart.isEmpty() && fractionalPart.isEmpty()) {
                throw new IllegalArgumentException("Invalid decimal value '" + stringValue + "'");
            }

            int scale = fractionalPart.length();
            int precision;
            if (includeLeadingZerosInPrecision) {
                precision = leadingZeros.length() + integralPart.length() + scale;
            }
            else {
                precision = integralPart.length() + scale;
                if (precision == 0) {
                    precision = 1;
                }
            }

            String unscaledValue = sign + leadingZeros + integralPart + fractionalPart;
            Object value;
            if (precision <= MAX_SHORT_PRECISION) {
                value = Long.parseLong(unscaledValue);
            }
            else {
                value = new Decimal128().update(new BigInteger(unscaledValue), (short) 38);
            }

            return value;
        }

        private String getMatcherGroup(MatchResult matcher, int group)
        {
            String groupValue = matcher.group(group);
            if (groupValue == null) {
                groupValue = "";
            }
            return groupValue;
        }
    }

    @Test
    public void test()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();

        addDecimal128(data);
        addBigint(data);
        addBigDecimal(data);
        addDecimal(data);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        BenchmarkRunner.benchmark(BenchmarkAddPrecisionGreatThan18.class);
    }
}
