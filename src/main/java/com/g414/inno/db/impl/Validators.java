package com.g414.inno.db.impl;

import java.math.BigInteger;

import com.g414.inno.db.ColumnAttribute;
import com.g414.inno.db.ColumnDef;

public class Validators {
    public static Validator<String> VARCHAR = new Validator<String>() {
        public boolean isValid(String target, ColumnDef columnDef) {
            if (isBadNull(target, columnDef)) {
                return false;
            } else if (target == null) {
                return true;
            }

            if (target != null && isBadLength(target.length(), columnDef)) {
                return false;
            }

            return true;
        };
    };

    public static Validator<String> CHAR = new Validator<String>() {
        public boolean isValid(String target, ColumnDef columnDef) {
            if (isBadNull(target, columnDef)) {
                return false;
            } else if (target == null) {
                return true;
            }

            if (target != null && isBadLength(target.length(), columnDef)) {
                return false;
            }

            return true;
        };
    };

    public static Validator<byte[]> BINARY = new Validator<byte[]>() {
        public boolean isValid(byte[] target, ColumnDef columnDef) {
            if (isBadNull(target, columnDef)) {
                return false;
            } else if (target == null) {
                return true;
            }

            if (target != null && isBadLength(target.length, columnDef)) {
                return false;
            }

            return true;
        };
    };

    public static Validator<byte[]> BLOB = new Validator<byte[]>() {
        public boolean isValid(byte[] target, ColumnDef columnDef) {
            if (isBadNull(target, columnDef)) {
                return false;
            } else if (target == null) {
                return true;
            }

            if (target != null && isBadLength(target.length, columnDef)) {
                return false;
            }

            return true;
        };
    };

    public static Validator<Number> INT = new Validator<Number>() {
        public boolean isValid(Number target, ColumnDef columnDef) {
            if (isBadNull(target, columnDef)) {
                return false;
            } else if (target == null) {
                return true;
            }

            if (!(target instanceof Byte) && !(target instanceof Short)
                    && !(target instanceof Integer)
                    && !(target instanceof Long)
                    && !(target instanceof BigInteger)) {
                return false;
            }

            boolean isUnsigned = columnDef.getAttrs().contains(
                    ColumnAttribute.UNSIGNED);
            int signed = isUnsigned ? 0 : 1;
            int posBits = (8 * columnDef.getLength()) - signed;

            System.out.println(String.format("K: %d %d %d", signed, posBits,
                    target));

            if (target instanceof BigInteger) {
                BigInteger hiLimit = BigInteger.ONE.shiftLeft(posBits);
                BigInteger loLimit = isUnsigned ? BigInteger.ZERO
                        : ((BigInteger) hiLimit).negate().subtract(
                                BigInteger.ONE);

                System.out.println(String.format("V: %d %d %d", hiLimit,
                        loLimit, target));

                if (((BigInteger) target).compareTo(loLimit) < 0
                        || ((BigInteger) target).compareTo(hiLimit) > 0) {
                    return false;
                }
            } else {
                long hiLimit = Long.MAX_VALUE >> (63 - posBits);
                long loLimit = isUnsigned ? 0 : -((Long) hiLimit) - 1;
                long compare = target.longValue();

                System.out.println(String.format("V: %d %d %d", hiLimit,
                        loLimit, compare));

                if ((compare < loLimit) || (compare > hiLimit)) {
                    return false;
                }
            }

            return true;
        }
    };

    private static boolean isBadNull(Object target, ColumnDef columnDef) {
        return (target == null)
                && columnDef.getAttrs().contains(ColumnAttribute.NOT_NULL);
    }

    private static boolean isBadLength(int length, ColumnDef columnDef) {
        return columnDef.getLength() < length;
    }
}
