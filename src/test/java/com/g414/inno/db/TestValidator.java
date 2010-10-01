package com.g414.inno.db;

import java.math.BigInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestValidator {
    public void testChar() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.CHAR, 10);
        Assert.assertTrue(Validation.isValid(c1, ""));
        Assert.assertTrue(Validation.isValid(c1, "a test"));
        Assert.assertTrue(Validation.isValid(c1, null));
        Assert.assertTrue(!Validation.isValid(c1, "a longer test"));

        ColumnDef c2 = new ColumnDef(2, "c2", ColumnType.CHAR, 10,
                ColumnAttribute.NOT_NULL);
        Assert.assertTrue(Validation.isValid(c2, ""));
        Assert.assertTrue(Validation.isValid(c2, "a test"));
        Assert.assertTrue(!Validation.isValid(c2, null));
        Assert.assertTrue(!Validation.isValid(c2, "a longer test"));
    }

    public void testVarchar() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.VARCHAR, 10);
        Assert.assertTrue(Validation.isValid(c1, ""));
        Assert.assertTrue(Validation.isValid(c1, "a test"));
        Assert.assertTrue(Validation.isValid(c1, null));
        Assert.assertTrue(!Validation.isValid(c1, "a longer test"));

        ColumnDef c2 = new ColumnDef(2, "c2", ColumnType.VARCHAR, 10,
                ColumnAttribute.NOT_NULL);
        Assert.assertTrue(Validation.isValid(c2, ""));
        Assert.assertTrue(Validation.isValid(c2, "a test"));
        Assert.assertTrue(!Validation.isValid(c2, null));
        Assert.assertTrue(!Validation.isValid(c2, "a longer test"));
    }

    public void testInt08() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.INT, 1);
        Assert.assertTrue(Validation.isValid(c1, null));
        Assert.assertTrue(Validation.isValid(c1, 0));
        Assert.assertTrue(Validation.isValid(c1, Byte.MAX_VALUE));
        Assert.assertTrue(Validation.isValid(c1, Byte.MIN_VALUE));
        Assert.assertTrue(!Validation.isValid(c1, 128));
        Assert.assertTrue(!Validation.isValid(c1, -129));
        Assert.assertTrue(!Validation.isValid(c1, 255));
        Assert.assertTrue(!Validation.isValid(c1, 256));
        Assert.assertTrue(!Validation.isValid(c1, -256));

        ColumnDef c2 = new ColumnDef(1, "c2", ColumnType.INT, 1,
                ColumnAttribute.UNSIGNED);
        Assert.assertTrue(Validation.isValid(c2, null));
        Assert.assertTrue(Validation.isValid(c2, 0));
        Assert.assertTrue(Validation.isValid(c2, Byte.MAX_VALUE));
        Assert.assertTrue(Validation.isValid(c2, 128));
        Assert.assertTrue(Validation.isValid(c2, 255));
        Assert.assertTrue(!Validation.isValid(c2, 256));
        Assert.assertTrue(!Validation.isValid(c2, -129));
        Assert.assertTrue(!Validation.isValid(c2, Byte.MIN_VALUE));
        Assert.assertTrue(!Validation.isValid(c2, -256));
    }

    public void testInt16() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.INT, 2);
        Assert.assertTrue(Validation.isValid(c1, null));
        Assert.assertTrue(Validation.isValid(c1, 0));
        Assert.assertTrue(Validation.isValid(c1, Short.MAX_VALUE));
        Assert.assertTrue(Validation.isValid(c1, Short.MIN_VALUE));
        Assert.assertTrue(!Validation.isValid(c1, 32768));
        Assert.assertTrue(!Validation.isValid(c1, -32769));

        ColumnDef c2 = new ColumnDef(1, "c2", ColumnType.INT, 2,
                ColumnAttribute.UNSIGNED);
        Assert.assertTrue(Validation.isValid(c2, null));
        Assert.assertTrue(Validation.isValid(c2, 0));
        Assert.assertTrue(Validation.isValid(c2, Short.MAX_VALUE));
        Assert.assertTrue(Validation.isValid(c2, 65535));
        Assert.assertTrue(!Validation.isValid(c2, Short.MIN_VALUE));
        Assert.assertTrue(!Validation.isValid(c2, 65536));
        Assert.assertTrue(!Validation.isValid(c2, -32768));
        Assert.assertTrue(!Validation.isValid(c2, -32769));
    }

    public void testInt32() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.INT, 4);
        Assert.assertTrue(Validation.isValid(c1, null));
        Assert.assertTrue(Validation.isValid(c1, 0));
        Assert.assertTrue(Validation.isValid(c1, Integer.MAX_VALUE));
        Assert.assertTrue(Validation.isValid(c1, Integer.MIN_VALUE));
        Assert.assertTrue(!Validation.isValid(c1, 2147483648L));
        Assert.assertTrue(!Validation.isValid(c1, -2147483649L));

        ColumnDef c2 = new ColumnDef(1, "c2", ColumnType.INT, 4,
                ColumnAttribute.UNSIGNED);
        Assert.assertTrue(Validation.isValid(c2, null));
        Assert.assertTrue(Validation.isValid(c2, 0));
        Assert.assertTrue(Validation.isValid(c2, 4294967295L));
        Assert.assertTrue(!Validation.isValid(c2, 4294967296L));
        Assert.assertTrue(!Validation.isValid(c2, -4294967294L));
    }

    public void testInt64() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.INT, 8);
        Assert.assertTrue(Validation.isValid(c1, null));
        Assert.assertTrue(Validation.isValid(c1, 0));
        Assert.assertTrue(Validation.isValid(c1, Long.MAX_VALUE));
        Assert.assertTrue(Validation.isValid(c1, Long.MIN_VALUE));
        Assert
                .assertTrue(!Validation.isValid(c1, BigInteger.ONE
                        .shiftLeft(64)));
        Assert.assertTrue(!Validation.isValid(c1, BigInteger.ONE.shiftLeft(64)
                .negate()));

        ColumnDef c2 = new ColumnDef(1, "c2", ColumnType.INT, 8,
                ColumnAttribute.UNSIGNED);
        Assert.assertTrue(Validation.isValid(c2, null));
        Assert.assertTrue(Validation.isValid(c2, 0));
        Assert.assertTrue(Validation.isValid(c2, BigInteger.ONE.shiftLeft(64)
                .subtract(BigInteger.ONE)));
        Assert
                .assertTrue(!Validation.isValid(c2, BigInteger.ONE
                        .shiftLeft(65)));
        Assert.assertTrue(!Validation.isValid(c2, BigInteger.ONE.shiftLeft(64)
                .negate()));
        Assert.assertTrue(!Validation.isValid(c2, BigInteger.ONE.shiftLeft(65)
                .negate()));
    }
}
