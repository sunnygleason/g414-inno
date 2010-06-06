package com.g414.inno.db;

import java.math.BigInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.inno.db.impl.Validators;

@Test
public class TestValidator {
    public void testChar() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.CHAR, 10);
        Assert.assertTrue(Validators.CHAR.isValid("", c1));
        Assert.assertTrue(Validators.CHAR.isValid("a test", c1));
        Assert.assertTrue(Validators.CHAR.isValid(null, c1));
        Assert.assertTrue(!Validators.CHAR.isValid("a longer test", c1));

        ColumnDef c2 = new ColumnDef(2, "c2", ColumnType.CHAR, 10,
                ColumnAttribute.NOT_NULL);
        Assert.assertTrue(Validators.CHAR.isValid("", c1));
        Assert.assertTrue(Validators.CHAR.isValid("a test", c2));
        Assert.assertTrue(!Validators.CHAR.isValid(null, c2));
        Assert.assertTrue(!Validators.CHAR.isValid("a longer test", c2));
    }

    public void testVarchar() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.VARCHAR, 10);
        Assert.assertTrue(Validators.VARCHAR.isValid("", c1));
        Assert.assertTrue(Validators.VARCHAR.isValid("a test", c1));
        Assert.assertTrue(Validators.VARCHAR.isValid(null, c1));
        Assert.assertTrue(!Validators.VARCHAR.isValid("a longer test", c1));

        ColumnDef c2 = new ColumnDef(2, "c2", ColumnType.VARCHAR, 10,
                ColumnAttribute.NOT_NULL);
        Assert.assertTrue(Validators.VARCHAR.isValid("", c1));
        Assert.assertTrue(Validators.VARCHAR.isValid("a test", c2));
        Assert.assertTrue(!Validators.VARCHAR.isValid(null, c2));
        Assert.assertTrue(!Validators.VARCHAR.isValid("a longer test", c2));
    }

    public void testInt08() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.INT, 1);
        Assert.assertTrue(Validators.INT.isValid(null, c1));
        Assert.assertTrue(Validators.INT.isValid(0, c1));
        Assert.assertTrue(Validators.INT.isValid(Byte.MAX_VALUE, c1));
        Assert.assertTrue(Validators.INT.isValid(Byte.MIN_VALUE, c1));
        Assert.assertTrue(!Validators.INT.isValid(128, c1));
        Assert.assertTrue(!Validators.INT.isValid(-129, c1));
        Assert.assertTrue(!Validators.INT.isValid(255, c1));
        Assert.assertTrue(!Validators.INT.isValid(256, c1));
        Assert.assertTrue(!Validators.INT.isValid(-256, c1));

        ColumnDef c2 = new ColumnDef(1, "c2", ColumnType.INT, 1,
                ColumnAttribute.UNSIGNED);
        Assert.assertTrue(Validators.INT.isValid(null, c2));
        Assert.assertTrue(Validators.INT.isValid(0, c2));
        Assert.assertTrue(Validators.INT.isValid(Byte.MAX_VALUE, c2));
        Assert.assertTrue(Validators.INT.isValid(128, c2));
        Assert.assertTrue(Validators.INT.isValid(255, c2));
        Assert.assertTrue(!Validators.INT.isValid(256, c2));
        Assert.assertTrue(!Validators.INT.isValid(-129, c2));
        Assert.assertTrue(!Validators.INT.isValid(Byte.MIN_VALUE, c2));
        Assert.assertTrue(!Validators.INT.isValid(-256, c2));
    }

    public void testInt16() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.INT, 2);
        Assert.assertTrue(Validators.INT.isValid(null, c1));
        Assert.assertTrue(Validators.INT.isValid(0, c1));
        Assert.assertTrue(Validators.INT.isValid(Short.MAX_VALUE, c1));
        Assert.assertTrue(Validators.INT.isValid(Short.MIN_VALUE, c1));
        Assert.assertTrue(!Validators.INT.isValid(32768, c1));
        Assert.assertTrue(!Validators.INT.isValid(-32769, c1));

        ColumnDef c2 = new ColumnDef(1, "c2", ColumnType.INT, 2,
                ColumnAttribute.UNSIGNED);
        Assert.assertTrue(Validators.INT.isValid(null, c2));
        Assert.assertTrue(Validators.INT.isValid(0, c2));
        Assert.assertTrue(Validators.INT.isValid(Short.MAX_VALUE, c2));
        Assert.assertTrue(Validators.INT.isValid(65535, c2));
        Assert.assertTrue(!Validators.INT.isValid(Short.MIN_VALUE, c2));
        Assert.assertTrue(!Validators.INT.isValid(65536, c2));
        Assert.assertTrue(!Validators.INT.isValid(-32768, c2));
        Assert.assertTrue(!Validators.INT.isValid(-32769, c2));
    }

    public void testInt24() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.INT, 4);
        Assert.assertTrue(Validators.INT.isValid(null, c1));
        Assert.assertTrue(Validators.INT.isValid(0, c1));
        Assert.assertTrue(Validators.INT.isValid(Integer.MAX_VALUE, c1));
        Assert.assertTrue(Validators.INT.isValid(Integer.MIN_VALUE, c1));
        Assert.assertTrue(!Validators.INT.isValid(2147483648L, c1));
        Assert.assertTrue(!Validators.INT.isValid(-2147483649L, c1));

        ColumnDef c2 = new ColumnDef(1, "c2", ColumnType.INT, 4,
                ColumnAttribute.UNSIGNED);
        Assert.assertTrue(Validators.INT.isValid(null, c2));
        Assert.assertTrue(Validators.INT.isValid(0, c2));
        Assert.assertTrue(Validators.INT.isValid(4294967295L, c2));
        Assert.assertTrue(!Validators.INT.isValid(4294967296L, c2));
        Assert.assertTrue(!Validators.INT.isValid(-4294967294L, c2));
    }

    public void testInt64() {
        ColumnDef c1 = new ColumnDef(1, "c1", ColumnType.INT, 8);
        Assert.assertTrue(Validators.INT.isValid(null, c1));
        Assert.assertTrue(Validators.INT.isValid(0, c1));
        Assert.assertTrue(Validators.INT.isValid(Long.MAX_VALUE, c1));
        Assert.assertTrue(Validators.INT.isValid(Long.MIN_VALUE, c1));
        Assert.assertTrue(!Validators.INT.isValid(BigInteger.ONE.shiftLeft(64),
                c1));
        Assert.assertTrue(!Validators.INT.isValid(BigInteger.ONE.shiftLeft(64)
                .negate(), c1));

        ColumnDef c2 = new ColumnDef(1, "c2", ColumnType.INT, 8,
                ColumnAttribute.UNSIGNED);
        Assert.assertTrue(Validators.INT.isValid(null, c2));
        Assert.assertTrue(Validators.INT.isValid(0, c2));
        Assert.assertTrue(Validators.INT.isValid(BigInteger.ONE.shiftLeft(64)
                .subtract(BigInteger.ONE), c2));
        Assert.assertTrue(!Validators.INT.isValid(BigInteger.ONE.shiftLeft(65),
                c2));
        Assert.assertTrue(!Validators.INT.isValid(BigInteger.ONE.shiftLeft(64)
                .negate(), c2));
        Assert.assertTrue(!Validators.INT.isValid(BigInteger.ONE.shiftLeft(65)
                .negate(), c2));
    }
}
