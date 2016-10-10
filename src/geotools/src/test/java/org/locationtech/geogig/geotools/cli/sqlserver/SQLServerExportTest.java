/* Copyright (c) 2013-2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.geotools.cli.sqlserver;

import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.locationtech.geogig.api.porcelain.CommitOp;
import org.locationtech.geogig.cli.CommandFailedException;
import org.locationtech.geogig.cli.Console;
import org.locationtech.geogig.cli.GeogigCLI;
import org.locationtech.geogig.cli.InvalidParameterException;
import org.locationtech.geogig.geotools.cli.TestHelper;
import org.locationtech.geogig.test.integration.RepositoryTestCase;

public class SQLServerExportTest extends RepositoryTestCase {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private GeogigCLI cli;

    @Override
    public void setUpInternal() throws Exception {
        Console consoleReader = new Console().disableAnsi();
        cli = new GeogigCLI(consoleReader);

        cli.setGeogig(geogig);

        // Add points
        insertAndAdd(points1);
        insertAndAdd(points2);
        insertAndAdd(points3);

        geogig.command(CommitOp.class).call();

        // Add lines
        insertAndAdd(lines1);
        insertAndAdd(lines2);
        insertAndAdd(lines3);

        geogig.command(CommitOp.class).call();
    }

    @Override
    public void tearDownInternal() throws Exception {
        cli.close();
    }

    @Test
    public void testExport() throws Exception {

        SQLServerExport exportCommand = new SQLServerExport();
        exportCommand.args = Arrays.asList("Points", "Points");
        exportCommand.support.dataStoreFactory = TestHelper.createTestFactory();
        exportCommand.run(cli);
    }

    @Test
    public void testNullDataStore() throws Exception {
        SQLServerExport exportCommand = new SQLServerExport();
        exportCommand.args = Arrays.asList("Points", "Points");
        exportCommand.support.dataStoreFactory = TestHelper.createNullTestFactory();
        exception.expect(CommandFailedException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testNoArgs() throws Exception {
        SQLServerExport exportCommand = new SQLServerExport();
        exportCommand.args = Arrays.asList();
        exportCommand.support.dataStoreFactory = TestHelper.createNullTestFactory();
        exception.expect(CommandFailedException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportToTableThatExists() throws Exception {
        SQLServerExport exportCommand = new SQLServerExport();
        exportCommand.args = Arrays.asList("Points", "table1");
        exportCommand.support.dataStoreFactory = TestHelper.createTestFactory();
        exception.expect(CommandFailedException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportToTableThatExistsWithOverwrite() throws Exception {
        SQLServerExport exportCommand = new SQLServerExport();
        exportCommand.args = Arrays.asList("WORK_HEAD:Points", "testTable");
        exportCommand.support.dataStoreFactory = TestHelper.createTestFactory();
        exportCommand.run(cli);

        exportCommand.args = Arrays.asList("Lines", "testTable");
        exportCommand.overwrite = true;
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithNonexistentFeatureTypeTree() throws Exception {
        SQLServerExport exportCommand = new SQLServerExport();
        exportCommand.args = Arrays.asList("invalidType", "invalidTable");
        exportCommand.support.dataStoreFactory = TestHelper.createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithNullTable() throws Exception {
        SQLServerExport exportCommand = new SQLServerExport();
        exportCommand.args = Arrays.asList("Points", null);
        exportCommand.support.dataStoreFactory = TestHelper.createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithNullFeatureType() throws Exception {
        SQLServerExport exportCommand = new SQLServerExport();
        exportCommand.args = Arrays.asList("", "invalidTable");
        exportCommand.support.dataStoreFactory = TestHelper.createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithEmptyStringForFeatureType() throws Exception {
        SQLServerExport exportCommand = new SQLServerExport();
        exportCommand.args = Arrays.asList("", "invalidTable");
        exportCommand.support.dataStoreFactory = TestHelper.createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithEmptyStringForTable() throws Exception {
        SQLServerExport exportCommand = new SQLServerExport();
        exportCommand.args = Arrays.asList("Points", "");
        exportCommand.support.dataStoreFactory = TestHelper.createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithFeatureNameInsteadOfType() throws Exception {
        SQLServerExport exportCommand = new SQLServerExport();
        exportCommand.args = Arrays.asList("Points/Points.1", "invalidTable");
        exportCommand.support.dataStoreFactory = TestHelper.createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

}