/* Copyright (c) 2013-2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.geotools.cli;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.geotools.data.DataStore;
import org.locationtech.geogig.cli.AbstractCommand;
import org.locationtech.geogig.cli.CLICommand;
import org.locationtech.geogig.cli.CommandFailedException;
import org.locationtech.geogig.cli.GeogigCLI;
import org.locationtech.geogig.cli.annotation.ReadOnly;
import org.locationtech.geogig.geotools.plumbing.DescribeOp;
import org.locationtech.geogig.geotools.plumbing.GeoToolsOpException;

import com.beust.jcommander.Parameter;
import com.google.common.base.Optional;

/**
 * Describes a feature type from a {@link DataStore} given by the concrete subclass.
 * 
 * @see DescribeOp
 */
@ReadOnly
public abstract class DataStoreDescribe extends AbstractCommand implements CLICommand {

    /**
     * Table to describe.
     */
    @Parameter(names = { "--table", "-t" }, description = "Table to describe.", required = true)
    public String table = "";

    protected abstract DataStore getDataStore();

    /**
     * Executes the describe command using the provided options.
     */
    @Override
    protected final void runInternal(GeogigCLI cli) throws IOException {

        DataStore dataStore = getDataStore();

        try {
            cli.getConsole().println("Fetching table...");

            Optional<Map<String, String>> propertyMap = cli.getGeogig().command(DescribeOp.class)
                    .setTable(table).setDataStore(dataStore).call();

            if (propertyMap.isPresent()) {
                cli.getConsole().println("Table : " + table);
                cli.getConsole().println("----------------------------------------");
                for (Entry<String, String> entry : propertyMap.get().entrySet()) {
                    cli.getConsole().println("\tProperty  : " + entry.getKey());
                    cli.getConsole().println("\tType      : " + entry.getValue());
                    cli.getConsole().println("----------------------------------------");
                }
            } else {
                throw new CommandFailedException("Could not find the specified table.");
            }
        } catch (GeoToolsOpException e) {
            switch (e.statusCode) {
            case TABLE_NOT_DEFINED:
                throw new CommandFailedException("No table supplied.", true);
            case UNABLE_TO_GET_FEATURES:
                throw new CommandFailedException("Unable to read the feature source.", e);
            case UNABLE_TO_GET_NAMES:
                throw new CommandFailedException("Unable to read feature types.", e);
            default:
                throw new CommandFailedException("Exception: " + e.statusCode.name(), e);
            }

        } finally {
            dataStore.dispose();
            cli.getConsole().flush();
        }
    }
}
