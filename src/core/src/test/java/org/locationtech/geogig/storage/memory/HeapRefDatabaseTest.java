/* Copyright (c) 2015 Boundless.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage.memory;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.storage.RefDatabase;
import org.locationtech.geogig.test.integration.repository.RefDatabaseTest;

public class HeapRefDatabaseTest extends RefDatabaseTest {

    @Override
    protected RefDatabase createDatabase(Platform platform) throws Exception {
        return new HeapRefDatabase();
    }

}
