/* Copyright (c) 2012-2013 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.storage;

import org.locationtech.geogig.api.RevObject;

/**
 * Encapsulates a transaction.
 * <p>
 * Use the same ObjectInserter for a single transaction
 * </p>
 * 
 */
public class ObjectInserter {

    private ObjectStore objectDb;

    // TODO: transaction management
    /**
     * Constructs a new {@code ObjectInserter} with the given {@link ObjectDatabase}.
     * 
     * @param objectDatabase the database to insert to
     */
    public ObjectInserter(ObjectStore objectDatabase) {
        objectDb = objectDatabase;
    }

    /**
     * @param object
     */
    public void insert(RevObject object) {
        objectDb.put(object);
    }

}
