/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.model.impl;

import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevPerson;
import org.locationtech.geogig.model.RevTag;

public class RevTagBuilder {

    public static RevTag build(ObjectId id, String name, ObjectId commitId, String message,
            RevPerson tagger) {
        return new RevTagImpl(id, name, commitId, message, tagger);
    }

}
