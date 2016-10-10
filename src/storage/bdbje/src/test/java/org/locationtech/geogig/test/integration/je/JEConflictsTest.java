/* Copyright (c) 2013-2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Victor Olaya (Boundless) - initial implementation
 */
package org.locationtech.geogig.test.integration.je;

import java.util.List;

import org.junit.Test;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.plumbing.merge.Conflict;
import org.locationtech.geogig.storage.ConflictsDatabase;
import org.locationtech.geogig.test.integration.RepositoryTestCase;

import com.google.common.base.Optional;

public class JEConflictsTest extends RepositoryTestCase {

    @Override
    protected void setUpInternal() throws Exception {
        // TODO Auto-generated method stub
    }

    @Test
    public void testConflicts() {
        ConflictsDatabase db = geogig.getRepository().conflictsDatabase();

        List<Conflict> conflicts = db.getConflicts(null, null);
        assertTrue(conflicts.isEmpty());
        Conflict conflict = new Conflict(idP1, ObjectId.forString("ancestor"),
                ObjectId.forString("ours"), ObjectId.forString("theirs"));
        Conflict conflict2 = new Conflict(idP2, ObjectId.forString("ancestor2"),
                ObjectId.forString("ours2"), ObjectId.forString("theirs2"));
        db.addConflict(null, conflict);
        Optional<Conflict> returnedConflict = db.getConflict(null, idP1);
        assertTrue(returnedConflict.isPresent());
        assertEquals(conflict, returnedConflict.get());
        db.removeConflict(null, idP1);
        conflicts = db.getConflicts(null, null);
        assertTrue(conflicts.isEmpty());
        db.addConflict(null, conflict);
        db.addConflict(null, conflict2);
        assertEquals(2, db.getConflicts(null, null).size());
        db.removeConflicts(null);
        conflicts = db.getConflicts(null, null);
        assertTrue(conflicts.isEmpty());

        final String NS = "ns";
        db.addConflict(NS, conflict);
        db.addConflict(null, conflict2);
        returnedConflict = db.getConflict(NS, idP1);
        assertTrue(returnedConflict.isPresent());
        assertEquals(conflict, returnedConflict.get());
        assertEquals(1, db.getConflicts(NS, null).size());
        db.removeConflict(NS, idP1);
        conflicts = db.getConflicts(NS, null);
        assertTrue(conflicts.isEmpty());
        db.addConflict(NS, conflict);
        db.addConflict(NS, conflict2);
        assertEquals(2, db.getConflicts(NS, null).size());
        assertEquals(1, db.getConflicts(null, null).size());
        db.removeConflicts(NS);
        conflicts = db.getConflicts(NS, null);
        assertTrue(conflicts.isEmpty());
        conflicts = db.getConflicts(null, null);
        assertFalse(conflicts.isEmpty());

    }
}
