/* Copyright (c) 2013-2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Johnathan Garrett (LMN Solutions) - initial implementation
 */
package org.locationtech.geogig.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class RevPersonTest {

    @Test
    public void testRevPersonConstructorAndAccessors() {
        RevPerson person = new RevPersonImpl("test name", "test.email@test.com", 12345, 54321);

        assertEquals("test name", person.getName().get());
        assertEquals("test.email@test.com", person.getEmail().get());
        assertEquals(12345, person.getTimestamp());
        assertEquals(54321, person.getTimeZoneOffset());
    }

    @Test
    public void testRevPersonToString() {
        RevPerson person = new RevPersonImpl("test name", "test.email@test.com", 12345, 54321);

        String nameAndEmail = person.toString();

        assertEquals("test name <test.email@test.com> 12345/54321", nameAndEmail);
    }

    @Test
    public void testRevPersonEquals() {
        RevPerson person = new RevPersonImpl("test name", "test.email@test.com", 12345, 54321);
        RevPerson person2 = new RevPersonImpl("kishmael", "kelsey.ishmael@lmnsolutions.com", 54321,
                12345);
        assertFalse(person.equals(person2));
        person2 = new RevPersonImpl("test name", "kelsey.ishmael@lmnsolutions.com", 54321, 12345);

        assertFalse(person.equals(person2));
        person2 = new RevPersonImpl("test name", "test.email@test.com", 54321, 12345);
        assertFalse(person.equals(person2));
        person2 = new RevPersonImpl("test name", "test.email@test.com", 12345, 12345);
        assertFalse(person.equals(person2));
        assertFalse(person.equals("blah"));
        assertTrue(person.equals(person));
    }
}
