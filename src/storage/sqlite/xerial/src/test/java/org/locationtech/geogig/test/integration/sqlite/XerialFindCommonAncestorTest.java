/* Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Johnathan Garrett (LMN Solutions) - initial implementation
 */
package org.locationtech.geogig.test.integration.sqlite;

import static org.locationtech.geogig.test.integration.sqlite.XerialTests.injector;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geogig.api.Context;
import org.locationtech.geogig.api.TestPlatform;


public class XerialFindCommonAncestorTest extends
        org.locationtech.geogig.test.integration.FindCommonAncestorTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Override
    protected Context createInjector() {
        return injector(new TestPlatform(temp.getRoot()));
    }
}
