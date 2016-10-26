/* Copyright (c) 2014-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.rest;

import java.util.List;

import org.locationtech.geogig.rest.repository.RESTUtils;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.resource.Variant;

import com.google.common.base.Optional;

public class Variants {
    public static final MediaType CSV_MEDIA_TYPE = new MediaType("text/csv",
            "Comma-separated Values");

    public static final MediaType GEOPKG_MEDIA_TYPE = new MediaType(
            "application/octet-stream;type=geopackage", "GeoPackage database file");

    public static final Variant JSON = new Variant(MediaType.APPLICATION_JSON);

    public static final Variant XML = new Variant(MediaType.APPLICATION_XML);

    public static final Variant TEXT_XML = new Variant(MediaType.TEXT_XML);

    public static final Variant CSV = new Variant(CSV_MEDIA_TYPE);

    public static final Variant GEOPKG = new Variant(GEOPKG_MEDIA_TYPE);
    
    public static final Variant ZIP = new Variant(MediaType.APPLICATION_ZIP);

    public static Optional<Variant> getVariantByExtension(Request request, List<Variant> supported) {
        String extension = RESTUtils.getStringAttribute(request, "extension");
        Variant v = null;
        if ("xml".equals(extension) && supported.contains(XML)) {
            v = XML;
        } else if ("json".equals(extension) && supported.contains(JSON)) {
            v = JSON;
        } else if ("csv".equals(extension) && supported.contains(CSV)) {
            v = CSV;
        } else if ("geopkg".equals(extension) && supported.contains(GEOPKG)) {
            v = GEOPKG;
        }else if ("zip".equals(extension) && supported.contains(ZIP)) {
            v = ZIP;
        }
        return Optional.fromNullable(v);
    }

}
