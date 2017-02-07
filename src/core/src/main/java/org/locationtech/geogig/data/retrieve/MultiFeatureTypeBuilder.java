/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * David Blasby (Boundless) - initial implementation
 */
package org.locationtech.geogig.data.retrieve;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.jdt.annotation.Nullable;
import org.geotools.factory.Hints;
import org.locationtech.geogig.data.FeatureBuilder;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevFeature;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.repository.FeatureInfo;
import org.locationtech.geogig.storage.ObjectStore;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;

import com.google.common.base.Function;
import com.vividsolutions.jts.geom.GeometryFactory;

public class MultiFeatureTypeBuilder implements Function<FeatureInfo, SimpleFeature> {

    Map<ObjectId, FeatureBuilder> cache = new HashMap<ObjectId, FeatureBuilder>();

    ObjectStore odb;

    public MultiFeatureTypeBuilder(ObjectStore odb) {
        this.odb = odb;
    }

    public synchronized FeatureBuilder get(ObjectId metadataId) {
        FeatureBuilder featureBuilder = cache.get(metadataId);
        if (featureBuilder == null) {
            RevFeatureType revFtype = odb.getFeatureType(metadataId);
            featureBuilder = new FeatureBuilder(revFtype);
            cache.put(metadataId, featureBuilder);
        }
        return featureBuilder;
    }

    @Override
    public SimpleFeature apply(FeatureInfo info) {
        FeatureBuilder featureBuilder = get(info.getFeatureTypeId());
        return build(featureBuilder, info, null);
    }

    public static SimpleFeature build(FeatureBuilder featureBuilder, FeatureInfo info,
            @Nullable GeometryFactory geometryFactory) {

        String fid = info.getName();
        RevFeature revFeature = info.getFeature();
        Feature feature = featureBuilder.build(fid, revFeature, geometryFactory);
        feature.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
        feature.getUserData().put(Hints.PROVIDED_FID, fid);
        feature.getUserData().put(RevFeature.class, revFeature);
        feature.getUserData().put(RevFeatureType.class, featureBuilder.getType());
        return (SimpleFeature) feature;
    }
}
