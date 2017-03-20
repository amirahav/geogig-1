/* Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Victor Olaya (Boundless) - initial implementation
 */
package org.locationtech.geogig.geotools.plumbing;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.eclipse.jdt.annotation.Nullable;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.Hints;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.collection.BaseFeatureCollection;
import org.geotools.feature.collection.DelegateFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geogig.repository.AbstractGeoGigOp;
import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.repository.ProgressListener;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.model.RevFeature;
import org.locationtech.geogig.model.RevFeatureType;
import org.locationtech.geogig.model.impl.RevFeatureTypeBuilder;
import org.locationtech.geogig.model.RevObject.TYPE;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.plumbing.FindTreeChild;
import org.locationtech.geogig.plumbing.ResolveTreeish;
import org.locationtech.geogig.repository.DiffEntry;
import org.locationtech.geogig.porcelain.DiffOp;
import org.locationtech.geogig.geotools.plumbing.GeoToolsOpException.StatusCode;
import org.locationtech.geogig.storage.ObjectStore;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

/**
 * Internal operation for creating a FeatureCollection from a tree content.
 * 
 */

public class ExportBatchDiffOp extends AbstractGeoGigOp<SimpleFeatureStore> {

    public static final String CHANGE_TYPE_NAME = "changetype";
    public static final String CHANGE_AUTHOR_EMAIL = "email";
    public static final String CHANGE_AUTHOR_NAME = "authorname";
    public static final String CHANGE_AUTHOR_TIME = "changetime";

    private static final Function<Feature, Optional<Feature>> IDENTITY = new Function<Feature, Optional<Feature>>() {

        @Override
        @Nullable
        public Optional<Feature> apply(@Nullable Feature feature) {
            return Optional.fromNullable(feature);
        }

    };

    private String path;

    private static Boolean isAdmin = false;

    private Supplier<SimpleFeatureStore> targetStoreProvider;

    private Function<Feature, Optional<Feature>> function = IDENTITY;

    private boolean transactional;

    private boolean old;

    ObjectId defaultMetadataId = null;

    private Iterator<RevCommit> oldRefIterator;

    /**
     * Executes the export operation using the parameters that have been specified.
     * 
     * @return a FeatureCollection with the specified features
     */
    @Override
    protected SimpleFeatureStore _call() {

        final SimpleFeatureStore targetStore = getTargetStore();

        // final String refspec = old ? oldRef : newRef;

        final ProgressListener progressListener = getProgressListener();

        progressListener.started();
        progressListener.setDescription("Exporting diffs for path '" + path + "'... ");

        FeatureCollection<SimpleFeatureType, SimpleFeature> asFeatureCollection = new BaseFeatureCollection<SimpleFeatureType, SimpleFeature>() {

            @Override
            public FeatureIterator<SimpleFeature> features() {

                Iterator<DiffEntry> diffs = mergeDiffs();

                final Iterator<SimpleFeature> plainFeatures = getFeatures(diffs, old,
                        objectDatabase(), defaultMetadataId, progressListener);

                Iterator<Optional<Feature>> transformed = Iterators.transform(plainFeatures,
                        ExportBatchDiffOp.this.function);

                Iterator<SimpleFeature> filtered = Iterators.filter(Iterators.transform(
                        transformed, new Function<Optional<Feature>, SimpleFeature>() {
                            @Override
                            public SimpleFeature apply(Optional<Feature> input) {
                                return (SimpleFeature) (input.isPresent() ? input.get() : null);
                            }
                        }), Predicates.notNull());

                return new DelegateFeatureIterator<SimpleFeature>(filtered);
            }
        };

        // add the feature collection to the feature store
        final Transaction transaction;
        if (transactional) {
            transaction = new DefaultTransaction("create");
        } else {
            transaction = Transaction.AUTO_COMMIT;
        }
        try {
            targetStore.setTransaction(transaction);
            try {
                targetStore.addFeatures(asFeatureCollection);
                transaction.commit();
            } catch (final Exception e) {
                if (transactional) {
                    transaction.rollback();
                }
                Throwables.propagateIfInstanceOf(e, GeoToolsOpException.class);
                throw new GeoToolsOpException(e, StatusCode.UNABLE_TO_ADD);
            } finally {
                transaction.close();
            }
        } catch (IOException e) {
            throw new GeoToolsOpException(e, StatusCode.UNABLE_TO_ADD);
        }

        progressListener.complete();

        return targetStore;

    }

    private static Iterator<SimpleFeature> getFeatures(Iterator<DiffEntry> diffs,
            final boolean old, final ObjectStore database, final ObjectId metadataId,
            final ProgressListener progressListener) {

        final SimpleFeatureType featureType = addChangeTypeAttribute(database
                .getFeatureType(metadataId));
        final RevFeatureType revFeatureType = RevFeatureTypeBuilder.build(featureType);
        final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);

        Function<DiffEntry, SimpleFeature> asFeature = new Function<DiffEntry, SimpleFeature>() {

            @Override
            @Nullable
            public SimpleFeature apply(final DiffEntry input) {
                NodeRef nodeRef = old ? input.getOldObject() : input.getNewObject();
                if (nodeRef == null) {
                    return null;
                }
                final RevFeature revFeature = database.getFeature(nodeRef.getObjectId());
                ImmutableList<Optional<Object>> values = revFeature.getValues();
                int newattribcount = isAdmin ? 4 : 3;
                for (int i = 0; i < values.size(); i++) {
                    String name = featureType.getDescriptor(i + newattribcount).getLocalName();
                    Object value = values.get(i).orNull();
                   // System.out.println(name+':'+value);
                    featureBuilder.set(name, value);
                }
                featureBuilder.set(CHANGE_TYPE_NAME, input.changeType().name().charAt(0));
                if(isAdmin)
                    featureBuilder.set(CHANGE_AUTHOR_EMAIL, input.getCommitAuthorEmail().orNull());
                //featureBuilder.set(CHANGE_AUTHOR_EMAIL, anonymizeEmail(isAdmin,input.getCommitAuthorEmail().orNull()));
                featureBuilder.set(CHANGE_AUTHOR_NAME, anonymizeName(isAdmin, input.getCommitAuthorName().orNull()));
                featureBuilder.set(CHANGE_AUTHOR_TIME,  input.getCommitTime());
              //  System.out.println(input.changeType().name().charAt(0)+input.getCommitAuthorName().orNull()+input.getCommitTime());
                Feature feature = featureBuilder.buildFeature(nodeRef.name());
              //  System.out.println("Feature built");
                feature.getUserData().put(Hints.USE_PROVIDED_FID, true);
                feature.getUserData().put(RevFeature.class, revFeature);
                feature.getUserData().put(RevFeatureType.class, revFeatureType);
             //   System.out.println(feature instanceof SimpleFeature);

                if (feature instanceof SimpleFeature) {
                        return (SimpleFeature) feature;

                }
                return null;
            }

        };

        Iterator<SimpleFeature> asFeatures = Iterators.transform(diffs, asFeature);

        UnmodifiableIterator<SimpleFeature> filterNulls = Iterators.filter(asFeatures,
                Predicates.notNull());

        return filterNulls;
    }

    private static String anonymizeEmail(Boolean isAdmin, String email){
        if(isAdmin) {
            return email;
        }else{
            return null;
        }
    }

    private static String anonymizeName(Boolean isAdmin, String name){
        if(isAdmin) {
            return name;
        }
        if(name!=null){
            String[] firstlast = name.split(" ");
            if(firstlast.length>1){
                //return firstlast[0].substring(0,1)+firstlast[1];
                return firstlast[0];
            }else{
                return name;
            }

        }else{
            return null;
        }
    }

    private Iterator<DiffEntry> mergeDiffs() {
        List<DiffEntry> diffentries = new ArrayList<DiffEntry>();
        int i = 0;
        while (oldRefIterator.hasNext()) {

            RevCommit commit = oldRefIterator.next();
            if (i == 0) {
                RevTree rootTree = resolveRootTree(commit.getId().toString());
                NodeRef typeTreeRef = resolTypeTreeRef(commit.getId().toString(), path, rootTree);
                defaultMetadataId = typeTreeRef.getMetadataId();
                i++;
            }
            String parentId = commit.getParentIds().size() >= 1 ? commit.getParentIds().get(0)
                    .toString() : ObjectId.NULL.toString();
            Iterator<DiffEntry> diffs = command(DiffOp.class).setOldVersion(parentId)
                    .setNewVersion(commit.getId().toString()).setFilter(path).call();
            while (diffs.hasNext()) {
            	DiffEntry diff = diffs.next();
            	diff.setCommitAuthorEmail(commit.getAuthor().getEmail());
            	diff.setCommitAuthorName(commit.getAuthor().getName());
            	diff.setCommitTime(new Date(commit.getAuthor().getTimestamp()).toString());
                diffentries.add(diff);
            }
        }

        return diffentries.iterator();

    }

    private static SimpleFeatureType addChangeTypeAttribute(RevFeatureType revFType) {
        SimpleFeatureType featureType = (SimpleFeatureType) revFType.type();
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.add(CHANGE_TYPE_NAME, String.class);
        if(isAdmin)
            builder.add(CHANGE_AUTHOR_EMAIL, String.class);
        builder.add(CHANGE_AUTHOR_NAME, String.class);
        builder.add(CHANGE_AUTHOR_TIME, String.class);
        for (AttributeDescriptor descriptor : featureType.getAttributeDescriptors()) {
            builder.add(descriptor);
        }
        builder.setName(featureType.getName());
        builder.setCRS(featureType.getCoordinateReferenceSystem());
        featureType = builder.buildFeatureType();
        return featureType;
    }

    private NodeRef resolTypeTreeRef(final String refspec, final String treePath,
            final RevTree rootTree) {
        Optional<NodeRef> typeTreeRef = command(FindTreeChild.class).setParent(rootTree)
                .setChildPath(treePath).call();
        checkArgument(typeTreeRef.isPresent(), "Type tree %s does not exist", refspec);
        checkArgument(TYPE.TREE.equals(typeTreeRef.get().getType()),
                "%s did not resolve to a tree", refspec);
        return typeTreeRef.get();
    }

    private RevTree resolveRootTree(final String refspec) {
        Optional<ObjectId> rootTreeId = command(ResolveTreeish.class).setTreeish(refspec).call();

        checkArgument(rootTreeId.isPresent(), "Invalid tree spec: %s", refspec);

        RevTree rootTree = objectDatabase().getTree(rootTreeId.get());
        return rootTree;
    }

    private SimpleFeatureStore getTargetStore() {
        SimpleFeatureStore targetStore;
        try {
            targetStore = targetStoreProvider.get();
        } catch (Exception e) {
            throw new GeoToolsOpException(StatusCode.CANNOT_CREATE_FEATURESTORE);
        }
        if (targetStore == null) {
            throw new GeoToolsOpException(StatusCode.CANNOT_CREATE_FEATURESTORE);
        }
        return targetStore;
    }

    /**
     * 
     * @param featureStore a supplier that resolves to the feature store to use for exporting
     * @return
     */
    public ExportBatchDiffOp setFeatureStore(Supplier<SimpleFeatureStore> featureStore) {
        this.targetStoreProvider = featureStore;
        return this;
    }

    /**
     * 
     * @param featureStore the feature store to use for exporting The schema of the feature store
     *        must be equal to the one of the layer whose diffs are to be exported, plus an
     *        additional "geogig_fid" field of type String, which is used to include the id of each
     *        feature.
     * 
     * @return
     */
    public ExportBatchDiffOp setFeatureStore(SimpleFeatureStore featureStore) {
        this.targetStoreProvider = Suppliers.ofInstance(featureStore);
        return this;
    }

    /**
     * @param path the path to export
     * @return {@code this}
     */
    public ExportBatchDiffOp setPath(String path) {
        this.path = path;
        return this;
    }

    public ExportBatchDiffOp setAdmin(Boolean isAdmin){
        this.isAdmin = isAdmin;
        return this;
    }

    /**
     * Sets the function to use for creating a valid Feature that has the FeatureType of the output
     * FeatureStore, based on the actual FeatureType of the Features to export.
     * 
     * The Export operation assumes that the feature returned by this function are valid to be added
     * to the current FeatureSource, and, therefore, performs no checking of FeatureType matching.
     * It is up to the user performing the export to ensure that the function actually generates
     * valid features for the current FeatureStore.
     * 
     * If no function is explicitly set, an identity function is used, and Features are not
     * converted.
     * 
     * This function can be used as a filter as well. If the returned object is Optional.absent, no
     * feature will be added
     * 
     * @param function
     * @return {@code this}
     */
    public ExportBatchDiffOp setFeatureTypeConversionFunction(
            Function<Feature, Optional<Feature>> function) {
        this.function = function == null ? IDENTITY : function;
        return this;
    }

    /**
     * @param transactional whether to use a geotools transaction for the operation, defaults to
     *        {@code true}
     */
    public ExportBatchDiffOp setTransactional(boolean transactional) {
        this.transactional = transactional;
        return this;
    }

    public ExportBatchDiffOp setOldRefIterator(Iterator<RevCommit> oldRefIterator) {
        this.oldRefIterator = oldRefIterator;
        return this;
    }

}