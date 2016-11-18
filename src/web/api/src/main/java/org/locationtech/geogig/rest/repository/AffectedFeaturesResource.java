/* Copyright (c) 2014-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.rest.repository;

import static org.locationtech.geogig.web.api.RESTUtils.getGeogig;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;

import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevCommit;
import org.locationtech.geogig.porcelain.DiffOp;
import org.locationtech.geogig.repository.AutoCloseableIterator;
import org.locationtech.geogig.repository.DiffEntry;
import org.locationtech.geogig.repository.Repository;
import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.OutputRepresentation;
import org.restlet.resource.Resource;
import org.restlet.resource.Variant;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Returns a list of all feature ids affected by a specified commit.
 */
public class AffectedFeaturesResource extends Resource {

    @Override
    public void init(Context context, Request request, Response response) {
        super.init(context, request, response);
        List<Variant> variants = getVariants();
        variants.add(new AffectedFeaturesRepresentation(request));
    }

    private static class AffectedFeaturesRepresentation extends OutputRepresentation {

        private Request request;

        public AffectedFeaturesRepresentation(Request request) {
            super(MediaType.TEXT_PLAIN);
            this.request = request;
        }

        @Override
        public void write(OutputStream out) throws IOException {
            PrintWriter w = new PrintWriter(out);
            Form options = request.getResourceRef().getQueryAsForm();

            Optional<String> commit = Optional
                    .fromNullable(options.getFirstValue("commitId", null));

            Preconditions.checkState(commit.isPresent(), "No commit specified.");

            Repository repo = getGeogig(request).get();

            ObjectId commitId = ObjectId.valueOf(commit.get());

            RevCommit revCommit = repo.getCommit(commitId);

            if (revCommit.getParentIds() != null && revCommit.getParentIds().size() > 0) {
                ObjectId parentId = revCommit.getParentIds().get(0);
                try (final AutoCloseableIterator<DiffEntry> diff = repo.command(DiffOp.class)
                        .setOldVersion(parentId).setNewVersion(commitId).call()) {
                    while (diff.hasNext()) {
                        DiffEntry diffEntry = diff.next();
                        if (diffEntry.getOldObject() != null) {
                            w.write(diffEntry.getOldObject().getNode().getObjectId().toString()
                                    + "\n");
                        }
                    }
                    w.flush();
                }
            }
        }
    }
}
