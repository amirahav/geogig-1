package org.locationtech.geogig.rest.zip;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.geotools.data.DataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.locationtech.geogig.api.AbstractGeoGigOp;
import org.locationtech.geogig.geotools.plumbing.DataStoreExportOp;
import org.locationtech.geogig.geotools.zip.ZipDataStoreExportOp;
import org.locationtech.geogig.rest.AsyncCommandRepresentation;
import org.locationtech.geogig.rest.AsyncContext.AsyncCommand;
import org.locationtech.geogig.rest.CommandRepresentationFactory;
import org.locationtech.geogig.rest.Variants;
import org.locationtech.geogig.rest.geotools.Export;
import org.locationtech.geogig.rest.repository.RESTUtils;
import org.locationtech.geogig.web.api.CommandContext;
import org.locationtech.geogig.web.api.ParameterSet;
import org.restlet.data.MediaType;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;


public class ZipExportOutputFormat extends Export.OutputFormat {
    public static final String PATH_PARAM = "path";

    public static final String DOITT_IMPORT_PARAM = "doitt";

    private TempShpSupplier dataStore;

    public String path;

    public Boolean doitt;

    private static class TempShpSupplier implements Supplier<DataStore> {

        private File targetFile;

        private DataStore dataStore;

        private String path;

        public TempShpSupplier(String path) {
            this.path = path;
        }

        public File getTargetFile() {
            if (targetFile == null) {
                try {
                    Path myTempDir = Files.createTempDirectory("zipshpexport");
                    File shppath = myTempDir.toFile();
                    targetFile = new File(myTempDir.toString(), path + ".shp");
                    targetFile.deleteOnExit();

                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
            return targetFile;
        }

        @Override
        public DataStore get() {
            if (dataStore == null) {
                final File shapeFile = getTargetFile();

                final ShapefileDataStoreFactory factory = new ShapefileDataStoreFactory();
                DataStore dataStore;
                try {
                    final Map<String, Serializable> params = ImmutableMap.of("url", shapeFile
                            .toURI().toURL(), "create spatial index", Boolean.TRUE);

                    dataStore = factory.createDataStore(params);
                } catch (IOException ioe) {
                    throw new RuntimeException("Unable to create ShapeFileDataStore", ioe);
                }
                if (null == dataStore) {
                    throw new RuntimeException("Unable to create ShapeFileDataStore");
                }
                this.dataStore = dataStore;

            }
            return dataStore;
        }

    }



    public ZipExportOutputFormat(ParameterSet options) {
        this.path = options.getFirstValue(PATH_PARAM);
        this.doitt = Boolean.parseBoolean(options.getFirstValue(DOITT_IMPORT_PARAM));
        this.dataStore = new TempShpSupplier(this.path);
    }

    @Override
    public String getCommandDescription() {
        return "Export to Shapefile in ZIP";
    }

    @Override
    public DataStoreExportOp<File> createCommand(final CommandContext context) {
        boolean doitt = this.doitt;
        return context.getGeoGIG().command(ZipDataStoreExportOp.class)
                .setShapeFile(dataStore.getTargetFile()).setDoittImport(doitt);
    }

    @Override
    public Supplier<DataStore> getDataStore() {
        return dataStore;
    }

    public static class RepresentationFactory implements CommandRepresentationFactory<File> {

        @Override
        public boolean supports(Class<? extends AbstractGeoGigOp<?>> cmdClass) {
            return ZipDataStoreExportOp.class.equals(cmdClass);
        }

        @Override
        public AsyncCommandRepresentation<File> newRepresentation(AsyncCommand<File> cmd,
                MediaType mediaType, String baseURL) {

            return new ZipExportRepresentation(mediaType, cmd, baseURL);
        }
    }

    public static class ZipExportRepresentation extends AsyncCommandRepresentation<File> {

        public ZipExportRepresentation(MediaType mediaType, AsyncCommand<File> cmd,
                String baseURL) {
            super(mediaType, cmd, baseURL);
        }

        @Override
        protected void writeResultBody(XMLStreamWriter w, File result) throws XMLStreamException {

            final String link = "tasks/" + super.cmd.getTaskId() + "/download";
            encodeDownloadURL(w, link);

        }

        private void encodeDownloadURL(XMLStreamWriter w, String link) throws XMLStreamException {

            final MediaType format = getMediaType();
            final MediaType outputFormat = Variants.ZIP.getMediaType();

            if (MediaType.TEXT_XML.equals(format) || MediaType.APPLICATION_XML.equals(format)) {
                w.writeStartElement("atom:link");
                w.writeAttribute("xmlns:atom", "http://www.w3.org/2005/Atom");
                w.writeAttribute("rel", "alternate");
                w.writeAttribute("href", RESTUtils.buildHref(baseURL, link, null));
                w.writeAttribute("type", outputFormat.toString());
                w.writeEndElement();
            } else if (MediaType.APPLICATION_JSON.equals(format)) {
                element(w, "href", RESTUtils.buildHref(baseURL, link, null));
            }
        }
    }
}
