package org.apache.poi.poifs.poibrowser;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.poi.hpsf.MarkUnsupportedException;
import org.apache.poi.hpsf.NoPropertySetStreamException;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSDocumentPath;


public class PropertySetDescriptor extends DocumentDescriptor {
	protected PropertySet propertySet;

	public PropertySet getPropertySet() {
		return propertySet;
	}

	public PropertySetDescriptor(final String name, final POIFSDocumentPath path, final DocumentInputStream stream, final int nrOfBytesToDump) throws IOException, UnsupportedEncodingException, MarkUnsupportedException, NoPropertySetStreamException {
		super(name, path, stream, nrOfBytesToDump);
		propertySet = PropertySetFactory.create(stream);
	}
}

