/**
 * Copyright 2006 OCLC Online Computer Library Center Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or
 * agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.oclc.oai.server.catalog.helpers;

import java.io.StringWriter;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import org.oclc.oai.util.OAIUtil;

public class RecordStringHandler extends DefaultHandler {

    /** Class logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordStringHandler.class);

    private static final String OAI_NS = "http://www.openarchives.org/OAI/2.0/";
    private static final String DATABASE_NS = "http://www.oclc.org/pears/";
    // private static final String OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/";
    // private static final String MARC21_NS = "http://www.loc.gov/MARC21/slim";
    // private static final String REG_NS = "http://info-uri.info/registry";
    // private static final String MTX_NS = "http://www.w3.org/1999/xhtml";
    // private static final String PRO_NS = "info:ofi/pro";
    // private static final String XSD_NS = "http://www.w3.org/2001/XMLSchema";
    // private static final String XSL_NS = "http://www.w3.org/1999/XSL/Transform";
    private static final String XSI_NS = "http://www.w3.org/2001/XMLSchema-instance";
    private SortedMap<String, Object> nativeRecords = new TreeMap<String, Object>();
    private int recordFlag = 0;
    private int metadataFlag = 0;
    private StringWriter metadata = null;
    private int recordidFlag = 0;
    private StringBuilder recordid = null;
    private String schemaLocation = null;
    private int identifierFlag = 0;
    private StringBuilder identifier = null;
    private int datestampFlag = 0;
    private StringBuilder datestamp = null;
    private List<String> setSpecs = null;
    private int setSpecFlag = 0;
    private StringBuilder setSpec = null;

    public SortedMap<String, Object> getNativeRecords() {
        return nativeRecords;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attrs) {

        LOGGER.debug("startElement: " + uri + ", " + localName + ", " + qName + ", ");

        if (OAI_NS.equals(uri) && "record".equals(localName)) {
            setSpecs = new ArrayList<String>();
            recordFlag++;
        }
        if (metadataFlag > 0) {
            metadata.write("<" + getName(localName, qName));
            if (attrs != null) {
                for (int i = 0; i < attrs.getLength(); ++i) {
                    String attributeName = getName(attrs.getLocalName(i), attrs.getQName(i));

                    // modified by Colin DOig, 6 September 2006
                    // xmlEncode ",&,< etc within attributes
                    // previously invalid XML was being produced.
                    metadata.write(" " + attributeName + "=\"" + OAIUtil.xmlEncode(attrs.getValue(i)) + "\"");
                }
            }
            metadata.write(">");
        }
        if (schemaLocation == null && metadataFlag == 1) {
            schemaLocation = attrs.getValue(XSI_NS, "schemaLocation");
        }
        if (OAI_NS.equals(uri) && "metadata".equals(localName)) {
            if (metadata == null) {
                metadata = new StringWriter();
            }
            metadataFlag++;
        }
        if (OAI_NS.equals(uri) && "identifier".equals(localName)) {
            if (identifier == null) {
                identifier = new StringBuilder();
            }
            identifierFlag++;
        }
        if (DATABASE_NS.equals(uri) && "recordid".equals(localName)) {
            if (recordid == null) {
                recordid = new StringBuilder();
            }
            recordidFlag++;
        }
        if (OAI_NS.equals(uri) && "datestamp".equals(localName)) {
            if (datestamp == null) {
                datestamp = new StringBuilder();
            }
            datestampFlag++;
        }
        if (OAI_NS.equals(uri) && "setSpec".equals(localName)) {
            if (setSpec == null) {
                setSpec = new StringBuilder();
            }
            setSpecFlag++;
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
        if (OAI_NS.equals(uri) && "identifier".equals(localName)) {
            identifierFlag--;
        }
        if (DATABASE_NS.equals(uri) && "recordid".equals(localName)) {
            recordidFlag--;
        }
        if (OAI_NS.equals(uri) && "datestamp".equals(localName)) {
            datestampFlag--;
        }
        if (OAI_NS.equals(uri) && "setSpec".equals(localName)) {
            setSpecs.add(setSpec.toString());
            setSpec = null;
            setSpecFlag--;
        }
        if (OAI_NS.equals(uri) && "record".equals(localName)) {
            recordFlag--;
            if (recordFlag == 0) {
                Map<String, Object> nativeRecord = new HashMap<String, Object>();
                nativeRecord.put("recordString", metadata.toString());
                LOGGER.debug("metadata: " + metadata.toString());
                nativeRecord.put("localIdentifier", identifier.toString());
                LOGGER.debug("localIdentifier=" + identifier.toString());
                nativeRecord.put("recordid", recordid.toString());
                LOGGER.debug("recordid=" + recordid.toString());
                nativeRecord.put("schemaLocation", schemaLocation);
                LOGGER.debug("schemaLocation=" + schemaLocation);
                nativeRecord.put("datestamp", datestamp.toString());
                LOGGER.debug("datestamp=" + datestamp.toString());
                nativeRecord.put("setSpecs", setSpecs);
                nativeRecords.put(recordid.toString().toLowerCase(), nativeRecord);
                setSpecs = null;
                identifier = null;
                metadata = null;
                recordid = null;
                schemaLocation = null;
                datestamp = null;
            }
        }
        if (OAI_NS.equals(uri) && "metadata".equals(localName)) {
            metadataFlag--;
        }
        if (metadataFlag > 0) {
            metadata.write("</" + getName(localName, qName) + ">");
        }
    }

    public void characters(char[] ch, int start, int length) {
        String s = new String(ch, start, length);
        if (metadataFlag > 0) {
            metadata.write(OAIUtil.xmlEncode(s));
        }
        if (identifierFlag > 0) {
            identifier.append(s);
        }
        if (recordidFlag > 0) {
            recordid.append(s);
        }
        if (datestampFlag > 0) {
            datestamp.append(s);
        }
        if (setSpecFlag > 0) {
            setSpec.append(s);
        }
    }

    private String getName(String s1, String s2) {
        return (s2 == null || "".equals(s2)) ? s1 : s2;
    }
}
