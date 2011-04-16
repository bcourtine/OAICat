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
package org.oclc.oai.server.catalog;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;

import org.apache.xpath.XPathAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/** NodeRecordFactory converts native XML "items" to "record" Strings. */
public class XerSRURecordFactory extends RecordFactory {

    /** Class logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(XerSRURecordFactory.class);

    private static Element xmlnsEl = null;
    private static DocumentBuilderFactory factory = null;

    static {
        try {
            factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            DOMImplementation impl = builder.getDOMImplementation();
            Document xmlnsDoc = impl.createDocument("http://www.oclc.org/research/software/oai/harvester", "harvester:xmlnsDoc", null);
            xmlnsEl = xmlnsDoc.getDocumentElement();
            xmlnsEl.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:xsd", "http://www.w3.org/2001/XMLSchema");
            xmlnsEl.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:srw", "http://www.loc.gov/zing/srw/");
            xmlnsEl.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:oai", "http://www.openarchives.org/OAI/2.0/");
            xmlnsEl.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:explain", "http://explain.z3950.org/dtd/2.0/");
        } catch (Exception e) {
            LOGGER.error("An Exception occured", e);
        }
    }

    public XerSRURecordFactory(Properties properties)
            throws IllegalArgumentException {
        super(properties);
//        this(properties, getCrosswalkMap(properties.getProperty("SRUOAICatalog.sruURL")));
    }

    /**
     * Construct an NodeRecordFactory capable of producing the Crosswalk(s)
     * specified in the properties file.
     *
     * @param properties Contains information to configure the factory:
     * specifically, the names of the crosswalk(s) supported
     * @throws IllegalArgumentException Something is wrong with the argument.
     */
    public XerSRURecordFactory(Properties properties, HashMap crosswalkMap)
            throws IllegalArgumentException {
        super(crosswalkMap);
    }

    /**
     * Utility method to parse the 'local identifier' from the OAI identifier
     *
     * @param identifier OAI identifier (e.g. oai:oaicat.oclc.org:ID/12345)
     * @return local identifier (e.g. ID/12345).
     */
    public String fromOAIIdentifier(String identifier) {
        return identifier;
    }

    /**
     * Construct an OAI identifier from the native item
     *
     * @param nativeItem native Item object
     * @return OAI identifier
     */
    public String getOAIIdentifier(Object nativeItem) {
        return getLocalIdentifier(nativeItem);
    }

    /**
     * Extract the local identifier from the native item
     *
     * @param nativeItem native Item object
     * @return local identifier
     */
    public String getLocalIdentifier(Object nativeItem) {
        try {
            Element recordEl = (Element) nativeItem;
            return "foo";
        } catch (Exception e) {
            LOGGER.error("An Exception occured", e);
        }
        return null;
    }

    /**
     * get the datestamp from the item
     *
     * @param nativeItem a native item presumably containing a datestamp somewhere
     * @return a String containing the datestamp for the item
     * @throws IllegalArgumentException Something is wrong with the argument.
     */
    public String getDatestamp(Object nativeItem)
            throws IllegalArgumentException {
        Element recordData = (Element) nativeItem;
        try {
            String datetime = XPathAPI.eval(recordData,
                    "/ber/tag0/tag0[@class='private'/tag0]/",
                    xmlnsEl)
                    .str();
            return datetime;
        } catch (TransformerException e) {
            LOGGER.error("An Exception occured", e);
        }
        return null;
    }

    /**
     * get the setspec from the item
     *
     * @param nativeItem a native item presumably containing a setspec somewhere
     * @return a String containing the setspec for the item
     * @throws IllegalArgumentException Something is wrong with the argument.
     */
    public Iterator getSetSpecs(Object nativeItem) throws IllegalArgumentException {
        return null;
    }

    /**
     * Get the about elements from the item
     *
     * @param nativeItem a native item presumably containing about information somewhere
     * @return a Iterator of Strings containing &lt;about&gt;s for the item
     * @throws IllegalArgumentException Something is wrong with the argument.
     */
    public Iterator getAbouts(Object nativeItem) throws IllegalArgumentException {
        return null;
    }

    /**
     * Is the record deleted?
     *
     * @param nativeItem a native item presumably containing a possible delete indicator
     * @return true if record is deleted, false if not
     * @throws IllegalArgumentException Something is wrong with the argument.
     */
    public boolean isDeleted(Object nativeItem) throws IllegalArgumentException {
        return false;
    }

    /**
     * Allows classes that implement RecordFactory to override the default create() method.
     * This is useful, for example, if the entire &lt;record&gt; is already packaged as the native
     * record. Return null if you want the default handler to create it by calling the methods
     * above individually.
     *
     * @param nativeItem the native record
     * @return a String containing the OAI &lt;record&gt; or null if the default method should be used.
     */
    public String quickCreate(Object nativeItem, String schemaLocation, String metadataPrefix) {
        return null;
    }
}
