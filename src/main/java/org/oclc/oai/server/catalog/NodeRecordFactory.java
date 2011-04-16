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

import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;

import org.apache.xpath.XPathAPI;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXParseException;

import org.oclc.oai.server.crosswalk.CrosswalkItem;
import org.oclc.oai.server.crosswalk.NodePassThruCrosswalk;
import org.oclc.oai.server.verb.OAIInternalServerError;

/** NodeRecordFactory converts native XML "items" to "record" Strings. */
public class NodeRecordFactory extends RecordFactory {

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
            // xmlnsEl.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:mx", "http://www.loc.gov/MARC21/slim");
            xmlnsEl.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:xsd", "http://www.w3.org/2001/XMLSchema");
            xmlnsEl.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:srw", "http://www.loc.gov/zing/srw/");
            xmlnsEl.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:oai", "http://www.openarchives.org/OAI/2.0/");
            xmlnsEl.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:explain", "http://explain.z3950.org/dtd/2.0/");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public NodeRecordFactory(Properties properties) throws IllegalArgumentException {
        this(properties, getCrosswalkMap(properties.getProperty("SRUOAICatalog.sruURL"), false));
    }

    /**
     * Construct an NodeRecordFactory capable of producing the Crosswalk(s)
     * specified in the properties file.
     *
     * @param properties Contains information to configure the factory:
     * specifically, the names of the crosswalk(s) supported
     * @throws IllegalArgumentException Something is wrong with the argument.
     */
    public NodeRecordFactory(Properties properties, Map<String, Object> crosswalkMap) throws IllegalArgumentException {
        super(crosswalkMap);
    }

    private static Map<String, Object> getCrosswalkMap(String sruURL, boolean enrich)
            throws IllegalArgumentException {
        try {
            DocumentBuilder parser = factory.newDocumentBuilder();
            System.out.println("sruURL=" + sruURL);
            Document explainDoc = parser.parse(sruURL);
            System.out.println("explainDoc=" + explainDoc);
            NodeList schemas = XPathAPI.selectNodeList(explainDoc, "/srw:explainResponse/srw:record/srw:recordData/explain:explain/explain:schemaInfo/explain:schema", xmlnsEl);

            // Load the formats the repository supports directly
            Map<String, Object> crosswalkMap = new HashMap<String, Object>();
            for (int i = 0; i < schemas.getLength(); ++i) {
                Object[] crosswalkItem = crosswalkItemFactory(schemas.item(i));
                for (int j = 0; j < crosswalkItem.length; ++j) {
                    CrosswalkItem currentItem = (CrosswalkItem) crosswalkItem[j];
                    // 		logger.debug(currentItem.toString());
                    String key = currentItem.getMetadataPrefix();
                    CrosswalkItem storedItem = (CrosswalkItem) crosswalkMap.get(key);
                    if (storedItem == null || (currentItem.getRank() < storedItem.getRank())) {
                        crosswalkMap.put(key, currentItem);
                    }
                }
            }

            Map<String, Object> moreCrosswalkMap = new HashMap<String, Object>();

            // Now, combine the lists with the originals taking precidence
            moreCrosswalkMap.putAll(crosswalkMap);
            return moreCrosswalkMap;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException(sruURL);
        }
    }

    public static Object[] crosswalkItemFactory(Node explainSchemaNode) throws TransformerException, OAIInternalServerError {
        List<CrosswalkItem> crosswalkItemList = new ArrayList<CrosswalkItem>();
        String nativeRecordSchema = XPathAPI.eval(explainSchemaNode, "@identifier", xmlnsEl).str();
        String metadataPrefix = XPathAPI.eval(explainSchemaNode, "@name", xmlnsEl).str();
        String schema = XPathAPI.eval(explainSchemaNode, "@location", xmlnsEl).str();
        String metadataNamespace = null;
        try {
            DocumentBuilder parser = factory.newDocumentBuilder();
            Document schemaDoc = parser.parse(schema);
            metadataNamespace = XPathAPI.eval(schemaDoc, "/xsd:schema/@targetNamespace", xmlnsEl).str();
        } catch (SAXParseException e) {
            metadataNamespace = "";
        } catch (Exception e) {
            System.out.println("Failed to get schema: " + schema);
            e.printStackTrace();
            metadataNamespace = "";
        }
        CrosswalkItem crosswalkItem = new CrosswalkItem(nativeRecordSchema, metadataPrefix, schema, metadataNamespace, NodePassThruCrosswalk.class);
        crosswalkItemList.add(crosswalkItem);
        return crosswalkItemList.toArray();
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
        Map<String, Object> hashMap = (Map<String, Object>) nativeItem;
        Node dataNode = (Node) hashMap.get("header");
        try {
            Element recordEl = (Element) dataNode;
            Node identifierNode = XPathAPI.selectSingleNode(recordEl, "/oai:header/oai:identifier", xmlnsEl);
            if (identifierNode != null) {
                return XPathAPI.eval(identifierNode, "string()").str();
            }
        } catch (Exception e) {
            e.printStackTrace();
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
    public String getDatestamp(Object nativeItem) throws IllegalArgumentException {
        Map<String, Object> hashMap = (Map<String, Object>) nativeItem;
        Node dataNode = (Node) hashMap.get("header");
        try {
            Element recordEl = (Element) dataNode;
            Node datestampNode = XPathAPI.selectSingleNode(recordEl, "/oai:header/oai:datestamp", xmlnsEl);
            if (datestampNode != null) {
                return XPathAPI.eval(datestampNode, "string()").str();
            }
        } catch (Exception e) {
            e.printStackTrace();
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
     * @return a String containing the OAI &lt;record&gt; or null if the default method should be
     *         used.
     */
    public String quickCreate(Object nativeItem, String schemaLocation, String metadataPrefix) {
        return null;
    }
}
