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
package org.oclc.oai.server.crosswalk;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

/**
 * Convert native "item" to oai_dc. In this case, the native "item"
 * is assumed to already be formatted as an OAI <record> element,
 * with the possible exception that multiple metadataFormats may
 * be present in the <metadata> element. The "crosswalk", merely
 * involves pulling out the one that is requested.
 */
public class NodePassThruCrosswalk extends Crosswalk {

    /** Class logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(NodePassThruCrosswalk.class);

    private static Transformer transformer = null;

    /**
     * The constructor assigns the schemaLocation associated with this crosswalk. Since
     * the crosswalk is trivial in this case, no properties are utilized.
     *
     * @param crosswalkItem
     */
    public NodePassThruCrosswalk(CrosswalkItem crosswalkItem) {
        super(crosswalkItem.getMetadataNamespace() + " " + crosswalkItem.getSchema());

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        try {
            transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.setOutputProperty(OutputKeys.STANDALONE, "no");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        } catch (Exception e) {
            LOGGER.error("An Exception occured", e);

        }
    }

    /**
     * Can this nativeItem be represented in DC format?
     *
     * @param nativeItem a record in native format
     * @return true if DC format is possible, false otherwise.
     */
    public boolean isAvailableFor(Object nativeItem) {
        return true;
    }

    /**
     * Perform the actual crosswalk.
     *
     * @param nativeItem the native "item". In this case, it is
     * already formatted as an OAI <record> element, with the
     * possible exception that multiple metadataFormats are
     * present in the <metadata> element.
     * @return a String containing the XML to be stored within the <metadata> element.
     */
    public String createMetadata(Object nativeItem) {
        try {
            Map<String, Object> hashMap = (HashMap) nativeItem;
            Element dataNode = (Element) hashMap.get("metadata");
            DOMSource source = new DOMSource(dataNode);
            StringWriter sw = new StringWriter();
            StreamResult result = new StreamResult(sw);
            synchronized (transformer) {
                transformer.transform(source, result);
            }
            return sw.toString();
        } catch (Exception e) {
            LOGGER.error("An Exception occured", e);
            return e.getMessage();
        }
    }
}
