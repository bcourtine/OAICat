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

import java.io.FileInputStream;
import java.util.Properties;

import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;

import org.oclc.oai.server.verb.OAIInternalServerError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert native "item" to oai_dc. In this case, the native "item"
 * is assumed to already be formatted as an OAI <record> element,
 * with the possible exception that multiple metadataFormats may
 * be present in the <metadata> element. The "crosswalk", merely
 * involves pulling out the one that is requested.
 */
public class XSLToai_dcCrosswalk extends XSLTCrosswalk {

    /** Class logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(XSLToai_dcCrosswalk.class);
    
    /**
     * The constructor assigns the schemaLocation associated with this crosswalk. Since
     * the crosswalk is trivial in this case, no properties are utilized.
     *
     * @param properties properties that are needed to configure the crosswalk.
     */
    public XSLToai_dcCrosswalk(Properties properties) throws OAIInternalServerError {
 	super(properties, "http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd", (String)null);
        try {
            String xsltName = properties.getProperty("XSLToai_dcCrosswalk.xsltName");
            LOGGER.debug("XSLToai_dcCrosswalk.XSLToai_dcCrosswalk: xsltName=" + xsltName);
            if (xsltName != null) {
                StreamSource xslSource = new StreamSource(new FileInputStream(xsltName));
                TransformerFactory tFactory = TransformerFactory.newInstance();
                this.transformer = tFactory.newTransformer(xslSource);
            }
        } catch (Exception e) {
            LOGGER.error("An Exception occured", e);
            throw new OAIInternalServerError(e.getMessage());
        }
    }
}
