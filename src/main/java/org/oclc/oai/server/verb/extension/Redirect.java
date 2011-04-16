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
package org.oclc.oai.server.verb.extension;

import java.io.FileNotFoundException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;

import org.oclc.oai.server.catalog.AbstractCatalog;
import org.oclc.oai.server.crosswalk.Crosswalks;
import org.oclc.oai.server.verb.BadArgumentException;
import org.oclc.oai.server.verb.BadVerb;
import org.oclc.oai.server.verb.CannotDisseminateFormatException;
import org.oclc.oai.server.verb.IdDoesNotExistException;
import org.oclc.oai.server.verb.OAIInternalServerError;
import org.oclc.oai.server.verb.ServerVerb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a Redirect response on either the server or
 * the client.
 *
 * @author Jeffrey A. Young, OCLC Online Computer Library Center
 */
public class Redirect extends ServerVerb {

    /** Class logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(Redirect.class);

    private static Transformer transformer;

    static {
        String xsltString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\"\n"
                + "                              xmlns:oai=\"http://www.openarchives.org/OAI/2.0/\"\n"
                + "                              xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\"\n"
                + "                              xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\n"
                + "  <xsl:output method=\"html\" version=\"4.0\"/>\n"
                + "  <xsl:param name=\"base.url\"/>\n"
                + "\n"
                + "  <xsl:template match=\"/\">\n"
                + "    <html xmlns=\"http://www.w3.org/1999/xhtml\">\n"
                + "      <xsl:choose>\n"
                + "        <xsl:when test=\"record/metadata/oai_dc:dc/dc:identifier[1]\">\n"
                + "          <head>\n"
                + "            <meta http-equiv=\"Refresh\">\n"
                + "              <xsl:attribute name=\"content\"><xsl:text>0; URL=</xsl:text><xsl:value-of select=\"record/metadata/oai_dc:dc/dc:identifier[1]\" /></xsl:attribute>\n"
                + "            </meta>\n"
                + "          </head>\n"
                + "          <body/>\n"
                + "       </xsl:when>\n"
                + "       <xsl:when test=\"$base.url\">\n"
                + "          <head>\n"
                + "            <meta http-equiv=\"Refresh\">\n"
                + "              <xsl:attribute name=\"content\"><xsl:text>0; URL=</xsl:text><xsl:value-of select=\"$base.url\" /><xsl:text>/extension?verb=GetMetadata&amp;metadataPrefix=oai_dc&amp;identifier=</xsl:text><xsl:value-of select=\"record/header/identifier\" /></xsl:attribute>\n"
                + "            </meta>\n"
                + "          </head>\n"
                + "          <body/>\n"
                + "       </xsl:when>\n"
                + "       <xsl:otherwise>\n"
                + "          <head>\n"
                + "            <title><xsl:text>No dc:identifier field found for &apos;</xsl:text><xsl:value-of select=\"record/header/identifier\"/><xsl:text>&apos;</xsl:text></title>\n"
                + "          </head>\n"
                + "          <body>\n"
                + "            <h1><xsl:text>No dc:identifier field found for &apos;</xsl:text><xsl:value-of select=\"record/header/identifier\"/><xsl:text>&apos;</xsl:text></h1>\n"
                + "          </body>\n"
                + "       </xsl:otherwise>\n"
                + "      </xsl:choose>\n"
                + "    </html>\n"
                + "  </xsl:template>\n"
                + "</xsl:stylesheet>\n";
        try {
            LOGGER.debug("Redirect.<init>: xsltString=" + xsltString);
            StreamSource xslSource = new StreamSource(new StringReader(xsltString));
            TransformerFactory tFactory = TransformerFactory.newInstance();
            transformer = tFactory.newTransformer(xslSource);
        } catch (TransformerException e) {
            LOGGER.error("An Exception occured", e);
        }
    }

    private static List<String> validParamNames = new ArrayList<String>();

    static {
        validParamNames.add("verb");
        validParamNames.add("identifier");
    }

    /**
     * Construct the xml response on the server-side.
     *
     * @param context the servlet context
     * @param request the servlet request
     * @return a String containing the XML response
     * @throws OAIInternalServerError an http 500 status error occurred
     */
    public static String construct(HashMap context, HttpServletRequest request, HttpServletResponse response, Transformer serverTransformer)
            throws FileNotFoundException, TransformerException {
        Properties properties = (Properties) context.get("OAIHandler.properties");
        AbstractCatalog abstractCatalog = (AbstractCatalog) context.get("OAIHandler.catalog");
        String baseURL = properties.getProperty("OAIHandler.baseURL");
        if (baseURL == null) {
            try {
                baseURL = request.getRequestURL().toString();
            } catch (java.lang.NoSuchMethodError f) {
                baseURL = request.getRequestURL().toString();
            }
        }
        StringBuilder sb = new StringBuilder();
        String identifier = request.getParameter("identifier");
        LOGGER.debug("Redirect.construct: identifier=" + identifier);
        Crosswalks crosswalks = abstractCatalog.getCrosswalks();
        try {
            if (identifier == null || identifier.length() == 0) {
                LOGGER.warn("Bad argument");
                throw new BadArgumentException();
            } else if (!crosswalks.containsValue("oai_dc")) {
                LOGGER.warn("crosswalk not present: oai_dc");
                throw new CannotDisseminateFormatException("oai_dc");
            } else {
                String metadata = abstractCatalog.getRecord(identifier, "oai_dc");
                if (metadata != null) {
                    sb.append(metadata);
                } else {
                    LOGGER.warn("ID does not exist");
                    throw new IdDoesNotExistException(identifier);
                }
            }
        } catch (BadArgumentException e) {
            LOGGER.error("An Exception occured", e);
            throw new FileNotFoundException(e.getMessage());
        } catch (CannotDisseminateFormatException e) {
            LOGGER.error("An Exception occured", e);
            throw new FileNotFoundException(e.getMessage());
        } catch (IdDoesNotExistException e) {
            LOGGER.error("An Exception occured", e);
            throw new FileNotFoundException(e.getMessage());
        } catch (OAIInternalServerError e) {
            LOGGER.error("An Exception occured", e);
            return BadVerb.construct(context, request, response, serverTransformer);
        }
        LOGGER.debug("Redirect.construct: prerendered sb=" + sb.toString());
        LOGGER.debug("Redirect.construct: transformer=" + transformer);
        synchronized (transformer) {
            transformer.setParameter("base.url", baseURL);
            String out = render(response, (String) null, sb.toString(), transformer);
            transformer.clearParameters();
            LOGGER.debug("Redirect.construct: out=" + out);
            return out;
        }
    }
}
