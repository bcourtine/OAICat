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
package org.oclc.oai.server;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.util.*;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.oclc.oai.server.catalog.AbstractCatalog;
import org.oclc.oai.server.verb.OAIInternalServerError;
import org.oclc.oai.server.verb.ServerVerb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAIHandler is the primary Servlet for OAICat.
 *
 * @author Jeffrey A. Young, OCLC Online Computer Library Center
 */
public class OAIHandler extends HttpServlet {

    private static final long serialVersionUID = 1L;

    public static final String PROPERTIES_SERVLET_CONTEXT_ATTRIBUTE = OAIHandler.class.getName() + ".properties";

    private static final String VERSION = "1.5.59";

    protected Map<String, Object> attributesMap = new HashMap<String, Object>();

    /** Class logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(OAIHandler.class);

    /** Get the VERSION number */
    public static String getVERSION() {
        return VERSION;
    }

    /**
     * init is called one time when the Servlet is loaded. This is the
     * place where one-time initialization is done. Specifically, we
     * load the properties file for this application, and create the
     * AbstractCatalog object for subsequent use.
     *
     * @param config servlet configuration information
     * @throws ServletException there was a problem with initialization
     */
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        try {
            Map<String, Object> attributes = null;
            ServletContext context = getServletContext();
            Properties properties = (Properties) context.getAttribute(PROPERTIES_SERVLET_CONTEXT_ATTRIBUTE);
            if (properties == null) {
                final String PROPERTIES_INIT_PARAMETER = "properties";
                LOGGER.debug("OAIHandler.init(..): No '" + PROPERTIES_SERVLET_CONTEXT_ATTRIBUTE + "' servlet context attribute. Trying to use init parameter '" + PROPERTIES_INIT_PARAMETER + "'");

                String fileName = config.getServletContext().getInitParameter(PROPERTIES_INIT_PARAMETER);
                InputStream in;
                try {
                    LOGGER.debug("fileName=" + fileName);
                    in = new FileInputStream(fileName);
                } catch (FileNotFoundException e) {
                    LOGGER.debug("file not found. Try the classpath: " + fileName);
                    in = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
                }
                if (in != null) {
                    LOGGER.debug("file was found: Load the properties");
                    properties = new Properties();
                    properties.load(in);
                    attributes = getAttributes(properties);
                    LOGGER.debug("OAIHandler.init: fileName=" + fileName);
                }
            } else {
                LOGGER.debug("Load context properties");
                attributes = getAttributes(properties);
            }

            LOGGER.debug("Store global properties");
            attributesMap.put("global", attributes);
        } catch (FileNotFoundException e) {
            LOGGER.error("An Exception occured", e);
            throw new ServletException(e.getMessage());
        } catch (ClassNotFoundException e) {
            LOGGER.error("An Exception occured", e);
            throw new ServletException(e.getMessage());
        } catch (IllegalArgumentException e) {
            LOGGER.error("An Exception occured", e);
            throw new ServletException(e.getMessage());
        } catch (IOException e) {
            LOGGER.error("An Exception occured", e);
            throw new ServletException(e.getMessage());
        } catch (Throwable e) {
            LOGGER.error("An Exception occured", e);
            throw new ServletException(e.getMessage());
        }
    }

    public Map<String, Object> getAttributes(Properties properties) throws Throwable {
        Map<String, Object> attributes = new HashMap<String, Object>();
        Enumeration attrNames = getServletContext().getAttributeNames();
        while (attrNames.hasMoreElements()) {
            String attrName = (String) attrNames.nextElement();
            attributes.put(attrName, getServletContext().getAttribute(attrName));
        }
        attributes.put("OAIHandler.properties", properties);

        String missingVerbClassName = properties.getProperty("OAIHandler.missingVerbClassName", "org.oclc.oai.server.verb.BadVerb");
        Class missingVerbClass = Class.forName(missingVerbClassName);
        attributes.put("OAIHandler.missingVerbClass", missingVerbClass);
        if (!"true".equals(properties.getProperty("OAIHandler.serviceUnavailable"))) {
            attributes.put("OAIHandler.version", VERSION);
            AbstractCatalog abstractCatalog = AbstractCatalog.factory(properties, getServletContext());
            attributes.put("OAIHandler.catalog", abstractCatalog);
        }
        boolean forceRender = false;
        if ("true".equals(properties.getProperty("OAIHandler.forceRender"))) {
            forceRender = true;
        }
        String xsltName = properties.getProperty("OAIHandler.styleSheet");
        String appBase = properties.getProperty("OAIHandler.appBase");
        if (appBase == null) {
            appBase = "webapps";
        }
        if (xsltName != null && ("true".equalsIgnoreCase(properties.getProperty("OAIHandler.renderForOldBrowsers")) || forceRender)) {
            InputStream is;
            try {
                is = new FileInputStream(appBase + "/" + xsltName);
            } catch (FileNotFoundException e) {
                // This is a silly way to skip the context name in the xsltName
                is = new FileInputStream(getServletContext().getRealPath(xsltName.substring(xsltName.indexOf("/", 1) + 1)));
            }
            StreamSource xslSource = new StreamSource(is);
            TransformerFactory tFactory = TransformerFactory.newInstance();
            Transformer transformer = tFactory.newTransformer(xslSource);
            attributes.put("OAIHandler.transformer", transformer);
        }
        return attributes;
    }

    public Map<String, Object> getAttributes(String pathInfo) {
        Map<String, Object> attributes = null;
        LOGGER.debug("pathInfo=" + pathInfo);
        if (pathInfo != null && pathInfo.length() > 0) {
            if (attributesMap.containsKey(pathInfo)) {
                LOGGER.debug("attributesMap containsKey");
                attributes = (HashMap) attributesMap.get(pathInfo);
            } else {
                LOGGER.debug("!attributesMap containsKey");
                try {
                    String fileName = pathInfo.substring(1) + ".properties";
                    LOGGER.debug("attempting load of " + fileName);
                    InputStream in = Thread.currentThread()
                            .getContextClassLoader()
                            .getResourceAsStream(fileName);
                    if (in != null) {
                        LOGGER.debug("file found");
                        Properties properties = new Properties();
                        properties.load(in);
                        attributes = getAttributes(properties);
                    } else {
                        LOGGER.warn("file not found");
                    }
                    attributesMap.put(pathInfo, attributes);
                } catch (Throwable e) {
                    LOGGER.error("Couldn't load file", e);
                    // do nothing
                }
            }
        }
        if (attributes == null) {
            LOGGER.debug("use global attributes");
        }
        attributes = (Map<String, Object>) attributesMap.get("global");
        return attributes;
    }

    /**
     * Peform the http GET action. Note that POST is shunted to here as well.
     * The verb widget is taken from the request and used to invoke an
     * OAIVerb object of the corresponding kind to do the actual work of the verb.
     *
     * @param request the servlet's request information
     * @param response the servlet's response information
     * @throws IOException an I/O error occurred
     */
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        Map<String, Object> attributes = getAttributes(request.getPathInfo());
        if (!filterRequest(request, response)) {
            return;
        }
        LOGGER.debug("attributes=" + attributes);
        Properties properties = (Properties) attributes.get("OAIHandler.properties");
        boolean monitor = false;
        if (properties.getProperty("OAIHandler.monitor") != null) {
            monitor = true;
        }
        boolean serviceUnavailable = isServiceUnavailable(properties);
        String extensionPath = properties.getProperty("OAIHandler.extensionPath", "/extension");

        Map<String, Class<?>> serverVerbs = ServerVerb.getVerbs(properties);
        Map<String, Class<?>> extensionVerbs = ServerVerb.getExtensionVerbs(properties);

        Transformer transformer = (Transformer) attributes.get("OAIHandler.transformer");

        boolean forceRender = false;
        if ("true".equals(properties.getProperty("OAIHandler.forceRender"))) {
            forceRender = true;
        }

        request.setCharacterEncoding("UTF-8");

        Date then = null;
        if (monitor) {
            then = new Date();
        }

        Enumeration headerNames = request.getHeaderNames();
        LOGGER.debug("OAIHandler.doGet: ");
        while (headerNames.hasMoreElements()) {
            String headerName = (String) headerNames.nextElement();
            LOGGER.debug(headerName + ":" + request.getHeader(headerName));
        }

        if (serviceUnavailable) {
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Sorry. This server is down for maintenance");
        } else {
            try {
                String userAgent = request.getHeader("User-Agent");
                if (userAgent == null) {
                    userAgent = "";
                } else {
                    userAgent = userAgent.toLowerCase();
                }
                Transformer serverTransformer = null;
                if (transformer != null) {
                    // return HTML if the client is an old browser
                    if (forceRender || userAgent.indexOf("opera") != -1 || (userAgent.startsWith("mozilla") && userAgent.indexOf("msie 6") == -1)) {
                        serverTransformer = transformer;
                    }
                }
                String result = getResult(attributes, request, response, serverTransformer, serverVerbs, extensionVerbs, extensionPath);

                Writer out = getWriter(request, response);
                out.write(result);
                out.close();
            } catch (FileNotFoundException e) {
                LOGGER.error("SC_NOT_FOUND: ", e);
                response.sendError(HttpServletResponse.SC_NOT_FOUND, e.getMessage());
            } catch (TransformerException e) {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            } catch (OAIInternalServerError e) {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            } catch (SocketException e) {
                LOGGER.debug(e.getMessage());
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            } catch (Throwable e) {
                LOGGER.error("An Exception occured", e);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }
        if (monitor) {
            StringBuilder reqUri = new StringBuilder(request.getRequestURI().toString());
            String queryString = request.getQueryString();   // d=789
            if (queryString != null) {
                reqUri.append("?").append(queryString);
            }
            Runtime rt = Runtime.getRuntime();
            LOGGER.debug(rt.freeMemory() + "/" + rt.totalMemory() + " " + ((new Date()).getTime() - then.getTime()) + "ms: " + reqUri.toString());
        }
    }

    /**
     * Should the server report itself down for maintenance? Override this
     * method if you want to do this check another way.
     *
     * @param properties
     * @return true=service is unavailable, false=service is available
     */
    protected boolean isServiceUnavailable(Properties properties) {
        if (properties.getProperty("OAIHandler.serviceUnavailable") != null) {
            return true;
        }
        return false;
    }

    /**
     * Override to do any prequalification; return false if
     * the response should be returned immediately, without
     * further action.
     *
     * @param request
     * @param response
     * @return false=return immediately, true=continue
     */
    protected boolean filterRequest(HttpServletRequest request, HttpServletResponse response) {
        return true;
    }

    public static String getResult(Map<String, Object> attributes, HttpServletRequest request, HttpServletResponse response, Transformer serverTransformer,
            Map<String, Class<?>> serverVerbs, Map<String, Class<?>> extensionVerbs, String extensionPath) throws Throwable {
        try {
            boolean isExtensionVerb = extensionPath.equals(request.getPathInfo());
            String verb = request.getParameter("verb");
            LOGGER.debug("OAIHandler.g<etResult: verb=>" + verb + "<");
            String result;
            Class verbClass = null;
            if (isExtensionVerb) {
                verbClass = extensionVerbs.get(verb);
            } else {
                verbClass = serverVerbs.get(verb);
            }
            if (verbClass == null) {
                verbClass = (Class) attributes.get("OAIHandler.missingVerbClass");
            }
            Method construct = verbClass.getMethod("construct", new Class[]{HashMap.class, HttpServletRequest.class, HttpServletResponse.class, Transformer.class});
            try {
                result = (String) construct.invoke(null, new Object[]{attributes, request, response, serverTransformer});
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
            LOGGER.debug(result);
            return result;
        } catch (NoSuchMethodException e) {
            throw new OAIInternalServerError(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new OAIInternalServerError(e.getMessage());
        }
    }

    /**
     * Get a response Writer depending on acceptable encodings
     *
     * @param request the servlet's request information
     * @param response the servlet's response information
     * @throws IOException an I/O error occurred
     */
    public static Writer getWriter(HttpServletRequest request, HttpServletResponse response) throws IOException {
        Writer out;
        String encodings = request.getHeader("Accept-Encoding");
        LOGGER.debug("encodings=" + encodings);
        if (encodings != null && encodings.indexOf("gzip") != -1) {
            response.setHeader("Content-Encoding", "gzip");
            out = new OutputStreamWriter(new GZIPOutputStream(response.getOutputStream()), "UTF-8");
        } else if (encodings != null && encodings.indexOf("deflate") != -1) {
            response.setHeader("Content-Encoding", "deflate");
            out = new OutputStreamWriter(new DeflaterOutputStream(response.getOutputStream()), "UTF-8");
        } else {
            out = response.getWriter();
        }
        return out;
    }

    /**
     * Peform a POST action. Actually this gets shunted to GET
     *
     * @param request the servlet's request information
     * @param response the servlet's response information
     * @throws IOException an I/O error occurred
     */
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        doGet(request, response);
    }
}
