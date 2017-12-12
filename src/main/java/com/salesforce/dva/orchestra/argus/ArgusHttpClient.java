/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * 3. Neither the name of Salesforce.com nor the names of its contributors may
 * be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.salesforce.dva.orchestra.argus;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.salesforce.dva.orchestra.OrchestraException;
import com.salesforce.dva.orchestra.argus.entity.Annotation;
import com.salesforce.dva.orchestra.argus.entity.Credentials;
import com.salesforce.dva.orchestra.argus.entity.Metric;
import com.salesforce.dva.orchestra.util.Configuration;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.salesforce.dva.orchestra.util.Assert.requireArgument;

/**
 * HTTP based API client for Argus.
 *
 * @author  Sharanya Santhanam (ssanthanam@salesforce.com)
 * @author  Anand Subramanian (a.subramanian@salesforce.com)
 * @author  Tom Valine (tvaline@salesforce.com)
 * @author  Bhagyashree Shekhawat (bbhati@salesforce.com)
 */
public class ArgusHttpClient {

    //~ Static fields/initializers *******************************************************************************************************************

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(ArgusHttpClient.class);
    private static final String EXPRESSION_ARG = "expression";

    static {
        MAPPER.setVisibility(PropertyAccessor.GETTER, Visibility.ANY);
        MAPPER.setVisibility(PropertyAccessor.SETTER, Visibility.ANY);
    }

    //~ Instance fields ******************************************************************************************************************************

    int maxConn = 100;
    int connTimeout = 10000;
    int connRequestTimeout = 10000;
    private boolean preview = false;
    String endpoint;
    CloseableHttpClient httpClient;
    PoolingHttpClientConnectionManager connMgr;
    //private BasicCookieStore cookieStore;
    private BasicHttpContext httpContext;
    private String accessToken = null;
	private String refreshToken = null;

    //~ Constructors *********************************************************************************************************************************

    /**
     * Creates a new Argus HTTP client.
     *
     * @param   endpoint    The URL of the read endpoint including the port number. Must not be null.
     * @param   maxConn     The maximum number of concurrent connections. Must be greater than 0.
     * @param   timeout     The connection timeout in milliseconds. Must be greater than 0.
     * @param   reqTimeout  The connection request timeout in milliseconds. Must be greater than 0.
     * @param   isPreview   Set to true if the collector should skip actually submitting the data.
     *
     * @throws  OrchestraException  If an error occurs.
     */
    public ArgusHttpClient(String endpoint, int maxConn, int timeout, int reqTimeout, boolean isPreview) {
        requireArgument((endpoint != null) && (!endpoint.isEmpty()), "Illegal endpoint URL.");
        requireArgument(maxConn >= 2, "At least two connections are required.");
        requireArgument(timeout >= 1, "Timeout must be greater than 0.");
        requireArgument(reqTimeout >= 1, "Request timeout must be greater than 0.");
        try {
            preview = isPreview;
            if (preview) {
                MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
            }

            URL url = new URL(endpoint);
            int port = url.getPort();

            requireArgument(port != -1, "Endpoint must include explicit port.");
            connMgr = new PoolingHttpClientConnectionManager();
            connMgr.setMaxTotal(maxConn);
            connMgr.setDefaultMaxPerRoute(maxConn);

            String routePath = endpoint.substring(0, endpoint.lastIndexOf(':'));
            HttpHost host = new HttpHost(routePath, port);
            RequestConfig defaultRequestConfig = RequestConfig.custom().setConnectionRequestTimeout(reqTimeout).setConnectTimeout(timeout).build();

            connMgr.setMaxPerRoute(new HttpRoute(host), maxConn / 2);
            httpClient = HttpClients.custom().setConnectionManager(connMgr).setDefaultRequestConfig(defaultRequestConfig).build();
            //cookieStore = new BasicCookieStore();
            httpContext = new BasicHttpContext();
            //httpContext.setAttribute(ClientContext.COOKIE_STORE, cookieStore);
        } catch (MalformedURLException ex) {
            throw new OrchestraException("Error initializing the Argus HTTP Client.", ex);
        }
        LOGGER.info("Argus HTTP Client initialized using " + endpoint);
        this.endpoint = endpoint;
    }

    //~ Methods **************************************************************************************************************************************

    /**
     * Closes the client connections and prepares the client for garbage collection. This method may be invoked on a client which has already been
     * disposed.
     */
    public void dispose() {
        try {
            logout();
            httpClient.close();
        } catch (IOException ex) {
            LOGGER.warn("The HTTP client failed to shutdown properly.", ex);
        }
    }

    void login(String username, String password) {
        String requestUrl = endpoint + "/v2/auth/login";
        Credentials creds = new Credentials();

        creds.setPassword(password);
        creds.setUsername(username);

        HttpResponse response = null;

        try {
            StringEntity entity = new StringEntity(toJson(creds));
            response = executeHttpRequest(RequestType.POST, requestUrl, entity);
            
            int status = response.getStatusLine().getStatusCode();
			if(status == HttpStatus.SC_OK) {
				Map<String, String> tokenMap = MAPPER.readValue(extractStringResponse(response), new TypeReference<Map<String, String>>() {});
				accessToken = tokenMap.get("accessToken");
				refreshToken = tokenMap.get("refreshToken");
			} else {
				throw new OrchestraException("Failed to log in to Argus. Server responded with status code: " + status + 
						". Reason: " + response.getStatusLine().getReasonPhrase());
			}
			
        } catch (IOException | RuntimeException ex) {
            throw new OrchestraException(ex);
        }

        
        LOGGER.info("Logged in as " + username);
    }
    
    private void refresh() throws RefreshTokenExpiredException {
		String requestUrl = endpoint + "/v2/auth/token/refresh";
		
		HttpResponse response = null;
		
		try {
			Map<String, String> map = new HashMap<>();
			map.put("refreshToken", refreshToken);
			StringEntity entity = new StringEntity(toJson(map));
			
			response = executeHttpRequest(RequestType.POST, requestUrl, entity);
			
			int status = response.getStatusLine().getStatusCode();
			if(status == HttpStatus.SC_OK) {
				Map<String, String> tokenMap = MAPPER.readValue(extractStringResponse(response), new TypeReference<Map<String, String>>() {});
				accessToken = tokenMap.get("accessToken");
				LOGGER.info("Successfully obtained new access Token.");
			} else if(status == HttpStatus.SC_UNAUTHORIZED) {
				String message = "Looks like refresh token has expired. Server responded with status code: " + status + 
						". Reason: " + response.getStatusLine().getReasonPhrase();
				LOGGER.warn(message);
				throw new RefreshTokenExpiredException(message);
			} else {
				throw new OrchestraException("Failed to obtain new access token. Server responded with status code: " + status + 
						". Reason: " + response.getStatusLine().getReasonPhrase());
			}
		} catch (IOException ex) {
			LOGGER.warn("IOException while trying to log in to Argus.", ex);
			throw new OrchestraException("IO Exception occured while executing HTTP request: ", ex);
		}
	}

    void logout() {
        String requestUrl = endpoint + "/v2/auth/logout";
        HttpResponse response = null;

        try {
            response = executeHttpRequest(RequestType.GET, requestUrl, null);
            EntityUtils.consume(response.getEntity());
        } catch (Exception ex) {
            throw new OrchestraException(ex);
        }
        if (response.getStatusLine().getStatusCode() != 200) {
            String message = response.getStatusLine().getReasonPhrase();

            throw new OrchestraException(message);
        }
        LOGGER.info("Logout succeeded");
    }

    /**
     * Posts metric data.
     *
     * @param   data  The list of metrics to post. Cannot be null, but may be empty.
     *
     * @throws  OrchestraException  If an error occurs.
     */
    public void putMetricData(List<Metric> data) {
        String requestUrl = endpoint + "/collection/metrics";
        HttpResponse response = null;

        try {
            String json = toJson(data.toArray());

            if (!preview) {
                response = executeHttpRequest(RequestType.POST, requestUrl, new StringEntity(json));
                EntityUtils.consume(response.getEntity());
            } else {
                System.out.println(json);
            }
        } catch (IOException | RuntimeException ex) {
            throw new OrchestraException(ex);
        }
        if (!preview && response.getStatusLine().getStatusCode() != 200) {
            String message = response.getStatusLine().getReasonPhrase();

            throw new OrchestraException(message);
        }
        LOGGER.info("Posted {} metrics.", data.size());
    }

    /**
     * Executes a metric query against the Argus web services.
     *
     * @param   expression  The query expression to execute. Cannot be null or empty.
     *
     * @return  The HTTP response containing the resulting metric data.
     *
     * @throws  OrchestraException  If an error occurs.
     */
    public HttpResponse getMetricData(String expression) {
        String requestUrl = String.format(endpoint + "/metrics?%s=%s", EXPRESSION_ARG, expression);

        try {
            return executeHttpRequest(RequestType.GET, requestUrl, null);
        } catch (Exception ex) {
            throw new OrchestraException(ex);
        }
    }

    /**
     * Posts annotation data.
     *
     * @param   annotations  The list of annotations to submit. Cannot be null, but may be empty.
     *
     * @throws  OrchestraException  If an error occurs.
     */
    public void putAnnotationData(List<Annotation> annotations) {
        String requestUrl = endpoint + "/collection/annotations";
        HttpResponse response = null;

        try {
            String json = toJson(annotations);

            if (!preview) {
                response = executeHttpRequest(RequestType.POST, requestUrl, new StringEntity(json));
                EntityUtils.consume(response.getEntity());
            } else {
                System.out.println(json);
            }
        } catch (IOException | RuntimeException ex) {
            throw new OrchestraException(ex);
        }
        if (!preview && response.getStatusLine().getStatusCode() != 200) {
            String message = response.getStatusLine().getReasonPhrase();

            throw new OrchestraException(message);
        }
        LOGGER.info("Posted {} annotations.", annotations.size());
    }

    /* Execute a request given by type requestType. */
    private HttpResponse executeHttpRequest(RequestType requestType, String url, StringEntity entity) throws IOException {
        HttpResponse httpResponse = null;

        if (entity != null) {
            entity.setContentType("application/json");
        }
        switch (requestType) {
            case POST:

                HttpPost post = new HttpPost(url);
                post.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
                post.setEntity(entity);
                httpResponse = httpClient.execute(post, httpContext);
                break;
            case GET:

                HttpGet httpGet = new HttpGet(url);
                httpGet.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
                httpResponse = httpClient.execute(httpGet, httpContext);
                break;
            case DELETE:

                HttpDelete httpDelete = new HttpDelete(url);
                httpDelete.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
                httpResponse = httpClient.execute(httpDelete, httpContext);
                break;
            case PUT:

                HttpPut httpput = new HttpPut(url);
                httpput.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
                httpput.setEntity(entity);
                httpResponse = httpClient.execute(httpput, httpContext);
                break;
            default:
                throw new IllegalArgumentException(" Request Type " + requestType + " not a valid request type. ");
        }
        
        
		if(!url.contains("auth") && httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
			LOGGER.warn("Looks like access token has expired. Will try to obtain a new access token using refresh token.");
			try {
				refresh();
			} catch(RefreshTokenExpiredException e) {
				login(Configuration.getParameter(Configuration.Parameter.ARGUSWS_USERNAME), 
						Configuration.getParameter(Configuration.Parameter.ARGUSWS_PASSWORD));
			}
			
			executeHttpRequest(requestType, url, entity);
			
		}
        
        return httpResponse;
    }

    private <T> String toJson(T type) {
        try {
            return MAPPER.writeValueAsString(type);
        } catch (IOException ex) {
            throw new OrchestraException(ex);
        }
    }
    
    private String extractStringResponse(HttpResponse content) {
        requireArgument(content != null, "Response content is null.");

        String result;
        HttpEntity entity = null;

        try {
            entity = content.getEntity();
            if (entity == null) {
                result = "";
            } else {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();

                entity.writeTo(baos);
                result = baos.toString("UTF-8");
            }
            return result;
        } catch (IOException ex) {
            throw new OrchestraException(ex);
        } finally {
            if (entity != null) {
                try {
                    EntityUtils.consume(entity);
                } catch (IOException ex) {
                    LOGGER.warn("Failed to close entity stream.", ex);
                }
            }
        }
    }

    //~ Enums ****************************************************************************************************************************************

    /**
     * The HTTP request type to use.
     *
     * @author  Tom Valine (tvaline@salesforce.com)
     */
    private static enum RequestType {

        POST("post"),
        GET("get"),
        DELETE("delete"),
        PUT("put");

        private final String requestType;

        private RequestType(String requestType) {
            this.requestType = requestType;
        }

        /**
         * Returns the request type as a string.
         *
         * @return  The request type.
         */
        @SuppressWarnings("unused")
		public String getRequestType() {
            return requestType;
        }
    }
    
    @SuppressWarnings("serial")
	class RefreshTokenExpiredException extends Exception {
    	
    	public RefreshTokenExpiredException(String msg) {
    		super(msg);
    	}

    	public RefreshTokenExpiredException(Throwable cause) {
    		super(cause);
    	}

    	public RefreshTokenExpiredException(String msg, Throwable cause) {
    		super(msg, cause);
    	}
    }
}
/* Copyright (c) 2016, Salesforce.com, Inc.  All rights reserved. */
