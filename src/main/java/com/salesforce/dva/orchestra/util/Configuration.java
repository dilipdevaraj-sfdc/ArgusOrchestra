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
package com.salesforce.dva.orchestra.util;

import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration properties for Orchestra. The location of the configuration property file can be specified by using
 * '-Dorchestra.configuration=&lt;path&gt;'.
 *
 * @author  Tom Valine (tvaline@salesforce.com)
 */
public class Configuration {

    //~ Static fields/initializers *******************************************************************************************************************

    private static final Properties DEFAULT_CONFIGURATION;
    private static final Properties CONFIGURATION;

    static {
        DEFAULT_CONFIGURATION = new Properties();
        for (Parameter param : Parameter.values()) {
            DEFAULT_CONFIGURATION.put(param.keyName, param.defaultValue);
        }
        CONFIGURATION = new Properties(DEFAULT_CONFIGURATION);

        InputStream is = null;

        try {
            is = new FileInputStream(System.getProperty("orchestra.configuration"));
            CONFIGURATION.load(is);
        } catch (IOException | RuntimeException ex) {
            String msg = "Could not load Orchestra configuration.  " +
                "Please specify the configuration file location using -Dorchestra.configuration=<path>.";

            LoggerFactory.getLogger(Configuration.class).warn(msg);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ex) {
                    assert (false) : "This should never happen";
                }
            }
        }
    }

    //~ Methods **************************************************************************************************************************************

    /**
     * Returns the key name of a parameter.
     *
     * @param   key  The parameter key.
     *
     * @return  The parameter name.
     */
    public static String getParameter(Parameter key) {
        return CONFIGURATION.getProperty(key.keyName);
    }

    /**
     * Overrides properties.
     *
     * @param  properties  The properties used to override the configuration loaded by the default mechanism.
     */
    public static void load(Properties properties) {
        CONFIGURATION.putAll(properties);
    }

    //~ Enums ****************************************************************************************************************************************

    /**
     * Supported configuration parameters.
     *
     * @author  Tom Valine (tvaline@salesforce.com)
     */
    public enum Parameter {

        /** Indicates the HTTP endpoint for Argus. Defaults to 'http://127.0.0.1:4242'. */
        ARGUSWS_ENDPOINT("argusws.endpoint", ""), // NOSONAR

        /** The username to authenticate to the web services with. No default. */
        ARGUSWS_USERNAME("argusws.username", ""),
        /** The password to authenticate to the web services with. No default. */
        ARGUSWS_PASSWORD("argusws.password", "");

        private String keyName;
        private String defaultValue;

        private Parameter(String name, String defaultValue) {
            this.keyName = name;
            this.defaultValue = defaultValue;
        }
    }
}
/* Copyright (c) 2016, Salesforce.com, Inc.  All rights reserved. */
