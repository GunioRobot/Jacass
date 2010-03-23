package com.digg.client;

/**
 * User: Arin Sarkissian
 * Date: Mar 14, 2010
 * Time: 10:11:36 PM
 */
class HostPort {
    private final String host;
    private final int port;

    HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
