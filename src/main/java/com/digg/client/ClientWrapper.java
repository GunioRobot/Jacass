package com.digg.client;

/**
 * User: Arin Sarkissian
 * Date: Mar 14, 2010
 * Time: 8:05:06 PM
 */
public class ClientWrapper {
    protected Object client;
    protected boolean isFree;

    public ClientWrapper(Object client, boolean free) {
        this.client = client;
        isFree = free;
    }

    public ClientWrapper(Object client) {
        this(client, true);
    }

    public ClientWrapper() {
    }

    public void free() {
        isFree = true;
    }

    public void use() {
        isFree = false;
    }

    public boolean isFree() {
        return isFree;
    }

    public Object getClient() {
        return null;
    }

    public void setClient(Object client) {
        this.client = client;
    }
}
