package com.jiangdx.ipc;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The IPC connection header sent by the client to the server on connection
 * establishment.
 */
class ConnectionHeader implements Serializable {
	/**
	 * UID for serializing this object.
	 */
	private static final long serialVersionUID = -2352420453680384570L;

	public static final Log LOG = LogFactory.getLog(ConnectionHeader.class);

	private String protocol;

	public ConnectionHeader() {
	}

	/**
	 * Create a new {@link ConnectionHeader} with the given
	 * <code>protocol</code> and {@link UserGroupInformation}. F
	 */
	public ConnectionHeader(String protocol) {
		this.protocol = protocol;
	}

	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		byte [] bytes = new byte[length];
	    in.readFully(bytes, 0, length);
		
	    LOG.info("will read length: " + length + " bytes");
	    
	    ByteBuffer buff = ByteBuffer.wrap(bytes);
	    protocol = buff.toString();
		if (protocol.isEmpty()) {
			protocol = null;
		}
	}
	
	public void readFields(byte[] buff) throws IOException {
		protocol = new String(buff);
		if (protocol.isEmpty()) {
			protocol = null;
		}
	}

	public void write(DataOutput out) throws IOException {
		String s = protocol == null ? "" : protocol;

		ByteBuffer bytes = ByteBuffer.wrap(s.getBytes());
		int length = bytes.limit();

		out.write(bytes.array(), 0, length);
	}

	public String getProtocol() {
		return protocol;
	}
}
