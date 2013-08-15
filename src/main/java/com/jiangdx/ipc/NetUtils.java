package com.jiangdx.ipc;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NetUtils {
	private static final Log LOG = LogFactory.getLog(NetUtils.class);

	private static Map<String, String> hostToResolved = new HashMap<String, String>();

	/**
	 * Get the socket factory for the given class according to its configuration
	 * parameter <tt>hadoop.rpc.socket.factory.class.&lt;ClassName&gt;</tt>.
	 * When no such parameter exists then fall back on the default socket
	 * factory as configured by <tt>hadoop.rpc.socket.factory.class.default</tt>
	 * . If this default socket factory is not configured, then fall back on the
	 * JVM default socket factory.
	 * 
	 * @return a socket factory
	 */
	public static SocketFactory getSocketFactory() {
		return getDefaultSocketFactory();
	}

	/**
	 * Get the default socket factory as specified by the configuration
	 * parameter <tt>hadoop.rpc.socket.factory.default</tt>
	 * 
	 * @param conf
	 *            the configuration
	 * @return the default socket factory as specified in the configuration or
	 *         the JVM default socket factory if the configuration does not
	 *         contain a default socket factory property.
	 */
	public static SocketFactory getDefaultSocketFactory() {
		return SocketFactory.getDefault();
	}

	/**
	 * Create a socket address with the given host and port. The hostname might
	 * be replaced with another host that was set via
	 * {@link #addStaticResolution(String, String)}. The value of
	 * hadoop.security.token.service.use_ip will determine whether the standard
	 * java host resolver is used, or if the fully qualified resolver is used.
	 * 
	 * @param host
	 *            the hostname or IP use to instantiate the object
	 * @param port
	 *            the port number
	 * @return InetSocketAddress
	 */
	public static InetSocketAddress makeSocketAddr(String host, int port) {
		String staticHost = getStaticResolution(host);
		String resolveHost = (staticHost != null) ? staticHost : host;

		InetSocketAddress addr;
		try {
			InetAddress iaddr = InetAddress.getByName(resolveHost);
			// if there is a static entry for the host, make the returned
			// address look like the original given host
			if (staticHost != null) {
				iaddr = InetAddress.getByAddress(host, iaddr.getAddress());
			}
			addr = new InetSocketAddress(iaddr, port);
		} catch (UnknownHostException e) {
			addr = InetSocketAddress.createUnresolved(host, port);
		}
		return addr;
	}

	protected interface HostResolver {
		InetAddress getByName(String host) throws UnknownHostException;
	}

	/**
	 * Retrieves the resolved name for the passed host. The resolved name must
	 * have been set earlier using
	 * {@link NetUtils#addStaticResolution(String, String)}
	 * 
	 * @param host
	 * @return the resolution
	 */
	public static String getStaticResolution(String host) {
		synchronized (hostToResolved) {
			return hostToResolved.get(host);
		}
	}

	/**
	 * Returns InetSocketAddress that a client can use to connect to the server.
	 * Server.getListenerAddress() is not correct when the server binds to
	 * "0.0.0.0". This returns "127.0.0.1:port" when the getListenerAddress()
	 * returns "0.0.0.0:port".
	 * 
	 * @param server
	 * @return socket address that a client can use to connect to the server.
	 */
	public static InetSocketAddress getConnectAddress(Server server) {
		InetSocketAddress addr = server.getListenerAddress();
		if (addr.getAddress().isAnyLocalAddress()) {
			addr = makeSocketAddr("127.0.0.1", addr.getPort());
		}
		return addr;
	}

	/**
	 * Same as getInputStream(socket, socket.getSoTimeout()).<br>
	 * <br>
	 * 
	 * From documentation for {@link #getInputStream(Socket, long)}:<br>
	 * Returns InputStream for the socket. If the socket has an associated
	 * SocketChannel then it returns a {@link SocketInputStream} with the given
	 * timeout. If the socket does not have a channel,
	 * {@link Socket#getInputStream()} is returned. In the later case, the
	 * timeout argument is ignored and the timeout set with
	 * {@link Socket#setSoTimeout(int)} applies for reads.<br>
	 * <br>
	 * 
	 * Any socket created using socket factories returned by {@link NetUtils},
	 * must use this interface instead of {@link Socket#getInputStream()}.
	 * 
	 * @see #getInputStream(Socket, long)
	 * 
	 * @param socket
	 * @return InputStream for reading from the socket.
	 * @throws IOException
	 */
	public static InputStream getInputStream(Socket socket) throws IOException {
		return getInputStream(socket, socket.getSoTimeout());
	}

	/**
	 * Returns InputStream for the socket. If the socket has an associated
	 * SocketChannel then it returns a {@link SocketInputStream} with the given
	 * timeout. If the socket does not have a channel,
	 * {@link Socket#getInputStream()} is returned. In the later case, the
	 * timeout argument is ignored and the timeout set with
	 * {@link Socket#setSoTimeout(int)} applies for reads.<br>
	 * <br>
	 * 
	 * Any socket created using socket factories returned by {@link NetUtils},
	 * must use this interface instead of {@link Socket#getInputStream()}.
	 * 
	 * @see Socket#getChannel()
	 * 
	 * @param socket
	 * @param timeout
	 *            timeout in milliseconds. This may not always apply. zero for
	 *            waiting as long as necessary.
	 * @return InputStream for reading from the socket.
	 * @throws IOException
	 */
	public static InputStream getInputStream(Socket socket, long timeout)
			throws IOException {
		return (socket.getChannel() == null) ? socket.getInputStream()
				: new SocketInputStream(socket, timeout);
	}

	/**
	 * Same as getOutputStream(socket, 0). Timeout of zero implies write will
	 * wait until data is available.<br>
	 * <br>
	 * 
	 * From documentation for {@link #getOutputStream(Socket, long)} : <br>
	 * Returns OutputStream for the socket. If the socket has an associated
	 * SocketChannel then it returns a {@link SocketOutputStream} with the given
	 * timeout. If the socket does not have a channel,
	 * {@link Socket#getOutputStream()} is returned. In the later case, the
	 * timeout argument is ignored and the write will wait until data is
	 * available.<br>
	 * <br>
	 * 
	 * Any socket created using socket factories returned by {@link NetUtils},
	 * must use this interface instead of {@link Socket#getOutputStream()}.
	 * 
	 * @see #getOutputStream(Socket, long)
	 * 
	 * @param socket
	 * @return OutputStream for writing to the socket.
	 * @throws IOException
	 */
	public static OutputStream getOutputStream(Socket socket)
			throws IOException {
		return getOutputStream(socket, 0);
	}

	/**
	 * Returns OutputStream for the socket. If the socket has an associated
	 * SocketChannel then it returns a {@link SocketOutputStream} with the given
	 * timeout. If the socket does not have a channel,
	 * {@link Socket#getOutputStream()} is returned. In the later case, the
	 * timeout argument is ignored and the write will wait until data is
	 * available.<br>
	 * <br>
	 * 
	 * Any socket created using socket factories returned by {@link NetUtils},
	 * must use this interface instead of {@link Socket#getOutputStream()}.
	 * 
	 * @see Socket#getChannel()
	 * 
	 * @param socket
	 * @param timeout
	 *            timeout in milliseconds. This may not always apply. zero for
	 *            waiting as long as necessary.
	 * @return OutputStream for writing to the socket.
	 * @throws IOException
	 */
	public static OutputStream getOutputStream(Socket socket, long timeout)
			throws IOException {
		return (socket.getChannel() == null) ? socket.getOutputStream()
				: new SocketOutputStream(socket, timeout);
	}

	/**
	 * This is a drop-in replacement for
	 * {@link Socket#connect(SocketAddress, int)}. In the case of normal sockets
	 * that don't have associated channels, this just invokes
	 * <code>socket.connect(endpoint, timeout)</code>. If
	 * <code>socket.getChannel()</code> returns a non-null channel, connect is
	 * implemented using Hadoop's selectors. This is done mainly to avoid Sun's
	 * connect implementation from creating thread-local selectors, since Hadoop
	 * does not have control on when these are closed and could end up taking
	 * all the available file descriptors.
	 * 
	 * @see java.net.Socket#connect(java.net.SocketAddress, int)
	 * 
	 * @param socket
	 * @param endpoint
	 * @param timeout
	 *            - timeout in milliseconds
	 */
	public static void connect(Socket socket, SocketAddress endpoint,
			int timeout) throws IOException {
		if (socket == null || endpoint == null || timeout < 0) {
			throw new IllegalArgumentException("Illegal argument for connect()");
		}

		SocketChannel ch = socket.getChannel();

		if (ch == null) {
			// let the default implementation handle it.
			socket.connect(endpoint, timeout);
		} else {
			SocketIOWithTimeout.connect(ch, endpoint, timeout);
		}

		// There is a very rare case allowed by the TCP specification, such that
		// if we are trying to connect to an endpoint on the local machine,
		// and we end up choosing an ephemeral port equal to the destination
		// port,
		// we will actually end up getting connected to ourself (ie any data we
		// send just comes right back). This is only possible if the target
		// daemon is down, so we'll treat it like connection refused.
		if (socket.getLocalPort() == socket.getPort()
				&& socket.getLocalAddress().equals(socket.getInetAddress())) {
			LOG.info("Detected a loopback TCP socket, disconnecting it");
			socket.close();
			throw new ConnectException(
					"Localhost targeted connection resulted in a loopback. "
							+ "No daemon is listening on the target port.");
		}
	}

	/**
	 * Checks if {@code host} is a local host name and return
	 * {@link InetAddress} corresponding to that address.
	 * 
	 * @param host
	 *            the specified host
	 * @return a valid local {@link InetAddress} or null
	 * @throws SocketException
	 *             if an I/O error occurs
	 */
	public static InetAddress getLocalInetAddress(String host)
			throws SocketException {
		if (host == null) {
			return null;
		}
		InetAddress addr = null;
		try {
			addr = InetAddress.getByName(host);
			if (NetworkInterface.getByInetAddress(addr) == null) {
				addr = null; // Not a local address
			}
		} catch (UnknownHostException ignore) {
		}
		return addr;
	}
	
	/**
	 * return {@link InetAddress} corresponding to that address.
	 * 
	 * @return a valid local {@link InetAddress} or null
	 * @throws SocketException
	 *             if an I/O error occurs
	 */
	public static InetAddress getLocalInetAddress() throws SocketException {
		String host = getLocalAddress();
		return getLocalInetAddress(host);
	}
	
	/**
	 * Get the local host address.
	 * @return the site address or `127.0.0.1` 
	 */
	public static String getLocalAddress() {
		String host = "127.0.0.1";
		Enumeration<NetworkInterface> ifaces = null;
		try {
			ifaces = NetworkInterface.getNetworkInterfaces();
			while (ifaces.hasMoreElements()) {
				NetworkInterface ni = ifaces.nextElement();
				Enumeration<InetAddress> ips = ni.getInetAddresses();
				while (ips.hasMoreElements()) {
					InetAddress addr = ips.nextElement();
					if (addr.isSiteLocalAddress())
						return host = addr.getHostAddress();
				}
			}
		} catch (Exception e) {
		}
		return host;
	}
}
