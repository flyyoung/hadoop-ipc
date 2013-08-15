package com.jiangdx.ipc;


import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple RPC mechanism.
 * 
 * A <i>protocol</i> is a Java interface. All parameters and return types must
 * be one of:
 * 
 * <ul>
 * <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 * 
 * <li>a {@link String}; or</li>
 * 
 * <li>an array of the above types</li>
 * </ul>
 * 
 * All methods in the protocol should throw only IOException. No field data of
 * the protocol instance is transmitted.
 */
/*RPC是对Client和Server的包装*/
public class RPC {
	private static final Log LOG = LogFactory.getLog(RPC.class);

	/** no public ctor */
	private RPC() {
	}                               

	/* Cache a client using its socket factory as the hash key */
	/*缓存Client SocketFactory创建套接字 可以被其他工厂子类化*/
	static private class ClientCache {
		private Map<SocketFactory, Client> clients = new HashMap<SocketFactory, Client>();

		/**
		 * Construct & cache an IPC client with the user-provided SocketFactory
		 * if no cached client exists.
		 * 
		 * @param conf
		 *            Configuration
		 * @return an IPC client
		 */
		private synchronized Client getClient(SocketFactory factory) {
			// Construct & cache client. The configuration is only used for
			// timeout,
			// and Clients have connection pools. So we can either (a) lose some
			// connection pooling and leak sockets, or (b) use the same timeout
			// for all
			// configurations. Since the IPC is usually intended globally, not
			// per-job, we choose (a).
			Client client = clients.get(factory);
			if (client == null) {
				//创建客户端
				client = new Client(factory);
				clients.put(factory, client);
			} else {
				//引用数增加1
				client.incCount();
			}
			return client;
		}

		/**
		 * Stop a RPC client connection A RPC client is closed only when its
		 * reference count becomes zero.
		 */
		private void stopClient(Client client) {
			synchronized (this) {
				client.decCount();
				if (client.isZeroReference()) {
					clients.remove(client.getSocketFactory());
				}
			}
			if (client.isZeroReference()) {
				client.stop();
			}
		}
	}

	private static ClientCache CLIENTS = new ClientCache();
  
	/*动态代理的实现类 实现InvocationHandler接口*/
	private static class Invoker<T> implements InvocationHandler {
		private Client.ConnectionId remoteId;
		private Class<T> iface;
		private Client client;
		private boolean isClosed = false;

		//iface代表协议
		public Invoker(final Class<T> iface,
				InetSocketAddress address, SocketFactory factory, int rpcTimeout)
				throws IOException {
			this.iface = iface;
			this.remoteId = Client.ConnectionId.getConnectionId(address,
					iface, rpcTimeout);
			this.client = CLIENTS.getClient(factory);
		}

		/*Invocation封装方法名和参数*/
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			final boolean logDebug = LOG.isDebugEnabled();
			long startTime = 0;
			if (logDebug) {
				startTime = System.currentTimeMillis();
			}
			//调用Client call方法
			Invocation value = client.call(new Invocation(iface, method, args),
					remoteId);
			if (logDebug) {
				long callTime = System.currentTimeMillis() - startTime;
				LOG.debug("Call: " + method.getName() + "() " + callTime);
			}
			return value.getResult();
		}

		/* close the IPC client that's responsible for this invoker's RPCs */
		synchronized private void close() {
			if (!isClosed) {
				isClosed = true;
				CLIENTS.stopClient(client);
			}
		}
	}

	/**
	 * A version mismatch for the RPC protocol.
	 */
	public static class VersionMismatch extends IOException {
		private static final long serialVersionUID = 7728059388772409773L;
		//协议名
		private String interfaceName;
		private long clientVersion;
		private long serverVersion;

		/**
		 * Create a version mismatch exception
		 * 
		 * @param interfaceName
		 *            the name of the protocol mismatch
		 * @param clientVersion
		 *            the client's version of the protocol
		 * @param serverVersion
		 *            the server's version of the protocol
		 */
		public VersionMismatch(String interfaceName, long clientVersion,
				long serverVersion) {
			super("Protocol " + interfaceName + " version mismatch. (client = "
					+ clientVersion + ", server = " + serverVersion + ")");
			this.interfaceName = interfaceName;
			this.clientVersion = clientVersion;
			this.serverVersion = serverVersion;
		}

		/**
		 * Get the interface name
		 * 
		 * @return the java class name (eg.
		 *         org.apache.hadoop.mapred.InterTrackerProtocol)
		 */
		public String getInterfaceName() {
			return interfaceName;
		}

		/**
		 * Get the client's preferred version
		 */
		public long getClientVersion() {
			return clientVersion;
		}

		/**
		 * Get the server's agreed to version.
		 */
		public long getServerVersion() {
			return serverVersion;
		}
	}
  
	public static <T> T waitForProxy(final Class<T> iface,
			InetSocketAddress addr) throws IOException {
		return waitForProxy(iface, addr, 0, Long.MAX_VALUE);
	}

	/**
	 * Get a proxy connection to a remote server
	 * 
	 * @param protocol
	 *            protocol class
	 * @param addr
	 *            remote address
	 * @param conf
	 *            configuration to use
	 * @param connTimeout
	 *            time in milliseconds before giving up
	 * @return the proxy
	 * @throws IOException
	 *             if the far end through a RemoteException
	 */
	static <T> T waitForProxy(final Class<T> iface, InetSocketAddress addr,
			long connTimeout) throws IOException {
		return waitForProxy(iface, addr, 0, connTimeout);
	}

	static <T> T waitForProxy(final Class<T> iface, InetSocketAddress addr,
			int rpcTimeout, long connTimeout) throws IOException {
		long startTime = System.currentTimeMillis();
		IOException ioe;
		while (true) {
			try {
				return getProxy(iface, addr, rpcTimeout);
			} catch (ConnectException se) { // namenode has not been started
				LOG.info("Server at " + addr + " not available yet, Zzzzz...");
				ioe = se;
			} catch (SocketTimeoutException te) { // namenode is busy
				LOG.info("Problem connecting to server: " + addr);
				ioe = te;
			}
			// check if timed out
			if (System.currentTimeMillis() - connTimeout >= startTime) {
				throw ioe;
			}

			// wait for retry
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ie) {
				// IGNORE
			}
		}
	}

    
	/**
	 * Construct a client-side proxy object that implements the named protocol,
	 * talking to a server at the named address.
	 */
	public static <T> T getProxy(final Class<T> iface, InetSocketAddress addr,
			SocketFactory factory) throws IOException {
		return getProxy(iface, addr, factory, 0);
	}

	/**
	 * Construct a client-side proxy object that implements the named protocol,
	 * talking to a server at the named address.
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getProxy(final Class<T> iface, InetSocketAddress addr,
			SocketFactory factory, int rpcTimeout) throws IOException {
		//生成动态代理对象 Invoker代理调用类
		T proxy = (T) Proxy.newProxyInstance(iface.getClassLoader(),
				new Class[] { iface }, new Invoker<T>(iface, addr, factory,
						rpcTimeout));
		return proxy;
	}

	/**
	 * Construct a client-side proxy object with the default SocketFactory
	 * 
	 * @param iface
	 * @param addr
	 * @return a proxy instance
	 * @throws IOException
	 */
	public static <T> T getProxy(final Class<T> iface, InetSocketAddress addr)
			throws IOException {
		return getProxy(iface, addr, NetUtils.getDefaultSocketFactory(), 0);
	}

	public static <T> T getProxy(final Class<T> iface, InetSocketAddress addr,
			int rpcTimeout) throws IOException {
		return getProxy(iface, addr, NetUtils.getDefaultSocketFactory(),
				rpcTimeout);
	}

	/**
	 * Stop this proxy and release its invoker's resource
	 * 
	 * @param proxy
	 *            the proxy to be stopped
	 */
	public static void stopProxy(Object proxy) {
		if (proxy != null) {
			//返回代理实例的调用处理程序
			((Invoker) Proxy.getInvocationHandler(proxy)).close();
		}
	}

	/**
	 * Construct a server for a protocol implementation instance listening on a
	 * port and address.
	 */
	public static Server getServer(final String bindAddress, final int port)
			throws IOException {
		return getServer(bindAddress, port, 1, false);
	}

	/**
	 * Construct a server for a protocol implementation instance listening on a
	 * port and address, with a secret manager.
	 */
	public static Server getServer(final String bindAddress, final int port,
			final int numHandlers, final boolean verbose) throws IOException {
		return new Server(bindAddress, port, numHandlers, verbose);
	}

	/** An RPC Server. */
	public static class Server extends com.jiangdx.ipc.Server {
		private boolean verbose;

		/**
		 * Construct an RPC server.
		 * 
		 * @param bindAddress
		 *            the address to bind on to listen for connection
		 * @param port
		 *            the port to listen for connections on
		 */
		public Server(String bindAddress, int port) throws IOException {
			this(bindAddress, port, 1, false);
		}

		/**
		 * Construct an RPC server.
		 * 
		 * @param bindAddress
		 *            the address to bind on to listen for connection
		 * @param port
		 *            the port to listen for connections on
		 * @param numHandlers
		 *            the number of method handler threads to run
		 * @param verbose
		 *            whether each call should be logged
		 */
		public Server(String bindAddress, int port,
				int numHandlers, boolean verbose) throws IOException {
			super(bindAddress, port, numHandlers);
			this.verbose = verbose;
		}
        //重写Sever类的call方法
		public Serializable call(Class<?> iface, Serializable param,
				long receivedTime) throws IOException {
			try {
				Invocation call = (Invocation) param; //调用参数 Invocationd对象包含方法名称 形式参数列表和实际参数列表
				if (verbose)
					log("Call: " + call);
				//从实例缓存中按照接口寻找实例对象
				Object instance = INSTANCE_CACHE.get(iface);
				if (instance == null)
					throw new IOException("interface `" + iface	+ "` not inscribe.");
				//通过Class对象获取Method对象
				Method method = iface.getMethod(call.getMethodName(),
						call.getParameterClasses());
				//取消Java语言访问权限检查
				method.setAccessible(true);

				long startTime = System.currentTimeMillis();
				//调用Method对象的invoke方法
				Object value = method.invoke(instance, call.getParameters());
				int processingTime = (int) (System.currentTimeMillis() - startTime);
				int qTime = (int) (startTime - receivedTime);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Served: " + call.getMethodName()
							+ " queueTime= " + qTime + " procesingTime= "
							+ processingTime);
				}
				if (verbose)
					log("Return: " + value);

				call.setResult(value); //向Invocation对象设置结果
				return call;
			} catch (InvocationTargetException e) {
				Throwable target = e.getTargetException();
				if (target instanceof IOException) {
					throw (IOException) target;
				} else {
					IOException ioe = new IOException(target.toString());
					ioe.setStackTrace(target.getStackTrace());
					throw ioe;
				}
			} catch (Throwable e) {
				if (!(e instanceof IOException)) {
					LOG.error("Unexpected throwable object ", e);
				}
				IOException ioe = new IOException(e.toString());
				ioe.setStackTrace(e.getStackTrace());
				throw ioe;
			}
		}
	}

	private static void log(String value) {
		if (value != null && value.length() > 55)
			value = value.substring(0, 55) + "...";
		LOG.info(value);
	}
}
