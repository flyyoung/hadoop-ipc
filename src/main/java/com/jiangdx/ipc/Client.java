package com.jiangdx.ipc;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** A client for an IPC service.  IPC calls take a single {@link Invocation} as a
 * parameter, and return a {@link Invocation} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Server
 */
public class Client {
	public static final Log LOG = LogFactory.getLog(Client.class);
	//与远程服务器连接的缓存池 
	private Hashtable<ConnectionId, Connection> connections = new Hashtable<ConnectionId, Connection>();  

	private int counter; // counter for call ids
	private AtomicBoolean running = new AtomicBoolean(true); // if client runs

	private SocketFactory socketFactory; // how to create sockets
	private int refCount = 1;

	//ping服务端的间隔  
	final static int DEFAULT_PING_INTERVAL = 60000; // 1 min  
	final static int PING_CALL_ID = -1;

	/**
	 * set the ping interval value in configuration
	 * 
	 * @param pingInterval
	 *            the ping interval
	 */
	final public static void setPingInterval(int pingInterval) {
		// conf.setInt(PING_INTERVAL_NAME, pingInterval);
	}

	/**
	 * Get the ping interval from configuration; If not set in the
	 * configuration, return the default value.
	 * 
	 * @return the ping interval
	 */
	final static int getPingInterval() {
		// return conf.getInt(PING_INTERVAL_NAME, DEFAULT_PING_INTERVAL);
		return DEFAULT_PING_INTERVAL;
	}

	/**
	 * Increment this client's reference count
	 * 
	 */
	synchronized void incCount() {
		refCount++;
	}

	/**
	 * Decrement this client's reference count
	 * 
	 */
	synchronized void decCount() {
		refCount--;
	}
  
	/**
	 * Return if this client has no reference
	 * 
	 * @return true if this client has no reference; false otherwise
	 */
	synchronized boolean isZeroReference() {
		return refCount == 0;
	}

	/** A call waiting for a value. */
	private class Call {
		int id;                 //调用标示ID  
		Serializable param;     //调用参数 
		Invocation value;       //调用返回的值
		IOException error;      //异常信息 
		boolean done;           //调用是否完成

		protected Call(Serializable param) {
			this.param = param;
			synchronized (Client.this) {
				this.id = counter++;
			}
		}

		/**
		 * Indicate when the call is complete and the value or error are
		 * available. Notifies by default.
		 */
		protected synchronized void callComplete() {
			this.done = true;
			notify(); //唤醒client等待线程
		}

		/**
		 * Set the exception when there is an error. Notify the caller the call
		 * is done.
		 * 
		 * @param error
		 *            exception thrown by the call; either local or remote
		 */
		public synchronized void setException(IOException error) {
			this.error = error;
			callComplete();
		}

		/**
		 * Set the return value when there is no error. Notify the caller the
		 * call is done.
		 * 
		 * @param value
		 *            return value of the call.
		 */
		public synchronized void setValue(Invocation value) {
			this.value = value;
			callComplete(); //Call.done标志位被置位 即远程调用结果已经准备好 同时调用notify() 通知CClient.call()方法停止等待
		}
	}

	/**
	 * Thread that reads responses and notifies callers. Each connection owns a
	 * socket connected to a remote address. Calls are multiplexed through this
	 * socket: responses may be delivered out of order.
	 */
	private class Connection extends Thread {
		private InetSocketAddress server; // server ip:port      服务端ip:port  
		private ConnectionHeader header; // connection header    连接头信息，该实体类封装了连接协议与用户信息UserGroupInformation
		private final ConnectionId remoteId; // connection id    连接ID 

		private Socket socket = null; // connected socket        客户端已连接的Socket 
		private DataInputStream in;
		private DataOutputStream out;
    
		private int rpcTimeout;
		/** connections will be culled if it was idle for maxIdleTime msecs */
		private int maxIdleTime;
		// the max. no. of retries for socket connections
		private int maxRetries;
		private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
		private int pingInterval; // how often sends ping to the server in msecs

		// currently active calls
		private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>(); //待处理的RPC队列 
		/** last I/O activity time */
		private AtomicLong lastActivity = new AtomicLong();                      //最后I/O活跃的时间  
		/** indicate if the connection is closed */
		private AtomicBoolean shouldCloseConnection = new AtomicBoolean();       //连接是否关闭  
		private IOException closeException; // close reason                        连接关闭原因 

		public Connection(ConnectionId remoteId) throws IOException {
			this.remoteId = remoteId;
			this.server = remoteId.getAddress();
			if (server.isUnresolved()) {
				throw new UnknownHostException("unknown host: "
						+ remoteId.getAddress().getHostName());
			}
			this.maxIdleTime = remoteId.getMaxIdleTime();
			this.maxRetries = remoteId.getMaxRetries();
			this.tcpNoDelay = remoteId.getTcpNoDelay();
			this.pingInterval = remoteId.getPingInterval();
			if (LOG.isDebugEnabled()) {
				LOG.debug("The ping interval is " + this.pingInterval + " ms.");
			}
			this.rpcTimeout = remoteId.getRpcTimeout();
			Class<?> protocol = remoteId.getProtocol();
			header = new ConnectionHeader(protocol == null ? null
					: protocol.getName());     //连接头

			this.setName("IPC Client (" + socketFactory.hashCode()
					+ ") connection to " + remoteId.getAddress().toString()
					+ " from an unknown user");
			this.setDaemon(true);
		}

		/** Update lastActivity with the current time. */
		//更新最近活动时间
		private void touch() {
			lastActivity.set(System.currentTimeMillis());
		}

		/**
		 * Add a call to this connection's call queue and notify a listener;
		 * synchronized. Returns false if called during shutdown.
		 * 
		 * @param call
		 *            to add
		 * @return true if the call was added.
		 */
		private synchronized boolean addCall(Call call) {
			if (shouldCloseConnection.get())
				return false;
			calls.put(call.id, call);
			notify();
			return true;
		}

		/**
		 * This class sends a ping to the remote side when timeout on reading.
		 * If no failure is detected, it retries until at least a byte is read.
		 */
		//在SocketInputStream之上添加心跳检查能力
		private class PingInputStream extends FilterInputStream {
			/* constructor */
			protected PingInputStream(InputStream in) {
				super(in);
			}

			/*
			 * Process timeout exception if the connection is not going to be
			 * closed or is not configured to have a RPC timeout, send a ping.
			 * (if rpcTimeout is not set to be 0, then RPC should timeout.
			 * otherwise, throw the timeout exception.
			 */
			//处理超时 如果客户端处于运行状态 调用sendPing()
			private void handleTimeout(SocketTimeoutException e)
					throws IOException {
				if (shouldCloseConnection.get() || !running.get()
						|| rpcTimeout > 0) {
					throw e;
				} else {
					if (LOG.isDebugEnabled())
						LOG.debug("handle timeout, then send ping...");
					sendPing();
				}
			}

			/**
			 * Read a byte from the stream. Send a ping if timeout on read.
			 * Retries if no failure is detected until a byte is read.
			 * 
			 * @throws IOException
			 *             for any IO problem other than socket timeout
			 */
			public int read() throws IOException {
				do {
					try {
						return super.read();//读取一个字节
					} catch (SocketTimeoutException e) {
						handleTimeout(e);
					}
				} while (true);
			}

			/**
			 * Read bytes into a buffer starting from offset <code>off</code>
			 * Send a ping if timeout on read. Retries if no failure is detected
			 * until a byte is read.
			 * 
			 * @return the total number of bytes read; -1 if the connection is
			 *         closed.
			 */
			public int read(byte[] buf, int off, int len) throws IOException {
				do {
					try {
						return super.read(buf, off, len);
					} catch (SocketTimeoutException e) {
						handleTimeout(e);
					}
				} while (true);
			}
		}

		/**
		 * Update the server address if the address corresponding to the host
		 * name has changed.
		 * 
		 * @return true if an addr change was detected.
		 * @throws IOException
		 *             when the hostname cannot be resolved.
		 */
		private synchronized boolean updateAddress() throws IOException {
			// Do a fresh lookup with the old host name.
			InetSocketAddress currentAddr = NetUtils.makeSocketAddr(
					server.getHostName(), server.getPort());

			if (!server.equals(currentAddr)) {
				LOG.warn("Address change detected. Old: " + server.toString()
						+ " New: " + currentAddr.toString());
				server = currentAddr;
				return true;
			}
			return false;
		}
    
		private synchronized void setupConnection() throws IOException {
			short ioFailures = 0;
			short timeoutFailures = 0;
			while (true) {
				try {
					this.socket = socketFactory.createSocket();//创建Socket
					this.socket.setTcpNoDelay(tcpNoDelay);

					//设置连接超时为20s 
					NetUtils.connect(this.socket, server, 20000);
					if (rpcTimeout > 0) {
						/** rpcTimeout overwrites pingInterval */
						pingInterval = rpcTimeout;
					}

					this.socket.setSoTimeout(pingInterval);
					return;
				} catch (SocketTimeoutException toe) {
					/*
					 * Check for an address change and update the local
					 * reference. Reset the failure counter if the address was
					 * changed
					 */
					if (updateAddress()) {
						timeoutFailures = ioFailures = 0;
					}
					 /* 设置最多连接重试为45次。 
			         * 总共有20s*45 = 15 分钟的重试时间。 
			         */  
					handleConnectionFailure(timeoutFailures++, 45, toe);
				} catch (IOException ie) {
					if (updateAddress()) {
						timeoutFailures = ioFailures = 0;
					}
					handleConnectionFailure(ioFailures++, maxRetries, ie);
				}
			}
		}

		/**
		 * Connect to the server and set up the I/O streams. It then sends a
		 * header to the server and starts the connection thread that waits for
		 * responses.
		 */
		private synchronized void setupIOstreams() throws InterruptedException {
			if (socket != null || shouldCloseConnection.get()) {
				return;
			}

			try {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Connecting to " + server);
				}
				while (true) {
					setupConnection();                           //建立连接  
					InputStream inStream = NetUtils.getInputStream(socket);//SocketInputStream 有超时时间
					OutputStream outStream = NetUtils.getOutputStream(socket);
					writeRpcHeader(outStream);//IPC连接魔数（hrpc）协议版本号
					// fall back to simple auth because server told us so.
					header = new ConnectionHeader(header.getProtocol());//连接头
					this.in = new DataInputStream(new BufferedInputStream(
							new PingInputStream(inStream)));//将输入流装饰成DataInputStream  
					this.out = new DataOutputStream(new BufferedOutputStream(
							outStream));                    //将输出流装饰成DataOutputStream
					writeHeader();//发送ConnectionHeader

					//更新活动时间 
					touch();

					//当连接建立时，启动接受线程等待服务端传回数据，注意：Connection继承了Tread  
					start();
					return;
				}
			} catch (IOException e) {
				markClosed(e);
				close();
			}
		}
    
		private void closeConnection() {
			// close the current connection
			try {
				socket.close();
			} catch (IOException e) {
				LOG.warn("Not able to close a socket", e);
			}
			// set socket to null so that the next call to setupIOstreams
			// can start the process of connect all over again.
			socket = null;
		}

		/*
		 * Handle connection failures
		 * 
		 * If the current number of retries is equal to the max number of
		 * retries, stop retrying and throw the exception; Otherwise backoff 1
		 * second and try connecting again.
		 * 
		 * This Method is only called from inside setupIOstreams(), which is
		 * synchronized. Hence the sleep is synchronized; the locks will be
		 * retained.
		 * 
		 * @param curRetries current number of retries
		 * 
		 * @param maxRetries max number of retries allowed
		 * 
		 * @param ioe failure reason
		 * 
		 * @throws IOException if max number of retries is reached
		 */
		private void handleConnectionFailure(int curRetries, int maxRetries,
				IOException ioe) throws IOException {
			closeConnection();

			// throw the exception if the maximum number of retries is reached
			if (curRetries >= maxRetries) {
				throw ioe;
			}

			// otherwise back off and retry
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ignored) {
			}

			LOG.info("Retrying connect to server: " + server
					+ ". Already tried " + curRetries + " time(s).");
		}

		/* Write the RPC header */
		//写RPC头
		private void writeRpcHeader(OutputStream outStream) throws IOException {
			DataOutputStream out = new DataOutputStream(
					new BufferedOutputStream(outStream));
			// Write out the header, version and authentication method
			out.write(Server.HEADER.array());
			out.write(Server.CURRENT_VERSION);
			out.flush();
		}
    
		/*
		 * Write the protocol header for each connection Out is not synchronized
		 * because only the first thread does this.
		 */
		private void writeHeader() throws IOException {
			// Write out the ConnectionHeader
//			ByteArrayOutputStream 
			DataOutputBuffer buf = new DataOutputBuffer();
			header.write(buf);

			// Write out the payload length
			int bufLen = buf.getLength();
			out.writeInt(bufLen);//头长度
			out.write(buf.getData(), 0, bufLen);
		}
    
		/*
		 * wait till someone signals us to start reading RPC response or it is
		 * idle too long, it is marked as to be closed, or the client is marked
		 * as not running.
		 * 
		 * Return true if it is time to read a response; false otherwise.
		 */
		//返回false 会导致IPC连接关闭
		private synchronized boolean waitForWork() {
			//1 目前没有正在处理的远程调用 2 连接不需要关闭 3 客户端害处于运行状态
			if (calls.isEmpty() && !shouldCloseConnection.get()
					&& running.get()) {
				long timeout = maxIdleTime
						- (System.currentTimeMillis() - lastActivity.get());
				if (timeout > 0) {
					try {
						// 通过wait等待 可能被到达的数据打断
						//也可能被Client.stop()打断或超时
						wait(timeout);
					} catch (InterruptedException e) {
					}
				}
			}

			if (!calls.isEmpty() && !shouldCloseConnection.get()
					&& running.get()) {
				return true;//需要等待连接上返回的远程调用结果
			} else if (shouldCloseConnection.get()) {
				return false;  //shouldCloseConnection 置位 关闭连接
			} else if (calls.isEmpty()) { // idle connection closed or stopped
				markClosed(null);
				return false;//连接长时间处于关闭状态
			} else { // get stopped but there are still pending requests
				markClosed((IOException) new IOException()
						.initCause(new InterruptedException()));
				return false;
			}
		}

		public InetSocketAddress getRemoteAddress() {
			return server;
		}

		/*
		 * Send a ping to the server if the time elapsed since last I/O activity
		 * is equal to or greater than the ping interval
		 */
		//发送心跳信息
		private synchronized void sendPing() throws IOException {
			long curTime = System.currentTimeMillis();
			if (curTime - lastActivity.get() >= pingInterval) {
				lastActivity.set(curTime);
				synchronized (out) {
					out.writeInt(PING_CALL_ID);//发送-1
					out.flush();
				}
			}
		}

		public void run() {
			if (LOG.isDebugEnabled())
				LOG.debug(getName() + ": starting, having connections "
						+ connections.size());
			/** wait here for work - read or close connection*/
			while (waitForWork()) {
				receiveResponse();   //如果连接上还有数据要处理 处理数据
			}

			close();//关闭连接

			if (LOG.isDebugEnabled())
				LOG.debug(getName() + ": stopped, remaining connections "
						+ connections.size());
		}

		/**
		 * Initiates a call by sending the parameter to the remote server. Note:
		 * this is not called from the Connection thread, but by other threads.
		 */
		public void sendParam(Call call) {
			if (shouldCloseConnection.get()) {
				return;
			}

			DataOutputBuffer d = null;
			ByteArrayOutputStream baos = null;
			ObjectOutputStream data = null;
			try {
				synchronized (this.out) {
					if (LOG.isDebugEnabled())
						LOG.debug(getName() + " sending #" + call.id);

					// for serializing the
					// data to be written
					//创建一个缓冲区
					d = new DataOutputBuffer();
					
//					d.writeInt(call.id);
//					call.param.write(d);
//					out.writeInt(call.id);
					
					baos = new ByteArrayOutputStream();
					data = new ObjectOutputStream(baos);
					data.writeInt(call.id);        //调用标示ID
					data.writeObject(call.param);  //调用参数
					data.flush();
					
					byte[] buff = baos.toByteArray();
					int length = buff.length;
					
					out.writeInt(length);        //首先写出数据的长度 
					out.write(buff, 0, length);  //向服务端写数据
					out.flush();
				}
			} catch (IOException e) {
				markClosed(e);
			} finally {
				// the buffer is just an in-memory buffer, but it is still
				// polite to close early
				WritableUtils.closeStream(d, data, baos);
			}
		}  

		/*
		 * Receive a response. Because only one receiver, so no synchronization
		 * on in.
		 */
		private void receiveResponse() {
			if (shouldCloseConnection.get()) {
				return;
			}
			touch();

			try {
				int id = in.readInt(); //阻塞读取id 

				if (LOG.isDebugEnabled())
					LOG.debug(getName() + " got value #" + id);

				Call call = calls.get(id); //在calls池中找到发送时的那个对象 

				int state = in.readInt();  //阻塞读取call对象的状态 
				if (state == Status.SUCCESS.state) {
					Invocation value = null;
					try {
						ObjectInputStream ois = new ObjectInputStream(in);  //读取数据 
						value = (Invocation) ois.readObject();
					} catch (ClassNotFoundException e) {
						LOG.warn("not found class: " + e);
					}
					//将读取到的值赋给call对象，同时唤醒Client等待线程
					call.setValue(value);//会调用callComplete（）方法 Client.Call
					calls.remove(id);      //删除已处理的call
				} else if (state == Status.ERROR.state) {
					call.setException(new RemoteException(WritableUtils
							.readString(in), WritableUtils.readString(in)));
					calls.remove(id);
				} else if (state == Status.FATAL.state) {
					// Close the connection
					markClosed(new RemoteException(
							WritableUtils.readString(in),
							WritableUtils.readString(in)));
				}
			} catch (IOException e) {
				markClosed(e);
			}
		}

		private synchronized void markClosed(IOException e) {
			if (shouldCloseConnection.compareAndSet(false, true)) {
				closeException = e;
				notifyAll();
			}
		}
    
		/** Close the connection. */
		private synchronized void close() {
			if (!shouldCloseConnection.get()) {
				LOG.error("The connection is not in the closed state");
				return;
			}

			// release the resources
			// first thing to do;take the connection out of the connection list
			//从连接列表中移除当前连接
			synchronized (connections) {
				if (connections.get(remoteId) == this) {
					connections.remove(remoteId);
				}
			}

			//关闭输入/输出流
			WritableUtils.closeStream(out, in);

			// clean up all calls
			if (closeException == null) {
				if (!calls.isEmpty()) {
					LOG.warn("A connection is closed for no cause and calls are not empty");

					// clean up calls anyway
					closeException = new IOException(
							"Unexpected closed connection");
					cleanupCalls();
				}
			} else {
				// log the info
				if (LOG.isDebugEnabled()) {
					LOG.debug("closing ipc connection to " + server + ": "
							+ closeException.getMessage(), closeException);
				}

				// cleanup calls
				cleanupCalls();
			}
			if (LOG.isDebugEnabled())
				LOG.debug(getName() + ": closed");
		}

		//为当前连接上未完成的远程调用设置异常 结束远程远程调用
		private void cleanupCalls() {
			Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator();
			while (itor.hasNext()) {
				Call c = itor.next().getValue();
				c.setException(closeException); // local exception
				itor.remove();
			}
		}
	}

	/**
	 * Construct an IPC client whose values are of the given
	 * {@link SocketFactory} class.
	 */
	public Client(SocketFactory factory) {
		this.socketFactory = factory;
	}

	/**
	 * Construct an IPC client with the default SocketFactory
	 */
	public Client() {
		this(NetUtils.getDefaultSocketFactory());
	}
 
	/**
	 * Return the socket factory of this client
	 * 
	 * @return this client's socket factory
	 */
	SocketFactory getSocketFactory() {
		return socketFactory;
	}

	/**
	 * Stop all threads related to this client. No further calls may be made
	 * using this client.
	 */
	public void stop() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Stopping client");
		}

		if (!running.compareAndSet(true, false)) {
			return;
		}

		// wake up all connections
		synchronized (connections) {
			for (Connection conn : connections.values()) {
				conn.interrupt();//中断线程
			}
		}

		// wait until all connections are closed
		while (!connections.isEmpty()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}
  
	/**
	 * Make a call, passing <code>param</code>, to the IPC server running at
	 * <code>address</code> which is servicing the <code>protocol</code>
	 * protocol, with the <code>ticket</code> credentials,
	 * <code>rpcTimeout</code> as timeout and <code>conf</code> as configuration
	 * for this connection, returning the value. Throws exceptions if there are
	 * network problems or if the remote code threw an exception.
	 */
	public Serializable call(Invocation param, InetSocketAddress addr,
			Class<?> protocol, int rpcTimeout) throws InterruptedException,
			IOException {
		ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
				rpcTimeout);
		return call(param, remoteId);
	}

	/**
	 * Make a call, passing <code>param</code>, to the IPC server defined by
	 * <code>remoteId</code>, returning the value. Throws exceptions if there
	 * are network problems or if the remote code threw an exception.
	 */
	/*call()在RPC.Invoker invoke()中*/
	public Invocation call(Invocation invoked, ConnectionId remoteId)
			throws InterruptedException, IOException {
		Call call = new Call(invoked);    //将传入的数据封装成call对象 Serializable接口
		//已经向服务器端 RPCHeader ConnectionHeader验证
		Connection connection = getConnection(remoteId, call); //获得一个连接  
		connection.sendParam(call); // 向服务端发送Call对象
		boolean interrupted = false;
		synchronized (call) {
			while (!call.done) {
				try {
					call.wait(); //等待结果的返回，在Call类的callComplete()方法里有notify()方法用于唤醒线程  
				} catch (InterruptedException ie) {
					// save the fact that we were interrupted
					interrupted = true;
				}
			}

			if (interrupted) {
				//因中断异常而终止，设置标志interrupted为true 
				Thread.currentThread().interrupt();
			}

			if (call.error != null) {
				if (call.error instanceof RemoteException) {
					call.error.fillInStackTrace();
					throw call.error;
				} else {
					/* local exception use the connection because it will
					 * reflect an ip change, unlike the remoteId
					 */
					throw wrapException(connection.getRemoteAddress(),
							call.error);
				}
			} else {
				return call.value; //返回结果数据  
			}
		}
	}

	/**
	 * Take an IOException and the address we were trying to connect to and
	 * return an IOException with the input exception as the cause. The new
	 * exception provides the stack trace of the place where the exception is
	 * thrown and some extra diagnostics information. If the exception is
	 * ConnectException or SocketTimeoutException, return a new one of the same
	 * type; Otherwise return an IOException.
	 * 
	 * @param addr
	 *            target address
	 * @param exception
	 *            the relevant exception
	 * @return an exception to throw
	 */
	private IOException wrapException(InetSocketAddress addr,
			IOException exception) {
		if (exception instanceof ConnectException) {
			// connection refused; include the host:port in the error
			return (ConnectException) new ConnectException("Call to " + addr
					+ " failed on connection exception: " + exception)
					.initCause(exception);
		} else if (exception instanceof SocketTimeoutException) {
			return (SocketTimeoutException) new SocketTimeoutException(
					"Call to " + addr + " failed on socket timeout exception: "
							+ exception).initCause(exception);
		} else {
			return (IOException) new IOException("Call to " + addr
					+ " failed on local exception: " + exception)
					.initCause(exception);

		}
	}

	// for unit testing only
	Set<ConnectionId> getConnectionIds() {
		synchronized (connections) {
			return connections.keySet();
		}
	}

	/**
	 * Get a connection from the pool, or create a new one and add it to the
	 * pool. Connections to a given ConnectionId are reused.
	 */
	private Connection getConnection(ConnectionId remoteId, Call call)
			throws IOException, InterruptedException {
		if (!running.get()) {
			//如果client关闭了  
			throw new IOException("The client is stopped");
		}
		Connection connection;
		//如果connections连接池中有对应的连接对象，就不需重新创建了；如果没有就需重新创建一个连接对象。  
		//但请注意，该连接对象只是存储了remoteId的信息，其实还并没有和服务端建立连接。 
		do {
			synchronized (connections) {
				connection = connections.get(remoteId);
				if (connection == null) {
					connection = new Connection(remoteId);
					connections.put(remoteId, connection);
				}
			}
		} while (!connection.addCall(call));//将call对象放入对应连接中的calls池

		// we don't invoke the method below inside "synchronized (connections)"
		// block above. The reason for that is if the server happens to be slow,
		// it will take longer to establish a connection and that will slow the
		// entire system down.
		//这句代码才是真正的完成了和服务端建立连接
		connection.setupIOstreams();
		return connection;
	}

	/**
	 * This class holds the address and the user ticket. The client connections
	 * to servers are uniquely identified by <remoteAddress, protocol, ticket>
	 */
	static class ConnectionId {
		InetSocketAddress address;                 //连接实例的Socket地址 
		Class<?> protocol;                         //连接的协议
		private static final int PRIME = 16777619;
		private int rpcTimeout;
		private String serverPrincipal;
		/**connections will be culled if it was idle for maxIdleTime msecs */
		private int maxIdleTime;
		// the max. no. of retries for socket connections
		private int maxRetries;
		private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
		private int pingInterval; // how often sends ping to the server in msecs

		ConnectionId(InetSocketAddress address, Class<?> protocol,
				int rpcTimeout, String serverPrincipal, int maxIdleTime,
				int maxRetries, boolean tcpNoDelay, int pingInterval) {
			this.protocol = protocol;
			this.address = address;
			this.rpcTimeout = rpcTimeout;
			this.serverPrincipal = serverPrincipal;
			this.maxIdleTime = maxIdleTime;
			this.maxRetries = maxRetries;
			this.tcpNoDelay = tcpNoDelay;
			this.pingInterval = pingInterval;
		}

		InetSocketAddress getAddress() {
			return address;
		}

		Class<?> getProtocol() {
			return protocol;
		}

		private int getRpcTimeout() {
			return rpcTimeout;
		}

		String getServerPrincipal() {
			return serverPrincipal;
		}

		int getMaxIdleTime() {
			return maxIdleTime;
		}

		int getMaxRetries() {
			return maxRetries;
		}

		boolean getTcpNoDelay() {
			return tcpNoDelay;
		}

		int getPingInterval() {
			return pingInterval;
		}

		static ConnectionId getConnectionId(InetSocketAddress addr,
				Class<?> protocol) throws IOException {
			return getConnectionId(addr, protocol, 0);
		}

		static ConnectionId getConnectionId(InetSocketAddress addr,
				Class<?> protocol, int rpcTimeout) throws IOException {
			return new ConnectionId(addr, protocol, rpcTimeout, null, 10000,
					10, false, Client.getPingInterval());
		}     
     
		static boolean isEqual(Object a, Object b) {
			return a == null ? b == null : a.equals(b);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (obj instanceof ConnectionId) {
				ConnectionId that = (ConnectionId) obj;
				return isEqual(this.address, that.address)
						&& this.maxIdleTime == that.maxIdleTime
						&& this.maxRetries == that.maxRetries
						&& this.pingInterval == that.pingInterval
						&& isEqual(this.protocol, that.protocol)
						&& this.rpcTimeout == that.rpcTimeout
						&& isEqual(this.serverPrincipal, that.serverPrincipal)
						&& this.tcpNoDelay == that.tcpNoDelay;
			}
			return false;
		}

		@Override
		public int hashCode() {
			int result = 1;
			result = PRIME * result
					+ ((address == null) ? 0 : address.hashCode());
			result = PRIME * result + maxIdleTime;
			result = PRIME * result + maxRetries;
			result = PRIME * result + pingInterval;
			result = PRIME * result
					+ ((protocol == null) ? 0 : protocol.hashCode());
			result = PRIME * rpcTimeout;
			result = PRIME * result
					+ ((serverPrincipal == null) ? 0 : serverPrincipal
							.hashCode());
			result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
			return result;
		}
	}
}
