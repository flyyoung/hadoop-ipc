package com.jiangdx.ipc;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An abstract IPC service. IPC calls take a single {@link Invocation} as a
 * parameter, and return a {@link Invocation} as their value. A service runs on a
 * port and is defined by a parameter class and a value class.
 * 
 * @see Client
 */
public abstract class Server {
	/**
	 * The first four bytes of Hadoop RPC connections
	 */
	public static final ByteBuffer HEADER = ByteBuffer.wrap("hrpc".getBytes());
  
	// 1 : Introduce ping and server does not throw away RPCs
	// 3 : Introduce the protocol into the RPC connection header
	// 4 : Introduced SASL security layer
	public static final byte CURRENT_VERSION = 4;
  
	public static final int IPC_SERVER_RPC_READ_THREADS_DEFAULT = 1;

	/**
	 * How many calls/handler are allowed in the queue.
	 */
	private static final int IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT = 100;

	/**
	 * Initial and max size of response buffer
	 */
	static int INITIAL_RESP_BUF_SIZE = 10240;
	static final int IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT = 1024 * 1024;

	public static final Log LOG = LogFactory.getLog(Server.class);

	private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

	private static final Map<String, Class<?>> PROTOCOL_CACHE = new ConcurrentHashMap<String, Class<?>>();
	/** <interface, implementation> map for invocation */
	protected static final Map<Class<?>, Object> INSTANCE_CACHE = new ConcurrentHashMap<Class<?>, Object>();
  
	/*根据协议名获得协议类*/
	static Class<?> getProtocolClass(String protocolName)
			throws ClassNotFoundException {
		Class<?> protocol = PROTOCOL_CACHE.get(protocolName);
		if (protocol == null)
			throw new ClassNotFoundException("class `" + protocolName
					+ "` not found.");
		return protocol;
	}

	/** inscribe to the `<interface, implementation>` map. */
	//缓存实例 协议类
	public void inscribe(Class<?> iface, Object instance) {
		if (iface == null || instance == null)
			throw new IllegalArgumentException("Both interface and implementation require.");

		Object cached = INSTANCE_CACHE.get(iface);
		if (cached == null)
			INSTANCE_CACHE.put(iface, instance);
		
		/** <interface_name, interface> */
		Class<?> protocol = PROTOCOL_CACHE.get(iface.getName());
		if (protocol == null) {
			PROTOCOL_CACHE.put(iface.getName(), iface);
		}
	}
	
	/**
	 * Returns the server instance called under or null. May be called under
	 * {@link #call(Invocation, long)} implementations, and under {@link Invocation}
	 * methods of paramters and return values. Permits applications to access
	 * the server context.
	 */
	public static Server get() {
		return SERVER.get();
	}

	/**
	 * This is set to Call object before Handler invokes an RPC and reset after
	 * the call returns.
	 */
	/*Call用于存储客户端发来的请求*/
	private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

	/**
	 * Returns the remote side ip address when invoked inside an RPC Returns
	 * null incase of an error.
	 */
	public static InetAddress getRemoteIp() {
		Call call = CurCall.get();
		if (call != null) {
			return call.connection.socket.getInetAddress();
		}
		return null;
	}

	/**
	 * Returns remote address as a string when invoked inside an RPC. Returns
	 * null in case of an error.
	 */
	public static String getRemoteAddress() {
		InetAddress addr = getRemoteIp();
		return (addr == null) ? null : addr.getHostAddress();
	}

	private String bindAddress;                    //服务端绑定的地址
	/** port we listen on */
	private int port;                              //服务端监听端口 
	/** number of handler threads */
	private int handlerCount;                      //处理器的数量
	/** number of read threads */
	private int readThreads;                       //
	/** the maximum idle time after which a client may be disconnected */
	private int maxIdleTime;                       //一个客户端连接后的最大空闲时间
	/**
	 * the number of idle connections after which we will start cleaning up idle
	 * connections
	 */
	private int thresholdIdleConnections;         //可维护的最大连接数量
	/** the max number of connections to nuke during a cleanup */
	int maxConnectionsToNuke;                     //

	private int maxQueueSize;                     //处理器Handler实例队列大小
	private final int maxRespSize;
	private int socketSendBufferSize;             //Socket Buffer大小
	/** if T then disable Nagle's Algorithm */
	private final boolean tcpNoDelay;             //TCP连接是否不延迟
	/** true while server runs */
	volatile private boolean running = true;      //Server是否运行  
	//存放Call的队列
	private BlockingQueue<Call> callQueue;        //待处理的rpc调用队列
	/** maintain a list of client connections */
	//RPC Server数据接收者 提供接收数据 解析数据包的功能
	//维护客户端连接的列表  
	private List<Connection> connectionList = Collections  
			.synchronizedList(new LinkedList<Connection>());
	//RPC Server的监听者 用来接收RPC Client的连接请求和数据 其中数据封装成Call后PUSH到Call队列
	private Listener listener = null;
	//RPC Server的响应者 Server.Handler按照异步非阻塞的方式向RPC Client发送响应 如果有未发送出的数据 交由Server.Responder来完成
	private Responder responder = null;
	//连接数量
	private int numConnections = 0;
	//RPC Server的Call处理者 和Server.Listener通过Call队列交互
	private Handler[] handlers = null;

  /**
   * A convenience method to bind to a given address and report 
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(ServerSocket socket, InetSocketAddress address, 
                          int backlog) throws IOException {
    try {
      socket.bind(address, backlog);
    } catch (BindException e) {
      BindException bindException = new BindException("Problem binding to " + address
                                                      + " : " + e.getMessage());
      bindException.initCause(e);
      throw bindException;
    } catch (SocketException e) {
      // If they try to bind to a different host's address, give a better
      // error message.
      if ("Unresolved address".equals(e.getMessage())) {
        throw new UnknownHostException("Invalid hostname for server: " + 
                                       address.getHostName());
      } else {
        throw e;
      }
    }
  }
  
  /** A call queued for handling. */
  private static class Call {
    private int id;                               // the client's call id
    private Serializable param;                       // the parameter passed
    private Connection connection;                // connection to client
    private long timestamp;     // the time received when response is null
                                   // the time served when response is not null
    private ByteBuffer response;                      // the response for this call

    public Call(int id, Serializable param, Connection connection) { 
      this.id = id;
      this.param = param;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
    }
    
    @Override
    public String toString() {
      return param.toString() + " from " + connection.toString();
    }

    public void setResponse(ByteBuffer response) {
      this.response = response;
    }
  }

	/** Listens on the socket. Creates jobs for the handler threads */
	private class Listener extends Thread {
		/** the accept channel */
		private ServerSocketChannel acceptChannel = null;
		/** the selector that we use for the server */
		private Selector selector = null;
		private Reader[] readers = null;
		private int currentReader = 0;
		/** the address we bind at */
		private InetSocketAddress address; 
		private Random rand = new Random();
		/** the last time when a cleanup connection (for idle connections) */
		private long lastCleanupRunTime = 0;       //上一次清理客户端连接的时间 
		/** the minimum interval between two cleanup runs */
		private long cleanupInterval = 10000;      //清理客户端端连接的时间间隔 
		// private int backlogLength =
		// conf.getInt("ipc.server.listen.queue.size", 128);
		private int backlogLength = 128;           //允许客户端等待连接的队列长度

		private ExecutorService readPool;
   
		public Listener() throws IOException {
			address = new InetSocketAddress(bindAddress, port);
			// Create a new server socket and set to non blocking mode
			acceptChannel = ServerSocketChannel.open();
			acceptChannel.configureBlocking(false);

			// Bind the server socket to the local host and port
			bind(acceptChannel.socket(), address, backlogLength);
			port = acceptChannel.socket().getLocalPort(); // Could be an
															// ephemeral port
			// create a selector;
			selector = Selector.open();
			//Reader用来读取用户请求
			readers = new Reader[readThreads];
			readPool = Executors.newFixedThreadPool(readThreads);
			for (int i = 0; i < readThreads; i++) {
				Selector readSelector = Selector.open();
				Reader reader = new Reader(readSelector);
				readers[i] = reader;
				readPool.execute(reader);
			}

			// Register accepts on the server socket with the selector.
			acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
			this.setName("IPC Server listener on " + port);
			this.setDaemon(true);
		}
    
		private class Reader implements Runnable {
			//adding代表什么?
			private volatile boolean adding = false;
			private Selector readSelector = null;

			Reader(Selector readSelector) {
				this.readSelector = readSelector;
			}

			public void run() {
				LOG.info("Starting SocketReader");
				synchronized (this) {
					while (running) {
						SelectionKey key = null;
						try {
							readSelector.select();//无限阻塞
							while (adding)        //当adding为true时线程阻塞
								this.wait(1000);
							
							Iterator<SelectionKey> iter = readSelector
									.selectedKeys().iterator();
							while (iter.hasNext()) {
								key = iter.next();
								iter.remove();
								if (key.isValid()) {
									if (key.isReadable()) {
										doRead(key);
									}
								}
								key = null;
							}
						} catch (InterruptedException e) {
							if (running) { // unexpected -- log it
								LOG.info(getName() + " caught: " + e);
							}
						} catch (IOException ex) {
							LOG.error("Error in Reader", ex);
						}
					}
				}
			}

			/**
			 * This gets reader into the state that waits for the new channel to
			 * be registered with readSelector. If it was waiting in select()
			 * the thread will be woken up, otherwise whenever select() is
			 * called it will return even if there is nothing to read and wait
			 * in while(adding) for finishAdd call
			 */
			//使reader等待新的channel被注册到readSelector
			public void startAdd() {
				adding = true;
				readSelector.wakeup();
			}

			public synchronized SelectionKey registerChannel(
					SocketChannel channel) throws IOException {
				return channel.register(readSelector, SelectionKey.OP_READ);
			}

			public synchronized void finishAdd() {
				adding = false;
				this.notify();
			}
		}

    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all 
     * connections will be looked at for the cleanup.
     */
    private void cleanupConnections(boolean force) {
      if (force || numConnections > thresholdIdleConnections) {
        long currentTime = System.currentTimeMillis();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        int start = 0;
        int end = numConnections - 1;
        if (!force) {
          start = rand.nextInt() % numConnections;
          end = rand.nextInt() % numConnections;
          int temp;
          if (end < start) {
            temp = start;
            start = end;
            end = temp;
          }
        }
        int i = start;
        int numNuked = 0;
        while (i <= end) {
          Connection c;
          synchronized (connectionList) {
            try {
              c = connectionList.get(i);
            } catch (Exception e) {return;}
          }
          if (c.timedOut(currentTime)) {
            if (LOG.isDebugEnabled())
              LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
            closeConnection(c);
            numNuked++;
            end--;
            c = null;
            if (!force && numNuked == maxConnectionsToNuke) break;
          }
          else i++;
        }
        lastCleanupRunTime = System.currentTimeMillis();
      }
    }

    	@Override
		public void run() {
			LOG.info(getName() + ": starting");
			SERVER.set(Server.this);
			while (running) {
				SelectionKey key = null;
				try {
					selector.select();
					Iterator<SelectionKey> iter = selector.selectedKeys()
							.iterator();
					while (iter.hasNext()) {
						key = iter.next();
						iter.remove();
						try {
							if (key.isValid()) {
								if (key.isAcceptable())
									doAccept(key);//具体连接的方法
							}
						} catch (IOException e) {
						}
						key = null;
					}
				} catch (OutOfMemoryError e) {
					// we can run out of memory if we have too many threads
					// log the event and sleep for a minute and give
					// some thread(s) a chance to finish
					LOG.warn("Out of Memory in server select", e);
					closeCurrentConnection(key, e);
					cleanupConnections(true);
					try {
						Thread.sleep(60000);
					} catch (Exception ie) {
					}
				} catch (Exception e) {
					closeCurrentConnection(key, e);
				}
				cleanupConnections(false);
			}
			LOG.info("Stopping " + this.getName());

			synchronized (this) {
				try {
					acceptChannel.close();
					selector.close();
				} catch (IOException e) {
				}

				selector = null;
				acceptChannel = null;

				// clean up all connections
				while (!connectionList.isEmpty()) {
					closeConnection(connectionList.remove(0));
				}
			}
		}

		private void closeCurrentConnection(SelectionKey key, Throwable e) {
			if (key != null) {
				Connection c = (Connection) key.attachment();
				if (c != null) {
					if (LOG.isDebugEnabled())
						LOG.debug(getName() + ": disconnecting client "
								+ c.getHostAddress());
					closeConnection(c);
					c = null;
				}
			}
		}

		InetSocketAddress getAddress() {
			return (InetSocketAddress) acceptChannel.socket()
					.getLocalSocketAddress();
		}

		/*连接*/
		void doAccept(SelectionKey key) throws IOException, OutOfMemoryError {
			Connection c = null;
			ServerSocketChannel server = (ServerSocketChannel) key.channel();
			SocketChannel channel;
			while ((channel = server.accept()) != null) {          //建立连接
				channel.configureBlocking(false);
				channel.socket().setTcpNoDelay(tcpNoDelay);
				Reader reader = getReader();                      //从readers池中获得一个reader
				try {
					reader.startAdd();           //激活readSelector，设置adding为true 
					SelectionKey readKey = reader.registerChannel(channel);//在readSelector上注册
					c = new Connection(readKey, channel,
							System.currentTimeMillis());                   //创建一个连接对象
					//附加Connection对象
					readKey.attach(c);                                     //将connection对象注入readKey  
					synchronized (connectionList) {
						connectionList.add(numConnections, c);
						numConnections++;
					}
					if (LOG.isDebugEnabled())
						LOG.debug("Server connection from " + c.toString()
								+ "; # active connections: " + numConnections
								+ "; # queued calls: " + callQueue.size());
				} finally {
					//设置adding为false，采用notify()唤醒一个reader
					reader.finishAdd();
				}
			}
		}

		/*读取*/
		void doRead(SelectionKey key) throws InterruptedException {
			int count = 0;
			//取出附加的Connection对象
			Connection c = (Connection) key.attachment();
			if (c == null) {
				return;
			}
			c.setLastContact(System.currentTimeMillis());

			try {
				//接受并处理请求
				count = c.readAndProcess(); //调用Connection.readAndProcess()
			} catch (InterruptedException ieo) {
				LOG.info(getName()
						+ ": readAndProcess caught InterruptedException", ieo);
				throw ieo;
			} catch (Exception e) {
				LOG.info(getName() + ": readAndProcess threw exception " + e
						+ ". Count of bytes read: " + count, e);
				count = -1; // so that the (count < 0) block is executed
			}
			if (count < 0) {
				if (LOG.isDebugEnabled())
					LOG.debug(getName() + ": disconnecting client " + c
							+ ". Number of active connections: "
							+ numConnections);
				closeConnection(c);
				c = null;
			} else {
				c.setLastContact(System.currentTimeMillis());
			}
		}

		synchronized void doStop() {
			if (selector != null) {
				selector.wakeup();
				Thread.yield();
			}
			if (acceptChannel != null) {
				try {
					acceptChannel.socket().close();
				} catch (IOException e) {
					LOG.info(getName()
							+ ":Exception in closing listener socket. " + e);
				}
			}
			readPool.shutdown();
		}

		// The method that will return the next reader to work with
		// Simplistic implementation of round robin for now
		Reader getReader() {
			currentReader = (currentReader + 1) % readers.length;
			return readers[currentReader];
		}
	}

  // Sends responses of RPC back to clients.
  private class Responder extends Thread {
    private Selector writeSelector;
    private int pending;         // connections waiting to register
    
    final static int PURGE_INTERVAL = 900000; // 15mins

    Responder() throws IOException {
      this.setName("IPC Server Responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(Server.this);
      long lastPurgeTime = 0;   // last check for old calls.

      while (running) {
        try {
          waitPending(); //等待通道登记
          writeSelector.select(PURGE_INTERVAL);//等待通道可写
          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
            	  //输出远程调用结果
                  doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.info(getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          long now = System.currentTimeMillis();
          if (now < lastPurgeTime + PURGE_INTERVAL) {
            continue;
          }
          lastPurgeTime = now;
          
          
          LOG.debug("Checking for old call responses.");
          //已经超过了清理时间 可以检查未处理的call
          ArrayList<Call> calls;
          
          //通过SelectionKey附件 从writeSelector中获得注册过的（未关闭）连接
          synchronized (writeSelector.keys()) {
            calls = new ArrayList<Call>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              Call call = (Call)key.attachment();
              if (call != null && key.channel() == call.connection.channel) { 
                calls.add(call);
              }
            }
          }
          
          for(Call call : calls) {
            try {
              doPurge(call, now);//调用doPurge进行清理
            } catch (IOException e) {
              LOG.warn("Error in purging old calls " + e);
            }
          }
        } catch (OutOfMemoryError e) {
          //
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give
          // some thread(s) a chance to finish
          //
          LOG.warn("Out of Memory in server select", e);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (Exception e) {
          LOG.warn("Exception in Responder " + 
                   e);
        }
      }
      LOG.info("Stopping " + this.getName());
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Call call = (Call)key.attachment();//
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      synchronized(call.connection.responseQueue) {
    	//调用processResponse（）写数据 返回true 表明通道上没有等待的数据
    	//processResponse()是应答器最终调用channelWrite()发送数据的地方
    	//参数inHandler为true 在Handler中被调用 同时 responseQueue只有一个等待被发送的远程调用
    	//参数inHandler为false 在Responder中被调用 同时 responseQueue有一个或多个等待被发送的远程调用
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);//清除兴趣操作集 也就是清除对OP_WRITE的监听
          } catch (CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

   //清理工作的时间间隔是15分钟（不可设置） 如果某个连接上有15分钟前的应答未发送 这个连接将被清除
    private void doPurge(Call call, long now) throws IOException {
      LinkedList<Call> responseQueue = call.connection.responseQueue;
      synchronized (responseQueue) {
        Iterator<Call> iter = responseQueue.listIterator(0);//返回下标从0开始的迭代器
        while (iter.hasNext()) {
          call = iter.next();
          if (now > call.timestamp + PURGE_INTERVAL) {//大于15分钟
            closeConnection(call.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    private boolean processResponse(LinkedList<Call> responseQueue,
                                    boolean inHandler) throws IOException {
      boolean error = true;
      boolean done = false;       // there is more data for this channel.
      int numElements = 0;
      Call call = null;
      try {
        synchronized (responseQueue) {
          //
          // If there are no items for this channel, then we are done
          //
          numElements = responseQueue.size();
          if (numElements == 0) {
            error = false;
            return true;              // 通道上没有等待的数据
          }
          
          //获取第一个应答
          call = responseQueue.removeFirst();
          SocketChannel channel = call.connection.channel;
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": responding to #" + call.id + " from " +
                      call.connection);
          }
          
          //异步写尽可能多的数据
          int numBytes = channelWrite(channel, call.response);
          if (numBytes < 0) {
            return true;
          }
          if (!call.response.hasRemaining()) {
        	//应答数据已经写完
            call.connection.decRpcCount();
            if (numElements == 1) {    
              done = true;             //该通道上没有需要写的数据
            } else {
              done = false;            //通道上还有数据要发送
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to #" + call.id + " from " +
                        call.connection + " Wrote " + numBytes + " bytes.");
            }
          } else {
            //应答数据还没有写完 插入队列头 等待再次发送
            call.connection.responseQueue.addFirst(call);
            
            if (inHandler) {//在Handler线程中
              //更新应答时间
              call.timestamp = System.currentTimeMillis();
              //成员变量pending++ 表示现在有多少个线程在进行通道注册
              incPending();
              try {
                //唤醒在select()方法上等待的Responder线程
            	 //这样我们才能调用channel.register()方法
                writeSelector.wakeup();
                channel.register(writeSelector, SelectionKey.OP_WRITE, call);//当前call作为SelectionKey的附件
              } catch (ClosedChannelException e) {
                //ClosedChannelException异常 通道可能在其他地方被关闭了
                done = true;
              } finally {
            	//成员变量pending-- 表示现在有多少个线程在进行通道注册
                decPending();
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to #" + call.id + " from " +
                        call.connection + " Wrote partial " + numBytes + 
                        " bytes.");
            }
          }
          error = false;              //没有错误
        }
      } finally {
        if (error && call != null) {
          LOG.warn(getName()+", call " + call + ": output error");
          //出错 关闭连接 连接关闭后当然通道上也就没有数据可写了
          done = true;               // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }

    //使用应答器发送IPC结果 
    //把应答对应的远程调用对象放入IPC连接的应答队列里
    void doRespond(Call call) throws IOException {
      synchronized (call.connection.responseQueue) {
        call.connection.responseQueue.addLast(call);
        //Responder不会马上将该Call的处理结果发送回客户端
        //如果IPC连接的应答队列只有一个元素 立即调用Handler的processResponse（）
        if (call.connection.responseQueue.size() == 1) {//应答队列只有一个元素
        	//返回响应结果，并激活writeSelector  
          processResponse(call.connection.responseQueue, true);
        }
      }
    }

    private synchronized void incPending() {   // call waiting to be enqueued.
      pending++;
    }

    private synchronized void decPending() { // call done enqueueing.
      pending--;
      notify();
    }

    private synchronized void waitPending() throws InterruptedException {
      while (pending > 0) { //挂起
        wait();
      }
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  public class Connection {
    private boolean rpcHeaderRead = false; // if initial rpc header is read
    private boolean headerRead = false;  //if the connection header that
                                         //follows version is read.

    private SocketChannel channel;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    private LinkedList<Call> responseQueue;
    private volatile int rpcCount = 0; // number of outstanding rpcs
    private long lastContact;
    private int dataLength;
    private Socket socket;
    // Cache the remote host & port info so that even if the socket is 
    // disconnected, we can say where it used to connect to.
    private String hostAddress;
    private int remotePort;
    private InetAddress addr;
    
    ConnectionHeader header = new ConnectionHeader();
    Class<?> protocol;//协议类
    boolean useSasl;
    private ByteBuffer rpcHeaderBuffer;
    
    private boolean useWrap = false;
    
    public Connection(SelectionKey key, SocketChannel channel, 
                      long lastContact) {
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);//分配一个4个字节缓冲区
      this.socket = channel.socket();
      this.addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new LinkedList<Call>();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }   

    @Override
    public String toString() {
      return getHostAddress() + ":" + remotePort; 
    }
    
    public String getHostAddress() {
      return hostAddress;
    }

    public InetAddress getHostInetAddress() {
      return addr;
    }
    
    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

		/* Return true if the connection has no outstanding rpc */
		private boolean isIdle() {
			return rpcCount == 0;
		}
    
		/* Decrement the outstanding RPC count */
		private void decRpcCount() {
			rpcCount--;
		}

		/* Increment the outstanding RPC count */
		private void incRpcCount() {
			rpcCount++;
		}
    
		private boolean timedOut(long currentTime) {
			if (isIdle() && currentTime - lastContact > maxIdleTime)
				return true;
			return false;
		}
        //读取和处理Call
		public int readAndProcess() throws IOException, InterruptedException {
			while (true) {
				/*
				 * Read at most one RPC. If the header is not read completely
				 * yet then iterate until we read first RPC or until there is no
				 * data left.
				 */
				int count = -1;
				//返回当前位置与限制之间的元素数
				if (dataLengthBuffer.remaining() > 0) {  
					//读入“显示长度” dataLengthBuffer一共4个字节
					count = channelRead(channel, dataLengthBuffer);
					if (count < 0 || dataLengthBuffer.remaining() > 0)//显示长度没有读完或者报错
						return count;
				}
				
				if (!rpcHeaderRead) {
					// Every connection is expected to send the header.
					if (rpcHeaderBuffer == null) {
						/** RPC header only contains the `VERSION` */
						rpcHeaderBuffer = ByteBuffer.allocate(1); //分配一个字节的缓冲区
					}
					count = channelRead(channel, rpcHeaderBuffer);
					if (count < 0 || rpcHeaderBuffer.remaining() > 0) {
						return count;
					}
					//读取请求版本号
					int version = rpcHeaderBuffer.get(0);
					dataLengthBuffer.flip();
					if (!HEADER.equals(dataLengthBuffer)
							|| version != CURRENT_VERSION) {
						// Warning is ok since this is not supposed to happen.
						LOG.warn("Incorrect header or version mismatch from "
								+ hostAddress + ":" + remotePort
								+ " got version " + version
								+ " expected version " + CURRENT_VERSION);
						return -1;
					}
					dataLengthBuffer.clear();
					rpcHeaderBuffer = null;
					rpcHeaderRead = true;
					continue;
				}

				if (data == null) {
					dataLengthBuffer.flip();
					dataLength = dataLengthBuffer.getInt();//数据长度已读取

					if (dataLength == Client.PING_CALL_ID) {//心跳信息
						if (!useWrap) { // covers the !useSasl too
							dataLengthBuffer.clear();
							return 0; // ping 信息
						}
					}
					if (dataLength < 0) {
						LOG.warn("Unexpected data length " + dataLength
								+ "!! from " + getHostAddress());
					}
					data = ByteBuffer.allocate(dataLength);//为读取数据分配缓冲区
				}
				// 读取请求
				count = channelRead(channel, data);

				if (data.remaining() == 0) {//读到一个完整的信息
					dataLengthBuffer.clear();
					data.flip();

					boolean isHeaderRead = headerRead; //默认false
					//处理请求
					processOneRpc(data.array());
					data = null;
					if (!isHeaderRead) {
						continue;
					}
				}
				return count;
			}
		}

		/** Reads the connection header following version */
		private void processHeader(byte[] buf) throws IOException {
			header.readFields(buf);
			try {
				String iface = header.getProtocol();//获取协议名称
				if (iface != null) {
					protocol = getProtocolClass(iface);//根据协议名称获取协议类
				}
			} catch (ClassNotFoundException cnfe) {
				throw new IOException("Unknown protocol: "
						+ header.getProtocol(), cnfe);
			}
		}
    
		private void processOneRpc(byte[] buf) throws IOException,
				InterruptedException {
			if (headerRead) {
				//处理实际数据
				processData(buf);
			} else {
				//处理连接头
				processHeader(buf);
				headerRead = true;
			}
		}
    //处理一帧数据
    private void processData(byte[] buf) throws  IOException, InterruptedException {
			Invocation param = null;
			int id = 0;
			try {
				ByteArrayInputStream bais = new ByteArrayInputStream(buf);
				ObjectInputStream ois = new ObjectInputStream(bais);
				id = ois.readInt();       //读取调用标志符
				if (LOG.isDebugEnabled())
					LOG.debug(" got #" + id);
				param = (Invocation) ois.readObject(); //读取调用参数 Invocationd对象包含方法名称 形式参数列表和实际参数列表
			} catch (ClassNotFoundException e) {
				LOG.warn("not found class: " + e);
			}

			Call call = new Call(id, param, this);  //封装成call
			callQueue.put(call); //将call存入callQueue Listener-Handler 生产者-消费者
			incRpcCount(); //增加rpc请求的计数
		}

    
    private synchronized void close() throws IOException {
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception e) {}
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception e) {}
      }
      try {socket.close();} catch(Exception e) {}
    }
  }

	/** Handles queued calls . */
    //处理请求队列
	private class Handler extends Thread {
		public Handler(int instanceNumber) {
			this.setDaemon(true);
			this.setName("IPC Server handler " + instanceNumber + " on " + port);
		}

		@Override
		public void run() {
			LOG.info(getName() + ": starting");
			SERVER.set(Server.this);
			//创建大小为10240个字节的响应缓冲区
			ByteArrayOutputStream buf = new ByteArrayOutputStream(
					INITIAL_RESP_BUF_SIZE);
			while (running) {
				try {
					/** pop the queue; maybe blocked here */
					//获取一个远程调用请求 Server.Call
					final Call call = callQueue.take();       //弹出call，可能会阻塞 

					if (LOG.isDebugEnabled())
						LOG.debug(getName() + ": has #" + call.id + " from "
								+ call.connection);

					String errorClass = null;
					String error = null;
					Serializable value = null;
					
					/** process the current call. */
					//处理当前Call
					CurCall.set(call);
					try {
						//调用ipc.Server类中的call()方法，但该call()方法是抽象方法，具体实现在RPC.Server类中 
						value = call(call.connection.protocol, call.param,
								call.timestamp);  //Invocation对象
					} catch (Throwable e) {
						LOG.info(getName() + ", call " + call + ": error: " + e, e);
						errorClass = e.getClass().getName();
						error = e.getMessage();
					}
					
					CurCall.set(null);
					synchronized (call.connection.responseQueue) {
						/**
						 * setupResponse() needs to be sync'ed together with
						 * responder.doResponse() since setupResponse may use
						 * SASL to encrypt response data and SASL enforces its
						 * own message ordering.
						 */
						//将返回结果序列化到Call的成员变量response中
						setupResponse(
								buf,
								call,
								(error == null) ? Status.SUCCESS : Status.ERROR,
								value, errorClass, error);
						/* Discard the large buf and reset it back to smaller size to freeup heap*/
						//丢弃大的buf 重设到更小的容量 释放内存
						if (buf.size() > maxRespSize) {
							LOG.warn("Large response size " + buf.size()
									+ " for call " + call.toString());
							buf = new ByteArrayOutputStream(
									INITIAL_RESP_BUF_SIZE);
						}
						//给客户端响应请求
						responder.doRespond(call);//Responder在Server构造器初始化
					}
				} catch (InterruptedException e) {
					if (running) { // unexpected -- log it
						LOG.info(getName() + " caught: " + e);
					}
				} catch (Exception e) {
					LOG.info(getName() + " caught: " + e);
				}
			}
			LOG.info(getName() + ": exiting");
		}
	}

	/**
	 * Constructs a server listening on the named port and address. Parameters
	 * passed must be of the named class. The
	 * <code>handlerCount</handlerCount> determines
	 * the number of handler threads that will be used to process calls.
	 */
	protected Server(String bindAddress, int port, int handlerCount)
			throws IOException {
		this.bindAddress = bindAddress;
		this.port = port;
		this.handlerCount = handlerCount;
		this.socketSendBufferSize = 0;
		this.maxQueueSize = IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT;
		this.maxRespSize = IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT;
		this.readThreads = IPC_SERVER_RPC_READ_THREADS_DEFAULT;
		this.callQueue = new LinkedBlockingQueue<Call>(maxQueueSize);
		// this.maxIdleTime = 2*conf.getInt("ipc.client.connection.maxidletime",
		// 1000);
		this.maxIdleTime = 2 * 1000;
		// this.maxConnectionsToNuke = conf.getInt("ipc.client.kill.max", 10);
		this.maxConnectionsToNuke = 10;
		// this.thresholdIdleConnections =
		// conf.getInt("ipc.client.idlethreshold", 4000);
		this.thresholdIdleConnections = 4000;

		// Start the listener here and let it bind to the port
		listener = new Listener();
		this.port = listener.getAddress().getPort();
		// this.tcpNoDelay = conf.getBoolean("ipc.server.tcpnodelay", false);
		this.tcpNoDelay = false;

		//创建Responder
		responder = new Responder();
	}

	private void closeConnection(Connection connection) {
		synchronized (connectionList) {
			if (connectionList.remove(connection))
				numConnections--;
		}
		try {
			connection.close();
		} catch (IOException e) {
		}
	}

	/**
	 * Setup response for the IPC Call.
	 * 
	 * @param response
	 *            buffer to serialize the response into
	 * @param call
	 *            {@link Call} to which we are setting up the response
	 * @param status
	 *            {@link Status} of the IPC call
	 * @param rv
	 *            return value for the IPC Call, if the call was successful
	 * @param errorClass
	 *            error class, if the the call failed
	 * @param error
	 *            error message, if the call failed
	 * @throws IOException
	 */
	//为Sever.Call创建响应
	private void setupResponse(ByteArrayOutputStream response, Call call,
			Status status, Serializable rv, String errorClass, String error)
			throws IOException {
		response.reset();
		DataOutputStream out = new DataOutputStream(response);
		out.writeInt(call.id); // write call id
		out.writeInt(status.state); // write status

		if (status == Status.SUCCESS) {
			ObjectOutputStream oos = new ObjectOutputStream(out);
			oos.writeObject(rv);  //Invocation
			oos.flush(); //ObjectOutputStream具有缓冲功能
			
//			rv.write(out);
		} else {
			WritableUtils.writeString(out, errorClass);
			WritableUtils.writeString(out, error);
		}
		//将返回结果序列化到Sever.Call的成员变量response中
		call.setResponse(ByteBuffer.wrap(response.toByteArray()));
	}
  
  
	/** Sets the socket buffer size used for responding to RPCs */
	public void setSocketSendBufSize(int size) {
		this.socketSendBufferSize = size;
	}

	/** Starts the service. Must be called before any calls will be handled. */
	public synchronized void start() {
		responder.start();
		listener.start();
		handlers = new Handler[handlerCount];

		for (int i = 0; i < handlerCount; i++) {
			handlers[i] = new Handler(i);
			handlers[i].start();
		}
	}

	/** Stops the service. No new calls will be handled after this is called. */
	public synchronized void stop() {
		LOG.info("Stopping server on " + port);
		running = false;
		if (handlers != null) {
			for (int i = 0; i < handlerCount; i++) {
				if (handlers[i] != null) {
					handlers[i].interrupt();
				}
			}
		}
		listener.interrupt();
		listener.doStop();
		responder.interrupt();
		notifyAll();
	}

	/**
	 * Wait for the server to be stopped. Does not wait for all subthreads to
	 * finish. See {@link #stop()}.
	 */
	public synchronized void join() throws InterruptedException {
		while (running) {
			wait();
		}
	}

	/**
	 * Return the socket (ip+port) on which the RPC server is listening to.
	 * 
	 * @return the socket (ip+port) on which the RPC server is listening to.
	 */
	public synchronized InetSocketAddress getListenerAddress() {
		return listener.getAddress();
	}

	/** Called for each call. */
	public abstract Serializable call(Class<?> iface, Serializable param,
			long receiveTime) throws IOException;

	/**
	 * The number of open RPC connections
	 * 
	 * @return the number of open rpc connections
	 */
	public int getNumOpenConnections() {
		return numConnections;
	}

	/**
	 * The number of rpc calls in the queue.
	 * 
	 * @return The number of rpc calls in the queue.
	 */
	public int getCallQueueLen() {
		return callQueue.size();
	}

	/**
	 * When the read or write buffer size is larger than this limit, i/o will be
	 * done in chunks of this size. Most RPC requests and responses would be be
	 * smaller.
	 * PS: should not be more than 64KB.
	 */
	private static int NIO_BUFFER_LIMIT = 8 * 1024;
	
	/**
	 * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
	 * If the amount of data is large, it writes to channel in smaller chunks.
	 * This is to avoid jdk from creating many direct buffers as the size of
	 * buffer increases. This also minimizes extra copies in NIO layer as a
	 * result of multiple write operations required to write a large buffer.
	 * 
	 * @see WritableByteChannel#write(ByteBuffer)
	 */
	//向通道里写数据
	private int channelWrite(WritableByteChannel channel, ByteBuffer buffer)
			throws IOException {
		int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ? channel
				.write(buffer) : channelIO(null, channel, buffer);
		return count;
	}

	/**
	 * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}. If
	 * the amount of data is large, it writes to channel in smaller chunks. This
	 * is to avoid jdk from creating many direct buffers as the size of
	 * ByteBuffer increases. There should not be any performance degredation.
	 * 
	 * @see ReadableByteChannel#read(ByteBuffer)
	 */
	private int channelRead(ReadableByteChannel channel, ByteBuffer buffer)
			throws IOException {
		//ReadableByteChannel.read()将字节序列从此通道中读入给定的缓冲区
		int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ? channel
				.read(buffer) : channelIO(channel, null, buffer);
		return count;
	}

	/**
	 * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)} and
	 * {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only one of
	 * readCh or writeCh should be non-null.
	 * 
	 * @see #channelRead(ReadableByteChannel, ByteBuffer)
	 * @see #channelWrite(WritableByteChannel, ByteBuffer)
	 */
	private static int channelIO(ReadableByteChannel readCh,
			WritableByteChannel writeCh, ByteBuffer buf) throws IOException {
		int originalLimit = buf.limit();
		int initialRemaining = buf.remaining();
		int ret = 0;

		while (buf.remaining() > 0) {
			try {
				int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
				buf.limit(buf.position() + ioSize);

				ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);
				if (ret < ioSize) {
					break;
				}
			} finally {
				buf.limit(originalLimit);
			}
		}

		int nBytes = initialRemaining - buf.remaining();
		return (nBytes > 0) ? nBytes : ret;
	}
}
