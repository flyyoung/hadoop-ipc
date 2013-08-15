package examples;


import java.net.InetSocketAddress;

import com.jiangdx.ipc.NetUtils;
import com.jiangdx.ipc.RPC;

public class InstancedClient {
	/**
	 * The Server Instance Main.
	 * @param args
	 * 		[port, host]
	 * 		--port	if not configured, the default is {@link InstancedServer.PORT}
	 * 		--host	if not configured, default is the first record of
	 * 				running the command `ifconfig` or `ipconfig`
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String host = NetUtils.getLocalAddress();
		int port = InstancedServer.PORT;
		if (args.length > 0)
			port = Integer.parseInt(args[0]);
		if (args.length > 1)
			host = args[1];

		InetSocketAddress addr = NetUtils.makeSocketAddr(host, port);
		Echo proxy = RPC.getProxy(Echo.class, addr);
		System.out.println("VVVVVVVVVV: " + proxy.who());
		proxy.from("jakkkkkkki");

		ICode proxyCode = RPC.getProxy(ICode.class, addr);
		Code v = proxyCode.generate("bbbbbbbbbbbbbbb");
		System.out.println(v.getLink().length());
	}

}
