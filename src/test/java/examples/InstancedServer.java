package examples;


import com.jiangdx.ipc.NetUtils;
import com.jiangdx.ipc.RPC;
import com.jiangdx.ipc.Server;

public class InstancedServer {
	public static final int PORT = 4453;

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

		Echo instance = new EchoImpl();
		ICode code = new CodeImpl();

		Server server = RPC.getServer(host, port);
		server.inscribe(Echo.class, instance);
		server.inscribe(ICode.class, code);
		server.start();
	}

}
