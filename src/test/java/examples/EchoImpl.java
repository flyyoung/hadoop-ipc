package examples;


import java.io.IOException;

public class EchoImpl implements Echo {

	@Override
	public String who() throws IOException {
		return "EchoImpl, from RPC-Server";
	}

	@Override
	public void from(String name) throws IOException {
		System.out.println("receipted, name: " + name);
	}
}
