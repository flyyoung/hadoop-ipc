package examples;


import java.io.IOException;

public class CodeImpl implements ICode {
	final String ONE_KB = "qwertyuiopasdfghjklzxcvb0123456789qwertyuiopasdfghjklzxcvbnm,./;0123456789qwertyuiopasdfghjklzxcvbnm,./;012345678901234567890123456789qwertyuiopasdfghjklzxcvbnm,./;0123456789qwertyuiopasdfghjklzxcvbnm,./;012345678901234567890123456789qwertyuiopasdfghjklzxcvbnm,./;0123456789qwertyuiopasdfghjklzxcvbnm,./;012345678901234567890123456789qwertyuiopasdfghjklzxcvbnm,./;0123456789qwertyuiopasdfghjklzxcvbnm,./;012345678901234567890123456789qwertyuiopasdfghjklzxcvbnm,./;0123456789qwertyuiopasdfghjklzxcvbnm,./;012345678901234567890123456789qwertyuiopasdfghjklzxcvbnm,./;0123456789qwertyuiopasdfghjklzxcvbnm,./;012345678901234567890123456789qwertyuiopasdfghjklzxcvbnm,./;0123456789qwertyuiopasdfghjklzxcvbnm,./;012345678901234567890123456789qwertyuiopasdfghjklzxcvbnm,./;0123456789qwertyuiopasdfghjklzxcvbnm,./;012345678901234567890123456789qwertyuiopasdfghjklzxcvbnm,./;0123456789qwertyuiopasdfghjklzxcvbnm,./;012345678901234567890123456789qwertyuiopasdfghjklzxcvbnm,./;0123456789qwertyuiopasdfghjklzxcvbnm,./;01234567890123456789";

	@Override
	public Code generate(String man) throws IOException{
		Code code = new Code();
		code.setId(1000);
		StringBuffer buf = new StringBuffer(ONE_KB);
		for (int i = 0; i < 70; i++) {
			buf.append(ONE_KB);	
		}
		code.setLink(buf.toString());
		code.setName(buf.toString());
		
		System.out.println(code.getLink().length());

		return code;
	}
}
