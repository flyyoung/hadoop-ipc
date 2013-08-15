package com.jiangdx.ipc;


import java.io.IOException;
import java.lang.reflect.Constructor;

public class RemoteException extends IOException {
	/** For java.io.Serializable */
	private static final long serialVersionUID = 1L;

	private String className;

	public RemoteException(String className, String msg) {
		super(msg);
		this.className = className;
	}

	public String getClassName() {
		return className;
	}

	/**
	 * If this remote exception wraps up one of the lookupTypes then return this
	 * exception.
	 * <p>
	 * Unwraps any IOException.
	 * 
	 * @param lookupTypes
	 *            the desired exception class.
	 * @return IOException, which is either the lookupClass exception or this.
	 */
	public IOException unwrapRemoteException(Class<?>... lookupTypes) {
		if (lookupTypes == null)
			return this;
		for (Class<?> lookupClass : lookupTypes) {
			if (!lookupClass.getName().equals(getClassName()))
				continue;
			try {
				return instantiateException(lookupClass
						.asSubclass(IOException.class));
			} catch (Exception e) {
				// cannot instantiate lookupClass, just return this
				return this;
			}
		}
		// wrapped up exception is not in lookupTypes, just return this
		return this;
	}

	/**
	 * Instantiate and return the exception wrapped up by this remote exception.
	 * 
	 * <p>
	 * This unwraps any <code>Throwable</code> that has a constructor taking a
	 * <code>String</code> as a parameter. Otherwise it returns this.
	 * 
	 * @return <code>Throwable
	 */
	public IOException unwrapRemoteException() {
		try {
			Class<?> realClass = Class.forName(getClassName());
			return instantiateException(realClass.asSubclass(IOException.class));
		} catch (Exception e) {
			// cannot instantiate the original exception, just return this
		}
		return this;
	}

	private IOException instantiateException(Class<? extends IOException> cls)
			throws Exception {
		Constructor<? extends IOException> cn = cls
				.getConstructor(String.class);
		cn.setAccessible(true);
		String firstLine = this.getMessage();
		int eol = firstLine.indexOf('\n');
		if (eol >= 0) {
			firstLine = firstLine.substring(0, eol);
		}
		IOException ex = cn.newInstance(firstLine);
		ex.initCause(this);
		return ex;
	}
}
