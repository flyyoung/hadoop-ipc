package com.jiangdx.ipc;


import java.io.Serializable;
import java.lang.reflect.Method;

/** A method invocation, including the method name and its parameters. */
public class Invocation implements Serializable {
	/**
	 * UID for serialized class.
	 */
	private static final long serialVersionUID = 8129216020076782983L;

	/** the exchange interface */
	private Class<?> iface;
	private String methodName;           //方法名  
	private Class<?>[] parameterClasses; //参数类型集合 
	private Object[] parameters;         //参数值  
	private Object result;               //结果

	public Invocation() {
	}

	public Invocation(Method method, Object[] parameters) {
		this.methodName = method.getName();
		this.parameterClasses = method.getParameterTypes();
		this.parameters = parameters;
	}

	public Invocation(Class<?> iface, Method method, Object[] parameters) {
		this.iface = iface;
		this.methodName = method.getName();
		this.parameterClasses = method.getParameterTypes();
		this.parameters = parameters;
	}

	/** The name of the method invoked. */
	public String getMethodName() {
		return methodName;
	}

	/** The parameter classes. */
	public Class<?>[] getParameterClasses() {
		return parameterClasses;
	}

	/** The parameter instances. */
	public Object[] getParameters() {
		return parameters;
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(methodName);
		buffer.append("(");
		for (int i = 0; i < parameters.length; i++) {
			if (i != 0)
				buffer.append(", ");
			buffer.append(parameters[i]);
		}
		buffer.append(")");
		return buffer.toString();
	}

	public Class<?> getIface() {
		return iface;
	}

	public void setIface(Class<?> iface) {
		this.iface = iface;
	}

	public Object get() {
		return result;
	}
	
	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}
}