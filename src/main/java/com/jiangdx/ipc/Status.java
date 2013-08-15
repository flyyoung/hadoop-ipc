package com.jiangdx.ipc;


/**
 * Status of a Hadoop IPC call.
 */
enum Status {
	SUCCESS(0), ERROR(1), FATAL(-1);

	int state;

	private Status(int state) {
		this.state = state;
	}
}
