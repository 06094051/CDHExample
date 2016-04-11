package com.mfniu.example.zookeeper;

public class ZookeeperApp {

	public static void main(String[] args) {

		args = new String[] { "datacenter1:2181,datacenter2:2181,datacenter3:2181", "/mfniu/monitor/cmd", "logs/zoo.log", "dir" };

		if (args.length < 4) {
			System.err.println("USAGE: Executor hostPort znode filename program [args ...]");
			System.exit(2);
		}

		String hostPort = args[0];
		String znode = args[1];
		String filename = args[2];
		String exec[] = new String[args.length - 3];
		System.arraycopy(args, 3, exec, 0, exec.length);

		try {

			new Executor(hostPort, znode, filename, exec).run();

		} catch (Exception e) {

			e.printStackTrace();

		}

	}

}
