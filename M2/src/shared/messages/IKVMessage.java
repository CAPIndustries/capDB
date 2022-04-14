package shared.messages;

public interface IKVMessage {

	public enum StatusType {
		GET((byte) 0), /* Get - request */
		GET_ERROR((byte) 1), /* requested tuple (i.e. value) not found */
		GET_SUCCESS((byte) 2), /* requested tuple (i.e. value) found */
		PUT((byte) 3), /* Put - request */
		PUT_SUCCESS((byte) 4), /* Put - request successful, tuple inserted */
		PUT_UPDATE((byte) 5), /* Put - request successful, i.e. value updated */
		PUT_ERROR((byte) 6), /* Put - request not successful */
		DELETE_SUCCESS((byte) 7), /* Delete - request successful */
		DELETE_ERROR((byte) 8), /* Delete - request successful */
		BAD_REQUEST((byte) 9), /* Server received a bad request */
		HEARTBEAT((byte) 10), /* Check if server is alive */
		SERVER_STOPPED((byte) 11), /* Server is stopped, no requests are processed */
		SERVER_WRITE_LOCK((byte) 12), /* Server locked for write, only get possible */
		SERVER_NOT_RESPONSIBLE(
				(byte) 13), /*
							 * Request not successful, server not responsible for key
							 */
		METADATA(
				(byte) 14), /*
							 * Upon successful client connection, send metadata to inform client
							 */
		ADD_NODES((byte) 15),
		START((byte) 16),
		STOP((byte) 17),
		SHUTDOWN((byte) 18),
		SHUTDOWN_ECS((byte) 19),
		REMOVE_NODE((byte) 20),
		LIST((byte) 21),
		ACK((byte) 22),
		NACK((byte) 23),
		BALANCE((byte) 24);

		private final byte val;

		private static final StatusType[] hash = StatusType.values();

		StatusType(byte val) {
			this.val = val;
		}

		public byte getVal() {
			return this.val;
		}

		public static StatusType parse(byte val) {
			return hash[val];
		}
	}

	/**
	 * @return the key that is associated with this message, null if not key is
	 *         associated.
	 */
	public String getKey();

	/**
	 * @return the value that is associated with this message, null if not value is
	 *         associated.
	 */
	public String getValue();

	/**
	 * @return a status string that is used to identify request types, response
	 *         types and error
	 *         types associated to the message.
	 */
	public StatusType getStatus();

	public String print();

	public boolean equal(IKVMessage other);

}
