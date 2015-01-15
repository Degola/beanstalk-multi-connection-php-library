<?php

/**
 * Packets which were received from the receive method.
 */
class Driver_Packet {
	protected $beanstalkConnection;
	protected $handler;
	protected $header;
	protected $type;
	protected $data;
	protected $expectedType = null;
	public function __construct(Driver_Connection $beanstalkConnection, $handler, $header, $expectedType = null) {
		$this->beanstalkConnection = $beanstalkConnection;
		$this->handler = $handler;
		$this->header = explode(' ', trim($header));
		$this->expectedType = $expectedType;
		$this->parse();
	}

	/**
	 * Parses the packets received by the receive method depending on the their header:
	 * 'RESERVED' packets are read from the socket connection, 'DELETED', 'TIMED_OUT', 'RELEASED'
	 * and 'WATCHED' packets are just marked accordingly.
	 */
	protected function parse() {
		list($handlerId) = array_keys($this->handler);
		$handler = $this->handler[$handlerId];
		switch($this->header[0]) {
			case 'RESERVED':
				if (Driver_Connection::DEBUG) {
					echo "reading data for reserved packet...";
				}
				$jobId = $this->header[1];
				$size = $this->header[2];
				$data = '';
				$startReadTime = time();

				if(Driver_Connection::DEBUG)
					echo "jobId ".$jobId.", size ".$size."...";

				do {
					$data .= fread($handler, $size);
					$data .= fread($handler, strlen(Driver_Connection::CRLF));
					if(Driver_Connection::DEBUG)
						echo "read (".strlen($data).")...";
				} while(substr($data, strlen(Driver_Connection::CRLF) * -1) != Driver_Connection::CRLF && (time() - $startReadTime) < 20);
				if((time() - $startReadTime) > 19) {
					echo "\nproblem detected while reading reserved data, after 20 seconds no CRLF, stopped reading for jobId $jobId.\n";
				}
				if (Driver_Connection::DEBUG)  {
					echo "done: ".$jobId.' => '.number_format($size, 0, '.', '.')."b\n";
				}
				$this->type = 'package';
				$this->data = new Driver_Job(
					$this->beanstalkConnection,
					$this->handler,
					$jobId,
					$data,
					$size
				);
				break;
			case 'OK':
				$this->type = $this->expectedType;
				$size = $this->header[1];
				$data = '';
				do {
					$data .= fread($handler, $size);
					$data .= fread($handler, strlen(Driver_Connection::CRLF));
				} while(substr($data, strlen(Driver_Connection::CRLF) * -1) != Driver_Connection::CRLF);

				switch($this->expectedType) {
					case 'stats':
						$this->data = new Driver_StatsResult($data);
						break;
					case 'stats-tube':
						$this->data = new Driver_StatsTubeResult($data);
						break;
				}
				break;
			case 'DELETED':
				$this->type = 'deleted';
				break;
			case 'TIMED_OUT':
				$this->type = 'timeout';
				break;
			case 'RELEASED':
				$this->type = 'released';
				break;
			case 'WATCHING':
				$this->type = 'watching status';
				$this->data = implode(' ', $this->header);
				break;
			case 'NOT_FOUND':
				$this->type = 'not found';
				break;
			case 'INSERTED':
				$this->type = 'inserted';
				break;
			case 'BAD_FORMAT':
				throw new Driver_Packet_Exception('BAD_FORMAT', array(
					'Header' => $this->header
				));
				break;
			default:
				if(Driver_Connection::DEBUG)
					echo "unknown result: ".print_r($this->header, true)."\n";
				$this->data = implode(' ', $this->header);
		}
	}

	/**
	 * Fetches the packet's data.
	 *
	 * @return mixed
	 */
	public function getData() {
		return $this->data;
	}

	/**
	 * Fetches the packet's type.
	 *
	 * @return mixed
	 */
	public function getType() {
		return $this->type;
	}

	/**
	 * Fetches the packet's handler.
	 *
	 * @return mixed
	 */
	public function getHandler() {
		return $this->handler;
	}
}

?>