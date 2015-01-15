<?php

namespace slc\beanstalkmc;

/**
 * Handles all connections to the beanstalk server.
 */
class Beanstalk_Connection {
	const DEBUG = false;
	const CRLF  = "\r\n";
	protected $host;
	protected $port         = 11300;
	protected $handlers     = array();
	protected $lastCommands = array();
	protected $onReconnect  = null;

	public function __construct($host, $port=11300, $connections=10) {
		$this->host = $host;
		$this->port = $port;
		for($i = 0; $i < $connections; $i++) {
			if(Beanstalk_Connection::DEBUG) {
				echo "open connection ".($i)."...";
			}
			$this->connect($i);
			if(Beanstalk_Connection::DEBUG) {
				echo "done.\n";
			}
		}
	}

	/**
	 * Sets the onReconnect property.
	 * @param $method - The method which will be called whenever a connection cannot be established.
	 */
	public function setOnReconnect($method) {
		$this->onReconnect = $method;
	}

	/**
	 * Opens a new socket connection with the given ID, if it does not already exist.
	 * @param $id - The ID which will correspond to the newly opened socket connection.
	 * @param int $reconnectCounter - The number of connections calls that have failed.
	 * @return bool
	 * @throws Beanstalk_Exception - If more than 3 connection calls failed or
	 * if either the host or the port are not defined..
	 */
	protected function connect($id, $reconnectCounter=0) {
		if (!$this->isConnected($id)) {
			if (!isset($this->host) || !isset($this->port)) {
				throw new Beanstalk_Exception('CONFIGURATION_MISMATCH', array(
					'host' => isset($this->host) ? $this->host : 'undefined',
					'port' => isset($this->port) ? $this->port : 'undefined',
				));
			}
			$this->handlers[$id] = @fsockopen($this->host, $this->port, $errorNumber, $errorString, 5);
			if($this->isConnected($id)) {
				stream_set_blocking($this->handlers[$id], 0);
			} else {
				if ($reconnectCounter > 3) {
					throw new Beanstalk_Exception('CONNECTION_FAILED', array(
						'host' => $this->host,
						'port' => $this->port,
					));
				}
				usleep(100000);
				$this->connect($id, ++$reconnectCounter);
			}
		}
		return true;
	}

	/**
	 * Checks whether the given ID corresponds to an already open socket connection.
	 * @param $id
	 * @return bool
	 */
	protected function isConnected($id) {
		if (!isset($this->handlers[$id]) || !$this->handlers[$id] || feof($this->handlers[$id])) {
			return false;
		}
		return true;
	}

	/**
	 * Sends a message to either all of the handlers or just a subset.
	 * @param $string - The message that will be sent.
	 * @param null $handlers
	 */
	public function send($string, $handlers=null) {
		if (is_null($handlers)) {
			$handlers = $this->handlers;
		}
		foreach($handlers AS $i => $handler) {
			if(Beanstalk_Connection::DEBUG) {
				echo "sending to connection ".($i)." (".$string.")...";
				$s = microtime(true);
			}
			// check connection first to avoid connection issues
			if (!$this->isConnected($i) || feof($handler)) {
				$this->connect($i);
			}

			@stream_set_blocking($handler, 1);

			if(!fputs($handler, $string.static::CRLF))
				throw new Beanstalk_Connection_Exception('WRITE_FAILED', array('String' => $string));

			fflush($handler);
			@stream_set_blocking($handler, 0);

			$this->lastCommands[$i] = $string;
			if (Beanstalk_Connection::DEBUG) {
				echo "done. (".number_format(microtime(true) - $s, 5)."s)\n";
			}
		}
	}

	/**
	 * Wait for (and then return) packages from all of the currently open socket connections.
	 * @param $callback - If provided, it will be called at the start of each loop. Whenever
	 * the callback provides a return value, the execution of fetchReserved is stopped, so you
	 * need to be careful with this.
	 * @return BeanstalkConnection_Packet[]
	 */
	public function receive($callback=null, $handlers = null, $expectedType = null) {
		if(is_null($handlers)) $handlers = $this->handlers;
		$result = array();
		$fetchTries = array();
		$null = null;
		do {
			if (isset($callback)) {
				$response = call_user_func($callback);
			}
			if (isset($response)) {
				return $response;
			}
			foreach ($handlers AS $connId => $handler) {
				if (!is_bool($handler) && $this->isConnected($connId) && !feof($handler)) {
					if ($line = fgets($handler, 8192)) {
						if (Beanstalk_Connection::DEBUG) {
							echo "read data from connection ".$connId."...";
							$s = microtime(true);
						}
						$result[] = new Beanstalk_Packet(
							$this,
							array($connId => $handler),
							$line,
							$expectedType
						);
						if (Beanstalk_Connection::DEBUG) {
							echo "done. (".number_format(microtime(true) - $s, 5)."s)\n";
						}
					} else {
						//track the number of fetch tries to do a reconnect if it
						//is exceeding
						if (!isset($fetchTries[$connId])) {
							$fetchTries[$connId] = 1;
						} else {
							$fetchTries[$connId]++;
						}
					}
				} else {
					$this->connect($connId);
					if($this->onReconnect) {
						$func = $this->onReconnect;
						$func($this, array($connId => $this->handlers[$connId]), $this->lastCommands[$connId]);
					}
//                    $this->send($this->lastCommands[$connId], array($connId => $handler));
				}
			}
			if(($retSize = sizeof($result)) == 0) {
				usleep(10000);
			}
		} while($retSize == 0);
		return $result;
	}

	/**
	 * defines the tube which should be used
	 *
	 * @param $tube
	 * @param bool $allHandlers
	 * @return array
	 * @throws Beanstalk_Exception
	 */
	public function useTube($tube, $allHandlers = true) {
		if(!$this->validateTubeName($tube))
			throw new Beanstalk_Exception('INVALID_TUBE_NAME', array('Tube' => $tube));

		if($allHandlers === true)
			$handlers = $this->handlers;
		else {
			$handlers = array(
				$this->handlers[array_rand($this->handlers)]
			);
		}
		$this->send(
			sprintf('use %s', $tube),
			$handlers
		);
		$result = $this->receive(null, $handlers, 'using');
		return $handlers;
	}

	/**
	 * adds a single job to beanstalkd
	 *
	 * @param $jobData
	 * @param array $handlers
	 * @param int $priority
	 * @param int $delay
	 * @param int $timeToRun
	 */
	public function addJob($jobData, array $handlers = null, $priority = 0, $delay = 0, $timeToRun = 600) {
		if(is_null($handlers)) $handlers = $this->handlers;

		$jobData = json_encode($jobData);
		$message = sprintf('put %d %d %d %d', $priority, $delay, $timeToRun, strlen($jobData));
		$this->send($message, $handlers);
		$this->send($jobData, $handlers);
		$result = $this->receive(null, $handlers, 'inserted');
	}

	/**
	 * returns statistics from beanstalkd
	 *
	 * @return Beanstalk_StatsResult
	 * @throws Beanstalk_StatsResultException
	 */
	public function getStats() {
		$handler = array($this->handlers[array_rand($this->handlers)]);
		$this->send('stats', $handler);
		$result = $this->receive(null, $handler, 'stats');
		return array_shift($result)->getData();
	}

	/**
	 * returns tube statistics from beanstalkd
	 *
	 * @param $tube name of tube
	 * @return Beanstalk_StatsTubeResult
	 * @throws Beanstalk_StatsResultException
	 * @throws Beanstalk_Exception
	 */
	public function getStatsTube($tube) {
		if(!$this->validateTubeName($tube))
			throw new Beanstalk_Exception('INVALID_TUBE_NAME', array('Tube' => $tube));

		$handler = array($this->handlers[array_rand($this->handlers)]);
		$this->send(sprintf('stats-tube %s', $tube), $handler);
		$tmp = $this->receive(null, $handler, 'stats-tube');
		$result = array_shift($tmp);
		unset($tmp);
		switch($result->getType()) {
			case 'stats-tube':
				return $result->getData();
				break;
			case 'not found':
				return null;
				break;
			case 'bad format':
			default:
				throw new Beanstalk_Exception('INVALID_STATS_RESULT', array('Type' => $result->getType(), 'Data' => $result->getData()));
		}
	}

	/**
	 * returns if the given tube name is valid or not
	 *
	 * @param $tube
	 * @return bool
	 */
	protected function validateTubeName($tube) {
		return (bool)preg_match('/^([a-z0-9+;\$\/\.\(\)]{1})([a-z0-9+;\$\/\-\_\.\(\)]{3,199})$/i', $tube);
	}

	public function disconnect(array $handlers = array()) {
		if(sizeof($handlers) == 0) $handlers = $this->handlers;
		try {
			$this->send('quit', $handlers);
		} catch(Beanstalk_Connection_Exception $ex) {
			if($ex->getCode() !== 50002001)
				throw $ex;
		}
		$this->close($handlers);
		return sizeof($this->handlers);
	}
	protected function close(array $handlers = array()) {
		if(sizeof($handlers) == 0) $handlers = $this->handlers;
		foreach($handlers AS $handlerId => $handler) {
			@fclose($handler);
			unset($this->handlers[$handlerId]);
		}
	}
}

?>