<?php

namespace slc\beanstalkmc;

/**
 *
 * User: Sebastian Lagemann <sl@honeytracks.com>
 * Date: 13.11.2012
 * Time: 15:40
 *
 * Small beanstalk driver class which allows fetching messages from the queue faster in high
 * latency environments through multiple connections and socket stream selects.
 */

class Beanstalk {
	const DEBUG = false;
	protected $config = null;
	protected $BeanstalkConnection = null;
	protected $tube = null;

	public function __construct(array $config) {
		$this->config = (object) $config;

		if(!isset($this->config->Host))
			throw new Driver_Exception('CONFIGURATION_MISMATCH', array('Configuration' => $this->config));

		if (!isset($this->config->Port)) {
			$this->config->Port = 11300;
		}
		if (!isset($this->config->Connections)) {
			$this->config->Connections = 10;
		}
	}

	/**
	 * Fetches the Beanstalk connection if it exists and creates it otherwise.
	 * @return Driver_Connection
	 */
	protected function getConnection() {
		if (is_null($this->BeanstalkConnection)) {
			$this->BeanstalkConnection = new Driver_Connection(
				$this->config->Host,
				$this->config->Port,
				$this->config->Connections
			);
			if($this->tube) {
				$this->BeanstalkConnection->setOnReconnect(create_function(
						'$connection, $handler, $lastCommand',
						'try {'.
						'$connection->send(sprintf("watch %s", "'.$this->tube.'"), $handler);'.
						'$connection->send($lastCommand, $handler);'.
						'} catch(Exception $ex) {'.
						'die($ex->getTraceAsString);'.
						'}'
					)
				);
			}
		}
		return $this->BeanstalkConnection;
	}

	/**
	 * @param $tube - The tube where the messages will be fetched from.
	 * @return bool
	 */
	public function watch($tube) {
		$this->tube = $tube;
		$conn = $this->getConnection();
		if (static::DEBUG) {
			echo "watch ".$tube."\n";
		}
		$conn->send(sprintf('watch %s', $tube));
		$counter = 0;
		do {
			$package = $conn->receive();
			if(static::DEBUG) echo "receive\n";
			foreach ($package AS $packet) {
				if (preg_match('/WATCHING/', $packet->getData()) && $packet->getType() == 'watching status') {
					if(static::DEBUG) echo "watching\n";
					$counter++;
				}
			}
		} while ($counter < $this->config->Connections);
		return true;
	}

	public function publishMessage($message, $tube = 'DefaultTube', $priority = 0, $delay = 0, $timeToRun = 600) {
		$handlers = $this->getConnection()->useTube($tube, false);
		$this->getConnection()->addJob($message, $handlers, $priority, $delay, $timeToRun);
	}
	/**
	 * Starts
	 */
	public function startReserve() {
		$this->startReserveCalled = time();
		$this->getConnection()->send('reserve-with-timeout 10');
	}

	/**
	 * Fetches reserved messages and points out whenever a message has been deleted, released or
	 * timed out.
	 * @param $callback - If provided, it will be provided as an argument for each call of the
	 * Beanstalk_Connection::receive() method. Is used in order to stop a beanstalkd
	 * consumer.
	 * @return Driver_Job[]|mixed
	 */
	public function fetchReserved($callback=null) {
		$return = array();
		$response = null;
		$conn = $this->getConnection();
		do {
			$package = $conn->receive($callback);
			if ($package == 'true') {
				return $package;
			}
			foreach($package AS $packet) {
				switch($packet->getType()) {
					case 'package':
						$return[] = $packet->getData();
						if (Beanstalk::DEBUG) {
							echo ".";
						}
						break;
					case 'deleted':
						$conn->send('reserve-with-timeout 10', $packet->getHandler());
						if (Beanstalk::DEBUG) {
							echo "d";
						}
						break;
					case 'released':
						$conn->send('reserve-with-timeout 10', $packet->getHandler());
						if (Beanstalk::DEBUG) {
							echo "r";
						}
						break;
					case 'timeout':
						$conn->send('reserve-with-timeout 10', $packet->getHandler());
						if (Beanstalk::DEBUG) {
							echo "t";
						}
						break;
					case 'not found':
						/**
						 * be careful with this, not found means that a delete operation failed and the job is still in
						 * the queue, ensure that timeout to run value is high enough during publishing the message
						 */
						$conn->send('reserve-with-timeout 10', $packet->getHandler());
						if(Beanstalk::DEBUG)
							echo "N";
						break;
				}
			}
		} while(sizeof($return) == 0);
		return $return;
	}

	/**
	 * returns statistics from beanstalkd
	 *
	 * @return Driver_StatsResult
	 * @throws Driver_StatsResultException
	 */
	public function getStats() {
		return $this->getConnection()->getStats();
	}

	/**
	 * returns tube statistics from beanstalkd
	 *
	 * @param $tube name of tube
	 * @return Driver_StatsTubeResult
	 * @throws Driver_StatsResultException
	 */
	public function getStatsTube($tube) {
		return $this->getConnection()->getStatsTube($tube);
	}

	/**
	 * disconnects get connection
	 *
	 * @param $forceHarshDisconnect tells the disconnect method to not wait for received data and to kill the connection
	 * immediately otherwise it could happen that we hang in an endless loop
	 * @return bool
	 */
	public function disconnect($forceHarshDisconnect = false) {
		$conn = $this->getConnection();
		$remainingConnections = 1;
		if($forceHarshDisconnect === false) {
			do {
				$package = $conn->receive();
				foreach($package AS $packet) {
					$remainingConnections = $conn->disconnect($packet->getHandler());
				}
			} while($remainingConnections > 0);
		} else {
			$conn->disconnect();
		}
		$this->shutdown = true;
	}
}

?>