<?php

namespace slc\beanstalkmc;

/**
 * A single beanstalk job, fetched with reserve.
 */
class Beanstalk_Job {
	protected $BeanstalkConnection;
	protected $handler;
	protected $jobId;
	protected $data;
	protected $size;

	public function __construct($BeanstalkConnection, $handler, $jobId, $data, $size) {
		$this->BeanstalkConnection = $BeanstalkConnection;
		$this->handler             = $handler;
		$this->jobId               = $jobId;
		$this->data                = $data;
		$this->size                = $size;
	}

	/**
	 * Fetches the $data property.
	 * @return mixed
	 */
	public function getData() {
		return $this->data;
	}

	public function getJobId() {
		return $this->jobId;
	}

	/**
	 * Deletes a job.
	 */
	public function delete() {
		$this->BeanstalkConnection->send(sprintf('delete %s', $this->jobId), $this->handler);
		if(Beanstalk::DEBUG) echo "D";
	}

	/**
	 * Releases a job.
	 * @param $delay
	 * @param $priority
	 */
	public function release($delay=1, $priority=1000) {
		$this->BeanstalkConnection->send(sprintf('release %s %d %d', $this->jobId, $priority, $delay), $this->handler);
	}
}

?>