<?php

class Driver_Exception extends \Exception {
	const EXCEPTION_BASE = 50000000;
	const CONNECTION_FAILED = 10;
	const CONFIGURATION_MISMATCH = 1;
	const INVALID_STATS_RESULT = 2;
	const INVALID_TUBE_NAME = 3;
	
	protected $ExceptionData = null;
	protected $ExceptionMessage = null;
	public function __construct($ExceptionMessage, $ExceptionData) {
		parent::__construct($ExceptionMessage."\n".print_r($ExceptionData, true), crc32($ExceptionMessage));
		$this->ExceptionMessage = $ExceptionMessage." (".(defined("static::".$ExceptionMessage)?constant("static::".$ExceptionMessage):"unknown").")";
		$this->ExceptionData = $ExceptionData;
	}
	public function getExceptionMessage() {
		return $this->ExceptionMessage;
	}

}

?>