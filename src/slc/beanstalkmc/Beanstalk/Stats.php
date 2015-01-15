<?php

/**
 * Class Beanstalk_Stats
 *
 * parses stats results from beanstalkd
 */
class Beanstalk_Stats {
	protected $data;
	public function __construct($data) {
		$this->data = $this->parseYaml($data);
	}
	protected function parseYaml($data) {
		$retArray = array();
		foreach(explode("\n", $data) AS $line) {
			if(strpos($line, ':') !== false) {
				list($field, $value) = explode(":", $line, 2);
				$retArray[$field] = trim($value);
			}
		}
		return $retArray;
	}
	protected function validateData($expectedKeys, array $data) {
		if(sizeof(array_diff($expectedKeys, array_keys($data))) > 0) {
			return false;
		}
		return true;
	}
	public function get($varname) {
		if(isset($this->data[$varname]))
			return $this->data[$varname];
		return null;
	}

}

?>