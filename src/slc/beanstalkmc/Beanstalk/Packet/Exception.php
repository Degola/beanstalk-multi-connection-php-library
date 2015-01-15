<?php

namespace slc\beanstalkmc;

class Beanstalk_Packet_Exception extends Beanstalk_Exception {
	const EXCEPTION_BASE = 50003000;
	const BAD_FORMAT = 1;
}

?>