# beanstalk-multi-connection-php-library
Beanstalk Multi Connection PHP Library

This library is for the beanstalkd message queue (http://kr.github.io/beanstalkd/) and was designed to handle huge amount of messages within high-latency networks where the message ping-pong between the receipient and queue server slows down the processing dramatically.

To avoid that we built a library which makes use of hundred or even thousands of connections to the message queue server to be able retrieve all messages as fast as possible.

We use this library in a productive environment with a throughput of thousands of messages per second. 


## Create instance and connect to beanstalk

	$beanstalk = new \slc\beanstalkmc\Beanstalk(array(
		'Host' => $beanstalk_host,
		'Port' => $beanstalk_port,
		'Connections' => 1
	));

## Publish message / job

	$beanstalk->publishMessage(
		$message,
	    $tube = "DefaultTube",
	    $priority = 0,
	    $delay = 0,
	    $timeToRun = 600
	);


## Watch tube

	$beanstalk->watch($tube);


## Start reserving jobs on all open connections

	$beanstalk->startReserve();


## Fetching jobs as soon as they come in

This is the most important part, all opened connection will checked permanently for new data, newly received data will be putted into an array which will be returned after all open and non-blocking connections were handled.

	while(is_array($jobs = $beanstalk->fetchReserved())) {
		foreach($jobs AS $job) {
	         // $job is a Beanstalk_Job instance, $job->getData() gets the message content
         
	         // delete job
	         $job->delete();
             // or release job with $job->release();
		}
	}

