<?php

namespace slc\beanstalkmc;

/**
 * Class Beanstalk_StatsResult
 *
 * allows access to stats command to beanstalkd
 */
class Beanstalk_StatsResult extends Beanstalk_Stats {
	public function __construct($data) {
		parent::__construct($data);
		if(!$this->validateData(array(
			'current-jobs-urgent',
			'current-jobs-ready',
			'current-jobs-reserved',
			'current-jobs-delayed',
			'current-jobs-buried',
			'cmd-put',
			'cmd-peek',
			'cmd-peek-ready',
			'cmd-peek-delayed',
			'cmd-peek-buried',
			'cmd-reserve',
			'cmd-reserve-with-timeout',
			'cmd-delete',
			'cmd-release',
			'cmd-use',
			'cmd-watch',
			'cmd-ignore',
			'cmd-bury',
			'cmd-kick',
			'cmd-touch',
			'cmd-stats',
			'cmd-stats-job',
			'cmd-stats-tube',
			'cmd-list-tubes',
			'cmd-list-tube-used',
			'cmd-list-tubes-watched',
			'cmd-pause-tube',
			'job-timeouts',
			'total-jobs',
			'max-job-size',
			'current-tubes',
			'current-connections',
			'current-producers',
			'current-workers',
			'current-waiting',
			'total-connections',
			'pid',
			'version',
			'rusage-utime',
			'rusage-stime',
			'uptime',
			'binlog-oldest-index',
			'binlog-current-index',
			'binlog-max-size',
		), $this->data)) throw new Beanstalk_StatsResultException('INVALID_FORMAT', array('Data' => $data));
	}
}

?>