<?php
set_time_limit(0);

$child_pid = pcntl_fork();
if ($child_pid) {
    exit();
}
posix_setsid();

$baseDir = dirname(__FILE__);
ini_set('error_log',$baseDir.'/error.log');
fclose(STDIN);
fclose(STDOUT);
fclose(STDERR);
$STDIN = fopen('/dev/null', 'r');
$STDOUT = fopen($baseDir.'/daemon.log', 'ab');
$STDERR = fopen($baseDir.'/error.log', 'ab');

pcntl_signal_dispatch();

class DaemonClass {
    public $maxProcesses = 50;
    public $maxSubChunk = 10;
    protected $currentProcs = array();
    protected $busyProcs = array();

    protected $blockedAccounts = [];
    protected $queue = [];

    public function __construct() {
        if (!file_exists(__DIR__.DIRECTORY_SEPARATOR.'/threads')) {
            mkdir(__DIR__.'/threads', 0775);
        }
        if (!file_exists(__DIR__.DIRECTORY_SEPARATOR.'/processed_events')) {
            mkdir(__DIR__.'/processed_events', 0775);
        }
        pcntl_signal(SIGTERM, array($this, "childSignalHandler"));
        pcntl_signal(SIGCHLD, array($this, "childSignalHandler"));
    }

    public function run() {
        // echo "Running daemon controller".PHP_EOL;
        while (true) {
            while(count($this->currentProcs) >= $this->maxProcesses) {
                sleep(1);
                $this->isNeedNewEvents();
                $this->checkBusyProcesses();

            }
            $this->launchThread();
            sleep(1);
        }
    }

    protected function sendSubChunkToChild() {
        if (count($this->queue)>0 && count($this->busyProcs)<$this->maxProcesses) {
            foreach (array_diff(array_keys($this->currentProcs), array_keys($this->busyProcs)) as $thread) {
                // order in sub-chunk, block if next sub-chunk
                $subchunk = [];
                foreach ($this->queue as $key => $acc) {
                    if (count($subchunk) > $this->maxSubChunk) break;
                    if (empty($this->blockedAccounts) || !in_array($acc, array_keys($this->blockedAccounts)) || in_array($acc, $subchunk)) {
                        $subchunk[$key] = $acc;
                        $this->blockedAccounts[$acc] = true;
                        unset($this->queue[$key]);
                    }
                }
                $this->busyProcs[$thread] = $subchunk;
                $fp = fopen('threads/' . $thread . ".json", "w");
                fwrite($fp, json_encode((object)$subchunk));
                fclose($fp);
            }
        }
    }

    protected function isNeedNewEvents() {
        if (count($this->queue)< 5*$this->maxProcesses) {
            $this->getNextChunk();
        }
    }

    protected function checkBusyProcesses() {
        foreach (array_keys($this->currentProcs) as $thread) {
            if (!file_exists('threads/'.$thread.'.json')) {
                if (array_key_exists($thread,  $this->busyProcs)) {
                    foreach ($this->busyProcs[$thread] as $acc) {
                        unset($this->blockedAccounts[$acc]);
                    }
                    unset($this->busyProcs[$thread]);
                } else {
                   # echo 'big shit happens '.$thread. '-->'.implode(' ', array_keys($this->busyProcs)).PHP_EOL;
                    $this->sendSubChunkToChild();
                }
            }
        }
    }

    public function getNextChunk() {
        $tempArr = array_map(
            function ($file) {
                return filemtime('events/'.$file);
            },
            $filesArr = array_slice(scandir('events'), 2)
        );

        asort($tempArr);
        $oldestFiles = array_slice(array_keys(
            $tempArr
        ), 0, 3);

        foreach ($oldestFiles as $index) {
            $handle = fopen($fname = 'events/'.$filesArr[$index], "r");
            $contents = fread($handle, filesize($fname));
            fclose($handle);
            copy($fname, 'processed_events/'.$filesArr[$index]);
            unlink($fname);
            $this->queue = ($this->queue + json_decode($contents, true));
        }
    }

    public function childSignalHandler($signo, $pid = null, $status = null) {
        switch($signo) {
            case SIGTERM:
                break;
            case SIGCHLD:
                if (!$pid) {
                    $pid = pcntl_waitpid(-1, $status, WNOHANG);
                }
                while ($pid > 0) {
                    if ($pid && isset($this->currentProcs[$pid])) {
                        unset($this->currentProcs[$pid]);
                    }
                    $pid = pcntl_waitpid(-1, $status, WNOHANG);
                }

                break;
            default:
        }
    }

    protected function launchThread() {
        $pid = pcntl_fork();
        if ($pid == -1) {
            return FALSE;
        }
        elseif ($pid) {
            // parent
            $this->currentProcs[$pid] = TRUE;
        }
        else {
            // child
            while (true) {
                if (file_exists($filename = 'threads/'.getmypid().'.json')) {
                    $handle = fopen($filename, "r");
                    $contents = json_decode($contents1 = fread($handle, filesize($filename)), true);
                    fclose($handle);
                    foreach ($contents as $order => $acc) {
                        echo "process ".getmypid()." - event #".$order." for acc #".$acc." - ".date('d.m.Y H:i:s') . PHP_EOL;
                        // task action
                        sleep(1);
                    }
                    unlink($filename);
                }
                sleep(1);
            }
        }
        return TRUE;
    }
}

$daemon = new DaemonClass();
$daemon->run();