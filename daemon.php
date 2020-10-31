<?php
//ini_set('display_errors', 0);
//ini_set('display_startup_errors', 0);
//error_reporting(0);

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
    public $maxSubChunk = 5;
    protected $stop_server = FALSE;
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
        echo "Running daemon controller".PHP_EOL;

        while (true) {
            while(count($this->currentProcs) >= $this->maxProcesses) {
                sleep(1);
                # echo "sleep ".date('H:i"s').PHP_EOL;
                $this->isNeedNewEvents();
                $this->checkBusyProcesses();

            }
            $this->launchThread();
            sleep(1);
        }
    }

    protected function sendSubChunkToChild() {
        // send to child
        //if (count($this->queue)>$this->maxProcesses && count($this->busyProcs)<$this->maxProcesses) {
        if (count($this->queue)>0 && count($this->busyProcs)<$this->maxProcesses) {
            //echo 'send to child '.(count($this->queue)>$this->maxProcesses)?'x':'y' .PHP_EOL;
            foreach (array_diff(array_keys($this->currentProcs), array_keys($this->busyProcs)) as $thread) {
                // order in sub-chunk, block if next sub-chunk
                $subchunk = [];
                //$i = 0;
                //while (count($subchunk)<=$this->maxProcesses/2 && $i<3*$this->maxProcesses) {
                foreach ($this->queue as $key => $acc) {
                    // echo 'lll: '.$key.' --- '.$acc;
                    if (count($subchunk) > $this->maxSubChunk) break;
                    //if (count($subchunk) > 0) break;
                    // not blocked or in chunk yet
                    //if (empty($this->blockedAccounts) || !in_array($this->queue[$i], array_keys($this->blockedAccounts)) || in_array($this->queue[$i], $subchunk)) {
                    if (empty($this->blockedAccounts) || !in_array($acc, array_keys($this->blockedAccounts)) || in_array($acc, $subchunk)) {
                        //$subchunk = array_merge($subchunk, $this->queue[$key]);
                        $subchunk[$key] = $acc;//$this->queue[$key];
//                            echo "ok! ".$this->queue[$key]. ' ---'.$acc.' ---- '.count($subchunk).PHP_EOL;
                        //$this->blockedAccounts[$this->queue[$i]] = true;
                        $this->blockedAccounts[$acc] = true;
//                          //  echo "before! ".implode(' ', $this->queue).PHP_EOL;
                        unset($this->queue[$key]);
//                          //  echo "after! ".implode(' ', $this->queue).PHP_EOL;
                    } else {
//                            echo 'xxx '.$this->queue[$key].'---'.(empty($this->blockedAccounts)?1:2). ' --- '. (!in_array($this->queue[$key], array_keys($this->blockedAccounts))? 1:2). '---'. (in_array($this->queue[$key], $subchunk)?1:2) .PHP_EOL;
//                            echo 'yyy '.implode(' ', $this->blockedAccounts).PHP_EOL;
//                            echo 'yyy '.implode(' ', array_keys($this->blockedAccounts)).PHP_EOL;
//                            echo implode(' ', $this->queue).PHP_EOL;
//                            echo implode(' ', array_keys($this->queue)).PHP_EOL;
                    }
                    //$i++;

                }
               # echo 'subchunk ' . count($subchunk) . PHP_EOL;
                //$subchunk = array_slice($this->queue, 0, $this->maxProcesses/2, true);
//                    foreach ($subchunk as $acc) {
//                        $this->blockedAccounts[$acc] = true;
//                    }
                $this->busyProcs[$thread] = $subchunk;
                $fp = fopen('threads/' . $thread . ".json", "w");
                fwrite($fp, json_encode((object)$subchunk));
                //array_splice($this->queue, 0, count($subchunk));
                fclose($fp);
            }
        }
    }

    protected function isNeedNewEvents() {
        // get chunk
        if (count($this->queue)< 5*$this->maxProcesses) {
            $this->getNextChunk();
           # echo 'getNextChunk'.count($this->queue).PHP_EOL;
        }
    }

    protected function checkBusyProcesses() {
        // check busy
        // if (!empty($this->currentProcs)) {
       # echo 'check busy'.PHP_EOL;
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
//            } else {
//                echo 'shit happens'.PHP_EOL;
//            }
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
            // $this->queue = array_merge($this->queue, json_decode($contents, true));
            $this->queue = ($this->queue + json_decode($contents, true));
        }
    }

    public function childSignalHandler($signo, $pid = null, $status = null) {
        switch($signo) {
            case SIGTERM:
                // $this->stop_server = true;
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
//        echo 'time: '. date('H:i:s').PHP_EOL;
//        echo 'queue: '. implode(' ', $this->queue).PHP_EOL;
//        echo 'currentProcs: '. implode(' ', array_keys( $this->currentProcs)).PHP_EOL;
//        echo 'busyProcs: '. implode(' ', array_keys($this->busyProcs)).PHP_EOL;
//        echo 'blockedAccounts: '. implode(' ', array_keys($this->blockedAccounts)).PHP_EOL;
        //$sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        $pid = pcntl_fork();
        if ($pid == -1) {
            // error_log('Could not launch new job, exiting');
            return FALSE;
        }
        elseif ($pid) {
            // parent
            $this->currentProcs[$pid] = TRUE;

          //  while (true) {




//            // check busy
//            if (!empty($this->currentProcs)) {
//                echo 'check busy'.PHP_EOL;
//                foreach (array_keys($this->currentProcs) as $thread) {
//                    if (!file_exists('threads/'.$thread.'.json')) {
//                        if (array_key_exists($thread, array_keys($this->busyProcs) )) {
//                            foreach ($this->busyProcs[$thread] as $acc) {
//                                unset($this->blockedAccounts[$acc]);
//                            }
//                            unset($this->busyProcs[$thread]);
//                        } else {
//                            echo 'big shit happens '.implode(' ').PHP_EOL;
//                        }
//                    }
//                }
//            } else {
//                echo 'shit happens'.PHP_EOL;
//            }






//            foreach ($this->currentProcs as $k=>$v) {
//                echo $k;
//            }
//            if (!empty(fgets($sockets[0]))) {
//                if (is_array(unserialize(fgets($sockets[0])))) {
//                    $this->blockedAccounts = unserialize(fgets($sockets[0]));
//                }
//
//                echo "In parent: ". fgets($sockets[0]);
//            }
            //fclose($sockets[0]);

            //fwrite($sockets[1], "child PID: $pid\n");
            //echo fgets($sockets[1]);

            //fclose($sockets[1]);
         //   }
        }
        else {
            //sleep(2);
            // child
//            array_splice($this->blockedAccounts, 0, 1);
//            echo implode(' ', $this->blockedAccounts);
//            echo "child process ID ".getmypid().PHP_EOL;

            while (true) {
                if (file_exists($filename = 'threads/'.getmypid().'.json')) {
                    $handle = fopen($filename, "r");
                    $contents = json_decode($contents1 = fread($handle, filesize($filename)), true);
                    fclose($handle);
//                    if ($contents1!=='{}')
//                     echo '!child '.$contents1.PHP_EOL;
                    foreach ($contents as $order => $acc) {
                        echo "process ".getmypid()." - event #".$order." for acc #".$acc." - ".date('d.m.Y H:i:s') . PHP_EOL;
                        // task action
                        sleep(1);
                    }
                    // signal free
//                    echo "end... unlink ".$filename.PHP_EOL;
                    unlink($filename);
                }
                sleep(1);
            }

            //$this->blockedAccounts = array_merge([rand(10,100)], $this->blockedAccounts);
            /* дочерний процесс */
            //fclose($sockets[1]);
            //fwrite($sockets[0], serialize(array_merge([rand(10,100)], $this->blockedAccounts)));
            //echo fgets($sockets[0]);

            //fclose($sockets[0]);


            //pcntl_signal(SIGCHLD, array($this, "launch111Job"));
            //pcntl_signal(SIGTSTP, array($this, "launch111Job"));
            //exit();
        }
        return TRUE;
    }
}

$daemon = new DaemonClass();
$daemon->run();