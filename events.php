<?php
set_time_limit(0);
$baseDir = dirname(__FILE__);
ini_set('error_log',$baseDir.'/error.log');
fclose(STDIN);
fclose(STDOUT);
fclose(STDERR);
$STDIN = fopen('/dev/null', 'r');
$STDOUT = fopen($baseDir.'/events.log', 'ab');
$STDERR = fopen($baseDir.'/error.log', 'ab');

pcntl_signal_dispatch();

class EventsClass {
    public $accountsQty = 1000;
    public $eventsQty = 10000;
    public $maxSendPerTime = 20;
    protected $eventsQueue;

    public function __construct() {
        echo "Сonstructed events generator - ".date('d.m.Y H:i:s').PHP_EOL;
        if (!file_exists(__DIR__.DIRECTORY_SEPARATOR.'/events')) {
            mkdir(__DIR__.'/events', 0775);
        }
    }

    public function generateQueue() {
        $queue = range(1,$this->eventsQty);
        $accountsQty = $this->accountsQty;

        array_walk(
            $queue,
            function (&$v) use ($accountsQty) {
                $v = rand(1, $accountsQty);
            }
        );
        $this->eventsQueue = $queue;
        echo "Generated ".count($this->eventsQueue)." events for ". $this->accountsQty. " accounts - ".date('d.m.Y H:i:s') . PHP_EOL;
    }

    public function run() {
        echo "Running events generator - ".date('d.m.Y H:i:s').PHP_EOL;
        $wereSended = 0;
        $chunkIterator = 1;

        $this->generateQueue();

        while ($wereSended < $this->eventsQty) {
            $fp = fopen(__DIR__.'/events/'.$chunkIterator.".json", "w");
            fwrite($fp, json_encode((object)$chunkArr = array_slice($this->eventsQueue, $wereSended, rand(5, $this->maxSendPerTime), true)));
            fclose($fp);
            $wereSended += count($chunkArr);
            $chunkIterator += 1;
            echo "Chunk $chunkIterator (length = ".count($chunkArr).") was sended - ".date('d.m.Y H:i:s')." : ".implode(', ', array_map(
                function ($v, $k) { return $k."=>".$v; },
                $chunkArr,
                array_keys($chunkArr)
            )) . PHP_EOL;
            sleep(1);
        }
    }
}

$daemon = new EventsClass();
$daemon->run();