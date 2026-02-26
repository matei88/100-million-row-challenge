<?php

namespace App;

use function shmop_open;
use function shmop_read;
use function shmop_write;
use function shmop_delete;

final class Parser
{
    private const int WORKER_COUNT = 2;
    private const int MEMORY_SIZE = 10 * 1024 * 1024;

    public function __construct(
        private ?\Shmop $sharedMemoryId = null,
    ) {
        $sharedMemoryKey = ftok(__FILE__, 't');
        /*$existing = @shmop_open($sharedMemoryKey, 'w', 0, 0);
        if ($existing !== false) {
            shmop_delete($existing);
        }*/

        $this->sharedMemoryId = shmop_open($sharedMemoryKey, 'c', 0644, (self::WORKER_COUNT * self::MEMORY_SIZE));
    }

    public function __destruct()
    {
        shmop_delete($this->sharedMemoryId);
    }

    /**
     * @throws \JsonException
     */
    public function parse(string $inputPath, string $outputPath): void
    {
        $pids = [];
        $chunks = $this->get_file_chunks($inputPath);

        // Fork processes
        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException('Could not fork process');
            }

            if ($pid) {
                // Parent process
                $pids[] = $pid;
            } else {
                // Child process
                $this->performTask($inputPath, $chunks[$i][0], $chunks[$i][1], $i);
                exit(0); // Exit child process
            }
        }

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $results = [];
        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $header = shmop_read($this->sharedMemoryId, $i * self::MEMORY_SIZE, 4);
            $length = unpack('N', $header)[1];
            #echo "Worker $i: serialized length = $length, MEMORY_SIZE = " . self::MEMORY_SIZE . "\n";

            $lines = shmop_read($this->sharedMemoryId, $i * self::MEMORY_SIZE + 4, $length);
            $chunk = igbinary_unserialize($lines);

            foreach ($chunk as $route => $dates) {
                foreach ($dates as $date => $count) {
                    $results[$route][$date] = ($results[$route][$date] ?? 0) + $count;
                }
            }
        }

        shmop_delete($this->sharedMemoryId);

        foreach ($results as &$dates) {
            ksort($dates);
        }

        unset($dates);

        file_put_contents($outputPath, json_encode($results, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));
    }

    /**
     * Get the chunks that each process needs to process with start and end position.
     * These positions are aligned to \n chars because we use `fgets()` to read
     * which itself reads till a \n character.
     *
     * @return array<int, array{0: int, 1: int}>
     */
    private function get_file_chunks(string $file): array
    {
        $size = filesize($file);

        if (self::WORKER_COUNT === 1) {
            $chunk_size = $size;
        } else {
            $chunk_size = (int)($size / self::WORKER_COUNT);
        }

        $fp = fopen($file, 'rb');

        $chunks = [];
        $chunk_start = 0;
        while ($chunk_start < $size) {
            $chunk_end = min($size, $chunk_start + $chunk_size);

            if ($chunk_end < $size) {
                fseek($fp, $chunk_end);
                fgets($fp);
                $chunk_end = ftell($fp);
            }

            $chunks[] = [
                $chunk_start,
                $chunk_end
            ];

            $chunk_start = $chunk_end;
        }

        fclose($fp);
        return $chunks;
    }

    private function performTask(string $file, int $chunk_start, int $chunk_end, int $processId): void
    {
        $values = [];

        $handle = fopen($file, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $chunk_start);

        while (($line = fgets($handle)) !== false && ftell($handle) <= $chunk_end) {
            // start line before `/blog/` char 19
            $line = substr($line, 19);
            $route = strtok($line, ',');
            $date = strtok('T');

            $count = &$values[$route][$date];
            if ($count !== null) {
                $count++;
            } else {
                $values[$route][$date] = 1;
            }
        }

        fclose($handle);

        $serialized = igbinary_serialize($values);
        $packed = pack('N', strlen($serialized)) . $serialized; // 4-byte length prefix
        shmop_write($this->sharedMemoryId, $packed, $processId * self::MEMORY_SIZE);
    }
}