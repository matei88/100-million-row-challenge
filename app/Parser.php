<?php

namespace App;

use App\Commands\Visit;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function shmop_delete;

class Parser
{
    private const int WORKER_COUNT = 8;
    private const int  MEMORY_SIZE  = 5 * 1024 * 1024; // 5MB per worker

    private \Shmop $sharedMemoryId;

    private array $routeMap = [];
    private array $routeList = [];
    private array $paddedRouteIds = [];
    private array $dateCache = [];

    public function __construct()
    {
        // Build a static route map BEFORE fork
        $this->buildRouteMap();

        $this->sharedMemoryId = shmop_open(
            ftok(__FILE__, 't'),
            'c',
            0644,
            self::WORKER_COUNT * self::MEMORY_SIZE
        );
    }

    private function buildRouteMap(): void
    {
        foreach (Visit::all() as $id => $visit) {
            $path = substr($visit->uri, 25);
            $this->routeMap[$path] = $id;
            $this->routeList[$id]  = $path;
            $this->paddedRouteIds[$id] = sprintf('%04d', $id);
        }
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize  = filesize($inputPath);
        $chunkSize = (int) ceil($fileSize / self::WORKER_COUNT);

        $pids = [];

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException('Fork failed');
            }

            if ($pid === 0) {
                // Worker
                $start = $i * $chunkSize;
                $end   = min(($i + 1) * $chunkSize, $fileSize);

                $data = $this->performTask($inputPath, $start, $end);

                $buffer = '';

                foreach ($data as $flatKey => $count) {
                    $routeId = intdiv($flatKey, 100_000_000);
                    $dateInt  = $flatKey % 100_000_000;
                    $buffer .= pack('nnN', $routeId, $dateInt, $count);
                }

                if (strlen($buffer) > self::MEMORY_SIZE) {
                    throw new \RuntimeException('Shared memory overflow');
                }

                $lenHeader = pack('N', strlen($buffer));
                shmop_write($this->sharedMemoryId, $lenHeader . $buffer, $i * self::MEMORY_SIZE);

                exit(0);
            }

            $pids[] = $pid;
        }

        // Wait workers
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        // Merge results
        $results = [];

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $lenData = unpack('N', shmop_read($this->sharedMemoryId, $i * self::MEMORY_SIZE, 4));
            $len = $lenData[1];
            if ($len === 0) {
                continue;
            }
            $raw = shmop_read($this->sharedMemoryId, $i * self::MEMORY_SIZE + 4, $len);

            $recordSize = 8; // 2 + 2 + 4
            $len = strlen(rtrim($raw, "\0"));
            $offset = 0;

            while ($offset + $recordSize <= $len) {
                $record = unpack('nrouteId/ndateDays/Ncount', $raw, $offset);
                $offset += $recordSize;

                if (!isset($this->routeList[$record['routeId']])) {
                    continue;
                }

                $route = $this->routeList[$record['routeId']];
                $date = sprintf('%04d-%02d-%02d',
                    intdiv($record['dateInt'], 10000),
                    intdiv($record['dateInt'] % 10000, 100),
                    $record['dateInt'] % 100
                );

                $results[$route][$date] = ($results[$route][$date] ?? 0) + $record['count'];
            }
        }

        shmop_delete($this->sharedMemoryId);

        file_put_contents($outputPath, json_encode($results));
    }

    private function performTask(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 256 * 1024); // 256KB buffer
        fseek($handle, $start);

        if ($start !== 0) {
            fgets($handle); // skip partial line
        }

        $values = [];

        $pos = $start;
        if ($start !== 0) {
            $pos += strlen(fgets($handle));
        }

        while ($pos < $end && ($line = fgets($handle)) !== false) {
            $pos += strlen($line);
            $route = strtok($line, ',');
            $date = strtok('T');
            $routeId = $this->routeMap[$route] ?? null;
            $dateInt = (int) str_replace('-', '', $date);

            if ($routeId === null) {
                continue;
            }

            $flatKey = $this->paddedRouteIds[$routeId] * 100_000_000 + $dateInt;

            $count = &$values[$flatKey];
            if ($count !== null) {
                $count++;
            } else {
                $values[$flatKey] = 1;
            }
        }

        fclose($handle);

        return $values;
    }
}
