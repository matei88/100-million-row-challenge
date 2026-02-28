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

    private const int DATE_EPOCH_TIMESTAMP = 1704060000;

    private \Shmop $sharedMemoryId;

    private array $routeMap = [];
    private array $routeList = [];

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
        $routeMap  = [];
        $routeList = [];

        foreach (Visit::all() as $id => $visit) {
            $path = substr($visit->uri, 25);
            $routeMap[$path] = $id;
            $routeList[$id]  = $path;
        }

        $this->routeMap  = $routeMap;
        $this->routeList = $routeList;
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
                $dateCache = [];

                foreach ($data as $flatKey => $count) {
                    $routeId = (int) substr($flatKey, 0, 4);
                    $date = substr($flatKey, 4);
                    $encodedDate = ($dateCache[$date] ??= $this->encodeDate($date));
                    $buffer .= pack('nnN', $routeId, $encodedDate, $count);
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
                $date  = $this->decodeDate($record['dateDays']);

                $results[$route][$date] = ($results[$route][$date] ?? 0) + $record['count'];
            }
        }

        shmop_delete($this->sharedMemoryId);

        file_put_contents($outputPath, json_encode($results));
    }

    private function performTask(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'rb');
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

            if ($routeId === null) {
                continue;
            }

            $flatKey = sprintf('%04d', $routeId) . $date;

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

    private function encodeDate(string $date): int
    {
        return (int) ((strtotime($date) - self::DATE_EPOCH_TIMESTAMP) / 86400);
    }

    private function decodeDate(int $days): string
    {
        return date('Y-m-d', self::DATE_EPOCH_TIMESTAMP + ($days * 86400));
    }
}
