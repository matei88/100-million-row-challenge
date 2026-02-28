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
            $path = parse_url($visit->uri, PHP_URL_PATH);
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

                $serialized = serialize($data);

                if (strlen($serialized) > self::MEMORY_SIZE) {
                    throw new \RuntimeException('Shared memory overflow');
                }

                shmop_write(
                    $this->sharedMemoryId,
                    $serialized,
                    $i * self::MEMORY_SIZE
                );

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
            $raw = shmop_read(
                $this->sharedMemoryId,
                $i * self::MEMORY_SIZE,
                self::MEMORY_SIZE
            );

            $raw = rtrim($raw, "\0");
            if ($raw === '') {
                continue;
            }

            $data = unserialize($raw);

            foreach ($data as $routeId => $dates) {
                $route = $this->routeList[$routeId];

                foreach ($dates as $date => $count) {
                    $results[$route][$date] =
                        ($results[$route][$date] ?? 0) + $count;
                }
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

        // Preallocate outer array (faster, avoids dynamic growth)
        $values = array_fill(0, count($this->routeList), []);

        while (ftell($handle) < $end && ($line = fgets($handle)) !== false) {
            $commaPos = strpos($line, ',');

            if ($commaPos === false) {
                continue;
            }

            $route = substr($line, 0, $commaPos);
            $routeId = $this->routeMap[$route] ?? null;

            if ($routeId === null) {
                continue; // skip unknown routes safely
            }

            $date = trim(substr($line, $commaPos + 1));

            $values[$routeId][$date] =
                ($values[$routeId][$date] ?? 0) + 1;
        }

        fclose($handle);

        return $values;
    }
}
