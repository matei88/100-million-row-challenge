<?php

namespace App;

use App\Commands\Visit;

class Parser
{
    private const int WORKER_COUNT = 8;

    private array $routeMap = [];
    private array $routeList = [];
    private array $paddedRouteIds = [];

    public function __construct()
    {
        // Build a static route map BEFORE fork
        $this->buildRouteMap();
    }

    private function workerFile(int $i): string
    {
        return sys_get_temp_dir() . "/100m_worker_{$i}.bin";
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

                file_put_contents($this->workerFile($i), $buffer);
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
            $path = $this->workerFile($i);
            $raw = file_get_contents($path);
            unlink($path);

            $recordSize = 8; // 2 + 2 + 4
            $len = strlen($raw);
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

        file_put_contents($outputPath, json_encode($results));
    }

    private function performTask(string $inputPath, int $start, int $end): array
    {
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0); // 256KB buffer
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
