<?php

namespace App;

use App\Commands\Visit;

class Parser
{
    private const int WORKER_COUNT = 8;
    private const int DATE_STRIDE = 2000;

    private array $routeMap = [];
    private array $routeList = [];
    private array $dateMap = [];
    private array $dateList = [];

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
        }

        $epoch = strtotime('2021-01-01');
        for ($d = 0; $d < 2000; $d++) {
            $date = date('Y-m-d', $epoch + $d * 86400);
            $this->dateMap[$date] = $d;
            $this->dateList[$d] = $date;
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
                    $routeId = intdiv($flatKey, self::DATE_STRIDE);
                    $dateId  = $flatKey % self::DATE_STRIDE;
                    $buffer .= pack('nnN', $routeId, $dateId, $count);
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

                $route = $this->routeList[$record['routeId']];
                $date  = $this->dateList[$record['dateId']];

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

        $values = [];

        $pos = $start;
        if ($start !== 0) {
            $pos += strlen(fgets($handle));
        }

        while ($pos < $end && ($line = fgets($handle)) !== false) {
            $pos += strlen($line);
            $route = strtok($line, ',');
            $date = strtok('T');
            $routeId = &$this->routeMap[$route];
            $dateId = &$this->dateMap[$date];

            if ($routeId === null || $dateId === null) {
                continue;
            }

            $flatKey = $routeId * self::DATE_STRIDE + $dateId;

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
